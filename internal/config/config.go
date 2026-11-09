// Package config loads the service's JSON configuration: which Sites it reports
// on, and the Kubernetes objects that make up each environment.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Config is the whole configuration file.
type Config struct {
	// ImageUpdateAutomation names the ImageUpdateAutomation all Sites share, in
	// flux-system.
	ImageUpdateAutomation string `json:"imageUpdateAutomation"`
	Sites                 []Site `json:"sites"`
}

// Site is one configured Site.
type Site struct {
	ID string `json:"id"`
	// Environments is keyed by the name used in the URL, such as "production".
	Environments map[string]Environment `json:"environments"`
}

// Environment names the objects that make up one environment of a Site. The
// Deployment and HelmRelease live in Namespace; the rest live in flux-system.
type Environment struct {
	Namespace       string `json:"namespace"`
	Deployment      string `json:"deployment"`
	HelmRelease     string `json:"helmRelease"`
	ImagePolicy     string `json:"imagePolicy"`
	ImageRepository string `json:"imageRepository"`
	Kustomization   string `json:"kustomization"`
}

// Load reads and validates the configuration file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: the operator chooses this path; it is not user input.
	if err != nil {
		return nil, fmt.Errorf("read configuration: %w", err)
	}
	cfg, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("configuration %s: %w", path, err)
	}
	return cfg, nil
}

// Parse decodes and validates a configuration document. It rejects unknown
// keys, duplicate keys and duplicate Sites, environments or objects, so that a
// mistake stops the service at startup instead of hiding a Site.
func Parse(data []byte) (*Config, error) {
	if err := rejectDuplicateKeys(data); err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("unexpected data after the JSON document")
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Find returns the environment of a Site, or false when either is unknown.
func (c *Config) Find(siteID, environment string) (Environment, bool) {
	for _, site := range c.Sites {
		if site.ID == siteID {
			env, ok := site.Environments[environment]
			return env, ok
		}
	}
	return Environment{}, false
}

func (c *Config) validate() error {
	if err := checkName("imageUpdateAutomation", c.ImageUpdateAutomation); err != nil {
		return err
	}
	if len(c.Sites) == 0 {
		return errors.New("sites: at least one Site is required")
	}

	var (
		seenSites = map[string]bool{}
		// Each of these objects belongs to exactly one environment.
		seenDeployments = map[string]string{}
		seenReleases    = map[string]string{}
		seenPolicies    = map[string]string{}
	)
	for i, site := range c.Sites {
		if err := checkLabel(fmt.Sprintf("sites[%d].id", i), site.ID); err != nil {
			return err
		}
		if seenSites[site.ID] {
			return fmt.Errorf("sites[%d].id: duplicate Site %q", i, site.ID)
		}
		seenSites[site.ID] = true

		if len(site.Environments) == 0 {
			return fmt.Errorf("site %q: at least one environment is required", site.ID)
		}
		for name, env := range site.Environments {
			where := fmt.Sprintf("site %q environment %q", site.ID, name)
			if err := checkLabel(where, name); err != nil {
				return err
			}
			if err := env.validate(where); err != nil {
				return err
			}
			for _, unique := range []struct {
				seen map[string]string
				what string
				key  string
			}{
				{seenDeployments, "Deployment", env.Namespace + "/" + env.Deployment},
				{seenReleases, "HelmRelease", env.Namespace + "/" + env.HelmRelease},
				{seenPolicies, "ImagePolicy", env.ImagePolicy},
			} {
				if other, dup := unique.seen[unique.key]; dup {
					return fmt.Errorf("%s: %s %s is also used by %s", where, unique.what, unique.key, other)
				}
				unique.seen[unique.key] = where
			}
		}
	}
	return nil
}

func (e Environment) validate(where string) error {
	if err := checkLabel(where+": namespace", e.Namespace); err != nil {
		return err
	}
	for _, field := range []struct{ key, value string }{
		{"deployment", e.Deployment},
		{"helmRelease", e.HelmRelease},
		{"imagePolicy", e.ImagePolicy},
		{"imageRepository", e.ImageRepository},
		{"kustomization", e.Kustomization},
	} {
		if err := checkName(where+": "+field.key, field.value); err != nil {
			return err
		}
	}
	return nil
}

// checkLabel requires a DNS label: Kubernetes namespaces, and the Site and
// environment names that appear in URLs.
func checkLabel(field, value string) error {
	if msgs := validation.IsDNS1123Label(value); len(msgs) > 0 {
		return fmt.Errorf("%s: %q is not a DNS label: %s", field, value, strings.Join(msgs, "; "))
	}
	return nil
}

// checkName requires a Kubernetes object name.
func checkName(field, value string) error {
	if msgs := validation.IsDNS1123Subdomain(value); len(msgs) > 0 {
		return fmt.Errorf("%s: %q is not an object name: %s", field, value, strings.Join(msgs, "; "))
	}
	return nil
}

// rejectDuplicateKeys walks the document's tokens, because encoding/json keeps
// the last of two equal keys without saying so. It compares keys ignoring case
// for the same reason: the decoder matches struct fields case-insensitively.
func rejectDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := walkValue(dec, "$"); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}

func walkValue(dec *json.Decoder, path string) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}

	switch delim {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			keyToken, err := dec.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("%s: object key is not a string", path)
			}
			folded := strings.ToLower(key)
			if seen[folded] {
				return fmt.Errorf("%s: duplicate key %q", path, key)
			}
			seen[folded] = true
			if err := walkValue(dec, path+"."+key); err != nil {
				return err
			}
		}
	case '[':
		for i := 0; dec.More(); i++ {
			if err := walkValue(dec, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	_, err = dec.Token() // The closing delimiter.
	return err
}
