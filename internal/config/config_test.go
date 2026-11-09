package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validConfig = `{
  "imageUpdateAutomation": "image-tags",
  "sites": [
    {
      "id": "fisioindoor",
      "environments": {
        "production": {
          "namespace": "fisioindoor",
          "deployment": "fisioindoor-sws",
          "helmRelease": "fisioindoor",
          "imagePolicy": "fisioindoor-production",
          "imageRepository": "fisioindoor",
          "kustomization": "fisioindoor"
        }
      }
    },
    {
      "id": "vertex-glow",
      "environments": {
        "production": {
          "namespace": "vertex-glow",
          "deployment": "vertex-glow-sws",
          "helmRelease": "vertex-glow",
          "imagePolicy": "vertex-glow-production",
          "imageRepository": "vertex-glow",
          "kustomization": "vertex-glow"
        }
      }
    }
  ]
}`

func TestParseValid(t *testing.T) {
	cfg, err := Parse([]byte(validConfig))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.ImageUpdateAutomation != "image-tags" {
		t.Errorf("ImageUpdateAutomation = %q", cfg.ImageUpdateAutomation)
	}
	env, ok := cfg.Find("vertex-glow", "production")
	if !ok {
		t.Fatal("Find(vertex-glow, production) = not found")
	}
	if env.Namespace != "vertex-glow" || env.Deployment != "vertex-glow-sws" ||
		env.HelmRelease != "vertex-glow" || env.ImagePolicy != "vertex-glow-production" ||
		env.ImageRepository != "vertex-glow" || env.Kustomization != "vertex-glow" {
		t.Errorf("environment = %+v", env)
	}
	if _, ok := cfg.Find("vertex-glow", "staging"); ok {
		t.Error("Find found an environment that isn't configured")
	}
	if _, ok := cfg.Find("nope", "production"); ok {
		t.Error("Find found a Site that isn't configured")
	}
}

func TestParseRejects(t *testing.T) {
	site := func(id, env, body string) string {
		return `{"id":"` + id + `","environments":{"` + env + `":` + body + `}}`
	}
	envBody := func(namespace, deployment string) string {
		return `{"namespace":"` + namespace + `","deployment":"` + deployment + `","helmRelease":"` + namespace + `","imagePolicy":"` + namespace + `-production","imageRepository":"` + namespace + `","kustomization":"` + namespace + `"}`
	}
	doc := func(sites ...string) string {
		return `{"imageUpdateAutomation":"image-tags","sites":[` + strings.Join(sites, ",") + `]}`
	}

	good := envBody("a", "a-sws")

	tests := []struct {
		name string
		doc  string
		want string
	}{
		{"not JSON", `{`, "decode"},
		{"trailing data", doc(site("a", "production", good)) + `{}`, "after the JSON document"},
		{"unknown top-level key", strings.Replace(validConfig, `"sites"`, `"extra":1,"sites"`, 1), "unknown field"},
		{"unknown site key", doc(`{"id":"a","color":"red","environments":{"production":` + good + `}}`), "unknown field"},
		{"unknown environment key", doc(site("a", "production", strings.Replace(good, `"namespace"`, `"replicas":1,"namespace"`, 1))), "unknown field"},
		{"duplicate JSON key", strings.Replace(validConfig, `"id": "fisioindoor"`, `"id": "fisioindoor", "id": "other"`, 1), "duplicate key"},
		{"duplicate environment", doc(`{"id":"a","environments":{"production":` + good + `,"production":` + good + `}}`), "duplicate key"},
		{"duplicate key differing in case", strings.Replace(validConfig, `"id": "fisioindoor"`, `"id": "fisioindoor", "ID": "other"`, 1), "duplicate key"},
		{"duplicate Site", doc(site("a", "production", good), site("a", "staging", envBody("b", "b-sws"))), `duplicate Site "a"`},
		{"two environments share a Deployment", doc(site("a", "production", good), site("b", "production", strings.Replace(envBody("b", "b-sws"), `"namespace":"b","deployment":"b-sws"`, `"namespace":"a","deployment":"a-sws"`, 1))), "Deployment a/a-sws is also used by"},
		{"two environments share a HelmRelease", doc(site("a", "production", good), site("b", "production", strings.Replace(envBody("b", "b-sws"), `"namespace":"b","deployment":"b-sws","helmRelease":"b"`, `"namespace":"a","deployment":"b-sws","helmRelease":"a"`, 1))), "HelmRelease a/a is also used by"},
		{"two environments share an ImagePolicy", doc(site("a", "production", good), site("b", "production", strings.Replace(envBody("b", "b-sws"), `b-production`, `a-production`, 1))), "ImagePolicy a-production is also used by"},
		{"no sites", `{"imageUpdateAutomation":"image-tags","sites":[]}`, "at least one Site"},
		{"no automation", `{"sites":[` + site("a", "production", good) + `]}`, "imageUpdateAutomation"},
		{"no environments", doc(`{"id":"a","environments":{}}`), "at least one environment"},
		{"empty id", doc(site("", "production", good)), "not a DNS label"},
		{"id is not a DNS label", doc(site("Bad_Site", "production", good)), "not a DNS label"},
		{"environment is not a DNS label", doc(site("a", "Prod", good)), "not a DNS label"},
		{"missing namespace", doc(site("a", "production", strings.Replace(good, `"namespace":"a"`, `"namespace":""`, 1))), "namespace"},
		{"missing deployment", doc(site("a", "production", strings.Replace(good, `"deployment":"a-sws"`, `"deployment":""`, 1))), "deployment"},
		{"missing helmRelease", doc(site("a", "production", strings.Replace(good, `"helmRelease":"a"`, `"helmRelease":""`, 1))), "helmRelease"},
		{"missing imagePolicy", doc(site("a", "production", strings.Replace(good, `"imagePolicy":"a-production"`, `"imagePolicy":""`, 1))), "imagePolicy"},
		{"missing imageRepository", doc(site("a", "production", strings.Replace(good, `"imageRepository":"a"`, `"imageRepository":""`, 1))), "imageRepository"},
		{"missing kustomization", doc(site("a", "production", strings.Replace(good, `"kustomization":"a"`, `"kustomization":""`, 1))), "kustomization"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.doc))
			if err == nil {
				t.Fatal("Parse succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestParseAllowsSharedRepositoryAndKustomization(t *testing.T) {
	// Production and staging of a Site share an ImageRepository; a Kustomization
	// may render more than one environment.
	doc := `{"imageUpdateAutomation":"image-tags","sites":[{"id":"a","environments":{
		"production":{"namespace":"a","deployment":"a-sws","helmRelease":"a","imagePolicy":"a-production","imageRepository":"a","kustomization":"apps"},
		"staging":{"namespace":"a-staging","deployment":"a-sws","helmRelease":"a","imagePolicy":"a-staging","imageRepository":"a","kustomization":"apps"}
	}}]}`
	if _, err := Parse([]byte(doc)); err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestExampleConfigIsValid(t *testing.T) {
	if _, err := Load(filepath.Join("..", "..", "config.example.json")); err != nil {
		t.Fatalf("config.example.json: %v", err)
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(validConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if _, err := Load(filepath.Join(dir, "absent.json")); err == nil {
		t.Error("Load of a missing file succeeded")
	}

	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"unexpected":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(bad); err == nil || !strings.Contains(err.Error(), bad) {
		t.Errorf("Load error = %v, want it to name the file", err)
	}
}
