// Package rollout holds the rules that turn what the cluster reports into a
// Site's status. Nothing here reads the cluster or the clock: the caller passes
// an Observation and the current time.
package rollout

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Grace is how long a Flux object may stay Ready=False before it counts as a
// failure, measured from the condition's lastTransitionTime. These objects
// retry on their own, so a shorter failure usually clears up.
const Grace = 10 * time.Minute

// Phase is where a Site's rollout stands.
type Phase string

// The three phases of the HTTP contract.
const (
	PhaseLive      Phase = "live"
	PhaseDeploying Phase = "deploying"
	PhaseFailed    Phase = "failed"
)

// Condition is one entry of a Kubernetes object's status.conditions.
type Condition struct {
	Type               string
	Status             string
	Reason             string
	Message            string
	LastTransitionTime time.Time
}

// Object is what the service saw of one configured object.
type Object struct {
	Kind       string
	Namespace  string
	Name       string
	Found      bool
	Conditions []Condition
}

// ReplicaSet is one ReplicaSet owned by the environment's Deployment.
type ReplicaSet struct {
	Name              string
	Image             string
	CreatedAt         time.Time
	Revision          int64
	AvailableReplicas int64
}

// Observation is everything the rules look at for one environment.
type Observation struct {
	Deployment            Object
	HelmRelease           Object
	ImageUpdateAutomation Object
	Kustomization         Object
	ImageRepository       Object
	ImagePolicy           Object
	// Target is the ImagePolicy's status.latestRef.tag, empty when it has none.
	Target string
	// ReplicaSets are those the Deployment owns, in any order.
	ReplicaSets []ReplicaSet
}

// Status is the JSON answer for one environment. Target, Running and Reason are
// null when there is nothing to report.
type Status struct {
	Target  *string `json:"target"`
	Running *string `json:"running"`
	Phase   Phase   `json:"phase"`
	Reason  *string `json:"reason"`
}

// Entry is one row of an environment's rollout history.
type Entry struct {
	Tag       *string   `json:"tag"`
	CreatedAt time.Time `json:"createdAt"`
}

// Evaluate applies the phase rules to an observation at the time now.
func Evaluate(o Observation, now time.Time) Status {
	running, runningNote := runningTag(o.ReplicaSets)
	onTarget := o.Target != "" && running == o.Target

	rollout := rolloutSignals(o, now)
	discovery := discoverySignals(o, now)
	unknown := unknownObjects(o)
	if runningNote != "" {
		unknown = append(unknown, runningNote)
	}

	var reasons []string
	if !onTarget {
		// A rollout signal counts only while the versions differ, so a passing
		// error can't mark a Site that already runs its target as failed.
		reasons = append(reasons, rollout...)
	}
	// A discovery signal stops target from moving, so it is reported whatever
	// the versions say. It never changes the phase.
	reasons = append(reasons, discovery...)
	reasons = append(reasons, unknown...)

	phase := PhaseDeploying
	switch {
	case onTarget && len(unknown) == 0:
		phase = PhaseLive
	case !onTarget && len(rollout) > 0:
		phase = PhaseFailed
	}

	return Status{
		Target:  optional(o.Target),
		Running: optional(running),
		Phase:   phase,
		Reason:  optional(strings.Join(reasons, "; ")),
	}
}

// History lists the ReplicaSets newest first, one entry per run of the same
// image. An entry's time is that of the oldest ReplicaSet in its run: template
// changes that keep the image make more ReplicaSets, but the rollout of that
// image started with the first.
func History(replicaSets []ReplicaSet) []Entry {
	entries := []Entry{}
	previous := ""
	for _, rs := range newestFirst(replicaSets) {
		if rs.Image == previous && len(entries) > 0 {
			entries[len(entries)-1].CreatedAt = rs.CreatedAt
			continue
		}
		previous = rs.Image
		entries = append(entries, Entry{Tag: optional(ImageTag(rs.Image)), CreatedAt: rs.CreatedAt})
	}
	return entries
}

// ImageTag returns the tag of an image reference, which may also carry a
// digest, or "" when it has none. A colon before the last slash belongs to the
// registry's port, not to a tag.
func ImageTag(image string) string {
	if at := strings.IndexByte(image, '@'); at >= 0 {
		image = image[:at]
	}
	name := image[strings.LastIndexByte(image, '/')+1:]
	if colon := strings.LastIndexByte(name, ':'); colon >= 0 {
		return name[colon+1:]
	}
	return ""
}

// runningTag returns the tag of the newest ReplicaSet with available replicas.
// The note is set when that ReplicaSet's image has no tag: nothing is wrong
// with no ReplicaSet being available, as during a pod's restart, so that case
// leaves the note empty and only reports running as unknown.
func runningTag(replicaSets []ReplicaSet) (tag, note string) {
	for _, rs := range newestFirst(replicaSets) {
		if rs.AvailableReplicas > 0 {
			tag = ImageTag(rs.Image)
			if tag == "" {
				note = fmt.Sprintf("ReplicaSet %s runs image %s, which has no tag", rs.Name, rs.Image)
			}
			return tag, note
		}
	}
	return "", ""
}

func newestFirst(replicaSets []ReplicaSet) []ReplicaSet {
	sorted := slices.Clone(replicaSets)
	slices.SortFunc(sorted, func(a, b ReplicaSet) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		// Timestamps have one-second resolution: the revision breaks ties, and
		// the name keeps the order the same on every request.
		return cmp.Or(cmp.Compare(b.Revision, a.Revision), strings.Compare(b.Name, a.Name))
	})
	return sorted
}

func rolloutSignals(o Observation, now time.Time) []string {
	var signals []string
	if c, ok := find(o.Deployment, "Progressing"); ok && c.Status == "False" && c.Reason == "ProgressDeadlineExceeded" {
		signals = append(signals, describe(o.Deployment, c))
	}
	if c, ok := find(o.HelmRelease, "Released"); ok && c.Status == "False" {
		signals = append(signals, describe(o.HelmRelease, c))
	}
	for _, obj := range []Object{o.ImageUpdateAutomation, o.Kustomization} {
		if c, ok := notReadyPastGrace(obj, now); ok {
			signals = append(signals, describe(obj, c))
		}
	}
	return signals
}

func discoverySignals(o Observation, now time.Time) []string {
	var signals []string
	for _, obj := range []Object{o.ImageRepository, o.ImagePolicy} {
		if c, ok := notReadyPastGrace(obj, now); ok {
			signals = append(signals, describe(obj, c))
		}
	}
	return signals
}

// notReadyPastGrace finds a Ready=False older than Grace. A condition without a
// timestamp can't be shown to have lasted, so it doesn't count.
func notReadyPastGrace(obj Object, now time.Time) (Condition, bool) {
	c, ok := find(obj, "Ready")
	if !ok || c.Status != "False" || c.LastTransitionTime.IsZero() {
		return Condition{}, false
	}
	return c, now.Sub(c.LastTransitionTime) > Grace
}

// unknownObjects reports what stops the service from giving a full answer: a
// configured object that doesn't exist, or an ImagePolicy with no resolved tag.
func unknownObjects(o Observation) []string {
	var notes []string
	for _, obj := range []Object{
		o.Deployment, o.HelmRelease, o.ImageUpdateAutomation,
		o.Kustomization, o.ImageRepository, o.ImagePolicy,
	} {
		if !obj.Found {
			notes = append(notes, fmt.Sprintf("%s %s/%s not found", obj.Kind, obj.Namespace, obj.Name))
		}
	}
	if o.ImagePolicy.Found && o.Target == "" {
		notes = append(notes, fmt.Sprintf("%s %s/%s has no resolved tag", o.ImagePolicy.Kind, o.ImagePolicy.Namespace, o.ImagePolicy.Name))
	}
	return notes
}

func find(obj Object, conditionType string) (Condition, bool) {
	for _, c := range obj.Conditions {
		if c.Type == conditionType {
			return c, true
		}
	}
	return Condition{}, false
}

func describe(obj Object, c Condition) string {
	text := fmt.Sprintf("%s %s/%s: %s=%s (%s)", obj.Kind, obj.Namespace, obj.Name, c.Type, c.Status, c.Reason)
	if c.Message != "" {
		text += ": " + c.Message
	}
	return text
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
