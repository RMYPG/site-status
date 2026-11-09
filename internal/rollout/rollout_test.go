package rollout

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

const (
	imageOld = "ghcr.io/rmypg/fisioindoor:v1.0.2"
	imageNew = "ghcr.io/rmypg/fisioindoor:v1.0.3"
)

// site returns a healthy Site that runs its target, v1.0.2.
func site() Observation {
	ready := Condition{Type: "Ready", Status: "True", Reason: "Succeeded", LastTransitionTime: now.Add(-24 * time.Hour)}
	return Observation{
		Deployment: Object{Kind: "Deployment", Namespace: "fisioindoor", Name: "fisioindoor-sws", Found: true, Conditions: []Condition{
			{Type: "Progressing", Status: "True", Reason: "NewReplicaSetAvailable"},
		}},
		HelmRelease: Object{Kind: "HelmRelease", Namespace: "fisioindoor", Name: "fisioindoor", Found: true, Conditions: []Condition{
			{Type: "Released", Status: "True", Reason: "UpgradeSucceeded"}, ready,
		}},
		ImageUpdateAutomation: Object{Kind: "ImageUpdateAutomation", Namespace: "flux-system", Name: "image-tags", Found: true, Conditions: []Condition{ready}},
		Kustomization:         Object{Kind: "Kustomization", Namespace: "flux-system", Name: "fisioindoor", Found: true, Conditions: []Condition{ready}},
		ImageRepository:       Object{Kind: "ImageRepository", Namespace: "flux-system", Name: "fisioindoor", Found: true, Conditions: []Condition{ready}},
		ImagePolicy:           Object{Kind: "ImagePolicy", Namespace: "flux-system", Name: "fisioindoor-production", Found: true, Conditions: []Condition{ready}},
		Target:                "v1.0.2",
		ReplicaSets: []ReplicaSet{
			{Name: "fisioindoor-sws-new", Image: imageOld, CreatedAt: now.Add(-48 * time.Hour), Revision: 2, AvailableReplicas: 1},
		},
	}
}

// behind makes target newer than what runs.
func behind(o *Observation) { o.Target = "v1.0.3" }

func notReady(age time.Duration, reason string) Condition {
	return Condition{Type: "Ready", Status: "False", Reason: reason, Message: "it broke", LastTransitionTime: now.Add(-age)}
}

// failing gives obj a not-Ready episode of the given age that has seen
// Ready=False.
func failing(obj *Object, age time.Duration, reason string) {
	failure := notReady(age, reason)
	obj.NotReady = &Episode{Since: now.Add(-age), Failure: &failure}
}

// signal replaces obj's conditions.
func signal(obj *Object, conditions ...Condition) {
	obj.Conditions = conditions
}

func TestEvaluate(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*Observation)
		// Empty means null in the answer.
		wantTarget, wantRunning string
		wantPhase               Phase
		// A substring the reason must contain; empty means reason must be null.
		wantReason string
	}{
		{
			name:        "running equals target",
			wantTarget:  "v1.0.2",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseLive,
		},
		{
			name:        "running differs from target and nothing is wrong",
			mut:         behind,
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseDeploying,
		},

		// Rollout signals, while running differs from target.
		{
			name: "Deployment past its progress deadline",
			mut: func(o *Observation) {
				behind(o)
				signal(&o.Deployment, Condition{Type: "Progressing", Status: "False", Reason: "ProgressDeadlineExceeded", Message: "stuck"})
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseFailed,
			wantReason:  "Deployment fisioindoor/fisioindoor-sws: Progressing=False (ProgressDeadlineExceeded): stuck",
		},
		{
			name: "Deployment Progressing=False for another reason is not a failure",
			mut: func(o *Observation) {
				behind(o)
				signal(&o.Deployment, Condition{Type: "Progressing", Status: "False", Reason: "SomethingElse"})
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseDeploying,
		},
		{
			name: "HelmRelease upgrade failed",
			mut: func(o *Observation) {
				behind(o)
				signal(&o.HelmRelease, Condition{Type: "Released", Status: "False", Reason: "UpgradeFailed", Message: "boom"})
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseFailed,
			wantReason:  "HelmRelease fisioindoor/fisioindoor: Released=False (UpgradeFailed): boom",
		},
		{
			name: "ImageUpdateAutomation not ready past the grace period",
			mut: func(o *Observation) {
				behind(o)
				failing(&o.ImageUpdateAutomation, 11*time.Minute, "PushFailed")
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseFailed,
			wantReason:  "ImageUpdateAutomation flux-system/image-tags: Ready=False (PushFailed): it broke",
		},
		{
			name: "Kustomization not ready past the grace period",
			mut: func(o *Observation) {
				behind(o)
				failing(&o.Kustomization, 11*time.Minute, "HealthCheckFailed")
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseFailed,
			wantReason:  "Kustomization flux-system/fisioindoor: Ready=False (HealthCheckFailed): it broke",
		},

		// HelmRelease noise.
		{
			name: "Ready=Unknown during a no-op upgrade of a Site on target",
			mut: func(o *Observation) {
				signal(&o.HelmRelease,
					Condition{Type: "Ready", Status: "Unknown", Reason: "Progressing", LastTransitionTime: now.Add(-time.Hour)},
					Condition{Type: "Released", Status: "True", Reason: "UpgradeSucceeded"},
				)
			},
			wantTarget:  "v1.0.2",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseLive,
		},
		{
			name: "Ready=Unknown during a no-op upgrade while a rollout is waiting",
			mut: func(o *Observation) {
				behind(o)
				signal(&o.HelmRelease,
					Condition{Type: "Ready", Status: "Unknown", Reason: "Progressing", LastTransitionTime: now.Add(-time.Hour)},
					Condition{Type: "Released", Status: "Unknown", Reason: "Progressing"},
				)
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseDeploying,
		},
		{
			name: "Ready=False on a HelmRelease is not a signal: only Released is",
			mut: func(o *Observation) {
				behind(o)
				signal(&o.HelmRelease,
					notReady(time.Hour, "UpgradeFailed"),
					Condition{Type: "Released", Status: "True", Reason: "UpgradeSucceeded"},
				)
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseDeploying,
		},
		{
			name: "Ready=Unknown past the grace period on a Flux object is not a failure",
			mut: func(o *Observation) {
				behind(o)
				o.Kustomization.NotReady = &Episode{Since: now.Add(-time.Hour)}
				o.ImageRepository.NotReady = &Episode{Since: now.Add(-time.Hour)}
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseDeploying,
		},

		// Discovery signals leave the phase alone and set the reason.
		{
			name: "ImageRepository scan failing on a live Site",
			mut: func(o *Observation) {
				failing(&o.ImageRepository, 11*time.Minute, "ScanFailed")
			},
			wantTarget:  "v1.0.2",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseLive,
			wantReason:  "ImageRepository flux-system/fisioindoor: Ready=False (ScanFailed): it broke",
		},
		{
			name: "ImagePolicy failing on a live Site",
			mut: func(o *Observation) {
				failing(&o.ImagePolicy, 11*time.Minute, "PolicyFailed")
			},
			wantTarget:  "v1.0.2",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseLive,
			wantReason:  "ImagePolicy flux-system/fisioindoor-production: Ready=False (PolicyFailed): it broke",
		},
		{
			name: "a discovery signal while deploying stays deploying",
			mut: func(o *Observation) {
				behind(o)
				failing(&o.ImageRepository, 11*time.Minute, "ScanFailed")
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseDeploying,
			wantReason:  "ScanFailed",
		},
		{
			name: "a failed rollout and a discovery signal report both",
			mut: func(o *Observation) {
				behind(o)
				signal(&o.Deployment, Condition{Type: "Progressing", Status: "False", Reason: "ProgressDeadlineExceeded"})
				failing(&o.ImagePolicy, 11*time.Minute, "PolicyFailed")
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseFailed,
			wantReason:  "Deployment fisioindoor/fisioindoor-sws: Progressing=False (ProgressDeadlineExceeded); ImagePolicy flux-system/fisioindoor-production: Ready=False (PolicyFailed): it broke",
		},

		// What the service can't determine.
		{
			name: "ImagePolicy has no resolved tag",
			mut: func(o *Observation) {
				o.Target = ""
			},
			wantRunning: "v1.0.2",
			wantPhase:   PhaseDeploying,
			wantReason:  "ImagePolicy flux-system/fisioindoor-production has no resolved tag",
		},
		{
			name: "ImagePolicy does not exist",
			mut: func(o *Observation) {
				o.Target = ""
				o.ImagePolicy.Found = false
			},
			wantRunning: "v1.0.2",
			wantPhase:   PhaseDeploying,
			wantReason:  "ImagePolicy flux-system/fisioindoor-production not found",
		},
		{
			name: "Deployment does not exist",
			mut: func(o *Observation) {
				o.Deployment.Found = false
				o.ReplicaSets = nil
			},
			wantTarget: "v1.0.2",
			wantPhase:  PhaseDeploying,
			wantReason: "Deployment fisioindoor/fisioindoor-sws not found",
		},
		{
			name: "a missing object is only a reason on a Site on target",
			mut: func(o *Observation) {
				o.HelmRelease.Found = false
			},
			wantTarget:  "v1.0.2",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseLive,
			wantReason:  "HelmRelease fisioindoor/fisioindoor not found",
		},
		{
			name: "a rollout signal on a Site on target with a missing object is not failed",
			mut: func(o *Observation) {
				o.HelmRelease.Found = false
				signal(&o.Deployment, Condition{Type: "Progressing", Status: "False", Reason: "ProgressDeadlineExceeded"})
			},
			wantTarget:  "v1.0.2",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseLive,
			wantReason:  "HelmRelease fisioindoor/fisioindoor not found",
		},
		{
			name: "no objects at all",
			mut: func(o *Observation) {
				*o = Observation{
					Deployment:            Object{Kind: "Deployment", Namespace: "n", Name: "d"},
					HelmRelease:           Object{Kind: "HelmRelease", Namespace: "n", Name: "h"},
					ImageUpdateAutomation: Object{Kind: "ImageUpdateAutomation", Namespace: "flux-system", Name: "i"},
					Kustomization:         Object{Kind: "Kustomization", Namespace: "flux-system", Name: "k"},
					ImageRepository:       Object{Kind: "ImageRepository", Namespace: "flux-system", Name: "r"},
					ImagePolicy:           Object{Kind: "ImagePolicy", Namespace: "flux-system", Name: "p"},
				}
			},
			wantPhase:  PhaseDeploying,
			wantReason: "Deployment n/d not found; HelmRelease n/h not found; ImageUpdateAutomation flux-system/i not found; Kustomization flux-system/k not found; ImageRepository flux-system/r not found; ImagePolicy flux-system/p not found",
		},
		{
			name: "null versions are never equal",
			mut: func(o *Observation) {
				o.Target = ""
				o.ReplicaSets = nil
			},
			wantPhase:  PhaseDeploying,
			wantReason: "no resolved tag",
		},
		{
			name: "both versions null and a rollout signal is failed",
			mut: func(o *Observation) {
				o.Target = ""
				o.ReplicaSets = nil
				signal(&o.HelmRelease, Condition{Type: "Released", Status: "False", Reason: "UpgradeFailed"})
			},
			wantPhase:  PhaseFailed,
			wantReason: "HelmRelease fisioindoor/fisioindoor: Released=False (UpgradeFailed); ImagePolicy flux-system/fisioindoor-production has no resolved tag",
		},
		{
			name: "no ReplicaSet has available replicas, as during a pod restart",
			mut: func(o *Observation) {
				o.ReplicaSets[0].AvailableReplicas = 0
			},
			wantTarget: "v1.0.2",
			wantPhase:  PhaseDeploying,
		},
		{
			name: "nothing running and a rollout signal is failed",
			mut: func(o *Observation) {
				o.ReplicaSets = nil
				signal(&o.Deployment, Condition{Type: "Progressing", Status: "False", Reason: "ProgressDeadlineExceeded"})
			},
			wantTarget: "v1.0.2",
			wantPhase:  PhaseFailed,
			wantReason: "ProgressDeadlineExceeded",
		},
		{
			name: "the running image has no tag",
			mut: func(o *Observation) {
				o.ReplicaSets[0].Image = "ghcr.io/rmypg/fisioindoor@sha256:abc"
			},
			wantTarget: "v1.0.2",
			wantPhase:  PhaseDeploying,
			wantReason: "has no tag",
		},

		// Which ReplicaSet counts as running.
		{
			name: "a newer ReplicaSet without available replicas is not running yet",
			mut: func(o *Observation) {
				behind(o)
				o.ReplicaSets = append(o.ReplicaSets, ReplicaSet{Name: "new", Image: imageNew, CreatedAt: now.Add(-time.Minute), Revision: 3, AvailableReplicas: 0})
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseDeploying,
		},
		{
			name: "the newest ReplicaSet with available replicas wins over an older one",
			mut: func(o *Observation) {
				behind(o)
				o.ReplicaSets = append(o.ReplicaSets, ReplicaSet{Name: "new", Image: imageNew, CreatedAt: now.Add(-time.Minute), Revision: 3, AvailableReplicas: 1})
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.3",
			wantPhase:   PhaseLive,
		},
		{
			name: "same creation second: the higher revision is newest",
			mut: func(o *Observation) {
				behind(o)
				o.ReplicaSets = []ReplicaSet{
					{Name: "a", Image: imageNew, CreatedAt: now.Add(-time.Hour), Revision: 3, AvailableReplicas: 1},
					{Name: "b", Image: imageOld, CreatedAt: now.Add(-time.Hour), Revision: 2, AvailableReplicas: 1},
				}
			},
			wantTarget:  "v1.0.3",
			wantRunning: "v1.0.3",
			wantPhase:   PhaseLive,
		},
		{
			name: "a tag with a digest still parses",
			mut: func(o *Observation) {
				o.ReplicaSets[0].Image = imageOld + "@sha256:0a1b2c"
			},
			wantTarget:  "v1.0.2",
			wantRunning: "v1.0.2",
			wantPhase:   PhaseLive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := site()
			if tt.mut != nil {
				tt.mut(&o)
			}
			got := Evaluate(o, now)
			assertString(t, "target", got.Target, tt.wantTarget)
			assertString(t, "running", got.Running, tt.wantRunning)
			if got.Phase != tt.wantPhase {
				t.Errorf("phase = %q, want %q", got.Phase, tt.wantPhase)
			}
			switch {
			case tt.wantReason == "" && got.Reason != nil:
				t.Errorf("reason = %q, want null", *got.Reason)
			case tt.wantReason != "" && got.Reason == nil:
				t.Errorf("reason = null, want it to contain %q", tt.wantReason)
			case tt.wantReason != "" && !strings.Contains(*got.Reason, tt.wantReason):
				t.Errorf("reason = %q, want it to contain %q", *got.Reason, tt.wantReason)
			}
		})
	}
}

func TestGracePeriod(t *testing.T) {
	objects := []struct {
		name string
		get  func(*Observation) *Object
		// A rollout signal fails a Site that is behind. A discovery signal leaves
		// the phase alone.
		rollout bool
	}{
		{"ImageUpdateAutomation", func(o *Observation) *Object { return &o.ImageUpdateAutomation }, true},
		{"Kustomization", func(o *Observation) *Object { return &o.Kustomization }, true},
		{"ImageRepository", func(o *Observation) *Object { return &o.ImageRepository }, false},
		{"ImagePolicy", func(o *Observation) *Object { return &o.ImagePolicy }, false},
	}
	// Literal durations, so that changing Grace fails this test.
	ages := []struct {
		name  string
		age   time.Duration
		count bool
	}{
		{"9m", 9 * time.Minute, false},
		{"just under 10m", 10*time.Minute - time.Second, false},
		{"exactly 10m", 10 * time.Minute, false},
		{"just over 10m", 10*time.Minute + time.Second, true},
		{"10m30s", 10*time.Minute + 30*time.Second, true},
	}

	for _, obj := range objects {
		for _, age := range ages {
			t.Run(obj.name+"/"+age.name, func(t *testing.T) {
				o := site()
				behind(&o)
				failing(obj.get(&o), age.age, "Broken")

				got := Evaluate(o, now)

				wantPhase := PhaseDeploying
				if age.count && obj.rollout {
					wantPhase = PhaseFailed
				}
				if got.Phase != wantPhase {
					t.Errorf("phase = %q, want %q", got.Phase, wantPhase)
				}
				if age.count && got.Reason == nil {
					t.Error("reason = null, want it set")
				}
				if !age.count && got.Reason != nil {
					t.Errorf("reason = %q, want null", *got.Reason)
				}
			})
		}
	}
}

func TestGracePeriodMeasuresFromTheStartOfTheEpisode(t *testing.T) {
	o := site()
	behind(&o)
	failing(&o.Kustomization, 5*time.Minute, "Broken")

	if got := Evaluate(o, now); got.Phase != PhaseDeploying {
		t.Fatalf("at 5 minutes: phase = %q, want deploying", got.Phase)
	}
	if got := Evaluate(o, now.Add(6*time.Minute)); got.Phase != PhaseFailed {
		t.Fatalf("at 11 minutes: phase = %q, want failed", got.Phase)
	}
}

// ready is one reading of a Ready condition, with the fresh lastTransitionTime
// Flux gives it on each change.
func ready(status string, at time.Duration) *Condition {
	return &Condition{Type: "Ready", Status: status, Reason: "Attempt" + status, Message: "attempt", LastTransitionTime: now.Add(at)}
}

func TestEpisodeAcrossAttempts(t *testing.T) {
	// A Kustomization whose every attempt flips Ready, as kustomize-controller
	// does: each reading carries a lastTransitionTime no older than itself.
	steps := []struct {
		at         time.Duration
		status     string
		wantPhase  Phase
		wantReason string
	}{
		{0, "False", PhaseDeploying, ""},
		{time.Minute, "Unknown", PhaseDeploying, ""},
		{2 * time.Minute, "False", PhaseDeploying, ""},
		{3 * time.Minute, "Unknown", PhaseDeploying, ""},
		{9 * time.Minute, "False", PhaseDeploying, ""},
		{10 * time.Minute, "Unknown", PhaseDeploying, ""},
		{10*time.Minute + 30*time.Second, "False", PhaseFailed, "Kustomization flux-system/fisioindoor: Ready=False (AttemptFalse): attempt"},
		// Between attempts the current value is Unknown: the phase must not flip.
		{11 * time.Minute, "Unknown", PhaseFailed, "Kustomization flux-system/fisioindoor: Ready=False (AttemptFalse)"},
		{12 * time.Minute, "False", PhaseFailed, "AttemptFalse"},
		// True ends the episode.
		{13 * time.Minute, "True", PhaseDeploying, ""},
		// The next failure starts a new wait.
		{14 * time.Minute, "False", PhaseDeploying, ""},
		{20 * time.Minute, "False", PhaseDeploying, ""},
		{25 * time.Minute, "False", PhaseFailed, "AttemptFalse"},
	}

	var episode *Episode
	for _, step := range steps {
		at := now.Add(step.at)
		episode = NextEpisode(episode, ready(step.status, step.at), at)

		o := site()
		behind(&o)
		o.Kustomization.NotReady = episode

		got := Evaluate(o, at)
		if got.Phase != step.wantPhase {
			t.Errorf("at %s (%s): phase = %q, want %q", step.at, step.status, got.Phase, step.wantPhase)
		}
		switch {
		case step.wantReason == "" && got.Reason != nil:
			t.Errorf("at %s (%s): reason = %q, want null", step.at, step.status, *got.Reason)
		case step.wantReason != "" && (got.Reason == nil || !strings.Contains(*got.Reason, step.wantReason)):
			t.Errorf("at %s (%s): reason = %v, want it to contain %q", step.at, step.status, got.Reason, step.wantReason)
		}
	}
}

func TestEpisodeOnADiscoveryObjectLeavesThePhaseAlone(t *testing.T) {
	var episode *Episode
	episode = NextEpisode(episode, ready("False", 0), now)
	episode = NextEpisode(episode, ready("Unknown", 5*time.Minute), now.Add(5*time.Minute))
	episode = NextEpisode(episode, ready("False", 11*time.Minute), now.Add(11*time.Minute))

	o := site()
	o.ImageRepository.NotReady = episode
	got := Evaluate(o, now.Add(11*time.Minute))

	if got.Phase != PhaseLive {
		t.Errorf("phase = %q, want live", got.Phase)
	}
	if got.Reason == nil || !strings.Contains(*got.Reason, "ImageRepository") {
		t.Errorf("reason = %v, want the scan failure", got.Reason)
	}
}

func TestEpisodeOnlyEverUnknownIsNotAFailure(t *testing.T) {
	var episode *Episode
	for _, at := range []time.Duration{0, 5 * time.Minute, 11 * time.Minute, 30 * time.Minute} {
		episode = NextEpisode(episode, ready("Unknown", at), now.Add(at))
	}

	o := site()
	behind(&o)
	o.Kustomization.NotReady = episode
	got := Evaluate(o, now.Add(30*time.Minute))

	if got.Phase != PhaseDeploying || got.Reason != nil {
		t.Errorf("got phase %q reason %v, want deploying and null", got.Phase, got.Reason)
	}
}

func TestEpisodeCountsAtOnceWhenFalseFollowsALongUnknown(t *testing.T) {
	var episode *Episode
	episode = NextEpisode(episode, ready("Unknown", 0), now)
	episode = NextEpisode(episode, ready("False", 11*time.Minute), now.Add(11*time.Minute))

	o := site()
	behind(&o)
	o.Kustomization.NotReady = episode
	if got := Evaluate(o, now.Add(11*time.Minute)); got.Phase != PhaseFailed {
		t.Errorf("phase = %q, want failed", got.Phase)
	}
}

func TestEpisodeSeeding(t *testing.T) {
	tests := []struct {
		name        string
		current     *Episode
		ready       *Condition
		wantSince   time.Time
		wantFailure bool
	}{
		{
			name:        "no episode known: False seeds from lastTransitionTime, as after a restart",
			ready:       ready("False", -20*time.Minute),
			wantSince:   now.Add(-20 * time.Minute),
			wantFailure: true,
		},
		{
			name:      "no episode known: Unknown seeds from lastTransitionTime, no failure yet",
			ready:     ready("Unknown", -time.Minute),
			wantSince: now.Add(-time.Minute),
		},
		{
			name:        "no lastTransitionTime: starts at the observation",
			ready:       &Condition{Type: "Ready", Status: "False"},
			wantSince:   now,
			wantFailure: true,
		},
		{
			name:      "no Ready condition yet: starts at the observation",
			ready:     nil,
			wantSince: now,
		},
		{
			name:        "an existing episode keeps its start",
			current:     &Episode{Since: now.Add(-time.Hour)},
			ready:       ready("False", 0),
			wantSince:   now.Add(-time.Hour),
			wantFailure: true,
		},
		{
			name:        "Unknown keeps a failure already seen",
			current:     &Episode{Since: now.Add(-time.Hour), Failure: ready("False", -time.Minute)},
			ready:       ready("Unknown", 0),
			wantSince:   now.Add(-time.Hour),
			wantFailure: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NextEpisode(tt.current, tt.ready, now)
			if got == nil {
				t.Fatal("NextEpisode = nil, want an episode")
			}
			if !got.Since.Equal(tt.wantSince) {
				t.Errorf("Since = %v, want %v", got.Since, tt.wantSince)
			}
			if (got.Failure != nil) != tt.wantFailure {
				t.Errorf("Failure = %v, want set: %v", got.Failure, tt.wantFailure)
			}
		})
	}
}

func TestEpisodeRestartRestartsTheWait(t *testing.T) {
	// Before the restart the failure had lasted 30 minutes, but Flux had just
	// rewritten lastTransitionTime, so the new process starts waiting again.
	episode := NextEpisode(nil, ready("False", -time.Minute), now)

	o := site()
	behind(&o)
	o.Kustomization.NotReady = episode
	if got := Evaluate(o, now); got.Phase != PhaseDeploying {
		t.Errorf("right after the restart: phase = %q, want deploying", got.Phase)
	}
	if got := Evaluate(o, now.Add(10*time.Minute)); got.Phase != PhaseFailed {
		t.Errorf("11 minutes after the seed: phase = %q, want failed", got.Phase)
	}
}

func TestReadyTrueEndsTheEpisode(t *testing.T) {
	if got := NextEpisode(&Episode{Since: now, Failure: ready("False", 0)}, ready("True", 0), now); got != nil {
		t.Errorf("NextEpisode = %+v, want nil", got)
	}
	if got := NextEpisode(nil, ready("True", 0), now); got != nil {
		t.Errorf("NextEpisode = %+v, want nil", got)
	}
}

func TestRolloutSignalIgnoredWhenRunningEqualsTarget(t *testing.T) {
	signals := []struct {
		name string
		mut  func(*Observation)
	}{
		{"Deployment", func(o *Observation) {
			signal(&o.Deployment, Condition{Type: "Progressing", Status: "False", Reason: "ProgressDeadlineExceeded"})
		}},
		{"HelmRelease", func(o *Observation) {
			signal(&o.HelmRelease, Condition{Type: "Released", Status: "False", Reason: "UpgradeFailed"})
		}},
		{"ImageUpdateAutomation", func(o *Observation) {
			failing(&o.ImageUpdateAutomation, time.Hour, "PushFailed")
		}},
		{"Kustomization", func(o *Observation) {
			failing(&o.Kustomization, time.Hour, "HealthCheckFailed")
		}},
	}
	for _, s := range signals {
		t.Run(s.name, func(t *testing.T) {
			o := site()
			s.mut(&o)
			got := Evaluate(o, now)
			if got.Phase != PhaseLive {
				t.Errorf("phase = %q, want live", got.Phase)
			}
			if got.Reason != nil {
				t.Errorf("reason = %q, want null", *got.Reason)
			}
		})
	}
}

func TestStatusEncodesNulls(t *testing.T) {
	data, err := json.Marshal(Evaluate(Observation{}, now))
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"target", "running"} {
		value, present := decoded[key]
		if !present || value != nil {
			t.Errorf("%s = %v (present %v), want an explicit null", key, value, present)
		}
	}
}

func TestImageTag(t *testing.T) {
	tests := []struct{ image, want string }{
		{"ghcr.io/rmypg/fisioindoor:v1.0.3", "v1.0.3"},
		{"ghcr.io/rmypg/fisioindoor:v1.0.3@sha256:0a1b2c", "v1.0.3"},
		{"ghcr.io/rmypg/fisioindoor@sha256:0a1b2c", ""},
		{"registry.local:5000/site:v2", "v2"},
		{"registry.local:5000/site", ""},
		{"registry.local:5000/site:v2@sha256:0a1b", "v2"},
		{"nginx", ""},
		{"nginx:1.27", "1.27"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := ImageTag(tt.image); got != tt.want {
			t.Errorf("ImageTag(%q) = %q, want %q", tt.image, got, tt.want)
		}
	}
}

func TestHistory(t *testing.T) {
	day := func(d int) time.Time { return time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC) }

	tests := []struct {
		name string
		in   []ReplicaSet
		want []Entry
	}{
		{name: "no ReplicaSets", in: nil, want: []Entry{}},
		{
			name: "newest first",
			in: []ReplicaSet{
				{Name: "a", Image: "x:v1", CreatedAt: day(1), Revision: 1},
				{Name: "c", Image: "x:v3", CreatedAt: day(3), Revision: 3},
				{Name: "b", Image: "x:v2", CreatedAt: day(2), Revision: 2},
			},
			want: []Entry{{Tag: ptr("v3"), CreatedAt: day(3)}, {Tag: ptr("v2"), CreatedAt: day(2)}, {Tag: ptr("v1"), CreatedAt: day(1)}},
		},
		{
			name: "one entry per run of the same image, timed at the run's oldest ReplicaSet",
			in: []ReplicaSet{
				{Name: "a", Image: "x:v1", CreatedAt: day(1), Revision: 1},
				{Name: "b", Image: "x:v1", CreatedAt: day(2), Revision: 2},
				{Name: "c", Image: "x:v1", CreatedAt: day(3), Revision: 3},
				{Name: "d", Image: "x:v2", CreatedAt: day(4), Revision: 4},
			},
			want: []Entry{{Tag: ptr("v2"), CreatedAt: day(4)}, {Tag: ptr("v1"), CreatedAt: day(1)}},
		},
		{
			name: "an image that comes back is a new run",
			in: []ReplicaSet{
				{Name: "a", Image: "x:v1", CreatedAt: day(1), Revision: 1},
				{Name: "b", Image: "x:v2", CreatedAt: day(2), Revision: 2},
				{Name: "c", Image: "x:v1", CreatedAt: day(3), Revision: 3},
			},
			want: []Entry{{Tag: ptr("v1"), CreatedAt: day(3)}, {Tag: ptr("v2"), CreatedAt: day(2)}, {Tag: ptr("v1"), CreatedAt: day(1)}},
		},
		{
			name: "an image without a tag has a null tag",
			in: []ReplicaSet{
				{Name: "a", Image: "x@sha256:0a1b", CreatedAt: day(1), Revision: 1},
			},
			want: []Entry{{Tag: nil, CreatedAt: day(1)}},
		},
		{
			name: "tags with digests",
			in: []ReplicaSet{
				{Name: "a", Image: "x:v1@sha256:0a", CreatedAt: day(1), Revision: 1},
			},
			want: []Entry{{Tag: ptr("v1"), CreatedAt: day(1)}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := History(tt.in)
			if got == nil {
				t.Fatal("History returned nil, want an empty list")
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d entries %v, want %d", len(got), got, len(tt.want))
			}
			for i := range tt.want {
				assertPtr(t, "tag", got[i].Tag, tt.want[i].Tag)
				if !got[i].CreatedAt.Equal(tt.want[i].CreatedAt) {
					t.Errorf("entry %d createdAt = %v, want %v", i, got[i].CreatedAt, tt.want[i].CreatedAt)
				}
			}
		})
	}
}

func ptr(s string) *string { return &s }

func assertString(t *testing.T, name string, got *string, want string) {
	t.Helper()
	if want == "" {
		if got != nil {
			t.Errorf("%s = %q, want null", name, *got)
		}
		return
	}
	if got == nil {
		t.Errorf("%s = null, want %q", name, want)
		return
	}
	if *got != want {
		t.Errorf("%s = %q, want %q", name, *got, want)
	}
}

func assertPtr(t *testing.T, name string, got, want *string) {
	t.Helper()
	if want == nil {
		assertString(t, name, got, "")
		return
	}
	assertString(t, name, got, *want)
}
