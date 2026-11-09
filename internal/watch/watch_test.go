package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RMYPG/site-status/internal/config"
	"github.com/RMYPG/site-status/internal/rollout"
	"github.com/RMYPG/site-status/internal/server"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	apiwatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

const testConfig = `{
	"imageUpdateAutomation": "image-tags",
	"sites": [
		{"id": "fisioindoor", "environments": {"production": {
			"namespace": "fisioindoor", "deployment": "fisioindoor-sws", "helmRelease": "fisioindoor",
			"imagePolicy": "fisioindoor-production", "imageRepository": "fisioindoor", "kustomization": "fisioindoor"
		}}},
		{"id": "vertex-glow", "environments": {"production": {
			"namespace": "vertex-glow", "deployment": "vertex-glow-sws", "helmRelease": "vertex-glow",
			"imagePolicy": "vertex-glow-production", "imageRepository": "vertex-glow", "kustomization": "vertex-glow"
		}}}
	]
}`

var discardLogger = slog.New(slog.NewJSONHandler(io.Discard, nil))

var listKinds = map[schema.GroupVersionResource]string{
	deployments:            "DeploymentList",
	replicaSets:            "ReplicaSetList",
	helmReleases:           "HelmReleaseList",
	imageRepositories:      "ImageRepositoryList",
	imagePolicies:          "ImagePolicyList",
	imageUpdateAutomations: "ImageUpdateAutomationList",
	kustomizations:         "KustomizationList",
}

func parseConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(testConfig))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newClient(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objects...)
}

func object(apiVersion, kind, namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   map[string]any{"name": name, "namespace": namespace},
	}}
}

func withConditions(u *unstructured.Unstructured, conditions ...map[string]any) *unstructured.Unstructured {
	list := make([]any, len(conditions))
	for i, condition := range conditions {
		list[i] = condition
	}
	if err := unstructured.SetNestedSlice(u.Object, list, "status", "conditions"); err != nil {
		panic(err)
	}
	return u
}

func replicaSet(namespace, name, image, ownerUID string, created time.Time, available int64) *unstructured.Unstructured {
	rs := object("apps/v1", "ReplicaSet", namespace, name)
	rs.SetCreationTimestamp(metav1.NewTime(created))
	rs.SetAnnotations(map[string]string{revisionAnnotation: "2"})
	controller := true
	rs.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "owner", UID: types.UID(ownerUID), Controller: &controller}})
	if err := unstructured.SetNestedSlice(rs.Object, []any{map[string]any{"name": "sws", "image": image}}, "spec", "template", "spec", "containers"); err != nil {
		panic(err)
	}
	if err := unstructured.SetNestedField(rs.Object, available, "status", "availableReplicas"); err != nil {
		panic(err)
	}
	return rs
}

// fisioindoorObjects is one Site's objects, deploying from v1.0.2 to v1.0.3.
func fisioindoorObjects(created time.Time) []runtime.Object {
	deployment := withConditions(object("apps/v1", "Deployment", "fisioindoor", "fisioindoor-sws"),
		map[string]any{"type": "Progressing", "status": "True", "reason": "ReplicaSetUpdated"})
	deployment.SetUID("deployment-uid")

	release := withConditions(object("helm.toolkit.fluxcd.io/v2", "HelmRelease", "fisioindoor", "fisioindoor"),
		map[string]any{"type": "Released", "status": "True", "reason": "UpgradeSucceeded"})
	automation := withConditions(object("image.toolkit.fluxcd.io/v1", "ImageUpdateAutomation", FluxNamespace, "image-tags"),
		map[string]any{"type": "Ready", "status": "True", "reason": "Succeeded"})
	kustomization := withConditions(object("kustomize.toolkit.fluxcd.io/v1", "Kustomization", FluxNamespace, "fisioindoor"),
		map[string]any{"type": "Ready", "status": "True", "reason": "ReconciliationSucceeded"})
	imageRepository := withConditions(object("image.toolkit.fluxcd.io/v1", "ImageRepository", FluxNamespace, "fisioindoor"),
		map[string]any{"type": "Ready", "status": "True", "reason": "Succeeded"})
	imagePolicy := withConditions(object("image.toolkit.fluxcd.io/v1", "ImagePolicy", FluxNamespace, "fisioindoor-production"),
		map[string]any{"type": "Ready", "status": "True", "reason": "Succeeded", "lastTransitionTime": created.Format(time.RFC3339)})
	if err := unstructured.SetNestedField(imagePolicy.Object, "v1.0.3", "status", "latestRef", "tag"); err != nil {
		panic(err)
	}

	return []runtime.Object{
		deployment, release, automation, kustomization, imageRepository, imagePolicy,
		replicaSet("fisioindoor", "fisioindoor-sws-old", "ghcr.io/rmypg/fisioindoor:v1.0.2@sha256:abc", "deployment-uid", created.Add(-time.Hour), 1),
		replicaSet("fisioindoor", "fisioindoor-sws-new", "ghcr.io/rmypg/fisioindoor:v1.0.3", "deployment-uid", created, 0),
		// A different Deployment's newest, available ReplicaSet must not count.
		replicaSet("fisioindoor", "fisioindoor-sws-foreign", "ghcr.io/rmypg/foreign:v9.9.9", "other-uid", created.Add(time.Hour), 1),
	}
}

func startSynced(t *testing.T, client *dynamicfake.FakeDynamicClient, cfg *config.Config) *Watcher {
	t.Helper()
	watcher := New(client, cfg, time.Now, discardLogger)
	watcher.Start(t.Context())
	waitFor(t, "informers to sync", watcher.Synced)
	return watcher
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type recorded struct {
	namespace, resource, verb string
}

func TestWatchesOnlyTheConfiguredNamespaces(t *testing.T) {
	client := newClient()
	startSynced(t, client, parseConfig(t))

	// HasSynced returns after the list; each reflector then opens its watch.
	var expected []recorded
	for _, namespace := range []string{"fisioindoor", "vertex-glow"} {
		for _, resource := range []string{"deployments", "replicasets", "helmreleases"} {
			for _, verb := range []string{"list", "watch"} {
				expected = append(expected, recorded{namespace, resource, verb})
			}
		}
	}
	for _, resource := range []string{"imagerepositories", "imagepolicies", "imageupdateautomations", "kustomizations"} {
		for _, verb := range []string{"list", "watch"} {
			expected = append(expected, recorded{FluxNamespace, resource, verb})
		}
	}

	actions := func() map[recorded]bool {
		seen := map[recorded]bool{}
		for _, action := range client.Actions() {
			seen[recorded{action.GetNamespace(), action.GetResource().Resource, action.GetVerb()}] = true
		}
		return seen
	}
	waitFor(t, "every list and watch", func() bool {
		seen := actions()
		for _, want := range expected {
			if !seen[want] {
				return false
			}
		}
		return true
	})

	allowed := map[recorded]bool{}
	for _, want := range expected {
		allowed[want] = true
	}
	verbs := map[string]bool{"get": true, "list": true, "watch": true}
	for _, action := range client.Actions() {
		if !verbs[action.GetVerb()] {
			t.Errorf("verb %q on %s is not read-only", action.GetVerb(), action.GetResource().Resource)
		}
		if action.GetNamespace() == "" {
			t.Errorf("cluster-scoped %s on %s", action.GetVerb(), action.GetResource().Resource)
		}
		got := recorded{action.GetNamespace(), action.GetResource().Resource, action.GetVerb()}
		if !allowed[got] {
			t.Errorf("unexpected action %+v, outside the RBAC this service is granted", got)
		}
	}
}

func TestFakeClientRecordsClusterScopedCalls(t *testing.T) {
	// Guards the test above: a watch without a namespace records as such.
	client := newClient()
	if _, err := client.Resource(deployments).List(t.Context(), metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	var action k8stesting.Action
	for _, candidate := range client.Actions() {
		action = candidate
	}
	if action == nil || action.GetNamespace() != "" {
		t.Fatalf("expected a recorded cluster-scoped list, got %v", action)
	}
}

func TestObserveReadsAnEnvironment(t *testing.T) {
	created := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	cfg := parseConfig(t)
	watcher := startSynced(t, newClient(fisioindoorObjects(created)...), cfg)
	env, _ := cfg.Find("fisioindoor", "production")

	observation := watcher.Observe(env)

	if observation.Target != "v1.0.3" {
		t.Errorf("Target = %q, want v1.0.3", observation.Target)
	}
	for _, object := range []rollout.Object{
		observation.Deployment, observation.HelmRelease, observation.ImageUpdateAutomation,
		observation.Kustomization, observation.ImageRepository, observation.ImagePolicy,
	} {
		if !object.Found {
			t.Errorf("%s %s not found", object.Kind, object.Name)
		}
	}
	if observation.ImagePolicy.Conditions[0].LastTransitionTime.IsZero() {
		t.Error("ImagePolicy condition has no lastTransitionTime")
	}
	if observation.Deployment.NotReady != nil || observation.HelmRelease.NotReady != nil {
		t.Error("an object that doesn't retry on its own has an episode")
	}

	if len(observation.ReplicaSets) != 2 {
		t.Fatalf("ReplicaSets = %+v, want the two owned ones", observation.ReplicaSets)
	}

	status := rollout.Evaluate(observation, created)
	if status.Target == nil || *status.Target != "v1.0.3" {
		t.Errorf("target = %v", status.Target)
	}
	if status.Running == nil || *status.Running != "v1.0.2" {
		t.Errorf("running = %v, want the tag of the owned, available ReplicaSet", status.Running)
	}
	if status.Phase != rollout.PhaseDeploying || status.Reason != nil {
		t.Errorf("status = %+v", status)
	}
}

func TestObserveDoesNotFailOnMissingObjects(t *testing.T) {
	cfg := parseConfig(t)
	watcher := startSynced(t, newClient(), cfg)
	env, _ := cfg.Find("vertex-glow", "production")

	observation := watcher.Observe(env)

	if observation.Deployment.Found || observation.ImagePolicy.Found || observation.Target != "" || len(observation.ReplicaSets) != 0 {
		t.Errorf("Observation = %+v, want nothing found", observation)
	}
	if status := rollout.Evaluate(observation, time.Now()); status.Phase == rollout.PhaseLive {
		t.Errorf("status = %+v, a Site with no objects must not be live", status)
	}
}

func TestDropManagedFields(t *testing.T) {
	u := object("apps/v1", "Deployment", "n", "d")
	u.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "kubectl"}})
	u.SetGeneration(3)

	transformed, err := dropManagedFields(u)
	if err != nil {
		t.Fatal(err)
	}
	result := transformed.(*unstructured.Unstructured)
	if len(result.GetManagedFields()) != 0 || result.GetGeneration() != 3 {
		t.Errorf("got %+v", result)
	}
}

func TestServerAgainstARealWatcher(t *testing.T) {
	created := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	cfg := parseConfig(t)
	watcher := New(newClient(fisioindoorObjects(created)...), cfg, time.Now, discardLogger)
	h := server.New(cfg, watcher, func() time.Time { return created }, discardLogger)

	get := func(path string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
		return recorder
	}

	if response := get("/readyz"); response.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz before Start = %d, want 503", response.Code)
	}

	watcher.Start(t.Context())
	waitFor(t, "/readyz", func() bool { return get("/readyz").Code == http.StatusOK })

	response := get("/v1/sites/fisioindoor/environments/production")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body)
	}
	var body rollout.Status
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Target == nil || *body.Target != "v1.0.3" || body.Running == nil || *body.Running != "v1.0.2" || body.Phase != rollout.PhaseDeploying || body.Reason != nil {
		t.Errorf("body = %+v", body)
	}
}

func TestObserveReadsTheFirstContainer(t *testing.T) {
	created := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	cfg := parseConfig(t)

	deployment := object("apps/v1", "Deployment", "fisioindoor", "fisioindoor-sws")
	deployment.SetUID("deployment-uid")
	rs := replicaSet("fisioindoor", "two-containers", "ghcr.io/rmypg/fisioindoor:v1.0.2", "deployment-uid", created, 1)
	if err := unstructured.SetNestedSlice(rs.Object, []any{
		map[string]any{"name": "sws", "image": "ghcr.io/rmypg/fisioindoor:v1.0.2"},
		map[string]any{"name": "sidecar", "image": "ghcr.io/rmypg/sidecar:v9.9.9"},
	}, "spec", "template", "spec", "containers"); err != nil {
		t.Fatal(err)
	}

	watcher := startSynced(t, newClient(deployment, rs), cfg)
	env, _ := cfg.Find("fisioindoor", "production")
	got := watcher.Observe(env).ReplicaSets

	if len(got) != 1 {
		t.Fatalf("ReplicaSets = %+v, want one", got)
	}
	if got[0].Image != "ghcr.io/rmypg/fisioindoor:v1.0.2" {
		t.Errorf("Image = %q, want the first container's", got[0].Image)
	}
}

// readyObjectAt is a retrying object with one reading of Ready, carrying the
// fresh lastTransitionTime Flux gives each change.
func readyObjectAt(apiVersion, kind, name, status, message string, transition time.Time) *unstructured.Unstructured {
	return withConditions(object(apiVersion, kind, FluxNamespace, name),
		map[string]any{"type": "Ready", "status": status, "reason": "Attempt" + status, "message": message, "lastTransitionTime": transition.Format(time.RFC3339)})
}

func withUID(u *unstructured.Unstructured, uid string) *unstructured.Unstructured {
	u.SetUID(types.UID(uid))
	return u
}

func kustomizationAt(status, message string, transition time.Time) *unstructured.Unstructured {
	return readyObjectAt("kustomize.toolkit.fluxcd.io/v1", "Kustomization", "fisioindoor", status, message, transition)
}

// TestEpisodeFollowsInformerEvents runs through the informers for each retrying
// resource, so removing any of them from retrying fails it.
func TestEpisodeFollowsInformerEvents(t *testing.T) {
	tests := []struct {
		kind, apiVersion, name string
		resource               schema.GroupVersionResource
		get                    func(rollout.Observation) rollout.Object
	}{
		{"ImageRepository", "image.toolkit.fluxcd.io/v1", "fisioindoor", imageRepositories, func(o rollout.Observation) rollout.Object { return o.ImageRepository }},
		{"ImagePolicy", "image.toolkit.fluxcd.io/v1", "fisioindoor-production", imagePolicies, func(o rollout.Observation) rollout.Object { return o.ImagePolicy }},
		{"ImageUpdateAutomation", "image.toolkit.fluxcd.io/v1", "image-tags", imageUpdateAutomations, func(o rollout.Observation) rollout.Object { return o.ImageUpdateAutomation }},
		{"Kustomization", "kustomize.toolkit.fluxcd.io/v1", "fisioindoor", kustomizations, func(o rollout.Observation) rollout.Object { return o.Kustomization }},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			start := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
			cfg := parseConfig(t)
			env, _ := cfg.Find("fisioindoor", "production")
			at := func(status, message string, offset time.Duration) *unstructured.Unstructured {
				return readyObjectAt(tt.apiVersion, tt.kind, tt.name, status, message, start.Add(offset))
			}
			client := newClient(at("True", "ready", -time.Hour))
			watcher := startSynced(t, client, cfg)

			update := func(status, message string, offset time.Duration) {
				t.Helper()
				if _, err := client.Resource(tt.resource).Namespace(FluxNamespace).Update(t.Context(), at(status, message, offset), metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			waitForFailure := func(message string) *rollout.Episode {
				t.Helper()
				var episode *rollout.Episode
				waitFor(t, "failure "+message, func() bool {
					episode = watcher.episode(tt.resource, FluxNamespace, tt.name)
					return episode != nil && episode.Failure != nil && episode.Failure.Message == message
				})
				return episode
			}

			if episode := tt.get(watcher.Observe(env)).NotReady; episode != nil {
				t.Fatalf("Ready=True has episode %+v", episode)
			}

			// Each change carries a newer lastTransitionTime, but the episode keeps
			// the first start: Unknown between attempts must not restart the wait.
			update("False", "attempt 1", 0)
			waitForFailure("attempt 1")
			update("Unknown", "in progress", time.Minute)
			update("False", "attempt 2", 11*time.Minute)
			if episode := waitForFailure("attempt 2"); !episode.Since.Equal(start) {
				t.Errorf("Since = %v, want %v", episode.Since, start)
			}
			if observed := tt.get(watcher.Observe(env)).NotReady; observed == nil || !observed.Since.Equal(start) {
				t.Errorf("Observe's episode = %+v, want the tracked one", observed)
			}

			update("True", "ready", 12*time.Minute)
			waitFor(t, "True to clear the episode", func() bool {
				return watcher.episode(tt.resource, FluxNamespace, tt.name) == nil
			})

			// A later failure starts a new episode.
			update("False", "attempt 3", 13*time.Minute)
			if episode := waitForFailure("attempt 3"); !episode.Since.Equal(start.Add(13 * time.Minute)) {
				t.Errorf("Since = %v, want a new start", episode.Since)
			}

			if err := client.Resource(tt.resource).Namespace(FluxNamespace).Delete(t.Context(), tt.name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "deletion to forget the episode", func() bool {
				return watcher.episode(tt.resource, FluxNamespace, tt.name) == nil
			})
		})
	}
}

func TestRecreatedObjectStartsANewEpisode(t *testing.T) {
	start := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	cfg := parseConfig(t)
	env, _ := cfg.Find("fisioindoor", "production")

	original := kustomizationAt("False", "old", start.Add(-time.Hour))
	original.SetUID("old-uid")
	client := newClient(original)
	watcher := startSynced(t, client, cfg)

	// Deleted and recreated while the watch was disconnected: the relist then
	// delivers the replacement as an update to the same name.
	replacement := kustomizationAt("False", "new", start)
	replacement.SetUID("new-uid")
	if _, err := client.Resource(kustomizations).Namespace(FluxNamespace).Update(t.Context(), replacement, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	var episode *rollout.Episode
	waitFor(t, "the handler to see the replacement", func() bool {
		episode = watcher.episode(kustomizations, FluxNamespace, "fisioindoor")
		return episode != nil && episode.Failure != nil && episode.Failure.Message == "new"
	})
	if episode.UID != "new-uid" || !episode.Since.Equal(start) {
		t.Errorf("episode = %+v, the replacement inherited the old timer", episode)
	}
	if observed := watcher.Observe(env).Kustomization.NotReady; observed == nil || observed.UID != "new-uid" {
		t.Errorf("Observe's episode = %+v, want the replacement's", observed)
	}
}

// startWithPausableHandler starts a synced watcher and returns a function that
// pauses its event handler, so that the informer cache moves on without it. It
// returns a function that resumes the handler.
func startWithPausableHandler(t *testing.T, client *dynamicfake.FakeDynamicClient, cfg *config.Config) (*Watcher, func() func()) {
	t.Helper()
	watcher := New(client, cfg, time.Now, discardLogger)
	var gate atomic.Pointer[chan struct{}]
	track := watcher.track
	watcher.track = func(resource schema.GroupVersionResource, object any) {
		if wait := gate.Load(); wait != nil {
			<-*wait
		}
		track(resource, object)
	}
	watcher.Start(t.Context())
	waitFor(t, "informers to sync", watcher.Synced)

	return watcher, func() func() {
		wait := make(chan struct{})
		gate.Store(&wait)
		resume := sync.OnceFunc(func() { close(wait) })
		t.Cleanup(resume)
		return resume
	}
}

func TestCachedObjectIsNotJudgedByALaggingEpisode(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		// The object the handler has seen, and the one only the cache has.
		before, after *unstructured.Unstructured
		beforePhase   rollout.Phase
		wantEpisode   bool
	}{
		{
			name:        "a replacement doesn't inherit the old timer",
			before:      withUID(kustomizationAt("False", "old", now.Add(-time.Hour)), "old-uid"),
			after:       withUID(kustomizationAt("False", "new", now), "new-uid"),
			beforePhase: rollout.PhaseFailed,
			wantEpisode: true,
		},
		{
			name:        "Ready=True ignores an episode not yet cleared",
			before:      withUID(kustomizationAt("False", "old", now.Add(-time.Hour)), "same-uid"),
			after:       withUID(kustomizationAt("True", "new", now), "same-uid"),
			beforePhase: rollout.PhaseFailed,
		},
		{
			name:        "just turned False, no episode yet",
			before:      withUID(kustomizationAt("True", "old", now.Add(-time.Hour)), "same-uid"),
			after:       withUID(kustomizationAt("False", "new", now), "same-uid"),
			beforePhase: rollout.PhaseDeploying,
			wantEpisode: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseConfig(t)
			env, _ := cfg.Find("fisioindoor", "production")
			client := newClient(tt.before)
			watcher, pause := startWithPausableHandler(t, client, cfg)

			if got := rollout.Evaluate(watcher.Observe(env), now); got.Phase != tt.beforePhase {
				t.Fatalf("before the change: phase = %q, want %q", got.Phase, tt.beforePhase)
			}
			tracked := watcher.episode(kustomizations, FluxNamespace, "fisioindoor")
			resume := pause()

			if _, err := client.Resource(kustomizations).Namespace(FluxNamespace).Update(t.Context(), tt.after, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "the change to reach the cache", func() bool {
				list := watcher.Observe(env).Kustomization.Conditions
				return len(list) == 1 && list[0].Message == "new"
			})
			if got := watcher.episode(kustomizations, FluxNamespace, "fisioindoor"); got != tracked {
				t.Fatalf("tracked episode = %+v, the handler was meant to be paused", got)
			}

			observation := watcher.Observe(env)
			if (observation.Kustomization.NotReady != nil) != tt.wantEpisode {
				t.Errorf("episode = %+v, want one: %v", observation.Kustomization.NotReady, tt.wantEpisode)
			}
			got := rollout.Evaluate(observation, now)
			if got.Phase != rollout.PhaseDeploying {
				t.Errorf("phase = %q, want deploying", got.Phase)
			}
			if got.Reason != nil && strings.Contains(*got.Reason, "Ready=False") {
				t.Errorf("reason = %q, judged by the earlier version's episode", *got.Reason)
			}

			resume()
			waitFor(t, "the handler to catch up", func() bool {
				return watcher.episode(kustomizations, FluxNamespace, "fisioindoor") != tracked
			})
			if got := rollout.Evaluate(watcher.Observe(env), now); got.Phase != rollout.PhaseDeploying {
				t.Errorf("after the handler caught up: phase = %q, want deploying", got.Phase)
			}
		})
	}
}

func TestEpisodeSeededFromTheClusterAfterARestart(t *testing.T) {
	start := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	cfg := parseConfig(t)
	env, _ := cfg.Find("fisioindoor", "production")

	// A new process finds the object already failing.
	watcher := startSynced(t, newClient(kustomizationAt("False", "broken", start.Add(-20*time.Minute))), cfg)

	episode := watcher.Observe(env).Kustomization.NotReady
	if episode == nil || !episode.Since.Equal(start.Add(-20*time.Minute)) {
		t.Fatalf("episode = %+v, want one seeded from lastTransitionTime", episode)
	}
	if episode.Failure == nil {
		t.Error("the seeded episode lost its Ready=False")
	}
}

type testClock struct{ nanos atomic.Int64 }

func newTestClock(at time.Time) *testClock {
	clock := &testClock{}
	clock.nanos.Store(at.UnixNano())
	return clock
}

func (c *testClock) now() time.Time { return time.Unix(0, c.nanos.Load()) }

func (c *testClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

func TestWatchFailingWindow(t *testing.T) {
	clock := newTestClock(time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC))
	watcher := New(newClient(), parseConfig(t), clock.now, discardLogger)

	if watcher.WatchFailing() {
		t.Fatal("WatchFailing before any error")
	}

	watcher.recordWatchError(deployments, "fisioindoor", errors.New("forbidden"))
	if !watcher.WatchFailing() {
		t.Fatal("WatchFailing = false right after an error")
	}

	clock.advance(2*time.Minute - time.Second)
	if !watcher.WatchFailing() {
		t.Error("WatchFailing = false just under two minutes after the error")
	}
	clock.advance(time.Second)
	if watcher.WatchFailing() {
		t.Error("WatchFailing = true two minutes after the last error")
	}

	// An error that keeps occurring keeps it failing.
	watcher.recordWatchError(deployments, "fisioindoor", errors.New("forbidden"))
	if !watcher.WatchFailing() {
		t.Error("WatchFailing = false after a new error")
	}
}

func TestRoutineWatchClosesDoNotCount(t *testing.T) {
	watcher := New(newClient(), parseConfig(t), time.Now, discardLogger)

	for _, err := range []error{
		io.EOF,
		io.ErrUnexpectedEOF,
		apierrors.NewResourceExpired("too old"),
		&apierrors.StatusError{ErrStatus: metav1.Status{Code: http.StatusGone, Reason: metav1.StatusReasonGone}},
	} {
		watcher.recordWatchError(deployments, "fisioindoor", err)
		if watcher.WatchFailing() {
			t.Errorf("%v counted as a watch failure", err)
		}
	}
}

// blockingListClient holds one namespace's list of one resource until release
// closes. A reactor can't do this: the fake runs reactors under its lock, so a
// blocked one would stop every other list as well.
type blockingListClient struct {
	dynamic.Interface
	resource  schema.GroupVersionResource
	namespace string
	release   <-chan struct{}
}

// IsWatchListSemanticsUnSupported keeps the informers on list and watch, as they
// are with the fake this wraps, which can't stream an initial list.
func (c *blockingListClient) IsWatchListSemanticsUnSupported() bool { return true }

func (c *blockingListClient) Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	inner := c.Interface.Resource(resource)
	if resource != c.resource {
		return inner
	}
	return &blockingResource{NamespaceableResourceInterface: inner, parent: c}
}

type blockingResource struct {
	dynamic.NamespaceableResourceInterface
	parent *blockingListClient
}

func (r *blockingResource) Namespace(namespace string) dynamic.ResourceInterface {
	inner := r.NamespaceableResourceInterface.Namespace(namespace)
	if namespace != r.parent.namespace {
		return inner
	}
	return &blockingList{ResourceInterface: inner, release: r.parent.release}
}

type blockingList struct {
	dynamic.ResourceInterface
	release <-chan struct{}
}

func (l *blockingList) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	select {
	case <-l.release:
		return l.ResourceInterface.List(ctx, opts)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func request(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
	return recorder
}

// assertAnswering checks /readyz and both API endpoints all answer status.
func assertAnswering(t *testing.T, h http.Handler, status int) {
	t.Helper()
	for _, path := range []string{
		"/readyz",
		"/v1/sites/fisioindoor/environments/production",
		"/v1/sites/fisioindoor/environments/production/rollouts",
	} {
		if response := request(t, h, path); response.Code != status {
			t.Errorf("%s = %d: %s, want %d", path, response.Code, response.Body, status)
		}
	}
}

func TestNotSyncedWhileOneNamespaceListIsBlocked(t *testing.T) {
	created := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	cfg := parseConfig(t)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	blocked := informerKey{"vertex-glow", replicaSets}
	client := &blockingListClient{Interface: newClient(fisioindoorObjects(created)...), resource: replicaSets, namespace: "vertex-glow", release: release}

	watcher := New(client, cfg, time.Now, discardLogger)
	h := server.New(cfg, watcher, func() time.Time { return created }, discardLogger)
	watcher.Start(t.Context())

	// Every other informer finishes, so only this list holds readiness back.
	waitFor(t, "every other informer", func() bool {
		for key, informer := range watcher.informers {
			if key != blocked && !informer.Informer().HasSynced() {
				return false
			}
		}
		return true
	})
	if watcher.Synced() {
		t.Fatal("Synced = true while a site namespace's list is blocked")
	}
	assertAnswering(t, h, http.StatusServiceUnavailable)

	unblock()
	waitFor(t, "the blocked list", watcher.Synced)
	assertAnswering(t, h, http.StatusOK)
}

func TestNotSyncedWhileEventHandlersCatchUp(t *testing.T) {
	created := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	cfg := parseConfig(t)
	env, _ := cfg.Find("fisioindoor", "production")
	watcher := New(newClient(kustomizationAt("False", "broken", created)), cfg, time.Now, discardLogger)
	h := server.New(cfg, watcher, func() time.Time { return created }, discardLogger)

	// The event handler takes this lock for each event, so holding it keeps the
	// handler from finishing the initial list, as a slow handler would.
	watcher.episodesMu.Lock()
	unlock := sync.OnceFunc(watcher.episodesMu.Unlock)
	t.Cleanup(unlock)
	watcher.Start(t.Context())

	waitFor(t, "every informer", func() bool {
		for _, informer := range watcher.informers {
			if !informer.Informer().HasSynced() {
				return false
			}
		}
		return true
	})
	if watcher.Synced() {
		t.Fatal("Synced = true before the event handler saw the initial list")
	}
	assertAnswering(t, h, http.StatusServiceUnavailable)

	unlock()
	waitFor(t, "the event handler", watcher.Synced)
	if episode := watcher.Observe(env).Kustomization.NotReady; episode == nil || !episode.Since.Equal(created) {
		t.Errorf("episode = %+v, want one seeded from the initial list", episode)
	}
}

func TestWrappedEOFCounts(t *testing.T) {
	// A failed list reports EOF wrapped in its request; only a bare EOF is the
	// server ending a watch.
	watcher := New(newClient(), parseConfig(t), time.Now, discardLogger)

	watcher.recordWatchError(deployments, "fisioindoor", fmt.Errorf("Get https://10.96.0.1/apis: %w", io.EOF))

	if !watcher.WatchFailing() {
		t.Error("a wrapped EOF did not count as a failure")
	}
}

func TestObserveReadsTheRevision(t *testing.T) {
	created := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	cfg := parseConfig(t)
	env, _ := cfg.Find("fisioindoor", "production")

	deployment := object("apps/v1", "Deployment", "fisioindoor", "fisioindoor-sws")
	deployment.SetUID("deployment-uid")
	rs := replicaSet("fisioindoor", "fisioindoor-sws-a", "ghcr.io/rmypg/fisioindoor:v1.0.2", "deployment-uid", created, 1)
	rs.SetAnnotations(map[string]string{revisionAnnotation: "7"})

	got := startSynced(t, newClient(deployment, rs), cfg).Observe(env).ReplicaSets
	if len(got) != 1 || got[0].Revision != 7 {
		t.Errorf("ReplicaSets = %+v, want revision 7", got)
	}
}

func TestForgetEpisodeUnwrapsATombstone(t *testing.T) {
	watcher := New(newClient(), parseConfig(t), time.Now, discardLogger)
	item := kustomizationAt("False", "broken", time.Now())

	watcher.trackEpisode(kustomizations, item)
	if watcher.episode(kustomizations, FluxNamespace, "fisioindoor") == nil {
		t.Fatal("no episode to forget")
	}

	watcher.forgetEpisode(kustomizations, cache.DeletedFinalStateUnknown{Key: FluxNamespace + "/fisioindoor", Obj: item})

	if got := watcher.episode(kustomizations, FluxNamespace, "fisioindoor"); got != nil {
		t.Errorf("episode = %+v, want it forgotten", got)
	}
}

func isProbe(action k8stesting.Action) bool {
	list, ok := action.(interface{ GetListOptions() metav1.ListOptions })
	return ok && action.GetVerb() == "list" && action.GetResource() == deployments && list.GetListOptions().Limit == 1
}

func countProbes(client *dynamicfake.FakeDynamicClient) int {
	count := 0
	for _, action := range client.Actions() {
		if isProbe(action) {
			count++
		}
	}
	return count
}

func TestProbeIsNamespacedAndOnlyLists(t *testing.T) {
	client := newClient()
	watcher := New(client, parseConfig(t), time.Now, discardLogger)

	watcher.probe(t.Context())

	actions := client.Actions()
	if len(actions) != 1 {
		t.Fatalf("actions = %v, want one list", actions)
	}
	if !isProbe(actions[0]) {
		t.Errorf("action = %+v, want a list of Deployments with limit 1", actions[0])
	}
	if got := actions[0].GetNamespace(); got != "fisioindoor" {
		t.Errorf("namespace = %q, want the first configured namespace", got)
	}
	if watcher.WatchFailing() {
		t.Error("a successful probe counted as a failure")
	}
}

func TestFailingProbeFailsTheServiceUntilItLapses(t *testing.T) {
	cfg := parseConfig(t)
	clock := newTestClock(time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC))
	client := newClient()
	var failing atomic.Bool
	client.PrependReactor("list", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if failing.Load() && isProbe(action) {
			return true, nil, errors.New("dial tcp 10.96.0.1:443: i/o timeout")
		}
		return false, nil, nil
	})

	watcher := New(client, cfg, clock.now, discardLogger)
	h := server.New(cfg, watcher, clock.now, discardLogger)
	watcher.Start(t.Context())
	waitFor(t, "first sync", watcher.Synced)
	assertAnswering(t, h, http.StatusOK)

	failing.Store(true)
	watcher.probe(t.Context())
	if !watcher.WatchFailing() {
		t.Fatal("WatchFailing = false after a failed probe")
	}
	assertAnswering(t, h, http.StatusServiceUnavailable)

	// A probe that succeeds doesn't end the window early.
	failing.Store(false)
	watcher.probe(t.Context())
	assertAnswering(t, h, http.StatusServiceUnavailable)

	clock.advance(2 * time.Minute)
	assertAnswering(t, h, http.StatusOK)
}

func TestProbeIsBoundedByItsTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	client := &blockingListClient{Interface: newClient(), resource: deployments, namespace: "fisioindoor", release: release}
	watcher := New(client, parseConfig(t), time.Now, discardLogger)
	watcher.probeTimeout = time.Millisecond

	watcher.probe(t.Context())

	if !watcher.WatchFailing() {
		t.Error("a probe that timed out did not count as a failure")
	}
}

func TestProbeCancelledByShutdownIsNotAFailure(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	client := &blockingListClient{Interface: newClient(), resource: deployments, namespace: "fisioindoor", release: release}
	watcher := New(client, parseConfig(t), time.Now, discardLogger)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	watcher.probe(ctx)

	if watcher.WatchFailing() {
		t.Error("a probe cancelled by shutdown counted as a failure")
	}
}

func TestStartRunsTheProbeOnItsSchedule(t *testing.T) {
	client := newClient()
	watcher := New(client, parseConfig(t), time.Now, discardLogger)
	watcher.probeEvery = time.Millisecond

	watcher.Start(t.Context())

	waitFor(t, "more than one probe", func() bool { return countProbes(client) >= 2 })
	for _, action := range client.Actions() {
		if action.GetNamespace() == "" {
			t.Errorf("cluster-scoped %s on %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestProbeLoopStopsWithItsContext(t *testing.T) {
	client := newClient()
	watcher := New(client, parseConfig(t), time.Now, discardLogger)
	watcher.probeEvery = time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		watcher.probeLoop(ctx)
		close(stopped)
	}()
	waitFor(t, "a probe", func() bool { return countProbes(client) >= 1 })

	cancel()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the probe loop did not stop with its context")
	}
}

func TestReadinessFailsWhenAWatchBreaksAfterSync(t *testing.T) {
	cfg := parseConfig(t)
	client := newClient()
	// RBAC, say, is removed after the first list.
	client.PrependWatchReactor("deployments", func(k8stesting.Action) (bool, apiwatch.Interface, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "", errors.New("no access"))
	})

	watcher := New(client, cfg, time.Now, discardLogger)
	watcher.Start(t.Context())
	waitFor(t, "first sync", watcher.Synced)
	waitFor(t, "the watch error", watcher.WatchFailing)

	h := server.New(cfg, watcher, time.Now, discardLogger)
	assertAnswering(t, h, http.StatusServiceUnavailable)
	if response := request(t, h, "/healthz"); response.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", response.Code)
	}
}
