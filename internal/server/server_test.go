package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/RMYPG/site-status/internal/config"
	"github.com/RMYPG/site-status/internal/rollout"
)

var now = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

type fakeSource struct {
	synced      bool
	observation rollout.Observation
}

func (f *fakeSource) Synced() bool { return f.synced }

func (f *fakeSource) Observe(config.Environment) rollout.Observation { return f.observation }

func testConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(`{
		"imageUpdateAutomation": "image-tags",
		"sites": [{"id": "fisioindoor", "environments": {"production": {
			"namespace": "fisioindoor", "deployment": "fisioindoor-sws", "helmRelease": "fisioindoor",
			"imagePolicy": "fisioindoor-production", "imageRepository": "fisioindoor", "kustomization": "fisioindoor"
		}}}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func object(kind, name string) rollout.Object {
	return rollout.Object{Kind: kind, Namespace: "flux-system", Name: name, Found: true}
}

// deployingObservation is a Site with a newer target than it runs.
func deployingObservation() rollout.Observation {
	return rollout.Observation{
		Deployment:            object("Deployment", "d"),
		HelmRelease:           object("HelmRelease", "h"),
		ImageUpdateAutomation: object("ImageUpdateAutomation", "i"),
		Kustomization:         object("Kustomization", "k"),
		ImageRepository:       object("ImageRepository", "r"),
		ImagePolicy:           object("ImagePolicy", "p"),
		Target:                "v1.0.3",
		ReplicaSets: []rollout.ReplicaSet{
			{Name: "new", Image: "ghcr.io/rmypg/x:v1.0.3", CreatedAt: now.Add(-time.Minute), Revision: 2, AvailableReplicas: 0},
			{Name: "old", Image: "ghcr.io/rmypg/x:v1.0.2", CreatedAt: now.Add(-time.Hour), Revision: 1, AvailableReplicas: 1},
		},
	}
}

func handler(t *testing.T, source Source) http.Handler {
	t.Helper()
	return New(testConfig(t), source, func() time.Time { return now }, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
	return recorder
}

func TestStatus(t *testing.T) {
	h := handler(t, &fakeSource{synced: true, observation: deployingObservation()})

	response := get(t, h, "/v1/sites/fisioindoor/environments/production")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", response.Code, response.Body)
	}
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"target": "v1.0.3", "running": "v1.0.2", "phase": "deploying", "reason": nil}
	if len(body) != len(want) {
		t.Fatalf("body = %v, want exactly %v", body, want)
	}
	for key, value := range want {
		got, present := body[key]
		if !present || got != value {
			t.Errorf("%s = %v (present %v), want %v", key, got, present, value)
		}
	}
	assertHeaders(t, response)
}

func TestStatusUsesTheClock(t *testing.T) {
	observation := deployingObservation()
	observation.Kustomization.Conditions = []rollout.Condition{
		{Type: "Ready", Status: "False", Reason: "Broken", LastTransitionTime: now.Add(-rollout.Grace - time.Second)},
	}
	h := handler(t, &fakeSource{synced: true, observation: observation})

	var body rollout.Status
	response := get(t, h, "/v1/sites/fisioindoor/environments/production")
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Phase != rollout.PhaseFailed || body.Reason == nil {
		t.Errorf("body = %+v, want failed with a reason", body)
	}
}

func TestReasonWhileLive(t *testing.T) {
	observation := deployingObservation()
	observation.Target = "v1.0.2"
	observation.ImageRepository.Conditions = []rollout.Condition{
		{Type: "Ready", Status: "False", Reason: "ScanFailed", Message: "registry down", LastTransitionTime: now.Add(-time.Hour)},
	}
	h := handler(t, &fakeSource{synced: true, observation: observation})

	var body rollout.Status
	if err := json.Unmarshal(get(t, h, "/v1/sites/fisioindoor/environments/production").Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Phase != rollout.PhaseLive {
		t.Errorf("phase = %q, want live", body.Phase)
	}
	if body.Reason == nil || !strings.Contains(*body.Reason, "ScanFailed") {
		t.Errorf("reason = %v, want the scan failure", body.Reason)
	}
}

func TestRollouts(t *testing.T) {
	h := handler(t, &fakeSource{synced: true, observation: deployingObservation()})

	response := get(t, h, "/v1/sites/fisioindoor/environments/production/rollouts")

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", response.Code, response.Body)
	}
	var body struct {
		Rollouts []struct {
			Tag       *string   `json:"tag"`
			CreatedAt time.Time `json:"createdAt"`
		} `json:"rollouts"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Rollouts) != 2 {
		t.Fatalf("rollouts = %+v, want two entries", body.Rollouts)
	}
	first, second := body.Rollouts[0], body.Rollouts[1]
	if first.Tag == nil || *first.Tag != "v1.0.3" || second.Tag == nil || *second.Tag != "v1.0.2" {
		t.Errorf("tags = %v, %v, want v1.0.3 then v1.0.2", first.Tag, second.Tag)
	}
	if !first.CreatedAt.Equal(now.Add(-time.Minute)) {
		t.Errorf("first createdAt = %v", first.CreatedAt)
	}
	assertHeaders(t, response)
}

func TestRolloutsEmptyList(t *testing.T) {
	observation := deployingObservation()
	observation.ReplicaSets = nil
	h := handler(t, &fakeSource{synced: true, observation: observation})

	response := get(t, h, "/v1/sites/fisioindoor/environments/production/rollouts")
	if strings.TrimSpace(response.Body.String()) != `{"rollouts":[]}` {
		t.Errorf("body = %s, want an empty list", response.Body)
	}
}

func TestNotFound(t *testing.T) {
	h := handler(t, &fakeSource{synced: true, observation: deployingObservation()})

	for _, path := range []string{
		"/v1/sites/absent/environments/production",
		"/v1/sites/fisioindoor/environments/staging",
		"/v1/sites/absent/environments/production/rollouts",
		"/v1/sites/fisioindoor/environments/staging/rollouts",
		"/v1/unknown",
		"/",
	} {
		t.Run(path, func(t *testing.T) {
			response := get(t, h, path)
			if response.Code != http.StatusNotFound {
				t.Errorf("status = %d, want 404", response.Code)
			}
			assertHeaders(t, response)
			assertError(t, response)
		})
	}
}

func TestNotFoundBeforeSync(t *testing.T) {
	// The configuration, not the cache, decides what exists.
	h := handler(t, &fakeSource{synced: false})

	response := get(t, h, "/v1/sites/absent/environments/production")
	if response.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", response.Code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := handler(t, &fakeSource{synced: true})

	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/sites/fisioindoor/environments/production", http.NoBody))

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", recorder.Code)
	}
	if recorder.Header().Get("Allow") != "GET, HEAD" {
		t.Errorf("Allow = %q", recorder.Header().Get("Allow"))
	}
	assertHeaders(t, recorder)
	assertError(t, recorder)
}

func TestReadinessBeforeSync(t *testing.T) {
	h := handler(t, &fakeSource{synced: false})

	if response := get(t, h, "/readyz"); response.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d, want 503", response.Code)
	} else {
		assertHeaders(t, response)
	}
	if response := get(t, h, "/healthz"); response.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", response.Code)
	}
	for _, path := range []string{
		"/v1/sites/fisioindoor/environments/production",
		"/v1/sites/fisioindoor/environments/production/rollouts",
	} {
		response := get(t, h, path)
		if response.Code != http.StatusServiceUnavailable {
			t.Errorf("%s = %d, want 503", path, response.Code)
		}
		assertHeaders(t, response)
		assertError(t, response)
	}
}

func TestReadinessAfterSync(t *testing.T) {
	h := handler(t, &fakeSource{synced: true, observation: deployingObservation()})

	if response := get(t, h, "/readyz"); response.Code != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", response.Code)
	}
}

func assertHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q", got)
	}
}

func assertError(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %s", response.Body)
	}
	if body["error"] == "" {
		t.Errorf("body = %v, want an error message", body)
	}
}
