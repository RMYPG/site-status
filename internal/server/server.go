// Package server answers the HTTP contract in docs/contract.md.
package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/RMYPG/site-status/internal/config"
	"github.com/RMYPG/site-status/internal/rollout"
)

// Source is what the handlers read: the watch's cache.
type Source interface {
	// Synced reports whether every watch has completed its first list.
	Synced() bool
	// WatchFailing reports whether watches have failed recently, so that the
	// cache may have stopped following the cluster.
	WatchFailing() bool
	// Observe reads one environment's objects from the cache.
	Observe(env config.Environment) rollout.Observation
}

type server struct {
	cfg    *config.Config
	source Source
	now    func() time.Time
	log    *slog.Logger
}

// New returns the service's HTTP handler. It takes the clock as an argument so
// that tests control the grace period.
func New(cfg *config.Config, source Source, now func() time.Time, log *slog.Logger) http.Handler {
	s := &server{cfg: cfg, source: source, now: now, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", readOnly(s.healthz))
	mux.HandleFunc("/readyz", readOnly(s.readyz))
	mux.HandleFunc("/v1/sites/{site}/environments/{environment}", readOnly(s.status))
	mux.HandleFunc("/v1/sites/{site}/environments/{environment}/rollouts", readOnly(s.rollouts))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		s.writeError(w, http.StatusNotFound, "not found")
	})

	return noStore(mux)
}

// noStore keeps caches from holding a rollout's state, including on errors.
func noStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// readOnly answers 405 to anything but GET and HEAD, as JSON.
func readOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeJSON(w, http.StatusMethodNotAllowed, errorBody{Error: "method not allowed"})
			return
		}
		next(w, r)
	}
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusBody{Status: "ok"})
}

func (s *server) readyz(w http.ResponseWriter, _ *http.Request) {
	switch {
	case !s.source.Synced():
		writeJSON(w, http.StatusServiceUnavailable, statusBody{Status: "syncing"})
	case s.source.WatchFailing():
		writeJSON(w, http.StatusServiceUnavailable, statusBody{Status: "watch failing"})
	default:
		writeJSON(w, http.StatusOK, statusBody{Status: "ok"})
	}
}

func (s *server) status(w http.ResponseWriter, r *http.Request) {
	env, ok := s.environment(w, r)
	if !ok {
		return
	}
	observation := s.source.Observe(env)
	writeJSON(w, http.StatusOK, rollout.Evaluate(observation, s.now()))
}

func (s *server) rollouts(w http.ResponseWriter, r *http.Request) {
	env, ok := s.environment(w, r)
	if !ok {
		return
	}
	observation := s.source.Observe(env)
	writeJSON(w, http.StatusOK, rolloutsBody{Rollouts: rollout.History(observation.ReplicaSets)})
}

// environment resolves the URL's Site and environment. It answers 404 for a
// name the configuration doesn't hold, and 503 until the watches have synced or
// while they keep failing: an empty or frozen cache says nothing reliable about
// a Site.
func (s *server) environment(w http.ResponseWriter, r *http.Request) (config.Environment, bool) {
	env, ok := s.cfg.Find(r.PathValue("site"), r.PathValue("environment"))
	if !ok {
		s.writeError(w, http.StatusNotFound, "site or environment not found")
		return config.Environment{}, false
	}
	switch {
	case !s.source.Synced():
		s.writeError(w, http.StatusServiceUnavailable, "watches have not synced yet")
		return config.Environment{}, false
	case s.source.WatchFailing():
		s.writeError(w, http.StatusServiceUnavailable, "watches are failing, so an answer could be stale")
		return config.Environment{}, false
	}
	return env, true
}

func (s *server) writeError(w http.ResponseWriter, status int, message string) {
	if status >= http.StatusInternalServerError {
		s.log.Warn("request failed", "status", status, "error", message)
	}
	writeJSON(w, status, errorBody{Error: message})
}

type (
	errorBody struct {
		Error string `json:"error"`
	}
	statusBody struct {
		Status string `json:"status"`
	}
	rolloutsBody struct {
		Rollouts []rollout.Entry `json:"rollouts"`
	}
)

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The header is sent: a failed write means the client left.
	_ = json.NewEncoder(w).Encode(body)
}
