package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

func noEnv(string) string { return "" }

func envOf(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestParseOptions(t *testing.T) {
	env := envOf(map[string]string{"SITE_STATUS_CONFIG": "/env.json", "SITE_STATUS_LISTEN": ":9000"})

	tests := []struct {
		name       string
		args       []string
		getenv     func(string) string
		wantConfig string
		wantListen string
		wantErr    bool
	}{
		{name: "flag", args: []string{"-config", "/flag.json"}, getenv: noEnv, wantConfig: "/flag.json", wantListen: defaultListen},
		{name: "environment", getenv: env, wantConfig: "/env.json", wantListen: ":9000"},
		{name: "flag wins over environment", args: []string{"-config", "/flag.json", "-listen", ":7000"}, getenv: env, wantConfig: "/flag.json", wantListen: ":7000"},
		{name: "no configuration", getenv: noEnv, wantErr: true},
		{name: "unexpected argument", args: []string{"-config", "/flag.json", "extra"}, getenv: noEnv, wantErr: true},
		{name: "unknown flag", args: []string{"-nope"}, getenv: noEnv, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOptions(tt.args, tt.getenv)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseOptions = %+v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.configPath != tt.wantConfig || got.listen != tt.wantListen {
				t.Errorf("parseOptions = %+v", got)
			}
		})
	}
}

func TestRunFailsOnABadConfiguration(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if err := run(t.Context(), []string{"-config", "/does/not/exist.json"}, noEnv, logger); err == nil {
		t.Fatal("run succeeded with a missing configuration file")
	}
}

func TestServeShutsDownGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	entered := make(chan struct{})
	release := make(chan struct{})
	httpServer := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(entered)
			<-release
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	// Only Shutdown runs this, so Close would fail the wait for it below.
	shuttingDown := make(chan struct{})
	httpServer.RegisterOnShutdown(func() { close(shuttingDown) })

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	served := make(chan error, 1)
	go func() {
		served <- serve(ctx, httpServer, listener, logger)
	}()

	statusCh := make(chan int, 1)
	go func() {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+listener.Addr().String()+"/", http.NoBody)
		if err != nil {
			t.Error(err)
			statusCh <- 0
			return
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Error(err)
			statusCh <- 0
			return
		}
		defer func() { _ = response.Body.Close() }()
		statusCh <- response.StatusCode
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request never reached the handler")
	}

	// A signal arrives while the request is in flight, which finishes only once
	// the shutdown has begun.
	cancel()
	select {
	case <-shuttingDown:
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not shut down gracefully")
	}
	close(release)

	select {
	case status := <-statusCh:
		if status != http.StatusNoContent {
			t.Errorf("in-flight request got %d, want 204", status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request did not complete")
	}
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("serve = %v, want a clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after shutdown")
	}
}
