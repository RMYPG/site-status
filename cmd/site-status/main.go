// Command site-status serves each Site's rollout status from the cluster. It
// reads Kubernetes with its ServiceAccount token, stores nothing, and writes
// nothing to the cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RMYPG/site-status/internal/config"
	"github.com/RMYPG/site-status/internal/server"
	"github.com/RMYPG/site-status/internal/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

const (
	defaultListen   = ":8080"
	shutdownTimeout = 10 * time.Second
)

// version is set at build time.
var version = "dev"

type options struct {
	configPath string
	listen     string
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	// client-go logs through klog; send it to the same JSON stream.
	klog.SetSlogLogger(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Getenv, logger); err != nil {
		logger.Error("exiting", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, logger *slog.Logger) error {
	opts, err := parseOptions(args, getenv)
	if err != nil {
		return err
	}

	cfg, err := config.Load(opts.configPath)
	if err != nil {
		return err
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("kubernetes client configuration: %w", err)
	}
	restConfig.UserAgent = "site-status/" + version
	client, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}

	watcher := watch.New(client, cfg, time.Now, logger)
	watcher.Start(ctx)

	httpServer := &http.Server{
		Addr:              opts.listen,
		Handler:           server.New(cfg, watcher, time.Now, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", opts.listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	logger.Info("starting", "version", version, "listen", listener.Addr().String(), "sites", len(cfg.Sites))
	return serve(ctx, httpServer, listener, logger)
}

// serve answers on listener until ctx ends, then shuts down gracefully:
// requests in flight finish, up to shutdownTimeout.
func serve(ctx context.Context, httpServer *http.Server, listener net.Listener, logger *slog.Logger) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

// parseOptions reads the flags, which win over the environment variables.
func parseOptions(args []string, getenv func(string) string) (options, error) {
	opts := options{}
	flags := flag.NewFlagSet("site-status", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.configPath, "config", getenv("SITE_STATUS_CONFIG"), "path of the JSON configuration file (or SITE_STATUS_CONFIG)")
	flags.StringVar(&opts.listen, "listen", defaultIfEmpty(getenv("SITE_STATUS_LISTEN"), defaultListen), "address to listen on (or SITE_STATUS_LISTEN)")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() > 0 {
		return options{}, fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	if opts.configPath == "" {
		return options{}, errors.New("no configuration file: set -config or SITE_STATUS_CONFIG")
	}
	return opts, nil
}

func defaultIfEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
