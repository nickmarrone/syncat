package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nickmarrone/syncat/internal/api"
	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/core"
	"github.com/nickmarrone/syncat/internal/transport"
)

// shutdownGrace bounds how long cmdDaemon waits for in-flight HTTP
// requests to finish before forcing the API listener closed, and
// separately how long it waits for it to finish before giving up on a
// clean core.Node shutdown. Generous, since the whole point is letting
// the trash janitor and in-flight transfers finish rather than being
// killed mid-write (SPEC.md §8's graceful-shutdown requirement).
const shutdownGrace = 15 * time.Second

// cmdDaemon implements `syncat daemon [--api ADDR]` (SPEC.md §1/§8): it
// constructs the core.Node, starts the REST API server on a loopback-only
// listener, and runs until SIGINT/SIGTERM, at which point it shuts both
// down in order — stop accepting new API requests, then close the node
// (which itself waits for every background goroutine, including the
// trash janitor and peer sessions, before returning).
func cmdDaemon(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("daemon", flag.ContinueOnError)
	apiAddr := fs.String("api", "", "API listen address, host:port (default: api_addr from config.json, normally 127.0.0.1:8347)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := paths.EnsureDirs(); err != nil {
		return err
	}

	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no config found at %s (run `syncat init` first): %w", paths.ConfigFile(), err)
		}
		return err
	}
	if *apiAddr != "" {
		cfg.APIAddr = *apiAddr
	}

	logger := log.New(os.Stderr, "syncat: ", log.LstdFlags)

	tcKey, _, err := config.LoadOrCreateTailcatKey(context.Background(), paths.TailcatKeyFile())
	if err != nil {
		return fmt.Errorf("load tailcat key: %w", err)
	}
	apiToken, err := config.LoadOrCreateAPIToken(paths.APITokenFile())
	if err != nil {
		return fmt.Errorf("load api token: %w", err)
	}

	// Bind the API listener before starting the node: a bad --api value
	// (non-loopback, unparsable, port in use) should fail fast rather
	// than after tailcat/index/watchers have already spun up.
	ln, err := api.ListenLoopback(cfg.APIAddr)
	if err != nil {
		return err
	}

	tr := transport.NewTailcatTransport(tcKey, nil)

	nodeCtx, nodeCancel := context.WithCancel(context.Background())
	defer nodeCancel()

	n, err := core.Open(nodeCtx, core.Options{
		Paths:     paths,
		Config:    cfg,
		Transport: tr,
		Logger:    logger,
	})
	if err != nil {
		ln.Close()
		return fmt.Errorf("start node: %w", err)
	}

	srv := api.NewServer(n, apiToken, ln.Addr().String(), logger)
	httpServer := &http.Server{Handler: srv.Handler()}

	serveErrCh := make(chan error, 1)
	go func() { serveErrCh <- httpServer.Serve(ln) }()

	logger.Printf("node %q (short id %s) started", n.Status().NodeName, n.Status().ShortID)
	logger.Printf("api listening on http://%s", ln.Addr().String())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		logger.Printf("received %s, shutting down gracefully", sig)
	case err := <-serveErrCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("api server error: %v", err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Printf("api server shutdown: %v", err)
	}

	if err := n.Close(); err != nil {
		logger.Printf("node close: %v", err)
	}
	logger.Printf("shutdown complete")
	return nil
}
