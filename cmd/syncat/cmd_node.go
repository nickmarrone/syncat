package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nickmarrone/syncat/internal/api"
	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/core"
	"github.com/nickmarrone/syncat/internal/transport"
)

// cmdInit implements `syncat init [--name NAME]`. It is idempotent: running
// it again never regenerates existing keys, the api token, or an existing
// config.json's node name.
func cmdInit(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	name := fs.String("name", "", "display name for this node (default: hostname)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := paths.EnsureDirs(); err != nil {
		return err
	}

	cfgPath := paths.ConfigFile()
	cfg, err := config.Load(cfgPath)
	switch {
	case err == nil:
		// Config already exists: leave its node name alone so init stays
		// idempotent even if --name is passed differently on a re-run.
	case errors.Is(err, os.ErrNotExist):
		nodeName := *name
		if nodeName == "" {
			nodeName, err = os.Hostname()
			if err != nil {
				return fmt.Errorf("determine hostname: %w", err)
			}
		}
		cfg = config.Default()
		cfg.NodeName = nodeName
		if err := config.Save(cfgPath, cfg); err != nil {
			return err
		}
	default:
		return err
	}

	idKey, idCreated, err := config.LoadOrCreateIdentityKey(paths.IdentityKeyFile())
	if err != nil {
		return err
	}

	tcKey, tcCreated, err := config.LoadOrCreateTailcatKey(context.Background(), paths.TailcatKeyFile())
	if err != nil {
		return err
	}

	if _, err := config.LoadOrCreateAPIToken(paths.APITokenFile()); err != nil {
		return err
	}

	fmt.Printf("node name:     %s\n", cfg.NodeName)
	fmt.Printf("short id:      %s\n", idKey.ShortID())
	fmt.Printf("config dir:    %s\n", paths.ConfigDir)
	fmt.Printf("data dir:      %s\n", paths.DataDir)
	if idCreated {
		fmt.Println("identity key:  generated")
	} else {
		fmt.Println("identity key:  already present")
	}
	if tcCreated {
		fmt.Printf("tailcat key:   generated (DERP region %d)\n", tcKey.Public.RegionID)
	} else {
		fmt.Println("tailcat key:   already present")
	}
	fmt.Println()
	fmt.Println("run `syncat token` to print this node's connection token.")
	return nil
}

// cmdToken implements `syncat token`. It derives the tailcat ConnBlob from
// the persisted key's already-resolved DERP region (PrivateKey.Public)
// rather than starting a tailcat.Server, so it works with no daemon
// running and no network access. The region was baked in at `syncat init`
// time precisely so this stays stable across invocations.
func cmdToken(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("token", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no config found at %s (run `syncat init` first): %w", paths.ConfigFile(), err)
		}
		return err
	}

	idKey, err := config.LoadIdentityKey(paths.IdentityKeyFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no identity key found (run `syncat init` first): %w", err)
		}
		return err
	}

	tcKey, err := config.LoadTailcatKey(paths.TailcatKeyFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no tailcat key found (run `syncat init` first): %w", err)
		}
		return err
	}

	connBlob := tcKey.Public.ConnBlob()
	tok, err := config.EncodeToken(string(connBlob), idKey.Public(), cfg.NodeName)
	if err != nil {
		return err
	}

	fmt.Println(tok)
	return nil
}

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

	logger := log.New(os.Stderr, "syncat: ", log.LstdFlags)

	nodeCtx, nodeCancel := context.WithCancel(context.Background())
	defer nodeCancel()

	n, httpServer, ln, err := startDaemon(nodeCtx, paths, *apiAddr, logger)
	if err != nil {
		return err
	}

	serveErrCh := make(chan error, 1)
	go func() { serveErrCh <- httpServer.Serve(ln) }()

	logger.Printf("node %q (short id %s) started", n.Status().NodeName, n.Status().ShortID)
	logger.Printf("api listening on http://%s", ln.Addr().String())

	waitAndShutdown(n, httpServer, serveErrCh, logger)
	return nil
}

// startDaemon does cmdDaemon's construction half: load config (with the
// --api override applied), load the tailcat key and api token, bind the
// loopback API listener, build the transport, open the core.Node and
// build the API server. The listener is bound but not yet served; the
// caller starts Serve. On error nothing is left running.
func startDaemon(nodeCtx context.Context, paths *config.Paths, apiAddr string, logger *log.Logger) (*core.Node, *http.Server, net.Listener, error) {
	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil, fmt.Errorf("no config found at %s (run `syncat init` first): %w", paths.ConfigFile(), err)
		}
		return nil, nil, nil, err
	}
	if apiAddr != "" {
		cfg.APIAddr = apiAddr
	}

	tcKey, _, err := config.LoadOrCreateTailcatKey(context.Background(), paths.TailcatKeyFile())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load tailcat key: %w", err)
	}
	apiToken, err := config.LoadOrCreateAPIToken(paths.APITokenFile())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load api token: %w", err)
	}

	// Bind the API listener before starting the node: a bad --api value
	// (non-loopback, unparsable, port in use) should fail fast rather
	// than after tailcat/index/watchers have already spun up.
	ln, err := api.ListenLoopback(cfg.APIAddr)
	if err != nil {
		return nil, nil, nil, err
	}

	// tailcat's own logging (netcheck reports, link-change/route-monitor
	// events) is routine network housekeeping, not something syncat's own
	// operational log should carry by default; only forward it when the
	// user has opted in via `syncat config set debug true`.
	var tcLogf func(string, ...any)
	if cfg.Debug {
		tcLogf = logger.Printf
	}
	tr := transport.NewTailcatTransport(tcKey, tcLogf)

	n, err := core.Open(nodeCtx, core.Options{
		Paths:     paths,
		Config:    cfg,
		Transport: tr,
		Logger:    logger,
	})
	if err != nil {
		ln.Close()
		return nil, nil, nil, fmt.Errorf("start node: %w", err)
	}

	srv := api.NewServer(n, apiToken, ln.Addr().String(), logger)
	httpServer := &http.Server{Handler: srv.Handler()}
	return n, httpServer, ln, nil
}

// waitAndShutdown does cmdDaemon's run-and-stop half: block until
// SIGINT/SIGTERM arrives or the API server's Serve returns on its own
// (serveErrCh), then shut down in order — stop accepting new API requests
// (bounded by shutdownGrace), then close the node, which itself waits for
// every background goroutine before returning.
func waitAndShutdown(n *core.Node, httpServer *http.Server, serveErrCh <-chan error, logger *log.Logger) {
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
}

// cmdStatus implements `syncat status [--watch] [--json]` (SPEC.md §8).
// --watch keeps it simple, as instructed: repoll every 2s and redraw by
// clearing the screen.
func cmdStatus(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	watch := fs.Bool("watch", false, "repoll and redraw every 2 seconds")
	asJSON := fs.Bool("json", false, "output raw JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}

	if !*watch {
		return printStatus(c, *asJSON)
	}
	for {
		fmt.Print("\033[H\033[2J")
		if err := printStatus(c, *asJSON); err != nil {
			fmt.Fprintln(os.Stderr, "syncat:", err)
		}
		time.Sleep(2 * time.Second)
	}
}

func printStatus(c *apiClient, asJSON bool) error {
	var st statusView
	if err := c.do(context.Background(), http.MethodGet, "/api/status", nil, &st); err != nil {
		return err
	}
	if asJSON {
		return printJSON(st)
	}

	fmt.Printf("node:     %s (short id %s)\n", st.NodeName, st.ShortID)
	fmt.Printf("uptime:   %s\n", time.Duration(st.UptimeSeconds*float64(time.Second)).Round(time.Second))
	fmt.Println()

	fmt.Printf("peers (%d):\n", len(st.Peers))
	for _, p := range st.Peers {
		fmt.Printf("  %-20s %s\tstate=%s\n", p.Name, p.ID, p.State)
	}

	fmt.Printf("shares (%d):\n", len(st.Shares))
	for _, s := range st.Shares {
		fmt.Printf("  %-20s %s\t%s\tapproval=%v\n", s.Name, s.Path, s.Permission, s.ApprovalRequired)
	}

	fmt.Printf("subscriptions (%d):\n", len(st.Subscriptions))
	for _, sub := range st.Subscriptions {
		fmt.Printf("  %s from %-16s -> %s\t[%s]\tpaused=%v connected=%v\n",
			sub.ShareName, sub.PeerName, sub.LocalPath, sub.Mode, sub.Paused, sub.Connected)
		for _, w := range sub.Warnings {
			fmt.Printf("    ! %s: %s (at %s)\n", w.RelPath, w.Reason, w.At.Local().Format(time.RFC3339))
		}
	}

	if len(st.Rejected) > 0 {
		fmt.Printf("rejected connections (%d):\n", len(st.Rejected))
		for _, r := range st.Rejected {
			fmt.Printf("  %s (%s) at %s: %s\n", r.PeerKey, r.PeerName, r.At.Local().Format(time.RFC3339), r.Reason)
		}
	}
	return nil
}

// configField describes one top-level scalar config.json field settable via
// `syncat config set`. Collection fields (peers, shares, subscriptions,
// global_ignores) have their own dedicated commands and are not listed
// here.
type configField struct {
	set func(cfg *config.Config, value string) error
	get func(cfg *config.Config) string
}

var configFields = map[string]configField{
	"node_name": {
		set: func(cfg *config.Config, value string) error { cfg.NodeName = value; return nil },
		get: func(cfg *config.Config) string { return cfg.NodeName },
	},
	"api_addr": {
		set: func(cfg *config.Config, value string) error { cfg.APIAddr = value; return nil },
		get: func(cfg *config.Config) string { return cfg.APIAddr },
	},
	"trash_retention_days": {
		set: func(cfg *config.Config, value string) error {
			n, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("trash_retention_days must be an integer: %w", err)
			}
			cfg.TrashRetentionDays = n
			return nil
		},
		get: func(cfg *config.Config) string { return strconv.Itoa(cfg.TrashRetentionDays) },
	},
	"rescan_interval_seconds": {
		set: func(cfg *config.Config, value string) error {
			n, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("rescan_interval_seconds must be an integer: %w", err)
			}
			cfg.RescanIntervalSeconds = n
			return nil
		},
		get: func(cfg *config.Config) string { return strconv.Itoa(cfg.RescanIntervalSeconds) },
	},
	"debug": {
		set: func(cfg *config.Config, value string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("debug must be a boolean (true/false): %w", err)
			}
			cfg.Debug = b
			return nil
		},
		get: func(cfg *config.Config) string { return strconv.FormatBool(cfg.Debug) },
	},
}

// cmdConfig implements `syncat config set FIELD VALUE` (SPEC.md §8-adjacent:
// direct field edits for the scalar settings in config.json — peers,
// shares, and subscriptions keep their own add/ls/rm/set commands). It
// edits config.json directly rather than going through the API, since some
// fields (e.g. debug) only take effect on the next `syncat daemon` start
// and there's no requirement that the daemon be running to change them.
func cmdConfig(paths *config.Paths, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: syncat config set FIELD VALUE")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "set":
		return cmdConfigSet(paths, rest)
	default:
		return fmt.Errorf("unknown config subcommand %q (want set)", sub)
	}
}

func cmdConfigSet(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("config set", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: syncat config set FIELD VALUE\n\nFIELD is one of: %s", knownConfigFields())
	}
	field, value := fs.Arg(0), fs.Arg(1)

	cf, ok := configFields[field]
	if !ok {
		return fmt.Errorf("unknown config field %q (want one of: %s)", field, knownConfigFields())
	}

	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no config found at %s (run `syncat init` first): %w", paths.ConfigFile(), err)
		}
		return err
	}

	if err := cf.set(cfg, value); err != nil {
		return err
	}

	if err := config.Save(paths.ConfigFile(), cfg); err != nil {
		return err
	}

	fmt.Printf("%s = %s\n", field, cf.get(cfg))
	return nil
}

func knownConfigFields() string {
	names := make([]string, 0, len(configFields))
	for name := range configFields {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
