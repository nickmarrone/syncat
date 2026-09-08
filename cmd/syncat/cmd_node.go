package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
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

// cmdInit implements `syncat init [--name NAME] [--reset [--yes]]`.
//
// Without --reset it is idempotent: running it again never regenerates
// existing keys, the api token, or an existing config.json's node name.
// With --reset it first deletes every file this node owns (see
// resetTargets) and then runs that same path over a clean slate, so the
// node comes back with a new identity — and --name means something again,
// since the config it would otherwise defer to is gone.
func cmdInit(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	name := fs.String("name", "", "display name for this node (default: hostname)")
	reset := fs.Bool("reset", false, "delete this node's config, keys, index and trash, then re-initialize from scratch")
	yes := fs.Bool("yes", false, "skip the --reset confirmation prompt (for scripts)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *yes && !*reset {
		return errors.New("--yes only applies to `syncat init --reset`")
	}

	if *reset {
		if !*yes && !stdinIsTerminal() {
			return errors.New("`init --reset` needs a terminal to confirm on; pass --yes to skip the prompt")
		}
		if err := resetState(paths, os.Stdin, os.Stdout, *yes); err != nil {
			return err
		}
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

// --- init --reset ------------------------------------------------------

// resetConfirmWord is what the user has to type at the --reset prompt.
// Case-sensitive and not a word anyone types by reflex, unlike "y".
const resetConfirmWord = "RESET"

// resetState performs `init --reset`'s destructive half: refuse if a
// daemon is live, show what goes and what stays, take the confirmation,
// then delete. cmdInit's ordinary path runs afterwards and rebuilds
// everything from nothing.
//
// in and out are parameters rather than os.Stdin/os.Stdout so the prompt
// is testable; cmdInit passes the real ones.
func resetState(paths *config.Paths, in io.Reader, out io.Writer, assumeYes bool) error {
	targets := resetTargets(paths)

	if err := checkResetOwnership(paths, targets); err != nil {
		return err
	}
	if addr, running := daemonRunning(paths); running {
		return fmt.Errorf("something is listening on %s — stop the syncat daemon before running `init --reset`", addr)
	}

	// Best-effort: a reset has to work when config.json is corrupt or
	// missing, which is one of the reasons to run it. A nil cfg just
	// means we can't name the directories being left alone.
	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		cfg = nil
	}

	kept := keptPaths(cfg)

	if !assumeYes {
		if err := confirmReset(in, out, targets, kept); err != nil {
			return err
		}
	}

	return removeState(targets)
}

// checkResetOwnership refuses a reset that would hand this node's state
// to a different user than the one that owns it today.
//
// The case this exists for is the packaged install (README, "System-wide
// service"): state under /etc/syncat and /var/lib/syncat owned by a
// dedicated `syncat` user, and a reset run as root instead. Nothing stops
// root from doing it — that is the problem. EnsureDirs and
// writeFileAtomic create as whoever is running, so the reset deletes
// syncat-owned files and puts root-owned ones back, and the daemon
// (User=syncat) can no longer read its own config or keys. It fails to
// start, and with Restart=on-failure it does so every five seconds.
//
// This is specifically a hazard --reset introduced. A plain `init` as the
// wrong user was harmless: it found every file already present and
// regenerated nothing, so ownership never moved. Deleting first is what
// turns the same mistake into a broken install, so the guard belongs
// here rather than in cmdInit.
//
// The config and data dirs are checked alongside the targets because a
// half-initialized install can have the right dirs and no files in them
// yet, which is the same trap one step earlier.
func checkResetOwnership(paths *config.Paths, targets []string) error {
	me := currentUID()
	checked := append([]string{paths.ConfigDir, paths.DataDir}, targets...)

	path, owner, conflict := ownershipConflict(checked, me, statOwner)
	if !conflict {
		return nil
	}
	return fmt.Errorf("%s is owned by %s, but this is running as %s.\n"+
		"A reset would delete that state and recreate it owned by %s, leaving a daemon\n"+
		"running as %s unable to read its own config and keys. Rerun it as the owner:\n"+
		"\n    sudo -u %s %s",
		path, userLabel(owner), userLabel(me), userLabel(me),
		userLabel(owner), userLabel(owner), strings.Join(os.Args, " "))
}

// ownershipConflict returns the first path in paths whose owner is
// somebody other than me. ownerOf reports (uid, true) for a path it could
// stat, and false for one that is absent or whose owner the platform
// won't tell us; either way there is nothing to compare and the path is
// skipped. Taking ownerOf as a parameter keeps this testable without
// needing to actually own files as two different users.
func ownershipConflict(paths []string, me int, ownerOf func(string) (int, bool)) (string, int, bool) {
	if me < 0 {
		return "", 0, false
	}
	for _, p := range paths {
		owner, ok := ownerOf(p)
		if !ok || owner == me {
			continue
		}
		return p, owner, true
	}
	return "", 0, false
}

// statOwner is ownershipConflict's production ownerOf. It does not follow
// symlinks: the question is who owns the thing reset would delete.
func statOwner(path string) (int, bool) {
	fi, err := os.Lstat(path)
	if err != nil {
		return 0, false
	}
	return fileOwner(fi)
}

// userLabel renders a uid as a username when one can be resolved and as
// "uid N" when it can't, so the error reads like the sudo line it is
// asking for rather than like a stat dump.
func userLabel(uid int) string {
	if u, err := user.LookupId(strconv.Itoa(uid)); err == nil && u.Username != "" {
		return u.Username
	}
	return "uid " + strconv.Itoa(uid)
}

// resetTargets lists every path `init --reset` deletes, in deletion
// order. This is an explicit list rather than a RemoveAll of ConfigDir
// and DataDir because --config/--data point wherever the user says, and
// blowing away a directory we were merely handed is a different and much
// worse operation than deleting the files we wrote into it.
//
// The three directories go whole: that is what takes index.db's -wal and
// -shm sidecars and the per-share trash subtrees without enumerating
// them. The .tmp-* glob covers writeFileAtomic's leftovers from a crashed
// write, since the config dir is the one we don't remove outright.
func resetTargets(p *config.Paths) []string {
	targets := []string{
		p.ConfigFile(),
		p.APITokenFile(),
		p.KeysDir(),
		p.DBDir(),
		p.TrashDir(),
	}
	// Glob errors only on a malformed pattern, and this one is a literal.
	tmps, _ := filepath.Glob(filepath.Join(p.ConfigDir, ".tmp-*"))
	sort.Strings(tmps)
	return append(targets, tmps...)
}

// keptPaths lists the directories holding actual files — this node's
// shares and its subscriptions' local copies — none of which a reset
// touches. It exists so the prompt can name them instead of just
// promising they are safe. Callers must build this before deleting
// config.json, which is the only record of where they are.
func keptPaths(cfg *config.Config) []string {
	if cfg == nil {
		return nil
	}
	var kept []string
	for _, sh := range cfg.Shares {
		kept = append(kept, sh.Path)
	}
	for _, sub := range cfg.Subscriptions {
		kept = append(kept, sub.LocalPath)
	}
	sort.Strings(kept)
	return kept
}

// confirmReset prints what is about to happen and requires the user to
// type resetConfirmWord. Any other input — including EOF — is an error,
// so a caller that ignores nothing deletes nothing.
func confirmReset(in io.Reader, out io.Writer, targets, kept []string) error {
	fmt.Fprintln(out, "This will PERMANENTLY delete:")
	for _, t := range targets {
		fmt.Fprintf(out, "  %s\n", t)
	}
	fmt.Fprintln(out)
	if len(kept) > 0 {
		fmt.Fprintln(out, "Your files are left in place:")
		for _, k := range kept {
			fmt.Fprintf(out, "  %s\n", k)
		}
	} else {
		fmt.Fprintln(out, "No share or subscription directory is touched.")
	}
	fmt.Fprintln(out)
	fmt.Fprintln(out, "This node gets a new identity key, so its token changes and every")
	fmt.Fprintln(out, "peer will have to add it again. The old identity cannot be recovered.")
	fmt.Fprintln(out)
	fmt.Fprintf(out, "Type %s to confirm: ", resetConfirmWord)

	line, err := bufio.NewReader(in).ReadString('\n')
	// A final line with no newline is still an answer, so only treat EOF
	// as fatal when it arrives with nothing before it.
	if err != nil && !(errors.Is(err, io.EOF) && line != "") {
		return errors.New("reset aborted, nothing was deleted")
	}
	if strings.TrimSpace(line) != resetConfirmWord {
		return errors.New("reset aborted, nothing was deleted")
	}
	return nil
}

// removeState deletes every target. A path that is already gone is not an
// error: reset's job is to end with them absent, not to have removed them.
func removeState(targets []string) error {
	for _, t := range targets {
		if err := os.RemoveAll(t); err != nil {
			return fmt.Errorf("reset: remove %s: %w", t, err)
		}
	}
	return nil
}

// stdinIsTerminal reports whether stdin is a character device, i.e. that
// there is a person on the other end to answer the prompt. Without this a
// redirect (`init --reset < /dev/null`) would read EOF, and a reset that
// nobody confirmed is exactly what the prompt exists to prevent — so a
// non-interactive stdin has to say --yes and mean it.
func stdinIsTerminal() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// daemonRunning reports whether anything answers on this node's API
// address, which is the closest thing to a liveness check syncat has —
// the daemon writes no pid file and takes no lock, so the bound port is
// the only trace it leaves (README: "writes no PID file").
//
// It dials rather than calling GET /api/status through newAPIClient
// because that needs both a parseable config.json and a valid api.token,
// and repairing exactly those is a reason to reset. The cost is that an
// unrelated process on the port reads as a live daemon; the error names
// the address so that is at least diagnosable.
//
// The check matters because unlinking index.db under a running daemon
// fails silently rather than loudly: POSIX lets the daemon keep writing
// to the now-nameless inode while its config and keys are replaced
// underneath it.
func daemonRunning(paths *config.Paths) (string, bool) {
	addr := config.DefaultAPIAddr
	if cfg, err := config.Load(paths.ConfigFile()); err == nil && cfg.APIAddr != "" {
		addr = cfg.APIAddr
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return addr, false
	}
	conn.Close()
	return addr, true
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

	logger := newDaemonLogger()

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

// newDaemonLogger builds the daemon's stderr logger, dropping the parts
// of each line that whatever is reading stderr already supplies.
//
// systemd sets JOURNAL_STREAM when a service's stdout/stderr is connected
// directly to the journal, and journald stamps every entry it receives with
// its own timestamp and the unit's syslog identifier. Our own prefix and
// LstdFlags are pure duplication there — `journalctl` shows
//
//	Sep 07 11:17:31 host syncat[98783]: syncat: 2026/09/07 11:17:31 api: ...
//
// with the date and the program name each appearing twice, which is both
// noise and, in the case of the date, two different formats of the same
// instant. Under journald we emit the bare message and let journald do the
// framing; run from a terminal, where nothing else adds context, we keep
// the full prefix and timestamp.
func newDaemonLogger() *log.Logger {
	if os.Getenv("JOURNAL_STREAM") != "" {
		return log.New(os.Stderr, "", 0)
	}
	return log.New(os.Stderr, "syncat: ", log.LstdFlags)
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
