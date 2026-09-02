package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"

	"github.com/nickmarrone/syncat/internal/config"
)

// cmdShare implements `syncat share add|ls|rm|set` (SPEC.md §8).
func cmdShare(paths *config.Paths, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: syncat share add PATH --name N [--perm ro|rw] [--approval] | ls | rm ID | set ID [...]")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "add":
		return cmdShareAdd(paths, rest)
	case "ls":
		return cmdShareLs(paths, rest)
	case "rm":
		return cmdShareRm(paths, rest)
	case "set":
		return cmdShareSet(paths, rest)
	default:
		return fmt.Errorf("unknown share subcommand %q (want add, ls, rm, or set)", sub)
	}
}

func cmdShareAdd(paths *config.Paths, args []string) error {
	positional, rest := splitLeadingPositional(args, 1)
	fs := flag.NewFlagSet("share add", flag.ContinueOnError)
	name := fs.String("name", "", "share display name (required)")
	perm := fs.String("perm", "ro", "permission: ro|rw")
	approval := fs.Bool("approval", false, "require approval before a peer gets access")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(positional) != 1 || fs.NArg() != 0 {
		return fmt.Errorf("usage: syncat share add PATH --name N [--perm ro|rw] [--approval]")
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	abs, err := filepath.Abs(positional[0])
	if err != nil {
		return fmt.Errorf("resolve %q: %w", positional[0], err)
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	body := map[string]any{"path": abs, "name": *name, "permission": *perm, "approval_required": *approval}
	var share shareView
	if err := c.do(context.Background(), http.MethodPost, "/api/shares", body, &share); err != nil {
		return err
	}
	fmt.Printf("added share %q (%s) at %s [%s, approval=%v]\n", share.Name, share.ID, share.Path, share.Permission, share.ApprovalRequired)
	return nil
}

func cmdShareLs(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("share ls", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "output raw JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var resp struct {
		Shares []shareView `json:"shares"`
	}
	if err := c.do(context.Background(), http.MethodGet, "/api/shares", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp.Shares)
	}
	if len(resp.Shares) == 0 {
		fmt.Println("no shares configured")
		return nil
	}
	for _, s := range resp.Shares {
		fmt.Printf("%s\t%-20s\t%s\t%s\tapproval=%v\n", s.ID, s.Name, s.Path, s.Permission, s.ApprovalRequired)
		for _, a := range s.Access {
			fmt.Printf("    peer=%s (%s) access=%s\n", a.PeerKey, a.PeerName, a.Access)
		}
	}
	return nil
}

func cmdShareRm(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("share rm", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: syncat share rm ID\n\nID accepts a name, an id, or a unique id prefix (see `syncat share ls`)")
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	if err := c.do(context.Background(), http.MethodDelete, "/api/shares/"+url.PathEscape(fs.Arg(0)), nil, nil); err != nil {
		return err
	}
	fmt.Printf("removed share %s\n", fs.Arg(0))
	return nil
}

func cmdShareSet(paths *config.Paths, args []string) error {
	positional, rest := splitLeadingPositional(args, 1)
	fs := flag.NewFlagSet("share set", flag.ContinueOnError)
	name := fs.String("name", "", "new display name")
	perm := fs.String("perm", "", "new permission: ro|rw")
	approval := fs.Bool("approval", false, "require approval before a peer gets access")
	noApproval := fs.Bool("no-approval", false, "do not require approval")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(positional) != 1 || fs.NArg() != 0 {
		return fmt.Errorf("usage: syncat share set ID [--name N] [--perm ro|rw] [--approval|--no-approval]\n\nID accepts a name, an id, or a unique id prefix (see `syncat share ls`)")
	}
	if *approval && *noApproval {
		return fmt.Errorf("--approval and --no-approval are mutually exclusive")
	}

	patch := map[string]any{}
	if *name != "" {
		patch["name"] = *name
	}
	if *perm != "" {
		patch["permission"] = *perm
	}
	if *approval {
		patch["approval_required"] = true
	}
	if *noApproval {
		patch["approval_required"] = false
	}
	if len(patch) == 0 {
		return fmt.Errorf("nothing to change: pass at least one of --name, --perm, --approval, --no-approval")
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var share shareView
	if err := c.do(context.Background(), http.MethodPatch, "/api/shares/"+url.PathEscape(positional[0]), patch, &share); err != nil {
		return err
	}
	fmt.Printf("updated share %q (%s) [%s, approval=%v]\n", share.Name, share.ID, share.Permission, share.ApprovalRequired)
	return nil
}

// cmdSubscription implements `syncat subscription add|ls|rm|pause|resume`
// (SPEC.md §8): the subscriber-side counterpart to `syncat share`, which
// manages what this node offers.
//
// This is deliberately a noun with subcommands rather than the bare
// `subscribe`/`unsubscribe` verbs it replaces. Every other collection the
// daemon exposes is already shaped that way on both sides of the API —
// `syncat peer` over /api/peers, `syncat share` over /api/shares — and the
// two loose verbs meant the subscription collection alone had no obvious
// home for its list, pause and resume operations. The REST API was never
// the inconsistent part; the CLI was.
//
// PEER and SHARE are references in the sense of internal/core/mutations.go:
// a canonical id, a display name, or a unique id prefix.
func cmdSubscription(paths *config.Paths, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: syncat subscription add PEER SHARE LOCALPATH [--mode mirror|receive] | ls | rm PEER SHARE | pause PEER SHARE | resume PEER SHARE")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "add":
		return cmdSubscriptionAdd(paths, rest)
	case "ls":
		return cmdSubscriptionLs(paths, rest)
	case "rm":
		return cmdSubscriptionRm(paths, rest)
	case "pause":
		return cmdSubscriptionSetPaused(paths, rest, true)
	case "resume":
		return cmdSubscriptionSetPaused(paths, rest, false)
	default:
		return fmt.Errorf("unknown subscription subcommand %q (want add, ls, rm, pause, or resume)", sub)
	}
}

func cmdSubscriptionAdd(paths *config.Paths, args []string) error {
	positional, rest := splitLeadingPositional(args, 3)
	fs := flag.NewFlagSet("subscription add", flag.ContinueOnError)
	mode := fs.String("mode", "mirror", "sync mode: mirror|receive")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(positional) != 3 || fs.NArg() != 0 {
		return fmt.Errorf("usage: syncat subscription add PEER SHARE LOCALPATH [--mode mirror|receive]\n\nPEER and SHARE each accept a name, an id, or a unique id prefix (see `syncat remote ls`)")
	}
	peer, shareID := positional[0], positional[1]
	abs, err := filepath.Abs(positional[2])
	if err != nil {
		return fmt.Errorf("resolve %q: %w", positional[2], err)
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	body := map[string]any{"peer": peer, "share_id": shareID, "local_path": abs, "mode": *mode}
	var view subscriptionView
	if err := c.do(context.Background(), http.MethodPost, "/api/subscriptions", body, &view); err != nil {
		return err
	}
	fmt.Printf("subscribed to share %s from %s -> %s [%s]\n", view.ShareID, view.PeerKey, view.LocalPath, view.Mode)
	return nil
}

func cmdSubscriptionLs(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("subscription ls", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "output raw JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var resp struct {
		Subscriptions []subscriptionView `json:"subscriptions"`
	}
	if err := c.do(context.Background(), http.MethodGet, "/api/subscriptions", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp.Subscriptions)
	}
	if len(resp.Subscriptions) == 0 {
		fmt.Println("no subscriptions configured")
		return nil
	}
	for _, s := range resp.Subscriptions {
		fmt.Printf("%s\t%-20s\tfrom %-16s\t-> %s\t[%s]\taccess=%s\tpaused=%v connected=%v\n",
			s.ShareID, s.ShareName, s.PeerName, s.LocalPath, s.Mode, s.Access, s.Paused, s.Connected)
	}
	return nil
}

func cmdSubscriptionRm(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("subscription rm", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: syncat subscription rm PEER SHARE\n\nPEER and SHARE each accept a name, an id, or a unique id prefix (see `syncat subscription ls`)")
	}
	peer, shareID := fs.Arg(0), fs.Arg(1)

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	if err := c.do(context.Background(), http.MethodDelete, "/api/subscriptions/"+subscriptionPath(peer, shareID), nil, nil); err != nil {
		return err
	}
	// Mirrors core.RemoveSubscription's contract: the subscription is
	// dropped from config and its watcher stopped, but files already
	// synced to the local path are deliberately left alone.
	fmt.Printf("unsubscribed from share %s on peer %s (local files left in place)\n", shareID, peer)
	return nil
}

func cmdSubscriptionSetPaused(paths *config.Paths, args []string, paused bool) error {
	verb := "resume"
	if paused {
		verb = "pause"
	}
	fs := flag.NewFlagSet("subscription "+verb, flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: syncat subscription %s PEER SHARE", verb)
	}
	peer, shareID := fs.Arg(0), fs.Arg(1)

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var view subscriptionView
	body := map[string]any{"paused": paused}
	if err := c.do(context.Background(), http.MethodPatch, "/api/subscriptions/"+subscriptionPath(peer, shareID), body, &view); err != nil {
		return err
	}
	fmt.Printf("%sd subscription to share %s from %s\n", verb, view.ShareID, view.PeerKey)
	return nil
}

// subscriptionPath builds the "{peer}:{share}" path segment the
// subscription endpoints address a single subscription by.
func subscriptionPath(peer, shareID string) string {
	return url.PathEscape(peer + ":" + shareID)
}

// cmdTrash implements `syncat trash ls SHARE` and `syncat trash restore
// SHARE PATH` (SPEC.md §7/§8).
//
// This used to operate directly on the on-disk trash and index,
// independent of whether a daemon was running (the same way `syncat
// token`/`syncat init` work offline against config and keys alone). Now
// that the REST API exists, trash is a thin API client like every other
// syncat subcommand (SPEC.md §1) rather than a second, divergent code
// path that touches the SQLite index directly — which would also race a
// live daemon's own index access if one happened to be running at the
// same time. The user-visible change: `syncat trash` now requires the daemon
// to be running, same as `peer`/`share`/`status`/etc.
func cmdTrash(paths *config.Paths, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: syncat trash ls SHARE | syncat trash restore SHARE PATH")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "ls":
		return cmdTrashLs(paths, rest)
	case "restore":
		return cmdTrashRestore(paths, rest)
	default:
		return fmt.Errorf("unknown trash subcommand %q (want ls or restore)", sub)
	}
}

func cmdTrashLs(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("trash ls", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "output raw JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: syncat trash ls SHARE\n\nSHARE accepts a name, an id, or a unique id prefix, for a share you offer or subscribe to")
	}
	shareID := fs.Arg(0)

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var resp struct {
		Entries []trashEntryView `json:"entries"`
	}
	if err := c.do(context.Background(), http.MethodGet, "/api/shares/"+url.PathEscape(shareID)+"/trash", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp.Entries)
	}
	if len(resp.Entries) == 0 {
		fmt.Printf("no trashed files for share %s\n", shareID)
		return nil
	}
	for _, e := range resp.Entries {
		fmt.Printf("%s\t%s\t%d bytes\n", e.TrashedAt.Local().Format("2006-01-02 15:04:05"), e.RelPath, e.Size)
	}
	return nil
}

func cmdTrashRestore(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("trash restore", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: syncat trash restore SHARE PATH\n\nSHARE accepts a name, an id, or a unique id prefix, for a share you offer or subscribe to")
	}
	shareID, relPath := fs.Arg(0), fs.Arg(1)

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	if err := c.do(context.Background(), http.MethodPost, "/api/shares/"+url.PathEscape(shareID)+"/trash/restore", map[string]string{"rel_path": relPath}, nil); err != nil {
		return err
	}
	fmt.Printf("restored %s\n", relPath)
	return nil
}
