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
// PEER and SHARE are references in the sense of internal/core/resolve.go:
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
