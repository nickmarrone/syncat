package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"

	"github.com/nickmarrone/syncat/internal/config"
)

// cmdPeer implements `syncat peer add|ls|rm|approve` (SPEC.md §8).
func cmdPeer(paths *config.Paths, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: syncat peer add TOKEN [--name N] | ls | rm ID | approve ID")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "add":
		return cmdPeerAdd(paths, rest)
	case "ls":
		return cmdPeerLs(paths, rest)
	case "rm":
		return cmdPeerRm(paths, rest)
	case "approve":
		return cmdPeerApprove(paths, rest)
	default:
		return fmt.Errorf("unknown peer subcommand %q (want add, ls, rm, or approve)", sub)
	}
}

func cmdPeerAdd(paths *config.Paths, args []string) error {
	positional, rest := splitLeadingPositional(args, 1)
	fs := flag.NewFlagSet("peer add", flag.ContinueOnError)
	name := fs.String("name", "", "display name for the peer (default: the peer's own suggested name)")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(positional) != 1 || fs.NArg() != 0 {
		return fmt.Errorf("usage: syncat peer add TOKEN [--name N]")
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var peer peerView
	body := map[string]string{"token": positional[0], "name": *name}
	if err := c.do(context.Background(), http.MethodPost, "/api/peers", body, &peer); err != nil {
		return err
	}
	fmt.Printf("added peer %q (%s)\n", peer.Name, peer.ID)
	return nil
}

func cmdPeerLs(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("peer ls", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "output raw JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var resp struct {
		Peers []peerView `json:"peers"`
	}
	if err := c.do(context.Background(), http.MethodGet, "/api/peers", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp.Peers)
	}
	if len(resp.Peers) == 0 {
		fmt.Println("no peers configured")
		return nil
	}
	for _, p := range resp.Peers {
		line := fmt.Sprintf("%s\t%-20s\t%-12s\tenabled=%v", p.ID, p.Name, p.State, p.Enabled)
		if p.LastError != "" {
			line += fmt.Sprintf("\terror=%q", p.LastError)
		}
		fmt.Println(line)
	}
	return nil
}

func cmdPeerRm(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("peer rm", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: syncat peer rm ID")
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	if err := c.do(context.Background(), http.MethodDelete, "/api/peers/"+url.PathEscape(fs.Arg(0)), nil, nil); err != nil {
		return err
	}
	fmt.Printf("removed peer %s\n", fs.Arg(0))
	return nil
}

// cmdPeerApprove hits the peer-approval endpoint, which is deferred past
// this build (SPEC.md §2.3's pending-peer queue — see internal/core's
// package doc comment); the server's 501 response message explains that
// to the user rather than the CLI pretending it doesn't exist.
func cmdPeerApprove(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("peer approve", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: syncat peer approve ID")
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	return c.do(context.Background(), http.MethodPost, "/api/peers/"+url.PathEscape(fs.Arg(0))+"/approve", nil, nil)
}
