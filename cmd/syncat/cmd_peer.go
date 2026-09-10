package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"time"

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
		Peers        []peerView        `json:"peers"`
		PendingPeers []pendingPeerView `json:"pending_peers"`
	}
	if err := c.do(context.Background(), http.MethodGet, "/api/peers", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp)
	}
	if len(resp.Peers) == 0 && len(resp.PendingPeers) == 0 {
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
	for _, p := range resp.PendingPeers {
		fmt.Printf("%s\t%-20s\tpending approval\tfirst_seen=%s\n", p.ID, p.Name, p.FirstSeen.Format(time.RFC3339))
	}
	return nil
}

func cmdPeerRm(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("peer rm", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: syncat peer rm ID\n\nID accepts a name, an id, or a unique id prefix (see `syncat peer ls`)")
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
	if err := c.do(context.Background(), http.MethodPost, "/api/peers/"+url.PathEscape(fs.Arg(0))+"/approve", nil, nil); err != nil {
		return err
	}
	fmt.Printf("approved peer %s\n", fs.Arg(0))
	return nil
}

// cmdRemote implements `syncat remote ls` (SPEC.md §8): everything
// configured peers offer us, with our access state to each.
func cmdRemote(paths *config.Paths, args []string) error {
	if len(args) == 0 || args[0] != "ls" {
		return fmt.Errorf("usage: syncat remote ls")
	}
	fs := flag.NewFlagSet("remote ls", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "output raw JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var resp struct {
		RemoteShares []remoteShareView `json:"remote_shares"`
	}
	if err := c.do(context.Background(), http.MethodGet, "/api/remote-shares", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp.RemoteShares)
	}
	if len(resp.RemoteShares) == 0 {
		fmt.Println("no remote shares seen yet")
		return nil
	}
	for _, rs := range resp.RemoteShares {
		fmt.Printf("%s\t%-20s\tfrom %s (%s)\t%s\taccess=%s\tapproval=%v\n",
			rs.ShareID, rs.Name, rs.PeerName, rs.PeerKey, rs.Permission, rs.Access, rs.ApprovalRequired)
	}
	return nil
}

// cmdApprovals implements `syncat approvals [grant|deny ID]` (SPEC.md §8).
func cmdApprovals(paths *config.Paths, args []string) error {
	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		var resp struct {
			Approvals []approvalView `json:"approvals"`
		}
		if err := c.do(context.Background(), http.MethodGet, "/api/approvals", nil, &resp); err != nil {
			return err
		}
		if len(resp.Approvals) == 0 {
			fmt.Println("no pending approvals")
			return nil
		}
		for _, approval := range resp.Approvals {
			if approval.Kind == "peer" {
				fmt.Printf("%s\tpeer\t%s (%s)\n", approval.ID, approval.PeerName, approval.PeerKey)
			} else {
				fmt.Printf("%s\tshare\t%s (%s) for %s (%s)\n", approval.ID, approval.ShareName, approval.ShareID, approval.PeerName, approval.PeerKey)
			}
		}
		return nil
	}
	if len(args) != 2 || (args[0] != "grant" && args[0] != "deny") {
		return fmt.Errorf("usage: syncat approvals [grant|deny ID]")
	}
	decision, id := args[0], args[1]
	if err := c.do(context.Background(), http.MethodPost, "/api/approvals/"+url.PathEscape(id), map[string]string{"decision": decision}, nil); err != nil {
		return err
	}
	past := "granted"
	if decision == "deny" {
		past = "denied"
	}
	fmt.Printf("%s approval %s\n", past, id)
	return nil
}
