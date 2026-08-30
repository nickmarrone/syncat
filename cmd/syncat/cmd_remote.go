package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"

	"github.com/nickmarrone/syncat/internal/config"
)

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
