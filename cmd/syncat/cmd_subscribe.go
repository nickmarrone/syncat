package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/nickmarrone/syncat/internal/config"
)

// cmdSubscribe implements `syncat subscribe PEER SHARE LOCALPATH [--mode
// mirror|receive]` (SPEC.md §8).
//
// PEER and SHARE are resolved by the daemon (internal/core/resolve.go) and
// accept any of a canonical id, a display name, or a unique id prefix — so
// the two readable names `syncat remote ls` prints can be typed directly.
func cmdSubscribe(paths *config.Paths, args []string) error {
	positional, rest := splitLeadingPositional(args, 3)
	fs := flag.NewFlagSet("subscribe", flag.ContinueOnError)
	mode := fs.String("mode", "mirror", "sync mode: mirror|receive")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(positional) != 3 || fs.NArg() != 0 {
		return fmt.Errorf("usage: syncat subscribe PEER SHARE LOCALPATH [--mode mirror|receive]\n\nPEER and SHARE each accept a name, an id, or a unique id prefix (see `syncat remote ls`)")
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
	var sub subscriptionView
	if err := c.do(context.Background(), http.MethodPost, "/api/subscriptions", body, &sub); err != nil {
		return err
	}
	fmt.Printf("subscribed to share %s from %s -> %s [%s]\n", sub.ShareID, sub.PeerKey, sub.LocalPath, sub.Mode)
	return nil
}
