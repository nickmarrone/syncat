package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"

	"github.com/nickmarrone/syncat/internal/config"
)

// cmdUnsubscribe implements `syncat unsubscribe PEER SHARE` (SPEC.md §8),
// the counterpart to `syncat subscribe`. The REST API has had
// DELETE /api/subscriptions/{peer-key}:{share-id} since Phase 8; this is
// the CLI that reaches it, so removing a subscription no longer means
// hand-rolling a curl or hand-editing config.json.
//
// PEER and SHARE are the same two references `syncat subscribe` takes: a
// name, an id, or a unique id prefix. A subscription whose peer is no
// longer configured has nothing to resolve against, and stays removable by
// its literal stored values — see core.resolveSubscriptionRef.
func cmdUnsubscribe(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("unsubscribe", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: syncat unsubscribe PEER SHARE\n\nPEER and SHARE each accept a name, an id, or a unique id prefix (see `syncat status`)")
	}
	peer, shareID := fs.Arg(0), fs.Arg(1)

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	id := url.PathEscape(peer + ":" + shareID)
	if err := c.do(context.Background(), http.MethodDelete, "/api/subscriptions/"+id, nil, nil); err != nil {
		return err
	}
	// Mirrors core.RemoveSubscription's contract: the subscription is
	// dropped from config and its watcher stopped, but files already
	// synced to the local path are deliberately left alone.
	fmt.Printf("unsubscribed from share %s on peer %s (local files left in place)\n", shareID, peer)
	return nil
}
