package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
)

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

	if len(st.Transfers) > 0 {
		fmt.Printf("transfers (%d):\n", len(st.Transfers))
		for _, t := range st.Transfers {
			fmt.Printf("  %s %s/%s\t%d/%d bytes\n", t.Direction, t.ShareID, t.RelPath, t.BytesTransferred, t.TotalBytes)
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
