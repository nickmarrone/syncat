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

// cmdShareSafety implements `syncat share safety SHARE [flags]`: with no
// flags it prints the share's hold and trash settings, otherwise it changes
// the ones given. SHARE is a share this node offers or subscribes to.
func cmdShareSafety(paths *config.Paths, args []string) error {
	positional, rest := splitLeadingPositional(args, 1)
	fs := flag.NewFlagSet("share safety", flag.ContinueOnError)
	holdPercent := fs.Int("hold-percent", -1, "hold a change that deletes at least this percentage of the share's files (0 = default)")
	holdMin := fs.Int("hold-min", -1, "...and at least this many of them (0 = default)")
	trashDays := fs.Int("trash-days", -1, "keep trash events for this many days (0 = node default)")
	trashMax := fs.String("trash-max", "\x00", `trash size limit: a percentage of the share's size like "25%" or a size like "2GiB" ("" = default)`)
	asJSON := fs.Bool("json", false, "output raw JSON")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(positional) != 1 || fs.NArg() != 0 {
		return fmt.Errorf("usage: syncat share safety SHARE [--hold-percent N] [--hold-min N] [--trash-days N] [--trash-max SIZE]\n\nSHARE accepts a name, an id, or a unique id prefix, for a share you offer or subscribe to")
	}
	ref := positional[0]

	patch := map[string]any{}
	if *holdPercent >= 0 {
		patch["hold_delete_percent"] = *holdPercent
	}
	if *holdMin >= 0 {
		patch["hold_delete_min"] = *holdMin
	}
	if *trashDays >= 0 {
		patch["trash_retention_days"] = *trashDays
	}
	if *trashMax != "\x00" {
		patch["trash_max_size"] = *trashMax
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	// PATCH with an empty body changes nothing and returns the current
	// settings, so one request serves both the show and the change.
	var s safetyView
	if err := c.do(context.Background(), http.MethodPatch, "/api/shares/"+url.PathEscape(ref)+"/safety", patch, &s); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(s)
	}
	fmt.Printf("hold a change that deletes >= %d%% and >= %d of the files\n", s.HoldDeletePercent, s.HoldDeleteMin)
	fmt.Printf("trash keeps events for %d days, up to %s\n", s.TrashRetentionDays, s.TrashMaxSize)
	return nil
}

// cmdShareConfirmDeletes implements `syncat share confirm-deletes SHARE
// [--peer P]`: lets a held change through.
func cmdShareConfirmDeletes(paths *config.Paths, args []string) error {
	positional, rest := splitLeadingPositional(args, 1)
	fs := flag.NewFlagSet("share confirm-deletes", flag.ContinueOnError)
	peer := fs.String("peer", "", "the peer whose held change to let through (needed only when the share has more than one hold)")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(positional) != 1 || fs.NArg() != 0 {
		return fmt.Errorf("usage: syncat share confirm-deletes SHARE [--peer P]\n\nSHARE accepts a name, an id, or a unique id prefix, for a share you offer or subscribe to")
	}
	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var h holdView
	if err := c.do(context.Background(), http.MethodPost, "/api/shares/"+url.PathEscape(positional[0])+"/holds/confirm", map[string]string{"peer": *peer}, &h); err != nil {
		return err
	}
	fmt.Printf("confirmed: %d file(s) deleted from %s\n", h.Deletes, shareLabel(h))
	return nil
}

func shareLabel(h holdView) string {
	if h.ShareName != "" {
		return h.ShareName
	}
	return h.ShareID
}

// describeHold renders one hold as a line of `syncat status`, with what
// to do about it.
func describeHold(h holdView) string {
	held := "on hold"
	confirm := fmt.Sprintf("syncat share confirm-deletes %s", shareLabel(h))
	if h.PeerKey != "" {
		name := h.PeerName
		if name == "" {
			name = h.PeerKey
		}
		held = "changes from " + name + " on hold"
		confirm += " --peer " + name
	}
	since := h.Since.Local().Format(time.DateTime)
	switch h.Reason {
	case "root-unavailable":
		return fmt.Sprintf("%s: %s since %s: the folder is missing or unreadable (restore it, or remove the share)", shareLabel(h), held, since)
	case "reserved-dir-taken":
		return fmt.Sprintf("%s: %s since %s: the folder has a .syncat directory syncat didn't create (move it away)", shareLabel(h), held, since)
	}
	return fmt.Sprintf("%s: %s since %s: %d of %d files would be deleted (fix the folder, or run `%s`)", shareLabel(h), held, since, h.Deletes, h.Live, confirm)
}
