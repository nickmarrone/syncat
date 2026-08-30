package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"

	"github.com/nickmarrone/syncat/internal/config"
)

// cmdTrash implements `syncat trash ls SHARE` and `syncat trash restore
// SHARE PATH` (SPEC.md §7/§8).
//
// Deviation from Phase 6's original implementation: this used to operate
// directly on the on-disk trash and index, independent of whether a
// daemon was running (the same way `syncat token`/`syncat init` work
// offline against config and keys alone). Now that the REST API exists,
// trash is rebuilt as a thin API client like every other syncat
// subcommand (SPEC.md §1) rather than a second, divergent code path that
// touches the SQLite index directly — which would also race a live
// daemon's own index access if one happened to be running at the same
// time. The user-visible change: `syncat trash` now requires the daemon
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
		return fmt.Errorf("usage: syncat trash ls SHARE")
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
		return fmt.Errorf("usage: syncat trash restore SHARE PATH")
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
