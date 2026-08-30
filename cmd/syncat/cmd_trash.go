package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
	syncpkg "github.com/nickmarrone/syncat/internal/sync"
)

// cmdTrash implements `syncat trash ls SHARE` and `syncat trash restore
// SHARE PATH` (SPEC.md §7/§8). It operates directly against the on-disk
// trash and index — it does not require the daemon to be running, the
// same way `syncat token`/`syncat init` work offline against config and
// keys alone.
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
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: syncat trash ls SHARE")
	}
	shareID := fs.Arg(0)

	if _, err := loadConfigOrHint(paths); err != nil {
		return err
	}

	tr := syncpkg.NewTrash(paths.TrashDir(), nil)
	entries, err := tr.List(shareID)
	if err != nil {
		return fmt.Errorf("list trash for share %s: %w", shareID, err)
	}
	if len(entries) == 0 {
		fmt.Printf("no trashed files for share %s\n", shareID)
		return nil
	}
	for _, e := range entries {
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
	shareID, relpath := fs.Arg(0), fs.Arg(1)

	cfg, err := loadConfigOrHint(paths)
	if err != nil {
		return err
	}
	shareRoot, err := shareRootFor(cfg, shareID)
	if err != nil {
		return err
	}
	idKey, err := config.LoadIdentityKey(paths.IdentityKeyFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no identity key found (run `syncat init` first): %w", err)
		}
		return err
	}

	tr := syncpkg.NewTrash(paths.TrashDir(), nil)
	entries, err := tr.List(shareID)
	if err != nil {
		return fmt.Errorf("list trash for share %s: %w", shareID, err)
	}
	// entries are most-recently-trashed first (see Trash.List); restoring
	// the newest match for relpath is the CLI's documented behavior when
	// a path was trashed more than once (e.g. overwritten twice remotely
	// before anyone restored it). `trash ls` shows every entry's exact
	// trashed-at time for a user who wants an older one instead.
	var match *syncpkg.Entry
	for i := range entries {
		if entries[i].RelPath == relpath {
			match = &entries[i]
			break
		}
	}
	if match == nil {
		return fmt.Errorf("no trashed entry %q for share %s", relpath, shareID)
	}

	ctx := context.Background()
	store, err := index.Open(ctx, paths.DBFile())
	if err != nil {
		return fmt.Errorf("open index: %w", err)
	}
	defer store.Close()

	row, err := tr.Restore(ctx, store, idKey.ShortID(), shareRoot, *match)
	if err != nil {
		if errors.Is(err, syncpkg.ErrRestoreDestExists) {
			return fmt.Errorf("restore %s: a file already exists at that path; move or remove it first: %w", relpath, err)
		}
		return fmt.Errorf("restore %s: %w", relpath, err)
	}

	fmt.Printf("restored %s (version %v)\n", relpath, row.Version)
	fmt.Println("note: the daemon must be running (or a rescan must happen) for this restore to propagate to peers.")
	return nil
}

// loadConfigOrHint loads config.json, turning a missing-file error into a
// hint to run `syncat init`, matching cmdToken's style.
func loadConfigOrHint(paths *config.Paths) (*config.Config, error) {
	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("no config found at %s (run `syncat init` first): %w", paths.ConfigFile(), err)
		}
		return nil, err
	}
	return cfg, nil
}

// shareRootFor resolves shareID to the local directory its trash entries
// should be restored into: the share's own path if this node offers it,
// or the local subscription path if this node subscribes to it from a
// peer. A share can appear on at most one side for a given ID from this
// node's own point of view, so the first match wins.
func shareRootFor(cfg *config.Config, shareID string) (string, error) {
	for _, s := range cfg.Shares {
		if s.ID == shareID {
			return s.Path, nil
		}
	}
	for _, sub := range cfg.Subscriptions {
		if sub.ShareID == shareID {
			return sub.LocalPath, nil
		}
	}
	return "", fmt.Errorf("share %q is not a local share or subscription in config.json", shareID)
}
