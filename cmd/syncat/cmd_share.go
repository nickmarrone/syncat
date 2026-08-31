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

// cmdShare implements `syncat share add|ls|rm|set` (SPEC.md §8).
func cmdShare(paths *config.Paths, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: syncat share add PATH --name N [--perm ro|rw] [--approval] | ls | rm ID | set ID [...]")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "add":
		return cmdShareAdd(paths, rest)
	case "ls":
		return cmdShareLs(paths, rest)
	case "rm":
		return cmdShareRm(paths, rest)
	case "set":
		return cmdShareSet(paths, rest)
	default:
		return fmt.Errorf("unknown share subcommand %q (want add, ls, rm, or set)", sub)
	}
}

func cmdShareAdd(paths *config.Paths, args []string) error {
	positional, rest := splitLeadingPositional(args, 1)
	fs := flag.NewFlagSet("share add", flag.ContinueOnError)
	name := fs.String("name", "", "share display name (required)")
	perm := fs.String("perm", "ro", "permission: ro|rw")
	approval := fs.Bool("approval", false, "require approval before a peer gets access")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(positional) != 1 || fs.NArg() != 0 {
		return fmt.Errorf("usage: syncat share add PATH --name N [--perm ro|rw] [--approval]")
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	abs, err := filepath.Abs(positional[0])
	if err != nil {
		return fmt.Errorf("resolve %q: %w", positional[0], err)
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	body := map[string]any{"path": abs, "name": *name, "permission": *perm, "approval_required": *approval}
	var share shareView
	if err := c.do(context.Background(), http.MethodPost, "/api/shares", body, &share); err != nil {
		return err
	}
	fmt.Printf("added share %q (%s) at %s [%s, approval=%v]\n", share.Name, share.ID, share.Path, share.Permission, share.ApprovalRequired)
	return nil
}

func cmdShareLs(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("share ls", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "output raw JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var resp struct {
		Shares []shareView `json:"shares"`
	}
	if err := c.do(context.Background(), http.MethodGet, "/api/shares", nil, &resp); err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp.Shares)
	}
	if len(resp.Shares) == 0 {
		fmt.Println("no shares configured")
		return nil
	}
	for _, s := range resp.Shares {
		fmt.Printf("%s\t%-20s\t%s\t%s\tapproval=%v\n", s.ID, s.Name, s.Path, s.Permission, s.ApprovalRequired)
		for _, a := range s.Access {
			fmt.Printf("    peer=%s (%s) access=%s\n", a.PeerKey, a.PeerName, a.Access)
		}
	}
	return nil
}

func cmdShareRm(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("share rm", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: syncat share rm ID\n\nID accepts a name, an id, or a unique id prefix (see `syncat share ls`)")
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	if err := c.do(context.Background(), http.MethodDelete, "/api/shares/"+url.PathEscape(fs.Arg(0)), nil, nil); err != nil {
		return err
	}
	fmt.Printf("removed share %s\n", fs.Arg(0))
	return nil
}

func cmdShareSet(paths *config.Paths, args []string) error {
	positional, rest := splitLeadingPositional(args, 1)
	fs := flag.NewFlagSet("share set", flag.ContinueOnError)
	name := fs.String("name", "", "new display name")
	perm := fs.String("perm", "", "new permission: ro|rw")
	approval := fs.Bool("approval", false, "require approval before a peer gets access")
	noApproval := fs.Bool("no-approval", false, "do not require approval")
	if err := fs.Parse(rest); err != nil {
		return err
	}
	if len(positional) != 1 || fs.NArg() != 0 {
		return fmt.Errorf("usage: syncat share set ID [--name N] [--perm ro|rw] [--approval|--no-approval]\n\nID accepts a name, an id, or a unique id prefix (see `syncat share ls`)")
	}
	if *approval && *noApproval {
		return fmt.Errorf("--approval and --no-approval are mutually exclusive")
	}

	patch := map[string]any{}
	if *name != "" {
		patch["name"] = *name
	}
	if *perm != "" {
		patch["permission"] = *perm
	}
	if *approval {
		patch["approval_required"] = true
	}
	if *noApproval {
		patch["approval_required"] = false
	}
	if len(patch) == 0 {
		return fmt.Errorf("nothing to change: pass at least one of --name, --perm, --approval, --no-approval")
	}

	c, err := newAPIClient(paths)
	if err != nil {
		return err
	}
	var share shareView
	if err := c.do(context.Background(), http.MethodPatch, "/api/shares/"+url.PathEscape(positional[0]), patch, &share); err != nil {
		return err
	}
	fmt.Printf("updated share %q (%s) [%s, approval=%v]\n", share.Name, share.ID, share.Permission, share.ApprovalRequired)
	return nil
}
