// Command syncat is the syncat daemon and CLI. See SPEC.md.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/nickmarrone/syncat/internal/config"
)

// command is one entry in the subcommand dispatch table (SPEC.md §8).
type command struct {
	name    string
	usage   string
	summary string
	run     func(paths *config.Paths, args []string) error
}

var commands = []command{
	{name: "init", usage: "syncat init [--name NAME]", summary: "generate keys and config for a new node", run: cmdInit},
	{name: "daemon", usage: "syncat daemon [--api ADDR]", summary: "run the syncat daemon", run: cmdDaemon},
	{name: "token", usage: "syncat token", summary: "print this node's connection token", run: cmdToken},
	{name: "peer", usage: "syncat peer add TOKEN [--name N] | ls | rm ID | approve ID", summary: "manage peers (ID: name, key, or key prefix)", run: cmdPeer},
	{name: "status", usage: "syncat status [--watch] [--json]", summary: "show node, peer and share status", run: cmdStatus},
	{name: "share", usage: "syncat share add PATH --name N [--perm ro|rw] [--approval] | ls | rm ID | set ID [...]", summary: "manage local shares (ID: name, id, or id prefix)", run: cmdShare},
	{name: "remote", usage: "syncat remote ls", summary: "list peers' offered shares", run: cmdRemote},
	{name: "subscription", usage: "syncat subscription add PEER SHARE LOCALPATH [--mode mirror|receive] | ls | rm PEER SHARE | pause PEER SHARE | resume PEER SHARE", summary: "manage subscriptions to peers' shares (PEER/SHARE: name, id, or id prefix)", run: cmdSubscription},
	{name: "approvals", usage: "syncat approvals [grant|deny ID]", summary: "manage pending approvals (not implemented — SPEC.md §2.3 is deferred)", run: cmdApprovals},
	{name: "trash", usage: "syncat trash ls SHARE | restore SHARE PATH", summary: "browse/restore trashed files (SHARE: name, id, or id prefix)", run: cmdTrash},
	{name: "config", usage: "syncat config set FIELD VALUE", summary: "set a top-level config field (e.g. debug, api_addr)", run: cmdConfig},
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "syncat: "+err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("syncat", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configDir := fs.String("config", "", "config directory override (default: $XDG_CONFIG_HOME/syncat or ~/.config/syncat)")
	dataDir := fs.String("data", "", "data directory override (default: $XDG_DATA_HOME/syncat or ~/.local/share/syncat)")
	fs.Usage = func() { printUsage(os.Stderr) }

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	rest := fs.Args()
	if len(rest) == 0 {
		printUsage(os.Stdout)
		return nil
	}

	name, subArgs := rest[0], rest[1:]

	var cmd *command
	for i := range commands {
		if commands[i].name == name {
			cmd = &commands[i]
			break
		}
	}
	if cmd == nil {
		printUsage(os.Stderr)
		return fmt.Errorf("unknown command %q", name)
	}

	paths, err := config.ResolvePaths(*configDir, *dataDir)
	if err != nil {
		return err
	}

	return cmd.run(paths, subArgs)
}

func printUsage(w *os.File) {
	fmt.Fprintln(w, "syncat — peer-to-peer directory sync over tailcat")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  syncat [--config DIR] [--data DIR] <command> [args...]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	for _, cmd := range commands {
		fmt.Fprintf(w, "  %-70s %s\n", cmd.usage, cmd.summary)
	}
}

// splitLeadingPositional splits args into up to n leading tokens that
// don't look like a flag (i.e. don't start with "-"), followed by
// whatever remains. SPEC.md §8's CLI usage puts positional arguments
// before flags for several subcommands (`share add PATH --name N`,
// `subscribe PEER SHARE LOCALPATH --mode M`, ...), but the stdlib flag
// package's FlagSet.Parse only recognizes flags that appear before the
// first positional argument — it stops at the first non-flag token and
// treats everything after it (flags included) as positional. Callers
// pre-split with this helper, then hand only rest to fs.Parse.
//
// A path that itself starts with "-" defeats this (it would be
// misidentified as the start of the flag section); that's an accepted,
// documented limitation rather than something worth a full flag/arg
// interleaving parser for this CLI's needs.
func splitLeadingPositional(args []string, n int) (positional, rest []string) {
	for len(positional) < n && len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		positional = append(positional, args[0])
		args = args[1:]
	}
	return positional, args
}
