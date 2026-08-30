// Command syncat is the syncat daemon and CLI. See SPEC.md.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/nickmarrone/syncat/internal/config"
)

// command is one entry in the subcommand dispatch table. Phase 1 implements
// only init and token; every other SPEC.md §8 subcommand is listed here so
// the CLI shape is visible from day one, but its run func reports that it
// isn't implemented yet.
type command struct {
	name        string
	usage       string
	summary     string
	implemented bool
	run         func(paths *config.Paths, args []string) error
}

var commands = []command{
	{name: "init", usage: "syncat init [--name NAME]", summary: "generate keys and config for a new node", implemented: true, run: cmdInit},
	{name: "daemon", usage: "syncat daemon [--api ADDR]", summary: "run the syncat daemon"},
	{name: "token", usage: "syncat token", summary: "print this node's connection token", implemented: true, run: cmdToken},
	{name: "peer", usage: "syncat peer add TOKEN [--name N] | ls | rm ID | approve ID", summary: "manage peers"},
	{name: "status", usage: "syncat status [--watch]", summary: "show node/peer/transfer status"},
	{name: "share", usage: "syncat share add PATH --name N [--perm ro|rw] [--approval] | ls | rm ID | set", summary: "manage local shares"},
	{name: "remote", usage: "syncat remote ls", summary: "list peers' offered shares"},
	{name: "subscribe", usage: "syncat subscribe PEER SHARE LOCALPATH [--mode mirror|receive]", summary: "sync a peer's share locally"},
	{name: "approvals", usage: "syncat approvals [grant|deny ID]", summary: "manage pending approvals"},
	{name: "trash", usage: "syncat trash ls|restore SHARE [PATH]", summary: "browse/restore trashed files"},
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
	if !cmd.implemented {
		return fmt.Errorf("command %q is not implemented yet (usage: %s)", name, cmd.usage)
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
		status := ""
		if !cmd.implemented {
			status = " (not yet implemented)"
		}
		fmt.Fprintf(w, "  %-70s %s%s\n", cmd.usage, cmd.summary, status)
	}
}
