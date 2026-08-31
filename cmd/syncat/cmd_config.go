package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/nickmarrone/syncat/internal/config"
)

// configField describes one top-level scalar config.json field settable via
// `syncat config set`. Collection fields (peers, shares, subscriptions,
// global_ignores) have their own dedicated commands and are not listed
// here.
type configField struct {
	set func(cfg *config.Config, value string) error
	get func(cfg *config.Config) string
}

var configFields = map[string]configField{
	"node_name": {
		set: func(cfg *config.Config, value string) error { cfg.NodeName = value; return nil },
		get: func(cfg *config.Config) string { return cfg.NodeName },
	},
	"api_addr": {
		set: func(cfg *config.Config, value string) error { cfg.APIAddr = value; return nil },
		get: func(cfg *config.Config) string { return cfg.APIAddr },
	},
	"trash_retention_days": {
		set: func(cfg *config.Config, value string) error {
			n, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("trash_retention_days must be an integer: %w", err)
			}
			cfg.TrashRetentionDays = n
			return nil
		},
		get: func(cfg *config.Config) string { return strconv.Itoa(cfg.TrashRetentionDays) },
	},
	"rescan_interval_seconds": {
		set: func(cfg *config.Config, value string) error {
			n, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("rescan_interval_seconds must be an integer: %w", err)
			}
			cfg.RescanIntervalSeconds = n
			return nil
		},
		get: func(cfg *config.Config) string { return strconv.Itoa(cfg.RescanIntervalSeconds) },
	},
	"debug": {
		set: func(cfg *config.Config, value string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("debug must be a boolean (true/false): %w", err)
			}
			cfg.Debug = b
			return nil
		},
		get: func(cfg *config.Config) string { return strconv.FormatBool(cfg.Debug) },
	},
}

// cmdConfig implements `syncat config set FIELD VALUE` (SPEC.md §8-adjacent:
// direct field edits for the scalar settings in config.json — peers,
// shares, and subscriptions keep their own add/ls/rm/set commands). It
// edits config.json directly rather than going through the API, since some
// fields (e.g. debug) only take effect on the next `syncat daemon` start
// and there's no requirement that the daemon be running to change them.
func cmdConfig(paths *config.Paths, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: syncat config set FIELD VALUE")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "set":
		return cmdConfigSet(paths, rest)
	default:
		return fmt.Errorf("unknown config subcommand %q (want set)", sub)
	}
}

func cmdConfigSet(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("config set", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: syncat config set FIELD VALUE\n\nFIELD is one of: %s", knownConfigFields())
	}
	field, value := fs.Arg(0), fs.Arg(1)

	cf, ok := configFields[field]
	if !ok {
		return fmt.Errorf("unknown config field %q (want one of: %s)", field, knownConfigFields())
	}

	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no config found at %s (run `syncat init` first): %w", paths.ConfigFile(), err)
		}
		return err
	}

	if err := cf.set(cfg, value); err != nil {
		return err
	}

	if err := config.Save(paths.ConfigFile(), cfg); err != nil {
		return err
	}

	fmt.Printf("%s = %s\n", field, cf.get(cfg))
	return nil
}

func knownConfigFields() string {
	names := make([]string, 0, len(configFields))
	for name := range configFields {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}
