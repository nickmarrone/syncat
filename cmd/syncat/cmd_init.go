package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/nickmarrone/syncat/internal/config"
)

// cmdInit implements `syncat init [--name NAME]`. It is idempotent: running
// it again never regenerates existing keys, the api token, or an existing
// config.toml's node name.
func cmdInit(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	name := fs.String("name", "", "display name for this node (default: hostname)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if err := paths.EnsureDirs(); err != nil {
		return err
	}

	cfgPath := paths.ConfigFile()
	cfg, err := config.Load(cfgPath)
	switch {
	case err == nil:
		// Config already exists: leave its node name alone so init stays
		// idempotent even if --name is passed differently on a re-run.
	case errors.Is(err, os.ErrNotExist):
		nodeName := *name
		if nodeName == "" {
			nodeName, err = os.Hostname()
			if err != nil {
				return fmt.Errorf("determine hostname: %w", err)
			}
		}
		cfg = config.Default()
		cfg.NodeName = nodeName
		if err := config.Save(cfgPath, cfg); err != nil {
			return err
		}
	default:
		return err
	}

	idKey, idCreated, err := config.LoadOrCreateIdentityKey(paths.IdentityKeyFile())
	if err != nil {
		return err
	}

	tcKey, tcCreated, err := config.LoadOrCreateTailcatKey(context.Background(), paths.TailcatKeyFile())
	if err != nil {
		return err
	}

	if _, err := config.LoadOrCreateAPIToken(paths.APITokenFile()); err != nil {
		return err
	}

	fmt.Printf("node name:     %s\n", cfg.NodeName)
	fmt.Printf("short id:      %s\n", idKey.ShortID())
	fmt.Printf("config dir:    %s\n", paths.ConfigDir)
	fmt.Printf("data dir:      %s\n", paths.DataDir)
	if idCreated {
		fmt.Println("identity key:  generated")
	} else {
		fmt.Println("identity key:  already present")
	}
	if tcCreated {
		fmt.Printf("tailcat key:   generated (DERP region %d)\n", tcKey.Public.RegionID)
	} else {
		fmt.Println("tailcat key:   already present")
	}
	fmt.Println()
	fmt.Println("run `syncat token` to print this node's connection token.")
	return nil
}
