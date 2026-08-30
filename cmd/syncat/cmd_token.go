package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/nickmarrone/syncat/internal/config"
)

// cmdToken implements `syncat token`. It derives the tailcat ConnBlob from
// the persisted key's already-resolved DERP region (PrivateKey.Public),
// rather than starting a tailcat.Server, since the daemon isn't implemented
// yet in this phase — and because the region was already baked in at
// `syncat init` time specifically so this needs no network access and is
// stable across invocations.
func cmdToken(paths *config.Paths, args []string) error {
	fs := flag.NewFlagSet("token", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(paths.ConfigFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no config found at %s (run `syncat init` first): %w", paths.ConfigFile(), err)
		}
		return err
	}

	idKey, err := config.LoadIdentityKey(paths.IdentityKeyFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no identity key found (run `syncat init` first): %w", err)
		}
		return err
	}

	tcKey, err := config.LoadTailcatKey(paths.TailcatKeyFile())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no tailcat key found (run `syncat init` first): %w", err)
		}
		return err
	}

	connBlob := tcKey.Public.ConnBlob()
	tok, err := config.EncodeToken(string(connBlob), idKey.Public(), cfg.NodeName)
	if err != nil {
		return err
	}

	fmt.Println(tok)
	return nil
}
