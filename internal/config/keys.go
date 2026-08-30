package config

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tailscale/tailcat"
)

// IdentityKey is the node's Ed25519 application identity keypair (SPEC.md
// §2), used to sign the syncat protocol handshake independent of whatever
// the tailcat transport itself authenticates.
//
// On-disk format (identity.key, mode 0600): the 64-byte Ed25519 private key
// (seed || public key, as returned by crypto/ed25519), hex-encoded, with a
// trailing newline. This is deliberately the simplest possible encoding;
// the public key is derived from it rather than stored separately.
type IdentityKey struct {
	Private ed25519.PrivateKey
}

// Public returns the Ed25519 public key.
func (k *IdentityKey) Public() ed25519.PublicKey {
	return k.Private.Public().(ed25519.PublicKey)
}

// ShortID returns the first 8 bytes of the Ed25519 public key, hex-encoded.
// This is the node id used as a version-vector key (SPEC.md §5).
func (k *IdentityKey) ShortID() string {
	pub := k.Public()
	return hex.EncodeToString(pub[:8])
}

// LoadIdentityKey reads an existing identity key from path. It returns an
// error wrapping os.ErrNotExist if the file doesn't exist.
func LoadIdentityKey(path string) (*IdentityKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read identity key %s: %w", path, err)
	}
	return parseIdentityKey(data)
}

// LoadOrCreateIdentityKey loads the identity key at path, generating and
// persisting a new one if none exists. created reports whether a new key
// was generated.
func LoadOrCreateIdentityKey(path string) (key *IdentityKey, created bool, err error) {
	if data, readErr := os.ReadFile(path); readErr == nil {
		key, err = parseIdentityKey(data)
		return key, false, err
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, false, fmt.Errorf("config: read identity key %s: %w", path, readErr)
	}

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, false, fmt.Errorf("config: generate identity key: %w", err)
	}
	key = &IdentityKey{Private: priv}
	if err := saveIdentityKey(path, key); err != nil {
		return nil, false, err
	}
	return key, true, nil
}

func parseIdentityKey(data []byte) (*IdentityKey, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, fmt.Errorf("config: decode identity key: %w", err)
	}
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("config: identity key has %d bytes, want %d", len(raw), ed25519.PrivateKeySize)
	}
	return &IdentityKey{Private: ed25519.PrivateKey(raw)}, nil
}

func saveIdentityKey(path string, key *IdentityKey) error {
	encoded := hex.EncodeToString(key.Private) + "\n"
	if err := writeFileAtomic(path, []byte(encoded), 0600); err != nil {
		return fmt.Errorf("config: save identity key %s: %w", path, err)
	}
	return nil
}

// LoadTailcatKey reads an existing tailcat saved key from path. It returns
// an error wrapping os.ErrNotExist if the file doesn't exist.
func LoadTailcatKey(path string) (*tailcat.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read tailcat key %s: %w", path, err)
	}
	var priv tailcat.PrivateKey
	if err := json.Unmarshal(data, &priv); err != nil {
		return nil, fmt.Errorf("config: parse tailcat key %s: %w", path, err)
	}
	return &priv, nil
}

// LoadOrCreateTailcatKey loads the tailcat saved key at path, generating and
// persisting a new one if none exists. created reports whether a new key
// was generated.
//
// Generating a key requires resolving a concrete DERP relay region once
// (network access) and baking it into PrivateKey.Public.RegionID. This
// deviates from leaving RegionID at tailcat's "auto" sentinel (-1): tailcat
// re-runs a latency netcheck on every server start when RegionID is -1,
// which can select a different region each run and would make `syncat
// token` print a different token on every invocation. Resolving once at
// init time and persisting the concrete RegionID keeps the token stable for
// the life of the key, matching cmd/tailcat's `genkey --fixed-region`.
func LoadOrCreateTailcatKey(ctx context.Context, path string) (key *tailcat.PrivateKey, created bool, err error) {
	if data, readErr := os.ReadFile(path); readErr == nil {
		var priv tailcat.PrivateKey
		if err := json.Unmarshal(data, &priv); err != nil {
			return nil, false, fmt.Errorf("config: parse tailcat key %s: %w", path, err)
		}
		return &priv, false, nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, false, fmt.Errorf("config: read tailcat key %s: %w", path, readErr)
	}

	priv := tailcat.NewPrivateKey()

	fetchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	dm, err := tailcat.FetchDERPMap(fetchCtx, tailcat.ExpandForServer)
	if err != nil {
		return nil, false, fmt.Errorf("config: fetch DERP map: %w", err)
	}
	regionID, err := tailcat.PickBestRegion(fetchCtx, dm)
	if err != nil {
		return nil, false, fmt.Errorf("config: pick nearest DERP region: %w", err)
	}
	if regionID == 0 {
		return nil, false, errors.New("config: could not determine nearest DERP region (no usable latency measurements)")
	}
	priv.Public.RegionID = regionID

	data, err := json.MarshalIndent(priv, "", "\t")
	if err != nil {
		return nil, false, fmt.Errorf("config: encode tailcat key: %w", err)
	}
	if err := writeFileAtomic(path, data, 0600); err != nil {
		return nil, false, fmt.Errorf("config: save tailcat key %s: %w", path, err)
	}
	return priv, true, nil
}
