package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
)

// apiTokenBytes is the number of random bytes in api.token, hex-encoded to
// 64 characters (SPEC.md §3).
const apiTokenBytes = 32

// LoadOrCreateAPIToken loads the REST API auth token from path, generating
// and persisting a new random 64-hex-character token (mode 0600) if none
// exists. Regenerating is a no-op if a valid token is already present.
func LoadOrCreateAPIToken(path string) (string, error) {
	if data, err := os.ReadFile(path); err == nil {
		tok := strings.TrimSpace(string(data))
		if _, err := hex.DecodeString(tok); err != nil || len(tok) != apiTokenBytes*2 {
			return "", fmt.Errorf("config: api token %s is malformed (want %d hex chars)", path, apiTokenBytes*2)
		}
		return tok, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("config: read api token %s: %w", path, err)
	}

	raw := make([]byte, apiTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("config: generate api token: %w", err)
	}
	tok := hex.EncodeToString(raw)
	if err := writeFileAtomic(path, []byte(tok), 0600); err != nil {
		return "", fmt.Errorf("config: save api token %s: %w", path, err)
	}
	return tok, nil
}
