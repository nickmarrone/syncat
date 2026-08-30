package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// NewShareID returns a random 8-byte hex-encoded (16 character) share id,
// generated at share creation and stable for the share's life (SPEC.md §3).
func NewShareID() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("config: generate share id: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
