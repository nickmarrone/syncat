package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAPITokenIsHex64AndPrivateMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "api.token")

	tok, err := LoadOrCreateAPIToken(path)
	if err != nil {
		t.Fatalf("LoadOrCreateAPIToken: %v", err)
	}
	if len(tok) != 64 {
		t.Errorf("token length = %d, want 64", len(tok))
	}
	for _, r := range tok {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			t.Errorf("token %q contains non-hex character %q", tok, r)
			break
		}
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("api.token mode = %o, want 0600", perm)
	}
}

func TestAPITokenRegenerateIsNoOp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "api.token")

	tok1, err := LoadOrCreateAPIToken(path)
	if err != nil {
		t.Fatalf("LoadOrCreateAPIToken (first): %v", err)
	}
	tok2, err := LoadOrCreateAPIToken(path)
	if err != nil {
		t.Fatalf("LoadOrCreateAPIToken (second): %v", err)
	}
	if tok1 != tok2 {
		t.Errorf("token changed across reload: %s != %s", tok1, tok2)
	}
}
