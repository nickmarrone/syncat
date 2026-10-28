package api

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
)

func TestSafetyPatchChangesOnlyGivenFields(t *testing.T) {
	srv, node, _ := newTestServer(t)
	if _, err := node.AddShare(t.TempDir(), "docs", config.PermissionReadWrite, false); err != nil {
		t.Fatal(err)
	}

	w := doRequest(t, srv, http.MethodPatch, "/api/shares/docs/safety", testToken, map[string]any{"hold_delete_percent": 30})
	if w.Code != http.StatusOK {
		t.Fatalf("PATCH safety: %d %s", w.Code, w.Body)
	}
	var got safetyDTO
	decodeJSONBody(t, w, &got)
	want := safetyDTO{30, config.DefaultHoldDeleteMin, config.DefaultTrashRetentionDays, config.DefaultTrashMaxSize}
	if got != want {
		t.Errorf("after first PATCH = %+v, want %+v", got, want)
	}

	w = doRequest(t, srv, http.MethodPatch, "/api/shares/docs/safety", testToken, map[string]any{"trash_max_size": "2GiB"})
	decodeJSONBody(t, w, &got)
	if got.HoldDeletePercent != 30 || got.TrashMaxSize != "2GiB" {
		t.Errorf("second PATCH lost the first one's change: %+v", got)
	}

	w = doRequest(t, srv, http.MethodPatch, "/api/shares/docs/safety", testToken, map[string]any{"trash_max_size": "lots"})
	if w.Code != http.StatusBadRequest {
		t.Errorf("PATCH with a bad size: %d, want 400", w.Code)
	}
}

func TestHoldConfirmEndpoint(t *testing.T) {
	srv, node, _ := newTestServer(t)
	dir := t.TempDir()
	for i := 0; i < 30; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%02d", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := node.AddShare(dir, "docs", config.PermissionReadWrite, false); err != nil {
		t.Fatal(err)
	}

	w := doRequest(t, srv, http.MethodPost, "/api/shares/docs/holds/confirm", testToken, map[string]string{})
	if w.Code != http.StatusNotFound {
		t.Errorf("confirm with no hold: %d, want 404", w.Code)
	}

	for i := 0; i < 25; i++ {
		if err := os.Remove(filepath.Join(dir, fmt.Sprintf("f%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// The share's watcher notices on its own.
	deadline := time.Now().Add(10 * time.Second)
	for len(node.Holds()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no hold placed after deleting 25 of 30 files")
		}
		time.Sleep(50 * time.Millisecond)
	}

	var st statusResponse
	decodeJSONBody(t, doRequest(t, srv, http.MethodGet, "/api/status", testToken, nil), &st)
	if len(st.Holds) != 1 || st.Holds[0].Deletes != 25 || !st.Holds[0].Confirmable {
		t.Fatalf("status holds = %+v, want one confirmable hold of 25", st.Holds)
	}

	w = doRequest(t, srv, http.MethodPost, "/api/shares/docs/holds/confirm", testToken, map[string]string{})
	if w.Code != http.StatusOK {
		t.Fatalf("confirm: %d %s", w.Code, w.Body)
	}
	if holds := node.Holds(); len(holds) != 0 {
		t.Errorf("holds after confirm = %+v, want none", holds)
	}
}
