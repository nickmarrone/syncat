package main

import (
	"strings"
	"testing"
	"time"
)

// describeHold shows the daemon's own words for a hold, and offers the
// confirm command only when there is something to confirm.
func TestDescribeHold(t *testing.T) {
	since := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	missing := describeHold(holdView{
		ShareName: "docs", Reason: "root-unavailable", Since: since,
		Problem: "the folder is missing or unreadable", Remedy: "restore it, or remove the share",
	})
	if !strings.Contains(missing, "docs: on hold since") || !strings.Contains(missing, "the folder is missing or unreadable (restore it, or remove the share)") {
		t.Errorf("unconfirmable hold = %q", missing)
	}
	if strings.Contains(missing, "confirm-deletes") {
		t.Errorf("unconfirmable hold offers confirm-deletes: %q", missing)
	}

	bulk := describeHold(holdView{
		ShareName: "docs", PeerKey: "aaaa", PeerName: "laptop", Reason: "bulk-delete", Since: since, Confirmable: true,
		Problem: "22 of 25 files would be deleted", Remedy: "fix the folder, or let the deletes through",
	})
	want := "changes from laptop on hold since"
	if !strings.Contains(bulk, want) || !strings.Contains(bulk, "22 of 25 files would be deleted (fix the folder, or let the deletes through: `syncat share confirm-deletes docs --peer laptop`)") {
		t.Errorf("confirmable peer hold = %q", bulk)
	}
}
