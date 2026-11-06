package core

import (
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
)

// Denying a pending peer drops it from the queue and from config.json, and
// gives it no session. Deny is not a block list: the node is queued again
// if it keeps dialing (MANUAL-TESTS §2).
func TestUnknownPeerDenied(t *testing.T) {
	nodeA := newTestNode(t, "denyA")
	nodeB := newTestNode(t, "denyB")

	if _, err := nodeB.AddPeer("A", peerToken(t, nodeA)); err != nil {
		t.Fatalf("nodeB AddPeer: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return len(nodeA.Approvals()) == 1 })
	approval := nodeA.Approvals()[0]
	if approval.Kind != ApprovalPeer || approval.PeerKey != nodeB.PeerKey() {
		t.Fatalf("pending approval = %+v", approval)
	}

	if err := nodeA.DecideApproval(approval.ID, "deny"); err != nil {
		t.Fatalf("deny pending peer: %v", err)
	}
	if got := nodeA.Approvals(); len(got) != 0 {
		// It may already be back if nodeB redialed; it must not be connected either way.
		for _, a := range got {
			if a.ID != approval.ID {
				t.Fatalf("unexpected approval after deny: %+v", a)
			}
		}
	}
	if len(nodeA.Status().Peers) != 0 {
		t.Fatalf("a denied peer was added as a peer: %+v", nodeA.Status().Peers)
	}
	persisted, err := config.Load(nodeA.paths.ConfigFile())
	if err != nil {
		t.Fatal(err)
	}
	if len(persisted.Peers) != 0 {
		t.Errorf("config.json has peers after a deny: %+v", persisted.Peers)
	}
	if peerConnected(nodeA) {
		t.Error("nodeA is connected to a peer it denied")
	}

	// Deciding an id that is no longer pending is a not-found error, not a crash.
	if err := nodeA.DecideApproval("peer-0000000000000000", "deny"); err == nil {
		t.Error("denying an id that was never pending succeeded")
	}
	if err := nodeA.DecideApproval(approval.ID, "maybe"); err == nil {
		t.Error("an invalid decision was accepted")
	}
}
