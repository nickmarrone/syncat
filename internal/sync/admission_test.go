package sync

import (
	"context"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/protocol"
)

func TestShareAdmissionDirections(t *testing.T) {
	s := &Session{shares: make(map[string]*ShareConfig)}
	s.AddShare(ShareConfig{ShareID: "rw"})
	s.AddShare(ShareConfig{ShareID: "read-only", Direction: readOnlyDir()})
	s.AddShare(ShareConfig{ShareID: "receive-only", Direction: receiveOnlyDir()})

	if !s.acceptsInboundIndex("rw") || !s.servesOutboundShare("rw") {
		t.Fatal("read-write share should admit traffic in both directions")
	}
	if s.acceptsInboundIndex("read-only") || !s.servesOutboundShare("read-only") {
		t.Fatal("read-only offer should reject inbound index state and serve outbound state")
	}
	if !s.acceptsInboundIndex("receive-only") || s.servesOutboundShare("receive-only") {
		t.Fatal("receive-only subscription should accept inbound state and reject outbound requests")
	}
	if s.acceptsInboundIndex("unknown") || s.servesOutboundShare("unknown") {
		t.Fatal("an unknown share must not be authorized by its wire ID")
	}
}

func TestUnauthorizedSnapshotAndAckDoNotCreateState(t *testing.T) {
	a, b := newTestNode(t, "admissionA"), newTestNode(t, "admissionB")
	sa, sb := connectSessions(t, a, b, Direction{}, Direction{})

	processed := make(chan struct{}, 1)
	sb.SetControlHandler(func(typ protocol.MsgType, _ []byte) {
		if typ == protocol.MsgPing {
			processed <- struct{}{}
		}
	})
	const unknown = "advertised-but-ungranted"
	if err := sa.writer.WriteMessage(protocol.MsgIndexSnapshotBegin, protocol.IndexSnapshotBegin{ShareID: unknown, SnapshotID: "forged", Epoch: "evil", HighSeq: 99}); err != nil {
		t.Fatal(err)
	}
	if err := sa.writer.WriteMessage(protocol.MsgIndexAck, protocol.IndexAck{ShareID: unknown, Epoch: "evil", AppliedSeq: 99}); err != nil {
		t.Fatal(err)
	}
	if err := sa.writer.WriteMessage(protocol.MsgPing, protocol.Ping{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-processed:
	case <-time.After(2 * time.Second):
		t.Fatal("session did not process test frames")
	}

	c, err := b.store.Cursor(context.Background(), a.id, unknown, "incoming")
	if err != nil {
		t.Fatal(err)
	}
	if c.Epoch != "" || c.AppliedSeq != 0 || c.SnapshotID != "" || c.SnapshotBatch != 0 {
		t.Fatalf("unauthorized snapshot created incoming cursor: %+v", c)
	}
	out, err := b.store.Cursor(context.Background(), a.id, unknown, "outgoing")
	if err != nil {
		t.Fatal(err)
	}
	if out.Epoch != "" || out.AppliedSeq != 0 || out.SnapshotID != "" || out.SnapshotBatch != 0 {
		t.Fatalf("unauthorized acknowledgement created outgoing cursor: %+v", out)
	}
}

func TestAnswerSyncRequestRejectsUnknownAndReceiveOnlyShares(t *testing.T) {
	n := newTestNode(t, "answer")
	s := NewSession(nil, n.store, n.id, "peer", nil, nil)
	s.AddShare(ShareConfig{ShareID: "receive-only", Root: n.root, Direction: receiveOnlyDir()})

	for _, shareID := range []string{"unknown", "receive-only"} {
		if err := s.answerSyncRequest(context.Background(), protocol.IndexSyncRequest{ShareID: shareID}); err == nil {
			t.Fatalf("answerSyncRequest(%q) succeeded, want authorization failure", shareID)
		}
	}
}
