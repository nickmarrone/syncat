package transport

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func TestKeepConnectionTable(t *testing.T) {
	pad := func(b ...byte) ed25519.PublicKey {
		out := make(ed25519.PublicKey, ed25519.PublicKeySize)
		copy(out, b)
		return out
	}
	low := pad(0x01, 0x02)
	high := pad(0x01, 0x03)

	cases := []struct {
		name        string
		local, peer ed25519.PublicKey
		dialed      bool
		want        bool
	}{
		{"local higher key, its outbound connection is kept", high, low, true, true},
		{"local higher key, its inbound connection is not kept", high, low, false, false},
		{"local lower key, its outbound connection is not kept", low, high, true, false},
		{"local lower key, its inbound connection is kept", low, high, false, true},
		{"equal keys, outbound not kept", low, low, true, false},
		{"equal keys, inbound kept", low, low, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := KeepConnection(c.local, c.peer, c.dialed)
			if got != c.want {
				t.Errorf("KeepConnection(local=%x, peer=%x, dialed=%v) = %v, want %v",
					c.local, c.peer, c.dialed, got, c.want)
			}
		})
	}
}

// TestKeepConnectionSymmetricAndTotal generates many random distinct key
// pairs and checks the dedup rule is:
//   - symmetric: both endpoints of a given physical connection agree on
//     whether it survives, despite computing the answer independently from
//     their own (local, peer, dialed) viewpoint;
//   - total: between the two possible duplicate connections for a pair
//     (A's dial of B, and B's dial of A), exactly one survives.
func TestKeepConnectionSymmetricAndTotal(t *testing.T) {
	newKey := func(t *testing.T) ed25519.PublicKey {
		t.Helper()
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		return pub
	}

	for i := 0; i < 200; i++ {
		a := newKey(t)
		b := newKey(t)

		// Connection dialed by A, accepted by B.
		aView := KeepConnection(a, b, true)
		bView := KeepConnection(b, a, false)
		if aView != bView {
			t.Fatalf("A-dialed connection: A says kept=%v, B says kept=%v (a=%x b=%x)", aView, bView, a, b)
		}

		// Connection dialed by B, accepted by A.
		bView2 := KeepConnection(b, a, true)
		aView2 := KeepConnection(a, b, false)
		if bView2 != aView2 {
			t.Fatalf("B-dialed connection: B says kept=%v, A says kept=%v (a=%x b=%x)", bView2, aView2, a, b)
		}

		// Totality: of the two possible duplicate connections, exactly
		// one survives.
		if aView == bView2 {
			t.Fatalf("neither or both connections kept: A-dialed=%v B-dialed=%v (a=%x b=%x)", aView, bView2, a, b)
		}
	}
}
