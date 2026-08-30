package sync

import (
	"math/rand"
	"testing"

	"github.com/nickmarrone/syncat/internal/protocol"
)

// vv is a small test helper for building a VersionVector literal:
// vv("a", 1, "b", 2) == protocol.VersionVector{"a": 1, "b": 2}.
func vv(pairs ...any) protocol.VersionVector {
	v := protocol.VersionVector{}
	for i := 0; i < len(pairs); i += 2 {
		v[pairs[i].(string)] = uint64(pairs[i+1].(int))
	}
	return v
}

func TestEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b protocol.VersionVector
		want bool
	}{
		{"both empty", vv(), vv(), true},
		{"nil vs empty", nil, vv(), true},
		{"nil vs nil", nil, nil, true},
		{"identical single key", vv("a", 1), vv("a", 1), true},
		{"identical multi key", vv("a", 1, "b", 2), vv("a", 1, "b", 2), true},
		{"different value", vv("a", 1), vv("a", 2), false},
		{"missing key as zero, equal", vv("a", 0), vv(), true},
		{"missing key as zero, not equal", vv("a", 1), vv(), false},
		{"disjoint keys, both nonzero", vv("a", 1), vv("b", 1), false},
		{"extra zero key still equal", vv("a", 1, "b", 0), vv("a", 1), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Equal(c.a, c.b); got != c.want {
				t.Errorf("Equal(a, b) = %v, want %v", got, c.want)
			}
			if got := Equal(c.b, c.a); got != c.want {
				t.Errorf("Equal(b, a) = %v, want %v", got, c.want)
			}
		})
	}
}

func TestDominates(t *testing.T) {
	cases := []struct {
		name       string
		a, b       protocol.VersionVector
		wantAOverB bool
		wantBOverA bool
	}{
		{"equal vectors", vv("a", 1), vv("a", 1), false, false},
		{"equal empty", vv(), vv(), false, false},
		{"strict single key a>b", vv("a", 2), vv("a", 1), true, false},
		{"strict multi key a>b all keys ahead", vv("a", 2, "b", 3), vv("a", 1, "b", 2), true, false},
		{"a dominates via missing key treated as zero", vv("a", 1), vv(), true, false},
		{"empty dominated by nonzero", vv(), vv("a", 1), false, true},
		{"disjoint keys: neither dominates", vv("a", 1), vv("b", 1), false, false},
		{"partial overlap, mixed: neither dominates", vv("a", 2, "b", 1), vv("a", 1, "c", 1), false, false},
		{"a ahead on shared key, b has no extra keys", vv("a", 2, "b", 1), vv("a", 1, "b", 1), true, false},
		{"equal on shared key, a has an extra nonzero key", vv("a", 1, "b", 1), vv("a", 1), true, false},
		{"equal on shared key, a has an extra zero key -> equal not dominates", vv("a", 1, "b", 0), vv("a", 1), false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Dominates(c.a, c.b); got != c.wantAOverB {
				t.Errorf("Dominates(a, b) = %v, want %v", got, c.wantAOverB)
			}
			if got := Dominates(c.b, c.a); got != c.wantBOverA {
				t.Errorf("Dominates(b, a) = %v, want %v", got, c.wantBOverA)
			}
		})
	}
}

func TestConcurrent(t *testing.T) {
	cases := []struct {
		name string
		a, b protocol.VersionVector
		want bool
	}{
		{"equal", vv("a", 1), vv("a", 1), false},
		{"a dominates b", vv("a", 2), vv("a", 1), false},
		{"b dominates a", vv("a", 1), vv("a", 2), false},
		{"disjoint keys, both nonzero", vv("a", 1), vv("b", 1), true},
		{"partial overlap, mixed direction", vv("a", 2, "b", 1), vv("a", 1, "b", 2), true},
		{"empty vs empty", vv(), vv(), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Concurrent(c.a, c.b); got != c.want {
				t.Errorf("Concurrent(a, b) = %v, want %v", got, c.want)
			}
			if got := Concurrent(c.b, c.a); got != c.want {
				t.Errorf("Concurrent(b, a) = %v, want %v", got, c.want)
			}
		})
	}
}

func TestMergeBasic(t *testing.T) {
	got := Merge(vv("a", 1, "b", 5), vv("a", 3, "c", 2))
	want := vv("a", 3, "b", 5, "c", 2)
	if !Equal(got, want) {
		t.Errorf("Merge = %v, want %v", got, want)
	}
}

func TestMergeDoesNotMutateInputs(t *testing.T) {
	a := vv("a", 1)
	b := vv("a", 2)
	_ = Merge(a, b)
	if a["a"] != 1 || b["a"] != 2 {
		t.Fatalf("Merge mutated an input: a=%v b=%v", a, b)
	}
}

func TestBump(t *testing.T) {
	v := vv("a", 1)
	got := Bump(v, "a")
	if got["a"] != 2 {
		t.Errorf("Bump existing key: got %v, want counter 2", got)
	}
	if v["a"] != 1 {
		t.Fatalf("Bump mutated its input: %v", v)
	}

	got2 := Bump(v, "b")
	if got2["a"] != 1 || got2["b"] != 1 {
		t.Errorf("Bump new key: got %v, want {a:1, b:1}", got2)
	}

	gotNil := Bump(nil, "a")
	if gotNil["a"] != 1 {
		t.Errorf("Bump(nil, ...) = %v, want {a:1}", gotNil)
	}
}

// --- Randomized property checks -------------------------------------------

func randVector(r *rand.Rand, nodes []string) protocol.VersionVector {
	v := protocol.VersionVector{}
	for _, n := range nodes {
		if r.Intn(3) == 0 {
			continue // leave the key absent sometimes, to exercise implicit-zero
		}
		v[n] = uint64(r.Intn(5))
	}
	return v
}

func TestVectorProperties(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	r := rand.New(rand.NewSource(42))

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		a := randVector(r, nodes)
		b := randVector(r, nodes)
		c := randVector(r, nodes)

		// Merge is commutative.
		if !Equal(Merge(a, b), Merge(b, a)) {
			t.Fatalf("Merge not commutative for a=%v b=%v", a, b)
		}

		// Merge is associative.
		lhs := Merge(Merge(a, b), c)
		rhs := Merge(a, Merge(b, c))
		if !Equal(lhs, rhs) {
			t.Fatalf("Merge not associative for a=%v b=%v c=%v: (a∘b)∘c=%v a∘(b∘c)=%v", a, b, c, lhs, rhs)
		}

		// Merge(a, b) dominates-or-equals both inputs.
		m := Merge(a, b)
		if !(Equal(m, a) || Dominates(m, a)) {
			t.Fatalf("Merge(a,b)=%v does not dominate-or-equal a=%v", m, a)
		}
		if !(Equal(m, b) || Dominates(m, b)) {
			t.Fatalf("Merge(a,b)=%v does not dominate-or-equal b=%v", m, b)
		}

		// Exactly one of Equal(a,b) / Dominates(a,b) / Dominates(b,a) /
		// Concurrent(a,b) holds.
		flags := 0
		if Equal(a, b) {
			flags++
		}
		if Dominates(a, b) {
			flags++
		}
		if Dominates(b, a) {
			flags++
		}
		if Concurrent(a, b) {
			flags++
		}
		if flags != 1 {
			t.Fatalf("expected exactly one relation to hold for a=%v b=%v, got %d: eq=%v a>b=%v b>a=%v conc=%v",
				a, b, flags, Equal(a, b), Dominates(a, b), Dominates(b, a), Concurrent(a, b))
		}
	}
}
