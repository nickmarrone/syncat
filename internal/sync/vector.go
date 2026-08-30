package sync

import "github.com/nickmarrone/syncat/internal/protocol"

// This file implements the version-vector algebra SPEC.md §5 rests on:
// Dominates, Concurrent, Equal, Merge, and Bump over protocol.VersionVector
// (map[string]uint64, keyed by node-short-id).
//
// Missing-key convention: a key absent from a VersionVector is an implicit
// zero counter, exactly as Go's map indexing already treats it (v[k] on a
// missing k yields the zero value with no separate "ok" check needed). Every
// function below relies on this directly rather than special-casing missing
// keys, so a nil VersionVector behaves identically to an empty one, and a
// vector that happens to store an explicit 0 for some key is indistinguishable
// from that key being absent. Bump never removes keys and Merge never invents
// zero entries, so in practice explicit zeros never appear — but the
// functions here are correct either way.

// Equal reports whether a and b represent the same version vector, treating
// a missing key in either as an implicit zero.
func Equal(a, b protocol.VersionVector) bool {
	for k, av := range a {
		if b[k] != av {
			return false
		}
	}
	for k, bv := range b {
		if a[k] != bv {
			return false
		}
	}
	return true
}

// Dominates reports whether a strictly dominates b: for every node key that
// appears in either vector, a's counter is >= b's, and at least one is
// strictly greater. The empty vector is dominated by any non-empty vector
// and dominates nothing (Dominates(empty, empty) is false, matching Equal).
func Dominates(a, b protocol.VersionVector) bool {
	strict := false
	for k := range a {
		av, bv := a[k], b[k]
		if av < bv {
			return false
		}
		if av > bv {
			strict = true
		}
	}
	for k := range b {
		if _, ok := a[k]; ok {
			continue // already compared above
		}
		if a[k] < b[k] { // a[k] is the implicit zero here
			return false
		}
	}
	return strict
}

// Concurrent reports whether a and b are neither equal nor ordered by
// Dominates in either direction. For any pair of vectors, exactly one of
// Equal(a,b), Dominates(a,b), Dominates(b,a), Concurrent(a,b) holds — see
// vector_test.go's property check.
func Concurrent(a, b protocol.VersionVector) bool {
	return !Equal(a, b) && !Dominates(a, b) && !Dominates(b, a)
}

// Merge returns the element-wise maximum of a and b over the union of their
// keys. Merge is commutative and associative, and Merge(a, b) always
// dominates-or-equals both a and b. Merge never mutates a or b.
func Merge(a, b protocol.VersionVector) protocol.VersionVector {
	out := make(protocol.VersionVector, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		if v > out[k] {
			out[k] = v
		}
	}
	return out
}

// Bump returns a copy of v with nodeID's counter incremented by one,
// recording a local modification by nodeID (SPEC.md §5). v itself is never
// mutated, since callers may still hold a reference to it (e.g. as a row's
// stored version) that must not change out from under them.
func Bump(v protocol.VersionVector, nodeID string) protocol.VersionVector {
	out := make(protocol.VersionVector, len(v)+1)
	for k, val := range v {
		out[k] = val
	}
	out[nodeID] = out[nodeID] + 1
	return out
}
