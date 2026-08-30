// Package protocol implements the syncat wire format: length-prefixed
// frames (frame.go), CBOR message types (message.go), the
// mutually-authenticated Ed25519 handshake (handshake.go), and the
// Ping/Pong idle/dead-connection timing logic (keepalive.go) —
// SPEC.md §4.
//
// This package has no filesystem, UI, or HTTP dependencies (SPEC.md §12:
// it must stay gomobile-safe) and is testable entirely over in-memory
// connections; see internal/transport's PipeTransport for the in-memory
// Transport used by this package's own tests.
package protocol
