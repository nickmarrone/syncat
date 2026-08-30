package transport

import (
	"context"
	"net"
)

// Transport abstracts peer connectivity (SPEC.md §10) so the sync core and
// everything above it never talks to tailcat, or any other carrier,
// directly. [TailcatTransport] is the production implementation; a
// [PipeTransport] backed by an in-memory connection stands in for it in
// tests. A future mobile-native carrier need only satisfy this interface.
//
// Per SPEC.md §4, one Transport connection (whether returned by Dial or
// handed to onConn by Start) carries exactly one syncat protocol stream —
// framing, handshake, and message types are Phase 3's concern, not this
// package's.
type Transport interface {
	// Start begins accepting inbound connections, invoking onConn in a new
	// goroutine for each one accepted. It returns once the transport is
	// ready to accept connections; onConn keeps being called, from
	// background goroutines, until the transport is closed.
	Start(ctx context.Context, onConn func(net.Conn)) error

	// Dial opens a connection to the peer identified by addr — the "tc"
	// field of that peer's sc1 token (SPEC.md §2) for [TailcatTransport],
	// or a registry key for [PipeTransport].
	Dial(ctx context.Context, addr string) (net.Conn, error)

	// LocalAddress returns this node's address blob, suitable for
	// embedding as the "tc" field of our own sc1 token. It is only valid
	// after Start has returned successfully.
	LocalAddress() (string, error)

	// Close shuts down the transport: it stops accepting new connections
	// and releases background resources. It does not forcibly close
	// connections already handed to onConn or returned by Dial — callers
	// own those and must close them separately. Close is safe to call more
	// than once, and safe to call without a prior Start.
	Close() error
}
