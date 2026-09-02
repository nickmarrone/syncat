// Package transport abstracts peer connectivity behind a Transport
// interface, with a tailcat implementation for production (tailcat.go)
// and an in-memory pipe implementation for tests (pipe.go) — SPEC.md §10.
// It also owns the reconnect schedule (Backoff/Supervisor) and SPEC.md
// §2.4's duplicate-connection tie-break (KeepConnection).
package transport

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"math"
	"math/rand"
	"net"
	"time"
)

// Transport abstracts peer connectivity (SPEC.md §10) so the sync core and
// everything above it never talks to tailcat, or any other carrier,
// directly. [TailcatTransport] is the production implementation; a
// [PipeTransport] backed by an in-memory connection stands in for it in
// tests. A future mobile-native carrier need only satisfy this interface.
//
// Per SPEC.md §4, one Transport connection (whether returned by Dial or
// handed to onConn by Start) carries exactly one syncat protocol stream —
// framing, handshake, and message types are internal/protocol's concern,
// not this package's.
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

	// DiscardPeer drops whatever per-peer state the transport has cached
	// for addr, so the next Dial to it starts from scratch. Called when a
	// connection to that peer has ended and anything cached for it should
	// be assumed stale — notably tailcat's per-peer Client, whose "tell the
	// server to add us as a WireGuard peer" handshake happens once per
	// Client and never again (see TailcatTransport.discardClient).
	//
	// Must be safe to call for an addr the transport has nothing cached
	// for, and safe to call concurrently with Dial.
	DiscardPeer(addr string)

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

// Default schedule parameters (SPEC.md §2.2): dial every configured peer
// continuously with exponential backoff, 1s initial, doubling, capped at
// 5 minutes, jittered.
const (
	DefaultBackoffInitial = 1 * time.Second
	DefaultBackoffMax     = 5 * time.Minute
	DefaultBackoffFactor  = 2.0
	// DefaultBackoffJitter is the fraction of the un-jittered delay that
	// may be randomly subtracted: the jittered delay is uniform in
	// [(1-j)*d, d].
	DefaultBackoffJitter = 0.5
)

// Backoff computes the reconnect delay schedule from SPEC.md §2.2. It is a
// pure, stateless value: [Backoff.NextDelay] takes the failure count and a
// random source as explicit parameters rather than reading global state
// (time.Now, math/rand's global source), so a test can drive the schedule
// deterministically without sleeping and without racing other tests over
// shared global randomness.
//
// The zero value is the documented default schedule (1s → 5min, factor 2,
// jitter 0.5).
type Backoff struct {
	Initial time.Duration // defaults to DefaultBackoffInitial if <= 0
	Max     time.Duration // defaults to DefaultBackoffMax if <= 0
	Factor  float64       // defaults to DefaultBackoffFactor if <= 0
	Jitter  float64       // defaults to DefaultBackoffJitter if <= 0; clamped to [0,1]
}

func (b Backoff) initial() time.Duration {
	if b.Initial <= 0 {
		return DefaultBackoffInitial
	}
	return b.Initial
}

func (b Backoff) max() time.Duration {
	if b.Max <= 0 {
		return DefaultBackoffMax
	}
	return b.Max
}

func (b Backoff) factor() float64 {
	if b.Factor <= 0 {
		return DefaultBackoffFactor
	}
	return b.Factor
}

func (b Backoff) jitter() float64 {
	j := b.Jitter
	if j <= 0 {
		j = DefaultBackoffJitter
	}
	if j > 1 {
		j = 1
	}
	return j
}

// NextDelay returns the delay to wait before the next dial attempt, given
// failures consecutive prior failures (0 for the delay before the very
// first retry). The un-jittered delay is Initial*Factor^failures, capped
// at Max.
//
// rnd supplies the randomness for jitter: the returned delay is uniform in
// [(1-Jitter)*d, d] where d is the un-jittered delay. A nil rnd disables
// jitter, returning d itself — useful in tests that want to assert the
// base schedule without also accounting for randomness.
func (b Backoff) NextDelay(failures int, rnd *rand.Rand) time.Duration {
	if failures < 0 {
		failures = 0
	}
	base := b.initial()
	max := b.max()

	delay := base
	if failures > 0 {
		mult := math.Pow(b.factor(), float64(failures))
		// Guard against float overflow to +Inf (or a value that no
		// longer fits in a Duration) for large failure counts: once the
		// scaled value would already exceed max, there's no need (and
		// it isn't safe) to compute the multiplication exactly.
		if mult >= float64(max)/float64(base) {
			delay = max
		} else {
			delay = time.Duration(float64(base) * mult)
		}
	}
	if delay > max {
		delay = max
	}

	if rnd == nil {
		return delay
	}
	j := b.jitter()
	if j <= 0 {
		return delay
	}
	minDelay := time.Duration(float64(delay) * (1 - j))
	span := delay - minDelay
	if span <= 0 {
		return delay
	}
	return minDelay + time.Duration(rnd.Int63n(int64(span)+1))
}

// Clock abstracts time so [Supervisor.Run] can be tested without real
// sleeping. [RealClock] is the production implementation.
type Clock interface {
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock is the production [Clock], backed by the time package.
var RealClock Clock = realClock{}

// Supervisor drives repeated dial attempts on Backoff's schedule: one
// instance is meant to be run (via [Supervisor.Run]) in its own goroutine
// per configured peer (SPEC.md §2.2). It owns only the retry timing; the
// peer manager (internal/core) owns the peer list, what "dial"
// actually does (Transport.Dial plus the protocol handshake), and the
// policy for what counts as a failure worth backing off from.
type Supervisor struct {
	Schedule Backoff
	Clock    Clock      // defaults to RealClock if nil
	Rand     *rand.Rand // defaults to no jitter if nil; see Backoff.NextDelay
}

// Run calls dial in a loop until ctx is done. Each call should attempt one
// connection and block for as long as it stays usable, then return: nil if
// the disconnect shouldn't count as a failure (e.g. a clean shutdown, or
// the losing side of dedup closing voluntarily — see [KeepConnection]), or
// a non-nil error otherwise. The failure count backing off the delay
// between calls resets to zero after any call that returns nil.
func (s Supervisor) Run(ctx context.Context, dial func(ctx context.Context) error) {
	clock := s.Clock
	if clock == nil {
		clock = RealClock
	}
	failures := 0
	for {
		if ctx.Err() != nil {
			return
		}
		err := dial(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			failures = 0
			continue
		}
		delay := s.Schedule.NextDelay(failures, s.Rand)
		failures++
		select {
		case <-clock.After(delay):
		case <-ctx.Done():
			return
		}
	}
}

// KeepConnection reports, per SPEC.md §2.4, whether a connection should
// survive when a duplicate exists: if both sides of a peering dial each
// other successfully, both keep the connection dialed by the node with the
// lexicographically higher Ed25519 public key and close the other.
//
// local and peer are the two endpoints' application identity keys (see
// internal/config.IdentityKey), and dialed reports whether the connection
// in question is the one the local node dialed (outbound) as opposed to
// the one it accepted (inbound). Applying the rule needs the peer identity
// that internal/protocol's handshake establishes, so this function only
// exposes the decision itself; internal/core's peer manager calls it once
// both connections to a peer are authenticated.
//
// Both ends of a duplicate pair call this with dialed flipped — one side's
// outbound connection is the other side's inbound connection — and always
// agree on which single connection survives: for connection C dialed by
// node D and accepted by node O, D calls KeepConnection(D.key, O.key,
// true) and O calls KeepConnection(O.key, D.key, false); both evaluate to
// bytes.Compare(D.key, O.key) > 0, so they reach the same answer about C
// without coordinating. (Equal keys can't arise between distinct peers in
// practice; KeepConnection still returns a well-defined, self-consistent
// answer for them — the "peer" with the equal key is treated as not
// higher — it just isn't a meaningful dedup decision.)
func KeepConnection(local, peer ed25519.PublicKey, dialed bool) bool {
	localIsHigher := bytes.Compare(local, peer) > 0
	return dialed == localIsHigher
}
