package core

import (
	"math/rand"
	"time"

	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
	"github.com/nickmarrone/syncat/internal/sync"
	"github.com/nickmarrone/syncat/internal/transport"
)

// Clock abstracts wall-clock time for every timing-dependent piece Node
// wires together: dial backoff (transport.Backoff/Supervisor), handshake
// keepalive (protocol.Keepalive), share watchers (index.Watcher), and the
// trash janitor (sync.Janitor). Each of those packages defines its own
// narrow Clock interface (Now/After, or just After for transport.Clock) for
// leaf-package independence — Clock here has both methods, so a single
// value satisfies all of them structurally, letting a test drive every
// timer Node starts from one fake clock with no real sleeping.
//
// The zero value is not usable; use RealClock in production or a fake in
// tests.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock is the production Clock, backed by the time package.
var RealClock Clock = realClock{}

// asTransportClock adapts a Clock to transport.Clock (After only).
func asTransportClock(c Clock) transport.Clock { return transportClockAdapter{c} }

type transportClockAdapter struct{ c Clock }

func (a transportClockAdapter) After(d time.Duration) <-chan time.Time { return a.c.After(d) }

// asProtocolClock adapts a Clock to protocol.Clock (Now+After) — Clock
// already satisfies this interface structurally, but the helper keeps call
// sites self-documenting about which package's Clock is being supplied.
func asProtocolClock(c Clock) protocol.Clock { return c }

// asIndexClock adapts a Clock to index.Clock (Now+After).
func asIndexClock(c Clock) index.Clock { return c }

// asJanitorClock adapts a Clock to sync.JanitorClock (Now+After).
func asJanitorClock(c Clock) sync.JanitorClock { return c }

// asSyncClock adapts a Clock to sync.Clock (a plain func() time.Time used
// only to timestamp conflict copies and trash entries).
func asSyncClock(c Clock) sync.Clock { return c.Now }

// defaultRand returns a Rand seeded from the current time, for production
// backoff jitter. Tests pass their own *rand.Rand (or nil, for a
// deterministic un-jittered schedule) via Options.Rand.
func defaultRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}
