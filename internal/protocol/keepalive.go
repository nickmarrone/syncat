package protocol

import (
	"context"
	"sync"
	"time"
)

// Default keepalive timing (SPEC.md §4): send Ping after 30s of outbound
// idleness; treat the connection as dead after 90s with nothing received
// at all (a Pong counts as received traffic, same as any other message).
const (
	DefaultPingInterval = 30 * time.Second
	DefaultDeadAfter    = 90 * time.Second
)

// Clock abstracts wall-clock time so [Keepalive.Run] can be driven
// deterministically in tests, with no real sleeping. [RealClock] is the
// production implementation. This mirrors transport.Clock but is defined
// locally rather than imported, so internal/protocol has no dependency on
// internal/transport (SPEC.md §10/§12 layering: protocol is a leaf
// package).
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock is the production [Clock], backed by the time package.
var RealClock Clock = realClock{}

// Keepalive tracks send/receive activity on one connection and decides
// when to send a Ping and when to declare the connection dead (SPEC.md
// §4). It does no I/O and starts no goroutines on its own: the owner
// (Phase 7's peer manager) calls [Keepalive.RecordSent] and
// [Keepalive.RecordReceived] from its read/write loop, and drives the
// timing with [Keepalive.Run] (or polls [Keepalive.NeedsPing] /
// [Keepalive.Dead] directly).
//
// The zero value is not usable; construct with [NewKeepalive].
type Keepalive struct {
	clock        Clock
	pingInterval time.Duration
	deadAfter    time.Duration

	mu       sync.Mutex
	lastSent time.Time
	lastRecv time.Time
}

// NewKeepalive returns a Keepalive using clock for timing (RealClock if
// nil), sending a Ping after pingInterval of outbound idleness (
// DefaultPingInterval if <= 0) and considering the connection dead after
// deadAfter with nothing received (DefaultDeadAfter if <= 0). Both
// lastSent and lastRecv start at clock.Now(), i.e. a freshly constructed
// Keepalive assumes the connection just did something in both directions
// (matching a connection that just finished its handshake).
func NewKeepalive(clock Clock, pingInterval, deadAfter time.Duration) *Keepalive {
	if clock == nil {
		clock = RealClock
	}
	if pingInterval <= 0 {
		pingInterval = DefaultPingInterval
	}
	if deadAfter <= 0 {
		deadAfter = DefaultDeadAfter
	}
	now := clock.Now()
	return &Keepalive{
		clock:        clock,
		pingInterval: pingInterval,
		deadAfter:    deadAfter,
		lastSent:     now,
		lastRecv:     now,
	}
}

// RecordSent notes that a frame (of any type — a Ping counts, and so does
// any other outbound message) was just sent, resetting the ping-idle
// timer.
func (k *Keepalive) RecordSent() {
	k.mu.Lock()
	k.lastSent = k.clock.Now()
	k.mu.Unlock()
}

// RecordReceived notes that a frame (of any type — a Pong counts, and so
// does any other inbound message) was just received, resetting the
// dead-connection timer.
func (k *Keepalive) RecordReceived() {
	k.mu.Lock()
	k.lastRecv = k.clock.Now()
	k.mu.Unlock()
}

// NeedsPing reports whether pingInterval has elapsed since the last
// RecordSent (or since construction, if none yet).
func (k *Keepalive) NeedsPing() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return !k.clock.Now().Before(k.lastSent.Add(k.pingInterval))
}

// Dead reports whether deadAfter has elapsed since the last
// RecordReceived (or since construction, if none yet).
func (k *Keepalive) Dead() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return !k.clock.Now().Before(k.lastRecv.Add(k.deadAfter))
}

// Run polls NeedsPing and Dead every checkInterval (pingInterval passed to
// [NewKeepalive] if checkInterval <= 0) until ctx is done, calling onPing
// each time a ping becomes due and onDead (once) the moment the
// connection is judged dead, at which point Run returns — the caller is
// expected to tear down and reconnect (SPEC.md §4), not keep polling a
// connection already declared dead. onPing and onDead may be nil.
//
// Run does no I/O itself: onPing is expected to actually send a Ping
// frame and then call RecordSent, and onDead to close the connection (and
// whatever else Phase 7's reconnect policy requires).
func (k *Keepalive) Run(ctx context.Context, checkInterval time.Duration, onPing, onDead func()) {
	if checkInterval <= 0 {
		checkInterval = k.pingInterval
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-k.clock.After(checkInterval):
		}
		if k.Dead() {
			if onDead != nil {
				onDead()
			}
			return
		}
		if k.NeedsPing() {
			if onPing != nil {
				onPing()
			}
		}
	}
}
