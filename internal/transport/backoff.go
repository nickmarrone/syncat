package transport

import (
	"context"
	"math"
	"math/rand"
	"time"
)

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
// peer manager (internal/core, Phase 7) owns the peer list, what "dial"
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
