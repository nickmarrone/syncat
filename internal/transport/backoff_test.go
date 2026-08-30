package transport

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"
)

func TestBackoffNextDelayNoJitter(t *testing.T) {
	b := Backoff{} // zero value: documented defaults (1s, 5min, factor 2)

	cases := []struct {
		failures int
		want     time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 64 * time.Second},
		{7, 128 * time.Second},
		{8, 256 * time.Second},
		{9, 5 * time.Minute},  // 512s uncapped; must clamp to the 300s cap
		{20, 5 * time.Minute}, // stays capped
		{-5, 1 * time.Second}, // negative failures clamp to 0
	}
	for _, c := range cases {
		got := b.NextDelay(c.failures, nil)
		if got != c.want {
			t.Errorf("NextDelay(%d, nil) = %v, want %v", c.failures, got, c.want)
		}
	}
}

func TestBackoffCustomSchedule(t *testing.T) {
	b := Backoff{Initial: 100 * time.Millisecond, Max: time.Second, Factor: 3}

	if got := b.NextDelay(0, nil); got != 100*time.Millisecond {
		t.Fatalf("failures=0: got %v, want 100ms", got)
	}
	if got := b.NextDelay(1, nil); got != 300*time.Millisecond {
		t.Fatalf("failures=1: got %v, want 300ms", got)
	}
	// Uncapped this would be 100ms * 3^5 = 24.3s; must clamp to Max.
	if got := b.NextDelay(5, nil); got != time.Second {
		t.Fatalf("failures=5: got %v, want capped at 1s", got)
	}
}

// TestBackoffJitterStaysWithinBound drives NextDelay with a deterministic
// (seeded) random source many times per failure count and checks every
// jittered delay falls in [(1-Jitter)*d, d], where d is the un-jittered
// delay for that failure count. No real sleeping is involved anywhere in
// this test: NextDelay is a pure computation.
func TestBackoffJitterStaysWithinBound(t *testing.T) {
	b := Backoff{Jitter: 0.5}
	rnd := rand.New(rand.NewSource(1))

	for failures := 0; failures < 12; failures++ {
		unjittered := b.NextDelay(failures, nil)
		lower := time.Duration(float64(unjittered) * 0.5)
		for i := 0; i < 200; i++ {
			got := b.NextDelay(failures, rnd)
			if got > unjittered {
				t.Fatalf("failures=%d: jittered delay %v exceeds un-jittered %v", failures, got, unjittered)
			}
			if got < lower {
				t.Fatalf("failures=%d: jittered delay %v below floor %v", failures, got, lower)
			}
		}
	}
}

func TestBackoffJitterDisabledByNilRand(t *testing.T) {
	b := Backoff{Jitter: 0.9}
	for failures := 0; failures < 5; failures++ {
		got := b.NextDelay(failures, nil)
		want := b.NextDelay(failures, nil)
		if got != want {
			t.Fatalf("NextDelay(%d, nil) not deterministic: %v vs %v", failures, got, want)
		}
	}
}

// fakeClock is a Clock whose After always returns the same channel, fed
// manually by the test — so Supervisor.Run's waits are driven by explicit
// sends rather than real time passing.
type fakeClock struct {
	ch chan time.Time
}

func (f *fakeClock) After(time.Duration) <-chan time.Time { return f.ch }

func TestSupervisorRunRetriesAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fc := &fakeClock{ch: make(chan time.Time, 1)}

	var attempts int
	done := make(chan struct{})
	go func() {
		defer close(done)
		s := Supervisor{Schedule: Backoff{Initial: time.Millisecond}, Clock: fc}
		s.Run(ctx, func(context.Context) error {
			attempts++
			if attempts >= 3 {
				cancel()
			}
			return errors.New("boom")
		})
	}()

	// Two failed attempts each wait once on the fake clock before trying
	// again; the third attempt cancels ctx from inside dial, so Run
	// notices ctx is done and returns without waiting a third time.
	for i := 0; i < 2; i++ {
		select {
		case fc.ch <- time.Now():
		case <-done:
			t.Fatalf("Run finished after only %d attempts, want at least 3", attempts)
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not consume the fake clock's timer channel")
		}
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after ctx was canceled")
	}

	if attempts != 3 {
		t.Fatalf("got %d attempts, want exactly 3", attempts)
	}
}

// TestSupervisorRunResetsFailuresAfterSuccess checks that a successful dial
// resets the retry loop: after attempt 1 fails (one wait on the fake
// clock) and attempt 2 succeeds, attempt 3 must run immediately with no
// second wait — if the failure count weren't reset, Run would try to
// consume the fake clock's channel again first and this test would time
// out waiting for `done`.
func TestSupervisorRunResetsFailuresAfterSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fc := &fakeClock{ch: make(chan time.Time, 1)}

	var attempts int
	done := make(chan struct{})
	go func() {
		defer close(done)
		s := Supervisor{Schedule: Backoff{Initial: time.Millisecond}, Clock: fc}
		s.Run(ctx, func(context.Context) error {
			attempts++
			switch attempts {
			case 1:
				return errors.New("boom") // fails: Run must wait once before retrying
			case 2:
				return nil // succeeds: resets the failure count
			default:
				cancel() // attempt 3: stop the loop
				return nil
			}
		})
	}()

	select {
	case fc.ch <- time.Now(): // unblock the one wait after attempt 1's failure
	case <-done:
		t.Fatalf("Run finished after only %d attempts before the expected wait", attempts)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not wait after the first failed attempt")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after ctx was canceled")
	}

	if attempts != 3 {
		t.Fatalf("got %d attempts, want exactly 3", attempts)
	}
}
