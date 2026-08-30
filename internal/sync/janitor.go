package sync

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DefaultJanitorInterval is how often the janitor sweeps the trash for
// expired entries in production (SPEC.md §7: "a daily janitor purges older
// entries").
const DefaultJanitorInterval = 24 * time.Hour

// JanitorClock abstracts wall-clock time for the janitor: both "how old is
// this trash entry" (Now) and "when does the next sweep fire" (After).
// This mirrors protocol.Clock and index.Clock exactly (each package
// defines its own copy rather than sharing one, for the same
// leaf-package-independence reason documented on protocol.Clock) — it is
// a different, richer type from this package's own Clock (a plain
// time.Time-returning func used only to timestamp conflict/trash names),
// which has no After and so can't drive deterministic interval timing in
// tests. JanitorClock lets a test replace both "now" and the sweep timer
// with a fake, with no real sleeping (see janitor_test.go).
type JanitorClock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realJanitorClock struct{}

func (realJanitorClock) Now() time.Time                         { return time.Now() }
func (realJanitorClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealJanitorClock is the production JanitorClock, backed by the time
// package.
var RealJanitorClock JanitorClock = realJanitorClock{}

// Janitor purges trash entries older than a configured retention window
// (config.TrashRetentionDays, default 30 — SPEC.md §7). It follows the
// same injectable-clock, explicit-Close pattern as protocol.Keepalive and
// index.Watcher.
//
// The zero value is not usable; construct with NewJanitor.
type Janitor struct {
	trash     *Trash
	retention time.Duration
	clock     JanitorClock
	interval  time.Duration
	onSweep   func(purged int, err error)

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewJanitor returns a Janitor that purges trash entries older than
// retention from trash, checking every interval (DefaultJanitorInterval if
// <= 0). clock is used both to decide "how old" and to drive the sweep
// timing; RealJanitorClock is used if clock is nil. onSweep, if non-nil, is
// called after every sweep the background loop runs (including a failed
// one) with the number of entries purged and any error — tests use it to
// observe sweeps deterministically instead of racing the background
// goroutine.
func NewJanitor(trash *Trash, retention time.Duration, clock JanitorClock, interval time.Duration, onSweep func(purged int, err error)) *Janitor {
	if clock == nil {
		clock = RealJanitorClock
	}
	if interval <= 0 {
		interval = DefaultJanitorInterval
	}
	return &Janitor{
		trash:     trash,
		retention: retention,
		clock:     clock,
		interval:  interval,
		onSweep:   onSweep,
		stop:      make(chan struct{}),
	}
}

// Start begins the janitor's periodic sweep loop in a background
// goroutine. It does not sweep immediately; the first sweep happens after
// one interval (matching index.Watcher's periodicLoop — a fresh daemon
// start shouldn't immediately start deleting things before its first
// scheduled tick). Call Sweep directly for an immediate, synchronous pass
// (e.g. right after startup, or from the CLI).
func (j *Janitor) Start(ctx context.Context) {
	j.wg.Add(1)
	go j.loop(ctx)
}

func (j *Janitor) loop(ctx context.Context) {
	defer j.wg.Done()
	for {
		select {
		case <-j.stop:
			return
		case <-ctx.Done():
			return
		case <-j.clock.After(j.interval):
			purged, err := j.Sweep(ctx)
			if j.onSweep != nil {
				j.onSweep(purged, err)
			}
		}
	}
}

// Close stops the janitor's background loop and waits for it to exit.
// Safe to call more than once, and even if Start was never called.
func (j *Janitor) Close() error {
	select {
	case <-j.stop:
		// already closed
	default:
		close(j.stop)
	}
	j.wg.Wait()
	return nil
}

// Sweep runs one purge pass immediately and synchronously: every trash
// entry older than j.retention is permanently deleted; everything else is
// left alone. It returns the number of entries purged.
//
// Sweep never walks outside j.trash.root (WalkDir is rooted there, and a
// directory walk does not follow symlinks into other trees), and
// tolerates the trash root not existing yet (a fresh install that has
// never trashed anything) by treating that as an empty, no-op sweep.
func (j *Janitor) Sweep(ctx context.Context) (int, error) {
	root := j.trash.root
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("sync: janitor: stat trash root: %w", err)
	}

	cutoff := j.clock.Now().Add(-j.retention)
	purged := 0

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			return nil
		}
		base := filepath.Base(p)
		m := trashSuffixRe.FindStringSubmatch(base)
		if m == nil {
			return nil // not one of ours; leave it alone
		}
		ts, err := parseUnixSeconds(m[2])
		if err != nil {
			return nil
		}
		if ts.Before(cutoff) {
			if rmErr := os.Remove(p); rmErr != nil && !os.IsNotExist(rmErr) {
				return fmt.Errorf("remove %s: %w", p, rmErr)
			}
			purged++
		}
		return nil
	})
	if err != nil {
		return purged, fmt.Errorf("sync: janitor: sweep: %w", err)
	}

	pruneEmptyDirs(root)
	return purged, nil
}

// pruneEmptyDirs best-effort removes now-empty share subdirectories left
// behind after a sweep purges every entry under them, so an old share's
// trash directory doesn't accumulate forever as an empty husk. Failures
// are ignored: this cleanup is cosmetic, never worth failing a sweep over.
// Only a plain os.Remove is used (never RemoveAll), so a directory that
// still has live content in it simply fails to remove and is left alone —
// no risk to anything not already expired.
func pruneEmptyDirs(root string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		sub, err := os.ReadDir(dir)
		if err == nil && len(sub) == 0 {
			_ = os.Remove(dir)
		}
	}
}

func parseUnixSeconds(s string) (time.Time, error) {
	var sec int64
	if _, err := fmt.Sscanf(s, "%d", &sec); err != nil {
		return time.Time{}, err
	}
	return time.Unix(sec, 0), nil
}
