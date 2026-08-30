package index

import (
	"sync"
	"time"
)

// debouncer collapses a stream of dirty-path marks arriving within
// `window` of each other into a single onFlush call carrying the
// accumulated, de-duplicated set. It is pure quiet-period debounce logic
// driven entirely by an injected Clock — no fsnotify, no real sleeping —
// which is what makes it possible to test the "a burst collapses into one
// rescan" behavior deterministically (see watcher_test.go's fakeClock):
// the timing-sensitive logic lives here, isolated from the real OS/
// filesystem event source that Watcher wires it to.
//
// Each markDirty call (re)starts a fresh `window`-long wait; onFlush fires
// once no mark has arrived for a full window, with the union of every
// relpath marked since the last flush. The zero value is not usable;
// construct with newDebouncer.
type debouncer struct {
	clock  Clock
	window time.Duration

	onFlush func(paths []string)

	mark chan string
	stop chan struct{}
	done chan struct{}
}

func newDebouncer(clock Clock, window time.Duration, onFlush func(paths []string)) *debouncer {
	return &debouncer{
		clock:   clock,
		window:  window,
		onFlush: onFlush,
		mark:    make(chan string),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// markDirty records relpath as dirty, (re)starting the debounce window.
// Safe to call from any goroutine; a no-op after close.
func (d *debouncer) markDirty(relpath string) {
	select {
	case d.mark <- relpath:
	case <-d.done:
	}
}

// run is the debouncer's event loop; call it in its own goroutine. wg is
// marked Done when run returns, matching Watcher's other goroutines'
// convention so Watcher.Close's wg.Wait() covers this loop too.
func (d *debouncer) run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(d.done)

	dirty := make(map[string]struct{})
	var timer <-chan time.Time

	flush := func() {
		if len(dirty) == 0 {
			return
		}
		paths := make([]string, 0, len(dirty))
		for p := range dirty {
			paths = append(paths, p)
		}
		dirty = make(map[string]struct{})
		if d.onFlush != nil {
			d.onFlush(paths)
		}
	}

	for {
		select {
		case <-d.stop:
			return
		case p := <-d.mark:
			dirty[p] = struct{}{}
			// Re-registering on every mark is the whole debounce
			// mechanism: whichever registration is still "current" when
			// it fires wins, and any earlier ones this replaced just go
			// unread (like any discarded time.After result — bounded,
			// GC'd, not a leak).
			timer = d.clock.After(d.window)
		case <-timer:
			timer = nil
			flush()
		}
	}
}

// close stops run and waits for it to exit. Safe to call once.
func (d *debouncer) close() {
	close(d.stop)
	<-d.done
}
