package index

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// --- the watcher: fsnotify plus a periodic full rescan -----------------

// Default watcher timing (SPEC.md §5): a burst of fsnotify events
// collapses into one rescan after 1s of quiet; the periodic full rescan
// (source of truth) runs every 300s regardless of watcher activity.
const (
	DefaultDebounce       = 1 * time.Second
	DefaultRescanInterval = 300 * time.Second
)

// Clock abstracts wall-clock time so debounce timing can be driven
// deterministically in tests, with no real sleeping (see debouncer and
// watcher_test.go's fakeClock). Defined locally rather than imported from
// internal/protocol or internal/transport, which each define their own
// identical interface for the same reason: internal/index has no business
// depending on either package just to reuse a two-method interface
// (mirrors the note already on protocol.Clock).
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock is the production Clock, backed by the time package.
var RealClock Clock = realClock{}

// WatcherOptions configures a Watcher.
type WatcherOptions struct {
	// Root is the absolute path of the share tree to watch.
	Root string
	// Clock drives debounce and periodic-rescan timing; RealClock if nil.
	Clock Clock
	// Debounce is the quiet-period window: a rescan fires this long after
	// the most recent fsnotify event in a batch. DefaultDebounce if <= 0.
	Debounce time.Duration
	// RescanInterval is how often OnPeriodic fires regardless of watcher
	// activity — the source-of-truth full rescan (SPEC.md §5).
	// DefaultRescanInterval if <= 0.
	RescanInterval time.Duration
	// OnDirty is called after the debounce window with the batch of
	// share-relative paths fsnotify reported dirty. May be nil.
	OnDirty func(relpaths []string)
	// OnPeriodic is called every RescanInterval. May be nil.
	OnPeriodic func()
	// OnWarn, if non-nil, is called for non-fatal watcher problems (e.g.
	// hitting the OS watch-descriptor limit) that degrade the watcher to
	// periodic-only operation rather than failing it.
	OnWarn func(format string, args ...any)
}

// Watcher provides best-effort real-time change notification for one
// share tree via fsnotify, plus the periodic full rescan that remains the
// source of truth (SPEC.md §5). It adds watches recursively, including for
// subdirectories created after the watcher starts, and debounces bursts of
// events into a single targeted rescan callback — a `git checkout`
// touching thousands of files fires OnDirty once, not thousands of times.
//
// If establishing filesystem watches fails or hits an OS limit (e.g.
// "too many open files" / inotify watch limit), Watcher degrades to
// periodic-only operation: OnWarn is called, real-time notifications stop,
// but OnPeriodic keeps firing on schedule. A construction-time failure to
// create even the base fsnotify.Watcher degrades the same way rather than
// making NewWatcher return an error, since a share with no live watches at
// all still syncs correctly via the periodic rescan alone.
//
// Every goroutine Watcher starts is stopped by Close; Close blocks until
// they've exited; there is no goroutine leak across repeated
// NewWatcher/Close cycles (see watcher_test.go).
type Watcher struct {
	root           string
	clock          Clock
	debounce       time.Duration
	rescanInterval time.Duration
	onWarn         func(format string, args ...any)

	fsw *fsnotify.Watcher // nil in degraded (periodic-only) mode

	deb *debouncer

	stop chan struct{}
	wg   sync.WaitGroup
}

// NewWatcher starts watching opts.Root. It never returns a non-nil error
// for problems establishing the underlying OS watches (see the degraded
// mode note on Watcher); it returns an error only if opts.Root cannot be
// resolved to an absolute path at all.
func NewWatcher(opts WatcherOptions) (*Watcher, error) {
	root, err := filepath.Abs(opts.Root)
	if err != nil {
		return nil, fmt.Errorf("index: watcher: resolve root %q: %w", opts.Root, err)
	}
	clock := opts.Clock
	if clock == nil {
		clock = RealClock
	}
	debounce := opts.Debounce
	if debounce <= 0 {
		debounce = DefaultDebounce
	}
	rescanInterval := opts.RescanInterval
	if rescanInterval <= 0 {
		rescanInterval = DefaultRescanInterval
	}

	w := &Watcher{
		root:           root,
		clock:          clock,
		debounce:       debounce,
		rescanInterval: rescanInterval,
		onWarn:         opts.OnWarn,
		stop:           make(chan struct{}),
	}

	w.deb = newDebouncer(clock, debounce, opts.OnDirty)

	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		w.warnf("index: watcher: create fsnotify watcher: %v (degrading to periodic-only rescans)", err)
	} else if err := addTreeWatches(fsw, root); err != nil {
		w.warnf("index: watcher: add watches under %s: %v (degrading to periodic-only rescans)", root, err)
		fsw.Close()
	} else {
		w.fsw = fsw
	}

	w.wg.Add(1)
	go w.deb.run(&w.wg)

	if w.fsw != nil {
		w.wg.Add(1)
		go w.eventLoop()
	}

	w.wg.Add(1)
	go w.periodicLoop(opts.OnPeriodic)

	return w, nil
}

func (w *Watcher) warnf(format string, args ...any) {
	if w.onWarn != nil {
		w.onWarn(format, args...)
	}
}

// Close stops the watcher's goroutines and releases the underlying
// fsnotify watcher (if any), then waits for everything to exit. Call it
// exactly once, matching the io.Closer convention (a second call panics on
// the already-closed stop channel).
func (w *Watcher) Close() error {
	close(w.stop)
	w.deb.close()
	var err error
	if w.fsw != nil {
		if cerr := w.fsw.Close(); cerr != nil {
			err = fmt.Errorf("index: watcher: close fsnotify watcher: %w", cerr)
		}
	}
	w.wg.Wait()
	return err
}

// eventLoop drains fsnotify events and errors, feeding dirty paths to the
// debouncer and adding watches for newly created subdirectories.
func (w *Watcher) eventLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.stop:
			return
		case ev, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			rel, ok := w.relPath(ev.Name)
			if !ok {
				continue
			}

			if ev.Op&fsnotify.Create != 0 {
				if fi, err := os.Lstat(ev.Name); err == nil && fi.IsDir() && fi.Mode()&os.ModeSymlink == 0 {
					// A directory (or a whole subtree, e.g. a fast `mkdir
					// -p` or an untar) can appear before we get a chance
					// to watch each level individually; walk it now so no
					// nested directory is missed. Do this *before*
					// markDirty below: markDirty is what a consumer of
					// OnDirty observes and may react to (e.g. by creating
					// files under the new directory once it learns the
					// directory exists), so the watch needs to already be
					// live by the time that notification goes out. Doing
					// it the other way around widens an already-real
					// kernel-level race (mkdir vs. our inotify_add_watch)
					// with an entirely avoidable one: our own debounce/
					// notification latency.
					if err := addTreeWatches(w.fsw, ev.Name); err != nil {
						w.warnf("index: watcher: add watch for new directory %s: %v", ev.Name, err)
					}
				}
			}

			w.deb.markDirty(rel)
		case err, ok := <-w.fsw.Errors:
			if !ok {
				return
			}
			w.warnf("index: watcher: fsnotify error: %v", err)
		}
	}
}

// periodicLoop calls onPeriodic every w.rescanInterval until Close.
func (w *Watcher) periodicLoop(onPeriodic func()) {
	defer w.wg.Done()
	for {
		select {
		case <-w.stop:
			return
		case <-w.clock.After(w.rescanInterval):
			if onPeriodic != nil {
				onPeriodic()
			}
		}
	}
}

// relPath converts an absolute fsnotify event path to a share-relative,
// forward-slash path, matching what Scanner produces. ok is false for a
// path outside the share root (shouldn't happen, but never trust it) or
// for the root itself (no meaningful relpath).
func (w *Watcher) relPath(absPath string) (rel string, ok bool) {
	r, err := filepath.Rel(w.root, absPath)
	if err != nil || r == "." || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
		return "", false
	}
	return filepath.ToSlash(r), true
}

// addTreeWatches adds an fsnotify watch for dir and every non-symlink
// subdirectory beneath it.
func addTreeWatches(fsw *fsnotify.Watcher, dir string) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			// A directory can vanish between being listed and being
			// visited (same race the scanner tolerates); skip it rather
			// than aborting the whole watch-tree setup.
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return fs.SkipDir
		}
		if err := fsw.Add(p); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Vanished between WalkDir listing it and Add(); ignore.
				return nil
			}
			return fmt.Errorf("watch %s: %w", p, err)
		}
		return nil
	})
}

// --- the debouncer: collapsing event bursts ----------------------------

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
