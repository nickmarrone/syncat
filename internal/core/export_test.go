package core

import "context"

// RescanShare runs an immediate, synchronous full scan of shareID and
// propagates the result, exactly like the periodic/fsnotify-triggered
// path. Exported for tests that want deterministic sync timing without
// waiting on the watcher's real debounce/periodic timers. (A production
// "rescan now" entry point would be this same one-line wrapper, moved out
// of the _test file.)
func (n *Node) RescanShare(ctx context.Context, shareID string) error {
	return n.rescanShare(ctx, shareID)
}
