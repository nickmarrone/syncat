// journal.go holds the retention sweep for index.Store's change journal —
// the append-only log of file changes that internal/sync replays to bring
// a reconnecting peer up to date without re-sending the whole share index.
// The journal is the one table in the database with no natural ceiling:
// every file change appends a row, nothing overwrites one, and a share
// under steady churn appends forever. This is what bounds it.
package core

import (
	"time"
)

// journalRetention is how long a change-journal entry is kept.
//
// The window trades disk and query cost against how long a peer can be
// away and still get an incremental resync. A peer whose cursor predates
// the oldest surviving entry is not stranded — internal/sync's
// canAnswerWithDeltas notices and answers with a full snapshot instead,
// which re-anchors it — so the only cost of pruning too eagerly is one
// snapshot for a peer that has been offline longer than this. A week is
// comfortably longer than the outages incremental sync exists to
// smooth over (a reboot, a laptop lid, a flaky link) and short enough
// that a busy share's journal does not grow without limit.
const journalRetention = 7 * 24 * time.Hour

// journalSweepInterval is how often the retention window is applied. The
// journal only ever grows between sweeps, and one sweep is a single
// indexed DELETE (change_journal_retention covers created_at), so there is
// nothing to gain from running it more often than the trash janitor does.
const journalSweepInterval = 24 * time.Hour

// startJournalSweeper runs the retention sweep on journalSweepInterval for
// the life of the node.
//
// Like index.Watcher's periodic rescan and the trash janitor, the first
// pass happens after one interval rather than at startup: nothing here is
// urgent, and a daemon that restarts frequently should not spend each
// start deleting rows.
func (n *Node) startJournalSweeper() {
	n.goTracked(func() {
		for {
			select {
			case <-n.ctx.Done():
				return
			case <-n.clock.After(journalSweepInterval):
				n.sweepJournal()
			}
		}
	})
}

// sweepJournal drops every change-journal entry older than
// journalRetention. A failure is logged and the loop continues: an
// unswept journal costs disk, while giving up on the sweep entirely would
// cost it forever.
func (n *Node) sweepJournal() {
	cutoff := n.clock.Now().Add(-journalRetention)
	if err := n.store.PruneJournal(n.ctx, cutoff); err != nil {
		n.logger.Printf("core: change journal sweep failed: %v", err)
		return
	}
	n.debugf("core: change journal swept of entries older than %s", journalRetention)
}
