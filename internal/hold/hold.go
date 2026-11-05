// Package hold is the hold (see GLOSSARY.md): the guard that stops a change
// that looks like a disaster from being recorded or spread.
//
// A hold is per link: a share, and either the peer whose incoming changes
// are held or "" for this node's own scans of it. Guard keeps every held
// link in memory and in the index store, so the check that every scan and
// every reconcile pass makes never queries the database, and a restart never
// forgets a hold. A forgotten hold would let the very change it stopped go
// through on the next scan.
//
// Guard answers one question, "may this change apply?", for two kinds of
// change. Scan asks it before a local scan result is applied, and Pass
// before a peer's reconcile pass applies anything. While this node's own
// copy of a share is held, every peer's pass is refused too: applying
// peers' changes into a folder in that state is how the damage would spread
// the other way.
//
// A hold ends one of three ways:
//
//   - The folder is fixed, so the next scan or pass would delete nothing and
//     the hold clears itself. Getting back under the thresholds is not
//     enough: after a share's files all vanish, restoring one would
//     otherwise clear the hold and let the rest go through as ordinary
//     deletes.
//   - The user confirms it (Confirm), and the next change that would have
//     been held applies instead.
//   - The share, subscription, or peer is no longer configured, and Retain
//     forgets it. Nothing would ever scan or sync that link again, so the
//     hold could not clear any other way.
package hold

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

// Reasons a link is on hold, stored in index.Hold.Reason and reported to
// API clients.
const (
	// RootUnavailable: the share's root is missing or unreadable while the
	// index still has live files. It cannot be confirmed: there is no change
	// to let through, only a folder to restore or a share to remove.
	RootUnavailable = "root-unavailable"

	// EverythingVanished: every live file in the share would be deleted.
	// This is an unmounted drive or an emptied folder, so it holds whatever
	// the share's thresholds say.
	EverythingVanished = "everything-vanished"

	// BulkDelete: the change would delete at least the share's configured
	// percentage and number of live files.
	BulkDelete = "bulk-delete"

	// ReservedDirTaken: the folder has a .syncat/ that syncat did not
	// create, so it has nowhere safe to keep the trash. It cannot be
	// confirmed; it clears once that directory is moved away.
	ReservedDirTaken = "reserved-dir-taken"
)

// ErrNotHeld is wrapped by the errors Only and Confirm return when there is
// no hold to act on.
var ErrNotHeld = errors.New("not on hold")

// Link is what one hold stops: changes to a share from one peer (PeerKey,
// its hex public key), or from this node's own scans (PeerKey "").
type Link struct {
	ShareID string
	PeerKey string
}

// Hold is one held link.
type Hold struct {
	index.Hold
}

// Confirmable reports whether the hold has a change the user can let
// through. A missing root, or a .syncat/ syncat did not create, has none:
// there is only a folder to fix.
func (h Hold) Confirmable() bool {
	return h.Reason != RootUnavailable && h.Reason != ReservedDirTaken
}

// Problem says what is wrong, as a clause such as "the folder is missing or
// unreadable".
func (h Hold) Problem() string {
	switch h.Reason {
	case RootUnavailable:
		return "the folder is missing or unreadable"
	case ReservedDirTaken:
		return "the folder has a " + index.ReservedDirName + " directory that syncat did not create"
	}
	return fmt.Sprintf("%d of %d files would be deleted", h.Deletes, h.Live)
}

// Remedy says what the user can do about it, as a clause such as "restore
// it, or remove the share".
func (h Hold) Remedy() string {
	switch h.Reason {
	case RootUnavailable:
		return "restore it, or remove the share"
	case ReservedDirTaken:
		return "move it away"
	}
	return "fix the folder, or let the deletes through"
}

// Options are what a Guard needs from the node it guards.
type Options struct {
	// Safety returns the hold thresholds in force for a share, defaults
	// filled in. It is read on every check, so a change applies to the next
	// scan or pass. Required.
	Safety func(shareID string) config.Safety

	// Now stamps a new hold. nil means time.Now.
	Now func() time.Time

	// Logger gets a line when a hold is placed, cleared, or forgotten. nil
	// discards them.
	Logger *log.Logger

	// PeerName returns a peer's display name for log lines and messages, or
	// "" if it has none. nil names every peer by its key.
	PeerName func(peerKey string) string
}

// Guard holds every link on hold. It is safe for concurrent use.
type Guard struct {
	ctx   context.Context
	store *index.Store
	opts  Options

	mu   sync.Mutex
	held map[Link]index.Hold // mirrors the store; written only after the store write succeeds
}

// Load returns a Guard over the holds recorded in store. ctx bounds every
// store write the Guard makes for the rest of its life, so it should be the
// node's own context, not a request's.
func Load(ctx context.Context, store *index.Store, opts Options) (*Guard, error) {
	if opts.Safety == nil {
		return nil, errors.New("hold: load: Options.Safety is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = log.New(io.Discard, "", 0)
	}
	list, err := store.ListHolds(ctx)
	if err != nil {
		return nil, fmt.Errorf("hold: load: %w", err)
	}
	g := &Guard{ctx: ctx, store: store, opts: opts, held: make(map[Link]index.Hold, len(list))}
	for _, h := range list {
		g.held[Link{h.ShareID, h.PeerKey}] = h
	}
	return g, nil
}

// --- the checks ------------------------------------------------------------

// Scan decides whether a local scan of shareID may be applied. existing is
// the index's rows for the share before the scan, and result is what the
// scan found. apply is false when the scan is held and must be dropped, so
// the index keeps describing the share as it was. released reports that
// this scan ended a hold on this node's own copy, so the peers' passes that
// hold stopped may now run.
func (g *Guard) Scan(shareID string, existing map[string]index.FileRow, result *index.ScanResult) (apply, released bool) {
	deleting := make(map[string]bool, len(result.Deleted))
	for _, r := range result.Deleted {
		deleting[r.RelPath] = true
	}
	deletes, live := measure(maps.Values(existing), deleting)
	return g.decide(Link{ShareID: shareID}, deletes, live)
}

// ScanFailed records what a failed scan of shareID means. It holds the
// share when err says the folder has a .syncat/ syncat did not create
// (sync.ErrReservedDirTaken), and when root itself is missing or unreadable
// while existing, the index's rows for the share, still has live files. Any
// other failure is transient and holds nothing.
func (g *Guard) ScanFailed(shareID, root string, existing map[string]index.FileRow, err error) {
	h := index.Hold{ShareID: shareID}
	if errors.Is(err, syncsvc.ErrReservedDirTaken) {
		h.Reason = ReservedDirTaken
	} else {
		_, live := measure(maps.Values(existing), nil)
		if live == 0 || !rootUnavailable(root) {
			return
		}
		h.Reason, h.Deletes, h.Live = RootUnavailable, live, live
	}
	g.mu.Lock()
	placed, perr := g.placeLocked(h)
	g.mu.Unlock()
	g.notePlaced(h, placed, perr)
}

// Pass decides whether a reconcile pass with the peer peerKey (never "")
// may apply to shareID. local is the share's index rows on this node, and
// deleting the paths the pass would delete. It refuses every pass while
// this node's own copy of the share is held, without holding the peer's
// link.
func (g *Guard) Pass(shareID, peerKey string, local []index.FileRow, deleting []string) bool {
	g.mu.Lock()
	_, ownHeld := g.held[Link{ShareID: shareID}]
	g.mu.Unlock()
	if ownHeld {
		return false
	}
	set := make(map[string]bool, len(deleting))
	for _, p := range deleting {
		set[p] = true
	}
	deletes, live := measure(slices.Values(local), set)
	apply, _ := g.decide(Link{ShareID: shareID, PeerKey: peerKey}, deletes, live)
	return apply
}

// decide is the one decision behind Scan and Pass: a change to link would
// delete deletes of the share's live files. It reports whether the change
// may apply, and whether that ended a hold on this node's own copy.
func (g *Guard) decide(link Link, deletes, live int) (apply, released bool) {
	reason := trigger(g.opts.Safety(link.ShareID), deletes, live)

	g.mu.Lock()
	h, held := g.held[link]
	if held {
		reason = stillHeld(h.Reason, reason, deletes)
	}
	if reason == "" || (held && h.Confirmed) {
		cleared, err := g.clearLocked(link)
		g.mu.Unlock()
		why := "nothing would be deleted any more"
		if reason != "" {
			why = "deletes confirmed"
		}
		g.noteCleared(link, cleared, err, why)
		return true, cleared && link.PeerKey == ""
	}
	next := index.Hold{ShareID: link.ShareID, PeerKey: link.PeerKey, Reason: reason, Deletes: deletes, Live: live}
	placed, err := g.placeLocked(next)
	g.mu.Unlock()
	g.notePlaced(next, placed, err)
	return false, false
}

// measure counts the live files among rows (not tombstones, not parked by
// an ignore rule, not directories) and how many of them deleting names.
// Live files are the denominator of every hold trigger. A directory is
// left out because it only goes once it is empty, so its files are already
// counted.
func measure(rows iter.Seq[index.FileRow], deleting map[string]bool) (deletes, live int) {
	for r := range rows {
		if r.Deleted || r.Ignored || r.Type == protocol.FileTypeDir {
			continue
		}
		live++
		if deleting[r.RelPath] {
			deletes++
		}
	}
	return deletes, live
}

// trigger returns why a change that would delete deletes of a share's live
// files puts it on hold under s, or "" if it does not. Fewer than two live
// files never counts as everything vanishing: deleting the only file in a
// share is an ordinary edit.
func trigger(s config.Safety, deletes, live int) string {
	if deletes == 0 {
		return ""
	}
	if live >= 2 && deletes >= live {
		return EverythingVanished
	}
	if deletes >= s.HoldDeleteMin && deletes*100 >= s.HoldDeletePercent*live {
		return BulkDelete
	}
	return ""
}

// stillHeld returns why a link already on hold with reason held stays on
// hold for a change that would delete deletes files and trips fresh (""
// for no trigger), or "" if the hold clears. Only a change that deletes
// nothing clears a hold over deletes; one that merely deletes fewer is the
// same disaster partly undone. A hold whose cause was never deletes ends
// once that cause is gone.
func stillHeld(held, fresh string, deletes int) string {
	if fresh != "" || deletes == 0 {
		return fresh
	}
	switch held {
	case ReservedDirTaken:
		return ""
	case RootUnavailable:
		// The folder is back, but not all of it: what is still missing
		// vanished with the rest.
		return EverythingVanished
	}
	return held
}

// rootUnavailable reports whether root itself is missing or unreadable,
// rather than a scan of it failing for some transient reason.
func rootUnavailable(root string) bool {
	f, err := os.Open(root)
	if err != nil {
		return true
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	return err != nil && !errors.Is(err, io.EOF)
}

// --- the user's side ---------------------------------------------------------

// List returns every hold, oldest first.
func (g *Guard) List() []Hold {
	g.mu.Lock()
	out := make([]Hold, 0, len(g.held))
	for _, h := range g.held {
		out = append(out, Hold{h})
	}
	g.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Since.Equal(out[j].Since) {
			return out[i].Since.Before(out[j].Since)
		}
		return out[i].ShareID+out[i].PeerKey < out[j].ShareID+out[j].PeerKey
	})
	return out
}

// Only returns the link of shareID's one hold, for a confirm that names no
// peer. It is an error for the share to have no hold (wrapping ErrNotHeld)
// or more than one.
func (g *Guard) Only(shareID string) (Link, error) {
	g.mu.Lock()
	var links []Link
	for link := range g.held {
		if link.ShareID == shareID {
			links = append(links, link)
		}
	}
	g.mu.Unlock()
	switch len(links) {
	case 0:
		return Link{}, fmt.Errorf("share %s is %w", shareID, ErrNotHeld)
	case 1:
		return links[0], nil
	}
	return Link{}, fmt.Errorf("share %s has %d holds; name the peer whose changes to confirm", shareID, len(links))
}

// Confirm lets the change held on link through: the next scan (for this
// node's own copy) or pass (for a peer's link) that would have been held
// applies instead, and clears the hold. Running that scan or pass is up to
// the caller. It returns the confirmed hold, or an error if link is not on
// hold (wrapping ErrNotHeld) or its hold has nothing to confirm.
func (g *Guard) Confirm(link Link) (Hold, error) {
	g.mu.Lock()
	h, ok := g.held[link]
	if ok && (Hold{h}).Confirmable() {
		h.Confirmed = true
		err := g.store.PutHold(g.ctx, h)
		if err == nil {
			g.held[link] = h
		}
		g.mu.Unlock()
		if err != nil {
			return Hold{}, fmt.Errorf("hold: confirm: %w", err)
		}
		return Hold{h}, nil
	}
	g.mu.Unlock()

	if !ok {
		if link.PeerKey != "" {
			return Hold{}, fmt.Errorf("share %s is %w for changes from %s", link.ShareID, ErrNotHeld, g.peerName(link.PeerKey))
		}
		return Hold{}, fmt.Errorf("share %s is %w", link.ShareID, ErrNotHeld)
	}
	held := Hold{h}
	return Hold{}, fmt.Errorf("share %s is on hold because %s; there is nothing to confirm. %s", link.ShareID, held.Problem(), sentence(held.Remedy()))
}

// Retain forgets every hold whose link keep rejects. keep is called with
// the Guard's lock held, so it must not call back into the Guard.
func (g *Guard) Retain(keep func(Link) bool) {
	type forgotten struct {
		link Link
		err  error
	}
	var gone []forgotten
	g.mu.Lock()
	for link := range g.held {
		if keep(link) {
			continue
		}
		err := g.store.DeleteHold(g.ctx, link.ShareID, link.PeerKey)
		if err == nil {
			delete(g.held, link)
		}
		gone = append(gone, forgotten{link, err})
	}
	g.mu.Unlock()
	for _, f := range gone {
		if f.err != nil {
			g.opts.Logger.Printf("hold: forget hold on share %s%s: %v", f.link.ShareID, g.peerSuffix(f.link.PeerKey), f.err)
			continue
		}
		g.opts.Logger.Printf("hold: share %s hold%s forgotten: it is no longer configured", f.link.ShareID, g.peerSuffix(f.link.PeerKey))
	}
}

// --- state changes, with g.mu held -----------------------------------------

// placeLocked records h, keeping the Since of a hold already on its link so
// the UI shows how long the share has been stuck, not when it last
// re-tripped. It reports whether the link was not held before.
func (g *Guard) placeLocked(h index.Hold) (placed bool, err error) {
	link := Link{h.ShareID, h.PeerKey}
	old, existed := g.held[link]
	if existed {
		if old.Reason == h.Reason && old.Deletes == h.Deletes && old.Live == h.Live {
			return false, nil
		}
		h.Since = old.Since
	} else {
		h.Since = g.opts.Now()
	}
	if err := g.store.PutHold(g.ctx, h); err != nil {
		return false, err
	}
	g.held[link] = h
	return !existed, nil
}

// clearLocked removes the hold on link, if there is one, and reports
// whether there was.
func (g *Guard) clearLocked(link Link) (bool, error) {
	if _, ok := g.held[link]; !ok {
		return false, nil
	}
	if err := g.store.DeleteHold(g.ctx, link.ShareID, link.PeerKey); err != nil {
		return false, err
	}
	delete(g.held, link)
	return true, nil
}

// --- log lines, written without g.mu held --------------------------------

func (g *Guard) notePlaced(h index.Hold, placed bool, err error) {
	switch {
	case err != nil:
		g.opts.Logger.Printf("hold: hold share %s%s: %v", h.ShareID, g.peerSuffix(h.PeerKey), err)
	case placed:
		held := Hold{h}
		g.opts.Logger.Printf("hold: share %s on hold%s: %s (%s)", h.ShareID, g.peerSuffix(h.PeerKey), held.Problem(), held.Remedy())
	}
}

func (g *Guard) noteCleared(link Link, cleared bool, err error, why string) {
	switch {
	case err != nil:
		g.opts.Logger.Printf("hold: clear hold on share %s%s: %v", link.ShareID, g.peerSuffix(link.PeerKey), err)
	case cleared:
		g.opts.Logger.Printf("hold: share %s hold%s cleared: %s", link.ShareID, g.peerSuffix(link.PeerKey), why)
	}
}

func (g *Guard) peerSuffix(peerKey string) string {
	if peerKey == "" {
		return ""
	}
	return " for changes from " + g.peerName(peerKey)
}

func (g *Guard) peerName(peerKey string) string {
	if g.opts.PeerName != nil {
		if name := g.opts.PeerName(peerKey); name != "" {
			return name
		}
	}
	return "peer " + peerKey
}

// sentence capitalizes a clause to start a sentence.
func sentence(clause string) string {
	if clause == "" {
		return ""
	}
	return strings.ToUpper(clause[:1]) + clause[1:]
}
