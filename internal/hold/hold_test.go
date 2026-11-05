package hold

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nickmarrone/syncat/internal/config"
	"github.com/nickmarrone/syncat/internal/index"
	"github.com/nickmarrone/syncat/internal/protocol"
	syncsvc "github.com/nickmarrone/syncat/internal/sync"
)

const (
	share = "share1"
	peer  = "aaaa"
)

var defaultSafety = config.Safety{HoldDeletePercent: config.DefaultHoldDeletePercent, HoldDeleteMin: config.DefaultHoldDeleteMin}

type fixture struct {
	store  *index.Store
	safety config.Safety
	now    time.Time
	g      *Guard
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	store, err := index.Open(context.Background(), filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	f := &fixture{store: store, safety: defaultSafety, now: time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)}
	f.g = f.load(t)
	return f
}

// load opens a Guard over f's store, as a restart would.
func (f *fixture) load(t *testing.T) *Guard {
	t.Helper()
	g, err := Load(context.Background(), f.store, Options{
		Safety: func(string) config.Safety { return f.safety },
		Now:    func() time.Time { return f.now },
		PeerName: func(key string) string {
			if key == peer {
				return "laptop"
			}
			return ""
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// files returns an index listing of n live files, f00 to f(n-1).
func files(n int) map[string]index.FileRow {
	out := make(map[string]index.FileRow, n)
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("f%02d", i)
		out[p] = index.FileRow{ShareID: share, RelPath: p, Type: protocol.FileTypeFile}
	}
	return out
}

// scanDeleting returns a scan result that deletes the first k of existing's
// files, f00 to f(k-1).
func scanDeleting(existing map[string]index.FileRow, k int) *index.ScanResult {
	res := &index.ScanResult{}
	for i := 0; i < k; i++ {
		r := existing[fmt.Sprintf("f%02d", i)]
		r.Deleted = true
		res.Deleted = append(res.Deleted, r)
	}
	return res
}

func rowsOf(m map[string]index.FileRow) []index.FileRow {
	out := make([]index.FileRow, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	return out
}

func paths(k int) []string {
	out := make([]string, k)
	for i := range out {
		out[i] = fmt.Sprintf("f%02d", i)
	}
	return out
}

// holdOn returns the hold on link, if any.
func holdOn(g *Guard, link Link) (Hold, bool) {
	for _, h := range g.List() {
		if (Link{h.ShareID, h.PeerKey}) == link {
			return h, true
		}
	}
	return Hold{}, false
}

var own = Link{ShareID: share}
var fromPeer = Link{ShareID: share, PeerKey: peer}

func TestScanTriggers(t *testing.T) {
	dirs := files(1)
	for i := 0; i < 5; i++ {
		p := fmt.Sprintf("d%d", i)
		dirs[p] = index.FileRow{RelPath: p, Type: protocol.FileTypeDir}
	}
	parked := files(3)
	for i := 0; i < 30; i++ {
		p := fmt.Sprintf("ignored%02d", i)
		parked[p] = index.FileRow{RelPath: p, Type: protocol.FileTypeFile, Ignored: true}
	}

	for _, tc := range []struct {
		name     string
		safety   config.Safety
		existing map[string]index.FileRow
		result   func(map[string]index.FileRow) *index.ScanResult
		want     string // reason, or "" to apply
	}{
		{name: "nothing deleted", existing: files(30), result: func(m map[string]index.FileRow) *index.ScanResult { return scanDeleting(m, 0) }},
		{name: "bulk delete", existing: files(30), result: func(m map[string]index.FileRow) *index.ScanResult { return scanDeleting(m, 20) }, want: BulkDelete},
		{name: "over the percentage, under the minimum", existing: files(10), result: func(m map[string]index.FileRow) *index.ScanResult { return scanDeleting(m, 8) }},
		{name: "everything in a small share", existing: files(3), result: func(m map[string]index.FileRow) *index.ScanResult { return scanDeleting(m, 3) }, want: EverythingVanished},
		{name: "the only file", existing: files(1), result: func(m map[string]index.FileRow) *index.ScanResult { return scanDeleting(m, 1) }},
		{name: "configured thresholds", safety: config.Safety{HoldDeletePercent: 30, HoldDeleteMin: 3}, existing: files(10), result: func(m map[string]index.FileRow) *index.ScanResult { return scanDeleting(m, 4) }, want: BulkDelete},
		// Five of six rows go, but none of them is a file.
		{
			name: "directories are not files", safety: config.Safety{HoldDeletePercent: 50, HoldDeleteMin: 1}, existing: dirs,
			result: func(m map[string]index.FileRow) *index.ScanResult {
				res := &index.ScanResult{}
				for i := 0; i < 5; i++ {
					res.Deleted = append(res.Deleted, m[fmt.Sprintf("d%d", i)])
				}
				return res
			},
		},
		// All 3 live files go: the 30 parked rows don't make the share
		// bigger.
		{name: "parked rows are not live", existing: parked, result: func(m map[string]index.FileRow) *index.ScanResult { return scanDeleting(m, 3) }, want: EverythingVanished},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			if tc.safety != (config.Safety{}) {
				f.safety = tc.safety
			}
			apply, released := f.g.Scan(share, tc.existing, tc.result(tc.existing))
			h, held := holdOn(f.g, own)
			if apply == (tc.want != "") || released {
				t.Errorf("Scan = (apply %v, released %v), want apply %v", apply, released, tc.want == "")
			}
			if tc.want == "" && held {
				t.Errorf("held %+v, want no hold", h)
			}
			if tc.want != "" && (!held || h.Reason != tc.want) {
				t.Errorf("hold = %+v (held %v), want reason %q", h, held, tc.want)
			}
		})
	}
}

// A hold over deletes only clears once nothing would be deleted: putting
// back enough to get under the thresholds is the same disaster partly undone.
func TestHoldClearsOnlyWhenNothingWouldBeDeleted(t *testing.T) {
	for _, tc := range []struct {
		name               string
		live, first, later int
		reason             string
	}{
		{"bulk delete", 30, 20, 5, BulkDelete},
		{"everything vanished", 3, 3, 2, EverythingVanished},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			existing := files(tc.live)
			if apply, _ := f.g.Scan(share, existing, scanDeleting(existing, tc.first)); apply {
				t.Fatal("first scan applied, want it held")
			}
			if apply, _ := f.g.Scan(share, existing, scanDeleting(existing, tc.later)); apply {
				t.Fatal("partly fixed scan applied, want it held")
			}
			if h, _ := holdOn(f.g, own); h.Reason != tc.reason || h.Deletes != tc.later || h.Live != tc.live {
				t.Errorf("hold after a partial fix = %+v, want %s of %d of %d", h, tc.reason, tc.later, tc.live)
			}
			apply, released := f.g.Scan(share, existing, scanDeleting(existing, 0))
			if !apply || !released {
				t.Errorf("fixed scan = (apply %v, released %v), want both", apply, released)
			}
			if holds := f.g.List(); len(holds) != 0 {
				t.Errorf("holds after fixing = %+v, want none", holds)
			}
		})
	}
}

func TestConfirmLetsTheNextChangeThrough(t *testing.T) {
	f := newFixture(t)
	existing := files(30)
	f.g.Scan(share, existing, scanDeleting(existing, 20))

	link, err := f.g.Only(share)
	if err != nil || link != own {
		t.Fatalf("Only = %v, %v, want own link", link, err)
	}
	h, err := f.g.Confirm(link)
	if err != nil || !h.Confirmed {
		t.Fatalf("Confirm = %+v, %v, want a confirmed hold", h, err)
	}
	// It applies even if more has gone since: the user said to let the
	// share's deletes through.
	apply, released := f.g.Scan(share, existing, scanDeleting(existing, 25))
	if !apply || !released {
		t.Errorf("confirmed scan = (apply %v, released %v), want both", apply, released)
	}
	if holds := f.g.List(); len(holds) != 0 {
		t.Errorf("holds after the confirmed scan = %+v, want none", holds)
	}
}

func TestMissingRoot(t *testing.T) {
	f := newFixture(t)
	root := filepath.Join(t.TempDir(), "share")
	existing := files(5)

	f.g.ScanFailed(share, root, existing, errors.New("lstat: no such file"))
	h, held := holdOn(f.g, own)
	if !held || h.Reason != RootUnavailable || h.Confirmable() || h.Deletes != 5 {
		t.Fatalf("hold = %+v (held %v), want an unconfirmable root-unavailable hold of 5", h, held)
	}
	if _, err := f.g.Confirm(own); err == nil || errors.Is(err, ErrNotHeld) {
		t.Errorf("Confirm = %v, want a refusal that is not ErrNotHeld", err)
	} else if !strings.Contains(err.Error(), "nothing to confirm. Restore it") {
		t.Errorf("Confirm error %q doesn't say what to do", err)
	}

	// The folder comes back with one file still missing: what's missing
	// vanished with the rest, so it stays held, now as a change to confirm.
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if apply, _ := f.g.Scan(share, existing, scanDeleting(existing, 1)); apply {
		t.Fatal("scan of a partly restored root applied, want it held")
	}
	if h, _ := holdOn(f.g, own); h.Reason != EverythingVanished || !h.Confirmable() || h.Deletes != 1 {
		t.Errorf("hold = %+v, want a confirmable everything-vanished hold of 1", h)
	}
}

func TestScanFailuresThatAreNotHolds(t *testing.T) {
	f := newFixture(t)
	f.g.ScanFailed(share, t.TempDir(), files(5), errors.New("transient"))
	f.g.ScanFailed(share, filepath.Join(t.TempDir(), "gone"), files(0), errors.New("no such file"))
	if holds := f.g.List(); len(holds) != 0 {
		t.Errorf("holds = %+v, want none: a readable root and an empty index hold nothing", holds)
	}
}

func TestReservedDirTaken(t *testing.T) {
	for _, deletes := range []int{0, 1} {
		t.Run(fmt.Sprintf("then %d deletes", deletes), func(t *testing.T) {
			f := newFixture(t)
			f.g.ScanFailed(share, t.TempDir(), nil, fmt.Errorf("rescan: %w", syncsvc.ErrReservedDirTaken))
			h, held := holdOn(f.g, own)
			if !held || h.Reason != ReservedDirTaken || h.Confirmable() {
				t.Fatalf("hold = %+v (held %v), want an unconfirmable reserved-dir-taken hold", h, held)
			}
			// Once the directory is moved away a scan runs, and its hold
			// ends: its cause was never deletes.
			existing := files(30)
			apply, released := f.g.Scan(share, existing, scanDeleting(existing, deletes))
			if !apply || !released {
				t.Errorf("scan = (apply %v, released %v), want both", apply, released)
			}
		})
	}
}

func TestOwnHoldRefusesEveryPeerPass(t *testing.T) {
	f := newFixture(t)
	existing := files(30)
	f.g.Scan(share, existing, scanDeleting(existing, 20))

	if f.g.Pass(share, peer, rowsOf(existing), nil) {
		t.Error("Pass while this node's copy is held = true, want false")
	}
	if _, held := holdOn(f.g, fromPeer); held {
		t.Error("the peer's link was held too, want only this node's own")
	}
	if !f.g.Pass("other-share", peer, rowsOf(existing), nil) {
		t.Error("Pass for another share = false, want true")
	}
}

func TestPeerPassIsHeldAndClears(t *testing.T) {
	f := newFixture(t)
	local := rowsOf(files(25))
	if f.g.Pass(share, peer, local, paths(22)) {
		t.Fatal("Pass deleting 22 of 25 = true, want it held")
	}
	h, held := holdOn(f.g, fromPeer)
	if !held || h.Reason != BulkDelete || h.Deletes != 22 || h.Live != 25 {
		t.Fatalf("hold = %+v (held %v), want bulk-delete of 22 of 25 from the peer", h, held)
	}
	// A path the pass deletes that isn't a live file here removes nothing.
	if !f.g.Pass(share, peer, local, []string{"never-had-it"}) {
		t.Error("Pass deleting nothing live = false, want it applied")
	}
	if _, held := holdOn(f.g, fromPeer); held {
		t.Error("the peer's hold stayed after a pass that deletes nothing")
	}
}

func TestReTrippedHoldKeepsSince(t *testing.T) {
	f := newFixture(t)
	existing := files(30)
	f.g.Scan(share, existing, scanDeleting(existing, 20))
	first, _ := holdOn(f.g, own)

	f.now = f.now.Add(time.Hour)
	f.g.Scan(share, existing, scanDeleting(existing, 25))
	later, _ := holdOn(f.g, own)
	if later.Deletes != 25 || !later.Since.Equal(first.Since) {
		t.Errorf("re-tripped hold = %+v, want 25 deletes since %v", later, first.Since)
	}
}

func TestHoldsSurviveARestart(t *testing.T) {
	f := newFixture(t)
	existing := files(30)
	// The peer's link first: once this node's own copy is held, no pass
	// gets as far as holding a peer.
	f.g.Pass(share, peer, rowsOf(existing), paths(22))
	f.g.Scan(share, existing, scanDeleting(existing, 20))
	if _, err := f.g.Confirm(fromPeer); err != nil {
		t.Fatal(err)
	}

	g := f.load(t)
	if h, ok := holdOn(g, own); !ok || h.Reason != BulkDelete || h.Confirmed {
		t.Errorf("own hold after reload = %+v (found %v), want an unconfirmed bulk-delete", h, ok)
	}
	if h, ok := holdOn(g, fromPeer); !ok || !h.Confirmed {
		t.Errorf("peer hold after reload = %+v (found %v), want it confirmed", h, ok)
	}
}

func TestOnlyAndConfirmName(t *testing.T) {
	f := newFixture(t)
	if _, err := f.g.Only(share); !errors.Is(err, ErrNotHeld) {
		t.Errorf("Only with no hold = %v, want ErrNotHeld", err)
	}
	if _, err := f.g.Confirm(fromPeer); !errors.Is(err, ErrNotHeld) || !strings.Contains(err.Error(), "from laptop") {
		t.Errorf("Confirm of an unheld peer link = %v, want ErrNotHeld naming the peer", err)
	}

	existing := files(30)
	f.g.Pass(share, peer, rowsOf(existing), paths(22))
	f.g.Pass(share, "bbbb", rowsOf(existing), paths(22))
	if _, err := f.g.Only(share); err == nil || errors.Is(err, ErrNotHeld) {
		t.Errorf("Only with two holds = %v, want an error asking for the peer", err)
	}
}

func TestProblemAndRemedy(t *testing.T) {
	for _, tc := range []struct {
		reason, problem, remedy string
	}{
		{RootUnavailable, "the folder is missing or unreadable", "restore it, or remove the share"},
		{ReservedDirTaken, "the folder has a .syncat directory that syncat did not create", "move it away"},
		{BulkDelete, "22 of 25 files would be deleted", "fix the folder, or let the deletes through"},
		{EverythingVanished, "22 of 25 files would be deleted", "fix the folder, or let the deletes through"},
	} {
		h := Hold{index.Hold{Reason: tc.reason, Deletes: 22, Live: 25}}
		if h.Problem() != tc.problem || h.Remedy() != tc.remedy {
			t.Errorf("%s: (%q, %q), want (%q, %q)", tc.reason, h.Problem(), h.Remedy(), tc.problem, tc.remedy)
		}
	}
}
