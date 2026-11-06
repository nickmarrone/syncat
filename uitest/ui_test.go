package uitest

// The tests, one per MANUAL-TESTS.md §10 behaviour. alice and bob are the
// harness's paired daemons; each test adds what it needs (shares,
// subscriptions, files) under names of its own and removes it again, so the
// tests do not depend on running in any order.

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// alice and bob, with a browser on alice's UI.
func pair(t *testing.T) (alice, bob *node) {
	e := requireEnv(t)
	return e.node("alice"), e.node("bob")
}

// peerRow is a JS expression for the peer row whose summary names peer.
func peerRow(peer string) string {
	return `__ut.all('.peer-row').find((r) => r.querySelector('.peer-name').textContent === ` + jsString(peer) + `)`
}

// inPeerRow is the first visible sel (containing text, if given) in peer's row.
func inPeerRow(peer, sel, text string) string {
	t := "undefined"
	if text != "" {
		t = jsString(text)
	}
	return `(() => { const r = ` + peerRow(peer) + `; return r ? __ut.find(` + jsString(sel) + `, ` + t + `, r) : null; })()`
}

// peerMetaMatches is a JS condition: peer's collapsed summary line matches
// the regexp re. A count rather than an exact string, because a share an
// earlier test removed can take a moment to leave the other node's view.
func peerMetaMatches(peer, re string) string {
	return `(() => { const m = ` + inPeerRow(peer, ".peer-meta", "") + `; return !!m && new RegExp(` + jsString(re) + `).test(m.textContent); })()`
}

// TestStatusRendersAndRefreshes:peers, shares and subscriptions render, and
// changes made behind the UI's back (here, through the CLI) appear on the
// next 2s poll without a reload.
func TestStatusRendersAndRefreshes(t *testing.T) {
	alice, bob := pair(t)
	ui := newBrowser(t)
	ui.open(alice.url(t))
	// A reload would wipe this; it still being set at the end proves every
	// change below arrived through the poll.
	ui.do("mark the page", `(() => { window.__noReload = true; return true; })()`)

	ui.wait("bob's peer row, connected", `!!(`+inPeerRow("bob", ".badge.state-connected", "Connected")+`)`)

	// A peer added behind the UI's back appears, and goes again.
	carol := testEnv.node("carol")
	alice.cli(t, "peer", "add", carol.cli(t, "token"), "--name", "carol-poll")
	t.Cleanup(func() { alice.run("peer", "rm", "carol-poll") })
	ui.wait("carol-poll's row to appear on the poll", `!!(`+peerRow("carol-poll")+`)`)
	alice.cli(t, "peer", "rm", "carol-poll")
	ui.wait("carol-poll's row to go on the poll", `!(`+peerRow("carol-poll")+`)`)

	// A share appears in the Shares view, and goes again.
	ui.route("/shares")
	dir := alice.dir(t, "poll-share")
	id := alice.addShare(t, dir, "poll-share", "--perm", "rw")
	ui.wait("poll-share to appear on the poll", `!!__ut.find('#app .card .title', 'poll-share')`)
	ui.wait("poll-share's path under its name", `!!__ut.find('#app .card .mono', `+jsString(dir)+`)`)

	// bob subscribing shows up in bob's UI, and as an offered share on
	// bob's row for alice.
	bobUI := newBrowser(t)
	bobUI.open(bob.url(t) + "#/shares")
	bobUI.wait("bob's empty subscriptions list", `!!__ut.find('#app .empty-state', 'not subscribed')`)
	bobDir := bob.dir(t, "poll-sub")
	bob.subscribe(t, alice, id, bobDir, "mirror")
	bobUI.wait("bob's subscription to appear on the poll", `!!__ut.find('.subscription-item .title', 'poll-share from alice')`)
	bobUI.wait("the subscription's mode and access", `!!__ut.find('.subscription-item', 'mode: mirror')`)
	bobUI.route("/")
	bobUI.wait("alice's row to count the offered share and the subscription",
		peerMetaMatches("alice", `\d+ shares? offered · 1 subscription`))

	alice.cli(t, "share", "rm", id)
	ui.wait("poll-share to go on the poll", `!__ut.find('#app .card .title', 'poll-share')`)
	if !value[bool](ui, `window.__noReload === true`) {
		t.Fatal("the page reloaded; the changes should have arrived through the poll")
	}
}

// TestNodeToken: the node token is truncated by default and expands to the
// full, wrapped token; Copy copies the whole token in either state; and the
// node card never scrolls sideways, even on a phone-width screen.
func TestNodeToken(t *testing.T) {
	alice, _ := pair(t)
	// The running daemon's token (what the UI shows), not `syncat token`'s:
	// the daemon's carries its live tailcat address, so the two differ.
	token, _ := alice.status(t)["node_token"].(string)
	if len([]rune(token)) <= 24 {
		t.Fatalf("token %q is too short to be truncated; the test needs a real one", token)
	}
	ui := newBrowser(t)
	ui.open(alice.url(t))

	tokenText := `document.querySelector('.token-box code').textContent`
	// Record what the UI hands the clipboard, then let the real write go
	// ahead (it must succeed, or the button would say "Copy failed").
	// Headless Chrome needs the clipboard granted and the page focused, as
	// a person's browser would have it after a click.
	ui.allowClipboard(alice.url(t))
	// Record what the UI hands the clipboard and how the write ends, rather
	// than reading the button's "Copied!", which the next poll re-render
	// replaces within 2s.
	ui.do("record clipboard writes", `(() => {
	  const orig = navigator.clipboard.writeText.bind(navigator.clipboard);
	  navigator.clipboard.writeText = (v) => {
	    window.__copied = v;
	    return orig(v).then(() => { window.__copyResult = 'ok'; }, (e) => { window.__copyResult = 'failed: ' + e; throw e; });
	  };
	  return true; })()`)
	noSideScroll := func(when string) {
		t.Helper()
		ui.wait("no horizontal overflow in the node card ("+when+")", `(() => {
		  const card = document.querySelector('.token-box').closest('.card');
		  const doc = document.documentElement;
		  return card.scrollWidth <= card.clientWidth && doc.scrollWidth <= doc.clientWidth; })()`)
	}
	copyGives := func(when string) {
		t.Helper()
		ui.do("forget the last copy", `(() => { window.__copied = null; window.__copyResult = null; return true; })()`)
		ui.click("Copy ("+when+")", `__ut.find('.token-box button', 'Copy')`)
		ui.wait("the clipboard write to finish ("+when+")", `window.__copyResult != null`)
		if res := value[string](ui, `window.__copyResult`); res != "ok" {
			t.Fatalf("Copy (%s): the clipboard write %s", when, res)
		}
		if got := value[string](ui, `window.__copied`); got != token {
			t.Fatalf("Copy (%s) put %q on the clipboard, want the full token %q", when, got, token)
		}
		if got := ui.clipboard(); got != token {
			t.Fatalf("Copy (%s): the clipboard holds %q, want the full token %q", when, got, token)
		}
	}

	truncated := []rune(token)[:24]
	ui.wait("the token to show truncated", tokenText+` === `+jsString(string(truncated)+"…"))
	ui.wait("the toggle to offer the full token", `!!__ut.find('.token-box button[aria-expanded="false"]', 'Show full')`)
	noSideScroll("truncated")
	copyGives("truncated")

	ui.click("Show full", `__ut.find('.token-box button', 'Show full')`)
	ui.wait("the full token, wrapped", tokenText+` === `+jsString(token)+` && document.querySelector('.token-box code').classList.contains('wrap')`)
	noSideScroll("expanded")
	copyGives("expanded")
	ui.waitPolls(2)
	ui.wait("the token to stay expanded across polls", tokenText+` === `+jsString(token))

	// A phone-width screen, where an unwrapped token would overflow.
	ui.viewport(375, 800)
	noSideScroll("expanded, 375px wide")
	ui.click("Hide", `__ut.find('.token-box button', 'Hide')`)
	ui.wait("the token to be truncated again", tokenText+` === `+jsString(string(truncated)+"…"))
	noSideScroll("truncated, 375px wide")
}

// TestPeerRowExpandCollapse: a peer row expands and collapses, stays as it
// was left across polls, and only the expanded row offers Remove peer and
// the list of shares the peer offers.
func TestPeerRowExpandCollapse(t *testing.T) {
	alice, bob := pair(t)
	shareID := bob.addShare(t, bob.dir(t, "offered"), "bob-offer")
	ui := newBrowser(t)
	ui.open(alice.url(t))
	eventually(t, discoveryTimeout, "alice to see bob's offered share", alice.seesRemoteShare(shareID))
	ui.wait("bob's collapsed row to count the offered share", peerMetaMatches("bob", `\d+ shares? offered`))

	collapsed := func(when string) {
		t.Helper()
		ui.wait("bob's row collapsed ("+when+")", `(() => { const r = `+peerRow("bob")+`; return !!r &&
		  r.querySelector('.peer-summary').getAttribute('aria-expanded') === 'false' &&
		  !__ut.find('button', 'Remove peer', r) && !__ut.find('h3', 'Shares they offer', r) &&
		  !r.querySelector('.peer-detail') && !r.textContent.includes('bob-offer'); })()`)
	}
	expanded := func(when string) {
		t.Helper()
		ui.wait("bob's row expanded ("+when+")", `(() => { const r = `+peerRow("bob")+`; return !!r &&
		  r.querySelector('.peer-summary').getAttribute('aria-expanded') === 'true' &&
		  !!__ut.find('button.danger', 'Remove peer', r) && !!__ut.find('h3', 'Shares they offer', r) &&
		  !!__ut.within('.peer-row.expanded li', 'bob-offer', 'button', 'Subscribe'); })()`)
	}

	collapsed("initially")
	ui.click("bob's row", inPeerRow("bob", ".peer-summary", ""))
	expanded("after a click")
	ui.waitPolls(2)
	expanded("after two polls")

	// Remove peer asks first; cancelling leaves bob in place.
	ui.click("Remove peer", inPeerRow("bob", "button.danger", "Remove peer"))
	ui.wait("the confirmation", `!!__ut.find('dialog[open]', 'Remove peer "bob"?')`)
	ui.click("Cancel", `__ut.find('dialog[open] button', 'Cancel')`)
	ui.wait("the confirmation to close", `!document.querySelector('dialog[open]')`)
	alice.peerKey(t, "bob") // still configured
	expanded("after cancelling Remove")

	ui.click("bob's row", inPeerRow("bob", ".peer-summary", ""))
	collapsed("after a second click")
	ui.waitPolls(2)
	collapsed("after two more polls")
}

// withoutKeys is m with keys removed, for comparing config entries that
// legitimately differ only in their ids, names or paths.
func withoutKeys(m map[string]any, keys ...string) map[string]any {
	c := maps.Clone(m)
	for _, k := range keys {
		delete(c, k)
	}
	return c
}

// configEntry is the entry of cfg[list] whose field equals want.
func configEntry(t *testing.T, cfg map[string]any, list, field, want string) map[string]any {
	t.Helper()
	items, _ := cfg[list].([]any)
	for _, it := range items {
		m, _ := it.(map[string]any)
		if m != nil && m[field] == want {
			return m
		}
	}
	t.Fatalf("config.json %s has no entry with %s=%q: %v", list, field, want, items)
	return nil
}

func sameConfig(t *testing.T, what string, ui, cli map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(ui, cli) {
		t.Fatalf("%s added through the UI differs from one added with the CLI:\n  UI:  %v\n  CLI: %v", what, ui, cli)
	}
	t.Logf("ok: %s from the UI matches the CLI's: %v", what, ui)
}

// TestAddThroughUIMatchesCLI: adding a peer, a share and a subscription
// through the UI writes the same config.json entry the CLI writes for the
// same request.
func TestAddThroughUIMatchesCLI(t *testing.T) {
	alice, bob := pair(t)
	carol := testEnv.node("carol")
	carolToken := carol.cli(t, "token")

	t.Run("share", func(t *testing.T) {
		ui := newBrowser(t)
		ui.open(alice.url(t) + "#/shares")
		uiDir, cliDir := alice.dir(t, "ui-share"), alice.dir(t, "cli-share")
		ui.fill("the share path", `document.getElementById('share-path-input')`, uiDir)
		ui.fill("the share name", `document.getElementById('share-name-input')`, "ui-share")
		ui.fill("the permission", `document.getElementById('share-perm-input')`, "read-write")
		ui.click("Require approval", `document.getElementById('share-approval-input')`)
		t.Cleanup(func() { alice.run("share", "rm", "ui-share") })
		ui.click("Add share", `__ut.find('button[type=submit]', 'Add share')`)
		ui.toast("Share added")
		ui.wait("ui-share in the list", `!!__ut.find('#app .card .title', 'ui-share')`)
		alice.addShare(t, cliDir, "cli-share", "--perm", "rw", "--approval")

		cfg := alice.config(t)
		uiEntry := configEntry(t, cfg, "shares", "name", "ui-share")
		if uiEntry["path"] != uiDir {
			t.Fatalf("the UI's share has path %v, want %s", uiEntry["path"], uiDir)
		}
		sameConfig(t, "a read-write share requiring approval",
			withoutKeys(uiEntry, "id", "name", "path"),
			withoutKeys(configEntry(t, cfg, "shares", "name", "cli-share"), "id", "name", "path"))
	})

	t.Run("peer", func(t *testing.T) {
		ui := newBrowser(t)
		ui.open(alice.url(t))
		t.Cleanup(func() { alice.run("peer", "rm", "carol") })
		t.Cleanup(func() { bob.run("peer", "rm", "carol") })
		ui.fill("the peer token", `document.getElementById('peer-token-input')`, carolToken)
		ui.fill("the peer name", `document.getElementById('peer-name-input')`, "carol")
		ui.click("Add peer", `__ut.find('button[type=submit]', 'Add peer')`)
		ui.toast("Peer added")
		ui.wait("carol's row", `!!(`+peerRow("carol")+`)`)
		ui.wait("the form to clear", `document.getElementById('peer-token-input').value === ''`)
		bob.cli(t, "peer", "add", carolToken, "--name", "carol")

		// The same peer added to two nodes: the entries are identical.
		sameConfig(t, "a peer",
			configEntry(t, alice.config(t), "peers", "name", "carol"),
			configEntry(t, bob.config(t), "peers", "name", "carol"))
	})

	t.Run("subscription", func(t *testing.T) {
		uiShare := alice.addShare(t, alice.dir(t, "sub-ui"), "sub-ui", "--perm", "rw")
		cliShare := alice.addShare(t, alice.dir(t, "sub-cli"), "sub-cli", "--perm", "rw")
		uiDir, cliDir := bob.dir(t, "sub-ui"), bob.dir(t, "sub-cli")
		t.Cleanup(func() { bob.run("subscription", "rm", "alice", uiShare) })

		ui := newBrowser(t)
		ui.open(bob.url(t))
		eventually(t, discoveryTimeout, "bob to see sub-ui", bob.seesRemoteShare(uiShare))
		ui.click("alice's row", inPeerRow("alice", ".peer-summary", ""))
		ui.click("Subscribe on sub-ui", `__ut.within('.peer-row.expanded li', 'sub-ui', 'button', 'Subscribe')`)
		ui.wait("the subscribe dialog", `!!__ut.find('dialog[open] h2', 'Subscribe to "sub-ui"')`)
		ui.fill("the local path", `__ut.find('dialog[open] input')`, uiDir)
		ui.fill("the mode", `__ut.find('dialog[open] select')`, "receive-only")
		ui.click("Subscribe", `__ut.find('dialog[open] button.primary', 'Subscribe')`)
		ui.toast("Subscribed to sub-ui")
		ui.wait("the dialog to close", `!document.querySelector('dialog[open]')`)
		ui.wait("the offered share to show its local path", `!!__ut.within('.peer-row.expanded li', 'sub-ui', '.mono', `+jsString(uiDir)+`)`)
		bob.subscribe(t, alice, cliShare, cliDir, "receive")

		cfg := bob.config(t)
		uiEntry := configEntry(t, cfg, "subscriptions", "share_id", uiShare)
		if uiEntry["local_path"] != uiDir {
			t.Fatalf("the UI's subscription has local_path %v, want %s", uiEntry["local_path"], uiDir)
		}
		sameConfig(t, "a receive-only subscription",
			withoutKeys(uiEntry, "share_id", "local_path"),
			withoutKeys(configEntry(t, cfg, "subscriptions", "share_id", cliShare), "share_id", "local_path"))
	})
}

// openSubscribeDialog offers a fresh share from alice, opens bob's UI, and
// opens the subscribe dialog for that share from alice's expanded row.
func openSubscribeDialog(t *testing.T, alice, bob *node, shareName string) *tab {
	t.Helper()
	shareID := alice.addShare(t, alice.dir(t, shareName), shareName, "--perm", "rw")
	ui := newBrowser(t)
	ui.open(bob.url(t))
	eventually(t, discoveryTimeout, "bob to see "+shareName, bob.seesRemoteShare(shareID))
	ui.click("alice's row", inPeerRow("alice", ".peer-summary", ""))
	ui.click("Subscribe on "+shareName, `__ut.within('.peer-row.expanded li', `+jsString(shareName)+`, 'button', 'Subscribe')`)
	ui.wait("the subscribe dialog", `!!__ut.find('dialog[open] h2', `+jsString(`Subscribe to "`+shareName+`"`)+`)`)
	return ui
}

// dialogError is the subscribe dialog's error line: its second faint
// paragraph, after "from alice".
const dialogError = `document.querySelector('dialog[open]').querySelectorAll('p.faint')[1].textContent`

// submitSubscribe enters path in the open subscribe dialog, presses
// Subscribe, and returns the error the dialog shows. It fails the test if
// that error is a raw JSON blob or the dialog closes.
func submitSubscribe(t *testing.T, ui *tab, path string) string {
	t.Helper()
	ui.do("clear the last error", `(() => { document.querySelector('dialog[open]').querySelectorAll('p.faint')[1].textContent = ''; return true; })()`)
	ui.fill("the local path", `__ut.find('dialog[open] input')`, path)
	ui.click("Subscribe", `__ut.find('dialog[open] button.primary', 'Subscribe')`)
	ui.wait("an error under the form for "+jsString(path), `(`+dialogError+`) !== ''`)
	msg := value[string](ui, dialogError)
	t.Logf("dialog says, for %q: %s", path, msg)
	if strings.ContainsAny(msg, "{}") || strings.Contains(msg, `"error"`) || strings.Contains(msg, `"code"`) {
		t.Errorf("the dialog shows a raw JSON error for %q: %s", path, msg)
	}
	ui.wait("the dialog to stay open, with Subscribe enabled again", `!!document.querySelector('dialog[open]') && !__ut.find('dialog[open] button.primary').disabled`)
	return msg
}

// TestSubscribeDialogBadInput: the subscribe dialog answers bad input with a
// readable sentence, not a JSON error blob, stays open so the input can be
// fixed, and adds no subscription.
func TestSubscribeDialogBadInput(t *testing.T) {
	alice, bob := pair(t)
	ownDir := bob.dir(t, "own-share")
	bob.addShare(t, ownDir, "bob-own")
	before := bob.subscriptionCount(t)
	ui := openSubscribeDialog(t, alice, bob, "dialog-share")

	if msg := submitSubscribe(t, ui, ""); msg != "Local path is required." {
		t.Errorf("an empty path gives %q, want %q", msg, "Local path is required.")
	}
	if msg := submitSubscribe(t, ui, "relative/sub"); !strings.Contains(msg, "must be absolute") {
		t.Errorf("a relative path gives %q, want it to say the path must be absolute", msg)
	}
	if msg := submitSubscribe(t, ui, filepath.Join(ownDir, "inner")); !strings.Contains(msg, "overlaps local share") {
		t.Errorf("a path inside one of bob's own shares gives %q, want it to say it overlaps that share", msg)
	}

	ui.click("Cancel", `__ut.find('dialog[open] button', 'Cancel')`)
	ui.wait("the dialog to close", `!document.querySelector('dialog[open]')`)
	if n := bob.subscriptionCount(t); n != before {
		t.Errorf("bob has %d subscriptions after only bad input, want %d", n, before)
	}
}

// TestSubscribeRejectedInputLeavesNoTrace: a subscription the daemon refuses
// creates nothing on disk, neither relative to the daemon's working
// directory (the runner starts bob from the work dir) nor inside a share.
func TestSubscribeRejectedInputLeavesNoTrace(t *testing.T) {
	t.Skip("see #78: a rejected subscription still creates its folder")
	alice, bob := pair(t)
	ownDir := bob.dir(t, "trace-share")
	bob.addShare(t, ownDir, "bob-trace")
	ui := openSubscribeDialog(t, alice, bob, "trace-dialog-share")

	submitSubscribe(t, ui, "relative-trace/sub")
	if rel := filepath.Join(testEnv.work, "relative-trace"); !pathAbsent(rel)() {
		t.Errorf("a refused relative path created %s in the daemon's working directory", rel)
	}
	inner := filepath.Join(ownDir, "inner-trace")
	submitSubscribe(t, ui, inner)
	if !pathAbsent(inner)() {
		t.Errorf("a refused overlapping path created %s inside bob's share", inner)
	}
}

// TestRenamePersists: renaming the node from the UI survives a reload of the
// page and a restart of the daemon.
func TestRenamePersists(t *testing.T) {
	requireEnv(t)
	carol := testEnv.node("carol")
	original := carol.config(t)["node_name"].(string)
	carol.start(t)
	t.Cleanup(func() {
		if carol.proc != nil {
			carol.api(t, "PATCH", "/api/node", map[string]string{"name": original})
		}
	})

	ui := newBrowser(t)
	ui.open(carol.url(t))
	ui.wait("the current name in the name field", `document.getElementById('node-name-input').value === `+jsString(original))
	ui.fill("the node name", `document.getElementById('node-name-input')`, "carol-renamed")
	ui.click("Save", `__ut.within('.card', 'This node', 'button.primary', 'Save')`)
	ui.toast("Node renamed")

	ui.reload()
	ui.wait("the new name after a reload", `document.getElementById('node-name-input').value === 'carol-renamed'`)
	if got := carol.status(t)["node_name"]; got != "carol-renamed" {
		t.Fatalf("the daemon reports node_name %v, want carol-renamed", got)
	}

	carol.stop(t)
	carol.start(t)
	ui.reload()
	ui.wait("the new name after a daemon restart", `document.getElementById('node-name-input').value === 'carol-renamed'`)
}

// TestHoldBanner: a held share shows a banner on every page, and letting the
// deletes through from the banner clears it.
func TestHoldBanner(t *testing.T) {
	alice, bob := pair(t)
	dir := alice.dir(t, "hold")
	for i := 1; i <= 25; i++ {
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%02d.txt", i)), []byte(fmt.Sprintf("file %d\n", i)), 0o644)
	}
	id := alice.addShare(t, dir, "hold-share", "--perm", "rw")
	// bob receiving all 25 is the proof alice has scanned them; deleting
	// before that would be a change from nothing, and nothing to hold.
	bobDir := bob.dir(t, "hold")
	bob.subscribe(t, alice, id, bobDir, "receive")
	eventually(t, syncTimeout, "all 25 files to reach bob", func() bool {
		ents, _ := os.ReadDir(bobDir)
		n := 0
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), "f") {
				n++
			}
		}
		return n == 25
	})

	ui := newBrowser(t)
	ui.open(alice.url(t))
	ui.wait("no hold banner yet", `!__ut.find('.hold-banner')`)
	for i := 1; i <= 22; i++ {
		os.Remove(filepath.Join(dir, fmt.Sprintf("f%02d.txt", i)))
	}
	banner := `__ut.find('.hold-banner', '"hold-share" is on hold: 22 of 25 files would be deleted')`
	ui.wait("the hold banner on the dashboard", `!!`+banner)
	ui.route("/shares")
	ui.wait("the hold banner on the shares page", `!!`+banner)

	ui.click("Let the deletes through", `__ut.find('.hold-banner button.danger', 'Let the deletes through')`)
	ui.wait("the confirmation", `!!__ut.find('dialog[open]', 'Delete 22 file(s) from "hold-share" on every peer?')`)
	ui.click("Confirm", `__ut.find('dialog[open] button.danger', 'Confirm')`)
	ui.toast("Deletes confirmed")
	ui.wait("the banner to clear on the shares page", `!__ut.find('.hold-banner', 'hold-share')`)
	ui.route("/")
	ui.wait("the banner to stay clear on the dashboard", `!__ut.find('.hold-banner', 'hold-share')`)
}

// TestTrashUndo: what a peer deleted is listed in the Trash and safety
// panel, and Undo puts it back, on both nodes.
func TestTrashUndo(t *testing.T) {
	alice, bob := pair(t)
	aliceDir := alice.dir(t, "trash")
	id := alice.addShare(t, aliceDir, "trash-share", "--perm", "rw")
	bobDir := bob.dir(t, "trash")
	bob.subscribe(t, alice, id, bobDir, "mirror")

	os.WriteFile(filepath.Join(aliceDir, "doomed.txt"), []byte("keep me\n"), 0o644)
	eventually(t, syncTimeout, "doomed.txt to reach bob", fileContentIs(filepath.Join(bobDir, "doomed.txt"), "keep me\n"))
	os.Remove(filepath.Join(aliceDir, "doomed.txt"))
	eventually(t, syncTimeout, "the delete to reach bob", pathAbsent(filepath.Join(bobDir, "doomed.txt")))

	ui := newBrowser(t)
	ui.open(bob.url(t) + "#/shares")
	sub := `'.subscription-item'`
	ui.click("Trash and safety on the subscription", `__ut.within(`+sub+`, 'trash-share', 'button.link', 'Trash and safety')`)
	event := `__ut.within(` + sub + `, 'trash-share', 'li.trash-event', 'alice deleted 1 file')`
	ui.wait("the delete in bob's trash", `!!`+event)
	ui.waitPolls(2)
	ui.wait("the panel to stay open across polls", `!!`+event)

	ui.click("the event", `(() => { const ev = `+event+`; return ev ? __ut.find('button.link', 'deleted 1 file', ev) : null; })()`)
	ui.wait("the event's file list", `!!__ut.within('li.trash-event', 'alice deleted 1 file', '.trash-files .mono', 'doomed.txt')`)
	ui.click("Undo", `(() => { const ev = `+event+`; return ev ? __ut.find('button', 'Undo', ev) : null; })()`)
	ui.toast("Restored 1 file")
	eventually(t, syncTimeout, "doomed.txt to be back on bob", fileContentIs(filepath.Join(bobDir, "doomed.txt"), "keep me\n"))
	eventually(t, syncTimeout, "the restore to reach alice", fileContentIs(filepath.Join(aliceDir, "doomed.txt"), "keep me\n"))
	ui.wait("the event to leave the trash", `!`+event)
}

// TestDaemonStopAndRestart: stopping the daemon under an open UI degrades
// to a clear "unreachable" state without throwing, keeps the last view on
// screen, and recovers by itself when the daemon comes back.
func TestDaemonStopAndRestart(t *testing.T) {
	requireEnv(t)
	carol := testEnv.node("carol")
	carol.start(t)

	ui := newBrowser(t)
	ui.open(carol.url(t))
	ui.wait("the connected indicator", `document.getElementById('conn-indicator').textContent.startsWith('connected · uptime')`)

	carol.stop(t)
	ui.wait("the indicator to say the daemon is unreachable",
		`(() => { const c = document.getElementById('conn-indicator'); return c.textContent === 'daemon unreachable' && c.classList.contains('error'); })()`)
	ui.wait("a lost-connection banner", `!!__ut.find('.error-banner', 'Lost connection to the daemon')`)
	ui.wait("the last view to stay on screen", `!!document.getElementById('node-name-input') && !!__ut.find('#app h2', 'This node')`)
	ui.waitPolls(2)
	ui.wait("the degraded state to hold across polls", `!!__ut.find('.error-banner', 'Lost connection to the daemon')`)

	carol.start(t)
	ui.wait("the indicator to recover", `document.getElementById('conn-indicator').textContent.startsWith('connected · uptime')`)
	ui.wait("the banner to clear", `!__ut.find('.error-banner')`)
	ui.wait("a working view", `!!document.getElementById('node-name-input')`)
	// And the recovered page still works: a reload gets a token and renders.
	ui.reload()
	ui.wait("the connected indicator after a reload", `document.getElementById('conn-indicator').textContent.startsWith('connected · uptime')`)
}

// TestTwoTabs: two tabs on the same daemon each act on it, and each picks up
// the other's change when brought to the front, so neither is left showing
// state the daemon no longer has.
func TestTwoTabs(t *testing.T) {
	alice, _ := pair(t)
	id := alice.addShare(t, alice.dir(t, "tabs"), "tabs-share")

	a := newBrowser(t)
	a.open(alice.url(t) + "#/shares")
	b := a.newTab("tab2")
	b.front()
	b.open(alice.url(t) + "#/shares")
	card := func(name string) string {
		return `__ut.all('#app .card').find((c) => { const t = c.querySelector('.title'); return t && t.textContent === ` + jsString(name) + `; })`
	}
	b.wait("tabs-share in tab 2", `!!`+card("tabs-share"))

	// Tab 1 renames the share.
	a.front()
	a.wait("tabs-share in tab 1", `!!`+card("tabs-share"))
	a.fill("the share's name", `(() => { const c = `+card("tabs-share")+`; return c ? c.querySelector('.field input[type=text]') : null; })()`, "tabs-renamed")
	a.click("Save", `(() => { const c = `+card("tabs-share")+`; return c ? __ut.find('button', 'Save', c) : null; })()`)
	a.toast("Share renamed")
	a.wait("tab 1 to show the new name", `!!`+card("tabs-renamed"))

	// Tab 2 follows when it comes to the front, and changes the permission.
	b.front()
	b.wait("tab 2 to show the rename", `!!`+card("tabs-renamed")+` && !`+card("tabs-share"))
	b.fill("the permission", `(() => { const c = `+card("tabs-renamed")+`; return c ? c.querySelector('select') : null; })()`, "read-write")
	b.toast("Permission updated")
	b.wait("tab 2 to show read-write", `(() => { const c = `+card("tabs-renamed")+`; return !!c && c.querySelector('select').value === 'read-write'; })()`)
	// Moving focus off the select lets tab 2's polls re-render again.
	b.do("blur the select", `(() => { document.activeElement && document.activeElement.blur(); return true; })()`)

	// Tab 1 follows too, and both agree with the daemon.
	a.front()
	a.wait("tab 1 to show read-write", `(() => { const c = `+card("tabs-renamed")+`; return !!c && c.querySelector('select').value === 'read-write'; })()`)
	for _, s := range alice.shares(t) {
		if s.ID == id && (s.Name != "tabs-renamed" || s.Permission != "read-write") {
			t.Fatalf("the daemon has %+v, want name tabs-renamed and read-write", s)
		}
	}
	b.front()
	b.waitPolls(1)
	b.wait("tab 2 to still agree", `(() => { const c = `+card("tabs-renamed")+`; return !!c && c.querySelector('select').value === 'read-write'; })()`)
}
