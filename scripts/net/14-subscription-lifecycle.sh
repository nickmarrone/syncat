#!/usr/bin/env bash
# Pausing, resuming, and removing a subscription.
#
# SPEC.md §8: `subscription pause` stops sync for that one subscription
# without touching the files, `resume` catches up on everything missed, and
# `rm` stops sync for good while leaving the local copy in place.
#
# Alice offers two shares and bob subscribes to both. The second one is the
# tracer: while the first is paused or removed, a change on the second
# arriving proves the connection is syncing, so "the paused one did not
# change" means something.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init subscription-lifecycle

node_add alice
node_add bob
DOCS="$(node_dir alice docs)"
OTHER="$(node_dir alice other)"
BOB_DOCS="$(node_dir bob docs)"
BOB_OTHER="$(node_dir bob other)"
node_start alice
node_start bob
pair_nodes alice bob

DOCS_ID="$(share_and_subscribe alice "$DOCS" docs rw bob "$BOB_DOCS" mirror)"
share_and_subscribe alice "$OTHER" other rw bob "$BOB_OTHER" mirror >/dev/null

sub_paused() {
	[ "$(nx bob subscription ls --json 2>/dev/null | jq -r --arg s "$DOCS_ID" '.[]? | select(.share_id==$s) | .paused')" = "$1" ]
}

log "Both subscriptions sync"
echo "one" >"$DOCS/one.txt"
echo "t0" >"$OTHER/tracer.txt"
wait_for "$SYNC_TIMEOUT" "one.txt to reach bob" file_content_is "$BOB_DOCS/one.txt" "one"
wait_for "$SYNC_TIMEOUT" "the tracer share to reach bob" file_content_is "$BOB_OTHER/tracer.txt" "t0"

log "Pausing docs stops it, and only it"
nx bob subscription pause alice docs >/dev/null || fail "subscription pause failed"
wait_for 30 "docs to be reported paused" sub_paused true
echo "while paused" >"$DOCS/two.txt"
echo "bob while paused" >"$BOB_DOCS/from-bob.txt"
echo "t1" >"$OTHER/tracer.txt"
wait_for "$SYNC_TIMEOUT" "the tracer share to keep syncing" file_content_is "$BOB_OTHER/tracer.txt" "t1"
hold_for 10 "two.txt to stay off bob while paused" path_absent "$BOB_DOCS/two.txt"
path_absent "$DOCS/from-bob.txt" || fail "bob's write crossed a paused subscription"
file_content_is "$BOB_DOCS/one.txt" "one" || fail "pausing touched bob's existing files"
info "ok: nothing crossed the paused subscription in either direction, and bob's files are untouched"

log "Resuming catches up both ways"
nx bob subscription resume alice docs >/dev/null || fail "subscription resume failed"
wait_for 30 "docs to be reported running" sub_paused false
wait_for "$SYNC_TIMEOUT" "the change made while paused to reach bob" file_content_is "$BOB_DOCS/two.txt" "while paused"
wait_for "$SYNC_TIMEOUT" "bob's change made while paused to reach alice" file_content_is "$DOCS/from-bob.txt" "bob while paused"

log "Removing the subscription keeps bob's copy and stops sync"
nx bob subscription rm alice docs >/dev/null || fail "subscription rm failed"
wait_for 30 "bob to have one subscription left" test "$(subscription_count bob)" = 1
echo "after rm" >"$DOCS/three.txt"
echo "t2" >"$OTHER/tracer.txt"
wait_for "$SYNC_TIMEOUT" "the tracer share to keep syncing" file_content_is "$BOB_OTHER/tracer.txt" "t2"
path_absent "$BOB_DOCS/three.txt" || fail "a change reached bob's folder after the subscription was removed"
for f in one.txt two.txt from-bob.txt; do
	path_present "$BOB_DOCS/$f" || fail "removing the subscription deleted bob's $f"
done
path_present "$DOCS/one.txt" || fail "removing bob's subscription deleted alice's files"
info "ok: bob kept his copy, alice kept hers, and nothing more crossed"

assert_mutually_clean alice bob

harness_pass "pause stopped one subscription only, resume caught up both ways, and rm kept both copies"
