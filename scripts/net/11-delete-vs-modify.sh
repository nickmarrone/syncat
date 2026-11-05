#!/usr/bin/env bash
# One side deletes a file while the other edits it: the edit wins.
#
# SPEC.md §5: a delete concurrent with a modify resolves to the modify, and
# the file comes back on the side that deleted it. Getting this wrong loses
# the only copy of someone's latest work, which is why it gets a scenario of
# its own, in both directions (offerer deletes, and subscriber deletes).
#
# As in scenario 05, both nodes go offline before diverging, so each side
# has indexed its own change before the two talk again. Otherwise one side's
# change can simply arrive first and dominate, and nothing concurrent ever
# happens.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init delete-vs-modify

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"

log "Seeding two files both sides start from"
echo "original a" >"$ALICE_SHARE/a.txt"
echo "original b" >"$ALICE_SHARE/b.txt"
wait_for "$SYNC_TIMEOUT" "a.txt to reach bob" file_content_is "$BOB_SYNC/a.txt" "original a"
wait_for "$SYNC_TIMEOUT" "b.txt to reach bob" file_content_is "$BOB_SYNC/b.txt" "original b"

log "Offline: alice deletes a.txt while bob edits it, and bob deletes b.txt while alice edits it"
node_stop alice
node_stop bob
rm "$ALICE_SHARE/a.txt"
echo "bob's edit of a" >"$BOB_SYNC/a.txt"
echo "alice's edit of b" >"$ALICE_SHARE/b.txt"
rm "$BOB_SYNC/b.txt"
echo "tracer" >"$ALICE_SHARE/tracer.txt"

log "Bringing both back"
node_start alice
node_start bob
wait_for "$CONNECT_TIMEOUT" "alice and bob to find each other again" mutually_connected alice bob
wait_for "$SYNC_TIMEOUT" "alice's tracer to reach bob (so the index exchange demonstrably ran)" \
	file_content_is "$BOB_SYNC/tracer.txt" "tracer"

log "Each edit must win over the other side's delete"
wait_for "$SYNC_TIMEOUT" "a.txt to come back on alice with bob's edit" \
	file_content_is "$ALICE_SHARE/a.txt" "bob's edit of a"
wait_for "$SYNC_TIMEOUT" "b.txt to come back on bob with alice's edit" \
	file_content_is "$BOB_SYNC/b.txt" "alice's edit of b"
hold_for 10 "bob to keep his edit of a.txt" file_content_is "$BOB_SYNC/a.txt" "bob's edit of a"
hold_for 5 "alice to keep her edit of b.txt" file_content_is "$ALICE_SHARE/b.txt" "alice's edit of b"

log "A delete losing to an edit is not a conflict: no conflict copies"
! conflict_copy_present "$ALICE_SHARE" || fail "alice has a conflict copy; delete-vs-modify should resurrect, not keep both"
! conflict_copy_present "$BOB_SYNC" || fail "bob has a conflict copy; delete-vs-modify should resurrect, not keep both"
info "ok: no conflict copy on either side"

DIFF="$(diff <(cd "$ALICE_SHARE" && ls -1) <(cd "$BOB_SYNC" && ls -1) || true)"
[ -z "$DIFF" ] || fail "the two directories did not converge:
$DIFF"
info "ok: alice and bob hold the same set of files"

assert_mutually_clean alice bob

harness_pass "a delete concurrent with an edit resolved to the edit, in both directions, with nothing lost"
