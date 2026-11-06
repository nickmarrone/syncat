#!/usr/bin/env bash
# Three nodes on one share. Every other test in the suite is two nodes, and
# fan-out is where the topology actually shows.
#
# syncat is hub-and-spoke: a share offered to several peers propagates
# *through the offerer*, and peers of the same share do not talk to each
# other (SPEC.md §5). So a write on one spoke has to reach the other by way
# of the hub, and the two spokes must never become peers behind the hub's
# back. This also covers a node holding several peer connections and several
# sessions at once, which nothing else here does.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init three-node-fanout

node_add alice
node_add bob
node_add carol
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
CAROL_SYNC="$(node_dir carol synced)"
node_start alice
node_start bob
node_start carol

log "Peering both spokes to the hub — and only to the hub"
pair_nodes alice bob
pair_nodes alice carol

log "Alice offers one share; both spokes subscribe to it"
SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"
CAROL_PEER="$(peer_id_of carol alice)"
wait_for 60 "carol to see alice's share" remote_share_seen carol "$SHARE_ID"
nx carol subscription add "$CAROL_PEER" "$SHARE_ID" "$CAROL_SYNC" --mode mirror >/dev/null

log "A write on the hub must fan out to both spokes"
echo "from the hub" >"$ALICE_SHARE/hub.txt"
wait_for "$SYNC_TIMEOUT" "hub.txt to reach bob" file_content_is "$BOB_SYNC/hub.txt" "from the hub"
wait_for "$SYNC_TIMEOUT" "hub.txt to reach carol" file_content_is "$CAROL_SYNC/hub.txt" "from the hub"

log "A write on one spoke must reach the other, by way of the hub"
echo "from bob" >"$BOB_SYNC/bob.txt"
wait_for "$SYNC_TIMEOUT" "bob's write to reach alice" file_content_is "$ALICE_SHARE/bob.txt" "from bob"
wait_for "$SYNC_TIMEOUT" "bob's write to reach carol via alice" file_content_is "$CAROL_SYNC/bob.txt" "from bob"

echo "from carol" >"$CAROL_SYNC/carol.txt"
wait_for "$SYNC_TIMEOUT" "carol's write to reach alice" file_content_is "$ALICE_SHARE/carol.txt" "from carol"
wait_for "$SYNC_TIMEOUT" "carol's write to reach bob via alice" file_content_is "$BOB_SYNC/carol.txt" "from carol"

log "An edit on one spoke must replace the content on the other two"
echo "from bob, v2" >"$BOB_SYNC/bob.txt"
wait_for "$SYNC_TIMEOUT" "bob's edit to reach alice" file_content_is "$ALICE_SHARE/bob.txt" "from bob, v2"
wait_for "$SYNC_TIMEOUT" "bob's edit to reach carol via alice" file_content_is "$CAROL_SYNC/bob.txt" "from bob, v2"

log "A delete on one spoke must reach the far spoke, and land in its trash"
rm "$CAROL_SYNC/carol.txt"
wait_for "$SYNC_TIMEOUT" "carol's delete to reach alice" path_absent "$ALICE_SHARE/carol.txt"
wait_for "$SYNC_TIMEOUT" "carol's delete to reach bob via alice" path_absent "$BOB_SYNC/carol.txt"
wait_for "$SYNC_TIMEOUT" "carol.txt in bob's trash (the far spoke)" trash_kind_is bob "$SHARE_ID" carol.txt deleted
trash_has carol "$SHARE_ID" carol.txt && fail "carol's own delete went into her trash"

log "A two-sided conflict between the spokes must resolve through the hub"
echo "contested" >"$ALICE_SHARE/contested.txt"
wait_for "$SYNC_TIMEOUT" "contested.txt to reach bob" file_content_is "$BOB_SYNC/contested.txt" "contested"
wait_for "$SYNC_TIMEOUT" "contested.txt to reach carol" file_content_is "$CAROL_SYNC/contested.txt" "contested"
# All three down, as in 05-offline-divergence.sh: each side must have indexed
# its own edit (core.Open's synchronous scan) before any of them can hear of
# the other's, or the later edit simply wins and no conflict ever exists.
node_stop alice
node_stop bob
node_stop carol
echo "bob's edit" >"$BOB_SYNC/contested.txt"
echo "carol's edit" >"$CAROL_SYNC/contested.txt"
echo "conflict tracer" >"$ALICE_SHARE/conflict-tracer.txt"
node_start alice
node_start bob
node_start carol
wait_for "$CONNECT_TIMEOUT" "alice and bob to reconnect" mutually_connected alice bob
wait_for "$CONNECT_TIMEOUT" "alice and carol to reconnect" mutually_connected alice carol
wait_for "$SYNC_TIMEOUT" "the tracer to reach bob" file_content_is "$BOB_SYNC/conflict-tracer.txt" "conflict tracer"
wait_for "$SYNC_TIMEOUT" "the tracer to reach carol" file_content_is "$CAROL_SYNC/conflict-tracer.txt" "conflict tracer"
for d in "$ALICE_SHARE" "$BOB_SYNC" "$CAROL_SYNC"; do
	wait_for 240 "a conflict copy in $(basename "$d")" conflict_copy_present "$d"
done
wait_for 90 "contested.txt to converge on alice and bob" files_identical "$ALICE_SHARE/contested.txt" "$BOB_SYNC/contested.txt"
wait_for 90 "contested.txt to converge on alice and carol" files_identical "$ALICE_SHARE/contested.txt" "$CAROL_SYNC/contested.txt"
wait_for 90 "the conflict copy's name to match on alice and bob" conflict_copy_names_match "$ALICE_SHARE" "$BOB_SYNC"
wait_for 90 "the conflict copy's name to match on alice and carol" conflict_copy_names_match "$ALICE_SHARE" "$CAROL_SYNC"
# Deliberately not asserting exactly ONE conflict copy: with two spokes
# conflicting through the hub, more than one node resolves the conflict and
# each names its copy after its own node id, so several same-content copies
# exist (the file sets still converge, checked below). See the linked issue.
WINNER="$(cat "$ALICE_SHARE/contested.txt")"
LOSER="$(cat "$(ls "$ALICE_SHARE"/contested.sync-conflict-*.txt | head -n1)")"
case "$WINNER" in "bob's edit" | "carol's edit") ;; *) fail "unexpected winner: '$WINNER'" ;; esac
case "$LOSER" in "bob's edit" | "carol's edit") ;; *) fail "unexpected conflict copy content: '$LOSER'" ;; esac
[ "$WINNER" != "$LOSER" ] || fail "the conflict copy duplicates the winner; one edit was lost"
info "winner '$WINNER', conflict copy '$LOSER'"

log "The spokes must not have become peers of each other"
[ "$(nx bob status --json | jq -r '.peers | length')" = "1" ] ||
	fail "bob has more than one peer — the spokes are talking to each other, not through the hub"
[ "$(nx carol status --json | jq -r '.peers | length')" = "1" ] ||
	fail "carol has more than one peer — the spokes are talking to each other, not through the hub"
[ "$(nx alice status --json | jq -r '.peers | length')" = "2" ] ||
	fail "alice is not holding both spoke connections"
info "ok: bob and carol each hold exactly one peer, and it is alice"

log "The hub must be holding both connections cleanly, at once"
assert_mutually_clean alice bob
assert_mutually_clean alice carol

log "All three directories must agree"
for pair in "$ALICE_SHARE:$BOB_SYNC" "$ALICE_SHARE:$CAROL_SYNC"; do
	a="${pair%%:*}"
	b="${pair##*:}"
	DIFF="$(diff <(cd "$a" && find . -path ./.syncat -prune -o -type f -print | sort) <(cd "$b" && find . -path ./.syncat -prune -o -type f -print | sort) || true)"
	[ -z "$DIFF" ] || fail "$a and $b did not converge:
$DIFF"
done
info "ok: all three copies hold the same set of files"

harness_pass "creates, edits, deletes (trashed on the far spoke) and a spoke-vs-spoke conflict all converged through the hub, with the spokes never peering directly"
