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
	DIFF="$(diff <(cd "$a" && ls -1) <(cd "$b" && ls -1) || true)"
	[ -z "$DIFF" ] || fail "$a and $b did not converge:
$DIFF"
done
info "ok: all three copies hold the same set of files"

harness_pass "a share fanned out to two spokes converged through the hub, with the spokes never peering directly"
