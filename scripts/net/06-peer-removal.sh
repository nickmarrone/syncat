#!/usr/bin/env bash
# Removing a peer, over a live connection.
#
# A subscription names the peer that offers its share, so one left behind
# after that peer is removed could never sync again — it would just sit in
# `syncat status` forever with a watcher still running. The removal must take
# the subscriptions with it, tear the session down on *both* sides, and leave
# every file it had already synced exactly where it is: removal unsubscribes,
# it does not delete the user's data.
#
# The re-add is the other half. The same key coming back must reconnect
# normally, and must not silently inherit anything from before.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init peer-removal

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"
echo "synced before removal" >"$ALICE_SHARE/keep.txt"
wait_for "$SYNC_TIMEOUT" "keep.txt to reach bob" file_content_is "$BOB_SYNC/keep.txt" "synced before removal"

ALICE_KEY="$(peer_key_of alice)"
ALICE_TOKEN="$(nx alice token)"
[ "$(subscription_count bob)" = "1" ] || fail "bob should have exactly one subscription before the removal"

log "Bob removes alice while the connection is live"
nx bob peer rm "$ALICE_KEY" >/dev/null

wait_for 60 "bob to drop the subscription along with the peer" \
	test "0" = "$(subscription_count bob)"
[ "$(nx bob status --json | jq -r '.peers | length')" = "0" ] ||
	fail "bob still lists alice after removing her"

log "The files bob already synced must still be on disk"
file_content_is "$BOB_SYNC/keep.txt" "synced before removal" ||
	fail "removing the peer deleted the user's synced data"
info "ok: keep.txt survived the removal"

log "Alice must lose the session too, and must not get it back"
# Alice still has bob configured, so she keeps dialing — but bob no longer
# knows her, so every attempt is rejected at the handshake.
wait_while 60 "alice's session with bob to end" peer_connected alice bob
wait_for "$CONNECT_TIMEOUT" "bob to start recording alice's dials as rejected" rejected_has bob "$ALICE_KEY"

log "Nothing may cross the removed link"
echo "must not arrive" >"$ALICE_SHARE/after-removal.txt"
# Give alice's watcher and a few dial attempts room to act, then assert the
# absence. A bare check would pass before the write had any chance to travel.
sleep 20
path_absent "$BOB_SYNC/after-removal.txt" ||
	fail "a write on alice reached bob after bob removed her as a peer"
info "ok: alice's post-removal write did not reach bob"

log "Re-adding the same key must reconnect normally"
nx bob peer add "$ALICE_TOKEN" --name alice >/dev/null
wait_for "$CONNECT_TIMEOUT" "bob to reconnect to alice" peer_connected bob alice
wait_for "$CONNECT_TIMEOUT" "alice to see bob connected again" peer_connected alice bob

# The subscription was removed with the peer and must stay removed: re-adding
# a peer restores the connection, not the user's subscriptions.
[ "$(subscription_count bob)" = "0" ] ||
	fail "re-adding the peer silently resurrected a subscription that was removed"
info "ok: the subscription stayed removed across the re-add"

assert_mutually_clean alice bob

harness_pass "peer removal drops subscriptions and the session, keeps the files, blocks further sync, and re-adds cleanly"
