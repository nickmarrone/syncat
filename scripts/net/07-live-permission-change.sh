#!/usr/bin/env bash
# Changing a share's permission on a live connection.
#
# SPEC.md §6 says the decision is pushed to the peer via AccessUpdate, so
# demoting a read-write share to read-only has to take effect on a connected
# subscriber without either side restarting. The subscriber keeps everything
# it has already synced — revocation stops sync, it does not claw data back —
# and the offerer's own changes keep flowing outward.
#
# The negative half of that needs care. "Bob's write did not arrive" is only
# meaningful once something else *has* arrived over the same connection since,
# which is what the tracer below is for: without it the assertion would pass
# just as readily against a connection that had died.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init live-permission-change

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"

log "While read-write, bob's writes must reach alice"
echo "from alice" >"$ALICE_SHARE/a.txt"
wait_for "$SYNC_TIMEOUT" "alice's file to reach bob" file_content_is "$BOB_SYNC/a.txt" "from alice"
echo "from bob while rw" >"$BOB_SYNC/b.txt"
wait_for "$SYNC_TIMEOUT" "bob's file to reach alice while read-write" \
	file_content_is "$ALICE_SHARE/b.txt" "from bob while rw"

log "Demoting the share to read-only on the live connection"
nx alice share set "$SHARE_ID" --perm ro >/dev/null

wait_for 60 "bob to see the share as read-only, with no restart on either side" \
	test "read-only" = "$(remote_share_perm bob "$SHARE_ID")"

log "Alice's changes must keep flowing outward"
echo "after demotion" >"$ALICE_SHARE/a.txt"
wait_for "$SYNC_TIMEOUT" "alice's post-demotion edit to reach bob" \
	file_content_is "$BOB_SYNC/a.txt" "after demotion"

log "Bob's changes must not flow back"
echo "must not arrive" >"$BOB_SYNC/blocked.txt"
# The tracer makes the absence below mean something: it proves a full sync
# round trip completed *after* bob's write, so "it hasn't arrived yet" is not
# an available explanation.
echo "tracer" >"$ALICE_SHARE/tracer.txt"
wait_for "$SYNC_TIMEOUT" "a later change from alice to reach bob" \
	file_content_is "$BOB_SYNC/tracer.txt" "tracer"
path_absent "$ALICE_SHARE/blocked.txt" ||
	fail "bob's write reached alice after the share was demoted to read-only"
info "ok: bob's write did not reach alice, on a connection that is demonstrably still syncing"

log "Everything bob had already synced must still be there"
file_content_is "$BOB_SYNC/b.txt" "from bob while rw" ||
	fail "revoking write access removed data bob had already synced"
info "ok: bob kept the file he wrote while the share was read-write"

assert_mutually_clean alice bob

harness_pass "a live rw->ro demotion propagates without a restart, stops the subscriber's writes, and keeps its data"
