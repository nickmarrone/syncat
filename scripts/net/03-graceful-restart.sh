#!/usr/bin/env bash
# A daemon restarted the way a package upgrade or `systemctl restart` does
# it: SIGTERM, a clean protocol shutdown, then start again.
#
# Two things are being pinned. First, that the surviving node notices
# *promptly* — a clean shutdown ends the peer's read loop, so detection must
# not wait out SPEC.md §4's 90s dead timer, which is the backstop for silence
# and not the mechanism for a hangup. Second, that recovery is automatic:
# the restarted node reconnects and sync resumes in both directions with
# nothing touched on the other side.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init graceful-restart

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"
echo "before restart" >"$ALICE_SHARE/before.txt"
wait_for "$SYNC_TIMEOUT" "before.txt to reach bob" file_content_is "$BOB_SYNC/before.txt" "before restart"

SINCE_BEFORE="$(peer_connected_since alice bob)"
info "alice's connected_since before the restart: $SINCE_BEFORE"

log "Stopping bob with SIGTERM (the graceful path)"
node_stop bob

# A clean shutdown closes the session, which ends alice's read loop straight
# away. Allowing anything close to DEAD_PEER_SECONDS here would let a
# regression that silently downgraded this to the dead-timer backstop pass.
wait_while 30 "alice to notice bob is gone (well inside the ${DEAD_PEER_SECONDS}s dead timer)" \
	peer_connected alice bob
assert_connected_since_cleared alice bob

log "Editing on alice while bob is down"
echo "written while bob was down" >"$ALICE_SHARE/during.txt"

log "Restarting bob"
node_start bob
wait_for "$CONNECT_TIMEOUT" "bob to reconnect to alice" peer_connected bob alice
wait_for "$CONNECT_TIMEOUT" "alice to see bob connected again" peer_connected alice bob

log "Sync must resume in both directions, with nothing touched on alice's side"
wait_for "$SYNC_TIMEOUT" "the edit made during the outage to reach bob" \
	file_content_is "$BOB_SYNC/during.txt" "written while bob was down"
echo "after restart" >"$BOB_SYNC/after.txt"
wait_for "$SYNC_TIMEOUT" "a post-restart write on bob to reach alice" \
	file_content_is "$ALICE_SHARE/after.txt" "after restart"

SINCE_AFTER="$(peer_connected_since alice bob)"
[ "$SINCE_AFTER" != "$SINCE_BEFORE" ] ||
	fail "alice's connected_since did not move across the restart ($SINCE_AFTER) — it is reporting the dead connection's start time"
info "alice's connected_since after the restart: $SINCE_AFTER"

# The subscription has to come back as a working one, not merely a listed one.
[ "$(nx bob subscription ls --json | jq -r '.[0].connected')" = "true" ] ||
	fail "bob's subscription is not reporting itself connected after the restart"

assert_mutually_clean alice bob

harness_pass "a SIGTERM restart is noticed immediately, recovers on its own, and resumes sync both ways"
