#!/usr/bin/env bash
# SOAK. A connection that is up now and one that survives a quarter of an
# hour are different claims, and only the second is worth anything.
#
# This is the window MANUAL-TESTS.md §3 was written for: "A handshake bug used
# to kill every connection at exactly 90-120s, and no automated test could see
# it, because the whole suite finishes in under 30 seconds of wall-clock
# time." The default here is six minutes, comfortably past that window and
# past several ping/dead cycles (30s ping interval, ${DEAD_PEER_SECONDS}s dead
# rule); SOAK_MINUTES lengthens it for an overnight run.
#
# The assertions are all about *absence*, which is why they lean on the
# daemon log. A status snapshot taken at the end cannot tell a connection
# that never dropped from one that dropped and reconnected forty times;
# connected_since and the log can.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init idle-stability

SOAK_MINUTES="${SOAK_MINUTES:-6}"
SOAK_SECONDS=$((SOAK_MINUTES * 60))

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"
echo "seed" >"$ALICE_SHARE/seed.txt"
wait_for "$SYNC_TIMEOUT" "seed.txt to reach bob" file_content_is "$BOB_SYNC/seed.txt" "seed"

SINCE_A="$(peer_connected_since alice bob)"
SINCE_B="$(peer_connected_since bob alice)"
CONNECTS_A="$(log_count alice ': connected (')"
CONNECTS_B="$(log_count bob ': connected (')"
info "baseline: alice connected_since=$SINCE_A, bob connected_since=$SINCE_B"

# connected_since is checked throughout the hold, not just at the end: a drop
# and an immediate reconnect would move it, and by the end everything would
# look healthy again.
still_same_connection() {
	mutually_connected alice bob &&
		[ "$(peer_connected_since alice bob)" = "$SINCE_A" ] &&
		[ "$(peer_connected_since bob alice)" = "$SINCE_B" ]
}

log "Holding the connection idle for ${SOAK_MINUTES} minute(s) (SOAK_MINUTES to change)"
hold_for "$SOAK_SECONDS" "the connection to stay up, unbroken, for ${SOAK_MINUTES}m" still_same_connection

log "Checking the logs for churn neither status snapshot would show"
for n in alice bob; do
	assert_log_lacks "$n" "i/o timeout" "a ping or write that timed out"
	assert_log_lacks "$n" "no traffic for the keepalive dead interval" \
		"the dead-peer rule firing on an idle but healthy connection"
	assert_log_lacks "$n" "disconnected after" "a disconnect"
done
[ "$(log_count alice ': connected (')" = "$CONNECTS_A" ] ||
	fail "alice reconnected during the idle window"
[ "$(log_count bob ': connected (')" = "$CONNECTS_B" ] ||
	fail "bob reconnected during the idle window"

log "The connection must still be usable, not merely alive"
echo "after idling" >"$ALICE_SHARE/late.txt"
wait_for "$SYNC_TIMEOUT" "a write after ${SOAK_MINUTES}m idle to reach bob" \
	file_content_is "$BOB_SYNC/late.txt" "after idling"
echo "and back" >"$BOB_SYNC/late-bob.txt"
wait_for "$SYNC_TIMEOUT" "a write after ${SOAK_MINUTES}m idle to reach alice" \
	file_content_is "$ALICE_SHARE/late-bob.txt" "and back"

assert_mutually_clean alice bob

harness_pass "a connection idled for ${SOAK_MINUTES}m without a single reconnect, ping timeout, or spurious dead-peer call, and still synced afterwards"
