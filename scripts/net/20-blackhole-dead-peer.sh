#!/usr/bin/env bash
# SOAK. The scenario MANUAL-TESTS.md §3 calls the highest-value one, and the
# only one in this suite that cannot be approximated by a Go test at all.
#
# A peer that stops answering *without hanging up* is a different failure from
# a peer that closes: there is no EOF, no reset, nothing for the read loop to
# notice. It is what a pulled cable, a suspended laptop, and a black-holing
# relay all look like from the other end, and the only thing that recovers
# from it is SPEC.md §4's rule — 90s without received traffic means the
# connection is dead. That rule is the reason the keepalive exists, and until
# now nothing exercised it end to end: internal/protocol tests the timer on a
# fake clock, and no test had ever let 90 real seconds pass.
#
# SIGSTOP produces exactly that condition with no root, no firewall and no
# network namespace. The frozen daemon's sockets stay open and the kernel
# keeps acknowledging at the TCP level, but the process answers nothing, so
# the peer's Pings go unanswered while the connection stays established.
#
# MANUAL-TESTS.md §3 also records the regression this guards: "A handshake bug
# used to kill every connection at exactly 90-120s, and no automated test
# could see it, because the whole suite finishes in under 30 seconds of
# wall-clock time."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init blackhole-dead-peer

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"
echo "before the freeze" >"$ALICE_SHARE/before.txt"
wait_for "$SYNC_TIMEOUT" "before.txt to reach bob" file_content_is "$BOB_SYNC/before.txt" "before the freeze"

log "Freezing bob with SIGSTOP — its socket stays open, but it answers nothing"
node_freeze bob

# Nothing may happen for a good while: this is the half of the rule that
# matters as much as the timeout itself. A connection that drops the moment a
# peer goes quiet would break every slow link and every busy machine.
log "Alice must NOT declare the peer dead early"
hold_for 60 "alice to keep the connection while bob has been silent for under a minute" \
	peer_connected alice bob

log "Alice must declare it dead once the ${DEAD_PEER_SECONDS}s rule is up"
# The keepalive polls on a 5s tick, so detection lands in
# [DEAD_PEER_SECONDS, DEAD_PEER_SECONDS+5]; allow generous slack above that
# without allowing so much that a regression to "notices eventually" passes.
wait_while $((DEAD_PEER_SECONDS + 60)) "alice to notice bob has gone silent" \
	peer_connected alice bob

assert_log_has alice "no traffic for the keepalive dead interval" \
	"the dead-peer rule firing (as distinct from a clean hangup)"
assert_connected_since_cleared alice bob

log "Thawing bob with SIGCONT — both sides must recover with no restart"
node_thaw bob

wait_for "$CONNECT_TIMEOUT" "alice to reconnect to bob after the blackout" peer_connected alice bob
wait_for "$CONNECT_TIMEOUT" "bob to see alice connected again" peer_connected bob alice

log "Sync must resume in both directions"
echo "after the thaw" >"$ALICE_SHARE/after.txt"
wait_for "$SYNC_TIMEOUT" "a post-thaw write on alice to reach bob" \
	file_content_is "$BOB_SYNC/after.txt" "after the thaw"
echo "bob is back" >"$BOB_SYNC/from-bob.txt"
wait_for "$SYNC_TIMEOUT" "a post-thaw write on bob to reach alice" \
	file_content_is "$ALICE_SHARE/from-bob.txt" "bob is back"

path_present "$BOB_SYNC/before.txt" || fail "before.txt vanished from bob across the blackout"

assert_mutually_clean alice bob

harness_pass "a silent peer was declared dead by SPEC.md §4's ${DEAD_PEER_SECONDS}s rule, not before it, and both sides recovered on their own"
