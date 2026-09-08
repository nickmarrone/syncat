#!/usr/bin/env bash
# The ugly restart: SIGKILL. No graceful shutdown, no protocol close, no
# TCP drain — the peer is left holding a connection to a process that no
# longer exists.
#
# The interesting half is what happens on the *survivor*. If it is still
# holding that corpse when the restarted node dials back in, the inbound
# connection is authenticated but rejected by the dedup rule, and it is
# notePeerRedialed that has to read that for what it is — a peer only dials
# when it holds no session, so if it has none while we think we have one,
# ours is dead — and tear the corpse down. Without that inference the
# survivor waits out the full 90s dead timer instead, and which node pays
# that wait depends on nothing but how the two keys happened to compare.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init hard-kill-restart

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

log "SIGKILLing bob — no clean shutdown, no protocol close"
node_kill bob

log "Restarting bob immediately, while alice may still be holding the dead connection"
node_start bob

# The recovery must not need the dead timer. staleInboundGrace gives a freshly
# adopted connection 15s of immunity, and a redial has to get through DERP
# first, so allow for that — but stay well under DEAD_PEER_SECONDS, because
# waiting that out is precisely the failure this covers.
wait_for "$CONNECT_TIMEOUT" "bob to reconnect after the hard kill" peer_connected bob alice
wait_for "$DEAD_PEER_SECONDS" "alice to converge on the new connection without waiting out the dead timer" \
	peer_connected alice bob

log "Sync must resume over the replacement connection"
echo "after the kill" >"$ALICE_SHARE/after-kill.txt"
wait_for "$SYNC_TIMEOUT" "a post-kill write on alice to reach bob" \
	file_content_is "$BOB_SYNC/after-kill.txt" "after the kill"
echo "from bob" >"$BOB_SYNC/from-bob.txt"
wait_for "$SYNC_TIMEOUT" "a post-kill write on bob to reach alice" \
	file_content_is "$ALICE_SHARE/from-bob.txt" "from bob"

# Files that existed before the kill must survive it: a reconnect is not a
# resync from nothing.
path_present "$BOB_SYNC/seed.txt" || fail "seed.txt vanished from bob across the hard kill"

assert_mutually_clean alice bob

harness_pass "a SIGKILLed peer restarts, replaces the survivor's dead connection without waiting out the dead timer, and resumes sync"
