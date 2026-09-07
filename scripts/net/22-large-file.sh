#!/usr/bin/env bash
# SOAK. A large file over the real relay.
#
# Everything else in this suite moves a few bytes, which never leaves the
# first frame. This one runs the transfer path for long enough to matter:
# many 1 MiB FileChunks (protocol.MaxFileChunkData) through the writer's bulk
# lane, sustained for minutes.
#
# The interesting assertion is not that the bytes arrive — it is that the
# connection survives them. The writer has two lanes precisely so control
# frames can overtake a queue of chunks; if a Ping ever queued behind the
# bulk lane, a long transfer would starve the keepalive and the peer would
# declare a busy, perfectly healthy connection dead at ${DEAD_PEER_SECONDS}s.
# README records that as fixed ("keepalives no longer wedge behind blocked
# writes"), and nothing has been checking it since.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init large-file

SOAK_FILE_MB="${SOAK_FILE_MB:-256}"

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"

log "Generating a ${SOAK_FILE_MB} MiB file (urandom, so nothing downstream can compress it away)"
dd if=/dev/urandom of="$ALICE_SHARE/big.bin" bs=1M count="$SOAK_FILE_MB" status=none ||
	fail "generating the test file"
WANT="$(sha_of "$ALICE_SHARE/big.bin")"
info "sha256: $WANT"

SINCE_A="$(peer_connected_since alice bob)"
SINCE_B="$(peer_connected_since bob alice)"

log "Waiting for it to cross the relay"
# Generous: this is a real DERP relay, and relayed throughput is not
# guaranteed to be anything in particular.
transfer_complete() {
	[ -f "$BOB_SYNC/big.bin" ] && [ "$(sha_of "$BOB_SYNC/big.bin")" = "$WANT" ]
}
wait_for $((SOAK_FILE_MB * 8 + 300)) "the ${SOAK_FILE_MB} MiB file to arrive with a matching sha256" transfer_complete

log "The connection must have survived the transfer, unbroken"
# Same connection, start to finish: a transfer that dropped the link and
# resumed on a new one would still end with matching bytes, and would still
# be a bug.
[ "$(peer_connected_since alice bob)" = "$SINCE_A" ] ||
	fail "alice's connection was replaced during the transfer — it did not survive its own payload"
[ "$(peer_connected_since bob alice)" = "$SINCE_B" ] ||
	fail "bob's connection was replaced during the transfer — it did not survive its own payload"
for n in alice bob; do
	assert_log_lacks "$n" "no traffic for the keepalive dead interval" \
		"the dead-peer rule firing during a large transfer (a keepalive stuck behind the bulk lane)"
	assert_log_lacks "$n" "i/o timeout" "a write that timed out under load"
done

log "The connection must still be usable for small changes afterwards"
echo "after the big one" >"$ALICE_SHARE/small.txt"
wait_for "$SYNC_TIMEOUT" "a small write after the transfer to reach bob" \
	file_content_is "$BOB_SYNC/small.txt" "after the big one"

assert_mutually_clean alice bob

harness_pass "a ${SOAK_FILE_MB} MiB file crossed the real relay intact without starving the keepalive or dropping the connection"
