#!/usr/bin/env bash
# SOAK. Kill the receiver in the middle of a large transfer.
#
# README is explicit that this is safe but not free: "Transfers interrupted by
# the restart are *not* resumed ... a file that was half-transferred starts
# over from byte 0 on reconnect." Resume is specified (SPEC.md §4's
# FileRequest carries an offset) but not implemented, so what this pins down
# is that the *recovery* works — the interrupted transfer is retried and
# completes, and the half-written file never appears as if it were whole.
#
# That last part is the one worth guarding. Apply is atomic: content lands in
# a .syncat.tmp file, is verified against its sha256, and only then renamed
# into place. A restart mid-transfer must therefore leave no truncated
# big.bin behind, only a temp file that is cleaned up or superseded.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init restart-mid-transfer

SOAK_FILE_MB="${SOAK_FILE_MB:-256}"

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"

log "Generating a ${SOAK_FILE_MB} MiB file on alice"
dd if=/dev/urandom of="$ALICE_SHARE/big.bin" bs=1M count="$SOAK_FILE_MB" status=none ||
	fail "generating the test file"
WANT="$(sha_of "$ALICE_SHARE/big.bin")"

log "Waiting for the transfer to be genuinely under way"
# A temp file with something in it is the proof that bytes are actually
# moving; killing before that would test a reconnect, not an interruption.
transfer_started() {
	local f
	for f in "$BOB_SYNC"/.syncat.tmp.*; do
		[ -f "$f" ] && [ "$(stat -c %s "$f" 2>/dev/null || echo 0)" -gt 1000000 ] && return 0
	done
	return 1
}
wait_for 300 "bob to have pulled at least a megabyte into a temp file" transfer_started

log "SIGKILLing bob mid-transfer"
node_kill bob

# The half-written content must not be masquerading as the real file.
path_absent "$BOB_SYNC/big.bin" ||
	fail "a partial big.bin was left in place by the interrupted transfer — apply is supposed to be atomic"
info "ok: no partial big.bin in place after the kill"

log "Restarting bob; the transfer must be retried and complete"
node_start bob
wait_for "$CONNECT_TIMEOUT" "bob to reconnect" peer_connected bob alice

transfer_complete() {
	[ -f "$BOB_SYNC/big.bin" ] && [ "$(sha_of "$BOB_SYNC/big.bin")" = "$WANT" ]
}
# No resume, so budget for the whole file again from byte 0.
wait_for $((SOAK_FILE_MB * 8 + 300)) "the interrupted transfer to complete with a matching sha256" transfer_complete

log "No temp files may be left behind"
LEFTOVER="$(find "$BOB_SYNC" -name '.syncat.tmp.*' 2>/dev/null | head -n 3)"
[ -z "$LEFTOVER" ] || fail "temp files survived the completed transfer:
$LEFTOVER"
info "ok: no .syncat.tmp.* left in bob's directory"

assert_mutually_clean alice bob

harness_pass "a transfer interrupted by a hard kill left no partial file, was retried on reconnect, and completed intact"
