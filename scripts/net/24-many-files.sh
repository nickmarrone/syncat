#!/usr/bin/env bash
# SOAK. Thousands of small files over the real relay.
#
# MANUAL-TESTS.md section 13 asked for "a directory with thousands of small
# files; initial scan and sync complete; note how long". Every other scenario
# moves a handful of files, so nothing exercises the index exchange, the
# per-file request/response path or the SQLite writes at any volume.
#
# Kept light on the shared public DERP relays: SOAK_FILES (default 3000)
# files of ~1 KiB is a few MB in total. Convergence is checked by comparing
# sorted sha256 manifests (digest + path) on both sides, so a missing file, a
# truncated file and a swapped file all show up. Then a modest subset is
# deleted, kept below the hold thresholds (SPEC.md section 7a: 20 files AND
# 50%) so the deletes are meant to propagate rather than trip a hold.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init many-files

SOAK_FILES="${SOAK_FILES:-3000}"
SOAK_DELETE="${SOAK_DELETE:-300}" # 10% of the default: well under the 50% hold threshold
# Generous for relayed throughput, but a hang should still fail in bounded time.
SCALE_TIMEOUT=$((SOAK_FILES / 2 + 300))

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"

# manifest DIR echoes "sha256  relpath" for every regular file, sorted by
# path, ignoring syncat's own reserved .syncat directory.
manifest() {
	(cd "$1" && find . -path ./.syncat -prune -o -type f -print0 | sort -z | xargs -0 -r sha256sum)
}
file_total() { find "$1" -path "$1/.syncat" -prune -o -type f -print | wc -l | tr -d ' '; }
bob_has_total() { [ "$(file_total "$BOB_SYNC")" = "$1" ]; }
manifests_match() { [ "$(manifest "$ALICE_SHARE")" = "$(manifest "$BOB_SYNC")" ]; }

log "Generating $SOAK_FILES files of ~1 KiB across nested directories"
GEN_START=$(date +%s)
for i in $(seq 1 "$SOAK_FILES"); do
	d="$ALICE_SHARE/d$((i % 10))/s$((i % 7))/t$((i % 3))"
	mkdir -p "$d"
	# ~1 KiB of content unique to the file, so a swapped file is detectable.
	head -c 768 /dev/urandom | base64 -w0 >"$d/f$i.dat"
done
[ "$(file_total "$ALICE_SHARE")" = "$SOAK_FILES" ] || fail "generator wrote the wrong number of files"
info "generated in $(($(date +%s) - GEN_START))s, $(du -sk "$ALICE_SHARE" | cut -f1) KiB on disk"

log "Waiting for all $SOAK_FILES files to reach bob"
SYNC_START=$(date +%s)
wait_for "$SCALE_TIMEOUT" "bob to hold $SOAK_FILES files" bob_has_total "$SOAK_FILES"
wait_for "$SCALE_TIMEOUT" "bob's sha256 manifest to match alice's" manifests_match
TIMING_SYNC=$(($(date +%s) - SYNC_START))
info "TIMING: $SOAK_FILES files synced and verified in ${TIMING_SYNC}s"
no_hold bob "$SHARE_ID" || fail "bob reports a hold after a plain bulk create"
no_hold alice "$SHARE_ID" || fail "alice reports a hold after a plain bulk create"

log "Deleting $SOAK_DELETE files (below the hold thresholds)"
DOOMED="$(cd "$ALICE_SHARE" && find . -path ./.syncat -prune -o -type f -print | sort | head -n "$SOAK_DELETE")"
[ "$(echo "$DOOMED" | wc -l | tr -d ' ')" = "$SOAK_DELETE" ] || fail "could not pick $SOAK_DELETE files to delete"
(cd "$ALICE_SHARE" && echo "$DOOMED" | xargs rm -f)
WANT=$((SOAK_FILES - SOAK_DELETE))
DEL_START=$(date +%s)
wait_for "$SCALE_TIMEOUT" "bob to drop to $WANT files" bob_has_total "$WANT"
wait_for "$SCALE_TIMEOUT" "the manifests to match after the deletes" manifests_match
TIMING_DEL=$(($(date +%s) - DEL_START))
info "TIMING: $SOAK_DELETE deletes converged in ${TIMING_DEL}s"
no_hold alice "$SHARE_ID" || fail "alice put the share on hold for a delete below the thresholds"
no_hold bob "$SHARE_ID" || fail "bob put the share on hold for a delete below the thresholds"

log "The connection must have stayed healthy under the load"
for n in alice bob; do
	assert_log_lacks "$n" "no traffic for the keepalive dead interval" "the dead-peer rule firing under load"
done
assert_mutually_clean alice bob

harness_pass "$SOAK_FILES small files synced with matching sha256 manifests (${TIMING_SYNC}s), then $SOAK_DELETE deletes converged (${TIMING_DEL}s)"
