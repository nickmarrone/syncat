#!/usr/bin/env bash
# A bulk delete is held, survives a restart, and only spreads once confirmed.
#
# SPEC.md §7a: a single change that would delete at least hold_delete_percent
# of a share's live files, and at least hold_delete_min of them, puts the
# share on hold. Nothing is applied or sent while it is held, a restart does
# not forget it, and `syncat share confirm-deletes` lets it through. This is
# the guard against an emptied or unmounted folder wiping every peer, which
# makes it the feature most worth proving on the real binary.
#
# The negative half (bob keeps his files while alice is held) has no tracer
# to lean on: a hold on alice's own copy stops every change in both
# directions, by design. Instead the hold itself is the proof that alice
# scanned the deletes, and the files are then watched for a fixed window.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init bulk-delete-hold

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"

log "Seeding 25 files, above the default hold_delete_min of 20"
for i in $(seq -w 1 25); do echo "file $i" >"$ALICE_SHARE/f$i.txt"; done
wait_for "$SYNC_TIMEOUT" "all 25 files to reach bob" file_count_is "$BOB_SYNC" 25

log "Alice deletes 22 of them (88%, 22 files) in one go"
for i in $(seq -w 1 22); do rm "$ALICE_SHARE/f$i.txt"; done
wait_for "$SYNC_TIMEOUT" "alice to put her own copy of the share on hold" has_hold alice "$SHARE_ID" ""
HOLD="$(nx alice status --json | jq -c --arg s "$SHARE_ID" '[.holds[] | select(.share_id==$s)][0]')"
assert_eq "$(echo "$HOLD" | jq -r '.peer_key // ""')" "" "the hold's peer_key (empty means alice's own copy)"
assert_eq "$(echo "$HOLD" | jq -r .confirmable)" "true" "the hold's confirmable flag"
assert_eq "$(echo "$HOLD" | jq -r .deletes)" "22" "the number of deletes held"

hold_for 15 "bob to keep all 25 files while alice is held" file_count_is "$BOB_SYNC" 25

log "A restart must not forget the hold"
node_restart alice
wait_for "$CONNECT_TIMEOUT" "alice and bob to reconnect" mutually_connected alice bob
has_hold alice "$SHARE_ID" "" || fail "alice's hold did not survive a restart"
info "ok: alice is still on hold after a restart"
hold_for 15 "bob to keep all 25 files after alice restarts" file_count_is "$BOB_SYNC" 25

log "Alice confirms: the deletes leave alice, and bob holds them on receipt"
# Holds are checked on both sides (SPEC.md §7a): the same bulk delete
# arriving from a peer puts bob's link with alice on hold, so one node
# confirming never empties another node's folder on its own say-so.
nx alice share confirm-deletes docs >/dev/null || fail "alice: share confirm-deletes failed"
wait_for "$SYNC_TIMEOUT" "alice's hold to clear" no_hold alice "$SHARE_ID"
ALICE_KEY="$(peer_key_of alice)"
wait_for "$SYNC_TIMEOUT" "bob to hold the bulk delete arriving from alice" has_hold bob "$SHARE_ID" "$ALICE_KEY"
hold_for 10 "bob to keep all 25 files while his link with alice is held" file_count_is "$BOB_SYNC" 25

log "Bob confirms too, and the deletes apply"
nx bob share confirm-deletes "$SHARE_ID" --peer alice >/dev/null || fail "bob: share confirm-deletes --peer alice failed"
wait_for "$SYNC_TIMEOUT" "the 22 deletes to reach bob" file_count_is "$BOB_SYNC" 3
wait_for "$SYNC_TIMEOUT" "bob's hold to clear" no_hold bob "$SHARE_ID"
wait_for "$SYNC_TIMEOUT" "a deleted file to land in bob's trash" trash_has bob "$SHARE_ID" f01.txt
for i in 23 24 25; do
	file_content_is "$BOB_SYNC/f$i.txt" "file $i" || fail "bob lost f$i.txt, which alice never deleted"
done
info "ok: the three files alice kept are still on bob"

assert_mutually_clean alice bob

harness_pass "a bulk delete was held on the sender across a restart and again on the receiver, and applied only once each side confirmed"
