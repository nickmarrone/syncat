#!/usr/bin/env bash
# A receive-only subscription flags a local edit when it is made, and only
# reverts it later (SPEC.md §5 "Direction rules"; issue #43).
#
#   rw share, subscribed with --mode receive. Bob edits a synced file:
#     - the edit is warned about right away, before alice changes anything,
#       and is not marked reverted;
#     - bob's file still holds his edit, and nothing he wrote reaches alice;
#     - when alice next changes the file, bob's edit goes to his trash as
#       'overwritten', bob has alice's version, and the warning reads reverted.
#
# "Did not arrive" is checked only after a later change from alice has
# arrived over the same connection (the tracer).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

# subscription_warned NAME SHARE_ID RELPATH: a warning stands for RELPATH.
subscription_warned() {
	[ -n "$(nx "$1" status --json 2>/dev/null | jq -r --arg s "$2" --arg r "$3" \
		'.subscriptions[]? | select(.share_id==$s) | .warnings[]? | select(.rel_path==$r) | .rel_path')" ]
}
# subscription_warning_reverted NAME SHARE_ID RELPATH: that warning says reverted.
subscription_warning_reverted() {
	[ "$(nx "$1" status --json 2>/dev/null | jq -r --arg s "$2" --arg r "$3" \
		'[.subscriptions[]? | select(.share_id==$s) | .warnings[]? | select(.rel_path==$r)][0].reverted // empty')" = "true" ]
}
# trash_kind NAME SHARE_ID RELPATH echoes the kind of the newest trashed copy.
trash_kind() {
	nx "$1" trash ls --json "$2" 2>/dev/null |
		jq -r --arg r "$3" '[.events[]?.items[]? | select(.rel_path==$r)][0].kind // empty'
}
trash_kind_is() { [ "$(trash_kind "$1" "$2" "$3")" = "$4" ]; }

harness_init receive-only-flags-local-edits

node_add alice
node_add bob
RW_SHARE="$(node_dir alice rw-share)"
BOB_RX="$(node_dir bob rx-synced)"
node_start alice
node_start bob
pair_nodes alice bob

RX_ID="$(share_and_subscribe alice "$RW_SHARE" rx-docs rw bob "$BOB_RX" receive)"

log "Alice's file reaches bob"
echo "rx original" >"$RW_SHARE/doc.txt"
wait_for "$SYNC_TIMEOUT" "the file to reach bob" file_content_is "$BOB_RX/doc.txt" "rx original"

log "Bob edits it and adds a new file"
echo "bob new" >"$BOB_RX/bob-new.txt"
echo "bob's local edit" >"$BOB_RX/doc.txt"

log "The edit is warned about before alice changes anything, and not reverted"
wait_for "$SYNC_TIMEOUT" "bob's edit to be warned about" subscription_warned bob "$RX_ID" doc.txt
subscription_warning_reverted bob "$RX_ID" doc.txt && fail "the warning says reverted, but alice has not changed the file"
file_content_is "$BOB_RX/doc.txt" "bob's local edit" || fail "bob's edit was reverted before alice changed the file: $(cat "$BOB_RX/doc.txt")"
[ -z "$(trash_kind bob "$RX_ID" doc.txt)" ] || fail "bob's edit is in his trash before alice changed the file"
info "ok: flagged, still on disk, nothing trashed"

log "Nothing bob wrote reaches alice"
echo "tracer" >"$RW_SHARE/tracer.txt"
wait_for "$SYNC_TIMEOUT" "a later change to reach bob" file_content_is "$BOB_RX/tracer.txt" "tracer"
path_absent "$RW_SHARE/bob-new.txt" || fail "bob's new file reached alice"
file_content_is "$RW_SHARE/doc.txt" "rx original" || fail "bob's edit reached alice: $(cat "$RW_SHARE/doc.txt")"
file_content_is "$BOB_RX/doc.txt" "bob's local edit" || fail "bob's edit was reverted by an unrelated change: $(cat "$BOB_RX/doc.txt")"

log "Alice changes the file: bob's edit is trashed and replaced"
echo "rx second" >"$RW_SHARE/doc.txt"
wait_for "$SYNC_TIMEOUT" "alice's change to replace bob's edit" file_content_is "$BOB_RX/doc.txt" "rx second"
wait_for "$SYNC_TIMEOUT" "bob's edit in his trash as 'overwritten'" trash_kind_is bob "$RX_ID" doc.txt overwritten
wait_for "$SYNC_TIMEOUT" "the warning to read reverted" subscription_warning_reverted bob "$RX_ID" doc.txt
path_present "$BOB_RX/bob-new.txt" || fail "bob's own new file was removed"

assert_mutually_clean alice bob

harness_pass "a receive-only edit was flagged when made, kept until alice changed the file, then trashed and replaced"
