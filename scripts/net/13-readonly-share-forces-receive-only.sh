#!/usr/bin/env bash
# A read-only share forces its subscribers to receive-only (SPEC.md §5, §6).
#
# Bob subscribes with --mode mirror, which on a read-only share is
# deliberate: the permission wins over the mode he asked for. So nothing bob
# writes reaches alice, and when alice next changes a file bob edited, bob's
# edit goes to his trash as "overwritten" and alice's version replaces it.
# He must not get a ".sync-conflict-*" copy, which is what a full mirror
# does, and the edit is flagged as a warning on his subscription.
#
# "Did not arrive" is only asserted after a later change from alice has
# arrived over the same connection (the tracer), so it cannot pass just
# because sync was slow or dead.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

# --- helpers (scenario-local) ----------------------------------------------

# subscription_warned NAME SHARE_ID RELPATH succeeds once NAME's subscription
# to SHARE_ID carries a warning about RELPATH.
subscription_warned() {
	[ -n "$(nx "$1" status --json 2>/dev/null | jq -r --arg s "$2" --arg r "$3" \
		'.subscriptions[]? | select(.share_id==$s) | .warnings[]? | select(.rel_path==$r) | .rel_path')" ]
}

# trash_kind NAME SHARE_ID RELPATH echoes the kind of the newest trashed copy
# of RELPATH, or nothing.
trash_kind() {
	nx "$1" trash ls --json "$2" 2>/dev/null |
		jq -r --arg r "$3" '[.events[]?.items[]? | select(.rel_path==$r)][0].kind // empty'
}
trash_kind_is() { [ "$(trash_kind "$1" "$2" "$3")" = "$4" ]; }

# --- scenario ----------------------------------------------------------------

harness_init readonly-share-forces-receive-only

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs ro bob "$BOB_SYNC" mirror)"

log "Alice's file reaches bob"
echo "original" >"$ALICE_SHARE/doc.txt"
wait_for "$SYNC_TIMEOUT" "alice's file to reach bob" file_content_is "$BOB_SYNC/doc.txt" "original"

log "Bob writes a new file and edits a synced one"
echo "bob new" >"$BOB_SYNC/bob-new.txt"
echo "bob's local edit" >"$BOB_SYNC/doc.txt"

echo "tracer" >"$ALICE_SHARE/tracer.txt"
wait_for "$SYNC_TIMEOUT" "a later change from alice to reach bob" file_content_is "$BOB_SYNC/tracer.txt" "tracer"
path_absent "$ALICE_SHARE/bob-new.txt" || fail "bob's new file reached alice's read-only share"
file_content_is "$ALICE_SHARE/doc.txt" "original" ||
	fail "bob's edit reached alice's read-only share: $(cat "$ALICE_SHARE/doc.txt")"
info "ok: nothing bob wrote reached alice, on a connection that is demonstrably syncing"

log "Alice changes the file bob edited: bob's edit is trashed and replaced"
echo "second" >"$ALICE_SHARE/doc.txt"
wait_for "$SYNC_TIMEOUT" "alice's change to replace bob's edit" file_content_is "$BOB_SYNC/doc.txt" "second"
wait_for "$SYNC_TIMEOUT" "bob's edit in his trash as 'overwritten'" trash_kind_is bob "$SHARE_ID" doc.txt overwritten
wait_for "$SYNC_TIMEOUT" "bob's edit to be warned about" subscription_warned bob "$SHARE_ID" doc.txt
conflict_copy_present "$BOB_SYNC" && fail "bob got a conflict copy: $(ls "$BOB_SYNC")"
path_present "$BOB_SYNC/bob-new.txt" || fail "bob's own new file was removed"
info "ok: bob's edit was trashed as overwritten, no conflict copy, his new file stayed local"

assert_mutually_clean alice bob

harness_pass "a mirror subscription to a read-only share is forced receive-only: edits are not sent, are trashed as overwritten and warned about, with no conflict copy"
