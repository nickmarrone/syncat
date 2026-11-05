#!/usr/bin/env bash
# What a peer deletes or overwrites goes to the trash, and undoing it spreads.
#
# SPEC.md §7: content discarded on a peer's behalf (a remote delete, or a
# remote edit landing on top of a local file) is moved into the trash inside
# the share, never removed. `syncat trash undo` puts it back as a new local
# change, which then propagates like any other edit. A node's own deletes are
# never trashed: the trash undoes what *peers* did.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init trash-undo

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"

log "Seeding the files alice is about to delete and overwrite"
echo "keep me" >"$ALICE_SHARE/deleted.txt"
echo "first version" >"$ALICE_SHARE/edited.txt"
wait_for "$SYNC_TIMEOUT" "deleted.txt to reach bob" file_content_is "$BOB_SYNC/deleted.txt" "keep me"
wait_for "$SYNC_TIMEOUT" "edited.txt to reach bob" file_content_is "$BOB_SYNC/edited.txt" "first version"

log "Alice deletes one file and overwrites the other"
rm "$ALICE_SHARE/deleted.txt"
echo "second version" >"$ALICE_SHARE/edited.txt"
wait_for "$SYNC_TIMEOUT" "the delete to reach bob" path_absent "$BOB_SYNC/deleted.txt"
wait_for "$SYNC_TIMEOUT" "the overwrite to reach bob" file_content_is "$BOB_SYNC/edited.txt" "second version"

log "Bob's trash holds both, each with the right kind"
wait_for "$SYNC_TIMEOUT" "deleted.txt in bob's trash as 'deleted'" trash_kind_is bob "$SHARE_ID" deleted.txt deleted
wait_for "$SYNC_TIMEOUT" "edited.txt's first version in bob's trash as 'overwritten'" \
	trash_kind_is bob "$SHARE_ID" edited.txt overwritten
trash_has alice "$SHARE_ID" deleted.txt && fail "alice's own delete went into her trash; only peers' changes should"
info "ok: alice's own delete is not in her trash"

event_with() {
	nx bob trash ls --json "$SHARE_ID" | jq -r --arg r "$1" '[.events[] | select(any(.items[]; .rel_path==$r))][0].id'
}

log "Undoing the delete on bob brings the file back on both sides"
EVENT="$(event_with deleted.txt)"
[ -n "$EVENT" ] && [ "$EVENT" != "null" ] || fail "no trash event on bob holds deleted.txt"
nx bob trash undo "$SHARE_ID" "$EVENT" deleted.txt >/dev/null || fail "trash undo of deleted.txt failed"
wait_for "$SYNC_TIMEOUT" "deleted.txt to be back on bob" file_content_is "$BOB_SYNC/deleted.txt" "keep me"
wait_for "$SYNC_TIMEOUT" "the restore to reach alice" file_content_is "$ALICE_SHARE/deleted.txt" "keep me"

log "Undoing the overwrite on bob brings the first version back on both sides"
EVENT="$(event_with edited.txt)"
[ -n "$EVENT" ] && [ "$EVENT" != "null" ] || fail "no trash event on bob holds edited.txt"
nx bob trash undo "$SHARE_ID" "$EVENT" edited.txt >/dev/null || fail "trash undo of edited.txt failed"
wait_for "$SYNC_TIMEOUT" "edited.txt's first version to be back on bob" file_content_is "$BOB_SYNC/edited.txt" "first version"
wait_for "$SYNC_TIMEOUT" "the restore to reach alice" file_content_is "$ALICE_SHARE/edited.txt" "first version"
# Restoring over a file trashes the file it replaces, so the undo is itself undoable.
wait_for "$SYNC_TIMEOUT" "the replaced second version in bob's trash" trash_kind_is bob "$SHARE_ID" edited.txt overwritten

! conflict_copy_present "$ALICE_SHARE" || fail "a restore produced a conflict copy on alice"
! conflict_copy_present "$BOB_SYNC" || fail "a restore produced a conflict copy on bob"
info "ok: restores propagated as ordinary edits, with no conflict copies"

assert_mutually_clean alice bob

harness_pass "a peer's delete and overwrite were trashed, and undoing each restored it on both nodes"
