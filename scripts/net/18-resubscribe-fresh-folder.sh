#!/usr/bin/env bash
# Removing a subscription and subscribing again, into a different folder.
#
# `subscription rm` leaves bob's old folder alone but must also forget the
# index state it kept for the share. If it doesn't, the new, empty folder is
# read as the old copy with every file deleted: the share goes on hold, and
# the hold's own remedy (`share confirm-deletes`) would push deletes for the
# whole share to alice. Issue #44.
#
# Re-subscribing is by full keys: subscribing by name hits a separate bug (#42).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init resubscribe-fresh-folder

node_add alice
node_add bob
DOCS="$(node_dir alice docs)"
OTHER="$(node_dir alice other)"
BOB_DOCS="$(node_dir bob docs)"
BOB_OTHER="$(node_dir bob other)"
BOB_DOCS2="$(node_dir bob docs2)"
node_start alice
node_start bob
pair_nodes alice bob

DOCS_ID="$(share_and_subscribe alice "$DOCS" docs rw bob "$BOB_DOCS" mirror)"
share_and_subscribe alice "$OTHER" other rw bob "$BOB_OTHER" mirror >/dev/null

log "Syncing files in both directions"
echo "one" >"$DOCS/one.txt"
echo "two" >"$DOCS/two.txt"
wait_for "$SYNC_TIMEOUT" "alice's files to reach bob" file_content_is "$BOB_DOCS/two.txt" "two"
echo "from bob" >"$BOB_DOCS/from-bob.txt"
wait_for "$SYNC_TIMEOUT" "bob's file to reach alice" file_content_is "$DOCS/from-bob.txt" "from bob"

log "Removing the subscription keeps bob's copy and stops sync"
nx bob subscription rm alice docs >/dev/null || fail "subscription rm failed"
wait_for 30 "bob to have one subscription left" test "$(subscription_count bob)" = 1
echo "after rm" >"$DOCS/three.txt"

holds_on_bob() { nx bob status --json 2>/dev/null | jq -r '.holds | length'; }

log "Subscribing again, into a fresh folder, starts clean"
nx bob subscription add "$(peer_id_of bob alice)" "$DOCS_ID" "$BOB_DOCS2" --mode mirror >/dev/null ||
	fail "re-subscribing failed"
wait_for "$SYNC_TIMEOUT" "the whole share to arrive in the new folder" file_content_is "$BOB_DOCS2/three.txt" "after rm"
for f in one.txt two.txt from-bob.txt; do
	path_present "$BOB_DOCS2/$f" || fail "re-subscribing did not bring $f"
done
assert_eq "$(holds_on_bob)" 0 "bob holds after re-subscribing"
echo "after re-add" >"$BOB_DOCS2/four.txt"
wait_for "$SYNC_TIMEOUT" "bob's change in the new folder to reach alice" file_content_is "$DOCS/four.txt" "after re-add"

log "Alice must have lost nothing, and bob's old folder must be untouched"
for f in one.txt two.txt from-bob.txt three.txt; do
	path_present "$DOCS/$f" || fail "alice lost $f after bob re-subscribed"
done
for f in one.txt two.txt from-bob.txt; do
	path_present "$BOB_DOCS/$f" || fail "bob's old folder lost $f"
done
path_absent "$BOB_DOCS/three.txt" || fail "the removed subscription's old folder kept syncing"
assert_eq "$(holds_on_bob)" 0 "bob holds at the end"

assert_mutually_clean alice bob

harness_pass "re-subscribing into a fresh folder pulls the whole share, with no hold and no deletes sent to the offerer"
