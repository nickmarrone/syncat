#!/usr/bin/env bash
# Subscribing by peer and share NAME, as documented.
#
# `syncat subscription add alice docs DIR` takes names, not keys. The
# subscription used to be created but the API answered 500 ("subscription
# added but not found afterward") and the CLI exited non-zero, because the
# handler looked the new subscription up by the raw refs instead of the
# resolved keys (#42). Every other scenario passes full keys via
# share_and_subscribe, so none of them could see it. This one passes names
# and requires exit 0, the share to arrive, and a change to flow back.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init subscribe-by-name

node_add alice
node_add bob
DOCS="$(node_dir alice docs)"
BOB_DOCS="$(node_dir bob docs)"
node_start alice
node_start bob
pair_nodes alice bob

echo "one" >"$DOCS/one.txt"
echo "two" >"$DOCS/two.txt"
nx alice share add "$DOCS" --name docs --perm rw >/dev/null
SID="$(share_id_of alice docs)"
[ -n "$SID" ] || fail "could not determine alice's share id for 'docs'"
wait_for 60 "bob to see alice's 'docs' share" remote_share_seen bob "$SID"

log "Subscribing with the peer name and the share name, not keys"
nx bob subscription add alice docs "$BOB_DOCS" --mode mirror >/dev/null ||
	fail "subscription add by names exited non-zero"

wait_for "$SYNC_TIMEOUT" "the whole share to arrive in bob's folder" file_content_is "$BOB_DOCS/two.txt" "two"
for f in one.txt two.txt; do
	path_present "$BOB_DOCS/$f" || fail "subscribing by name did not bring $f"
done

log "A change from bob must reach alice"
echo "from bob" >"$BOB_DOCS/from-bob.txt"
wait_for "$SYNC_TIMEOUT" "bob's new file to reach alice" file_content_is "$DOCS/from-bob.txt" "from bob"

assert_mutually_clean alice bob

harness_pass "subscription add by peer and share name exits 0 and syncs both ways"
