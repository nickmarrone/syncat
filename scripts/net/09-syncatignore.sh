#!/usr/bin/env bash
# .syncatignore: gitignore-syntax exclusions, enforced in both directions.
#
# SPEC.md §5 puts the ignore file at the share root. What makes it worth a
# scenario of its own is that "ignored" has to mean three different things at
# once, and only the first is obvious:
#
#   1. An ignored file never leaves this node.
#   2. An ignored file is never accepted from a peer either — and crucially,
#      not accepting it must not turn into deleting the peer's copy. A node
#      that pulled an ignored file would index it, ignore it on the next
#      scan, tombstone it, and propagate that tombstone back.
#   3. Adding a rule for a file that has ALREADY synced must leave every
#      peer's copy untouched. Ignoring is not deleting, and this is the one
#      that silently destroys user data if it is wrong.
#
# The ignore file itself is deliberately NOT synced: each node keeps its own,
# so the two sides can legitimately disagree about what is excluded.
#
# Every negative assertion below is paired with a tracer, the discipline
# 07-live-permission-change.sh uses: "it did not arrive" only means something
# once something else demonstrably did arrive over the same connection since.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init syncatignore

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"

log "Baseline: without any rules, everything syncs"
echo "baseline" >"$ALICE_SHARE/baseline.txt"
wait_for "$SYNC_TIMEOUT" "baseline.txt to reach bob" \
	file_content_is "$BOB_SYNC/baseline.txt" "baseline"

log "Alice starts ignoring *.log and build/"
printf '# alice does not share build output\n*.log\nbuild/\n' >"$ALICE_SHARE/.syncatignore"
echo "noisy" >"$ALICE_SHARE/debug.log"
mkdir -p "$ALICE_SHARE/build"
echo "artifact" >"$ALICE_SHARE/build/out.o"
echo "please share" >"$ALICE_SHARE/notes.txt"

# The tracer: notes.txt is written alongside the ignored files, so once it
# has crossed we know a full sync round has completed since they appeared.
wait_for "$SYNC_TIMEOUT" "the non-ignored sibling to reach bob" \
	file_content_is "$BOB_SYNC/notes.txt" "please share"

path_absent "$BOB_SYNC/debug.log" ||
	fail "an ignored file (*.log) reached bob"
path_absent "$BOB_SYNC/build" ||
	fail "an ignored directory (build/) reached bob"
info "ok: ignored paths did not reach bob, on a connection that is demonstrably syncing"

log "The ignore file itself stays local to the node that wrote it"
path_absent "$BOB_SYNC/.syncatignore" ||
	fail ".syncatignore was synced to bob; it is per-node and must not be"

log "Ignoring an already-synced file must not delete it on the peer"
echo "we both have this" >"$ALICE_SHARE/shared.txt"
wait_for "$SYNC_TIMEOUT" "shared.txt to reach bob before it is ignored" \
	file_content_is "$BOB_SYNC/shared.txt" "we both have this"

printf '# alice does not share build output\n*.log\nbuild/\nshared.txt\n' >"$ALICE_SHARE/.syncatignore"

# Force the rescan that picks up the edited rule, then prove a full sync
# round completed afterwards before asserting bob still has the file.
echo "tracer one" >"$ALICE_SHARE/tracer1.txt"
wait_for "$SYNC_TIMEOUT" "a later change from alice to reach bob" \
	file_content_is "$BOB_SYNC/tracer1.txt" "tracer one"

# Hold, rather than check once: a propagating tombstone would arrive shortly
# after the rescan, not instantly.
hold_for 15 "bob's copy of a newly-ignored file to survive" \
	path_present "$BOB_SYNC/shared.txt"
info "ok: ignoring a synced file left the peer's copy alone"

log "The reverse direction: bob ignores what alice is still sharing"
printf '*.secret\n' >"$BOB_SYNC/.syncatignore"
echo "alice's secret" >"$ALICE_SHARE/creds.secret"
echo "tracer two" >"$ALICE_SHARE/tracer2.txt"
wait_for "$SYNC_TIMEOUT" "a later change from alice to reach bob" \
	file_content_is "$BOB_SYNC/tracer2.txt" "tracer two"

path_absent "$BOB_SYNC/creds.secret" ||
	fail "bob pulled a file its own .syncatignore excludes"
# The flap loop's final act would have been to propagate a tombstone back and
# delete alice's original, so this is the assertion that catches it.
path_present "$ALICE_SHARE/creds.secret" ||
	fail "alice's own file was deleted — bob pulled an ignored file and tombstoned it back"
info "ok: bob refused the file and alice's original is intact"

log "Removing a rule lets the file sync again"
printf '# alice does not share build output\n*.log\nbuild/\n' >"$ALICE_SHARE/.syncatignore"
echo "un-ignored now" >"$ALICE_SHARE/shared.txt"
wait_for "$SYNC_TIMEOUT" "shared.txt to sync again once the rule is removed" \
	file_content_is "$BOB_SYNC/shared.txt" "un-ignored now"

harness_pass
