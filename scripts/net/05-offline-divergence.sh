#!/usr/bin/env bash
# Both nodes edit the same share while they cannot see each other, then meet
# again. This is the case that makes a sync tool worth having, and the one
# with the most ways to be subtly wrong.
#
# The divergence is deliberately mixed, because the four cases resolve by
# different rules and a test that only conflicts one file checks one of them:
#
#   both.txt     edited on both sides      -> keep-both, one winner plus an
#                                             identically-named conflict copy
#   alice.txt    created only on alice     -> arrives on bob
#   bob.txt      created only on bob       -> arrives on alice
#   gone.txt     deleted on alice          -> deleted on bob, into bob's trash
#
# Convergence here means both directories agree afterwards, not merely that
# each side kept something.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init offline-divergence

node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNC="$(node_dir bob synced)"
node_start alice
node_start bob
pair_nodes alice bob

SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNC" mirror)"

log "Seeding the files both sides will start from"
echo "original" >"$ALICE_SHARE/both.txt"
echo "doomed" >"$ALICE_SHARE/gone.txt"
wait_for "$SYNC_TIMEOUT" "both.txt to reach bob" file_content_is "$BOB_SYNC/both.txt" "original"
wait_for "$SYNC_TIMEOUT" "gone.txt to reach bob" file_content_is "$BOB_SYNC/gone.txt" "doomed"

log "Taking BOTH nodes offline and diverging them"
# Both, not just bob. A conflict only exists if each side has *indexed* its
# own edit before the two talk again. A node left running indexes through
# its watcher's 1s debounce, which races the other node restarting and
# reconnecting — and when it loses that race the incoming version simply
# dominates: the local edit is overwritten (into the trash, so recoverable,
# but out of the working tree) and no conflict copy is ever created. That is
# a real property of an index-based reconciler, not something a test should
# be gambling on.
#
# Stopping both removes the race outright, because core.Open runs a
# synchronous full scan of every share before it starts dialing peers, so
# each side is guaranteed to know its own edits before either can hear about
# the other's. It is also the scenario MANUAL-TESTS.md §9 lists as untested:
# both daemons down, divergent edits, then converge.
node_stop alice
node_stop bob

echo "alice edit" >"$ALICE_SHARE/both.txt"
echo "only alice" >"$ALICE_SHARE/alice.txt"
rm "$ALICE_SHARE/gone.txt"
echo "bob edit" >"$BOB_SYNC/both.txt"
echo "only bob" >"$BOB_SYNC/bob.txt"
# Proof, below, that the index exchange actually ran — so "no conflict copy"
# can never be mistaken for "the two nodes never talked".
echo "tracer" >"$ALICE_SHARE/tracer.txt"

log "Bringing both back"
node_start alice
node_start bob
wait_for "$CONNECT_TIMEOUT" "alice and bob to find each other again" mutually_connected alice bob
wait_for "$SYNC_TIMEOUT" "alice's tracer to reach bob (so the index exchange demonstrably ran)" \
	file_content_is "$BOB_SYNC/tracer.txt" "tracer"

log "Every kind of divergence must resolve"
wait_for "$SYNC_TIMEOUT" "alice-only file to reach bob" file_content_is "$BOB_SYNC/alice.txt" "only alice"
wait_for "$SYNC_TIMEOUT" "bob-only file to reach alice" file_content_is "$ALICE_SHARE/bob.txt" "only bob"
wait_for "$SYNC_TIMEOUT" "alice's delete to reach bob" path_absent "$BOB_SYNC/gone.txt"
wait_for "$SYNC_TIMEOUT" "the deleted file to land in bob's trash" trash_has bob "$SHARE_ID" gone.txt

log "The two-sided edit must resolve keep-both, identically on both sides"
wait_for 240 "a conflict copy on alice" conflict_copy_present "$ALICE_SHARE"
wait_for 240 "a conflict copy on bob" conflict_copy_present "$BOB_SYNC"
wait_for 90 "both.txt to converge" files_identical "$ALICE_SHARE/both.txt" "$BOB_SYNC/both.txt"
wait_for 90 "the conflict copy's name to converge" conflict_copy_names_match "$ALICE_SHARE" "$BOB_SYNC"

WINNER="$(cat "$ALICE_SHARE/both.txt")"
LOSER="$(cat "$(ls "$ALICE_SHARE"/both.sync-conflict-*.txt | head -n1)")"
case "$WINNER" in "alice edit" | "bob edit") ;; *) fail "unexpected winner for both.txt: '$WINNER'" ;; esac
case "$LOSER" in "alice edit" | "bob edit") ;; *) fail "unexpected conflict-copy content: '$LOSER'" ;; esac
[ "$WINNER" != "$LOSER" ] || fail "the conflict copy holds the same content as the winner — nothing was actually kept"
info "winner '$WINNER', conflict copy '$LOSER'"

log "The two directories must now agree, file for file"
DIFF="$(diff <(cd "$ALICE_SHARE" && ls -1) <(cd "$BOB_SYNC" && ls -1) || true)"
[ -z "$DIFF" ] || fail "the two directories did not converge:
$DIFF"
info "ok: alice and bob hold the same set of files"

assert_mutually_clean alice bob

harness_pass "edits, creations, deletes and a two-sided conflict all diverged offline and converged identically"
