#!/usr/bin/env bash
# scripts/e2e.sh — the canonical end-to-end check (SPEC.md §14): two real
# syncat daemons, on separate config/data dirs and API ports, talking to each
# other over real tailcat (DERP-relayed unless direct connectivity is
# available). Exercises the product the way a user would — init, exchange
# tokens, peer, share, subscribe — and then verifies bidirectional sync,
# delete/trash, and conflict resolution actually happen on disk.
#
# Usage: scripts/e2e.sh
#
# This is the happy path. The networking *failure* scenarios — link loss,
# hard kills, divergence, fan-out — live in scripts/net/ and run via
# scripts/run-net-tests.sh. Both are built on scripts/lib/harness.sh.
#
# Runnable from any directory. Cleans and reuses a fixed temp dir on every
# run (idempotent). Always tears down both daemons on exit, success or
# failure.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/lib/harness.sh"

harness_init e2e

log "Initializing alice and bob as independent nodes"
node_add alice
node_add bob
ALICE_SHARE="$(node_dir alice share)"
BOB_SYNCDIR="$(node_dir bob synced)"

log "Starting both daemons"
node_start alice
node_start bob

log "Exchanging node tokens and peering (both directions — peering is mutual)"
pair_nodes alice bob

log "Alice offers a read-write share; bob subscribes in mirror mode"
# read-write + mirror => bidirectional (SPEC.md §1).
SHARE_ID="$(share_and_subscribe alice "$ALICE_SHARE" docs rw bob "$BOB_SYNCDIR" mirror)"
info "share id: $SHARE_ID"

log "Alice writes a file; verifying it replicates to bob"
echo "hello from alice" >"$ALICE_SHARE/hello.txt"
wait_for "$SYNC_TIMEOUT" "hello.txt to replicate to bob with identical content" \
	file_content_is "$BOB_SYNCDIR/hello.txt" "hello from alice"

log "Bob writes a file; verifying it replicates to alice (bidirectional)"
echo "hello from bob" >"$BOB_SYNCDIR/from-bob.txt"
wait_for "$SYNC_TIMEOUT" "from-bob.txt to replicate to alice with identical content" \
	file_content_is "$ALICE_SHARE/from-bob.txt" "hello from bob"

log "Deleting a file on alice; verifying it disappears on bob and lands in bob's trash"
rm "$ALICE_SHARE/hello.txt"
wait_for "$SYNC_TIMEOUT" "hello.txt to be deleted on bob" path_absent "$BOB_SYNCDIR/hello.txt"
wait_for "$SYNC_TIMEOUT" "a trash copy of hello.txt on bob (the receiving side)" \
	trash_has bob "$SHARE_ID" "hello.txt"

log "Setting up a file both sides already have, for the conflict test"
echo "original content" >"$ALICE_SHARE/conflict.txt"
wait_for "$SYNC_TIMEOUT" "conflict.txt to replicate to bob before going offline" \
	file_content_is "$BOB_SYNCDIR/conflict.txt" "original content"

log "Stopping BOTH daemons to force an offline conflict"
# Both, not just bob. A conflict only exists if both sides have *indexed*
# their own edit before they talk again, and a node that is left running
# indexes through its watcher's 1s debounce — which races the other node
# restarting and reconnecting, a race it loses often enough to matter. When
# it loses, the incoming version simply dominates: the local edit is
# overwritten (into the trash, so it is recoverable, but out of the working
# tree) and no conflict copy is ever created.
#
# Stopping both removes the race rather than papering over it, because
# core.Open runs a *synchronous* full scan of every share before it starts
# dialing peers. Each side is therefore guaranteed to know its own edit
# before either can hear about the other's. It is also the more realistic
# scenario, and the one MANUAL-TESTS.md §9 lists as untested: both daemons
# down, divergent edits, then converge.
node_stop alice
node_stop bob

log "Editing the same file differently on both sides while both are offline"
echo "alice offline edit" >"$ALICE_SHARE/conflict.txt"
echo "bob offline edit" >"$BOB_SYNCDIR/conflict.txt"
# A file only alice has, used below as proof that the index exchange after
# the restart actually ran — so "no conflict copy" can never be confused
# with "the two nodes never talked".
echo "tracer" >"$ALICE_SHARE/tracer.txt"

log "Restarting both daemons"
node_start alice
node_start bob
wait_for "$CONNECT_TIMEOUT" "alice and bob to find each other again" mutually_connected alice bob

wait_for "$SYNC_TIMEOUT" "alice's tracer file to reach bob (so the index exchange demonstrably ran)" \
	file_content_is "$BOB_SYNCDIR/tracer.txt" "tracer"

log "Waiting for both sides to converge with a .sync-conflict- copy (SPEC.md §5: keep-both, newest wins the name)"
# A generous timeout: SPEC.md §4's dead-peer detection only fires after 90s
# without received traffic, so a reconnect that lands on a connection which
# looks handshaked but is still draining the old, now-dead session on the
# other side can legitimately take a bit over 90s to recover before a fresh
# dial even starts — on top of DERP setup and the sync itself.
wait_for 240 "a conflict copy to appear on alice" conflict_copy_present "$ALICE_SHARE"
wait_for 240 "a conflict copy to appear on bob" conflict_copy_present "$BOB_SYNCDIR"
wait_for 90 "conflict.txt content to converge between alice and bob" \
	files_identical "$ALICE_SHARE/conflict.txt" "$BOB_SYNCDIR/conflict.txt"
wait_for 90 "the conflict copy's filename to converge between alice and bob" \
	conflict_copy_names_match "$ALICE_SHARE" "$BOB_SYNCDIR"

WINNER="$(cat "$ALICE_SHARE/conflict.txt")"
case "$WINNER" in
"alice offline edit" | "bob offline edit") ;;
*) fail "unexpected winning content for conflict.txt: $WINNER" ;;
esac

ALICE_CONFLICT_FILE="$(ls "$ALICE_SHARE"/conflict.sync-conflict-*.txt | head -n1)"
BOB_CONFLICT_FILE="$(ls "$BOB_SYNCDIR"/conflict.sync-conflict-*.txt | head -n1)"
LOSER_ALICE="$(cat "$ALICE_CONFLICT_FILE")"
LOSER_BOB="$(cat "$BOB_CONFLICT_FILE")"
[ "$LOSER_ALICE" = "$LOSER_BOB" ] ||
	fail "conflict-copy content differs between alice ($LOSER_ALICE) and bob ($LOSER_BOB)"
case "$LOSER_ALICE" in
"alice offline edit" | "bob offline edit") ;;
*) fail "unexpected conflict-copy content: $LOSER_ALICE" ;;
esac
[ "$LOSER_ALICE" != "$WINNER" ] ||
	fail "conflict copy has the same content as the winner ($WINNER) — the conflict wasn't actually resolved"
info "winner: '$WINNER'   conflict copy ($(basename "$ALICE_CONFLICT_FILE")): '$LOSER_ALICE'"

log "Checking both nodes still report a clean, connected peer"
assert_mutually_clean alice bob

harness_pass "two real syncat daemons peered over tailcat and converged
      (bidirectional sync, delete+trash, and a two-sided conflict)."
