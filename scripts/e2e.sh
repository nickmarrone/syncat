#!/usr/bin/env bash
# scripts/e2e.sh — manual end-to-end verification (SPEC.md §14): two real
# syncat daemons, on separate config/data dirs and API ports, talking to
# each other over real tailcat (DERP-relayed unless direct connectivity is
# available). Exercises the product the way a user would: init, exchange
# tokens, peer, share, subscribe, and then verify bidirectional sync,
# delete/trash, and conflict resolution actually happen on disk.
#
# Usage: scripts/e2e.sh
#
# Runnable from any directory. Cleans and reuses a fixed temp dir on every
# run (idempotent). Always tears down both daemons on exit, success or
# failure.

set -euo pipefail

# --- layout --------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

WORK="${TMPDIR:-/tmp}/syncat-e2e"
BIN="$WORK/bin/syncat"

ALICE_CONFIG="$WORK/alice/config"
ALICE_DATA="$WORK/alice/data"
ALICE_SHARE="$WORK/alice/share"
ALICE_LOG="$WORK/alice/daemon.log"

BOB_CONFIG="$WORK/bob/config"
BOB_DATA="$WORK/bob/data"
BOB_SYNCDIR="$WORK/bob/synced"
BOB_LOG="$WORK/bob/daemon.log"

ALICE_API_ADDR="127.0.0.1:18347"
BOB_API_ADDR="127.0.0.1:18348"

ALICE_PID=""
BOB_PID=""
SHARE_ID=""

# --- output helpers --------------------------------------------------------

STEP=0
log() { STEP=$((STEP + 1)); printf '\n[%02d] %s\n' "$STEP" "$*"; }
info() { printf '      %s\n' "$*"; }

fail() {
	echo
	echo "FAIL: $*" >&2
	if [ -f "$ALICE_LOG" ]; then
		echo "--- alice daemon log (tail) ---" >&2
		tail -n 60 "$ALICE_LOG" >&2 || true
	fi
	if [ -f "$BOB_LOG" ]; then
		echo "--- bob daemon log (tail) ---" >&2
		tail -n 60 "$BOB_LOG" >&2 || true
	fi
	exit 1
}

cleanup() {
	local ec=$?
	if [ -n "$ALICE_PID" ]; then
		kill "$ALICE_PID" >/dev/null 2>&1 || true
		wait "$ALICE_PID" 2>/dev/null || true
	fi
	if [ -n "$BOB_PID" ]; then
		kill "$BOB_PID" >/dev/null 2>&1 || true
		wait "$BOB_PID" 2>/dev/null || true
	fi
	if [ $ec -eq 0 ]; then
		echo
		echo "PASS: two real syncat daemons peered over tailcat and converged"
		echo "      (bidirectional sync, delete+trash, and a two-sided conflict)."
	fi
	exit $ec
}
trap cleanup EXIT

alice() { "$BIN" --config "$ALICE_CONFIG" --data "$ALICE_DATA" "$@"; }
bob() { "$BIN" --config "$BOB_CONFIG" --data "$BOB_DATA" "$@"; }

# wait_for TIMEOUT_SECS DESCRIPTION PREDICATE [ARGS...]
# Polls PREDICATE (a shell function or command) once a second until it
# exits 0, or fails loudly with a log dump once TIMEOUT_SECS elapses.
# Never a bare sleep: every asynchronous condition in this script is
# awaited through this poll loop.
wait_for() {
	local timeout="$1" desc="$2"
	shift 2
	local start elapsed
	start=$(date +%s)
	until "$@" >/dev/null 2>&1; do
		elapsed=$(($(date +%s) - start))
		if [ "$elapsed" -ge "$timeout" ]; then
			fail "timed out after ${timeout}s waiting for: $desc"
		fi
		sleep 1
	done
	info "ok: $desc (${elapsed:-0}s)"
}

# --- predicates used with wait_for -----------------------------------------

alice_api_up() { alice status >/dev/null 2>&1; }
bob_api_up() { bob status >/dev/null 2>&1; }

alice_sees_bob_connected() {
	[ "$(alice status --json 2>/dev/null | jq -r '.peers[]? | select(.name=="bob") | .state // empty')" = "connected" ]
}
bob_sees_alice_connected() {
	[ "$(bob status --json 2>/dev/null | jq -r '.peers[]? | select(.name=="alice") | .state // empty')" = "connected" ]
}

bob_sees_alice_share() {
	[ -n "$(bob remote ls --json 2>/dev/null | jq -r --arg id "$SHARE_ID" '.[]? | select(.share_id==$id) | .share_id')" ]
}

file_content_is() {
	local path="$1" expected="$2"
	[ -f "$path" ] && [ "$(cat "$path" 2>/dev/null)" = "$expected" ]
}

path_absent() { [ ! -e "$1" ]; }

bob_trash_has() {
	local rel="$1"
	[ -n "$(bob trash ls --json "$SHARE_ID" 2>/dev/null | jq -r --arg r "$rel" '.[]? | select(.rel_path==$r) | .rel_path')" ]
}

conflict_copy_present() {
	local dir="$1"
	compgen -G "$dir/conflict.sync-conflict-*.txt" >/dev/null 2>&1
}

conflict_txt_converged() {
	[ -f "$ALICE_SHARE/conflict.txt" ] && [ -f "$BOB_SYNCDIR/conflict.txt" ] &&
		[ "$(cat "$ALICE_SHARE/conflict.txt")" = "$(cat "$BOB_SYNCDIR/conflict.txt")" ]
}

conflict_copy_name_converged() {
	local a b
	a="$(basename "$(ls "$ALICE_SHARE"/conflict.sync-conflict-*.txt 2>/dev/null | head -n1)" 2>/dev/null || true)"
	b="$(basename "$(ls "$BOB_SYNCDIR"/conflict.sync-conflict-*.txt 2>/dev/null | head -n1)" 2>/dev/null || true)"
	[ -n "$a" ] && [ "$a" = "$b" ]
}

# --- run ---------------------------------------------------------------

log "Cleaning up any previous run"
rm -rf "$WORK"
mkdir -p "$WORK/bin" "$ALICE_CONFIG" "$ALICE_DATA" "$ALICE_SHARE" "$BOB_CONFIG" "$BOB_DATA" "$BOB_SYNCDIR"
info "work dir: $WORK"

log "Building syncat (CGO_ENABLED=0, pure-Go SQLite per SPEC.md §12)"
(cd "$REPO_ROOT" && CGO_ENABLED=0 go build -o "$BIN" ./cmd/syncat)
info "built $BIN"

log "Initializing alice and bob as independent nodes"
alice init --name alice
bob init --name bob

log "Assigning each node its own loopback API port"
set_api_addr() {
	local cfg="$1" addr="$2" tmp
	tmp="$(mktemp)"
	jq --arg a "$addr" '.api_addr = $a' "$cfg" >"$tmp" && mv "$tmp" "$cfg"
}
set_api_addr "$ALICE_CONFIG/config.json" "$ALICE_API_ADDR"
set_api_addr "$BOB_CONFIG/config.json" "$BOB_API_ADDR"
info "alice api: $ALICE_API_ADDR   bob api: $BOB_API_ADDR"

log "Starting both daemons in the background"
# `exec` inside the backgrounded subshell replaces the subshell's process
# image with syncat itself, so $! is the daemon's actual PID — without
# it, $! would be the subshell wrapper's PID, `kill "$ALICE_PID"` would
# leave the real daemon process running as an orphan, and the trap's
# "clean teardown on every exit path" guarantee would be broken.
(exec "$BIN" --config "$ALICE_CONFIG" --data "$ALICE_DATA" daemon) >"$ALICE_LOG" 2>&1 &
ALICE_PID=$!
(exec "$BIN" --config "$BOB_CONFIG" --data "$BOB_DATA" daemon) >"$BOB_LOG" 2>&1 &
BOB_PID=$!
info "alice pid=$ALICE_PID  bob pid=$BOB_PID"

log "Waiting for both REST APIs to come up"
wait_for 20 "alice's API to accept requests" alice_api_up
wait_for 20 "bob's API to accept requests" bob_api_up

log "Exchanging node tokens and peering (both directions — peering is mutual)"
ALICE_TOKEN="$(alice token)"
BOB_TOKEN="$(bob token)"
alice peer add "$BOB_TOKEN" --name bob
bob peer add "$ALICE_TOKEN" --name alice

log "Waiting for the tunnel to establish over real tailcat (DERP relay setup can take a while)"
wait_for 180 "alice to see bob as connected" alice_sees_bob_connected
wait_for 180 "bob to see alice as connected" bob_sees_alice_connected

log "Alice offers a read-write share"
alice share add "$ALICE_SHARE" --name docs --perm rw
SHARE_ID="$(alice share ls --json | jq -r '.[] | select(.name=="docs") | .id')"
[ -n "$SHARE_ID" ] || fail "could not determine alice's share id for 'docs'"
info "share id: $SHARE_ID"

log "Waiting for bob to see alice's offered share"
wait_for 60 "bob to see alice's docs share" bob_sees_alice_share

ALICE_PEER_KEY="$(bob peer ls --json | jq -r '.[] | select(.name=="alice") | .id')"
[ -n "$ALICE_PEER_KEY" ] || fail "could not determine alice's peer key from bob"

log "Bob subscribes to alice's share in mirror mode (read-write + mirror => bidirectional, SPEC.md §1)"
bob subscribe "$ALICE_PEER_KEY" "$SHARE_ID" "$BOB_SYNCDIR" --mode mirror

log "Alice writes a file; verifying it replicates to bob"
echo "hello from alice" >"$ALICE_SHARE/hello.txt"
wait_for 60 "hello.txt to replicate to bob with identical content" file_content_is "$BOB_SYNCDIR/hello.txt" "hello from alice"

log "Bob writes a file; verifying it replicates to alice (bidirectional)"
echo "hello from bob" >"$BOB_SYNCDIR/from-bob.txt"
wait_for 60 "from-bob.txt to replicate to alice with identical content" file_content_is "$ALICE_SHARE/from-bob.txt" "hello from bob"

log "Deleting a file on alice; verifying it disappears on bob and lands in bob's trash"
rm "$ALICE_SHARE/hello.txt"
wait_for 60 "hello.txt to be deleted on bob" path_absent "$BOB_SYNCDIR/hello.txt"
wait_for 60 "a trash copy of hello.txt to appear on bob (the receiving side)" bob_trash_has "hello.txt"

log "Setting up a file both sides already have, for the conflict test"
echo "original content" >"$ALICE_SHARE/conflict.txt"
wait_for 60 "conflict.txt to replicate to bob before going offline" file_content_is "$BOB_SYNCDIR/conflict.txt" "original content"

log "Stopping bob's daemon to force an offline conflict"
kill "$BOB_PID"
wait "$BOB_PID" 2>/dev/null || true
BOB_PID=""

log "Editing the same file differently on both sides while bob is offline"
echo "alice offline edit" >"$ALICE_SHARE/conflict.txt"
echo "bob offline edit" >"$BOB_SYNCDIR/conflict.txt"

log "Restarting bob's daemon"
(exec "$BIN" --config "$BOB_CONFIG" --data "$BOB_DATA" daemon) >>"$BOB_LOG" 2>&1 &
BOB_PID=$!
wait_for 20 "bob's API to come back up" bob_api_up
wait_for 180 "bob to reconnect to alice" bob_sees_alice_connected

log "Waiting for both sides to converge with a .sync-conflict- copy (SPEC.md §5: keep-both, newest wins the name)"
# A generous timeout here: SPEC.md §4's dead-peer detection only fires
# after 90s without received traffic (the reconnect after "restarting
# bob's daemon" above can land on a connection that looks handshaked but
# is actually still draining the old, now-dead session on the other
# side), so recovery can legitimately take a bit over 90s before a fresh
# dial even starts, on top of DERP setup and the sync itself.
wait_for 240 "a conflict copy to appear on alice" conflict_copy_present "$ALICE_SHARE"
wait_for 240 "a conflict copy to appear on bob" conflict_copy_present "$BOB_SYNCDIR"
wait_for 90 "conflict.txt content to converge between alice and bob" conflict_txt_converged
wait_for 90 "the conflict copy's filename to converge between alice and bob" conflict_copy_name_converged

WINNER="$(cat "$ALICE_SHARE/conflict.txt")"
case "$WINNER" in
"alice offline edit" | "bob offline edit") ;;
*) fail "unexpected winning content for conflict.txt: $WINNER" ;;
esac

ALICE_CONFLICT_FILE="$(ls "$ALICE_SHARE"/conflict.sync-conflict-*.txt | head -n1)"
BOB_CONFLICT_FILE="$(ls "$BOB_SYNCDIR"/conflict.sync-conflict-*.txt | head -n1)"
LOSER_ALICE="$(cat "$ALICE_CONFLICT_FILE")"
LOSER_BOB="$(cat "$BOB_CONFLICT_FILE")"
[ "$LOSER_ALICE" = "$LOSER_BOB" ] || fail "conflict-copy content differs between alice ($LOSER_ALICE) and bob ($LOSER_BOB)"
case "$LOSER_ALICE" in
"alice offline edit" | "bob offline edit") ;;
*) fail "unexpected conflict-copy content: $LOSER_ALICE" ;;
esac
[ "$LOSER_ALICE" != "$WINNER" ] || fail "conflict copy has the same content as the winner ($WINNER) — conflict wasn't actually resolved"
info "winner content: '$WINNER'  conflict copy ($(basename "$ALICE_CONFLICT_FILE")): '$LOSER_ALICE'"

log "Tearing down both daemons"
kill "$ALICE_PID" >/dev/null 2>&1 || true
kill "$BOB_PID" >/dev/null 2>&1 || true
wait "$ALICE_PID" 2>/dev/null || true
wait "$BOB_PID" 2>/dev/null || true
ALICE_PID=""
BOB_PID=""

# cleanup's EXIT trap prints the final PASS line.
