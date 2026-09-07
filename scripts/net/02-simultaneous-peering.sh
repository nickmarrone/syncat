#!/usr/bin/env bash
# The dedup race, on the real relay, repeated.
#
# When both nodes are given each other's token at the same instant, both dial
# and SPEC.md §2.4 keeps exactly one of the two connections — the one dialed
# by the node with the higher Ed25519 key. The loser's connection is closed
# by the winner, and that close lands on whatever the losing side happened to
# be doing, including the last step of its own handshake.
#
# That is where a node used to end up reporting "backing_off", with the
# teardown's error attached, for the entire life of a healthy connection it
# was actively syncing over: the losing dial's failure overwrote the state
# the winning connection had already set. Both sides looked fine from the
# other end, which is why it survived so long.
#
# So the assertion here is deliberately stronger than "it connected": both
# sides must report connected *and* carry no error, every round.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init simultaneous-peering

ROUNDS="${SYNCAT_PEER_ROUNDS:-3}"

node_add alice
node_add bob
node_start alice
node_start bob

ALICE_KEY="$(peer_key_of alice)"
BOB_KEY="$(peer_key_of bob)"

peer_gone() { [ "$(nx "$1" status --json 2>/dev/null | jq -r '.peers | length')" = "0" ]; }

for round in $(seq 1 "$ROUNDS"); do
	log "Round $round/$ROUNDS: both nodes add each other at the same instant"
	# Backgrounded so the two dials genuinely overlap. Running them in
	# sequence lets the first connection settle before the second node even
	# starts dialing, which is exactly the case that never reproduced.
	nx alice peer add "$(nx bob token)" --name bob >/dev/null &
	local_a=$!
	nx bob peer add "$(nx alice token)" --name alice >/dev/null &
	local_b=$!
	wait $local_a || fail "round $round: alice peer add failed"
	wait $local_b || fail "round $round: bob peer add failed"

	wait_for "$CONNECT_TIMEOUT" "round $round: alice to see bob connected" peer_connected alice bob
	wait_for "$CONNECT_TIMEOUT" "round $round: bob to see alice connected" peer_connected bob alice

	# The point of the scenario. A node whose own dial lost the dedup rule
	# must not be reporting that loss as its own condition.
	assert_mutually_clean alice bob

	if [ "$round" -lt "$ROUNDS" ]; then
		log "Round $round: unpeering both sides to set up a fresh race"
		nx alice peer rm "$BOB_KEY" >/dev/null
		nx bob peer rm "$ALICE_KEY" >/dev/null
		wait_for 60 "alice to drop bob" peer_gone alice
		wait_for 60 "bob to drop alice" peer_gone bob
	fi
done

log "Confirming the surviving connection is a single stable one, not a flap"
# Every adoption logs one "connected" line and every teardown one
# "disconnected" line, so a pairing that settled once per round leaves
# exactly that many. More would mean the two sides never agreed on a winner
# and kept tearing each other's connections down.
for n in alice bob; do
	connects="$(log_count "$n" ': connected (')"
	[ "$connects" -le "$((ROUNDS * 2))" ] ||
		fail "$n logged $connects connections over $ROUNDS rounds — the pairing is flapping, not settling"
	info "ok: $n logged $connects connection(s) over $ROUNDS round(s)"
done

harness_pass "$ROUNDS simultaneous peerings each converged on one connection, with no side reporting a dial failure it had already recovered from"
