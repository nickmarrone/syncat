#!/usr/bin/env bash
# Peering is mutual (SPEC.md §2.1): pasting one node's token into the other
# is only half of it. This pins what the *unpeered* half does in the
# meantime — it must reject the connection and say so, not quietly accept it
# and not wedge — and that completing the pairing afterwards recovers with no
# restart, no stale error, and no manual intervention.
#
# README calls this out as the state users land in most often: "adding a peer
# on only one end leaves that peer in a rejected/pending state on the other."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init peering-one-sided

node_add alice
node_add bob
node_start alice
node_start bob

BOB_KEY="$(peer_key_of bob)"
[ -n "$BOB_KEY" ] || fail "could not read bob's peer key"

log "Peering one direction only: bob is given alice's token, alice knows nothing of bob"
nx bob peer add "$(nx alice token)" --name alice >/dev/null

log "Alice must reject bob's connection and record it (SPEC.md §2.3)"
# Bob dials on the backoff schedule, so this needs room for a few attempts on
# top of cold DERP setup.
wait_for "$CONNECT_TIMEOUT" "alice to record bob in rejected_connections" rejected_has alice "$BOB_KEY"

REASON="$(nx alice status --json | jq -r --arg k "$BOB_KEY" '.rejected_connections[]? | select(.peer_key==$k) | .reason')"
info "alice's recorded reason: $REASON"
[ -n "$REASON" ] || fail "alice recorded bob's key but with no reason attached"

log "Neither node may wedge: bob keeps trying, alice keeps serving"
BOB_STATE="$(peer_state bob alice)"
case "$BOB_STATE" in
connecting | backing_off) info "ok: bob reports '$BOB_STATE', still trying" ;;
connected) fail "bob reports 'connected' to a node that has never been given his token" ;;
*) fail "bob reports an unexpected state '$BOB_STATE'" ;;
esac
# Alice must not be holding a half-open session for a peer she has rejected.
[ "$(nx alice status --json | jq -r '.peers | length')" = "0" ] ||
	fail "alice lists a peer she was never given a token for"
node_api_up alice || fail "alice's API stopped responding while rejecting connections"

log "Completing the pairing from alice's side; both must converge with no restart"
nx alice peer add "$(nx bob token)" --name bob >/dev/null
wait_for "$CONNECT_TIMEOUT" "alice to see bob connected" peer_connected alice bob
wait_for "$CONNECT_TIMEOUT" "bob to see alice connected" peer_connected bob alice

# The rejections happened, and are meant to stay visible as history — this
# just confirms recovery did not depend on clearing them.
assert_mutually_clean alice bob

harness_pass "a one-sided peering is rejected, recorded, and recovers cleanly once completed"
