#!/usr/bin/env bash
# Approval-required shares and unknown peers, decided from the CLI.
#
# SPEC.md §6: a share marked approval-required queues each subscribe request
# instead of granting it; nothing syncs until the offerer grants, and a deny
# leaves the subscriber with no access. SPEC.md §2: a node that dials us with
# a valid token we never added becomes a pending peer, and approving it adds
# it with no token paste on our side.
#
# The "nothing synced yet" checks lean on a third, open share that bob is
# also subscribed to: its tracer arriving proves the connection is live.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

harness_init approvals

node_add alice
node_add bob
node_add carol
GATED="$(node_dir alice gated)"
SECRET="$(node_dir alice secret)"
OPEN="$(node_dir alice open)"
BOB_GATED="$(node_dir bob gated)"
BOB_SECRET="$(node_dir bob secret)"
BOB_OPEN="$(node_dir bob open)"
node_start alice
node_start bob
node_start carol
pair_nodes alice bob

share_and_subscribe alice "$OPEN" open rw bob "$BOB_OPEN" mirror >/dev/null
echo "gated content" >"$GATED/g.txt"
echo "secret content" >"$SECRET/s.txt"

BOB_KEY="$(peer_key_of bob)"
ALICE_KEY_FROM_BOB="$(peer_id_of bob alice)"

request_access() {
	local name="$1" dir="$2" local_dir="$3" sid
	nx alice share add "$dir" --name "$name" --perm rw --approval >/dev/null || fail "share add --approval $name"
	sid="$(share_id_of alice "$name")"
	wait_for 60 "bob to see alice's '$name' share" remote_share_seen bob "$sid"
	nx bob subscription add "$ALICE_KEY_FROM_BOB" "$sid" "$local_dir" --mode mirror >/dev/null ||
		fail "bob: subscription add $name"
	echo "$sid"
}

log "Bob asks for two approval-required shares; both queue on alice"
GATED_ID="$(request_access gated "$GATED" "$BOB_GATED")"
SECRET_ID="$(request_access secret "$SECRET" "$BOB_SECRET")"
wait_for "$SYNC_TIMEOUT" "bob's request for 'gated' to be pending on alice" approval_pending alice "^share-$GATED_ID-$BOB_KEY\$"
wait_for "$SYNC_TIMEOUT" "bob's request for 'secret' to be pending on alice" approval_pending alice "^share-$SECRET_ID-$BOB_KEY\$"
wait_for "$SYNC_TIMEOUT" "bob to see his access to 'gated' as pending" remote_share_access_is bob "$GATED_ID" pending
echo "t1" >"$OPEN/tracer.txt"
wait_for "$SYNC_TIMEOUT" "the open share to keep syncing" file_content_is "$BOB_OPEN/tracer.txt" "t1"
path_absent "$BOB_GATED/g.txt" || fail "an approval-required share synced before anyone approved it"
path_absent "$BOB_SECRET/s.txt" || fail "an approval-required share synced before anyone approved it"
info "ok: nothing from either gated share reached bob while pending"

log "Alice grants one and denies the other"
nx alice approvals grant "$(approval_id alice "^share-$GATED_ID-")" >/dev/null || fail "approvals grant"
nx alice approvals deny "$(approval_id alice "^share-$SECRET_ID-")" >/dev/null || fail "approvals deny"
wait_for "$SYNC_TIMEOUT" "the granted share's file to reach bob, with no restart" file_content_is "$BOB_GATED/g.txt" "gated content"
wait_for "$SYNC_TIMEOUT" "bob to see his access to 'secret' as denied" remote_share_access_is bob "$SECRET_ID" denied
no_approval_pending alice "^share-" || fail "a decided share request is still pending on alice"
echo "t2" >"$OPEN/tracer.txt"
wait_for "$SYNC_TIMEOUT" "the open share to keep syncing" file_content_is "$BOB_OPEN/tracer.txt" "t2"
path_absent "$BOB_SECRET/s.txt" || fail "a denied share synced to bob"
info "ok: the granted share synced, the denied one did not"

log "Carol dials alice with a valid token alice never added"
CAROL_KEY="$(peer_key_of carol)"
nx carol peer add "$(nx alice token)" --name alice >/dev/null || fail "carol: peer add alice"
wait_for "$CONNECT_TIMEOUT" "carol to be pending on alice" approval_pending alice "^peer-$CAROL_KEY\$"
peer_connected carol alice && fail "carol connected to alice before alice approved her"
info "ok: carol is pending, not connected"

log "Alice approves carol with no token paste, and they connect"
nx alice peer approve "peer-$CAROL_KEY" >/dev/null || fail "alice: peer approve carol"
wait_for "$CONNECT_TIMEOUT" "alice and carol to connect" mutually_connected alice carol
no_approval_pending alice "^peer-" || fail "carol is still pending after being approved"

assert_mutually_clean alice bob
assert_mutually_clean alice carol

harness_pass "approval-required shares synced only once granted and never once denied, and an unknown peer joined only once approved"
