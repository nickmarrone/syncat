#!/usr/bin/env bash
# The local REST API's security surface, against the real daemon
# (MANUAL-TESTS.md §12, SPEC.md §8).
#
#   - Every /api/* route refuses a request with no token or the wrong one,
#     and a refused write changes nothing.
#   - /ui-token, the one unauthenticated route, hands the token only to a
#     request whose Host and Origin name this server: the DNS-rebinding and
#     cross-site defence for the web UI.
#   - The listener is loopback-only: unreachable on the machine's LAN
#     address, and a non-loopback api_addr is refused outright.
#   - api.token and the key files are readable by their owner only.
#   - No token or key appears in a daemon log at the default level, after
#     the tokens have actually been used to pair two nodes.
#
# MANUAL-TESTS.md §12's remaining item, an unknown peer being rejected and
# recorded, is scenario 01's subject.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/../lib/harness.sh"

command -v curl >/dev/null 2>&1 || fail "curl is required"

harness_init api-security

node_add alice
node_add bob
node_start alice
node_start bob
pair_nodes alice bob

ADDR="$(jq -r .api_addr "$WORK/alice/config/config.json")"
TOKEN="$(cat "$WORK/alice/config/api.token")"
[ -n "$TOKEN" ] || fail "alice has no api.token"
BASE="http://$ADDR"

# http_code [CURL ARGS...] echoes the HTTP status, or 000 if nothing answered.
http_code() { curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$@" || true; }

log "Every API route refuses a missing or wrong token"
for path in /api/status /api/peers /api/shares /api/remote-shares /api/subscriptions /api/approvals /api/holds; do
	assert_eq "$(http_code "$BASE$path")" 401 "GET $path with no token"
	assert_eq "$(http_code -H "X-Syncat-Token: wrong-$TOKEN" "$BASE$path")" 401 "GET $path with a wrong token"
done
assert_eq "$(http_code -H "X-Syncat-Token: $TOKEN" "$BASE/api/status")" 200 "GET /api/status with the right token"

log "A refused write changes nothing"
SHARES_BEFORE="$(nx alice share ls --json | jq length)"
assert_eq "$(http_code -X POST -H 'Content-Type: application/json' \
	-d "{\"path\":\"$WORK\",\"name\":\"stolen\",\"permission\":\"read-write\"}" "$BASE/api/shares")" 401 "POST /api/shares with no token"
assert_eq "$(http_code -X DELETE "$BASE/api/peers/bob")" 401 "DELETE /api/peers/bob with no token"
assert_eq "$(nx alice share ls --json | jq length)" "$SHARES_BEFORE" "alice's share count after the refused write"
peer_connected alice bob || fail "a refused DELETE removed alice's peer"
info "ok: alice still has her peer"

log "/ui-token hands the token only to this server's own Host and Origin"
GOT="$(curl -s --max-time 5 "$BASE/ui-token" | jq -r .token)"
# Compared, never printed: test output ends up in public CI logs.
[ "$GOT" = "$TOKEN" ] || fail "/ui-token did not return alice's API token to a same-host request"
info "ok: /ui-token returns the API token to a same-host request"
assert_eq "$(http_code -H "Origin: $BASE" "$BASE/ui-token")" 200 "/ui-token with this server's own Origin"
assert_eq "$(http_code -H "Host: evil.example:${ADDR##*:}" "$BASE/ui-token")" 403 "/ui-token with a foreign Host (DNS rebinding)"
assert_eq "$(http_code -H "Origin: http://evil.example" "$BASE/ui-token")" 403 "/ui-token with a foreign Origin"
assert_eq "$(http_code -H "Origin: null" "$BASE/ui-token")" 403 "/ui-token with an opaque (null) Origin"
BODY="$(curl -s --max-time 5 -H "Origin: http://evil.example" "$BASE/ui-token")"
case "$BODY" in *"$TOKEN"*) fail "a refused /ui-token response still carried the token" ;; esac
info "ok: no refused response carried the token"

log "The listener is loopback-only"
LAN_IP="$(ip -4 -o addr show scope global 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -n1)"
if [ -n "$LAN_IP" ]; then
	assert_eq "$(http_code -H "X-Syncat-Token: $TOKEN" "http://$LAN_IP:${ADDR##*:}/api/status")" 000 \
		"a request to alice's port on the LAN address $LAN_IP"
else
	info "skipped: this machine has no non-loopback IPv4 address to try"
fi

log "A non-loopback API address is refused, not silently bound"
PORT="$(free_port)"
if "$BIN" --config "$WORK/bob/config" --data "$WORK/bob/data" daemon --api "0.0.0.0:$PORT" >"$WORK/bob-lan.log" 2>&1 &
then
	LAN_PID=$!
	# A refusal exits at once; a daemon that bound would still be running.
	for _ in $(seq 1 10); do kill -0 "$LAN_PID" 2>/dev/null || break; sleep 1; done
	if kill -0 "$LAN_PID" 2>/dev/null; then
		kill "$LAN_PID" 2>/dev/null
		wait "$LAN_PID" 2>/dev/null
		fail "daemon --api 0.0.0.0:$PORT kept running instead of refusing a non-loopback address"
	fi
	wait "$LAN_PID" 2>/dev/null && fail "daemon --api 0.0.0.0:$PORT exited 0"
	grep -q 'non-loopback' "$WORK/bob-lan.log" ||
		fail "daemon --api 0.0.0.0 failed without saying why: $(cat "$WORK/bob-lan.log")"
	info "ok: daemon --api 0.0.0.0:$PORT refused to start: $(grep -m1 'non-loopback' "$WORK/bob-lan.log")"
fi

log "api.token and the key files are owner-only"
for f in "$WORK/alice/config/api.token" "$WORK/alice/data/keys/identity.key" "$WORK/alice/data/keys/tailcat.key"; do
	[ -f "$f" ] || fail "expected $f to exist"
	mode="$(stat -c '%a' "$f")"
	[ $((8#$mode & 8#077)) -eq 0 ] || fail "$f is mode $mode; group or others can access it"
	info "ok: $(basename "$f") is mode $mode"
done

log "No token or key reached a daemon log"
SECRETS=(
	"alice api token:$TOKEN"
	"bob api token:$(cat "$WORK/bob/config/api.token")"
	"alice connection token:$(nx alice token)"
	"bob connection token:$(nx bob token)"
)
for name in alice bob; do
	for kf in identity.key tailcat.key; do
		# Key files may be binary; only look for a text encoding of them.
		if LC_ALL=C grep -qv '[^[:print:][:space:]]' "$WORK/$name/data/keys/$kf" 2>/dev/null; then
			SECRETS+=("$name $kf:$(tr -d '\n' <"$WORK/$name/data/keys/$kf")")
		fi
		SECRETS+=("$name $kf (hex):$(od -An -tx1 -v "$WORK/$name/data/keys/$kf" | tr -d ' \n' | head -c 64)")
	done
done
for entry in "${SECRETS[@]}"; do
	what="${entry%%:*}"
	secret="${entry#*:}"
	[ "${#secret}" -ge 16 ] || continue
	for name in alice bob; do
		grep -qF -- "$secret" "$(node_log "$name")" && fail "$name's daemon log contains $what"
	done
	info "ok: neither daemon log contains $what"
done

assert_mutually_clean alice bob

harness_pass "the API refused every unauthenticated request, /ui-token refused foreign Host and Origin, the listener stayed on loopback, secrets were owner-only, and no log leaked one"
