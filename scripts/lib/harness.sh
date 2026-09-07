# scripts/lib/harness.sh — shared plumbing for syncat's real-network test
# scripts. Sourced, never executed.
#
# Every scenario under scripts/net/ (and scripts/e2e.sh) drives real syncat
# daemons over real tailcat — DERP-relayed unless direct connectivity is
# available — and asserts on what actually lands on disk and in
# `syncat status --json`. Nothing here fakes the network. The point of these
# scripts is precisely the behaviour the Go suite structurally cannot see:
# real time passing, real process death, and a real relay in the middle.
#
# Usage:
#
#	SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#	. "$SCRIPT_DIR/../lib/harness.sh"
#	harness_init "my-scenario"
#	node_add alice; node_add bob
#	node_start alice; node_start bob
#	...
#	harness_pass "what this proved"
#
# harness_init installs an EXIT trap that tears down every node it knows
# about, on every exit path, success or failure.

set -euo pipefail

# --- constants from the implementation -------------------------------------
#
# These mirror values the daemon actually uses. A scenario that waits on one
# of them should reference the constant, so the reason for a timeout is
# legible and so that changing the daemon's schedule shows up here as a
# failing test rather than as a mystery.

# SPEC.md §4: 90s without received traffic means the connection is dead. The
# keepalive polls every 5s (protocol.DefaultKeepaliveCheckInterval), so
# detection lands in [90s, 95s].
readonly DEAD_PEER_SECONDS=90
# SPEC.md §2.2's dial schedule: 1s initial, doubling, 5min cap, jittered.
readonly BACKOFF_MAX_SECONDS=300
# config.Default()'s rescan_interval_seconds. SPEC.md §5 calls the periodic
# scan "the source of truth", so it is the backstop behind every watcher
# event, and any convergence wait meant to tolerate a missed fsnotify event
# has to be longer than this.
readonly RESCAN_INTERVAL_SECONDS=300

# How long to wait for a tunnel to come up. Cold DERP setup routinely takes
# tens of seconds: tailcat's own dial has a hard 10s meow-ping timeout, and a
# first attempt that lands before the peer has registered with its home relay
# fails and waits out a backoff step before retrying.
readonly CONNECT_TIMEOUT=${SYNCAT_CONNECT_TIMEOUT:-180}
# How long to wait for a file to make it across an established connection.
readonly SYNC_TIMEOUT=${SYNCAT_SYNC_TIMEOUT:-90}
# How long to wait for a daemon's REST API to start listening.
readonly API_TIMEOUT=30

# --- state -----------------------------------------------------------------

HARNESS_NAME=""
WORK=""
BIN=""
NODES=()           # registered node names, in creation order
FROZEN=()          # names currently SIGSTOPped, so cleanup can thaw them
STEP=0
HARNESS_PASS_MSG=""

# --- output ----------------------------------------------------------------

# Progress goes to stderr, results to stdout. That split is not cosmetic:
# helpers like share_and_subscribe echo a value *and* report progress, and
# with both on stdout the caller's $(...) would capture the log lines along
# with the share id.
log() {
	STEP=$((STEP + 1))
	printf '\n[%02d] %s\n' "$STEP" "$*" >&2
}
info() { printf '      %s\n' "$*" >&2; }

# fail aborts the scenario, dumping everything needed to diagnose it without
# a second run: each daemon's log tail, each node's view of the world, and
# the contents of every directory the scenario registered.
#
# The status and directory dumps are not decoration. The one real-DERP
# failure this suite was written after — an offline conflict that never
# converged — printed only two daemon logs that said nothing was wrong,
# because the interesting state was entirely in what the two nodes believed
# and what was on their disks.
fail() {
	echo
	echo "FAIL[$HARNESS_NAME]: $*" >&2
	local name
	for name in "${NODES[@]:-}"; do
		[ -n "$name" ] || continue
		if [ -f "$WORK/$name/daemon.log" ]; then
			echo "--- $name daemon log (tail) ---" >&2
			tail -n 60 "$WORK/$name/daemon.log" >&2 || true
		fi
		echo "--- $name status ---" >&2
		nx "$name" status --json 2>&1 | jq '{peers, subscriptions, shares: [.shares[]?|{id,name,permission}], rejected_connections}' >&2 2>/dev/null ||
			echo "(status unavailable — daemon not running?)" >&2
	done
	for name in "${NODES[@]:-}"; do
		[ -n "$name" ] || continue
		local d
		for d in "$WORK/$name"/dir-*; do
			[ -d "$d" ] || continue
			echo "--- $name: $(basename "$d") ---" >&2
			ls -la "$d" >&2 || true
		done
	done
	exit 1
}

# wait_for TIMEOUT_SECS DESCRIPTION PREDICATE [ARGS...]
#
# Polls PREDICATE once a second until it exits 0, or fails loudly once
# TIMEOUT_SECS elapses. Never a bare sleep: every asynchronous condition in
# these scripts is awaited through this loop, so a scenario that passes tells
# you how long each step actually took, and one that hangs tells you exactly
# which condition it hung on.
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
	elapsed=$(($(date +%s) - start))
	info "ok: $desc (${elapsed}s)"
}

# wait_while TIMEOUT_SECS DESCRIPTION PREDICATE [ARGS...]
#
# The inverse: polls until PREDICATE stops succeeding. Used by the scenarios
# that assert something goes away — a peer leaving "connected", a session
# being torn down.
wait_while() {
	local timeout="$1" desc="$2"
	shift 2
	local start elapsed
	start=$(date +%s)
	while "$@" >/dev/null 2>&1; do
		elapsed=$(($(date +%s) - start))
		if [ "$elapsed" -ge "$timeout" ]; then
			fail "timed out after ${timeout}s waiting for: $desc"
		fi
		sleep 1
	done
	elapsed=$(($(date +%s) - start))
	info "ok: $desc (${elapsed}s)"
}

# wait_fast TIMEOUT_SECS DESCRIPTION PREDICATE [ARGS...]
#
# wait_for with a busy poll instead of a 1s tick, for conditions that exist
# only briefly. Catching a transfer *while it is running* is the case that
# needs it: on a direct path a few hundred megabytes can cross in a couple of
# seconds, and a once-a-second poll will happily miss the entire window and
# then report a timeout waiting for something that already finished.
wait_fast() {
	local timeout="$1" desc="$2"
	shift 2
	local start elapsed
	start=$(date +%s)
	until "$@" >/dev/null 2>&1; do
		elapsed=$(($(date +%s) - start))
		if [ "$elapsed" -ge "$timeout" ]; then
			fail "timed out after ${timeout}s waiting for: $desc"
		fi
		sleep 0.02
	done
	info "ok: $desc ($(($(date +%s) - start))s)"
}

# hold_for SECONDS DESCRIPTION PREDICATE [ARGS...]
#
# The opposite of wait_for, and the shape most of the soak scenarios need:
# assert PREDICATE stays true for the whole window rather than becoming true
# once. A connection that is up now and a connection that survives fifteen
# minutes are different claims, and only the second one is interesting.
hold_for() {
	local secs="$1" desc="$2"
	shift 2
	local start elapsed
	start=$(date +%s)
	while :; do
		"$@" >/dev/null 2>&1 || fail "condition stopped holding after $(($(date +%s) - start))s: $desc"
		elapsed=$(($(date +%s) - start))
		[ "$elapsed" -ge "$secs" ] && break
		sleep 5
	done
	info "ok: held for ${secs}s: $desc"
}

# --- lifecycle -------------------------------------------------------------

harness_init() {
	HARNESS_NAME="${1:-scenario}"
	local script_dir repo_root
	script_dir="$(cd "$(dirname "${BASH_SOURCE[1]}")" && pwd)"
	repo_root="$(cd "$script_dir/.." && pwd)"
	[ -f "$repo_root/go.mod" ] || repo_root="$(cd "$script_dir/../.." && pwd)"

	command -v jq >/dev/null 2>&1 || {
		echo "FAIL[$HARNESS_NAME]: jq is required" >&2
		exit 1
	}

	WORK="${TMPDIR:-/tmp}/syncat-test/$HARNESS_NAME"

	# The work dir is a fixed path per scenario — reused and wiped on every
	# run, so there is always exactly one place to look afterwards. That only
	# works if one instance runs at a time: two concurrent runs of the same
	# scenario share config dirs and stomp each other, and the result is
	# baffling rather than obviously wrong (two shares with the same name, a
	# lookup by name that returns both, and a wait that can never succeed).
	# The runner is sequential, so this only catches a hand-started scenario
	# racing one already in flight — which is exactly when it is confusing.
	if pgrep -f "$WORK" >/dev/null 2>&1; then
		echo "FAIL[$HARNESS_NAME]: another run of this scenario is still using $WORK" >&2
		echo "  processes: $(pgrep -f "$WORK" | tr '\n' ' ')" >&2
		exit 1
	fi

	rm -rf "$WORK"
	mkdir -p "$WORK"

	# Reuse a binary the runner already built, so a full suite compiles once
	# rather than once per scenario.
	if [ -n "${SYNCAT_BIN:-}" ] && [ -x "${SYNCAT_BIN}" ]; then
		BIN="$SYNCAT_BIN"
	else
		BIN="${TMPDIR:-/tmp}/syncat-test/bin/syncat"
		mkdir -p "$(dirname "$BIN")"
		# CGO_ENABLED=0 per SPEC.md §12: the SQLite driver is pure Go and the
		# shipped binary is cgo-free, so the tests must exercise that build.
		(cd "$repo_root" && CGO_ENABLED=0 go build -o "$BIN" ./cmd/syncat) ||
			fail "building syncat"
	fi

	trap harness_cleanup EXIT
	log "Scenario '$HARNESS_NAME' — work dir $WORK"
}

# harness_cleanup runs on every exit path. Thawing before killing is
# load-bearing: a scenario that fails between node_freeze and node_thaw would
# otherwise leave a SIGSTOPped daemon behind, which ignores SIGTERM and
# survives the run.
harness_cleanup() {
	local ec=$?
	local name
	for name in "${FROZEN[@]:-}"; do
		[ -n "$name" ] || continue
		node_thaw "$name" 2>/dev/null || true
	done
	for name in "${NODES[@]:-}"; do
		[ -n "$name" ] || continue
		node_stop "$name" 2>/dev/null || true
	done
	if [ $ec -eq 0 ] && [ -n "$HARNESS_PASS_MSG" ]; then
		echo
		echo "PASS[$HARNESS_NAME]: $HARNESS_PASS_MSG"
	fi
	exit $ec
}

harness_pass() {
	HARNESS_PASS_MSG="$*"
}

# --- nodes -----------------------------------------------------------------

# free_port finds a TCP port on loopback nothing is listening on, so
# scenarios can run concurrently and never collide with a developer's real
# daemon on the default 127.0.0.1:8347.
free_port() {
	local port
	for _ in $(seq 1 200); do
		port=$((20000 + RANDOM % 20000))
		if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
			echo "$port"
			return 0
		fi
		exec 3>&- 2>/dev/null || true
	done
	fail "could not find a free loopback port"
}

# node_add NAME registers a node: its own config dir, data dir, log, and API
# port, and runs `syncat init`. It does not start the daemon.
node_add() {
	local name="$1"
	mkdir -p "$WORK/$name/config" "$WORK/$name/data"
	local port
	port="$(free_port)"
	eval "NODE_${name}_PORT=$port"
	eval "NODE_${name}_PID=''"
	NODES+=("$name")

	"$BIN" --config "$WORK/$name/config" --data "$WORK/$name/data" init --name "$name" >/dev/null ||
		fail "init node $name"

	# The CLI reads api_addr back out of config.json, so it has to be patched
	# there rather than passed to `daemon --api`, which would only bind the
	# listener and leave every client call pointed at the default port.
	local cfg="$WORK/$name/config/config.json" tmp
	tmp="$(mktemp)"
	local jq_prog='.api_addr = $a'
	[ "${SYNCAT_DEBUG:-0}" = "1" ] && jq_prog="$jq_prog | .debug = true"
	jq --arg a "127.0.0.1:$port" "$jq_prog" "$cfg" >"$tmp" && mv "$tmp" "$cfg"
	info "node $name: api 127.0.0.1:$port"
}

# node_dir NAME LABEL creates and echoes a directory belonging to NAME. The
# dir-* naming is what lets fail() dump every share and sync directory
# without the scenario having to register them.
node_dir() {
	local name="$1" label="$2" d="$WORK/$1/dir-$2"
	mkdir -p "$d"
	echo "$d"
}

node_pid() { eval "echo \"\${NODE_$1_PID:-}\""; }

# node_start launches the daemon and waits for its API. `exec` inside the
# backgrounded subshell replaces the subshell's image with syncat itself, so
# $! is the daemon's real PID — without it, kill would leave the actual
# daemon running as an orphan and the cleanup trap's guarantee would be void.
node_start() {
	local name="$1"
	[ -z "$(node_pid "$name")" ] || fail "node $name is already running"
	# A node's log accumulates across restarts, which is what makes it useful
	# — and also what makes it misleading without this line. Reading a
	# restart scenario's log, the previous process's final moments sit
	# directly above the new one's first, and it is very easy to attribute
	# both to the same daemon and conclude something impossible is happening.
	printf '\n===== %s: daemon starting (%s) =====\n' "$name" "$(date -u +%H:%M:%S)" >>"$WORK/$name/daemon.log"
	(exec "$BIN" --config "$WORK/$name/config" --data "$WORK/$name/data" daemon) >>"$WORK/$name/daemon.log" 2>&1 &
	eval "NODE_${name}_PID=$!"
	wait_for "$API_TIMEOUT" "$name's API to accept requests" node_api_up "$name"
}

node_stop() {
	local name="$1" pid
	pid="$(node_pid "$name")"
	[ -n "$pid" ] || return 0
	kill "$pid" >/dev/null 2>&1 || true
	wait "$pid" 2>/dev/null || true
	eval "NODE_${name}_PID=''"
}

# node_kill is SIGKILL: no graceful shutdown, no clean protocol close, no
# final FIN. The peer is left holding a connection to a process that no
# longer exists, which is a materially different recovery path from
# node_stop's SIGTERM.
node_kill() {
	local name="$1" pid
	pid="$(node_pid "$name")"
	[ -n "$pid" ] || return 0
	kill -KILL "$pid" >/dev/null 2>&1 || true
	wait "$pid" 2>/dev/null || true
	eval "NODE_${name}_PID=''"
}

node_restart() {
	node_stop "$1"
	node_start "$1"
}

# node_freeze SIGSTOPs the daemon: its sockets stay open and the kernel keeps
# ACKing at the TCP level, but the process answers nothing. That is exactly
# the black-hole condition SPEC.md §4's dead-peer rule exists for — a peer
# that has stopped responding without hanging up — and it reproduces a pulled
# cable or a suspended laptop without root, a firewall, or a network
# namespace.
node_freeze() {
	local name="$1" pid
	pid="$(node_pid "$name")"
	[ -n "$pid" ] || fail "node $name is not running, cannot freeze it"
	kill -STOP "$pid" || fail "SIGSTOP $name"
	FROZEN+=("$name")
	info "froze $name (pid $pid) — its peer should now see silence"
}

node_thaw() {
	local name="$1" pid
	pid="$(node_pid "$name")"
	[ -n "$pid" ] || return 0
	kill -CONT "$pid" 2>/dev/null || true
	local remaining=() n
	for n in "${FROZEN[@]:-}"; do
		[ -n "$n" ] && [ "$n" != "$name" ] && remaining+=("$n")
	done
	FROZEN=("${remaining[@]:-}")
}

# nx NAME ARGS... runs the CLI against that node.
nx() {
	local name="$1"
	shift
	"$BIN" --config "$WORK/$name/config" --data "$WORK/$name/data" "$@"
}

node_log() { echo "$WORK/$1/daemon.log"; }

# --- predicates (for wait_for / wait_while / hold_for) ----------------------

node_api_up() { nx "$1" status >/dev/null 2>&1; }

# peer_state NAME PEER echoes NAME's view of PEER's ConnState, or nothing.
peer_state() {
	nx "$1" status --json 2>/dev/null | jq -r --arg p "$2" '.peers[]? | select(.name==$p) | .state // empty'
}

peer_is() { [ "$(peer_state "$1" "$2")" = "$3" ]; }
peer_connected() { peer_is "$1" "$2" connected; }

# peers_connected NAME... asserts every listed node reports *all* its peers
# connected. The bidirectional form matters: half of the connection bugs this
# suite covers are visible on exactly one of the two sides.
mutually_connected() {
	local a="$1" b="$2"
	peer_connected "$a" "$b" && peer_connected "$b" "$a"
}

peer_last_error() {
	nx "$1" status --json 2>/dev/null | jq -r --arg p "$2" '.peers[]? | select(.name==$p) | .last_error // empty'
}

peer_connected_since() {
	nx "$1" status --json 2>/dev/null | jq -r --arg p "$2" '.peers[]? | select(.name==$p) | .connected_since // empty'
}

peer_key_of() { nx "$1" status --json 2>/dev/null | jq -r '.peer_key'; }

# share_id_of echoes the single share named $2, and fails loudly if the name
# is ambiguous. Returning two ids on two lines is worse than returning none:
# every predicate downstream then compares against a two-line string, matches
# nothing, and times out pointing at the wrong thing entirely.
share_id_of() {
	local ids
	ids="$(nx "$1" share ls --json 2>/dev/null | jq -r --arg n "$2" '.[]? | select(.name==$n) | .id')"
	if [ "$(echo "$ids" | grep -c .)" -gt 1 ]; then
		fail "$1 has more than one share named '$2': $(echo "$ids" | tr '\n' ' ')"
	fi
	echo "$ids"
}

# peer_id_of NAME PEER is the peer's full hex identity key as NAME knows it,
# which is what `subscription add` wants.
peer_id_of() {
	nx "$1" peer ls --json 2>/dev/null | jq -r --arg p "$2" '.[]? | select(.name==$p) | .id'
}

remote_share_seen() {
	[ -n "$(nx "$1" remote ls --json 2>/dev/null | jq -r --arg id "$2" '.[]? | select(.share_id==$id) | .share_id')" ]
}

remote_share_perm() {
	nx "$1" remote ls --json 2>/dev/null | jq -r --arg id "$2" '.[]? | select(.share_id==$id) | .permission // empty'
}

subscription_count() { nx "$1" subscription ls --json 2>/dev/null | jq -r 'length'; }

rejected_has() {
	[ -n "$(nx "$1" status --json 2>/dev/null | jq -r --arg k "$2" '.rejected_connections[]? | select(.peer_key==$k) | .peer_key')" ]
}

trash_has() {
	[ -n "$(nx "$1" trash ls --json "$2" 2>/dev/null | jq -r --arg r "$3" '.[]? | select(.rel_path==$r) | .rel_path')" ]
}

file_content_is() {
	[ -f "$1" ] && [ "$(cat "$1" 2>/dev/null)" = "$2" ]
}

path_absent() { [ ! -e "$1" ]; }
path_present() { [ -e "$1" ]; }

conflict_copy_present() { compgen -G "$1/*.sync-conflict-*" >/dev/null 2>&1; }

files_identical() {
	[ -f "$1" ] && [ -f "$2" ] && [ "$(cat "$1")" = "$(cat "$2")" ]
}

# conflict_copy_names_match checks both sides settled on the *same* conflict
# filename. SPEC.md §5 names the copy from the losing version's identity, so
# two nodes that resolve the same conflict independently must land on the
# same name; two different names would mean each side kept its own idea of
# who lost, and the directories would never converge.
conflict_copy_names_match() {
	local a b
	a="$(basename "$(ls "$1"/*.sync-conflict-* 2>/dev/null | head -n1)" 2>/dev/null || true)"
	b="$(basename "$(ls "$2"/*.sync-conflict-* 2>/dev/null | head -n1)" 2>/dev/null || true)"
	[ -n "$a" ] && [ "$a" = "$b" ]
}

sha_of() { sha256sum "$1" 2>/dev/null | cut -d' ' -f1; }

# --- assertions ------------------------------------------------------------

# assert_clean_peer is run at the end of every scenario. A peer reported as
# connected must carry no error: a stale last_error on a live connection is a
# bug in its own right, and it is exactly the shape of the one that had a
# healthy, actively-syncing node reporting itself as backing off for the
# whole life of the connection.
assert_clean_peer() {
	local a="$1" b="$2" state err
	state="$(peer_state "$a" "$b")"
	[ "$state" = "connected" ] || fail "$a reports peer $b as '$state', want 'connected'"
	err="$(peer_last_error "$a" "$b")"
	[ -z "$err" ] || fail "$a reports peer $b as connected but carrying last_error: $err"
	info "ok: $a sees $b connected, with no error attached"
}

assert_mutually_clean() {
	assert_clean_peer "$1" "$2"
	assert_clean_peer "$2" "$1"
}

# assert_connected_since_cleared pins PeerStatus.ConnectedSince's documented
# contract — zero unless the peer is connected — so a UI rendering "connected
# for X" from it cannot show a climbing duration for a peer that is gone.
assert_connected_since_cleared() {
	local a="$1" b="$2" since state
	state="$(peer_state "$a" "$b")"
	[ "$state" != "connected" ] || fail "$a still reports $b connected; nothing to assert about connected_since"
	since="$(peer_connected_since "$a" "$b")"
	case "$since" in
	"" | "0001-01-01T00:00:00Z") info "ok: $a reports a zero connected_since for the disconnected $b" ;;
	*) fail "$a reports $b as '$state' but still carries connected_since=$since" ;;
	esac
}

# assert_log_lacks is how the soak scenarios assert an absence. Reconnect
# churn and ping timeouts are both invisible in a status snapshot taken after
# the fact — the only record that they happened at all is the daemon log.
assert_log_lacks() {
	local name="$1" pattern="$2" what="${3:-$2}" hits
	hits="$(grep -c -- "$pattern" "$(node_log "$name")" 2>/dev/null || true)"
	[ "${hits:-0}" -eq 0 ] || fail "$name's log has $hits occurrence(s) of $what, want none:
$(grep -- "$pattern" "$(node_log "$name")" | head -n 5)"
	info "ok: nothing in $name's log matching $what"
}

assert_log_has() {
	local name="$1" pattern="$2" what="${3:-$2}"
	grep -q -- "$pattern" "$(node_log "$name")" 2>/dev/null ||
		fail "$name's log has no $what, expected one"
	info "ok: $name's log reports $what"
}

log_count() { grep -c -- "$2" "$(node_log "$1")" 2>/dev/null || echo 0; }

assert_eq() {
	[ "$1" = "$2" ] || fail "${3:-value} = '$1', want '$2'"
	info "ok: ${3:-value} is '$2'"
}

# --- composite setup -------------------------------------------------------

# pair_nodes A B exchanges tokens in both directions and waits for the tunnel.
# Peering is mutual (SPEC.md §2.1): a node that has only been given the other
# half's token rejects the inbound connection and records it, which is
# scenario 01's whole subject.
pair_nodes() {
	local a="$1" b="$2"
	nx "$a" peer add "$(nx "$b" token)" --name "$b" >/dev/null
	nx "$b" peer add "$(nx "$a" token)" --name "$a" >/dev/null
	wait_for "$CONNECT_TIMEOUT" "$a to see $b connected" peer_connected "$a" "$b"
	wait_for "$CONNECT_TIMEOUT" "$b to see $a connected" peer_connected "$b" "$a"
}

# share_and_subscribe OFFERER SHAREDIR SHARENAME PERM SUBSCRIBER LOCALDIR MODE
# runs the whole offer/discover/subscribe dance and echoes the share id.
share_and_subscribe() {
	local off="$1" dir="$2" sname="$3" perm="$4" sub="$5" local_dir="$6" mode="$7"
	nx "$off" share add "$dir" --name "$sname" --perm "$perm" >/dev/null
	local sid
	sid="$(share_id_of "$off" "$sname")"
	[ -n "$sid" ] || fail "could not determine $off's share id for '$sname'"
	wait_for 60 "$sub to see $off's '$sname' share" remote_share_seen "$sub" "$sid"
	local pkey
	pkey="$(peer_id_of "$sub" "$off")"
	[ -n "$pkey" ] || fail "could not determine $off's peer key from $sub"
	nx "$sub" subscription add "$pkey" "$sid" "$local_dir" --mode "$mode" >/dev/null
	echo "$sid"
}
