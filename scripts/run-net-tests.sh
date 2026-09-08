#!/usr/bin/env bash
# scripts/run-net-tests.sh — run syncat's real-network scenario suite.
#
# Every scenario under scripts/net/ drives real syncat daemons over real
# tailcat, DERP-relayed unless direct connectivity is available. Nothing here
# simulates the network, which is the whole point: these cover the behaviour
# the Go suite structurally cannot reach, because it finishes in well under a
# minute of wall-clock time and never kills a process.
#
# Usage:
#   scripts/run-net-tests.sh                 # the fast tier (~10 min)
#   scripts/run-net-tests.sh --soak          # fast tier plus the slow ones
#   scripts/run-net-tests.sh --only 05       # anything matching "05"
#   scripts/run-net-tests.sh --list
#
# Scenarios numbered below 20 are the fast tier. From 20 up they are soak
# tests: they wait out real timeouts — SPEC.md §4's 90s dead-peer rule, long
# idle windows, large transfers — and take minutes each by design, so they
# run only with --soak.
#
# Env:
#   SYNCAT_DEBUG=1        run the daemons at debug level
#   SOAK_MINUTES=15       lengthen the idle-stability soak
#   SOAK_FILE_MB=512      enlarge the big-file transfer
#   KEEP_GOING=0          stop at the first failing scenario

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SOAK=0
ONLY=""
LIST=0
KEEP_GOING="${KEEP_GOING:-1}"

while [ $# -gt 0 ]; do
	case "$1" in
	--soak) SOAK=1 ;;
	--only)
		ONLY="$2"
		shift
		;;
	--list) LIST=1 ;;
	-h | --help)
		sed -n '2,30p' "$0"
		exit 0
		;;
	*)
		echo "unknown flag: $1" >&2
		exit 2
		;;
	esac
	shift
done

command -v jq >/dev/null 2>&1 || {
	echo "run-net-tests: jq is required" >&2
	exit 1
}

# Selection ------------------------------------------------------------------

SCENARIOS=()
for f in "$SCRIPT_DIR"/net/*.sh; do
	[ -f "$f" ] || continue
	base="$(basename "$f" .sh)"
	num="${base%%-*}"
	# Soak scenarios are numbered from 20 up.
	if [ "$SOAK" -eq 0 ] && [ "$num" -ge 20 ] 2>/dev/null; then
		continue
	fi
	if [ -n "$ONLY" ] && [[ "$base" != *"$ONLY"* ]]; then
		continue
	fi
	SCENARIOS+=("$f")
done

if [ "${#SCENARIOS[@]}" -eq 0 ]; then
	echo "run-net-tests: no scenarios selected" >&2
	exit 2
fi

if [ "$LIST" -eq 1 ]; then
	for f in "${SCENARIOS[@]}"; do basename "$f"; done
	exit 0
fi

# Build once, so a full suite compiles one binary rather than one per
# scenario. CGO_ENABLED=0 per SPEC.md §12 — the SQLite driver is pure Go and
# the shipped binary is cgo-free, so that is the build worth testing.
BIN="${TMPDIR:-/tmp}/syncat-test/bin/syncat"
mkdir -p "$(dirname "$BIN")"
echo "building syncat (CGO_ENABLED=0)..."
(cd "$REPO_ROOT" && CGO_ENABLED=0 go build -o "$BIN" ./cmd/syncat) || {
	echo "run-net-tests: build failed" >&2
	exit 1
}
export SYNCAT_BIN="$BIN"

# Run ------------------------------------------------------------------------

LOGDIR="${TMPDIR:-/tmp}/syncat-test/logs"
rm -rf "$LOGDIR"
mkdir -p "$LOGDIR"

PASSED=()
FAILED=()
SUITE_START=$(date +%s)

for f in "${SCENARIOS[@]}"; do
	name="$(basename "$f" .sh)"
	printf '\n════ %s ════\n' "$name"
	start=$(date +%s)
	if bash "$f" 2>&1 | tee "$LOGDIR/$name.log"; then
		ok=1
	else
		ok=0
	fi
	# The scenario's own exit status, not tee's.
	[ "${PIPESTATUS[0]}" -eq 0 ] || ok=0
	took=$(($(date +%s) - start))
	if [ "$ok" -eq 1 ]; then
		PASSED+=("$name (${took}s)")
	else
		FAILED+=("$name (${took}s)")
		echo "── $name FAILED; full log: $LOGDIR/$name.log"
		[ "$KEEP_GOING" = "1" ] || break
	fi
done

# Summary --------------------------------------------------------------------

printf '\n════════ summary (%ds total) ════════\n' "$(($(date +%s) - SUITE_START))"
for s in "${PASSED[@]:-}"; do [ -n "$s" ] && printf '  PASS  %s\n' "$s"; done
for s in "${FAILED[@]:-}"; do [ -n "$s" ] && printf '  FAIL  %s\n' "$s"; done
printf '\n%d passed, %d failed\n' "${#PASSED[@]}" "${#FAILED[@]}"
[ "$SOAK" -eq 1 ] || printf 'soak scenarios skipped; run with --soak to include them\n'

[ "${#FAILED[@]}" -eq 0 ]
