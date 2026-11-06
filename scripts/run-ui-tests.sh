#!/usr/bin/env bash
# scripts/run-ui-tests.sh, run syncat's headless-Chrome web UI tests.
#
# The tests (uitest/, a Go module of its own so that chromedp stays out of
# syncat's go.mod and binary) drive the real web UI in headless Chrome
# against real daemons. This script sets those
# daemons up through scripts/lib/harness.sh, the same plumbing as the
# real-network scenarios: it builds the binary, creates alice, bob and carol,
# starts alice and bob and pairs them over real tailcat, then runs
# `go test` in that module with the environment the tests need. carol is left
# stopped; the tests that stop a daemon underneath the UI start and stop
# carol themselves, so the alice/bob pair the other tests share stays up.
#
# It is local only. Neither GitHub workflow runs it: it needs a Chrome, a real
# relay, and a minute or two of wall-clock time.
#
# Usage:
#   scripts/run-ui-tests.sh                   # every UI test
#   scripts/run-ui-tests.sh --run 'Token|Peer' # only tests matching a regexp
#
# Env:
#   UITEST_CHROME=/path/to/chrome   which browser to drive (default: the first
#                                   of google-chrome, google-chrome-stable,
#                                   chromium, chromium-browser,
#                                   /snap/bin/chromium)
#   SYNCAT_DEBUG=1                  run the daemons at debug level
#
# Everything a run leaves behind is under ${TMPDIR:-/tmp}/syncat-test/ui:
# each node's daemon.log, the go test output (go-test.log), and, for every
# failing test, a screenshot, the page text and the browser console under
# artifacts/.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

RUN=""
while [ $# -gt 0 ]; do
	case "$1" in
	--run)
		RUN="$2"
		shift
		;;
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

# Find a browser before building or starting anything, so a machine without
# one gets a clear skip rather than a half-started run. The Go tests repeat
# this check and skip with the same message when run directly.
find_chrome() {
	if [ -n "${UITEST_CHROME:-}" ]; then
		[ -x "$UITEST_CHROME" ] && echo "$UITEST_CHROME"
		return
	fi
	local c
	for c in google-chrome google-chrome-stable chromium chromium-browser; do
		if command -v "$c" >/dev/null 2>&1; then
			command -v "$c"
			return
		fi
	done
	[ -x /snap/bin/chromium ] && echo /snap/bin/chromium
}
CHROME="$(find_chrome)"
if [ -z "$CHROME" ]; then
	echo "run-ui-tests: SKIP: no Chrome or Chromium found; install one or set UITEST_CHROME" >&2
	exit 0
fi

. "$SCRIPT_DIR/lib/harness.sh"

harness_init ui
info "browser: $CHROME ($("$CHROME" --version 2>/dev/null || echo 'version unknown'))"

# Run the daemons from inside the work dir, so anything a daemon writes
# relative to its working directory lands where a test can see it, never in
# the repo.
cd "$WORK"

node_add alice
node_add bob
node_add carol
node_start alice
node_start bob
pair_nodes alice bob

ARTIFACTS="$WORK/artifacts"
mkdir -p "$ARTIFACTS"

log "Running the UI tests (go test in uitest/)"
GO_ARGS=(test -count=1 -v -timeout 20m ./...)
[ -n "$RUN" ] && GO_ARGS+=(-run "$RUN")
set +e
(
	cd "$REPO_ROOT/uitest" &&
		UITEST_WORK="$WORK" UITEST_BIN="$BIN" UITEST_ARTIFACTS="$ARTIFACTS" UITEST_CHROME="$CHROME" \
			go "${GO_ARGS[@]}"
) 2>&1 | tee "$WORK/go-test.log"
GO_STATUS=${PIPESTATUS[0]}
set -e

# carol belongs to the Go tests. They stop it themselves, but a test binary
# killed by its own timeout never runs its cleanups, so make sure.
pkill -f -- "--config $WORK/carol/config" 2>/dev/null || true

if [ "$GO_STATUS" -ne 0 ]; then
	if compgen -G "$ARTIFACTS/*" >/dev/null; then
		echo "--- failure artifacts (screenshots, page text, console) ---" >&2
		ls -1 "$ARTIFACTS" >&2
	fi
	fail "UI tests failed; go test output in $WORK/go-test.log, artifacts in $ARTIFACTS, daemon logs in $WORK/<node>/daemon.log"
fi

if grep -q -- '--- SKIP' "$WORK/go-test.log"; then
	info "skipped tests:"
	grep -A1 -e '--- SKIP' "$WORK/go-test.log" | sed 's/^/        /' >&2 || true
fi

harness_pass "the web UI behaved against real daemons; logs in $WORK"
