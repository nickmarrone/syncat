// Package uitest drives the real web UI in headless Chrome against real
// syncat daemons (MANUAL-TESTS.md §10).
//
// It is a Go module of its own, github.com/nickmarrone/syncat/uitest, so
// that chromedp and everything else browser-related stays out of syncat's
// go.mod, its module graph and its binary. A `go test ./...` at the repo
// root never enters it, and neither GitHub workflow runs it. It imports
// nothing from syncat: it drives the built binary, its CLI and its HTTP API,
// the way a user does.
//
// Run it with scripts/run-ui-tests.sh, which builds the binary, starts and
// pairs the daemons through scripts/lib/harness.sh, and then runs
// `go test` here with the environment the tests need. Run directly, without
// that environment or without a Chrome or Chromium, every test skips and
// says why.
package uitest
