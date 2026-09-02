# syncat

Peer-to-peer directory sync over [tailcat](https://github.com/tailscale/tailcat) —
Tailscale's Go library for point-to-point, WireGuard-encrypted tunnels that needs
**no Tailscale account, no control plane, and no root**. There is no server, no
discovery, and no central directory of nodes: you add a peer by pasting a token
it prints, the two nodes dial each other directly (relayed through Tailscale's
DERP servers when a direct path isn't available), and files sync straight
between them.

syncat is a single Go binary. `syncat daemon` runs the node — it maintains a
persistent identity, dials configured peers, watches shared directories, syncs
changes, and serves a REST API plus an embedded web UI on `127.0.0.1`. Every
other `syncat` subcommand is a thin client of that same API.

Target use: syncing directories between your own machines, or with a handful
of trusted friends/family — not a general file-sharing service.

## Build and quick start

Requires Go 1.27+. The build is cgo-free (pure-Go SQLite, see [Development](#development)):

```console
$ CGO_ENABLED=0 go build -o syncat ./cmd/syncat
```

Run one node:

```console
$ ./syncat init --name my-laptop
$ ./syncat daemon &
```

Open `http://127.0.0.1:8347` for the web UI, or drive it from the CLI — see
`./syncat -h` for the full subcommand list, one per REST endpoint.

### Two-node walkthrough

This is the flow `scripts/e2e.sh` automates end-to-end; here it is by hand,
using `--config`/`--data` overrides so two nodes can run on one machine.

```console
# --- terminal 1: alice -------------------------------------------------
$ ./syncat --config ~/.alice-cfg --data ~/.alice-data init --name alice
$ ./syncat --config ~/.alice-cfg --data ~/.alice-data daemon --api 127.0.0.1:18347 &

# --- terminal 2: bob ----------------------------------------------------
$ ./syncat --config ~/.bob-cfg --data ~/.bob-data init --name bob
$ ./syncat --config ~/.bob-cfg --data ~/.bob-data daemon --api 127.0.0.1:18348 &
```

`--api` on `daemon` only sets that process's listen address; the CLI reads
`api_addr` back out of `config.json`, so for two nodes on one machine either
edit `config.json`'s `api_addr` before starting the daemon, or pass
`--api` and always talk to that node's CLI with a matching `$CONFIG`/`$DATA`
pair (as above).

```console
# Exchange tokens and peer — both directions, since peering is mutual:
$ ALICE_TOKEN=$(./syncat --config ~/.alice-cfg --data ~/.alice-data token)
$ BOB_TOKEN=$(./syncat --config ~/.bob-cfg --data ~/.bob-data token)
$ ./syncat --config ~/.bob-cfg --data ~/.bob-data peer add "$ALICE_TOKEN" --name alice
$ ./syncat --config ~/.alice-cfg --data ~/.alice-data peer add "$BOB_TOKEN" --name bob

# Watch them connect (DERP relay setup can take a few seconds):
$ ./syncat --config ~/.alice-cfg --data ~/.alice-data status --watch

# Alice offers a share, bob subscribes to it:
$ ./syncat --config ~/.alice-cfg --data ~/.alice-data share add ~/Documents --name docs --perm rw
$ ./syncat --config ~/.bob-cfg --data ~/.bob-data remote ls
$ ./syncat --config ~/.bob-cfg --data ~/.bob-data subscription add <alice-name-or-id> <share-name-or-id> ~/docs-from-alice --mode mirror
```

Write a file under `~/Documents` on alice's side and it appears under
`~/docs-from-alice` on bob's — and, since the share is `read-write` and the
subscription is `mirror`, changes flow the other way too.

## How the three sync modes emerge

There is no single "sync mode" setting. It falls out of the offerer's share
permission crossed with the subscriber's chosen mode:

| Offerer permission | Subscriber mode           | Effective behavior                                             |
|---------------------|---------------------------|------------------------------------------------------------------|
| read-write          | mirror                    | **Bidirectional mirror** — both sides propagate changes           |
| read-only           | receive-only (forced)     | **One-way push** — offerer is the source of truth                |
| read-write          | receive-only              | **One-way pull / backup** — subscriber takes changes, never sends |

On a read-only share, subscribers are forced into receive-only: local edits to
the subscriber's copy are detected, logged as warnings, and overwritten (after
a trash copy is taken) the next time the offerer's copy changes.

## Topology: hub-and-spoke

A share propagates through its offerer only. If alice offers a share to both
bob and carol, changes flow alice↔bob and alice↔carol — bob and carol never
talk to each other about that share, even though they're both syncing it. If
alice's node is offline, bob and carol's copies simply stop syncing with
anyone until alice comes back; they do not sync with each other in the
meantime.

**A directory synced down from a peer cannot be re-shared.** Config validation
(`internal/config/config.go`) rejects offering a share whose path is, contains,
or is contained by an existing subscription's local path. This isn't
arbitrary: syncat's index is keyed by `(share_id, relpath)`, so re-offering a
subscribed directory as a new share would track the same files under two
independent share IDs and version-vector lineages on the middle node. A change
arriving on one share would look like a fresh local edit to the other, and the
two shares would bounce edits back and forth indefinitely — chaining `A → B →
C` through a node that was only ever supposed to be a spoke.

## Security model

- **The node token is a secret.** `syncat token` prints a `sc1…` blob wrapping
  your tailcat connection info, Ed25519 public key, and suggested display
  name. Anyone holding it can *attempt* to connect — treat it like a password,
  not a username.
- **Mutual authentication.** The tailcat tunnel gives you an encrypted pipe to
  *some* endpoint; syncat's own handshake (Ed25519 challenge/response over
  that pipe) is what proves it's the peer you actually added. An inbound
  connection from an unrecognized key is rejected and recorded, not trusted.
- **The REST API is loopback-only**, and every request (including the web
  UI's own `fetch` calls) requires the `X-Syncat-Token` header, checked with a
  constant-time comparison. The bootstrap `/ui-token` endpoint that hands the
  UI its token is itself guarded against DNS-rebinding by checking `Host`
  and `Origin`, and is only ever served to loopback callers.
- **Peering is mutual by construction.** Each side must independently add the
  other's token before either side sends any file data; adding a peer on only
  one end leaves that peer in a rejected/pending state on the other.

## Deviations from SPEC.md

Per SPEC.md §0, here's everywhere this implementation departs from the spec
text, and why:

- **`config.toml` → `config.json`.** SPEC.md §3 describes a TOML config file,
  but §10's dependency budget (tailcat, `modernc.org/sqlite`, `fsnotify`,
  `fxamacker/cbor`, a gitignore matcher, `x/crypto`) lists no TOML library.
  To stay inside that budget, config is encoded with the stdlib
  `encoding/json` instead and the file is named `config.json`. Field names,
  defaults, and validation are otherwise exactly as specified
  (`internal/config/config.go`).
- **`protocol.Error` gained optional `share_id`/`rel_path` fields.** SPEC.md
  §4's message table has a bare `Error{code, msg}`, with no way to say which
  in-flight request an error is about. With up to 4 concurrent pulls per
  peer, a transfer failure needs to be routed back to the specific
  `FileRequest` it answers, so `Error` additively gained two `omitempty`
  fields for that (`internal/protocol/message.go`).
- **`PATCH /api/node` was added.** SPEC.md §8's endpoint list omits it, but
  §9's dashboard requires an editable node name, and `core.Node` already
  exposed `RenameNode`. Wired up in `internal/api/server.go`/`handlers.go`.
- **The in-memory test transport is not `net.Pipe`.** SPEC.md §10/§13 name
  `net.Pipe` for the test transport, but the stdlib pipe is synchronous and
  unbuffered: a write blocks until the other end reads. SPEC.md §4 has both
  sides of the handshake send `Hello` before either reads, which deadlocks
  on a bare `net.Pipe`. `transport.PipeTransport` is therefore backed by a
  buffered in-memory `net.Conn` that behaves like a real socket's send
  buffer (`internal/transport/pipe.go`).
- **`syncat trash` now requires a running daemon.** SPEC.md's original phase
  plan had it operate directly on the on-disk trash/index, independent of a
  daemon (like `init`/`token`). Once the REST API existed, that would have
  been a second code path touching SQLite directly — and would race a live
  daemon's own index access if one happened to be running. `trash` is now an
  API client like every other subcommand (`cmd/syncat/cmd_share.go`).

## Deferred past the MVP

These are real gaps, not accidents — called out so nobody is surprised they
don't work:

- **Pending-peer approval and per-share `approval_required` enforcement.**
  An unrecognized inbound connection is rejected outright rather than queued
  (SPEC.md §2.3); every `SubscribeRequest` auto-grants regardless of a
  share's `approval_required` flag (SPEC.md §6). The flag is still persisted
  in config for when the queue exists. `POST /api/peers/{id}/approve`,
  `GET/POST /api/approvals` return 501.
- **`.syncatignore` gitignore-syntax matching.** Only a fixed set of
  always-ignored patterns (`.syncat.tmp.*`, OS junk files, global config
  ignores) is implemented; full gitignore syntax was deferred to avoid
  pulling in a matcher library before it was load-bearing
  (`internal/index/scanner.go`).
- **Symlink syncing.** Symlinks are detected during scanning and skipped
  entirely, with a warning — never entering the index or syncing to peers
  (`internal/index/scanner.go`). SPEC.md §5's "symlink entry itself syncs on
  Unix" is not implemented in this build.
- **Transfer resume.** `FileRequest.offset` is always sent as 0; an
  interrupted transfer restarts from the beginning rather than resuming
  (`internal/sync/session.go`). The `pending_transfers` table exists in the
  schema for it, but nothing reads or writes it yet.
- **Transfer progress in the status surface.** SPEC.md §8 lists transfer
  stats on `GET /api/status` and §9 wants progress bars on the dashboard.
  `internal/sync.Session` exposes no per-transfer byte counters, so nothing
  could populate them. The field and the dashboard card existed but were
  always empty, so both were removed rather than shipped as a permanent
  blank — `/api/status` has no `transfers` key and `syncat status` prints
  no transfer section.
- **Tombstone purging.** Deleted-file tombstones are kept in the index
  indefinitely; the 180-day purge described in SPEC.md §4 has no janitor.
- **`GET /api/events` (SSE).** The web UI polls the REST API instead of
  holding a live event stream open.
- **The Settings view.** Rescan interval, trash retention, global ignores,
  and API address are all editable in `config.json` and enforced by the
  daemon, just not from a UI panel yet.
- **Block-level delta transfer (SPEC.md §11).** All transfers are whole-file,
  as fixed for v1; FastCDC chunking and `BlockRequest` are speced but not
  built.
- **Mobile bindings (SPEC.md §12).** The core packages are already cgo-free
  and gomobile-shaped, but no gomobile build or mobile UI exists yet.

## Development

```console
$ CGO_ENABLED=0 go build ./...
$ go vet ./...
$ go test -count=1 ./...
$ go test -race ./...
$ go test -fuzz=FuzzDecode ./internal/protocol   # frame decoder, run for a bit then Ctrl-C
$ scripts/e2e.sh                                  # two real daemons over live tailcat
```

`CGO_ENABLED=0` is required, not just convenient: `modernc.org/sqlite` is a
pure-Go SQLite implementation chosen specifically so `internal/index` (and
everything above it) stays gomobile-compatible (SPEC.md §12 — "no cgo, no
CLI/HTTP imports inside core packages").

`scripts/e2e.sh` builds the binary, spins up two independent daemons with
separate config/data dirs and API ports, peers them over real tailcat,
shares/subscribes a directory, and asserts bidirectional sync, delete+trash,
and a two-sided conflict all converge correctly on disk. It cleans up both
daemons on any exit path and dumps the relevant daemon log tail on failure.

## Project layout

Nine packages, 29 source files. Every package is small enough to list in full:

| Package | Files |
|---|---|
| `cmd/syncat/` | `main.go` (subcommand dispatch, stdlib `flag`, no cobra) · `client.go` (REST client + response shapes) · `cmd_node.go` (`init`/`token`/`daemon`/`status`/`config`) · `cmd_peer.go` (`peer`/`remote`/`approvals`) · `cmd_share.go` (`share`/`subscription`/`trash`) |
| `internal/core/` | `node.go` (lifecycle, accept path, clock adapters) · `mutations.go` (the config-mutation API the REST layer calls, plus name/prefix reference resolution) · `peer.go` (per-peer dial/dedup/keepalive state machine) · `shares.go` (share dirs → scanner/watcher) · `status.go` (the read-only snapshot) — gomobile-safe, no UI/CLI deps |
| `internal/transport/` | `transport.go` (`Transport` interface, backoff/supervisor, dedup tie-break) · `tailcat.go` (production carrier) · `pipe.go` (in-memory transport used by every package's tests) |
| `internal/protocol/` | `message.go` (message types + frame codec) · `handshake.go` (mutual Ed25519 auth + keepalive) |
| `internal/index/` | `store.go` (SQLite schema, `files`, `peer_files`) · `scanner.go` (tree walk, hashing, ignore matching) · `watcher.go` (`fsnotify` + debounce + periodic rescan) |
| `internal/sync/` | `reconcile.go` (version vectors, actions, conflict naming, the reconciler — all pure, no I/O) · `session.go` (one peer connection: index exchange, pulls, serving) · `apply.go` (writing results to disk) · `path.go` (the single validation gate for peer-supplied relpaths) · `trash.go` (trash can + janitor) |
| `internal/config/` | `config.go` (schema, JSON encoding, load/save, re-share guard, atomic writes) · `keys.go` (identity key, tailcat key, `sc1` tokens, API token, share ids) · `paths.go` (XDG layout) |
| `internal/api/` | `server.go` (routing, auth, HTTP concerns) · `handlers.go` (endpoint handlers + JSON DTOs) |
| `internal/webui/` | `embed.go` + `static/` — `go:embed`-ed single-page UI (vanilla HTML/CSS/JS, no build step) |

Files long enough to need it open their sections with `// --- name ---`
markers, so `grep '^// --- ' internal/sync/reconcile.go` prints that file's
table of contents.
