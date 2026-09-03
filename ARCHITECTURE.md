# syncat architecture

This document explains how the code is organized and how the pieces fit
together. It is for someone opening the repository for the first time who
wants to know where to look. It does not restate the design rationale in
[SPEC.md](SPEC.md) or the user-facing behavior in [README.md](README.md);
where a section number appears (e.g. §5) it refers to SPEC.md.

Line numbers are avoided on purpose. Every file long enough to need it has
`// --- section ---` markers, so `grep '^// --- ' path/to/file.go` prints
its table of contents, and every package has a doc comment on one of its
files saying what each file is for.

## One paragraph

syncat is a single Go binary. `syncat daemon` runs a **node**: it holds a
persistent Ed25519 identity, dials configured **peers** over tailcat (a
WireGuard tunnel library with no control plane), watches **shares**
(directories this node offers) and **subscriptions** (a peer's share
mirrored locally), reconciles file indexes with each connected peer using
version vectors, moves file bytes over a single framed stream per peer, and
serves a loopback-only REST API plus an embedded web UI. Every other
`syncat` subcommand is a thin client of that API.

## Layers

Packages import strictly downward. `internal/core` is the top of the
library; everything above it is a delivery mechanism (HTTP, CLI). The
constraint that matters most (§12) is that `internal/core` and everything
below it are cgo-free and import nothing from `net/http`, the CLI, or the
UI, so the same code can be built for mobile later.

```
cmd/syncat            CLI: one subcommand per REST endpoint, plus daemon/init/token
    │
    ▼
internal/api          REST server: routing, auth, JSON shapes
    │           └──▶ internal/webui   go:embed'd single-page UI (static files)
    ▼
internal/core         the Node: lifecycle, peer manager, share registry, mutations
    │
    ├──▶ internal/sync        per-peer Session: reconcile + transfer + apply + trash
    │        ├──▶ internal/index     SQLite index, filesystem scanner, fsnotify watcher
    │        └──▶ internal/protocol  wire messages, framing, handshake, keepalive
    ├──▶ internal/transport   Transport interface, tailcat carrier, loopback test carrier
    └──▶ internal/config      config.json schema, keys, sc1 tokens, XDG paths
```

`internal/config` and `internal/transport` are leaves. `internal/protocol`
imports `config` only to parse `sc1` tokens during the handshake.
`internal/index` imports `protocol` only for the shared `FileInfo` /
`VersionVector` types. `internal/transport` does **not** import `protocol`:
it hands back a raw `net.Conn` and knows nothing about what flows over it.

## The packages

### `internal/config` — what is on disk under `~/.config` and `~/.local/share`

| File | Holds |
|---|---|
| `config.go` | `Config`/`Peer`/`Share`/`Subscription` schema, defaults, `Validate`, the JSON doc-types and `Load`/`Save`, the share/subscription overlap rule (`CheckSharePath`, `CheckSubscriptionPath`), `NewShareID` |
| `keys.go` | `IdentityKey` (Ed25519), the tailcat key, `sc1…` node tokens (`EncodeToken`/`ParseToken`), the REST API token |
| `paths.go` | `Paths` (XDG layout, drawn as a tree in its doc comment) and `writeFileAtomic`, which every persisted file goes through |

Key idea: `Config` is a plain in-memory struct; the on-disk JSON shape is a
separate set of `*Doc` types so that pointer-defaulting and future format
changes never leak into the rest of the program. The spec says TOML; this
implementation uses JSON to stay inside the dependency budget (README
"Deviations").

### `internal/transport` — getting a `net.Conn` to a peer

| File | Holds |
|---|---|
| `transport.go` | The `Transport` interface (`Start`/`Dial`/`DiscardPeer`/`LocalAddress`/`Close`); `Backoff` and `Supervisor` (the dial-with-exponential-backoff loop, §2.2); `KeepConnection`, the duplicate-connection tie-break (§2.4) |
| `tailcat.go` | `TailcatTransport`, the production carrier. Its long comments record hard-won facts about tailcat client lifetimes and DERP key collisions; read them before touching it |
| `pipe.go` | `PipeTransport`, used by every other package's tests. Address registry plus stdlib `net.Pipe`; the ordered v2 handshake has no simultaneous writes |

`KeepConnection` deserves a sentence: when two nodes dial each other at
once, each side independently computes the same answer about which of the
two connections survives from just the two public keys and which side
dialed. No coordination message is needed.

### `internal/protocol` — what goes over that `net.Conn`

| File | Holds |
|---|---|
| `message.go` | `MsgType` and every message struct (§4), CBOR-encoded; the length-prefixed frame codec `Writer`/`Reader`; `MaxFrameSize`, `MaxFileChunkData` |
| `stream.go` | `StreamWriter`: one goroutine, two bounded lanes (control frames jump ahead of `FileChunk`s), a per-frame write deadline. This is the only thing a `Session` writes through |
| `handshake.go` | `Handshake`: mutual Ed25519 challenge/response, version negotiation, and the check that the peer is one we configured (`HandshakeConfig.IsKnownPeer`) |
| `keepalive.go` | `Keepalive`: Ping/Pong idle and dead-connection timing. Pure timer state, no I/O; the session drives it |

Framing is `4-byte length | 1-byte type | CBOR body`, and `FileChunk` is
the one message with a raw byte payload after the CBOR header, so a 1 MiB
chunk is not CBOR-encoded byte by byte. `FuzzDecode` in `message_test.go`
targets the decoder.

### `internal/index` — what this node believes about each share

| File | Holds |
|---|---|
| `store.go` | `Store` over SQLite (`modernc.org/sqlite`, pure Go). Tables: `files` (our own view, keyed `(share_id, relpath)`), `peer_files` (each peer's last reported view), `pending_transfers` (schema only; resume is deferred). `FileRow` is the row shape; `ApplyScanResult` is the one write path a scan uses |
| `scanner.go` | `Scanner.Scan`: walk a share root, compare to the existing rows, classify every entry as added / content-changed / metadata-only / deleted / unchanged, hashing only when size or mtime moved. `Matcher` is the fixed ignore list (full `.syncatignore` syntax is deferred) |
| `watcher.go` | `Watcher`: `fsnotify` on the tree, a `debouncer` that collapses event bursts, and a periodic full rescan as the safety net for missed events |

The scanner never touches the network or `internal/sync`; it only produces
a `ScanResult`. That separation is load-bearing for the trash can (see
"Local deletes are never trashed" below).

### `internal/sync` — one peer connection, reconciled and applied

Imported as `syncsvc` everywhere because the name collides with stdlib
`sync`.

| File | Holds |
|---|---|
| `reconcile.go` | The pure decision layer: version-vector algebra (`Equal`/`Dominates`/`Concurrent`/`Merge`/`Bump`, §5), `Action`/`ActionKind`, `Direction`/`DirectionFor`, conflict-copy filenames, and `Reconcile` itself. **No I/O.** Its tests are table-driven and include a randomized algebra check |
| `session.go` | `Session`: lifecycle (`Start`/`Close`/`Done`), the `readLoop` that demuxes frames, `SyncShare` (send a full `IndexUpdate`), and `handleIndexUpdate` (the reconcile-and-apply pass) |
| `transfer.go` | The byte-moving half of `Session`: `pullFile` (bounded by `maxConcurrentPulls`, with a stall timeout) and the serving side (`handleFileRequest`/`streamFile`/`serveSymlink`) |
| `apply.go` | The disk-writing half of `Session`: `applyAction` and the per-kind appliers (`applyDelete`, `applyConflictCopy`, `pullAndInstall`, `applyLocallyModified`). Every destructive step goes through the trash hook first |
| `path.go` | `ValidateRelPath` and `JoinSharePath`: the single gate every peer-supplied relative path passes through before it touches the filesystem. Kept as its own file so security review can find it by name |
| `trash.go` | `Trash` (`Put`/`List`/`Restore`, §7) and `Janitor`, which expires entries on a timer |

`Session` is one type across three files by design: `session.go` says
what a session is and how it starts and stops, `transfer.go` moves bytes,
`apply.go` writes to disk. If you are looking for a `Session` method and
it is not in `session.go`, it is in one of the other two.

### `internal/core` — the node

| File | Holds |
|---|---|
| `node.go` | `Node`, `Options`, `Open` (startup ordering) and `Close` (shutdown ordering), `goTracked`, the inbound accept path (`onAccept`/`handleAccept`), and the small `Clock` adapters that let tests fake time in every lower package |
| `peer.go` | `peerConn`: one per configured peer. `runSupervisor` → `dialAttempt` → `offer` (dedup + adopt) → `runConnection`/`runKeepalive`. Also `ConnState` and the per-peer snapshot helpers `currentSession`/`neuterShare` |
| `access.go` | Share-access negotiation over an adopted connection (§6): the offerer side answers `SubscribeRequest`, the subscriber side reacts to `AccessUpdate`, and `ShareList` announcement. `neuterShareOnSessions` lives here |
| `shares.go` | `shareWatch`: wiring one local directory to an `index.Scanner`/`Watcher`; `rescanShare` (scan → `Bump` → persist → `propagateShare`) |
| `mutations.go` | The config-mutation API the REST layer and CLI call: `AddPeer`, `RemovePeer`, `AddShare`, `SetShareAccess`, `AddSubscription`, `PauseSubscription`, `RenameNode`, `ListTrash`, `RestoreTrash`, … Each one clones the config, mutates, validates, saves, then applies the live effects |
| `resolve.go` | Turning what a human typed (`alice`, `docs`, a 6-hex-char id prefix) into an exact peer key or share id, with a descriptive error when the reference is ambiguous or matches nothing |
| `status.go` | `Status()` and the plain structs it returns. Deliberately no `encoding/json` here — marshaling is `internal/api`'s job |

### `internal/api` and `internal/webui` — the REST surface

| File | Holds |
|---|---|
| `api/server.go` | `NewServer`, `ListenLoopback`, the route table, the `auth` middleware (constant-time `X-Syncat-Token` check), the unauthenticated `/ui-token` bootstrap endpoint with its DNS-rebinding defenses, and the shared helpers (`writeJSON`, `writeError`, `writeMutationError`, `mutationError`, `decodeJSON`) |
| `api/handlers.go` | One `handle*` method per endpoint, grouped by resource in the same order as the route table |
| `api/dto.go` | The JSON shapes every response goes through and the `to*DTO` converters from `core.Status` |
| `webui/embed.go` | `go:embed static`, served with pinned content types. `static/app.js` is a no-build-step vanilla-JS single-page app that polls the REST API |

### `cmd/syncat` — the CLI

| File | Holds |
|---|---|
| `main.go` | Global flags (`--config`, `--data`), the subcommand table, usage |
| `client.go` | `apiClient` (reads `api_addr` and the token from disk, adds the header) and the `*View` structs it decodes into |
| `cmd_node.go` | `init` and `token` and `config` (offline: touch disk directly, no daemon needed); `daemon` (is the daemon: `startDaemon` → `waitAndShutdown`); `status` (API client) |
| `cmd_peer.go` | `peer add/ls/rm/approve`, `remote ls`, `approvals` |
| `cmd_share.go` | `share …`, `subscription …`, `trash ls/restore` |

Only `init`, `token`, and `config` work without a running daemon.
Everything else, including `trash`, goes through the API so there is
exactly one process touching SQLite.

## The flows

### Startup: `syncat daemon`

`cmd_node.go: startDaemon` loads config and keys, binds the loopback
listener **before** starting the node (so a port clash fails fast), builds
a `TailcatTransport`, and calls `core.Open`. `Open` runs in this order, and
the order is the point:

1. `loadOpenInputs`: config (created with defaults on first run), identity,
   clock/rand/logger defaults.
2. Open the `index.Store`; construct the `Trash` and start the `Janitor`.
3. `Transport.Start` with `onAccept` as the inbound callback. From here on
   peers may connect.
4. Compute our own `sc1` token from the transport's local address.
5. `startConfiguredWatches`: one `shareWatch` per share and per
   non-paused subscription, each followed by an initial rescan.
6. `startConfiguredPeers`: one `peerConn` per configured peer, each with
   its own supervisor goroutine.

A single `cleanup` closure undoes 2–3 on any failure. `Close` cancels the
node context (which ends every peer's supervisor and session), waits for
all tracked goroutines, then closes watchers, janitor, store, and
transport, in that order.

### Connecting to a peer

Each `peerConn` runs `transport.Supervisor`, which calls `dialAttempt`
with exponential backoff (1s → 5min, jittered). Inbound connections arrive
via `Node.onAccept`. Both paths converge on the same steps:

1. The dialer runs `protocol.InitiateHandshake` and the acceptor runs
   `protocol.AcceptHandshake`. Their ordered Hello/HelloAuth/Auth/Finished
   exchange signs a role-labelled transcript of both Hellos. `IsKnownPeer`
   rejects an inbound key we did not configure; the rejection is recorded
   for the UI (`RejectedConnection`).
2. `peerConn.offer`: `transport.KeepConnection` decides whether this
   connection survives if the peer dialed us at the same moment. The loser
   is closed quietly and is not a backoff failure.
3. The winner becomes a `syncsvc.Session`. `offer` installs
   `peerConn.handleControl` as the control handler (for `ShareList`,
   `SubscribeRequest`, `AccessUpdate`, `Ping`/`Pong`), starts the session,
   starts the keepalive goroutine, and sends our `ShareList` plus a
   `SubscribeRequest` for every subscription we hold with this peer.
4. `access.go` takes over. When the offerer receives a `SubscribeRequest`
   it provisions the share on the session (`provisionShareForRequest`),
   persists the grant (`SetShareAccess`), and sends `AccessUpdate`. When
   the subscriber receives a granted `AccessUpdate`
   (`provisionAccessUpdate` → `finishAccessUpdate`) it marks the share
   active on the session and rescans, which sends the first `IndexUpdate`.

`runConnection` waits for the session to end for any reason, then tears
down in a fixed order: cancel, wait for keepalive, close session, close
conn, `Transport.DiscardPeer`, reset state, signal `dialAttempt` to redial.
That order is commented inline and is the most bug-sensitive code in the
package.

### A local file changes

```
fsnotify event ─▶ debouncer ─▶ Watcher.OnDirty ─▶ Node.rescanShareAsync
                                                        │
      Scanner.Scan(root, existingRows) ─▶ ScanResult   ◀┘
      Bump(version, ourID) on Added/ContentChanged/Deleted
      Store.ApplyScanResult
      Node.propagateShare ─▶ for each session with this share active:
                                 Session.SyncShare ─▶ IndexUpdate{Full: true}
```

Only content changes bump the version vector; metadata-only changes are
persisted without a bump so they do not cause a spurious conflict on the
peer. The periodic rescan runs the same path with no event.

### A peer's `IndexUpdate` arrives

`Session.readLoop` receives the frame and hands it to a new goroutine —
never handled inline, because applying it may need to `pullFile`, and a
pull is fed by the very read loop that would otherwise be blocked.

```
handleIndexUpdate
  Store.UpsertPeerFiles(peer's rows)            our mirror of their view
  Reconcile(localRows, remoteRows, dir, ourID)  pure; one Action per relpath
  for each Action (concurrently, bounded):
     applyAction ─▶ applyDelete | applyConflictCopy | pullAndInstall | applyLocallyModified
                    each destructive step: Trash.Put first, then act
                    pullAndInstall: FileRequest ─▶ FileChunk* ─▶ temp file ─▶ sha256 check ─▶ rename
     Store.PutFile(resulting row)
  if anything changed locally and outbound is not blocked:
     SyncShare  (so the peer's next reconcile converges)
```

`Reconcile`'s decision table is §5's: dominated side loses, equal content
with concurrent vectors is merged silently, a true concurrent edit produces
a conflict copy named with the losing node and a timestamp, and a
receive-only side that has local edits gets `ActionLocallyModified` (the
edit is trashed and overwritten, with a warning surfaced in status).

### A CLI or API mutation

```
syncat share add ~/Documents --name docs
  cmd_share.go: POST /api/shares          (apiClient adds X-Syncat-Token)
  api/handlers.go: handleSharesAdd        decode, validate shape
  core/mutations.go: AddShare
     resolve.go: resolve any name/prefix → id
     mutateConfig: clone cfg, edit, Validate, Save (atomic write), swap
     live effects, with cfgMu released: startShareWatch, rescan, broadcastShareList
  api: writeJSON(toShareDTO(...)) or writeMutationError(err)
```

`mutationError` maps `core` error text to HTTP status: "not configured" /
"no share matches" → 404, "already configured" / "overlaps" → 409, and
everything else (including an ambiguous name prefix) → 400, since it is
caller-fixable input. The API never imports `internal/sync` types except
to render them.

## Concurrency model

Goroutines, from the outside in:

- **Per node**: the transport's accept loop; the `Janitor` timer; one
  `Watcher` event loop and `debouncer` per share/subscription; anything
  started via `Node.goTracked`, which `Close` waits for.
- **Per peer**: the `Supervisor` dial loop. While connected: the
  `Session.readLoop`, the `StreamWriter.run` writer, the keepalive loop,
  and `runConnection` waiting for any of them to end.
- **Per incoming `IndexUpdate`**: one `handleIndexUpdate` goroutine, which
  fans out bounded per-action goroutines. Pulls are limited by
  `maxConcurrentPulls` (4); serves by `maxConcurrentServes`.

Rules that keep this sound:

1. **The read loop never blocks on anything that needs the read loop.**
   Control-frame replies (a `Pong`) are the only inline writes, and they
   are safe only because `StreamWriter` gives them a priority lane that
   cannot queue behind a `FileChunk`.
2. **Lock ordering in `core`**: `Node.cfgMu` (config) is never held while
   taking a `peerConn.mu`. Mutations clone-mutate-swap under `cfgMu`,
   release it, then apply live effects. `peersMu` guards only the peer
   map; `sharesMu` only the watch map.
3. **One writer per connection.** Everything a session sends goes through
   its `StreamWriter`; the mutex in `protocol.Writer` guarantees frames are
   never interleaved.
4. **Time is injectable.** `transport.Clock`, `protocol.Clock`,
   `index.Clock`, `sync.Clock`, and `sync.JanitorClock` exist so tests can
   drive timers deterministically. README "Transport and protocol" item 4
   proposes replacing all of them with `testing/synctest`.

## Security boundaries

- **The `sc1` token is a secret.** It carries the tailcat connection blob,
  the Ed25519 public key, and a display name. Anyone holding it can attempt
  to connect; the handshake is what proves they are the peer you added.
- **Handshake** (`protocol/handshake.go`): mutual challenge/response;
  unknown inbound keys rejected and recorded. Known limit: the transcript
  does not bind the tunnel, so a token tampered in transit enables a relay
  (README item 2).
- **Every peer-supplied path** goes through `sync/path.go` before it is
  joined to a share root. `Reconcile` drops invalid relpaths before they
  become `Action`s; `apply.go`, `transfer.go`, and `trash.go` call
  `JoinSharePath` again at the point of use.
- **Local deletes are never trashed; remote ones always are.** `Trash.Put`
  is called only from `apply.go`, which runs only from `handleIndexUpdate`.
  A local deletion is observed by `internal/index`, which cannot import
  `internal/sync` (it would be a cycle) and so cannot reach the trash even
  by mistake. `trash_test.go` exercises both paths side by side.
- **The REST API is loopback-only** (`ListenLoopback` refuses non-loopback
  bind addresses) and every request needs `X-Syncat-Token`, compared in
  constant time. `/ui-token` is the one unauthenticated route; it checks
  `Host`, `Origin`, and the remote address, and `isLoopbackAddr` there is
  deliberately stricter than `isLoopbackHost` (no `localhost` literal).

## On disk

```
~/.config/syncat/config.json          Config: node_name, api_addr, peers[], shares[], subscriptions[], …
~/.config/syncat/api.token            REST token the CLI and UI present
~/.local/share/syncat/keys/identity.key   Ed25519 seed
~/.local/share/syncat/keys/tailcat.key    tailcat node key
~/.local/share/syncat/db/index.db         SQLite: files, peer_files, pending_transfers
~/.local/share/syncat/trash/<share-id>/<relpath>.<unix-ts>
```

All writes go through `config.writeFileAtomic` (temp file + rename). The
index is keyed by `(share_id, relpath)`, which is why a subscribed
directory cannot be re-offered as a share: the same files under two share
ids would ping-pong edits forever (README "Topology").

## Testing

- Every package has `_test.go` files next to its sources. Test files map
  1:1 to source files except `sync/integration_test.go` (two real
  `Session`s over `PipeTransport`, end to end) and `api/server_test.go`
  (covers handlers and DTOs through HTTP).
- `transport.PipeTransport` is the carrier every test above `transport`
  uses; `tailcat_test.go` and one test in `config/keys_test.go` need real
  network access.
- `scripts/e2e.sh` builds the binary and runs two daemons over live
  tailcat, asserting bidirectional sync, delete+trash, and conflict
  convergence on disk.
- On some machines the tailscale dependency's init-time toolchain check
  panics before any test runs; `TS_PERMIT_TOOLCHAIN_MISMATCH=1` disables
  it. This is an environment quirk, not a code issue.

```
gofmt -l . && go vet ./... && CGO_ENABLED=0 go build ./...
TS_PERMIT_TOOLCHAIN_MISMATCH=1 go test -count=1 ./...
TS_PERMIT_TOOLCHAIN_MISMATCH=1 go test -race ./internal/core ./internal/sync ./internal/protocol
```

## Conventions

- **One idea per file, named for the idea.** A file with two "this file
  is about X" headers has drifted; split it. A file under ~100 lines that
  exists only to serve one neighbor should probably be merged, unless it is
  kept separate so that reviewers can find it by name (`sync/path.go`).
- **Sections.** Files long enough to need it use
  `// --- name ----…` markers (dashes to column 72). Constants belong
  *inside* the section that owns them, below the header.
- **Package doc lives in one file per package** and names what each other
  file in the package holds. Keep it current when adding a file.
- **Comments say why, not what.** Long comments in `transport/tailcat.go`,
  `core/peer.go`, and `sync/reconcile.go` record non-obvious behavior of
  dependencies or the protocol; they are meant to be read before editing.
  Comments that recount the history of a rename belong in `git log`.
- **Spec references.** `SPEC.md §N` pins a comment to the design document.
  README "Deviations from SPEC.md" lists every place the code intentionally
  departs from it.
- **`internal/core` types are plain.** No `encoding/json`, no HTTP, no
  CLI. `api/dto.go` and `cmd/syncat/client.go` own the JSON shapes.
- **Test-only exports** go in `export_test.go` (see `core.RescanShare`),
  not in the production API.

## Where to look for …

| … | Start at |
|---|---|
| How two peers decide who wins a simultaneous dial | `transport.KeepConnection`, then `core/peer.go: offer` |
| Why a file got a conflict copy | `sync/reconcile.go: reconcileConcurrent` |
| Why a file was overwritten on a receive-only side | `sync/reconcile.go: ActionLocallyModified`, `sync/apply.go: applyLocallyModified` |
| Why a peer keeps reconnecting | `core/peer.go: dialAttempt` and the constants at the top of that file |
| What a subcommand actually sends | `cmd/syncat/cmd_*.go` → the matching `api/handlers.go: handle*` |
| What the API returns | `api/dto.go` |
| Why a name like `docs` resolved to the wrong share, or errored | `core/resolve.go: matchRef` |
| How a share is granted or revoked | `core/access.go`, then `core/mutations.go: SetShareAccess` |
| Where a peer-supplied path is checked | `sync/path.go` |
| How the trash decides what to keep | `sync/trash.go: Put`, `Janitor.Sweep` |
| The wire format | `protocol/message.go` (types and framing), `SPEC.md §4` |
| How the web UI authenticates | `api/server.go: handleUIToken`, `webui/static/app.js` "API client" section |
