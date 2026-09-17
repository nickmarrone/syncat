# syncat architecture

This document describes the code structure and the main data flows. Read
[README.md](README.md) for user tasks. Read [SPEC.md](SPEC.md) for design
rules. A reference such as §5 means a section in SPEC.md.

Source files use `// --- section ---` markers. Run this command to list the
sections in a file:

```bash
grep '^// --- ' path/to/file.go
```

## Overview

syncat is one Go binary. `syncat daemon` runs a node. A node has an Ed25519
identity. It connects to configured peers through tailcat. It watches local
shares and subscriptions. It compares file indexes with peers. It transfers
file data on one framed connection for each peer. It also provides a
loopback-only REST API and an embedded web UI.

All other CLI commands use the REST API.

## Package layers

Packages import down this diagram. `internal/core` and lower packages must not
import HTTP, CLI, or UI code. They must remain free of cgo.

```text
cmd/syncat            CLI
    |
    v
internal/api          REST server, authentication, JSON
    |  \-> internal/webui     Embedded static web UI
    v
internal/core         Node lifecycle, peers, shares, mutations
    |
    +-> internal/sync         Sessions, reconcile, transfer, apply, trash
    |       +-> internal/index     SQLite, scan, ignore, watch
    |       +-> internal/protocol  Frames, handshake, keepalive
    +-> internal/transport    tailcat and test connections
    +-> internal/config       Config, keys, tokens, paths
```

`internal/config` and `internal/transport` are leaf packages.
`internal/protocol` imports `config` only to parse `sc1` tokens.
`internal/index` imports `protocol` for `FileInfo` and `VersionVector`.
`internal/transport` returns a `net.Conn`. It does not know the protocol.

## Packages

### `internal/config`

This package manages files below the XDG config and data directories.

| File | Purpose |
|---|---|
| `config.go` | `Config`, peers, pending peers, shares, subscriptions, defaults, validation, JSON load and save, and path checks |
| `keys.go` | Ed25519 identity, tailcat key, `sc1` token, and API token |
| `paths.go` | XDG paths and atomic file writes |
| `internal/version` | Product version and build commit ID |

`Config` is an in-memory Go struct. Separate document types define the JSON
format. This lets the program apply defaults and change the JSON format without
putting document rules in other packages. The specification names TOML. The
implementation uses JSON to avoid a TOML dependency.

### `internal/transport`

This package creates `net.Conn` connections.

| File | Purpose |
|---|---|
| `transport.go` | `Transport`, exponential-backoff supervisor, and duplicate connection rule |
| `tailcat.go` | Production tailcat transport |
| `pipe.go` | `net.Pipe` transport for tests |

Two peers can dial at the same time. `KeepConnection` uses both public keys and
the dial direction to choose one connection. Both peers choose the same result.

### `internal/protocol`

This package defines the peer wire protocol.

| File | Purpose |
|---|---|
| `message.go` | CBOR messages, frame codec, snapshots, deltas, acknowledgements, and transfer IDs |
| `stream.go` | One writer goroutine and four bounded queues |
| `handshake.go` | Mutual Ed25519 handshake, version check, and typed rejection |
| `keepalive.go` | Ping/Pong timer logic |
| `diagnostic.go` | Safe text for peer error messages |

A frame has this format: `4-byte length | 1-byte type | CBOR body`.
`FileChunk` has a CBOR header and raw file data. This avoids CBOR encoding each
byte of a file chunk.

The writer queues urgent messages, ordered control messages, replaceable
current-state messages, and bulk data. The queues have limits. The writer gives
control traffic priority but lets bulk data make progress. Each frame has a
write deadline.

### `internal/index`

This package stores the node's view of each share.

| File | Purpose |
|---|---|
| `store.go` | SQLite store, file rows, peer rows, cursors, journal, and migrations |
| `syncstore.go` | Snapshot staging and incremental index operations |
| `scanner.go` | Directory walk, file hash, and scan result |
| `ignore.go` | `.syncatignore` parser and matcher |
| `watcher.go` | fsnotify watcher, debounce, and periodic scan |

The store uses pure-Go SQLite. `files` is the local view. `peer_files` is the
last view received from each peer. `change_journal` records changes for
incremental index updates. `pending_transfers` is reserved for future resume
support.

When a new ignore rule matches an indexed path, the scanner parks the row. It
does not make a delete tombstone. A tombstone would tell peers to delete their
copies. The parked row keeps its version vector. If the rule is removed later,
the file can synchronize again.

The scanner does not use the network. It returns a `ScanResult` to core.

### `internal/sync`

This package implements one session for each peer connection. The package name
is imported as `syncsvc` because it conflicts with the standard `sync` package.

| File | Purpose |
|---|---|
| `reconcile.go` | Version vectors, action selection, conflicts, and direction rules. No I/O. |
| `session.go` | Session lifecycle, reader, index synchronization, and bounded work |
| `dispatch.go` | Validate and dispatch post-handshake frames |
| `transfer.go` | Pull, serve, cancel, stall handling, and transfer IDs |
| `apply.go` | Apply remote changes and use the trash before destructive work |
| `path.go` | Validate and join a peer path |
| `trash.go` | Trash store, restore, and cleanup timer |

`Session` spans four files. `session.go` owns lifecycle and index work.
`transfer.go` moves data. `apply.go` changes the file system. `dispatch.go`
checks and schedules received frames.

### `internal/core`

This package owns the node.

| File | Purpose |
|---|---|
| `node.go` | Node start and stop, accept path, and test clocks |
| `peer.go` | One `peerConn` per peer, dialing, deduplication, session, and keepalive |
| `access.go` | Share list, subscription request, access update, grant, and revoke |
| `approvals.go` | Persisted pending peer and share approval queues |
| `shares.go` | Share watcher, scan, version bump, and propagation |
| `journal.go` | Journal retention cleanup |
| `mutations.go` | Config changes from the API and CLI |
| `mutation_error.go` | Typed errors for mutations |
| `resolve.go` | Resolve names and ID prefixes |
| `network_status.go` | Low-cardinality network and session counters |
| `status.go` | Status snapshot and rejected connection history |

Core does not encode JSON. `internal/api` converts core values to JSON.

### `internal/api` and `internal/webui`

| File | Purpose |
|---|---|
| `api/server.go` | Listener, routes, API token check, `/ui-token`, and HTTP helpers |
| `api/handlers.go` | One handler for each API operation |
| `api/dto.go` | API JSON types and converters |
| `webui/embed.go` | Embedded static web files |

The API checks `X-Syncat-Token` with a constant-time comparison. `/ui-token`
is the only unauthenticated route. It checks the remote address, `Host`, and
`Origin` before it returns a token to the web UI.

### `cmd/syncat`

| File | Purpose |
|---|---|
| `main.go` | Global flags, command table, and usage |
| `client.go` | API client and response types |
| `cmd_node.go` | `init`, `token`, `config`, `daemon`, and `status` |
| `cmd_peer.go` | `peer`, `remote`, and `approvals` commands |
| `cmd_share.go` | `share`, `subscription`, and `trash` commands |

Only `init`, `token`, and `config` work without the daemon. Other commands use
the API. This gives SQLite one process owner.

## Main flows

### Start a daemon

`startDaemon` does these operations:

1. Load config and keys.
2. Bind the loopback API listener. This finds an address conflict early.
3. Create the tailcat transport.
4. Call `core.Open`.

`core.Open` does these operations:

1. Load configuration, identity, clock, random source, and logger.
2. Open the SQLite store. Create the trash and start its cleanup timer.
3. Start the transport accept function.
4. Make the local `sc1` token.
5. Start a watcher and initial scan for each active share and subscription.
6. Start one peer supervisor for each configured peer.

On a start failure, cleanup closes the store and transport. On shutdown, the
node cancels work, waits for tracked goroutines, then closes watchers, trash,
store, and transport.

### Connect to a peer

Each configured peer has a supervisor. It dials with exponential backoff.
Inbound and outbound connections use the same process:

1. Run the ordered `Hello`, `HelloAuth`, `Auth`, and `Finished` handshake.
2. Verify the peer key and protocol version.
3. Store a valid unknown peer as pending, then reject its connection.
4. Use `KeepConnection` to remove a duplicate connection.
5. Start a session and keepalive loop for the surviving connection.
6. Send the share list and subscription requests.
7. Grant, deny, revoke, or queue share access. A protected share queues a
   request until an operator decides it.
8. Activate granted shares. Start their index synchronization.

When a session ends, core cancels its work, waits for keepalive, closes the
session and connection, discards the transport peer state, and starts a new
dial attempt.

### Scan a local change

```text
fsnotify event -> debounce -> rescanShareAsync
                                 |
Scanner.Scan -> ScanResult ------+
                                 |
Bump version -> Store.ApplyScanResult -> propagateShare
                                              |
                                              v
                                  Session.SyncShare -> index request
                                                    -> snapshot or deltas
```

Content changes bump a version vector. Metadata-only changes do not bump it.
A periodic scan uses the same path. It finds changes that the watcher missed.

### Receive an index stream

The session read loop is the only reader of its connection. It validates each
message, checks share access, and starts bounded work.

1. A peer sends `IndexSyncRequest`.
2. syncat sends contiguous journal deltas if the peer cursor is valid.
3. Otherwise, syncat sends a staged full snapshot.
4. The receiver commits the snapshot or deltas and sends `IndexAck`.
5. The sender accepts an acknowledgement only for index data that it wrote.
6. The session reconciles local and peer file rows.
7. It applies required actions. A pull uses `FileRequest`, `FileChunk`, a hash
   check, and an atomic rename.
8. It requests another index stream when local work changed the share.

Repeated index work for one share replaces pending work. It does not create an
unbounded queue.

### Change config through the API

```text
syncat share add PATH --name N
  -> POST /api/shares
  -> core.AddShare
  -> clone config, change it, validate it, save it, and swap it
  -> start live effects after the config lock is released
```

Core returns typed mutation errors. API handlers map these errors to HTTP
status values. The API does not use `internal/sync` types as response values.

## Concurrency rules

- The transport has one accept loop. The node tracks background goroutines.
- A peer has one dial supervisor. A live connection has a session reader,
  writer, keepalive loop, and connection owner.
- A session has bounded index workers, pull workers, and serve workers.
- The index queue coalesces repeated work by share.
- The read loop does not wait for work that needs new input from the read loop.
- `cfgMu` is not held when code takes a `peerConn` lock.
- The stream writer is the only writer for a session connection.
- Test clocks let tests control time without waiting for wall-clock timers.

## Security boundaries

### Tailcat admission

An `sc1` token contains a tailcat address. The address contains the tailcat
server public key, discovery data, DERP region, and WireGuard pre-shared key.
The token is a bearer credential for the tailcat transport.

`TailcatTransport` does not set `tailcat.Server.AllowedClients`. The server
therefore accepts an ephemeral tailcat client key when the client has the
server address and pre-shared key. This admission occurs before the syncat
Ed25519 handshake.

The tailcat server has a narrow data path:

- `OnTCP` returns a handler only for TCP port 4197.
- `ServedTCPPorts` applies a second restriction to TCP port 4197.
- The server does not configure a UDP handler.
- The server does not configure TCP or UDP forwarding.

Tailcat uses a userspace network stack for this connection. This configuration
does not create a host network interface. It does not route traffic to the
host network or local network. For application traffic, a token holder can
reach only the syncat service on TCP port 4197.

Tailcat v0.6.0 keeps each admitted ephemeral client key in the server client
map until the server stops. A token holder can create multiple client keys.
Each new key increases transport state and rebuilds the tailcat network map.
This behavior creates a resource-exhaustion risk before syncat authentication.

### Application authorization

The syncat handshake verifies both Ed25519 identities after tailcat creates the
TCP connection. The received token identity must match the public key in the
handshake. The public key must also identify a configured peer.

A valid unknown peer becomes a pending approval. It does not get a session.
The node limits the total number of concurrent application handshakes. It also
limits concurrent handshakes for each known peer. These limits do not limit all
work in the tailcat transport.

The signed handshake data is not bound to the tailcat connection or its client
key. An attacker can first replace a configured peer token with a token for an
attacker-controlled tailcat server. The attacker can then relay the signed
handshake to the expected peer on a second tailcat connection. The peers
authenticate each other, but the attacker terminates both encrypted
connections. The attacker can read or change the application traffic. Section
11 of SPEC.md records future work for this boundary.

### Compromise impact

A stolen node token permits tailcat admission to the node that issued the
token. It does not provide the Ed25519 private key that authorizes a syncat
session. It still exposes tailcat, frame decoding, and handshake processing to
the token holder.

A complete copy of a node state includes `identity.key` and all configured peer
tokens. An attacker can use the identity key to impersonate that node to peers
that still trust it. The attacker can use existing share grants. The attacker
can also request access to shares that do not require approval.

This access applies only to direct syncat peers. The transport does not provide
general access to other hosts or services. A separate compromise of the host
operating system can provide more access than syncat provides.

The REST API is a local control interface. It accepts loopback connections
only, and API requests need `X-Syncat-Token`. This design protects against
remote API clients. It does not treat another process with the same user
identity as hostile.

### File-system safeguards

- Every peer path passes through `ValidateRelPath` and `JoinSharePath`.
- A remote delete uses the trash before it changes the local path.
- A local delete does not use the trash.
- The scanner does not follow symbolic links.

## Data on disk

syncat does not encrypt data at rest. Encoding data as hexadecimal, JSON, CBOR,
or base64 does not encrypt the data. Files that contain credentials have mode
`0600`. Directories that syncat creates have mode `0700`.

| Path | Contents | Confidentiality impact |
|---|---|---|
| `~/.config/syncat/config.json` | Configuration and peer or pending-peer tokens | Each token gives tailcat admission to the node that issued it. |
| `~/.config/syncat/api.token` | REST API token | A local process can control the daemon when it can also reach the loopback API. |
| `~/.local/share/syncat/keys/identity.key` | Ed25519 private identity key | The key can impersonate this syncat node. |
| `~/.local/share/syncat/keys/tailcat.key` | Tailcat private key and WireGuard pre-shared key | The key can reproduce this node's tailcat server identity and address. |
| `~/.local/share/syncat/db/index.db` | File metadata, hashes, peer state, and journal data | The database can disclose names and synchronization history. |
| `~/.local/share/syncat/trash/...` | File content that syncat replaced or deleted | The trash can contain sensitive historical content. |

The file modes prevent access by other unprivileged users when ownership and
directory permissions are correct. They do not prevent access by the same
user or by root. Full-disk encryption can protect a powered-off device. It
does not protect data after the operating system or daemon decrypts it.

Config writes use a temporary file and rename. The index key is `(share_id,
relpath)`. Do not offer a subscription directory as a share. The two share IDs
would cause repeated synchronization.

## Tests

- Each package has unit tests near its source files.
- `PipeTransport` supports in-process connection tests.
- Protocol tests include decoder fuzzing.
- Sync tests include adversarial, concurrency, and integration cases.
- API tests use HTTP handlers.
- `scripts/e2e.sh` tests two live daemons through tailcat.
- `scripts/run-net-tests.sh` tests live network failure cases.

Run these checks:

```bash
gofmt -l .
go vet ./...
CGO_ENABLED=0 go build ./...
go test -count=1 ./...
go test -race ./internal/core ./internal/sync ./internal/protocol
scripts/e2e.sh
scripts/run-net-tests.sh
```

Some environments need `TS_PERMIT_TOOLCHAIN_MISMATCH=1` for tailcat tests.

## Where to look

| Task | Start here |
|---|---|
| Simultaneous dial rule | `transport.KeepConnection`, then `core/peer.go` |
| File conflict | `sync/reconcile.go` |
| Receive-only overwrite | `sync/reconcile.go`, then `sync/apply.go` |
| Reconnect problem | `core/peer.go` |
| CLI API request | `cmd/syncat/cmd_*.go`, then `api/handlers.go` |
| API response | `api/dto.go` |
| Name or ID resolution | `core/resolve.go` |
| Share access or approval | `core/access.go`, `core/approvals.go`, `core/mutations.go` |
| Peer path check | `sync/path.go` |
| Trash retention | `sync/trash.go` |
| Wire format | `protocol/message.go` |
| Web UI token check | `api/server.go`, `webui/static/app.js` |
