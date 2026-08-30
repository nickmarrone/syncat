# Syncat — Peer-to-Peer Directory Sync over Tailcat

Specification for v1. This document is the source of truth for implementing the
application; follow it exactly unless it conflicts with reality (e.g. the tailcat API
differs from what's described — in that case adapt the transport wrapper and note the
deviation in the README).

## 0. Vision & constraints

Syncat is a directory-syncing tool built on
[tailcat](https://github.com/tailscale/tailcat) — Tailscale's Go library for
point-to-point WireGuard-encrypted tunnels that needs **no Tailscale account, no
control plane, no root**. Peers are added manually by pasting each other's secret
tokens; there is **no discovery**. Each node names itself, shares directories with
per-share permissions (read-only / read-write, approval-required), and connected peers
can see each other's name and offered shares.

Target use: personal machines, code/work directories, and sharing with trusted
friends/family. v1 is a Go CLI daemon serving an embedded web UI on localhost.
iOS/Android come later, so the sync core must be a reusable, cgo-free Go package
(gomobile-friendly).

Fixed decisions:
- **Sync modes (v1):** bidirectional mirror, one-way push (send-only), one-way pull (backup). No on-demand browse/fetch.
- **Conflicts:** keep both — newest wins the filename, loser saved as a conflict copy.
- **"Requires approval"** gates *peer access to a share* (not individual changes).
- **Interface:** headless CLI daemon serving a web UI on localhost; CLI subcommands drive the same REST API.
- **Safety:** simple trash can for remote deletes/overwrites, auto-cleaned after N days.
- **Transfers:** whole-file in v1 (with resume); block-level delta specified in §11 as v2.
- **UI stack:** vanilla HTML/CSS/JS embedded via `go:embed` — single binary, no build step.

## 1. Overview

Syncat is a single Go binary. `syncat daemon` runs the node: it maintains a persistent
tailcat identity, connects to configured peers, watches shared directories, syncs
changes, and serves a REST API + web UI on `127.0.0.1`. All other `syncat` subcommands
are thin clients of that REST API.

### Core concepts

- **Node** — one running syncat instance. Has a persistent keypair, a user-chosen
  display name, and a **node token** used by others to connect to it.
- **Peer** — another node you've added by pasting its token. Peering is mutual: each
  side must add (or approve) the other before any data flows.
- **Share** — a local directory a node offers, with a share name, permission
  (`read-only` | `read-write`), and an `approval_required` flag.
- **Subscription** — a peer's decision to sync one of your shares into a local
  directory of its own, with a local mode (`mirror` | `receive-only`).

### How the three sync modes emerge

Mode is not a single enum; it falls out of *offerer permission × subscriber mode*:

| Offerer permission | Subscriber mode | Effective behavior |
|---|---|---|
| read-write | mirror | **Bidirectional mirror** (both sides propagate changes) |
| read-only | receive-only (forced) | **One-way push** — offerer is source of truth |
| read-write | receive-only | **One-way pull / backup** — subscriber takes changes, never sends |

On a read-only share, subscribers are forced to receive-only; local modifications in
the subscriber's copy are detected, logged as warnings in the UI, and overwritten on
the next change from the offerer (after a trash copy is taken).

## 2. Identity, tokens, and peering

### Keys
- On first run (`syncat init`, or implicitly by the daemon) the node generates and
  persists a tailcat saved key (Curve25519, the WireGuard identity) **and** an Ed25519
  application identity keypair. Both stored under the data dir with mode `0600`.
- The tailcat key gives a stable node address; the Ed25519 key signs the application
  handshake so peers are authenticated at the syncat layer regardless of what the
  tailcat transport exposes.

### Syncat node token
The user-visible token wraps the tailcat connection blob plus app identity:

```
sc1<base64url(CBOR{
  tc:   <tailcat ConnBlob string, "tc...">,
  id:   <Ed25519 public key, 32 bytes>,
  name: <suggested display name, string>
})>
```

Prefix `sc1` versions the token format. `syncat token` prints it; the UI shows it with
a copy button. Treat it as a secret: possession lets someone *attempt* to connect
(they still land in the pending-approval list below).

### Peering flow
1. Alice pastes Bob's token (UI "Add peer" or `syncat peer add <token>`). Bob does the
   same with Alice's token. Order doesn't matter.
2. Each node runs a `tailcat.Server` at all times and also dials every configured peer
   as a `tailcat.Client` with exponential backoff (1s → 5min cap, jittered).
3. On any established tunnel, the syncat protocol handshake runs (§4). If an inbound
   connection authenticates as an Ed25519 key that is **not** a configured peer, it is
   recorded in a **pending peers** list (name + key fingerprint shown in UI) and the
   connection is closed. Approving a pending peer converts it to a configured peer —
   the handshake `Hello` includes the sender's own token for exactly this reason, so
   approval alone suffices.
4. **Connection dedup:** if both dials succeed, both sides keep the connection dialed
   by the node with the lexicographically higher Ed25519 public key and close the other.

## 3. Configuration and on-disk layout

```
~/.config/syncat/config.toml        # everything editable; daemon reloads on API writes
~/.config/syncat/api.token          # random 64-hex, 0600; REST auth
~/.local/share/syncat/
  keys/tailcat.key                  # 0600
  keys/identity.key                 # 0600
  db/index.db                       # SQLite (modernc.org/sqlite, pure Go — cgo-free for gomobile)
  trash/<share-id>/<relpath>.<unix-ts>   # trash can (§7)
```

`config.toml` holds: node name, listen address for the API (default `127.0.0.1:8347`),
trash retention days (default 30), rescan interval (default 300s), peers
(`[[peer]] name/token/enabled`), shares (`[[share]] id/name/path/permission/approval_required`,
plus `[share.access] peer-key = "granted"|"denied"`), and subscriptions
(`[[subscription]] peer/share-id/local-path/mode/paused`).

Share IDs are random 8-byte hex, generated at share creation, stable for the share's life.

## 4. Wire protocol (v1)

All syncat traffic runs over a single TCP stream inside the tailcat tunnel:
`client.DialTCPPort(ctx, 4197)`; the server side handles port 4197 in `OnTCP`.

**Framing:** every message is `[4-byte big-endian length][1-byte type][payload]`.
Control payloads are CBOR (fxamacker/cbor); `FileChunk` payloads are raw bytes
preceded by a small CBOR header (own type byte). Max frame 4 MiB.

**Handshake (mutual auth):**
1. Both sides immediately send `Hello{proto_version:1, node_name, ed25519_pub, token, nonce[32]}`.
2. Both reply `Auth{sig = Ed25519.Sign(identity_key, "syncat-auth-v1" || their_nonce || my_nonce)}`.
3. Each verifies the signature against the pubkey it has configured for this peer
   (or records a pending peer and closes, §2). `proto_version` mismatch: use
   `min(theirs, mine)` if supported, else close with `Error`.

**Messages after handshake:**

| Type | Direction | Payload |
|---|---|---|
| `ShareList` | both, on connect + on change | shares visible to this peer: `[{share_id, name, permission, approval_required, access: none\|pending\|granted}]` |
| `SubscribeRequest` | subscriber → offerer | `{share_id}` — request access; offerer auto-grants if `approval_required=false`, else queues for UI approval |
| `AccessUpdate` | offerer → subscriber | `{share_id, access: granted\|denied\|revoked}` |
| `IndexUpdate` | both (per granted share, respecting direction rules §5) | `{share_id, files: [FileInfo…], full: bool}` — full snapshot on first sync/reconnect, deltas after |
| `FileRequest` | puller → holder | `{share_id, relpath, version, offset}` — offset enables resume |
| `FileChunk` | holder → puller | header `{share_id, relpath, version, offset, eof}` + ≤1 MiB raw bytes |
| `Ping`/`Pong` | both, 30s idle | `{}` — 90s without traffic ⇒ reconnect |
| `Error` | both | `{code, msg}` |

`FileInfo`: `{relpath, type: file|dir|symlink, size, mtime_ns, mode, sha256, version: VersionVector, deleted: bool}`.
Deleted entries are tombstones, kept in the index for 180 days then purged.

Multiple transfers are interleaved on the one stream by alternating chunks across
active `FileRequest`s (max 4 concurrent pulls per peer).

## 5. Sync engine

### Index
SQLite, one database per node: `files(share_id, relpath, size, mtime_ns, mode,
sha256, version_json, deleted, updated_at)` plus `peer_files(...)` mirroring the latest
`IndexUpdate` from each peer, and `pending_transfers` for resume state.

### Scanning
- Full scan of each share on daemon start and every `rescan_interval` (default 300s).
- Real-time via `fsnotify` on the share tree (best-effort; the periodic scan is the
  source of truth — fsnotify only triggers an immediate targeted rescan of dirty paths,
  debounced 1s). Code-directory friendly: batches of events collapse into one scan.
- A file is "changed" when size or mtime differs from the index; then hash (sha256) to
  confirm. Hash-equal ⇒ metadata-only update, no transfer.
- Ignore rules: `.syncatignore` at share root (gitignore syntax, via a Go gitignore
  library); `.syncatignore` itself is synced. Always ignored: `.syncat.tmp.*`, OS junk
  (`.DS_Store`, `Thumbs.db`, `desktop.ini`), and the global ignore list in config.
- Symlinks are **not followed**; the symlink entry itself (target string) syncs on
  Unix and is skipped with a warning on Windows.

### Versioning & conflict detection
- Per-file **version vector**: map of `node-short-id → counter` (short id = first
  8 bytes of Ed25519 pub, hex). Local modification bumps the local counter.
- Remote version strictly dominates local ⇒ apply. Local dominates ⇒ ignore (peer will
  pull from us). Concurrent (neither dominates) ⇒ **conflict**:
  - Winner = larger `mtime_ns`, tie-break larger sha256.
  - Loser is written to `<name>.sync-conflict-YYYYMMDD-HHMMSS-<node-short-id><ext>`
    beside the winner; the conflict copy is a normal new file and syncs everywhere.
  - Delete-vs-modify conflict: modify wins; the file is resurrected.
- Merged vector after resolution = element-wise max + local bump.

### Applying remote changes
1. Download to `.syncat.tmp.<rand>` in the destination directory; verify sha256;
   fsync; set mtime/mode.
2. If a file is being replaced or deleted, move the old content to trash first (§7).
3. `rename(2)` into place (atomic on same filesystem).
4. Directory deletes apply only when empty after children sync; non-empty remote-deleted
   dirs containing local-only files are kept and the delete is logged.

### Direction rules
- Offerer with `read-only` share: sends `IndexUpdate`/`FileChunk`, **ignores** incoming
  `IndexUpdate` for that share.
- Subscriber in `receive-only` mode: applies remote changes, never sends `IndexUpdate`
  for that share; local edits are flagged "locally modified" in UI and reverted (via
  trash) when the offerer's copy next changes.
- Fan-out: a share offered to multiple peers propagates through the offerer (hub).
  Peers of the same share do not talk to each other in v1.

## 6. Permissions & approval

- Per share: `permission: read-only | read-write`, `approval_required: bool`.
- Peers always *see* your share list (name, permission, whether approval is needed).
  They get file data only after access is granted.
- `approval_required=false`: first `SubscribeRequest` auto-grants.
- `approval_required=true`: request lands in the UI/CLI approvals queue; offerer
  grants/denies; decision persists in config (`share.access` map) and is pushed via
  `AccessUpdate`. Access can later be revoked in the UI (peer keeps its local copy;
  sync just stops).

## 7. Trash can

- Any remote-initiated delete or overwrite moves the old file to
  `<datadir>/trash/<share-id>/<relpath>.<unix-ts>` (rename when same filesystem,
  copy+delete otherwise).
- Retention: `trash_retention_days` (default 30); a daily janitor purges older entries.
- UI: per-share trash browser with restore (restore = copy back + local version bump,
  so it propagates as a new change). CLI: `syncat trash ls|restore`.
- Local deletions by the user are **not** trashed (the OS/user owns those); only
  changes syncat itself applies on behalf of a peer are.

## 8. REST API & CLI

API on `127.0.0.1:8347`, JSON, header `X-Syncat-Token: <api.token contents>` required
on every request (CSRF protection; the web UI fetches the token via a login-less
same-origin bootstrap endpoint `GET /ui-token` that is only served to localhost).

```
GET  /api/status                 # node name, token, uptime, peer conn states, transfer stats
GET  /api/peers                  # configured + pending peers
POST /api/peers                  # {token, name?} add
POST /api/peers/{id}/approve     # approve pending
DELETE /api/peers/{id}
GET  /api/shares                 # local shares + per-peer access states
POST /api/shares                 # {path, name, permission, approval_required}
PATCH/DELETE /api/shares/{id}
GET  /api/remote-shares          # everything peers offer us, with access state
POST /api/subscriptions          # {peer, share_id, local_path, mode}
PATCH/DELETE /api/subscriptions/{id}     # pause/resume/mode/remove
GET  /api/approvals              # pending peer + share-access approvals
POST /api/approvals/{id}         # {decision: grant|deny}
GET  /api/shares/{id}/trash      # list; POST /restore
GET  /api/events                 # SSE stream: conn changes, transfers, conflicts, approvals
```

CLI (each maps 1:1 onto the API):

```
syncat init [--name NAME]           syncat share add PATH --name N [--perm ro|rw] [--approval]
syncat daemon [--api ADDR]          syncat share ls|rm|set
syncat token                        syncat remote ls            # peers' offered shares
syncat peer add TOKEN [--name N]    syncat subscribe PEER SHARE LOCALPATH [--mode mirror|receive]
syncat peer ls|rm|approve           syncat approvals [grant|deny ID]
syncat status [--watch]             syncat trash ls|restore SHARE [PATH]
```

## 9. Web UI

Embedded via `go:embed` from `internal/webui/static/` — hand-written HTML/CSS/JS, no
framework, no build step. One page app with hash routing and `fetch` + the SSE events
stream for live updates. Four views:

1. **Dashboard** — node name (editable), node token (copy button), peer cards with
   live connection state (connected / relay vs direct if tailcat exposes it /
   reconnecting), active transfer progress bars.
2. **Peers** — add-token form, pending-peer approvals, per-peer detail: their name,
   key fingerprint, shares they offer (with Subscribe buttons), shares of ours they use.
3. **Shares** — add/edit local shares (dir path, name, permission dropdown, approval
   toggle), per-share peer access list with revoke, subscriptions with mode + pause,
   trash browser, conflict list.
4. **Settings** — rescan interval, trash retention, global ignores, API address.

Style: minimal, clean, dark-mode-aware via `prefers-color-scheme`. A cat 🐱 favicon.

## 10. Project layout

```
cmd/syncat/           # main: subcommand dispatch (stdlib flag; no cobra)
internal/core/        # engine wiring: node lifecycle, peer manager  ← gomobile-safe, no UI/CLI deps
internal/transport/   # tailcat server+client wrapper, dial/backoff/dedup, Transport interface
internal/protocol/    # frame codec, message types, handshake
internal/index/       # SQLite index, scanner, fsnotify watcher, ignore matching
internal/sync/        # version vectors, reconciler, transfer manager, trash
internal/config/      # config.toml load/save, keys
internal/api/         # REST handlers + SSE
internal/webui/       # go:embed static assets
```

`internal/transport` defines a `Transport` interface (Dial/Accept returning
`net.Conn`s) with the tailcat implementation behind it — integration tests use an
in-memory `net.Pipe` implementation, and the future mobile apps reuse everything
above the interface. Dependency budget: tailcat, modernc.org/sqlite, fsnotify,
fxamacker/cbor, a gitignore matcher, x/crypto. Nothing else without cause.

## 11. Block-level delta transfer (v2 — design now, build later)

Written up so v1 choices don't paint us into a corner:

- **Chunking:** FastCDC content-defined chunking, min 64 KiB / avg 256 KiB / max 1 MiB.
  Content-defined (not fixed) so insertions don't shift every subsequent block —
  matters for code files and logs.
- **Index changes:** `files` gains `blocks_json` (ordered list of `{offset, size,
  sha256_16}` — truncated 16-byte hashes); whole-file sha256 stays as the integrity
  check. Blocks are computed during the hash pass (one read of the file).
- **Protocol:** `Hello.proto_version: 2`. `IndexUpdate.FileInfo` gains optional
  `blocks`. New `BlockRequest{share_id, relpath, version, block_hashes[]}` answered by
  `FileChunk`s tagged with block hash. v2 nodes fall back to v1 whole-file with v1 peers.
- **Reassembly:** puller builds the temp file by copying blocks it already has locally
  — from the old version of the same file *or any other indexed file with a matching
  block hash* (cheap dedupe) — then requests only missing blocks; verify whole-file
  sha256 before rename.
- **Resume** becomes per-block (the missing-block set persists in `pending_transfers`).
- v1 forward-compat obligations: version negotiation in the handshake (already
  specified), `FileInfo` encoded as CBOR maps (unknown fields ignored), sha256 as the
  end-to-end integrity check independent of transfer granularity.

## 12. Future: iOS/Android

- Everything under `internal/core` + `sync` + `index` + `protocol` + `transport`
  compiles without cgo (hence modernc sqlite) and behind interfaces for filesystem
  access — mobile filesystem sandboxes differ, so file I/O goes through an `fs.FS`-ish
  read side and a small write interface from day one.
- Bind via gomobile; native UIs replace the web UI, talking to the same core API surface.
- Not in scope for v1; the constraint it imposes now is only "no cgo, no
  CLI/HTTP imports inside core packages."

## 13. Milestones (implementation order)

1. **Skeleton** — repo, go.mod, config load/save, key generation, `init`/`token` commands.
2. **Transport** — tailcat wrapper, dial/backoff/dedup, `net.Pipe` test transport.
3. **Protocol** — framing, handshake with mutual Ed25519 auth, ping/pong. Two in-process nodes shake hands in a test.
4. **Index** — SQLite schema, scanner, hashing, fsnotify, ignore rules.
5. **Sync core** — version vectors, reconciler, whole-file transfer with resume, atomic apply, conflict copies. Integration test: two nodes, bidirectional sync incl. a conflict, over `net.Pipe`.
6. **Modes & safety** — permissions, approval flow, receive-only/read-only direction rules, trash + janitor.
7. **API + CLI** — REST endpoints, SSE, all subcommands.
8. **Web UI** — the four views against the live API.
9. **Hardening** — path traversal rejection (no `..`/absolute relpaths — verify on every incoming path), frame size limits, fuzz the frame decoder, README.

## 14. Verification

- `go test ./...` — unit tests: version-vector algebra, ignore matching, frame
  codec (incl. fuzz), scanner on temp dirs, trash janitor.
- Integration tests over the in-memory transport: full bidirectional sync, one-way
  push, backup mode, conflict copy creation, resume after interrupted transfer,
  approval grant/deny, receive-only revert.
- Manual end-to-end on one machine: two daemons with separate `--config`/data dirs and
  API ports, exchange tokens, share a directory, watch it sync through real tailcat;
  verify the web UI on both.
