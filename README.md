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

Requires Go 1.27.1+ (tailcat 0.6.0's minimum). The build is cgo-free (pure-Go
SQLite, see [Development](#development)):

```bash
CGO_ENABLED=0 go build -o syncat ./cmd/syncat
```

Run one node:

```bash
./syncat init --name my-laptop
./syncat daemon &
```

Open `http://127.0.0.1:8347` for the web UI, or drive it from the CLI — see
`./syncat -h` for the full subcommand list, one per REST endpoint.

### Versions

This is syncat **0.3**. `syncat version` (or `syncat --version`) prints it,
`GET /api/status` returns it as `version`, and the web UI shows it beside the
brand in the header. The commit is appended when the toolchain stamped one in,
so a build from a git checkout reports something like `0.3+c64ce12`, with
`.dirty` on the end if the tree had uncommitted changes. Nothing needs passing
at build time; `go build` records it on its own.

The UI's copy comes from the daemon's own `/api/status`, not from the bundled
assets, so what the header shows is the build actually serving the page.

This number is the *product* version and nothing else. syncat has three other
version numbers, each independent of it and of each other: `proto_version` in
the peer handshake, the `sc1` prefix on node tokens, and the index's
`PRAGMA user_version`. Bumping 0.3 says nothing about any of them.

### Two-node walkthrough

This is the flow `scripts/e2e.sh` automates end-to-end; here it is by hand,
using `--config`/`--data` overrides so two nodes can run on one machine.

```bash
# --- terminal 1: alice -------------------------------------------------
./syncat --config ~/.alice-cfg --data ~/.alice-data init --name alice
./syncat --config ~/.alice-cfg --data ~/.alice-data daemon --api 127.0.0.1:18347 &

# --- terminal 2: bob ----------------------------------------------------
./syncat --config ~/.bob-cfg --data ~/.bob-data init --name bob
./syncat --config ~/.bob-cfg --data ~/.bob-data daemon --api 127.0.0.1:18348 &
```

`--api` on `daemon` only sets that process's listen address; the CLI reads
`api_addr` back out of `config.json`, so for two nodes on one machine either
edit `config.json`'s `api_addr` before starting the daemon, or pass
`--api` and always talk to that node's CLI with a matching `$CONFIG`/`$DATA`
pair (as above).

```bash
# Exchange tokens and peer — both directions, since peering is mutual:
ALICE_TOKEN=$(./syncat --config ~/.alice-cfg --data ~/.alice-data token)
BOB_TOKEN=$(./syncat --config ~/.bob-cfg --data ~/.bob-data token)
./syncat --config ~/.bob-cfg --data ~/.bob-data peer add "$ALICE_TOKEN" --name alice
./syncat --config ~/.alice-cfg --data ~/.alice-data peer add "$BOB_TOKEN" --name bob

# Watch them connect (DERP relay setup can take a few seconds):
./syncat --config ~/.alice-cfg --data ~/.alice-data status --watch

# Alice offers a share, bob subscribes to it:
./syncat --config ~/.alice-cfg --data ~/.alice-data share add ~/Documents --name docs --perm rw
./syncat --config ~/.bob-cfg --data ~/.bob-data remote ls
./syncat --config ~/.bob-cfg --data ~/.bob-data subscription add <alice-name-or-id> <share-name-or-id> ~/docs-from-alice --mode mirror
```

Write a file under `~/Documents` on alice's side and it appears under
`~/docs-from-alice` on bob's — and, since the share is `read-write` and the
subscription is `mirror`, changes flow the other way too.

### Starting a node over

`syncat init` is idempotent — run it twice and the second run regenerates
nothing. When you actually want the opposite, `--reset` deletes this node's
entire local state and re-initializes from scratch:

```console
$ syncat init --reset --name my-laptop
This will PERMANENTLY delete:
  /home/u/.config/syncat/config.json
  /home/u/.config/syncat/api.token
  /home/u/.local/share/syncat/keys
  /home/u/.local/share/syncat/db
  /home/u/.local/share/syncat/trash

Your files are left in place:
  /home/u/Documents

This node gets a new identity key, so its token changes and every
peer will have to add it again. The old identity cannot be recovered.

Type RESET to confirm:
```

That is the whole list — the identity and tailcat keys, the API token,
`config.json` (so peers, shares and subscriptions go with it), the SQLite
index, and the trash can. Nothing outside those paths is touched: every
share directory and every subscription's local copy is left exactly as it
is, which is why the prompt names them back to you rather than just
promising it.

Two consequences worth being sure about before you type `RESET`:

- **The node's identity changes.** `identity.key` is what a peer knows you
  by, so a reset makes this a different node as far as every peer is
  concerned. You need a fresh `syncat token` and each peer has to
  `peer add` it again; removing the stale entry on their side is on them.
- **The trash can goes with it.** `~/.local/share/syncat/trash/` holds the
  bytes of files deleted from your shares, and those copies are the only
  ones left. Restore anything you still want (`syncat trash restore`)
  before resetting.

Stop the daemon first — `init --reset` refuses to run while anything is
listening on the node's `api_addr`, because deleting `index.db` out from
under a live daemon corrupts its state silently rather than loudly.

On a system-wide install, run it as the service user, the same as every
other CLI call (see [System-wide service](#system-wide-service)). A reset
recreates the files as whoever runs it, so doing it as root would leave
root-owned state that a daemon running as `syncat` can't read — it would
fail to start, every five seconds. `init --reset` checks for that and
refuses, naming the owner and the command to rerun, but the short version
is:

```bash
sudo systemctl stop syncat.service
sudo -u syncat /usr/local/bin/syncat --config /etc/syncat --data /var/lib/syncat init --reset
sudo systemctl start syncat.service
```

Pass `--yes` to skip the prompt in a script. Without it the reset needs a
terminal to ask on and fails rather than reading a redirect, so
`init --reset < /dev/null` can't quietly destroy a node.

## Running as a systemd service

`syncat daemon` is a plain foreground process: it logs to stderr, never forks,
writes no PID file, and shuts down cleanly on SIGINT/SIGTERM
(`cmd/syncat/cmd_node.go`). That is exactly what `Type=simple` wants, so the
unit files below are short.

**Prefer a user service.** syncat's whole on-disk layout is per-user — config
and the API token under `$XDG_CONFIG_HOME/syncat`, keys, index, and trash under
`$XDG_DATA_HOME/syncat`, and the directories you actually share are normally
inside your home directory. A `systemctl --user` unit inherits the right `$HOME`
and the right file ownership for free; a system unit has to be told all of it.

### User service (recommended)

Build once and install the binary somewhere stable, outside the checkout — the
unit will keep executing whatever path you point it at, and you don't want that
to be a build tree you might `git clean`:

```bash
CGO_ENABLED=0 go build -o syncat ./cmd/syncat
install -Dm755 syncat ~/.local/bin/syncat
~/.local/bin/syncat init --name my-laptop     # once, if you haven't already
```

`~/.config/systemd/user/syncat.service`:

```ini
[Unit]
Description=syncat peer-to-peer directory sync
Documentation=https://github.com/nickmarrone/syncat
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=%h/.local/bin/syncat daemon
Restart=on-failure
RestartSec=5s

# SIGTERM drains in-flight API requests within shutdownGrace (15s) and then
# waits for the node's background goroutines; leave room for both.
TimeoutStopSec=30s

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now syncat.service
systemctl --user status syncat.service
journalctl --user -u syncat.service -f      # the daemon's stderr log
```

By default a user manager exits with your last session, which would stop syncing
the moment you log out. Enable lingering so the node runs from boot regardless:

```bash
loginctl enable-linger "$USER"
```

Things worth knowing before you tune the unit:

- **Don't put `--api` in `ExecStart`.** It only changes what that *process*
  listens on; every CLI invocation reads `api_addr` back out of `config.json`
  and would then talk to the wrong port. To move the API, `syncat config set
  api_addr 127.0.0.1:9000` and restart the unit.
- **Hardening that hides `$HOME` breaks syncat.** `ProtectHome=yes` (or
  `read-only`) makes shares unreadable or unwritable, and a `ProtectSystem=strict`
  unit needs every share, subscription, config, and data path listed in
  `ReadWritePaths=`. `NoNewPrivileges=yes` and `PrivateTmp=yes` are free — syncat
  needs no privileges and no shared tmp. The API is loopback-only regardless
  (`api.ListenLoopback` refuses to bind anything else).
- **inotify limits.** The watcher registers a watch per directory, recursively,
  across every share and subscription (`internal/index/watcher.go`). Big trees
  can exhaust `fs.inotify.max_user_watches`; if the journal shows watch
  registration failing, raise it with a `sysctl` drop-in. Missed events are still
  picked up by the periodic rescan (`rescan_interval_seconds`), just late.
- **No inbound firewall ports.** tailcat only dials out — directly where NAT
  traversal succeeds, otherwise relayed over DERP. Port 4197 lives *inside* the
  tunnel, not on the host.

### System-wide service

Worth it only for a machine-scoped node — a NAS, a backup box, anything with no
interactive user to log in. Run it as a dedicated unprivileged account (syncat
never needs root) and be explicit about paths, since the defaults are derived
from `$HOME`:

`/etc/systemd/system/syncat.service`:

```ini
[Unit]
Description=syncat peer-to-peer directory sync
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=syncat
Group=syncat
ConfigurationDirectory=syncat
ConfigurationDirectoryMode=0700
StateDirectory=syncat
StateDirectoryMode=0700
Environment=HOME=/var/lib/syncat
ExecStart=/usr/local/bin/syncat --config /etc/syncat --data /var/lib/syncat daemon
Restart=on-failure
RestartSec=5s
TimeoutStopSec=30s
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ReadWritePaths=/etc/syncat /var/lib/syncat /srv/shared

[Install]
WantedBy=multi-user.target
```

`syncat init` and every later CLI call must run as that same user with the same
overrides — the client reads `api.token` (mode 0600) out of the config dir, so
running as yourself gets you a permission error, not a different node:

```bash
sudo -u syncat /usr/local/bin/syncat --config /etc/syncat --data /var/lib/syncat init --name nas
sudo systemctl enable --now syncat.service
sudo -u syncat /usr/local/bin/syncat --config /etc/syncat --data /var/lib/syncat status
journalctl -u syncat.service -f
```

A shell alias or a two-line wrapper script carrying `--config`/`--data` saves a
lot of typing here.

### Upgrades

syncat is a single static binary plus on-disk state that migrates itself
forward, so an upgrade is: build, swap the binary, restart the unit.

```bash
git pull
CGO_ENABLED=0 go build -o syncat ./cmd/syncat
install -Dm755 syncat ~/.local/bin/syncat
systemctl --user restart syncat.service
journalctl --user -u syncat.service -n 30
```

Replace the binary by *swapping the file*, not writing through it — `install`
(or `mv` from a temp name in the same directory) puts a new inode in place,
whereas `cp` writes into the inode the running daemon is executing, which Linux
refuses with `ETXTBSY`. Swapping also means you can stage the new binary at any
time and choose the restart window separately; the running process holds its old
inode until it exits.

What the restart actually does, and what it can't undo:

- **The index migrates itself, forward only.** The SQLite index records its
  schema version in `PRAGMA user_version`, and the daemon runs any pending
  migration steps at open, one transaction per step
  (`internal/index/store.go`) — there is nothing to run by hand. There are no
  down migrations, though: once a newer binary has migrated the index, an older
  binary refuses to open it with `database schema version N is newer than this
  binary supports (M)`. If you might roll back across a schema bump, stop the
  unit and copy `index.db`, `index.db-wal`, and `index.db-shm` out of
  `~/.local/share/syncat/db/` first.
- **`config.json` upgrades silently.** It's read at startup and missing fields
  are filled in by `ApplyDefaults` (`internal/config/config.go`), so a config
  written by an older build just works. The reverse is lossy: an older binary
  ignores keys it doesn't know, and the next config mutation rewrites the file
  without them.
- **Protocol version is lockstep across peers.** This build speaks
  `proto_version` 2 and both offers and requires it — `Config.MinVersion`
  defaults to `CurrentProtoVersion` (`internal/protocol/handshake.go`) — so
  across a protocol bump the two sides reject each other's handshake with
  `unsupported_proto_version` rather than negotiating down. Peers you haven't
  upgraded yet stop syncing (and log rejected handshakes) until you do, so a
  protocol bump wants a coordinated upgrade of every node. Within a single
  protocol version, mixed builds are fine and you can upgrade one node at a time.
- **The tailcat generation is lockstep too, and the upgrade to tailcat 0.6.0
  invalidates every existing token.** Two things changed inside the address the
  `sc1` token carries: tailcat 0.3.0 split path discovery from node identity
  (adding a disco key), and 0.6.0 added a WireGuard pre-shared key. Neither is
  negotiated, so a node on this build and a node on a pre-0.6.0 build cannot
  connect in *either* direction — verified by dialing all four version pairings
  over real DERP:

  | server | client | result |
  | --- | --- | --- |
  | 0.6.0 | 0.6.0 | connects |
  | 0.2.0 | 0.2.0 | connects |
  | 0.2.0 | 0.6.0 | fails fast: `legacy tailcat address lacks a separate disco key` |
  | 0.6.0 | 0.2.0 | **fails silently**: the old client drops the two fields it doesn't know, then the WireGuard handshake never completes and the dial just times out |

  The last row is the one to plan around: the un-upgraded side reports nothing
  more useful than a timeout. So upgrade every node first, then have each run
  `syncat token` and re-add the others from the freshly printed tokens — the
  old tokens in `config.json` are dead, and a node that keeps dialing one just
  retries forever.

  Since the failure looks like a timeout rather than a version complaint,
  confirm the builds by hand before hunting anything else: run `syncat version`
  on each node, or read the `version` field of `GET /api/status`. A node still
  reporting a pre-0.3 build (or no version at all, which is every build before
  this one) is the un-upgraded one. The version is *not* exchanged between
  peers, so a node cannot tell you what its peer is running — you have to ask
  each node itself.

  Your own key file migrates itself the first time anything reads it —
  whichever of `syncat token`, `syncat init`, or the daemon runs first.
  `LoadTailcatKey` backfills the disco key (derived from the node key you
  already have) and mints the pre-shared key (it can only be minted, not
  recovered), then writes both back to `keys/tailcat.key`. So the order you do
  things in doesn't matter, and `syncat token` never prints an address the
  daemon won't serve. The node key — and so the node's identity — is
  unchanged; only the address it is reachable at changes, once.
- **Restarting mid-sync is safe, just not free.** SIGTERM stops the API, then
  closes the node, which waits for its background goroutines. Peers see the
  session drop and reconnect with backoff. Transfers interrupted by the restart
  are *not* resumed — there is no resume yet (see [Deferred past the
  MVP](#deferred-past-the-mvp)) — so a file that was half-transferred starts
  over from byte 0 on reconnect. Restarting during a multi-gigabyte initial sync
  costs you that file's progress, nothing more; the index and everything already
  written to disk survive.
- **Rolling back** is the same three steps in reverse — stop the unit, put the
  old binary back, start it — and works as long as no schema migration ran. If
  one did, restore the database copy you took alongside it.
- **Editing the unit file** needs `systemctl --user daemon-reload` (or `sudo
  systemctl daemon-reload`) before the restart; replacing the binary alone does
  not.

### Reading the log

The daemon logs to stderr, which under systemd means `journalctl`. Two levels:

**On by default** — everything you need to tell a healthy node from a sick one,
quiet enough to leave running indefinitely:

- peer connect and disconnect, with how the connection arrived (dialed vs.
  accepted) and how long it lasted. A peer reconnecting every few seconds and
  one up for a week both log a single "disconnected"; the duration is what
  tells them apart.
- a connection dropped by the keepalive dead rule (SPEC.md §4) — the peer
  stopped answering entirely, as distinct from hanging up cleanly.
- dial and handshake failures, rate-limited so a peer that has been unreachable
  for a day does not fill the journal.
- trash sweeps that actually purged something, and any sweep that failed.
  Retention deletion is irreversible, so it leaves a record.
- API requests, with status and duration. Successful `GET /api/status` polls
  are skipped: the web UI polls every 2 seconds for as long as a tab is open,
  and logging those drowns everything else. A *failing* status poll is still
  logged.

**Behind `debug`** — routine activity, for working out why a share will not
converge:

```bash
syncat config set debug true
systemctl --user restart syncat.service   # picked up at startup
```

Adds per-pass reconcile summaries (`Pull=3 Delete=1`), scan results with
added/changed/deleted counts, a trail of every config mutation (peers, shares,
grants, subscriptions), and tailcat's own network chatter (netcheck reports,
link-change events). Set it back to `false` and restart when you are done.

When the daemon's stderr goes straight to the journal, it drops its own
`syncat:` prefix and timestamp, since journald already stamps both — so
`journalctl` shows one date and one program name, not two. Run from a terminal
it keeps them.

## Excluding files with `.syncatignore`

Put a `.syncatignore` at the root of a shared directory to keep matching
files out of sync. The syntax is gitignore's:

```gitignore
# comments and blank lines are skipped
*.log              # any depth
/build             # anchored to the share root
node_modules/      # directories only
docs/**/*.tmp      # ** crosses directory boundaries
!keep.log          # re-include; the last matching rule wins
```

Three things are worth knowing, because they are what people get wrong:

**It applies in both directions.** An ignored path is never sent to a peer
*and* never accepted from one. Both sides enforce their own rules, so a file
you ignore stays off your disk even if a peer is happily sharing it.

**The file itself is not synced.** Each node keeps its own `.syncatignore`,
and the two sides may legitimately disagree about what is excluded. Only the
share root's file is read — a `.syncatignore` in a subdirectory has no effect
(but is not synced either).

**Ignoring is not deleting.** Adding a rule for a file that has already synced
leaves every peer's copy exactly where it is; the file just stops being
tracked here. Remove the rule and it starts syncing again, with this node's
copy taking precedence.

Edits take effect on the next rescan — no restart. A malformed line is skipped
and logged, and the rest of the file still applies; an unreadable
`.syncatignore` keeps the rules last loaded rather than falling back to
excluding nothing. Always ignored regardless of the file: `.syncatignore`
itself, `.syncat.tmp.*` (in-flight downloads), and OS junk (`.DS_Store`,
`Thumbs.db`, `desktop.ini`) — a `!` rule cannot re-include these. The
`global_ignores` list in `config.json` applies to every share.

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

- **The node token is a secret**, and as of tailcat 0.6.0 more so than before.
  `syncat token` prints a `sc1…` blob wrapping your tailcat connection info,
  Ed25519 public key, and suggested display name. That connection info now
  carries a WireGuard pre-shared key — an independent random secret mixed into
  the handshake, which adds post-quantum confidentiality and keeps the DERP
  operator relaying your packets from being able to join the tunnel. So the
  token is no longer merely "enough to attempt a connection": it is key
  material. Treat it like a password, not a username, and prefer a channel you
  would send a password over.
- **Mutual authentication.** The tailcat tunnel gives you an encrypted pipe to
  *some* endpoint; syncat's own handshake (Ed25519 challenge/response over
  that pipe) is what proves it's the peer you actually added. An inbound
  connection from an unrecognized key is rejected and recorded, not trusted.
  Note the limit: the signed transcript covers both nonces but not the tunnel
  it travelled over, so it authenticates the peer without ruling out a relay
  by someone who tampered with a token's `tc` field before you pasted it —
  see item 2 of "Transport and protocol: planned work".
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
  `fxamacker/cbor`, `x/crypto`) lists no TOML library.
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
- **The test transport uses `net.Pipe`.** Its address registry still stands
  in for tailcat addresses. The v2 handshake assigns dialer/acceptor roles
  and alternates Hello, HelloAuth, Auth, and Finished, so every synchronous
  pipe write has a waiting reader. This also makes invalid ordering visible
  in tests instead of hiding it behind a TCP send buffer.
- **`syncat trash` now requires a running daemon.** SPEC.md's original phase
  plan had it operate directly on the on-disk trash/index, independent of a
  daemon (like `init`/`token`). Once the REST API existed, that would have
  been a second code path touching SQLite directly — and would race a live
  daemon's own index access if one happened to be running. `trash` is now an
  API client like every other subcommand (`cmd/syncat/cmd_share.go`).

## Deferred past the MVP

These are real gaps, not accidents — called out so nobody is surprised they
don't work:

- **Standalone approval queues.** An unrecognized inbound connection is
  rejected outright rather than queued (SPEC.md §2.3). Share requests with
  `approval_required` do fail closed and are recorded as pending; they can be
  granted or denied from the share's access controls in the UI/API. The
  dedicated `POST /api/peers/{id}/approve` and `GET/POST /api/approvals`
  endpoints still return 501.
- **Nested `.syncatignore` files.** Only the one at the share root is read
  (as SPEC.md §5 specifies); a `.syncatignore` in a subdirectory is ignored
  as a rules file, though it is never synced either.
- **Symlink syncing.** Symlinks are detected during scanning and skipped
  entirely, with a warning — never entering the index or syncing to peers
  (`internal/index/scanner.go`). SPEC.md §5's "symlink entry itself syncs on
  Unix" is not implemented in this build.
- **Transfer resume.** `FileRequest.offset` is always sent as 0; an
  interrupted transfer restarts from the beginning rather than resuming
  (`internal/sync/transfer.go`). The `pending_transfers` table exists in the
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

## Transport and protocol

A review of the networking layer turned up the following. The test transport
has been dealt with (see "Deviations from SPEC.md" above, and the `n.token`
race it exposed in `core.Open`). The remaining work follows the completed
item, in the order it is worth doing.

1. **Completed: keepalives no longer wedge behind blocked writes.** Session
   writes now go through one bounded, two-lane `protocol.StreamWriter` with
   control-frame priority and a per-frame write deadline. `Keepalive.Run`
   checks `Dead()` before attempting a ping, and the peer manager watches the
   session read loop directly so EOF triggers reconnect immediately instead
   of waiting for the dead timer. Silent file pulls also time out without
   occupying one of the four pull slots forever.

2. **Bind the handshake to the tunnel it runs over.** The Ed25519 handshake
   is not defence in depth over tailcat's — it is the *only* peer
   authentication we have. tailcat accepts any client holding the address
   ("until a key is allowed, all clients are allowed"), and `AddAllowedClient`
   is unavailable to us because client keys must be ephemeral for DERP
   routing (see `TailcatTransport.clientFor`). The signed transcript is
   v2 transcript binds both role-labelled Hellos but names neither endpoint's
   tailcat identity, so an attacker who alters the `tc` field of a token in
   transit — tokens are pasted over chat and email — can relay both
   handshakes between two honest nodes over two separate tunnels and sit in
   the middle of everything that follows. Fix: fold the dialed address and
   our own `Server.TailcatAddr()` into `authTranscript`, length-prefixed. That
   is a wire break, so it wants `proto_version: 2` or a transitional
   signature v1 peers still accept.

3. **Buffer the frame codec's I/O.** `Reader.ReadFrame` does two bare
   `io.ReadFull` calls straight onto tailcat's userspace socket and allocates
   a fresh body for every frame; `Writer.WriteFrame` copies the entire
   payload into a new buffer before writing, which for a 1 MiB `FileChunk` is
   a full extra copy and allocation per chunk. Fix: one `bufio.Reader` per
   session, a reusable body buffer for chunk-sized frames, and
   `net.Buffers{lenAndType, cborHeader, data}` written under the existing
   writer mutex — the mutex is what guarantees frames aren't interleaved, so
   a vectored write is just as safe as the single `Write` it replaces.

4. **Delete both `Clock` interfaces in favour of `testing/synctest`.**
   `transport.Clock`, `protocol.Clock`, `core`'s `asProtocolClock` /
   `asSyncClock` adapters, `Supervisor`'s injected `*rand.Rand`, and
   `handshake_test.go`'s `fakeClock` all exist for one reason: making timing
   fakeable. `go.mod` is on go 1.27 and `testing/synctest` has been stable
   since 1.25 — inside a bubble the real `time` package is already fake, so
   all of that collapses and the tests drive the production path instead of a
   parallel one built for them.

5. **Reconsider the hand-rolled stream multiplexer** (v2, and the structural
   answer if item 1's mitigations prove insufficient). `Session` demuxes
   interleaved `FileChunk`s by `(share_id, relpath)` into per-transfer
   channels, with a 4-pull semaphore and a documented deadlock hazard —
   `IndexUpdate` handling has to run off the read loop or a pull deadlocks
   against the very loop that would feed it. But tailcat hands us a whole
   userspace TCP stack, and `DialTCPPort` is callable more than once: a
   stream per transfer would bring per-transfer flow control, independent
   backpressure, and no head-of-line blocking of control frames behind file
   bytes, deleting the demux table and the semaphore along with them. It
   costs a stream-open token on the control stream, so a fresh stream can be
   bound to an already-authenticated peer, and it contradicts SPEC.md §4's
   single-stream framing.


## Development

```bash
CGO_ENABLED=0 go build ./...
go vet ./...
go test -count=1 ./...
go test -race ./...
go test -fuzz=FuzzDecode ./internal/protocol   # frame decoder, run for a bit then Ctrl-C
scripts/e2e.sh                                  # two real daemons over live tailcat
scripts/run-net-tests.sh                        # networking failure scenarios (~10 min)
scripts/run-net-tests.sh --soak                 # ... plus the slow ones (~30 min total)
```

`CGO_ENABLED=0` is required, not just convenient: `modernc.org/sqlite` is a
pure-Go SQLite implementation chosen specifically so `internal/index` (and
everything above it) stays gomobile-compatible (SPEC.md §12 — "no cgo, no
CLI/HTTP imports inside core packages").

`scripts/e2e.sh` builds the binary, spins up two independent daemons with
separate config/data dirs and API ports, peers them over real tailcat,
shares/subscribes a directory, and asserts bidirectional sync, delete+trash,
and a two-sided conflict all converge correctly on disk. It cleans up both
daemons on any exit path and dumps the relevant daemon state on failure.

`scripts/run-net-tests.sh` is the same machinery pointed at everything that
can go *wrong* on a network. Both it and `e2e.sh` are built on
`scripts/lib/harness.sh`, which owns the daemon lifecycle, the polling
primitives, and the assertions; the scenarios themselves live one per file
under `scripts/net/`, and any of them can be run on its own.

These exist because the Go suite, by construction, cannot see most
networking failures: it finishes in well under a minute of wall-clock time
and never kills a process. The scenarios cover one-sided peering, the
simultaneous-dial dedup race, SIGTERM and SIGKILL restarts, offline
divergence, peer removal, a live `--perm` change, and three-node fan-out.
The `--soak` tier additionally waits out real time: a peer frozen with
SIGSTOP must be declared dead by SPEC.md §4's 90s rule and recover on
SIGCONT, a connection must idle for minutes without a single reconnect or
ping timeout, and a multi-hundred-megabyte file must cross the relay without
starving the keepalive behind it.

SIGSTOP is how link failure is simulated: the frozen daemon's sockets stay
open and the kernel keeps acknowledging, but the process answers nothing,
which is exactly what a pulled cable or a suspended laptop looks like from
the other end — and it needs no root, firewall, or network namespace.

## Project layout

Nine packages, 35 source files. Every package is small enough to list in
full; [ARCHITECTURE.md](ARCHITECTURE.md) explains how they fit together and
walks the main flows.

| Package | Files |
|---|---|
| `cmd/syncat/` | `main.go` (subcommand dispatch, stdlib `flag`, no cobra) · `client.go` (REST client + response shapes) · `cmd_node.go` (`init`/`token`/`daemon`/`status`/`config`) · `cmd_peer.go` (`peer`/`remote`/`approvals`) · `cmd_share.go` (`share`/`subscription`/`trash`) |
| `internal/core/` | `node.go` (lifecycle, accept path, clock adapters) · `mutations.go` (the config-mutation API the REST layer calls) · `resolve.go` (names and id prefixes → ids) · `peer.go` (per-peer dial/dedup/keepalive state machine) · `access.go` (share-access negotiation: ShareList/SubscribeRequest/AccessUpdate) · `shares.go` (share dirs → scanner/watcher) · `status.go` (the read-only snapshot) — gomobile-safe, no UI/CLI deps |
| `internal/transport/` | `transport.go` (`Transport` interface, backoff/supervisor, dedup tie-break) · `tailcat.go` (production carrier) · `pipe.go` (`net.Pipe` transport used by every package's tests) |
| `internal/protocol/` | `message.go` (message types + frame codec) · `stream.go` (bounded, prioritized session writer) · `handshake.go` (mutual Ed25519 auth) · `keepalive.go` (Ping/Pong idle and dead timing) |
| `internal/index/` | `store.go` (SQLite schema, `files`, `peer_files`) · `scanner.go` (tree walk, hashing, ignore matching) · `ignore.go` (the `.syncatignore` gitignore-syntax matcher) · `watcher.go` (`fsnotify` + debounce + periodic rescan) |
| `internal/sync/` | `reconcile.go` (version vectors, actions, conflict naming, the reconciler — all pure, no I/O) · `session.go` (one peer connection: lifecycle, read loop, index exchange) · `transfer.go` (pulling and serving file bytes) · `apply.go` (writing results to disk) · `path.go` (the single validation gate for peer-supplied relpaths) · `trash.go` (trash can + janitor) |
| `internal/config/` | `config.go` (schema, JSON encoding, load/save, re-share guard, share ids) · `keys.go` (identity key, tailcat key, `sc1` tokens, API token) · `paths.go` (XDG layout, atomic writes) |
| `internal/api/` | `server.go` (routing, auth, `/ui-token`, HTTP helpers) · `handlers.go` (one handler per endpoint) · `dto.go` (the JSON shapes every response goes through) |
| `internal/webui/` | `embed.go` + `static/` — `go:embed`-ed single-page UI (vanilla HTML/CSS/JS, no build step) |
| `internal/version/` | `version.go` — the product version, and the commit `go build` stamped in |

Files long enough to need it open their sections with `// --- name ---`
markers, so `grep '^// --- ' internal/sync/reconcile.go` prints that file's
table of contents.
