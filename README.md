# syncat

syncat keeps directories synchronized between trusted peers. It is a single
binary that uses [tailcat](https://github.com/tailscale/tailcat) for encrypted
peer-to-peer connections. It needs no Tailscale account, server, control plane,
root access, or inbound firewall port.

You add peers by exchanging tokens. They connect directly when possible and
use a Tailscale DERP relay when a direct path is unavailable.

Run it on your own computers or with a small group of people you trust. It is
not a public file-sharing service: a peer with access to a read-write share can
change its contents.

`syncat daemon` maintains a node identity, connects to configured peers,
watches directories, and serves the local web UI. The other CLI commands
configure or inspect that daemon.

## Install and start

Building from this checkout requires Go 1.27.1 or newer; the build does not
use cgo.

```bash
CGO_ENABLED=0 go build -o syncat ./cmd/syncat
install -Dm755 syncat ~/.local/bin/syncat

syncat init --name my-laptop
syncat daemon
```

Ensure `~/.local/bin` is on your `PATH`, or invoke the installed binary by its
full path.

The daemon stays in the foreground, logs to standard error, and stops cleanly
on `SIGINT` and `SIGTERM`. Open <http://127.0.0.1:8347> in a browser to use
the web UI. Keep the daemon running while using daemon-backed CLI commands.

Run `syncat -h` for command usage and `syncat version` (or
`syncat --version`) for the binary version.

## First sync: connect two nodes

Every node has a secret connection token. Pairing is mutual: each node must
add the other node's token before normal synchronization begins. Exchange
tokens through an encrypted channel or in person.

This example runs Alice and Bob on one computer, using separate state
directories and local web UI ports. On separate machines, omit the
`--config`, `--data`, and alternate `--api` options.

```bash
# Terminal 1: Alice
syncat --config ~/.alice-cfg --data ~/.alice-data init --name alice
syncat --config ~/.alice-cfg --data ~/.alice-data daemon --api 127.0.0.1:18347
```

```bash
# Terminal 2: Bob
syncat --config ~/.bob-cfg --data ~/.bob-data init --name bob
syncat --config ~/.bob-cfg --data ~/.bob-data daemon --api 127.0.0.1:18348
```

In another terminal, get and exchange the tokens, then add the peer on both
nodes:

```bash
ALICE_TOKEN=$(syncat --config ~/.alice-cfg --data ~/.alice-data token)
BOB_TOKEN=$(syncat --config ~/.bob-cfg --data ~/.bob-data token)

syncat --config ~/.alice-cfg --data ~/.alice-data peer add "$BOB_TOKEN" --name bob
syncat --config ~/.bob-cfg --data ~/.bob-data peer add "$ALICE_TOKEN" --name alice

syncat --config ~/.alice-cfg --data ~/.alice-data status --watch
```

After the peers connect, Alice offers a directory and Bob subscribes to it:

```bash
syncat --config ~/.alice-cfg --data ~/.alice-data \
  share add ~/Documents --name docs --perm rw

syncat --config ~/.bob-cfg --data ~/.bob-data remote ls
syncat --config ~/.bob-cfg --data ~/.bob-data \
  subscription add alice docs ~/docs-from-alice --mode mirror
```

The peer and share arguments accept a display name, a full ID, or a unique ID
prefix. `remote ls` shows the names and IDs available to subscribe to. A file
created in Alice's `~/Documents` now appears in Bob's `~/docs-from-alice`.

`--api` applies only to that daemon process. The CLI otherwise reads
`api_addr` from its `config.json`, so always use the same `--config` and
`--data` pair when managing a test node.

## How syncat behaves

### Shares and subscriptions

A **share** is a local directory you offer. A **subscription** is a local
directory that receives a peer's share. Their permission and mode determine
which side may send changes:

| Share permission | Subscription mode | Result |
| --- | --- | --- |
| `read-write` (`rw`) | `mirror` | Two-way synchronization. |
| `read-write` (`rw`) | `receive-only` (`receive`) | The subscriber receives changes but never sends them back. |
| `read-only` (`ro`) | either mode | The share owner is the source of truth; subscriber changes are not sent. |

Use a read-only share whenever the other person should not change your source
directory. A receive-only subscription is useful for a backup or a local copy
that must not publish edits. Local edits in a receive-only copy are reported
as warnings and can be replaced by a later source update.

Subscription and share paths must not overlap: a directory received from a
peer cannot be offered back as a new local share. Removing a share or a
subscription stops synchronization but deliberately leaves existing files on
disk.

### Approvals and access

Adding both tokens is normally all that pairing requires. If an unknown node
contacts this node with a valid token, syncat records it as a pending peer
instead of accepting it. Review it with:

```bash
syncat approvals
syncat approvals grant peer-<peer-key>
syncat approvals deny peer-<peer-key>
# Equivalent peer-only approval:
syncat peer approve peer-<peer-key>
```

Add `--approval` when creating a sensitive share to require a separate access
decision for each subscription request:

```bash
syncat share add ~/Documents --name docs --perm rw --approval
syncat approvals
syncat approvals grant share-<share-id>-<peer-key>
```

You can later grant, deny, or revoke a peer's access from the web UI. Revoking
access stops future synchronization; it does not remove the peer's existing
local copy.

### Deletes, conflicts, and trash

When syncat applies a remote delete or overwrites an existing local file, it
first moves the replaced content to its per-share trash. Local deletions made
by you are not put in that trash. List and restore remote changes with:

```bash
syncat trash ls docs
syncat trash restore docs path/inside/the/share.txt
```

Restoring creates a new local change, so it can synchronize to peers. Trash
entries are retained for 30 days by default.

If peers edit the same file while disconnected, syncat keeps both versions.
One becomes the ordinary filename and the other is stored alongside it as a
file named like `report.sync-conflict-YYYYMMDD-HHMMSS-<node-id>.txt`.

syncat watches directories and also rescans them periodically. It does not
follow or synchronize symbolic links in this release.

### Ignore files

Put a `.syncatignore` file at a share root to exclude paths from that share.
It uses familiar gitignore-style rules, including comments, `!` negation,
anchored paths, directory rules, `*`, `?`, character classes, and `**`.

```gitignore
# Ignore editor and build output
*.swp
/build/
node_modules/

# Keep this generated file
!build/keep.txt
```

Only the root `.syncatignore` is used; nested ignore files are not read. The
ignore file itself is never synchronized, so each node can choose its own
rules. Ignoring a path stops it from being advertised or received; it does not
delete existing copies on peers.

## Use the web UI

Open the daemon's configured local address (normally
<http://127.0.0.1:8347>). The UI polls the local daemon every two seconds and
has two views.

- **Dashboard** lets you rename the node, copy its token, add a peer, inspect
  connection errors and available remote shares, subscribe to a remote share,
  and remove a peer. Expand a peer to see its relationships and connection
  details.
- **Shares** lets you add, rename, change the permission or approval setting,
  and remove local shares. It also provides per-peer Grant/Deny/Revoke
  controls, pause/remove controls for subscriptions, and a trash browser with
  Restore actions.

Use the CLI for configuration settings and detailed status output; the web UI
does not have a Settings page in this release.

## CLI reference

Global options go before the command:

```text
syncat [--config DIR] [--data DIR] COMMAND [ARGS...]
```

`--config` and `--data` choose alternative state directories. They are useful
for test nodes and are required consistently for every command that manages
one of those nodes.

| Command | What it does |
| --- | --- |
| `init [--name NAME]` | Create missing configuration, keys, and API credentials. It is safe to run again. |
| `init --reset [--yes] [--name NAME]` | Delete this node's state and create a new identity after confirmation. |
| `daemon [--api ADDR]` | Run the node and local web UI. The address must be loopback. |
| `token` | Print this node's secret connection token. |
| `status [--watch] [--json]` | Show node, peer, share, subscription, and warning status; `--watch` refreshes every two seconds. |
| `peer add TOKEN [--name NAME]` | Add a peer's token. Pairing must be completed on both peers. |
| `peer ls [--json]` | List configured and pending peers. |
| `peer rm ID` | Remove a peer and stop its synchronization. |
| `peer approve ID` | Accept a pending peer. |
| `share add PATH --name NAME [--perm ro\|rw] [--approval]` | Offer a directory. |
| `share ls [--json]` | List local shares and peer access. |
| `share set ID [--name NAME] [--perm ro\|rw] [--approval\|--no-approval]` | Change share metadata or its approval requirement. |
| `share rm ID` | Stop offering a directory without deleting its files. |
| `remote ls [--json]` | List shares offered by configured peers and their access status. |
| `subscription add PEER SHARE LOCALPATH [--mode mirror\|receive]` | Receive a peer's share into a local directory. |
| `subscription ls [--json]` | List subscriptions, connection state, access, and warnings. |
| `subscription pause PEER SHARE` / `resume PEER SHARE` | Temporarily stop or restart a subscription. |
| `subscription rm PEER SHARE` | Stop a subscription; local files remain. |
| `approvals` | List pending peer and share-access requests. |
| `approvals grant ID` / `deny ID` | Decide a pending peer or share-access request. |
| `trash ls SHARE [--json]` | List files syncat placed in a share's trash. |
| `trash restore SHARE PATH` | Restore a share-relative trashed path. |
| `config set FIELD VALUE` | Change a supported scalar configuration value. |
| `version` | Print the syncat version. |

Commands that talk to the daemon are `peer`, `status`, `share`, `remote`,
`subscription`, `approvals`, and `trash`. Start `syncat daemon` first. `init`,
`token`, `config`, and `version` work without a running daemon.

## Configuration and state

By default, syncat follows the XDG base-directory convention:

```text
$XDG_CONFIG_HOME/syncat/ or ~/.config/syncat/
  config.json                 # node name, settings, peers, shares, subscriptions
  api.token                   # local API credential

$XDG_DATA_HOME/syncat/ or ~/.local/share/syncat/
  keys/identity.key
  keys/tailcat.key
  db/index.db
  trash/
```

The generated configuration and key files contain secrets. syncat creates its
directories with mode `0700` and its credentials with mode `0600`.

Stop the daemon, use `config set` for these scalar fields, then start it again
so the new setting is loaded:

```bash
syncat config set node_name work-laptop
syncat config set api_addr 127.0.0.1:9347
syncat config set trash_retention_days 14
syncat config set rescan_interval_seconds 120
syncat config set debug true
```

`global_ignores` is an optional JSON array in `config.json` for patterns that
apply to every share. Stop the daemon before editing `config.json` directly,
then restart it. Prefer a share-root `.syncatignore` when the rule applies to
only one directory.

## Reset, service operation, and upgrades

`syncat init` is idempotent. Use `syncat init --reset` only when you intend to
make a completely new node. Stop the daemon first. Reset deletes `config.json`,
the local API credential, keys, index, and trash, but leaves share directories
and subscription copies in place. It changes the node identity and token, so
remove the old peer and pair the new one again on every other node. Restore any
needed trash files before resetting.

For a user-level systemd service, install and initialize the binary, then
create `~/.config/systemd/user/syncat.service`:

```ini
[Unit]
Description=syncat peer-to-peer directory sync
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=%h/.local/bin/syncat daemon
Restart=on-failure
RestartSec=5s
TimeoutStopSec=30s
NoNewPrivileges=yes
PrivateTmp=yes

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now syncat.service
journalctl --user -u syncat.service -f
```

Do not use `ProtectHome=yes`: the service must access the directories it
synchronizes. For a system-wide service, use explicit `--config` and `--data`
paths and run `init`, reset, and daemon commands as the service account.

To upgrade, build and install a replacement binary, then restart the service
or daemon. The index migrates forward when the daemon starts; back up
`index.db`, `index.db-wal`, and `index.db-shm` before rolling back across a
database change. Upgrade all connected nodes together when a release changes
the protocol or token compatibility requirements.

## Security and current limits

Treat every node token as a secret. It contains the information needed to
reach the encrypted transport, so do not put it in logs, issue trackers, or
unencrypted backups. The application still requires the peer's configured
identity before granting a sync session.

syncat does not encrypt its configuration, keys, index, trash, or synchronized
files at rest. Use full-disk encryption and protect backups. Use read-only
shares and approval-required shares to limit access, and remove a compromised
peer from every remaining node before resetting it.

Current limits include no transfer resume, no per-transfer progress display,
no mobile bindings, no block-level delta transfer, no settings page, and no
event stream. See [MANUAL-TESTS.md](MANUAL-TESTS.md) for real-world test
coverage and [ARCHITECTURE.md](ARCHITECTURE.md) for implementation details.

## Development

```bash
CGO_ENABLED=0 go build ./...
go vet ./...
go test -count=1 ./...
go test -race ./...
scripts/e2e.sh
scripts/run-net-tests.sh
```
