# syncat

syncat synchronizes directories between trusted peers. It uses
[tailcat](https://github.com/tailscale/tailcat) for encrypted peer-to-peer
connections.

syncat does not need a Tailscale account, a control plane, root access, a
server, or a peer directory. You add a peer with its token. The peers connect
directly when possible. They use a Tailscale DERP relay when they cannot make a
direct connection.

`syncat daemon` runs one node. The node keeps its identity, connects to its
configured peers, watches directories, synchronizes changes, and provides a
REST API and web UI on `127.0.0.1`. Other `syncat` commands use that API.

Use syncat for your own computers or a small group of trusted people. Do not
use it as a public file-sharing service.

## Build and start

You need Go 1.27.1 or later. The build does not use cgo.

```bash
CGO_ENABLED=0 go build -o syncat ./cmd/syncat
./syncat init --name my-laptop
./syncat daemon &
```

Open `http://127.0.0.1:8347` to use the web UI. Run `./syncat -h` to see the
CLI commands.

## Version

This source tree reports product version `0.3`. Run `syncat version` or
`syncat --version` to get the version of a binary. `GET /api/status` returns
the same value in `version`. The web UI gets this value from the daemon.

A build from a Git checkout can add the commit ID. For example, it can report
`0.3+c64ce12`. A dirty checkout adds `.dirty`.

The product version is not a protocol or data format version. syncat also has:

- `proto_version` in the peer handshake.
- The `sc1` prefix in a node token.
- SQLite `PRAGMA user_version` for the index database.

## Connect two nodes

This example runs two nodes on one computer. It uses separate config and data
directories.

```bash
# Terminal 1: alice
./syncat --config ~/.alice-cfg --data ~/.alice-data init --name alice
./syncat --config ~/.alice-cfg --data ~/.alice-data daemon --api 127.0.0.1:18347 &

# Terminal 2: bob
./syncat --config ~/.bob-cfg --data ~/.bob-data init --name bob
./syncat --config ~/.bob-cfg --data ~/.bob-data daemon --api 127.0.0.1:18348 &

# Get and exchange tokens. Add each peer on both nodes.
ALICE_TOKEN=$(./syncat --config ~/.alice-cfg --data ~/.alice-data token)
BOB_TOKEN=$(./syncat --config ~/.bob-cfg --data ~/.bob-data token)
./syncat --config ~/.bob-cfg --data ~/.bob-data peer add "$ALICE_TOKEN" --name alice
./syncat --config ~/.alice-cfg --data ~/.alice-data peer add "$BOB_TOKEN" --name bob

# Check the connection.
./syncat --config ~/.alice-cfg --data ~/.alice-data status --watch

# Add a share on alice. Subscribe on bob.
./syncat --config ~/.alice-cfg --data ~/.alice-data share add ~/Documents --name docs --perm rw
./syncat --config ~/.bob-cfg --data ~/.bob-data remote ls
./syncat --config ~/.bob-cfg --data ~/.bob-data subscription add <alice-name-or-id> <share-name-or-id> ~/docs-from-alice --mode mirror
```

`--api` changes the listen address of that daemon process. The CLI reads
`api_addr` from `config.json`. Use the same `--config` and `--data` values for
each command that controls a test node.

In this example, a file in Alice's `~/Documents` appears in Bob's
`~/docs-from-alice`. The `read-write` share and `mirror` subscription also let
Bob send changes to Alice.

## Approvals

An unknown node can send a valid handshake. syncat rejects that connection but
stores its name and token as a pending peer. The operator can then approve or
deny it.

```bash
syncat approvals
syncat approvals grant peer-<peer-key>
syncat approvals deny peer-<peer-key>
```

`syncat peer ls` also lists pending peers. `syncat peer approve <id>` approves
one pending peer.

Use `--approval` when you add a share to require approval for subscriptions.
The request remains pending until you grant or deny it.

```bash
syncat share add ~/Documents --name docs --perm rw --approval
syncat approvals grant share-<share-id>-<peer-key>
```

The peer and share queues are stored in `config.json`. They remain after a
restart. Each queue has a size limit.

## Reset a node

`syncat init` is safe to run again. It does not replace existing state.
Use `syncat init --reset` only when you want a new node.

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

Type RESET to confirm:
```

The reset deletes configuration, API credentials, identity keys, tailcat keys,
the index, and the trash. It does not delete a share directory or a local copy
of a subscription.

The new node has a new identity and token. Add it again on every peer. Restore
files from the trash before you reset the node.

Stop the daemon before you reset. The command refuses to run when another
process listens on `api_addr`. Use `--yes` only in a script that must skip the
confirmation prompt.

For a system service, run the reset as the service user:

```bash
sudo systemctl stop syncat.service
sudo -u syncat /usr/local/bin/syncat --config /etc/syncat --data /var/lib/syncat init --reset
sudo systemctl start syncat.service
```

## Run as a systemd service

`syncat daemon` is a foreground process. It logs to standard error and stops
cleanly on `SIGINT` and `SIGTERM`.

Use a user service when possible. syncat normally stores config and data in
the user's XDG directories. A user service uses the correct home directory and
file owner.

Build and install the binary outside the checkout:

```bash
CGO_ENABLED=0 go build -o syncat ./cmd/syncat
install -Dm755 syncat ~/.local/bin/syncat
~/.local/bin/syncat init --name my-laptop
```

Create `~/.config/systemd/user/syncat.service`:

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
TimeoutStopSec=30s
NoNewPrivileges=yes
PrivateTmp=yes

[Install]
WantedBy=default.target
```

Start the service:

```bash
systemctl --user daemon-reload
systemctl --user enable --now syncat.service
systemctl --user status syncat.service
journalctl --user -u syncat.service -f
loginctl enable-linger "$USER"
```

Do not put `--api` in `ExecStart`. Set `api_addr` with `syncat config set`
and restart the service instead. Do not use `ProtectHome=yes`. syncat must be
able to read and write its share directories.

syncat needs no inbound firewall port. tailcat creates outbound connections.
Port 4197 is inside the encrypted tunnel, not a host port.

## Upgrade

Build, install, and restart the service:

```bash
git pull
CGO_ENABLED=0 go build -o syncat ./cmd/syncat
install -Dm755 syncat ~/.local/bin/syncat
systemctl --user restart syncat.service
journalctl --user -u syncat.service -n 30
```

Replace the binary file. Do not write through the file that the daemon is
running. `install` replaces the file safely.

The SQLite index migrates forward when the daemon starts. There are no down
migrations. Before you roll back across a database change, stop the daemon and
copy `index.db`, `index.db-wal`, and `index.db-shm` from the data directory.

Missing fields in `config.json` get default values. An old binary can ignore a
new field and remove it at its next config write. Do not use an old binary to
change config after you upgrade.

This build uses protocol version 3. It requires version 3 from peers. A v2
peer cannot connect to a v3 peer. Upgrade all connected nodes in one change.

tailcat 0.6.0 changed the data in node tokens. Nodes that use an older tailcat
address cannot connect to this build. Make new tokens and add the peers again
when you move from a pre-0.6.0 build.

## Security and data handling

Use syncat only with peers that you trust. A peer can send file changes after
you grant access to a read-write share.

### Data at rest

syncat does not encrypt its stored data. It uses file permissions as the local
access control. Configuration files and key files have mode `0600`. Their
directories have mode `0700` when syncat creates them.

The following files contain secrets:

- `config.json` contains the node tokens for configured and pending peers.
- `api.token` contains the REST API token.
- `identity.key` contains the Ed25519 private identity key.
- `tailcat.key` contains the tailcat private key and WireGuard pre-shared key.

Hexadecimal, JSON, CBOR, and base64 are encodings. They are not encryption.
The SQLite index and the syncat trash are also not encrypted by syncat.

Use full-disk encryption on each node. Also encrypt backups that contain
syncat state or synchronized files. Full-disk encryption can protect a
powered-off device. It does not protect an unlocked device or a running
compromised process.

Application-level encryption can protect an offline copy or an exposed backup.
It is useful only when the decryption key is separate from the encrypted data.
It cannot protect credentials after an attacker controls the running daemon.

### Node tokens and tailcat access

Keep each `sc1` node token secret. The token contains a tailcat address and a
WireGuard pre-shared key. A person who has the token can enter the tailcat
transport and connect to the syncat service on TCP port 4197.

The token alone does not authorize a syncat session. The application handshake
also verifies the configured Ed25519 identity. An unknown identity enters the
pending-peer queue, and syncat closes the connection.

The token still gives access to code that runs before authorization. This code
includes tailcat, the TCP framing code, and the syncat handshake. A token holder
can also use connection attempts to consume CPU and memory.

The current tailcat configuration exposes only TCP port 4197. It does not
enable UDP or network forwarding. It does not provide shell access or a route
to the host network or local network.

If an attacker gets the complete syncat state, the attacker also gets
`identity.key`. The attacker can then impersonate the node to peers that still
trust that identity. The attacker can use shares that have an existing grant.
The attacker can also request shares that do not require approval.

### Protect a node

- Exchange node tokens through an encrypted channel or in person.
- Do not put tokens in logs, issue reports, or unencrypted backups.
- On a server, run syncat as a dedicated, unprivileged account.
- Permit the syncat account to access only the directories that it must
  synchronize.
- Check that syncat files have mode `0600` and syncat directories have mode
  `0700`.
- Require approval for sensitive shares.
- Use read-only shares when a peer does not need to send changes.
- Power off a portable device before you leave it in an untrusted location. A
  suspended device can keep disk keys in memory.

The API listens only on loopback. API requests need `X-Syncat-Token`. This
control protects the API from remote network clients. It does not protect the
API from a process that already runs as the syncat user.

syncat validates every peer path before it accesses the file system. A remote
delete moves a local file to the syncat trash. A local delete does not move the
file to the trash. The scanner does not synchronize symbolic links in this
release.

### Respond to a compromised node

Complete these steps if an attacker can read the syncat state of a node:

1. Disconnect the compromised machine from the network. Stop the syncat
   daemon.
2. Run `syncat peer rm ID` on every peer to remove the old node. Do this before
   you reset the compromised node. A reset on one node does not revoke its old
   identity on other nodes.
3. Reinstall the operating system or restore a known-good system image.
4. Run `syncat init --reset` on the compromised node. This command creates a
   new identity and a new tailcat token.
5. Add the rebuilt node to each peer again.

The compromised state also contains the node tokens of its peers. Removing the
compromised Ed25519 identity prevents an authorized syncat session. It does not
invalidate the copied tailcat tokens. A copied token can still reach TCP port
4197 on the node that issued the token.

To invalidate a copied tailcat token, rotate the tailcat identity of the node
that issued the token. The current supported rotation procedure is
`syncat init --reset`. This procedure also deletes that node's syncat
configuration, index, and trash. It changes the node identity and affects all
of its peer relationships. It does not delete share directories or local
subscription copies. Save required files from the trash before the reset. Add
all peers again after the reset.

## Known limits

- Only the `.syncatignore` file at the root of a share provides rules.
- Transfer resume is not available. A failed transfer starts at byte zero.
- Status has cumulative transport telemetry. It does not provide progress for
  each transfer.
- Deleted-file tombstones remain in the index.
- The web UI polls the API. `GET /api/events` is not available.
- The web UI has no Settings page.
- syncat does not provide block-level delta transfer or mobile bindings.

## Transport and protocol

The protocol uses one framed connection for each peer. The writer has four
bounded queues: urgent messages, ordered control messages, replaceable current
state messages, and bulk file data. A write deadline stops a blocked peer from
holding the connection forever.

Index synchronization uses acknowledged snapshots or journal deltas. syncat
coalesces repeated index work for one share. Transfers have IDs. A cancel
message stops a live transfer and releases blocked work.

`GET /api/status` and `syncat status --json` report low-cardinality network
data. The data includes connection state, queues, transfers, writer results,
and rejected protocol work.

Section 11 of [SPEC.md](SPEC.md) records future security and transfer work.
Other protocol work includes buffered frame I/O, `testing/synctest` for time
tests, and one stream for each file transfer.

## Development

```bash
CGO_ENABLED=0 go build ./...
go vet ./...
go test -count=1 ./...
go test -race ./...
go test -fuzz=FuzzDecode ./internal/protocol
scripts/e2e.sh
scripts/run-net-tests.sh
scripts/run-net-tests.sh --soak
```

`modernc.org/sqlite` is pure Go. This keeps the core packages free of cgo.

`scripts/e2e.sh` starts two real daemons and tests synchronization, deletes,
trash, and conflicts. `scripts/run-net-tests.sh` tests network failures. It
tests peer restarts, offline changes, peer removal, permission changes, and
three-node propagation. The soak tests also test long idle connections and
large relay transfers.

## Project layout

There are ten source packages and 46 production Go source files. See
[ARCHITECTURE.md](ARCHITECTURE.md) for package and data-flow details.

| Package | Main contents |
|---|---|
| `cmd/syncat/` | CLI commands and REST client |
| `internal/api/` | REST routes, authentication, handlers, and DTOs |
| `internal/config/` | Config, keys, tokens, and XDG paths |
| `internal/core/` | Node, peers, approvals, mutations, and status |
| `internal/index/` | SQLite index, scanner, ignore rules, and watcher |
| `internal/protocol/` | Wire messages, handshake, writer, and keepalive |
| `internal/sync/` | Reconciliation, sessions, dispatch, transfer, apply, and trash |
| `internal/transport/` | tailcat and test transports |
| `internal/webui/` | Embedded web UI |
| `internal/version/` | Product version |
