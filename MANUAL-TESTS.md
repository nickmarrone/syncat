# Manual test checklist

What to exercise by hand before putting syncat in front of other people.

This deliberately skips what `go test ./...`, `scripts/e2e.sh`, and
`scripts/run-net-tests.sh` already cover, and concentrates on what remains
genuinely out of reach of automation on one machine: **two real machines on
two real networks**, real reboots and suspends, hardware that goes away
without warning, and whether an error message actually tells a stranger what
to do next.

## Before you start

```console
$ CGO_ENABLED=0 go build ./...
$ go vet ./...
$ go test -count=1 ./...             # includes the live-DERP tests; needs network
$ bash scripts/e2e.sh                # two real daemons, happy path, ~2 min
$ bash scripts/run-net-tests.sh      # networking failure scenarios, ~10 min
$ bash scripts/run-net-tests.sh --soak   # ... plus the slow ones, ~30 min
```

All of these should be clean. Between them they prove, on one machine and
over real DERP: bidirectional sync, delete + trash, offline divergence and
two-sided conflicts, one-sided peering, the simultaneous-dial race, SIGTERM
and SIGKILL restarts, peer removal, a live `--perm` change, three-node
fan-out, and — with `--soak` — the 90s dead-peer rule, long-idle stability,
and large transfers. Treat all of that as regression-covered and spend your
manual time on the items below that are still unticked.

Items marked **[automated]** below are covered by a script and are listed
only so you know what it is you are *not* having to check by hand; the
script name is given so you can go read what it actually asserts.

You need **two machines** for most of this. At least one round should be on
two *different* networks (not both on your LAN), because that is the only
way to exercise the DERP relay path rather than a direct LAN hop.

---

## 1. Fresh node setup

- [ ] `syncat init` on a machine with no existing config. Succeeds, prints
      the paths it created.
- [ ] `syncat init` again. Idempotent — does **not** regenerate keys or
      overwrite `config.json`.
- [ ] `syncat token` prints an `sc1…` token, and prints the same token on
      every subsequent run.
- [ ] Files land where documented: `~/.config/syncat/{config.json,api.token}`
      and `~/.local/share/syncat/{keys,db,trash}`.
- [ ] Directories and key files are mode `0700`/`0600` — nothing
      world-readable.
- [ ] `syncat --config /tmp/x --data /tmp/y init` honours the overrides.
- [ ] `XDG_CONFIG_HOME` / `XDG_DATA_HOME` are honoured when set.
- [ ] Any subcommand other than `init`/`token`/`daemon` with no daemon
      running fails with a clear "is the daemon running?" style message,
      not a raw connection-refused stack.

## 1b. `syncat init --reset`

Do this on a throwaway node (`--config`/`--data` overrides), with a share
holding a file you can look for afterwards, and note `syncat token`'s short
id first so you can tell the identity actually rotated.

- [ ] `init --reset` with the daemon **running** refuses, and the message
      names the address it found something on.
- [ ] With the daemon stopped, the prompt lists all five paths it will
      delete *and* names the share and subscription directories it will not.
- [ ] Typing `reset`, `yes`, or just Enter aborts. Confirm afterwards that
      `config.json` and the keys are still there — nothing partial.
- [ ] Typing `RESET` goes through. `config.json`, `api.token`, `keys/`,
      `db/`, and `trash/` are gone and immediately rebuilt; the three
      directories are back at mode `0700` and empty of old state.
- [ ] `syncat token` prints a **different** short id than the one you noted.
- [ ] The share directory still has its files, byte for byte.
- [ ] `--name` takes effect on a reset (it is ignored on a plain re-`init`),
      and falls back to the hostname when omitted.
- [ ] `echo | syncat init --reset` fails on the non-terminal stdin instead
      of reading EOF and doing anything.
- [ ] `init --reset --yes` runs unattended with no prompt.
- [ ] `init --yes` without `--reset` is rejected.
- [ ] Reset a node that a peer was connected to, then re-pair from scratch.
      The peer's stale entry for the old identity is inert, and re-adding
      the new token works with no leftover state on either side.
- [ ] Corrupt `config.json` (write garbage into it), then `init --reset`.
      It still works — the prompt just can't name the kept directories.
- [ ] On a system-wide install (state owned by a `syncat` service user),
      `sudo syncat ... init --reset` as **root** is refused before anything
      is deleted, and the message names the owner and prints a `sudo -u`
      command that actually works when pasted.
- [ ] The same reset as `sudo -u syncat` goes through, and the recreated
      `/etc/syncat` and `/var/lib/syncat` files are still `syncat`-owned —
      `systemctl start syncat.service` comes up clean.

## 2. Pairing two nodes

- [automated] Exchanging tokens on both sides reaches `state=connected` on
      both, including when both `peer add` calls land at the same instant
      and the two dials race (`scripts/net/02-simultaneous-peering.sh`).
      Confirm by hand only that the *docs* make the mutuality obvious to
      someone who hasn't read the spec.
- [automated] Adding a peer on one side only is rejected and recorded by the
      other, neither daemon wedges, and completing the pairing recovers with
      no restart (`scripts/net/01-peering-one-sided.sh`).
- [ ] **Paste your own token into `syncat peer add`.** Must be rejected with
      a message telling you to use the *other* node's token. This failed
      silently for a long time.
- [ ] Add a peer while the daemon is already running (not just at startup)
      and confirm it connects. Runtime-added peers were once permanently
      stuck in `connecting`.
- [ ] Garbage token → clear parse error, nothing persisted.
- [ ] `syncat peer ls` shows both name and key; `syncat peer rm` works.

## 3. Connection durability

**Highest-value section.** A handshake bug used to kill every connection at
exactly 90–120s, and no automated test could see it, because the whole Go
suite finishes in under 30 seconds of wall-clock time.

Most of this section is now automated — `scripts/net/21-idle-stability.sh`
holds a connection idle past that 90–120s window and asserts the absence of
reconnects and ping timeouts, and `scripts/net/20-blackhole-dead-peer.sh`
freezes a daemon with SIGSTOP so its sockets stay open while it answers
nothing, which is what a pulled cable looks like from the other end. Both
are `--soak` scenarios. What is left here is what still needs real hardware
and a real network in between.

- [automated] Idle stability past the 90–120s window, with no reconnects and
      no `send ping: … i/o timeout` lines
      (`21-idle-stability.sh`; `SOAK_MINUTES=15` to lengthen it).
- [automated] A peer that goes silent is declared dead within ~90s, shows as
      not connected, and both sides recover with no restart
      (`20-blackhole-dead-peer.sh`).
- [ ] Leave it connected **overnight**, on two real machines. Still
      connected in the morning, and `connected since` has climbed the whole
      way without resetting.
- [ ] Actually pull the network cable / disable wifi on one side, rather
      than freezing the process. The other notices within ~90s and shows
      `connecting`; reconnecting the cable recovers both without a restart.
- [ ] Suspend one laptop, resume it later — reconnects on its own. (A
      suspend stops the clock as well as the process, which SIGSTOP does
      not, so this one is genuinely different from the automated version.)
- [ ] Move one machine between networks (wifi → tethered) while connected.

## 4. Sharing and subscribing

- [ ] `syncat share add ~/dir --name docs --perm rw`, then `syncat share ls`.
- [ ] The peer sees it in `syncat remote ls` within a few seconds.
- [ ] `syncat subscription add <peer> <share> ~/dest` **by display name** —
      the names printed by `remote ls` work directly.
- [ ] Same by full hex id, and by unique id prefix (git-style).
- [ ] Files flow to `~/dest`.
- [ ] `syncat subscription ls` shows it with the right mode, access, and
      connected state.
- [ ] Add a second peer with a **duplicate display name**, then subscribe by
      that name. Must be refused as ambiguous, naming both candidates — not
      silently resolved to one of them.
- [ ] Subscribe naming a peer that doesn't exist → error that lists the
      peers that do.
- [ ] Subscribe naming a share the peer doesn't offer → error that lists
      what it does offer.
- [ ] Subscribe to a local path inside an existing share (or vice versa) →
      rejected by overlap validation, with an explanation.

## 5. The permission × mode matrix

Each row is a separate share; verify the effective behaviour, not just that
the command succeeds.

- [ ] **rw + mirror** → bidirectional. Edit on both sides, both converge.
- [ ] **ro + anything** → one-way push. Subscriber is forced to
      receive-only even if `--mode mirror` was passed.
- [ ] **rw + receive** → one-way pull. Subscriber's local edits never reach
      the offerer.
- [ ] On a **read-only** share, edit the subscriber's copy, then change the
      offerer's copy. The local edit is warned about in `syncat status`, a
      trash copy is taken, and it is overwritten.
- [automated] `syncat share set <id> --perm ro` on a live rw share takes
      effect on the subscriber without a restart, stops its writes, and
      leaves its existing files alone
      (`scripts/net/07-live-permission-change.sh`).
- [ ] Changing a subscription's *mode* is refused with a message saying to
      remove and re-add. (Known limitation, but the message should say so.)

## 6. Deletes, trash, and conflicts

`e2e.sh` covers the happy path for all three on one machine — verify these
across two real machines.

- [ ] Delete on one side propagates; a trash copy appears on the receiving
      side under `~/.local/share/syncat/trash/<share-id>/`.
- [ ] `syncat trash ls <share>` lists it; `syncat trash restore <share>
      <path>` brings it back and re-syncs.
- [ ] Rename a file. Both sides converge (expect delete + create, not a
      true rename — confirm nothing is lost).
- [ ] Take one side offline, edit the same file differently on both, bring
      it back. A `.sync-conflict-<timestamp>-<hash>` copy appears on **both**
      sides with identical names and contents.
- [ ] Delete on one side while editing on the other, offline. Converges
      without losing the edit.
- [ ] Create a directory tree several levels deep. Structure arrives intact.
- [ ] Empty directories — decide whether the behaviour you see is what you
      want to document.
- [ ] Filenames with spaces, unicode, and emoji.
- [ ] A symlink in a shared directory is skipped with a warning, and does
      not break the scan. (Deferred feature, but must degrade cleanly.)

## 6b. `.syncatignore`

`scripts/net/09-syncatignore.sh` covers this on one machine — verify it
across two real machines, and take the third item seriously: it is the one
that destroys data if it regresses.

- [ ] Put `*.log` and `build/` in a `.syncatignore` at the share root.
      Matching files and directories never reach the peer; a non-ignored
      sibling written at the same time does.
- [ ] The `.syncatignore` itself does not sync. Give the two nodes different
      ignore files and confirm each honours only its own.
- [ ] **Ignoring is not deleting.** Let a file sync, then add a rule for it.
      The peer's copy must still be there several minutes later — a tombstone
      would arrive within a rescan interval, so wait longer than one.
- [ ] Remove that rule again. The file resumes syncing, and the peer ends up
      with this node's copy rather than staying on its stale one.
- [ ] Ignore a path on the *receiving* side that the offerer is still
      sharing. It never lands locally, and the offerer's own copy is not
      deleted.
- [ ] Edit `.syncatignore` while the daemon runs; the change takes effect on
      the next rescan without a restart.
- [ ] Put a malformed line in it (e.g. `[z-a]`). It is logged and skipped,
      and the surrounding rules still work.
- [ ] Ignore a large directory (`node_modules`) and confirm the daemon does
      not hold watch descriptors for it.

## 7. Subscription lifecycle

- [ ] `syncat subscription pause <peer> <share>` stops syncing; edits on
      either side do not propagate.
- [ ] `syncat subscription resume` picks up everything missed while paused.
- [ ] `syncat subscription rm` stops syncing and **leaves local files in
      place**.
- [ ] After `rm`, the offerer stops seeing the subscriber as subscribed.
- [ ] Re-subscribe to the same share afterwards. Works, no stale state.

## 8. Peer removal cleanup

- [automated] `syncat peer rm <peer>` over a live connection drops that
      peer's subscriptions, tears the session down on both sides, leaves the
      already-synced files on disk, blocks any further sync, and re-adds
      cleanly (`scripts/net/06-peer-removal.sh`).
- [ ] It says so clearly — check the wording, not the behaviour.
- [ ] It also revokes that peer's entries from your own shares' access
      lists. Verify by re-adding the *same* peer key and confirming it does
      **not** silently regain access to everything.
- [ ] Other peers' access to the same shares survives untouched.
- [ ] Local files from the removed peer's subscriptions are left alone.

## 9. Restart, reboot, and persistence

- [automated] Restart one daemon (SIGTERM). Peers reconnect, subscriptions
      resume, no re-pairing needed, and the survivor notices the shutdown
      immediately rather than waiting out the 90s dead timer
      (`scripts/net/03-graceful-restart.sh`).
- [automated] The same after a SIGKILL, where the survivor is left holding a
      connection to a process that no longer exists
      (`scripts/net/04-hard-kill-restart.sh`).
- [ ] Reboot the machine. Same. (A reboot is not a restart: the clock jumps,
      the network comes up underneath the daemon, and DERP has to be
      re-resolved.)
- [ ] Install as a systemd user service with `loginctl enable-linger`, then
      reboot **without logging in**. The daemon comes up anyway.
- [automated] A restart mid-transfer leaves no corruption and no partial file
      in place; the transfer starts over, since resume is not implemented
      (`scripts/net/23-restart-mid-transfer.sh`, `--soak`).
- [ ] `systemctl --user restart syncat` mid-sync specifically — the unit's
      `TimeoutStopSec` and the daemon's own drain interacting.
- [ ] Send `SIGTERM` during a large transfer. Shutdown is graceful and
      bounded — no partial files left in the share, no hang.
- [automated] Edit files on both sides **while both daemons are down**, then
      start both. Converges, keeping both versions
      (`scripts/net/05-offline-divergence.sh`).
- [ ] `journalctl --user -u syncat` shows clean startup with no panics or
      goroutine dumps.

## 10. Web UI

- [ ] Loads at the configured `api_addr`.
- [ ] Peers, shares, and subscriptions all render and refresh. (There is no
      transfer view — see the known gaps below.)
- [ ] The node token shows truncated by default and expands to the full
      wrapped token; Copy copies the whole token either way, and the card
      never grows a horizontal scrollbar.
- [ ] Peer rows expand and collapse, and stay as you left them across the
      2s poll refresh. Remove peer and the offered-share list are only
      reachable from an expanded row.
- [ ] Adding a peer, share, and subscription through the UI produces the
      same result as the CLI.
- [ ] The subscribe dialog rejects bad input with a readable message rather
      than a raw JSON error blob.
- [ ] Node rename works and persists.
- [ ] The UI stays sane when the daemon is stopped underneath it.
- [ ] Loading it in two tabs doesn't produce conflicting state.

## 11. Error messages and bad input

Read these as a stranger would. Every one should say what to do next.

- [ ] Wrong argument order on `subscription add`.
- [ ] Nonexistent local path, and a path you lack permission to write.
- [ ] Share a path that doesn't exist.
- [ ] Disk full on the receiving side mid-transfer.
- [ ] A file that changes while it is being read for transfer.
- [ ] Two peers offering shares with the same display name.
- [ ] `syncat peer approve` and `syncat approvals` → honest 501-backed
      "not implemented", not a crash.

## 12. Security surface

- [ ] `curl http://127.0.0.1:8347/api/status` with no `X-Syncat-Token`
      header → 401.
- [ ] With a wrong token → 401.
- [ ] From **another machine** on the LAN → refused; the listener is
      loopback-only.
- [ ] An unknown peer key dialling in is rejected and recorded in
      `syncat status`'s rejected-connections list.
- [ ] `api.token` and the key files are not world-readable.
- [ ] Nothing sensitive (tokens, keys) is printed at default log level.

## 13. Real data and scale

- [ ] A directory with **thousands** of small files. Initial scan and sync
      complete; note how long.
- [automated] A large file completes with a matching sha256, without
      starving the keepalive behind its own chunks
      (`scripts/net/22-large-file.sh`, `--soak`; `SOAK_FILE_MB=2048` for a
      multi-GB run).
- [ ] Watch RSS during that multi-GB run — memory must stay flat. The script
      asserts correctness, not memory.
- [ ] Many rapid edits in a row — the watcher coalesces rather than
      thrashing.
- [automated] Three peers on one share, confirming hub-and-spoke: spokes
      converge only through the offerer and never peer with each other
      (`scripts/net/08-three-node-fanout.sh`).
- [ ] Four or more peers, on separate machines, with a large share.
- [ ] Idle CPU with several shares configured is near zero.
- [ ] Sync a directory that is also being written by another application
      (a Git checkout, say) and confirm nothing is corrupted.

---

## Known gaps — don't file these as bugs

All documented in README.md's "Deferred past the MVP", listed here so
testers recognise them on sight:

- Peer-approval queue and per-share `approval_required` enforcement (every
  subscribe auto-grants; approval endpoints return 501).
- Nested `.syncatignore` — only the share root's file is read as rules.
- Symlink syncing — detected and skipped.
- Transfer resume — interrupted transfers restart from zero.
- Transfer progress — no per-transfer byte counters exist, so `/api/status`
  has no `transfers` key and the dashboard has no progress bars.
- Tombstone purging — deleted-file tombstones are kept indefinitely.
- `GET /api/events` (SSE) — the UI polls instead.
- The Settings view — config-file only.
- Block-level delta transfer — all transfers are whole-file.
- Mobile bindings.

## When something fails

Capture all of this before restarting anything:

```console
$ syncat status --json
$ journalctl --user -u syncat -n 500
$ syncat peer ls && syncat share ls && syncat subscription ls
```

Note both machines' OS, whether they were on the same network, and how long
the daemons had been up. Timing matters: several past bugs only appeared
after a fixed interval, so "it had been running about two minutes" is a real
clue, not a throwaway detail.
