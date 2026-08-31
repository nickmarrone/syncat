# Manual test checklist

What to exercise by hand before putting syncat in front of other people.

This deliberately skips what `go test ./...` and `scripts/e2e.sh` already
cover, and concentrates on the things automation here structurally cannot
see: real time passing, real reboots, two real machines on two real
networks, and whether an error message actually tells a stranger what to do
next.

## Before you start

```console
$ CGO_ENABLED=0 go build ./...
$ go vet ./...
$ go test -count=1 ./...          # includes the live-DERP tests; needs network
$ bash scripts/e2e.sh             # two real daemons, ~4 min
```

All four should be clean. `e2e.sh` already proves bidirectional sync,
delete + trash, and a two-sided offline conflict on one machine — so treat
those as regression-covered and spend your manual time below.

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

## 2. Pairing two nodes

- [ ] Exchange tokens and `syncat peer add` on **both** sides. Peering is
      mutual — confirm the docs make that obvious to someone who hasn't
      read the spec.
- [ ] `syncat status` on both reaches `state=connected`.
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
exactly 90–120s, and no automated test could see it, because the whole
suite finishes in under 30 seconds of wall-clock time.

- [ ] Leave two nodes connected and idle for **at least 15 minutes**.
      `connected since` in `syncat status` climbs continuously and never
      resets.
- [ ] Over that window, the log contains **no** `send ping: … i/o timeout`
      lines and no repeated reconnects.
- [ ] Leave it connected **overnight**. Still connected in the morning.
- [ ] Pull the network cable / disable wifi on one side. The other notices
      within ~90s and shows `connecting`.
- [ ] Reconnect it. Both return to `connected` without a restart.
- [ ] Suspend one laptop, resume it later — reconnects on its own.

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
- [ ] `syncat share set <id> --perm ro` on a live rw share takes effect on
      the subscriber without a restart.
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

## 7. Subscription lifecycle

- [ ] `syncat subscription pause <peer> <share>` stops syncing; edits on
      either side do not propagate.
- [ ] `syncat subscription resume` picks up everything missed while paused.
- [ ] `syncat subscription rm` stops syncing and **leaves local files in
      place**.
- [ ] After `rm`, the offerer stops seeing the subscriber as subscribed.
- [ ] Re-subscribe to the same share afterwards. Works, no stale state.

## 8. Peer removal cleanup

- [ ] `syncat peer rm <peer>` drops that peer's subscriptions, and says so.
- [ ] It also revokes that peer's entries from your own shares' access
      lists. Verify by re-adding the *same* peer key and confirming it does
      **not** silently regain access to everything.
- [ ] Other peers' access to the same shares survives untouched.
- [ ] Local files from the removed peer's subscriptions are left alone.

## 9. Restart, reboot, and persistence

- [ ] Restart one daemon. Peers reconnect, subscriptions resume, no
      re-pairing needed.
- [ ] Reboot the machine. Same.
- [ ] Install as a systemd user service with `loginctl enable-linger`, then
      reboot **without logging in**. The daemon comes up anyway.
- [ ] `systemctl --user restart syncat` mid-sync leaves no corruption; the
      transfer restarts (resume is not implemented — it starts over).
- [ ] Send `SIGTERM` during a large transfer. Shutdown is graceful and
      bounded — no partial files left in the share, no hang.
- [ ] Edit files on both sides **while both daemons are down**, then start
      both. Converges.
- [ ] `journalctl --user -u syncat` shows clean startup with no panics or
      goroutine dumps.

## 10. Web UI

- [ ] Loads at the configured `api_addr`.
- [ ] Peers, shares, subscriptions, and transfers all render and refresh.
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
- [ ] A **multi-GB** file. Completes; memory stays flat (watch RSS).
- [ ] Many rapid edits in a row — the watcher coalesces rather than
      thrashing.
- [ ] Three or more peers on one share. Remember it is hub-and-spoke: spokes
      sync only through the offerer, never with each other.
- [ ] Idle CPU with several shares configured is near zero.
- [ ] Sync a directory that is also being written by another application
      (a Git checkout, say) and confirm nothing is corrupted.

---

## Known gaps — don't file these as bugs

All documented in README.md's "Deferred past the MVP", listed here so
testers recognise them on sight:

- Peer-approval queue and per-share `approval_required` enforcement (every
  subscribe auto-grants; approval endpoints return 501).
- `.syncatignore` gitignore syntax — only a fixed ignore set exists.
- Symlink syncing — detected and skipped.
- Transfer resume — interrupted transfers restart from zero.
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
