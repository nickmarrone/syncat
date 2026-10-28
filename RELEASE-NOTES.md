# Unreleased

A safety release. syncat now refuses to spread a disaster, and its trash
moves into your folders where you can see it.

## Upgrading

**The trash moves.** On first start, everything in the old trash
(`~/.local/share/syncat/trash/`) moves into the trash inside each share or
subscription, at `.syncat/trash/`. Anything that can't be moved, such as the
trash of a share you have since removed, stays where it was and is logged;
delete it by hand once you're sure you don't need it. `syncat trash restore`
is replaced by `syncat trash undo` (below).

The index database gains a table for holds. Nothing else on disk changes, and
nodes on this release still talk to 0.4 nodes.

## What's new

**Holds stop a disaster from spreading.** If a share's folder goes missing,
everything in it disappears, or one change would delete at least half of it
(and at least 20 files), syncat puts the share on hold instead of telling your
peers to delete their copies. The same check guards your copy against a peer's
mass delete. Fix the folder and the hold clears by itself, or let the deletes
through with `syncat share confirm-deletes SHARE`. The thresholds are set per
share with `syncat share safety`. Holds show in `syncat status` and as a
banner in the web UI.

**Unreadable files are no longer deletes.** A permissions change that made a
folder unreadable used to look like the folder's files had been deleted, and
that delete spread to every peer. Now the files are left alone and a warning
is logged.

**The trash lives in your folders, one folder per change.** What a peer
deletes or overwrites goes into `.syncat/trash/` inside the share, in a folder
like `2026-10-27 10.32.05 from laptop`, laid out like the share. Browse it in
a file manager, or use `syncat trash ls SHARE` and `syncat trash undo SHARE N`
to put a whole change (or chosen files) back. Undoing never loses anything:
whatever is in the way goes into the trash first. The web UI lists each
share's trash by change, with Undo and Restore buttons and a filter.

**The trash has a size limit.** A change is kept for 30 days or until the
trash outgrows 25% of the share, whichever comes first, oldest first; the
newest change is always kept. Both are set per share.

**syncat is not a backup.** The trash undoes what peers change; it doesn't
recover what you delete yourself. A receive-only subscription is no longer
described as a backup, because it receives deletes too. The README now has a
section on backing up your folders with restic or filesystem snapshots.

# syncat 0.4.0

A hardening release. Most of the work is in how two nodes talk to each other:
the connection is harder to abuse, harder to jam, and easier to see into. It
also finishes the approval flow that 0.3 left out.

## Upgrading

**Upgrade every node together.** 0.4 speaks version 3 of the wire protocol,
and 0.3 spoke version 2. A 0.4 node and a 0.3 node refuse each other during
the handshake, and `syncat status` shows the rejection. Nothing on disk
changes: your config, keys, tokens, and index carry over as they are, so there
is no need to re-add peers.

## What's new

**Approve peers and shares after the fact.** When a node you haven't added
tries to connect, syncat now remembers it instead of just refusing it. The
same goes for a peer asking for a share you marked as needing approval. Both
land in one queue:

- `syncat approvals` lists what is waiting.
- `syncat approvals grant ID` or `syncat approvals deny ID` decides.
- `syncat peer approve ID` adds a waiting node as a peer, with no token paste
  needed.

The queue survives a restart. A granted request takes effect on the live
connection straight away. Until someone grants it, a protected share stays
closed: nothing about it is sent, and nothing is accepted for it.

**Network status in the API.** `GET /api/status` now reports counters for
each peer: dial attempts and failures, backoff, reconnects, handshake
rejections, bytes and files moved, stalled or cancelled transfers, queue depth
in each session, and keepalive round-trip time. The counters keep counting
across reconnects. They never include file paths, share contents, or tunnel
credentials.

**Transfers can be cancelled cleanly.** Every file request now carries its own
ID. A transfer that stalls for 60 seconds, or that the receiver gives up on,
is cancelled on both sides instead of holding a slot until the connection
drops. Four slow files can no longer block every other transfer to a peer.

**Control messages don't wait behind file data.** Keepalives, access changes,
and errors now skip ahead of file chunks. Two nodes each sending a large file
to the other can no longer stall each other's heartbeats and drop the link.

## Fixes

- **Protected shares were not protected.** A peer could reach a share marked
  as needing approval before anyone approved it. Access is now checked before
  any index or file traffic for that share is accepted.
- **A connected peer could touch shares it had no grant for.** Index and file
  messages for a share that isn't active on that connection, or that its
  direction blocks, are now refused before they reach the database or the
  disk. This also stops a receive-only subscriber from leaking its changes
  back.
- **Messages from peers are checked in full.** Paths, IDs, sizes, hashes,
  version vectors, and ranges are validated before anything acts on them.
  Malformed or hostile encodings are rejected.
- **A peer could flood the node.** Handshakes, index work, and file serving
  now each have a fixed limit. An overloaded connection is closed and retried
  instead of piling up work. No single peer can use up the handshake limit for
  everyone else.
- **An acknowledgement could skip changes.** A peer could confirm index
  changes that had never actually been sent to it, and those changes then
  never reached it. A node now accepts only acknowledgements for data it has
  written to that connection.
- **Tunnel secrets could appear in logs.** Tunnel addresses and keys are now
  removed from logs, error messages, API responses, and the UI. Names and
  error text sent by a peer are cleaned up before they are shown.
- **Shutdown and startup races.** Stopping the daemon while tailcat was still
  starting could leak resources. Starting and stopping a session at the same
  moment could misbehave. Both now follow a strict order.
- **The local API is stricter.** Slow or oversized HTTP requests are cut off.
  `localhost` and IPv6 loopback addresses now work in `api_addr`, and the web
  UI's token check still rejects other hosts.
- **API errors use the right status.** A bad request returns 400, a missing
  item 404, and a conflict 409.

## Also in this release

- syncat is now licensed under the BSD 3-Clause License.
- The README has more usage examples. The README and ARCHITECTURE.md have
  been rewritten to be easier to read, and ARCHITECTURE.md now has diagrams.

## Still not in yet

- Interrupted transfers restart from the beginning instead of resuming.
- No transfer progress display anywhere.
- Symlinks are skipped, not synced.
- The settings panel in the web UI is missing.
- Whole files are sent on every change. There is no block-level delta yet.

## Version

`syncat version` reports `0.4.0`, plus the build commit when it was built from
a git checkout.

---

# syncat 0.3

The first real release. syncat keeps directories in sync between computers you
trust, with no server in the middle and no account to sign up for.

## What it does

**Syncs folders straight between machines.** Two nodes talk to each other
directly over an encrypted tunnel. When your network won't allow a direct
path, the connection falls back to a relay, and the relay still can't read
anything going through it.

**No accounts, no server, no setup service.** You run `syncat init`, start the
daemon, and that's the whole install. There is nothing to host and nothing to
sign into.

**Peers are added by pasting a token.** Each node prints a short token with
`syncat token`. Send it to the other person, they paste it, you paste theirs,
and the two nodes find each other. Both sides have to add the other before any
file moves, so nobody can attach to your node on their own. Treat the token
like a password.

**You pick what gets shared and how.** You offer a directory as a share, and
the other side subscribes to it in a local directory of their choice. Crossing
your permission with their choice gives you three behaviors:

- Both sides send changes (a two-way mirror).
- You send, they only receive (a one-way push).
- They pull your changes but never send their own (a backup).

**Files sync as you work.** The daemon watches your shared directories and
picks up changes as they happen, with a periodic full rescan as a backstop.

**Skip files you don't want to send.** Drop a `.syncatignore` file in the root
of a share. It uses the same rules as `.gitignore`. The list works both ways:
an ignored file is never sent out and never accepted in.

**Conflicts keep both copies.** If the same file changed in two places at
once, the newer one keeps the name and the other is saved beside it with a
timestamp. Nothing is thrown away and you decide what to do.

**Deleted files go to a trash can first.** Anything syncat removes or
overwrites gets a copy in the trash, and `syncat trash restore` brings it
back. Old trash is cleaned up on a schedule you set.

**A web UI and a CLI, both on the same API.** The daemon serves a small web
page on `127.0.0.1:8347` for adding peers, watching connections, and managing
shares. Every CLI subcommand is a thin client of that same API, so scripts and
the browser see exactly the same thing. The API only listens on loopback and
every request needs a token.

**One binary, and it runs as a service.** syncat builds to a single file with
no C dependencies. There are systemd unit files for running it as your own
user or system-wide.

## Things that aren't in yet

Called out so nothing is a surprise:

- Interrupted transfers restart from the beginning instead of resuming.
- No transfer progress display anywhere.
- Symlinks are skipped, not synced.
- Approving a peer or a subscription after the fact isn't wired up; peers you
  haven't added are simply refused.
- The settings panel in the web UI is missing. Everything it would change is
  in `config.json` and the daemon honors it.
- Whole files are sent on every change. There is no block-level delta yet.

## Version

`syncat version` reports `0.3`, plus the commit it was built from when the
build came out of a git checkout (`0.3+5577476`). That number is the product
version only. The wire protocol, the token format, and the index schema each
have their own, and this release doesn't say anything about them.
