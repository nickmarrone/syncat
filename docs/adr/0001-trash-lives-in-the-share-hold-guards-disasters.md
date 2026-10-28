---
status: accepted
---

# The trash lives in the share; holds guard against disasters

syncat's safety story has three parts with separate jobs. A **hold** stops syncat from recording or spreading a change that looks like a disaster (a missing, unreadable or emptied share root, or a bulk delete), on both the sending and the receiving side. The **trash** is an undo for changes peers make: it keeps what a peer deleted or overwrote, and it now lives inside the share at `.syncat/trash/`, never synced, instead of under the data dir. A **backup** (point-in-time recovery, including of the user's own mistakes) is not syncat's job; the README points to restic or filesystem snapshots, and syncat neither bundles nor schedules them.

The trash moved into the share so users can browse it and restore from it with a file manager, so it is always on the same filesystem (moves are atomic renames, with no copy fallback), and so the user's own backup of the share covers it. The cost is a hidden directory inside the user's folder that other tools will see.

## Considered Options

- **Drop the trash entirely.** Simplest, but users who don't run backups would have no way to undo a peer deleting one file they needed.
- **Keep the trash under the data dir.** Hard to find and use, and it needs a copy+delete fallback whenever the data dir is on a different filesystem from the share.
- **Wrap or schedule restic from syncat.** Rejected: it would be a second product with its own credentials, repository and retention settings, and it would do a worse job than restic plus a systemd timer.
- **An archive mode for receive-only subscriptions that never applies deletes.** Rejected: an unbounded half-backup that keeps both sides of every rename and still loses overwritten contents.
