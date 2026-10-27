# syncat

Peer-to-peer folder sync between nodes that add each other manually, with no central server.

## Language

### Nodes and shares

**Node**:
One running syncat instance, identified by its keypair.
_Avoid_: Host, machine, device

**Peer**:
Another node this node has added and that has added it back.
_Avoid_: Remote, friend

**Share**:
A local directory a node offers to its peers.
_Avoid_: Shared folder, library

**Offerer**:
The node that owns a share.
_Avoid_: Source machine, host, owner

**Subscription**:
A peer's local copy of someone else's share, with its own mode.
_Avoid_: Mount, replica

### Safety

**Hold**:
The state of a share or subscription in which syncat records and exchanges no changes because a change looked like a disaster; the user clears it by fixing the folder or confirms the change to let it through.
_Avoid_: Pause (a pause is user-chosen; a hold is imposed), freeze, lock

**Bulk delete**:
A single change that would delete at least a locally configured percentage and number of a share's live files, which puts it on hold.
_Avoid_: Mass delete, wipe

**Trash**:
The copies a share keeps of files that peers deleted or overwrote, so those changes can be undone; it is an undo, not a backup.
_Avoid_: Recycle bin, versions, history

**Unreadable path**:
A path that exists but syncat cannot read; it is never treated as deleted.
_Avoid_: Missing file

**Backup**:
Point-in-time history that can recover any past state; syncat does not provide one and leaves it to tools built for it.
_Avoid_: Using "backup" for a receive-only subscription or for anything syncat keeps
