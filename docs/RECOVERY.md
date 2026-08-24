# Durability and recovery

## Acknowledged append

One writer owns the store directory. A checkpoint transaction is validated,
written into a preinitialized WAL range, flushed, and made durable with
`sync_data` before it is published to in-process state. If a process exits
after that sync but before memory publication, reopen parses the durable prefix
and recovers the checkpoint exactly once.

## Seal and reclaim

Sealing advances authority in this order:

1. write, sync, rename, and directory-sync an immutable segment;
2. do the same for its immutable route index;
3. write, sync, rename, and directory-sync the new manifest;
4. copy the remaining WAL suffix to a replacement reserve, sync it, rename it,
   and directory-sync the replacement.

The manifest is the authority boundary. Before its publication, recovery uses
the old sealed generation plus the original WAL. After publication, recovery
uses the new generation and either the old overlapping WAL or the recycled
suffix; reopen recognizes and normalizes the overlap. Unreferenced immutable
files are reclaimable only after readers release their shared generation gate.

## Integrity checks

`tulya-checkpoint --db STORE fsck` is independent and read-only. It does not
open a writer, create lock files, repair, recycle, or delete anything. It
validates the manifest and route layout, decompresses and hashes every sealed
block, parses the valid hot prefix, reconstructs every committed checkpoint,
and checks stored lengths and hashes.

Run `fsck` against an offline directory or stable filesystem snapshot; a live
writer can legitimately publish a new manifest during an unlocked scan.
Non-zero bytes after the valid hot prefix are reported separately. They can be
an unacknowledged torn transaction and are not treated as committed state.

## Claim boundary

The deterministic matrix covers process exit at named userspace durability
boundaries on the tested Linux/filesystem stack. It does not establish sudden
power-loss, drive-controller cache, torn-sector, cross-filesystem portability,
multi-writer, or distributed-consensus safety.
