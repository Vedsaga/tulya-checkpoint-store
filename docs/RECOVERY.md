# Durability, recovery, and crash testing

Tulya currently promises single-writer durability against tested userspace
process exits. It does not yet claim sudden-power-loss, torn-sector,
multi-writer, or distributed-consensus safety.

## Acknowledged append

One writer owns the store directory. A checkpoint transaction is validated,
written into a preinitialized WAL range, flushed, and made durable with
`sync_data` before publication to in-process state.

If the process exits after the sync but before memory publication, reopen
parses the durable WAL prefix and recovers the checkpoint exactly once.
Idempotent request identities prevent a resumed caller from duplicating an
already durable append.

## Seal and reclaim

Sealing advances authority in this order:

1. write, sync, rename, and directory-sync an immutable segment;
2. do the same for its immutable route index;
3. write, sync, rename, and directory-sync the new manifest;
4. copy the remaining WAL suffix into a replacement reserve, sync it, rename
   it, and directory-sync the replacement.

The manifest is the authority boundary. Before its publication, recovery uses
the old sealed generation plus the original WAL. After publication, recovery
uses the new generation and either the overlapping old WAL or the recycled
suffix. Reopen recognizes and normalizes that overlap.

Unreferenced immutable files become reclaimable only after readers release
their shared generation gate.

## Deterministic process-crash matrix

The `fault-injection` Cargo feature exits the process with code 86 when
`TULYA_CHECKPOINT_STORE_CRASH_POINT` names a compiled durability boundary.
Normal builds compile the same calls to no-ops.

Run the complete suite:

```bash
cargo test --locked --features fault-injection \
  --test crash_matrix -- --test-threads=1
```

The suite exercises all 16 boundaries twice: once while publishing the first
sealed generation and once while advancing a later generation. Every one of
the 32 cases must:

- exit at the requested point;
- reopen to exactly the old or target authority;
- reconstruct all sibling checkpoints;
- resume idempotently; and
- reopen again at the target.

The boundaries cover segment, route, manifest, and WAL replacement after each
write, file sync, rename, and parent-directory sync. An additional append
boundary exists after hot-WAL sync and before in-memory publication.

This instrumentation is for tests only. Do not ship a production binary built
with `fault-injection`.

## Independent integrity check

`tulya-checkpoint --db STORE fsck` is read-only and independent of the writer.
It does not create lock files, repair, recycle, or delete data. It validates the
manifest and route layout, decompresses and hashes every sealed block, parses
the valid hot prefix, reconstructs every committed checkpoint, and checks
stored lengths and hashes.

Run `fsck` against an offline directory or stable filesystem snapshot. A live
writer can legitimately publish a new manifest during an unlocked scan.
Non-zero bytes after the valid hot prefix are reported separately; they may be
an unacknowledged torn transaction and are not treated as committed state.

## Claim boundary

The matrix covers process exit at named userspace durability boundaries on the
tested Linux/filesystem stack. It does not establish sudden-power-loss,
drive-controller-cache, torn-sector, cross-filesystem, multi-writer, or
distributed-consensus safety.
