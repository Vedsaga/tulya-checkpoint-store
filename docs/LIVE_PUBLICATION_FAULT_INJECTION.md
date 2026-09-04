# Live manifest and WAL-recycle fault injection

Status: staged production-resilience evidence. Local Rust 1.80 verification is
required before this unit is accepted.

## DECISION

The existing `fault-injection` feature now includes a publication-I/O channel:

```text
TULYA_CHECKPOINT_STORE_PUBLICATION_IO_FAULT
```

Supported cases are:

```text
manifest-sync-eio-after
manifest-rename-eio-before
manifest-rename-eio-after
manifest-dir-sync-eio-after
wal-recycle-sync-eio-after
wal-recycle-rename-eio-before
wal-recycle-rename-eio-after
wal-recycle-dir-sync-eio-after
```

All publication fault parsing and state compile only with the
`fault-injection` Cargo feature.

The injected path continues to use the real filesystem. “After” faults perform
the real syscall first and then surface a synthetic I/O error so the test can
exercise an operation that may already be durable even though the caller sees
failure.

## WHY

The crash matrix proves process termination at publication boundaries. It does
not prove the error-return semantics of live syscalls or that the current
`CheckpointStore` handle transitions to the correct poisoned state.

This matrix verifies both the returned failure class and the authority recovered
after close/reopen.

## MANIFEST CASES

### Temporary-file sync error after real sync

`manifest-sync-eio-after` performs the real manifest temporary-file
`sync_all` and then returns EIO before rename.

Expected behavior:

- result is definite ordinary `Io`;
- `DurabilityIndeterminate` is absent;
- the writer is not poisoned;
- process and reopen both retain old manifest authority;
- ordinary hot-WAL mutation remains possible.

The synchronized temporary manifest is not authority.

### Rename error before the real rename

`manifest-rename-eio-before` returns a typed `DurabilityIndeterminate` with
`DurabilityOperation::Rename` without performing the rename.

The test deliberately knows this injected case resolves to old authority, but
the public contract remains indeterminate because a real rename failure is not
used as evidence for retry safety.

Expected behavior:

- current writer is poisoned;
- process-local manifest remains old/unresolved;
- reopen recovers old authority exactly;
- sealing can resume after reopen.

### Rename error after the real rename

`manifest-rename-eio-after` performs the real rename and then returns a typed
rename-indeterminate error before directory sync.

Expected behavior:

- current writer is poisoned;
- process-local manifest remains unresolved because publication returned error;
- reopen observes new authority on the live filesystem;
- old hot WAL is normalized against the new manifest;
- sealing can resume after reopen.

### Directory-sync error after real directory sync

`manifest-dir-sync-eio-after` performs the real parent-directory durability
barrier and then returns a typed
`DurabilityOperation::DirectorySync` indeterminate error.

Expected behavior:

- current writer is poisoned;
- reopen observes new authority;
- all checkpoints remain exact and independently verifiable.

## POST-AUTHORITY WAL RECYCLE CASES

The following faults occur only after `staged_write_manifest` has returned
success and the in-process manifest has been updated:

- recycle temporary-file sync completed, then EIO;
- EIO before WAL recycle rename;
- WAL recycle rename completed, then EIO;
- WAL recycle directory sync completed, then EIO.

For every case:

- the returned class is `RecoveryRequired`;
- the original error context reports
  `RecoveryRequired::authority_committed() == true`;
- the live store reports the newly sealed checkpoint watermark;
- subsequent mutations are blocked;
- exact checkpoint reads remain available;
- reopen recovers the new manifest authority regardless of whether the old full
  WAL or the recycled suffix is physically named `hot.wal`;
- a later seal can continue normally after reopen.

This specifically exercises both previous-generation normalization shapes:

```text
new manifest + old full hot.wal
new manifest + already-renamed suffix hot.wal
```

## ALTERNATIVES REJECTED

### Model all publication errors with process crashes

Rejected because a crash has no returned error object and cannot verify
`DurabilityIndeterminate` versus committed-authority `RecoveryRequired`.

### Mark manifest temporary-file sync failure indeterminate

Rejected because the temporary file is not authority and has not been renamed.

### Return success after WAL recycle maintenance fails

Rejected because the logical seal is committed, but the current writer has
observed a physical maintenance failure and should be reopened before mutation.

### Roll the in-memory manifest back after recycle failure

Rejected because that would intentionally make process state disagree with
known durable manifest authority.

## FORMAT IMPACT

None. No manifest, segment, route, WAL, checkpoint, or public format bytes
change.

## BOUNDARY OF THIS EVIDENCE

This unit covers live error returns for manifest authority publication and
post-authority WAL recycle.

It does not yet inject syscall-result failures into:

- immutable segment write/`sync_all`/rename/directory-sync;
- route write/`sync_all`/rename/directory-sync;
- prune replacement segment/route publication;
- old-generation file deletion/reclaim;
- backup/restore or migration.

Those remain required before the repository-wide live I/O fault-injection gate
is complete.
