# Live hot-WAL I/O fault injection

Status: staged production-resilience evidence. Local Rust 1.80 verification is required before this unit is accepted.

## DECISION

The existing `fault-injection` feature now has a hot-WAL syscall-result mode in addition to the existing process-crash points.

The injected path still writes through the real `std::fs::File`. Faults alter the result or maximum progress of selected operations at the I/O boundary rather than replacing the file with an in-memory mock.

Supported hot-WAL fault specifications are:

```text
short-write=N
write-enospc-after=N
flush-eio-after
sync-eio-after
reserve-enospc-after-set-len
```

The environment key is internal test infrastructure:

```text
TULYA_CHECKPOINT_STORE_WAL_IO_FAULT
```

All parsing, fault state, and fault bookkeeping compile only when the `fault-injection` Cargo feature is enabled. Normal builds do not consult this environment variable and do not carry the injected-write counters.

## WHY

The pure `HotWalCommitIo` scripted tests prove the commit state machine, but they do not prove that the production `File` adapter, the live `HotWal` handle, process-local state publication, and reopen logic compose correctly.

The live matrix therefore exercises the same production append path used by `CheckpointStore` while forcing filesystem-like outcomes at the narrow I/O boundary.

## ACCEPTANCE INVARIANTS

The matrix in `tests/hot_wal_faults.rs` checks:

1. **Short writes**
   - each underlying call writes at most the injected limit;
   - the commit loop completes the exact transaction;
   - reopen reconstructs the exact checkpoint.

2. **ENOSPC before any record byte**
   - result class is `Capacity` because ENOSPC occurred before WAL mutation;
   - the WAL prefix remains byte-for-byte unchanged;
   - the same open writer can retry after the fault is removed.

3. **ENOSPC after a real partial write**
   - physical WAL bytes actually change;
   - result class is `RecoveryRequired`;
   - the current handle blocks every later mutation;
   - process-local semantic state does not publish the checkpoint;
   - reopen ignores the torn suffix and recovers the old logical tail;
   - retry after reopen produces exactly one valid checkpoint.

4. **Flush failure after a complete write**
   - result class is `DurabilityIndeterminate` with `WalFlush` context;
   - the handle becomes recovery-required;
   - reopen resolves to a valid old-or-new state only;
   - retry, when needed, yields exactly one logical checkpoint.

5. **Injected error after real `sync_data` success**
   - the real durability syscall completes first;
   - the caller still receives `DurabilityIndeterminate` with `WalSyncData` context;
   - process-local semantic state remains unpublished;
   - reopen proves the transaction is present and exact.

6. **Reserve extension failure after `set_len`**
   - physical reserve length has already changed;
   - result class is `RecoveryRequired`;
   - the handle blocks further mutation;
   - reopen derives the old logical tail;
   - the enlarged physical reserve can be reused safely for a later successful retry.

Every reopened state is checked through exact checkpoint reads and `verify_all`.

## ALTERNATIVES REJECTED

### Test only the scripted in-memory I/O implementation

Rejected because it cannot expose mistakes in the actual `File` adapter or reopen behavior.

### Use only process crashes

Rejected because process termination does not model short writes, ENOSPC, or durability syscalls returning errors.

### Retry partial writes on the same handle

Rejected because the physical suffix is no longer known to correspond to process-local logical state.

### Treat post-sync injected failure as an ordinary I/O rejection

Rejected because the real `sync_data` has already succeeded. The caller must receive an indeterminate outcome and resolve it by reopening.

## FORMAT IMPACT

None. No persisted format bytes or public format versions change. The feature only changes syscall behavior in builds compiled with `fault-injection`.

## BOUNDARY OF THIS EVIDENCE

This unit covers foreground hot-WAL record publication and reserve growth only.

It does **not** yet prove the full publication fault matrix for:

- immutable segment `sync_all`;
- route `sync_all`;
- manifest staging and `sync_all`;
- rename/replace failures;
- parent-directory sync failures;
- WAL recycle rename/directory-sync result errors;
- deletion/compaction publication;
- migration and backup/restore.

Those remain required before the resilience gate or production-readiness fault-injection checklist can be completed.
