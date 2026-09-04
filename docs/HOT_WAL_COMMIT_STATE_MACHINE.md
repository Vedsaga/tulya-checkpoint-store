# Hot-WAL commit failure state machine

Status: production-wired resilience contract for foreground hot-WAL commit publication. Live syscall fault injection remains a separate acceptance unit.

## DECISION

Foreground WAL publication is modeled as an explicit state machine over a minimal I/O surface:

```text
healthy
  |
  | write complete record (short writes allowed)
  v
record-written
  |
  | flush
  v
flushed
  |
  | sync_data
  v
acknowledged
```

A writer that has changed WAL bytes but did not reach an acknowledged result is not reused. It enters `recovery-required` and remains there until the store is closed, reopened, and authoritative WAL state is reconstructed.

Failure classification is:

- write failure before any byte was reported written: ordinary `Io`; the same handle may retry;
- write failure after any record byte was written: `RecoveryRequired`; do not write again on that handle;
- flush failure after the complete record was written: `DurabilityIndeterminate`; do not write again on that handle;
- `sync_data` failure after the complete record was written: `DurabilityIndeterminate`; do not write again on that handle;
- any later mutation attempt on a poisoned writer: `RecoveryRequired`, with no I/O attempted;
- reserve-capacity preparation that fails after mutable WAL preparation begins: the handle is marked recovery-required before returning.

A successful commit performs exactly one successful `sync_data` for the record. There must be no fallible operation after the successful durability barrier and before the caller is told the commit succeeded. In particular, report-only metadata lookups must happen before commit or use already-known values; a post-sync metadata failure must never turn a durable commit into an apparent failed append.

## WHY

A database cannot safely infer commit outcome from a failed durability syscall. Once a complete record has reached the kernel, a flush or sync error can coexist with bytes that later appear during recovery. Continuing to append from the same process state would mix an unacknowledged physical record with an in-memory logical tail that did not advance.

Partial write failure is different: a complete canonical transaction was not reported written, so it is not classified as an indeterminate committed transaction. However, the physical tail may contain a torn prefix. The handle is therefore still poisoned and recovery must normalize the tail before another mutation.

Short writes are normal `Write` behavior, not failures. The commit loop must continue until the complete record is written or an error/zero-progress condition occurs.

## ALTERNATIVES REJECTED

### Keep using `write_all` + `?`

Rejected because it hides how many bytes were successfully written before an error and gives the caller no way to distinguish a clean pre-write failure from a torn-tail condition.

### Treat every I/O failure as retryable

Rejected because retrying on the same handle after a full record write plus flush/sync failure can create duplicate physical records or append after an unresolved record.

### Treat every write error as durability-indeterminate

Rejected because a failure before any byte was written is a definite non-mutation and should remain an ordinary I/O failure.

### Continue using the handle after resetting its file cursor

Rejected because cursor position is not the authority problem. The unresolved bytes on disk/page cache are. Reopen/recovery must decide which physical suffix is authoritative before another mutation.

### Query file metadata after `sync_data` to populate the append report

Rejected because a report-only failure after successful durability would surface an error for an already-committed transaction. Capacity must be known before the durability boundary or reported from cached geometry.

## FORMAT IMPACT

None. This changes runtime failure handling only. Format v1 bytes, Format-v2 staged bytes, transaction checksums, manifest semantics, and WAL framing remain unchanged.

## Current implementation boundary

`src/hot_wal_commit.rs` contains the commit state machine, the production `File` adapter, and deterministic scripted-I/O unit tests. `CheckpointStore::HotWal::append` now delegates complete-record write/flush/`sync_data` publication to this state machine. Report capacity is determined before the durability barrier, so successful `sync_data` is followed only by infallible in-memory bookkeeping.

`CheckpointStore` mutation entry points refuse append, seal, prune, reidentify, reclaim, or recycle work once the hot writer is recovery-required. Read-only checkpoint access remains available and reflects the last state published into the current process; callers that received `DurabilityIndeterminate` must reopen before treating that process-local view as authoritative.

The next acceptance unit injects failures through the live file-backed path, including short/partial writes, ENOSPC, flush/`sync_data` failures, reserve-extension failure, and reopen normalization. Publication/manifest/rename/directory-sync fault semantics remain a separate durability unit.
