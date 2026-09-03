# Checkpoint-store failure taxonomy

Status: staged resilience contract. This document adds no persisted bytes and does not change Format v1 or staged Format v2.

## Purpose

Database callers need to know what an error means operationally. Diagnostic strings are not a retry protocol. In particular, a failure after complete commit bytes have been written may mean either that durability failed or that durability succeeded but the acknowledgement path failed. Treating that case as an ordinary retryable I/O error can overwrite or duplicate an operation.

The crate therefore exposes a stable, non-exhaustive `CheckpointStoreFailureKind` independently of the older public `CheckpointStoreError` enum. This avoids a semver-breaking expansion of an enum that downstream code may match exhaustively.

## Failure classes

- `Corruption`: authoritative persisted bytes are malformed or internally inconsistent. Recovery/fsck must fail closed.
- `UnsupportedFormat`: bytes are well-framed enough to identify a format/version that this build does not support. Never reinterpret them as another format.
- `RequestConflict`: one durable request identity names a different logical operation. Retry is not permitted under that identity.
- `Deleted`: the exact identity is a durable tombstone. It must not be resurrected.
- `Stale`: the requested live identity no longer exists or the caller's reference is stale.
- `LockBusy`: another owner holds the required exclusive resource. No storage corruption is implied.
- `Capacity`: a configured, representable, or bounded resource limit was exceeded before commit authority changed.
- `Io`: an I/O operation definitely failed and is not classified as an indeterminate commit outcome.
- `DurabilityIndeterminate`: complete authoritative commit bytes may already have become durable although the caller did not receive success. The writer must not continue normal writes; reopen/recovery plus request identity resolves the result.
- `Precondition`: the operation is valid only in a different lifecycle state, for example pruning an unsealed store.
- `LegacyUnclassified`: an older error site has not yet been migrated with enough context for a precise behavioral class.

## Durability-indeterminate context

`DurabilityIndeterminate` retains:

- the durability operation (`WalFlush`, `WalSyncData`, `FileSyncAll`, `Rename`, or `DirectorySync`),
- the affected path,
- the original `std::io::Error` including its OS error code when available.

The context is typed. Callers and tests must not parse error messages to infer retry behavior.

The next resilience unit wires this context into an injectable hot-WAL I/O boundary and poisons the writable handle after an indeterminate result until reopen/recovery resolves authority.

## Compatibility rule

The existing `CheckpointStoreError` variants remain unchanged in this unit. `CheckpointStoreError::failure_kind()` is the behavioral API. New callers should prefer the classifier over exhaustive matching of the concrete enum.

Existing semantic variants map directly where meaning is already unambiguous:

- `RequestIdConflict` -> `RequestConflict`
- `CheckpointDeleted` -> `Deleted`
- `CheckpointNotFound` -> `Stale`
- writer/reclaim lock conflicts -> `LockBusy`
- prune lifecycle errors -> `Precondition`
- ordinary `Io` -> `Io`
- malformed JSON syntax/data/EOF -> `Corruption`

`Format(String)` intentionally maps to `LegacyUnclassified`. We will migrate those call sites individually rather than introduce string-prefix heuristics.

## Retry contract

A caller may retry automatically only when the operation's documented semantics and failure class make retry safe. `DurabilityIndeterminate` is never permission to issue a different write. The safe resolution sequence is:

1. stop normal writes on that handle,
2. reopen/recover authoritative state,
3. classify the original request identity against the recovered request ledger,
4. return already-committed, deleted, conflict, or retry-new accordingly.

Requestless writes cannot in general be resolved safely after an indeterminate durability result. Production adapter paths therefore need durable request identity for exactly-once retry semantics.

## Acceptance for this unit

- no persisted-format change,
- no removal or addition of `CheckpointStoreError` variants,
- `failure_kind()` classifies existing unambiguous variants without string parsing,
- malformed JSON is corruption by `serde_json` category rather than message text,
- an embedded durability-indeterminate error retains operation, path, and original OS error,
- an ordinary I/O error never becomes indeterminate accidentally,
- the existing no-panic/no-unsafe Clippy gate remains green.

## Decision ledger

**DECISION**

Add a separate non-exhaustive behavioral classifier and typed durability-indeterminate context before changing syscall behavior.

**WHY**

Retry safety is part of the database contract, but adding variants directly to the existing public error enum could break downstream exhaustive matches. A classifier provides stable operational meaning while allowing the concrete representation to evolve deliberately.

**ALTERNATIVES REJECTED**

- Add many new `CheckpointStoreError` variants immediately: semver risk before callers have a migration path.
- Infer error class from `Format(String)` text: brittle and unsafe for retry decisions.
- Treat every I/O error as retryable: incorrect after complete commit bytes may have reached stable storage.
- Treat every post-write error as committed: also incorrect; recovery must decide authority.

**FORMAT IMPACT**

None. This is runtime API/error semantics only. Format v1 and staged Format v2 bytes are unchanged.
