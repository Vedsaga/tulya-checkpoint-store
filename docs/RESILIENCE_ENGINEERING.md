# Resilience engineering contract

Status: production-readiness gate. This document defines requirements for all
new storage/recovery code and the audit target for existing runtime code.
Passing ordinary unit tests is not sufficient evidence for this gate.

## Decision ledger

**DECISION**

Treat panic-freedom for recoverable conditions, typed error propagation,
explicit durability outcomes, fail-closed recovery, bounded hostile-input
handling, and live I/O fault injection as prerequisites for production storage.

The library crate therefore forbids `unsafe` code and, outside `#[cfg(test)]`,
denies Clippy uses of `unwrap`, `expect`, `panic!`, `todo!`, and
`unimplemented!`.

Test fixtures may use panic-style assertions and `unwrap()` because a failed
test is intentionally process-fatal. Production/runtime paths may not use those
constructs to handle malformed bytes, filesystem failures, resource limits, or
other recoverable conditions.

**WHY**

A database-like component must remain deterministic when the environment is
hostile: writes can be short, synchronization can fail, files can be truncated,
space can be exhausted, processes can die at any instruction boundary, and
persisted bytes can be corrupt. Returning an error is useful only if the caller
can distinguish whether the operation was definitely rejected, definitely
committed, or may have committed durably before the error became observable.

The storage implementation must therefore make commit authority and recovery
rules explicit rather than relying on process memory or optimistic filesystem
behavior.

**ALTERNATIVES REJECTED**

- Review-only prohibition on `unwrap`: rejected because it is not mechanically
  enforced.
- Convert every failure into one generic string error: rejected because callers
  cannot determine retry or operator action.
- Treat every malformed final WAL record as a torn tail: rejected because
  complete corruption must fail closed.
- Fault-test only pure codecs: rejected because syscall ordering and durability
  failures are part of the storage protocol.
- Catch panics at the public API boundary: rejected because unwinding does not
  turn partially-mutated storage into a transactionally safe state.

**FORMAT IMPACT**

None. These are runtime and verification rules. Format v1 and all staged v2
bytes remain unchanged by this policy.

## Runtime no-panic rule

Recoverable runtime conditions must be represented by `Result` or another
explicit outcome. In particular, runtime code must not panic because of:

- malformed/corrupt persisted bytes;
- unsupported format/version/feature bits;
- invalid lengths, offsets, counts, conversions, or topology;
- missing files or permission changes;
- short reads/writes;
- write, flush, `sync_data`, `fsync`, rename, directory-sync, truncate, or
  preallocation failures;
- ENOSPC/quota exhaustion;
- request conflicts or stale/deleted identities;
- allocation/resource limits derived from persisted input.

Internal invariant bugs may still represent programmer defects, but storage
parsers and I/O state machines must not use assertions as substitutes for input
validation.

## Error taxonomy target

The public/runtime error model must eventually distinguish at least:

```text
Corruption
UnsupportedFormat
RequestConflict
DeletedOrStaleIdentity
LockBusy
Capacity / ENOSPC
Io
DurabilityIndeterminate
InternalInvariant
```

Errors that wrap an OS failure should retain operation context and the original
`std::io::Error`/kind where practical. Corruption diagnostics should retain the
record/file family and byte offset when known.

The important transaction result classification is:

```text
Rejected
    => durable authority is exactly the old state

Committed
    => durable authority contains exactly the requested new state

Indeterminate
    => the syscall result cannot prove whether durable authority crossed the
       commit boundary; retry by the same logical request identity must resolve
       to exactly zero or one logical commit
```

A generic `Io` error must not be used where the operation may already have
crossed the durable commit boundary.

## I/O abstraction target

Before public v2 writes are enabled, storage mutation must be routed through a
small I/O boundary that can be implemented by both the real filesystem and a
deterministic fault injector. The boundary must cover the operations whose
ordering participates in durability, including:

```text
read / read_exact
write / write_all semantics including short writes
flush
sync_data / sync_all
truncate
preallocate or reserve
rename / replace
create/remove
parent-directory sync where required by the supported platform contract
```

The abstraction should expose OS failures, not hide them. Production code may
use retry loops only where the operation contract proves retry is safe.

## Commit-authority rule

No successful acknowledgement may precede durable completion of the bytes that
recovery treats as commit authority.

For the staged v2 design, the intended ordering is conceptually:

```text
construct and fully validate semantic delta
        ↓
write T2C2 bytes
        ↓
write final T2E2 completion footer
        ↓
durability synchronization required by platform contract
        ↓
only then acknowledge success / expose committed outcome
```

The exact live-filesystem protocol is not accepted until fault injection proves
it. A failure after the durability boundary but before the caller observes
success must be classified as indeterminate and resolved through request
identity on reopen/retry.

## Recovery rule

Recovery may ignore bytes only when it can prove they are an incomplete final
write according to the record framing protocol. It must fail closed on:

- a complete record with bad digest/checksum;
- a complete record with invalid semantic references or topology;
- corruption before the authoritative logical tail;
- unknown format/version/feature bits;
- nonzero garbage inside a region claimed to be zero reserve;
- physical duplicate commits that violate the canonical WAL grammar.

Recovery must never search forward for a later magic value after corruption and
silently resume from it.

## Validate-before-mutate rule

A failed transaction must leave committed semantic state unchanged. Validation
of framing, checksum, geometry, references, commitments, request identity,
checkpoint topology, and resource bounds occurs before publication into the
committed state.

Where an operation cannot naturally be proven fail-atomic, build the change in
a temporary/delta state and publish only after all fallible validation has
succeeded.

## Hostile-input/resource bounds

Persisted values must be range-checked before allocation or conversion to
platform-width integers. Decoder-controlled counts must have explicit capacity
bounds derived from remaining bytes and format limits before
`Vec::with_capacity`, decompression, or large reads.

A corrupt length field must produce a bounded error, not process OOM or an
unbounded allocation attempt.

## Fault-injection acceptance matrix

The live I/O layer must eventually inject failures at every meaningful storage
operation and durability transition, including:

- short write at every record region;
- write error before/during/after commit framing;
- `flush`/`sync_data`/`sync_all` error;
- ENOSPC/quota error;
- truncate/preallocation error;
- rename/replace error;
- directory-sync error;
- process death after each durability transition;
- interrupted seal, WAL recycle, compaction, deletion, migration, and backup.

After each injected failure, immediate reopen must deterministically produce one
of the documented `Rejected`, `Committed`, or `Indeterminate` outcomes. Retained
checkpoint bytes must remain exact; deleted identities must not resurrect; safe
retry must not duplicate logical state.

## Current execution order

Do not resume public Format-v2 write integration until these steps are green:

```text
1. runtime panic/unsafe lint gate
2. audit and remove violations from production paths
3. typed storage error taxonomy
4. injectable live-I/O abstraction
5. syscall-level fault matrix and durability outcome tests
6. then resume v2 public authority/write integration
```

Existing v2 pure codecs, recovery state machines, and semantic structures remain
useful inputs to this work, but they are not production durability evidence by
themselves.
