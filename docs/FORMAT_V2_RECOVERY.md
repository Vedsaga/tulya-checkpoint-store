# Format v2 hot-WAL recovery

Status: staged internal design. This scanner is not yet wired into
`CheckpointStore::open`, and public `crate::format::VERSION` remains 1.

## Decision ledger

**DECISION**

Recover Format-v2 hot history by scanning authoritative outer `T2C2` records
in physical order and applying each record through the already-validated
committed-state apply boundary. A zero-filled reserve suffix terminates the
logical WAL. A final `T2C2` whose complete framed length is not present is an
uncommitted torn suffix and is ignored.

**WHY**

`T2C2` is the Format-v2 authority unit. Recovery therefore must not publish an
inner `T2W2` by itself, and it must reuse the same old-reference, state
commitment, parent, and request validation used by foreground apply. Keeping the
scanner filesystem-neutral lets framing/replay invariants be tested before the
production open path chooses v2.

**ALTERNATIVES REJECTED**

- Treat every `T2W2` as recoverable: rejected because request identity and the
  authority boundary live in `T2C2`.
- Accept a second physical commit for an exact request retry: rejected. API
  idempotency must prevent that record from being written; finding one in the
  authoritative WAL is noncanonical persisted state.
- Ignore complete records that fail integrity/topology checks: rejected because
  only an incomplete final record is allowed to behave as an uncommitted
  suffix.
- Guess a logical tail after arbitrary nonzero bytes in the zero reserve:
  rejected because malformed reserve bytes fail closed.

**FORMAT IMPACT**

None. The scanner consumes the staged `T2C2`/`T2W2` formats without changing
their bytes. Released Format v1 remains unchanged.

## Scan rules

Starting from the first unsealed v2 byte:

1. If the remaining bytes begin with the zero reserve and all remaining bytes
   are zero, stop. The current cursor is the logical tail.
2. If fewer than four bytes remain and they are a prefix of `T2C2`, stop as a
   torn final commit.
3. Otherwise the next outer magic must be `T2C2`. Bare `T2W2` fails closed.
4. Once the length field is available, it must be at least the 88-byte `T2C2`
   header size.
5. If that framed length extends beyond available bytes, stop before the record
   as a torn final commit.
6. If the complete framed record is present, its full commit/WAL/apply
   validation must succeed. Complete corruption is an error, not a truncation
   hint.
7. A successful physical record must produce `Applied`. `Replayed` and
   `RetiredRequest` outcomes are invalid in an authoritative WAL.
8. Advance by exactly the framed record length and continue.

The recovery result records the exact logical tail, number of applied hot
commits, stop reason, and reconstructed committed state.

## Atomicity interpretation

For a crash during the final append:

```text
complete previous commits || partial final T2C2 || zero reserve
```

recovery exposes exactly the complete previous state. If the final `T2C2` is
complete, it must validate completely and then becomes visible. No intermediate
`T2W2`, payload/node/version subset, or request-ledger subset is independently
authoritative.

## Current boundary

This unit does not yet:

- read the WAL directly from a `File`;
- use a manifest-provided sealed-prefix geometry;
- load a sealed Format-v2 generation;
- normalize a previous-generation v2 WAL after an interrupted seal/recycle;
- dispatch from `CheckpointStore::open`; or
- claim crash durability until the live filesystem path is fault-tested.

Those are integration units after this scanner is accepted.
