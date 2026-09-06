# Format v2 hot-WAL recovery

Status: staged internal design. This scanner is not yet wired into
`CheckpointStore::open`, and public `crate::format::VERSION` remains 1.

## Decision ledger

**DECISION**

Recover Format-v2 hot history by scanning `T2C2` logical commits wrapped in a
physical hot-WAL completion frame. The frame appends a fixed 40-byte `T2E2`
footer after each complete `T2C2`:

```text
0  .. 4   magic = "T2E2"
4  .. 8   complete frame bytes, u32
8  .. 40  exact T2C2 commit digest, [32]u8
```

The footer is written last. A `T2C2` is eligible for recovery only when its
canonical footer is complete. The scanner then applies the `T2C2` through the
already-validated committed-state apply boundary. A zero-filled reserve suffix
terminates the logical WAL.

**WHY**

`hot.wal` is physically preinitialized with zeros. After a process dies during
a foreground write, the file can therefore still be long enough to cover the
length advertised by a partially written `T2C2`; its unwritten remainder is
simply the old zero reserve. Length alone cannot prove that the append reached
the end of the record.

`T2E2` provides that proof without changing the already-accepted `T2C2` or
`T2W2` bytes. Because the footer follows the commit in the same sequential
append buffer, recovery can distinguish a zero-padded partial write from a
record whose physical write reached its canonical end marker.

`T2C2` remains the logical Format-v2 authority unit: request identity, operation
digest, transaction integrity, topology, and state semantics all remain inside
or are validated from `T2C2`. `T2E2` has no independent semantic meaning.

**ALTERNATIVES REJECTED**

- Infer completeness only from `T2C2.total_len`: rejected because the
  preinitialized zero reserve makes the physical file longer than a torn
  logical append.
- Treat every `T2W2` as recoverable: rejected because request identity and the
  logical authority boundary live in `T2C2`.
- Change the accepted `T2C2` envelope itself: rejected because a separate
  physical completion footer solves the crash-framing problem without changing
  its logical bytes.
- Accept a second physical commit for an exact request retry: rejected. API
  idempotency must prevent that frame from being written; finding one in the
  authoritative WAL is noncanonical persisted state.
- Ignore a complete framed record that fails integrity/topology checks: rejected.
  Once `T2E2` proves the write reached the end, validation failure is corruption,
  not a truncation hint.
- Guess a logical tail after arbitrary nonzero bytes in the zero reserve:
  rejected because malformed reserve bytes fail closed.

**FORMAT IMPACT**

New staged Format-v2 hot-file framing only. `T2C2`, `T2W2`, and released Format
v1 bytes are unchanged. Public `crate::format::VERSION` remains 1.

## Completion-footer rules

For a candidate `T2C2` at the recovery cursor:

1. Read its declared commit length once the first eight bytes are available.
2. The physical frame length is `commit_len + 40`, checked for overflow.
3. The expected footer is `T2E2 || frame_len:u32_le || T2C2.commit_digest`.
4. A footer that is exactly a canonical prefix followed only by zero reserve is
   treated as a torn final write.
5. A complete footer that differs from the expected footer is corruption and
   fails closed.
6. Only an exact complete footer allows the scanner to validate/apply the
   enclosed `T2C2`.

The prefix rule intentionally searches for a canonical-prefix boundary; it does
not treat the first zero byte as the write boundary because the little-endian
frame length and digest may themselves legitimately contain zeros.

## Scan rules

Starting from the first unsealed v2 byte:

1. If the remaining bytes begin with the zero reserve and all remaining bytes
   are zero, stop. The current cursor is the logical tail.
2. Bare `T2W2` fails closed.
3. Probe the next `T2C2 + T2E2` frame. A zero-padded partial commit/footer stops
   before the frame as `TornFinalCommit`.
4. For a complete frame, the full `T2C2` commit/WAL/apply validation must
   succeed. Complete corruption is an error, not a truncation hint.
5. A successful physical record must produce `Applied`. `Replayed` and
   `RetiredRequest` outcomes are invalid in an authoritative WAL.
6. Advance by exactly the complete frame length and continue.

The recovery result records the exact logical tail, number of applied hot
commits, stop reason, and reconstructed committed state.

## Atomicity interpretation

For a crash during the final append:

```text
complete previous frames || partial T2C2/T2E2 || zero reserve
```

recovery exposes exactly the complete previous state. If the final `T2E2` is
complete, the enclosed `T2C2` must validate completely and then becomes visible.
No intermediate `T2W2`, payload/node/version subset, request-ledger subset, or
footer alone is independently authoritative.

## Current boundary

This unit does not yet:

- write framed v2 commits through `CheckpointStore::HotWal`;
- read the WAL directly from a `File` without first materializing bytes;
- use a manifest-provided sealed-prefix geometry;
- load a sealed Format-v2 generation;
- normalize a previous-generation v2 WAL after an interrupted seal/recycle;
- dispatch from `CheckpointStore::open`; or
- claim crash durability until the live filesystem path is fault-tested.

Those are integration units after this scanner is accepted.
