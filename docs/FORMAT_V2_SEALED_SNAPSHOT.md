# Format v2 sealed semantic snapshot (`T2S2`)

Status: staged internal design. `T2S2` is not yet a published checkpoint-store
artifact, and public `crate::format::VERSION` remains 1.

Schema 2 supersedes the earlier staged schema 1 before any public Format-v2
authority exists. Schema 1 did not persist deleted checkpoint identities and is
therefore rejected rather than silently reinterpreted.

## Decision ledger

**DECISION**

Represent one immutable Format-v2 sealed/base state as a canonical `T2S2`
snapshot containing:

```text
T2S2 schema 2
├── optional exact T2I2 persistent-sequence arena image
├── ordered T2V2 version records
├── ordered T2P2 live checkpoint records
├── active request ledger
├── retired request ledger
└── deleted-checkpoint tombstones
```

For live state, the `T2I2` retained-root table and the `T2V2` version table
correspond one-for-one in version-ID order. `T2S2` has an outer SHA-256 digest,
but reopen also independently revalidates the nested arena, version roots,
checkpoint state commitments/topology, request-ledger semantics, and deletion
tombstones.

A state with no live checkpoints may still require a `T2S2` artifact when
retired request identities or deleted checkpoint identities remain durable.
Such a tombstone-only snapshot has no `T2I2` image and zero sequence geometry.
A genuinely new/never-used empty state has no snapshot artifact.

**WHY**

The accepted v2 hot-WAL scanner reconstructs only an unsealed suffix. A real
v2 reopen path also needs an immutable semantic base that is independent of the
released v1 22-stream `Manifest`/`StoreState` representation.

Reusing `T2I2`, `T2V2`, and `T2P2` keeps sequence representation, version
history, and checkpoint semantics at their existing validated boundaries. It
also prevents production integration from forcing balanced v2 state into v1
stream structures merely to reuse current code.

Deletion is a semantic state transition, not merely physical reclamation.
After a checkpoint is logically deleted, reopening a compacted/tombstone-only
store must still reject stale attempts to recreate that exact checkpoint
identity. Persisting only retired request IDs is insufficient because not every
checkpoint is required to have a request ID.

`T2S2` is deliberately an O(total sealed state) sealing/reopen artifact. The
foreground `T2C2 + T2E2` path remains append-local and must not rewrite this
snapshot per checkpoint.

**ALTERNATIVES REJECTED**

- Reuse the v1 22-stream sealed representation for v2: rejected because it
  would reinterpret released stream semantics and the v1 `state_hash_u64`
  field.
- Store only `T2I2` and reconstruct versions/checkpoints from node roots:
  rejected because version ancestry, checkpoint identifiers/channel selection,
  and request idempotency are semantic metadata, not derivable from the arena.
- Persist only retired request IDs after deletion: rejected because checkpoint
  identity itself must remain non-resurrectable even for requestless commits.
- Treat zero live geometry as equivalent to a brand-new store: rejected because
  deletion tombstones may still be authoritative.
- Silently extend staged schema 1 with tombstone semantics: rejected in favor
  of an explicit schema bump and fail-closed schema-1 rejection.
- Trust one outer snapshot checksum as semantic verification: rejected because
  a recomputed valid digest must not bless inconsistent roots, checkpoint
  commitments, parent topology, request ledgers, or live/deleted identity
  overlap.
- Snapshot the complete arena on every foreground append: rejected because it
  violates the O(log n + delta) foreground-write target.

**FORMAT IMPACT**

`T2S2` staged schema is now 2. The earlier internal schema 1 is intentionally
unsupported before public v2 publication. `T2I2`, `T2V2`, `T2P2`, `T2W2`,
`T2C2`, `T2E2`, and all released Format-v1 bytes remain unchanged.

## Canonical header

All integers are little-endian. Schema 2 keeps the 96-byte header:

```text
0   .. 4    magic = "T2S2"
4   .. 8    schema = 2, u32
8   .. 16   total snapshot bytes, u64
16  .. 24   nested T2I2 image bytes, u64
24  .. 32   version count, u64
32  .. 40   live checkpoint count, u64
40  .. 48   active request count, u64
48  .. 56   retired request count, u64
56  .. 60   T2V2 record width, u32 (0 when version count is 0)
60  .. 64   reserved = 0, u32
64  .. 96   snapshot digest, [32]u8
```

The body is exactly:

```text
[exact T2I2 image, absent for tombstone-only state]
[version_count fixed-width T2V2 records]
[checkpoint_count self-framing T2P2 records]
[active request records, strict lexical request-id order]
[retired request records, strict lexical request-id order]
[zero or more T2X2 deletion tombstones, strict (thread,id) lexical order]
```

The fixed request-ledger counts let the decoder know exactly where the trailing
`tombstone-to-end-of-body` section begins. Every trailing record must be a
canonical `T2X2`; arbitrary extra bytes still fail closed.

The digest is:

```text
SHA256(
    "tulya-checkpoint-v2/sealed-snapshot\0" ||
    header[0..64] ||
    body
)
```

The digest field itself is excluded.

## Request-ledger records

Active request records use `T2A2`:

```text
0   .. 4    magic = "T2A2"
4   .. 8    record bytes, u32
8   .. 16   checkpoint ordinal, u64
16  .. 20   request-id bytes, u32
20  .. 24   flags/reserved = 0, u32
24  .. 56   logical-operation digest, [32]u8
56  ..      request-id bytes
```

Retired request records use `T2D2`:

```text
0   .. 4    magic = "T2D2"
4   .. 8    record bytes, u32
8   .. 12   request-id bytes, u32
12  .. 16   flags/reserved = 0, u32
16  .. 48   logical-operation digest, [32]u8
48  ..      request-id bytes
```

Request IDs are opaque 1..=4096-byte values. The encoder sorts both ledgers
lexically so semantically identical ledger maps have deterministic bytes.
Active and retired identities must be individually unique and mutually
disjoint.

## Deleted-checkpoint record

Deleted checkpoint identities use `T2X2`:

```text
0   .. 4    magic = "T2X2"
4   .. 8    record bytes, u32
8   .. 12   thread-id bytes, u32
12  .. 16   checkpoint-id bytes, u32
16  ..      UTF-8 thread-id bytes followed by UTF-8 checkpoint-id bytes
```

Both identifiers are 1..=4096 bytes. Tombstones are sorted by
`(thread_id, checkpoint_id)` and must be unique. A checkpoint identity cannot
be both live and deleted.

## Reopen invariants

After the outer digest and framing pass, import still proves:

1. With live checkpoints, `T2I2` independently imports and verifies every
   payload/node/root record.
2. With live checkpoints, the number of retained `T2I2` roots exactly equals
   the version count.
3. `T2V2` IDs are sequential and each version root exactly equals the
   corresponding `T2I2` retained root.
4. Every live checkpoint channel version resolves in the version table.
5. Recomputing `checkpoint_state_metadata` from those resolved roots exactly
   equals each stored `T2P2` state length/commitment.
6. Live `(thread_id, checkpoint_id)` identities are unique.
7. A live checkpoint parent, when present, already exists earlier in the same
   thread.
8. Each active request checkpoint ordinal resolves and its stored operation
   digest exactly equals `checkpoint_operation_digest` for that checkpoint.
9. Active/retired request sets are valid, unique, disjoint, and canonically
   ordered on decode.
10. Deleted checkpoint identities are valid, unique, canonically ordered, and
    disjoint from all live checkpoint identities.
11. A tombstone-only snapshot has no image, versions, live checkpoints, or
    active requests. It must contain at least one retired request or deletion
    tombstone; otherwise the canonical representation is no snapshot at all.
12. Schema 1 and unknown future schema values fail closed.

The outer digest therefore detects accidental byte corruption quickly but is
not treated as a semantic proof.

## Frozen vectors

For one arena leaf containing `abc`, version 0, checkpoint `(thread, cp-1)`, and
active request `req-1`, the canonical schema-2 snapshot is 530 bytes. Its
independently calculated snapshot digest is:

```text
ae8abba06121c8c689ed2b3cf822fc750b184f373e0b947a9c0ab9ad448d0cfc
```

The checkpoint uses the already-frozen logical-operation digest:

```text
6e0363445809801219e0a146b177509d7a74ff7cbff1ee35013d94ad433e9eda
```

A tombstone-only snapshot containing exactly deleted checkpoint
`(thread, cp-old)` is 124 bytes with independently calculated digest:

```text
6a63a9f5cace063e835d3f61b4886d2a4a3fd6a95ac6f34162baf4c149825782
```

## Current boundary

This unit does not yet:

- publish `T2S2` files from `CheckpointStore`;
- define the public v2 manifest's generation/file metadata;
- implement the production delete/compaction operation that creates tombstones;
- normalize an interrupted v2 seal/recycle;
- migrate a v1 store to v2;
- change the default writer or `crate::format::VERSION`; or
- claim crash durability before the live filesystem path is fault-tested.

The staged backend-state layer now combines a validated schema-2 `T2S2` base
with the accepted hot-frame recovery scanner. Public filesystem authority is a
subsequent integration gate.
