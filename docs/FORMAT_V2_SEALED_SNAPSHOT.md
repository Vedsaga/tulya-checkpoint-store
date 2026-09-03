# Format v2 sealed semantic snapshot (`T2S2`)

Status: staged internal design. `T2S2` is not yet a published checkpoint-store
artifact, and public `crate::format::VERSION` remains 1.

## Decision ledger

**DECISION**

Represent one immutable Format-v2 sealed/base state as a canonical `T2S2`
snapshot containing:

```text
T2S2
├── exact T2I2 persistent-sequence arena image
├── ordered T2V2 version records
├── ordered T2P2 checkpoint records
├── active request ledger
└── retired request ledger
```

The `T2I2` retained-root table and the `T2V2` version table correspond
one-for-one in version-ID order. `T2S2` has an outer SHA-256 digest, but reopen
also independently revalidates the nested arena, version roots, checkpoint
state commitments/topology, and request-ledger semantics.

**WHY**

The accepted v2 hot-WAL scanner reconstructs only an unsealed suffix. A real
v2 reopen path also needs an immutable semantic base that is independent of the
released v1 22-stream `Manifest`/`StoreState` representation.

Reusing `T2I2`, `T2V2`, and `T2P2` keeps sequence representation, version
history, and checkpoint semantics at their existing validated boundaries. It
also prevents production integration from forcing balanced v2 state into v1
stream structures merely to reuse current code.

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
- Trust one outer snapshot checksum as semantic verification: rejected because
  a recomputed valid digest must not bless inconsistent roots, checkpoint
  commitments, parent topology, or request ledgers.
- Snapshot the complete arena on every foreground append: rejected because it
  violates the O(log n + delta) foreground-write target.

**FORMAT IMPACT**

New staged Format-v2 sealed bytes only. `T2I2`, `T2V2`, `T2P2`, `T2W2`,
`T2C2`, `T2E2`, and all released Format-v1 bytes remain unchanged.

## Canonical header

All integers are little-endian. Schema 1 uses a 96-byte header:

```text
0   .. 4    magic = "T2S2"
4   .. 8    schema = 1, u32
8   .. 16   total snapshot bytes, u64
16  .. 24   nested T2I2 image bytes, u64
24  .. 32   version count, u64
32  .. 40   checkpoint count, u64
40  .. 48   active request count, u64
48  .. 56   retired request count, u64
56  .. 60   T2V2 record width, u32
60  .. 64   reserved = 0, u32
64  .. 96   snapshot digest, [32]u8
```

The body is exactly:

```text
[exact T2I2 image]
[version_count fixed-width T2V2 records]
[checkpoint_count self-framing T2P2 records]
[active request records, strict lexical request-id order]
[retired request records, strict lexical request-id order]
```

The digest is:

```text
SHA256(
    "tulya-checkpoint-v2/sealed-snapshot\0" ||
    header[0..64] ||
    body
)
```

The digest field itself is excluded.

An empty store has no `T2S2` artifact. A present snapshot contains at least one
version and one checkpoint.

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

## Reopen invariants

After the outer digest and framing pass, import still proves:

1. `T2I2` independently imports and verifies every payload/node/root record.
2. The number of retained `T2I2` roots exactly equals the version count.
3. `T2V2` IDs are sequential and each version root exactly equals the
   corresponding `T2I2` retained root.
4. Every checkpoint channel version resolves in the version table.
5. Recomputing `checkpoint_state_metadata` from those resolved roots exactly
   equals each stored `T2P2` state length/commitment.
6. `(thread_id, checkpoint_id)` identities are unique.
7. A checkpoint parent, when present, already exists in the same thread.
8. Each active request checkpoint ordinal resolves and its stored operation
   digest exactly equals `checkpoint_operation_digest` for that checkpoint.
9. Active/retired request sets are valid, unique, disjoint, and canonically
   ordered on decode.

The outer digest therefore detects accidental byte corruption quickly but is
not treated as a semantic proof.

## Frozen one-checkpoint vector

For one arena leaf containing `abc`, version 0, checkpoint `(thread, cp-1)`, and
active request `req-1`, the canonical snapshot is 530 bytes. Its independently
calculated snapshot digest is:

```text
f31c62e5f4c884424addf7dcd6716c633a0e738be495f6de476ec70401db7516
```

The checkpoint uses the already-frozen logical-operation digest:

```text
6e0363445809801219e0a146b177509d7a74ff7cbff1ee35013d94ad433e9eda
```

## Current boundary

This unit does not yet:

- publish `T2S2` files from `CheckpointStore`;
- define the public v2 manifest's generation/file metadata;
- reconstruct `V2CommittedState` from a `T2S2` snapshot plus hot frames;
- normalize an interrupted v2 seal/recycle;
- migrate a v1 store to v2;
- change the default writer or `crate::format::VERSION`; or
- claim crash durability before the live filesystem path is fault-tested.

Those are subsequent integration gates. The next implementation boundary is a
v2 backend-state import/export layer that combines one validated `T2S2` base
with the already-accepted hot-frame recovery scanner.