# Format v2 WAL transaction (`T2W2`)

Status: staged internal design. `T2W2` is not yet accepted by the production
checkpoint-store WAL reader/writer, and public `crate::format::VERSION` remains
1 until migration and dual-version recovery are implemented.

## Decision ledger

**DECISION**

Use a distinct `T2W2` transaction envelope for Format-v2 checkpoint commits.
One transaction atomically carries:

1. newly appended persistent-sequence payload bytes;
2. newly allocated `T2N2` balanced nodes;
3. newly published `T2V2` version records; and
4. exactly one `T2P2` checkpoint record.

The transaction stores the starting payload/node/version/checkpoint watermarks
and a SHA-256 digest over its canonical header prefix and body.

**WHY**

The existing `T2W1` grammar is tied to compact/wide v1 nodes, `T2R1` roots,
`T2P1` checkpoints, and the legacy whole-state XXH3 value. Reusing or
reinterpretating those fields would silently change released Format-v1 bytes.
A distinct grammar also lets recovery distinguish old and new semantics before
parsing representation-specific records.

**ALTERNATIVES REJECTED**

- Extending `T2W1` with reserved flags: rejected because old readers would not
  understand the new node/checkpoint meaning.
- Persisting a complete `T2I2` arena image per append: rejected because append
  cost would grow with the unchanged parent/history.
- Treating the SHA-256 transaction digest as sufficient validation: rejected;
  recovery must also validate topology and section geometry after integrity
  succeeds.

**FORMAT IMPACT**

New Format-v2 bytes only. No v1 magic, checksum, record, manifest, segment, or
checkpoint field changes meaning.

## Canonical envelope

All integers are little-endian. Schema 1 uses a 120-byte header:

```text
0   .. 4    magic = "T2W2"
4   .. 8    total transaction bytes, u32
8   .. 12   transaction schema = 1, u32
12  .. 16   flags = 0, u32
16  .. 24   payload_start, u64
24  .. 32   payload_delta_len, u64
32  .. 40   node_start, u64
40  .. 48   node_delta_count, u64
48  .. 56   version_start, u64
56  .. 64   version_delta_count, u64
64  .. 72   checkpoint_start, u64
72  .. 76   T2P2 checkpoint record bytes, u32
76  .. 80   T2N2 record width = 72, u32
80  .. 84   T2V2 record width = 72, u32
84  .. 88   reserved = 0, u32
88  .. 120  SHA-256 transaction digest
```

The body immediately follows:

```text
[payload delta]
[T2N2 node delta records]
[T2V2 version delta records]
[one T2P2 checkpoint record]
```

The digest is:

```text
SHA256(
    "tulya-checkpoint-v2/wal-transaction\0" ||
    header[0..88] ||
    body
)
```

The digest field itself is excluded.

## Watermark invariants

When decoding against committed geometry `G`, the stored starts must equal
exactly:

```text
payload_start    == G.payload_len
node_start       == G.node_count
version_start    == G.version_count
checkpoint_start == G.checkpoint_count
```

The next committed geometry is obtained only with checked addition. One valid
transaction advances checkpoint count by exactly one. A torn suffix, wrong
starting geometry, overflowing end watermark, unsupported record width, or
trailing byte fails closed.

Version identifiers remain `u32` because `T2V2` uses fixed-width `u32` IDs.
`0xffffffff` remains the reserved optional-version sentinel, so the committed
version count may not advance beyond that sentinel boundary.

## Append-local node invariants

For every new `T2N2` record with global ID `node_start + local_index`:

- a branch child ID must be strictly less than the new node's global ID;
- a leaf payload range must lie wholly in the transaction's new payload delta;
- new leaf ranges, sorted by offset, must exactly and contiguously cover the
  complete payload delta with no gap, overlap, or unowned byte;
- a local `T2V2` root may reference an old or newly allocated node, but if it
  references a newly allocated node its persisted root metadata must exactly
  equal that node's length, height, and commitment.

These checks are independent of the outer transaction digest.

Validation that requires metadata from already committed old nodes/versions is
a later store-state integration responsibility. In particular, the staged
codec cannot recompute a checkpoint state commitment when the `T2P2` record
references versions that predate the transaction; production apply/recovery
must resolve those version roots and verify the `T2P2` state commitment before
publication.

## Zero-delta checkpoint forks

A checkpoint fork that reuses already committed channel versions is valid with:

```text
payload_delta_len = 0
node_delta_count = 0
version_delta_count = 0
```

Only the new `T2P2` record and transaction framing are written. This preserves
metadata-only fork cost rather than manufacturing duplicate sequence nodes.

## Recovery rule

A future Format-v2 hot-WAL scanner may treat an incomplete final transaction as
an uncommitted suffix. Any complete record selected as committed must pass, in
order:

1. header/length/schema validation;
2. SHA-256 transaction integrity;
3. expected starting-watermark validation;
4. node/version/checkpoint canonical decoding;
5. append-local topology/ownership validation; and
6. store-state validation of old references and checkpoint commitment.

No `T2W2` bytes become authoritative merely because their digest is valid.
