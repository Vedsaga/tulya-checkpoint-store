# Persistent AVL image and reopen contract

Status: internal Format-v2 persistence boundary for the balanced sequence core.
This does **not** change the public checkpoint-store format, WAL grammar,
manifest version, or `crate::format::VERSION`.

The implementation lives in:

- `src/persistent_sequence/image_v2.rs` — canonical `T2I2` arena-image framing;
- `src/persistent_sequence/avl.rs` — semantic reconstruction and verification.

## Decision ledger

### DECISION

Persist one complete append-only AVL arena as three canonical sections:

1. payload bytes in allocation order;
2. the complete fixed-width `T2N2` node-record table; and
3. an explicit retained-root table of `T2R2` records.

A fixed `T2I2` header carries section counts/widths and a SHA-256 digest over
its canonical header prefix plus the complete body.

### WHY

This gives the balanced representation a concrete reopen boundary before it is
integrated with checkpoint WAL/sealed-generation publication. Reopen can prove
that physical node IDs, payload offsets, logical lengths, heights, commitments,
and historical roots survive serialization exactly.

The outer image digest detects accidental framing/body corruption quickly. It
is not trusted as the tree verifier: import independently recomputes every leaf
commitment and every branch record from earlier child nodes.

### ALTERNATIVES REJECTED

- Reuse Format-v1 WAL records for v2 nodes: that would reinterpret a released
  representation and violate the format-version boundary.
- Serialize Rust structs with Serde/Bincode: committed bytes must have an
  explicit canonical codec and bounded decoder.
- Trust only the image SHA-256: a digest-valid image can still contain invalid
  topology if produced by a buggy or hostile writer.
- Persist only the latest root: historical roots and sibling branches are part
  of the checkpoint-store contract and must survive reopen.

### FORMAT IMPACT

None on the public store yet. `T2I2` is an internal staged image used to prove
v2 persistence/recovery semantics. The production integration step must decide
how these logical sections map into the existing durable WAL + immutable sealed
lifecycle and must publish Format v2 explicitly before any such bytes become a
public compatibility contract.

## Canonical image layout

The header is 72 bytes:

```text
0..4    magic = T2I2
4..8    image version = 2 (u32 LE)
8..16   payload length (u64 LE)
16..24  node count (u64 LE)
24..32  retained-root count (u64 LE)
32..36  node record width (u32 LE)
36..40  root record width (u32 LE)
40..72  SHA-256 image digest
```

The body is exactly:

```text
payload bytes || node records || retained-root records
```

The digest is domain separated:

```text
SHA256(
  "tulya-sequence-v2/image\0" ||
  header[0..40] ||
  body
)
```

The decoder performs checked length arithmetic before allocating section
vectors and rejects unsupported/truncated/inconsistent framing.

## Reopen invariants

Import accepts an image only when all of the following hold:

1. the outer `T2I2` framing and digest are valid;
2. every embedded `T2N2` and `T2R2` record is canonically decodable;
3. leaf payload ranges are contiguous in append-allocation order, with no
   overlap, gap, or unowned trailing payload bytes;
4. every branch child references an **earlier** arena node, preventing forward
   references and cycles;
5. every persisted branch left-length equals the actual left child length;
6. every leaf record/commitment recomputes from its exact payload bytes;
7. every branch record/commitment/height recomputes from its child roots; and
8. every retained-root record agrees exactly with the reconstructed arena node.

Because nodes are reconstructed in node-ID order and branches may reference
only earlier nodes, semantic verification is linear in the image size rather
than recursive verification of every historical root separately.

## Test contract

The unit tests cover:

- exact image encode/decode round trip;
- truncation rejection;
- payload/body corruption rejection by the outer image digest;
- canonical projection of leaf and branch record fields;
- historical-root and sibling preservation across AVL export/import;
- successful append after reopen using the recovered arena; and
- semantic branch corruption with a recomputed valid outer digest, proving that
  recovery fails closed on topology rather than relying only on SHA-256.

This unit establishes serialization/reopen correctness for the pure balanced
arena. Durable acknowledgement, crash atomic publication, migration from v1,
and sealed-generation integration remain later production gates.
