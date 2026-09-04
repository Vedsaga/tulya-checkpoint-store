# Format-v2 persistent-sequence metadata

Status: design + executable internal codec. This document does **not** publish
Format v2 as the current public store format. `crate::format::VERSION` remains
`1` until the writable v2 path, recovery, migration, and compatibility gates are
complete.

This design exists to remove the two Format-v1 locality blockers:

1. a checkpoint append must not read/hash the complete unchanged parent; and
2. repeated appends must not create an indefinitely left-deep history.

The design is informed by the persistent AVL interfaces in
`Vedsaga/Tulya-MDL-Lean`. It is not a claim that the Rust codec or future Rust
AVL implementation is mechanically proved to refine the Lean development.

## Decision ledger

### DECISION

Format v2 uses a persistent balanced binary sequence whose root metadata carries:

- physical root node ID (`u64`);
- exact logical byte length (`u64`);
- AVL height (`u16`); and
- a 256-bit domain-separated SHA-256 structural/content commitment.

Every branch node persists its total logical length and its left-subtree logical
length. Every leaf persists its payload offset and payload length. Physical node
IDs and payload offsets are deliberately excluded from the commitment so that
physical relocation does not alter logical integrity.

### WHY

A parent root now contains enough information to append a delta without
reconstructing the parent value. A branch commitment can be recomputed from its
children's fixed-size metadata, so an append or AVL rotation hashes only the new
leaf plus the O(log n) changed ancestor path.

SHA-256 is already a runtime dependency of this crate. Reusing it avoids adding
a new production dependency while providing a conventional 256-bit
collision-resistant integrity assumption. As with any practical digest, this is
an engineering assumption rather than Lean checksum injectivity.

### ALTERNATIVES REJECTED

- **Incremental XXH3 over concatenation:** rejected because the released v1
  whole-state XXH3 value is not a composable concatenation commitment.
- **Whole-state SHA-256:** cryptographically stronger than XXH3 but still
  O(parent) for every child append, so it does not solve the locality problem.
- **A custom associative rolling hash:** rejected because inventing a new hash
  algebra would increase collision-analysis and implementation risk.
- **BLAKE3 for this first v2 codec:** technically suitable and used as the
  runtime integrity primitive in the Lean repository's Rust implementation
  plan, but not required for the locality property. The existing SHA-256
  dependency gives the needed cryptographic commitment without dependency
  churn. Changing the digest after writable v2 bytes exist would be a format
  change, so this decision must be reviewed before v2 publication.

### FORMAT IMPACT

Format-v1 bytes are never reinterpreted. The new records use distinct magics:

```text
T2N2  Format-v2 sequence node
T2R2  Format-v2 sequence root metadata
```

The current public format version remains 1 until a migration/publication step
explicitly changes it.

## Commitment construction

All integers are canonical little-endian fixed-width values.

A leaf commitment is:

```text
SHA256(
  "tulya-sequence-v2/leaf\0" ||
  logical_len:u64_le ||
  exact_payload_bytes
)
```

A branch commitment is:

```text
SHA256(
  "tulya-sequence-v2/branch\0" ||
  height:u16_le ||
  left_len:u64_le ||
  left_commitment:[u8;32] ||
  right_len:u64_le ||
  right_commitment:[u8;32]
)
```

This is a tree/Merkle commitment, not a canonical whole-byte digest. It binds
the logical tree shape and heights, so a different valid balancing shape for
the same byte sequence can have a different commitment. That is intentional:
verification checks the exact persistent structure. The commitment excludes
physical node IDs and payload offsets, allowing relocation/compaction while
preserving the committed logical node when the logical tree is preserved.

## Node record (`T2N2`)

The node record is exactly 72 bytes.

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 4 | magic = `T2N2` |
| 4 | 1 | kind: `1=leaf`, `2=branch` |
| 5 | 1 | flags, must be zero |
| 6 | 2 | AVL height, little-endian |
| 8 | 8 | total logical length |
| 16 | 8 | field A |
| 24 | 8 | field B |
| 32 | 8 | field C |
| 40 | 32 | commitment |

Leaf interpretation:

```text
height = 1
logical_len > 0
field A = payload offset
field B = payload length = logical_len
field C = 0
```

The checked range `payload_offset + payload_len` must fit `u64`.

Branch interpretation:

```text
height >= 2
logical_len > 0
field A = left child node ID
field B = right child node ID
field C = left logical length
0 < left_len < logical_len
right_len = logical_len - left_len
```

`u64::MAX` is reserved as the absent-node sentinel and is invalid for a live
root or branch child.

The codec validates invariants available from one record. Cross-node invariants
such as child existence, `height = 1 + max(child heights)`, AVL balance,
child-length agreement, and recomputed commitments belong to the structural
verifier once the v2 arena is wired.

## Root record (`T2R2`)

The root record is exactly 56 bytes.

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 4 | magic = `T2R2` |
| 4 | 1 | representation = `1` (balanced v2) |
| 5 | 1 | flags, must be zero |
| 6 | 2 | root AVL height |
| 8 | 8 | root node ID |
| 16 | 8 | logical length |
| 24 | 32 | root commitment |

A live root requires non-zero logical length, positive height, and a node ID
other than `u64::MAX`. The higher-level sequence API represents absence of a
parent/root with `Option`; no zero-length physical leaf is encoded.

## Failure-closed decoding

The decoder accepts exactly one canonical fixed-width record and rejects:

- truncated or oversized records;
- wrong magic or unsupported kind/representation;
- non-zero reserved flags/fields;
- invalid leaf/branch/root heights;
- zero-length leaves/roots;
- inconsistent leaf payload length;
- invalid branch left-subtree splits;
- reserved absent-node IDs; and
- checked integer-range overflow.

Stored commitments are parsed but are not trusted merely because a record is
well-formed. The future `verify(root)` implementation must reload referenced
children/payload, recompute lengths/heights/commitments, and fail closed on any
mismatch.

## Golden vectors

The internal unit tests freeze canonical byte vectors for:

- leaf payload `abc` at payload offset 9;
- its root at node ID 7;
- a two-leaf branch (`abc` + `XYZ`); and
- that branch's root.

They also mutate canonical records to prove failure-closed decoding and exercise
checked balance/length construction failures.

## Next implementation boundary

This codec is intentionally not enough to mark production-readiness Sections 6
or 7 complete. The next storage unit must:

1. add the writable v2 sequence arena/edit abstraction;
2. perform persistent AVL append/rotations without mutating historical roots;
3. make the codec a real production caller and remove its temporary
   `dead_code` staging allowance;
4. implement exact v2 range reads using persisted `left_len` metadata; and
5. add structural verification before any v2 bytes become authoritative.

Only after durability/recovery/migration and locality evidence are complete may
`crate::format::VERSION` advance and the corresponding readiness boxes be
checked.
