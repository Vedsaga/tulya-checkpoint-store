# Format-v2 publication records

Status: staged internal Format-v2 publication design. These records are not yet
written by `CheckpointStore`, and public Format v1 remains the only accepted
store format until WAL, sealed-generation, migration, and recovery integration
are complete.

This unit freezes the metadata that sits between the balanced persistent
sequence and a future Format-v2 transaction:

- `T2R2`: one balanced persistent-sequence root (already defined);
- `T2V2`: one logical version plus its parent-version edge and `T2R2` root;
- `T2P2`: one checkpoint record plus its canonical-state metadata.

The next unit will wrap deltas of these records in a distinct `T2W2` WAL
transaction. Existing `T2W1`, `T2R1`, and `T2P1` bytes are never reinterpreted.

## Decision ledger

### DECISION

Format v2 keeps checkpoint semantics separate from physical tree placement.
Each channel version publishes a `T2R2` root containing logical length, height,
and a 32-byte Merkle commitment. A `T2V2` record adds only version identity and
its historical parent edge.

Each `T2P2` checkpoint stores:

- checkpoint number and string identifiers;
- identity/messages/result version references;
- canonical-state schema identifier;
- exact canonical-state logical byte length; and
- a 32-byte checkpoint-state commitment derived from channel root metadata.

The v2 checkpoint-state commitment is SHA-256 with domain separation. Physical
node IDs are deliberately excluded from that commitment.

### WHY

Format v1 stores an XXH3-64 hash of the fully reconstructed canonical state.
That checksum is exact for v1 but cannot be updated from `parent_hash + delta`
without rescanning the parent bytes. Format v2 therefore needs a different,
explicit semantic commitment rather than pretending XXH3 concatenation is
composable.

Channel roots already provide content/structure commitments that can be updated
with O(log n + delta) work. The checkpoint commitment composes those roots and
fixed canonical framing, so message append can derive child metadata without
materializing or hashing the unchanged parent state.

### ALTERNATIVES REJECTED

- Reuse the v1 `state_hash` field with a different meaning: silently breaks the
  released Format-v1 compatibility contract.
- Store only a logical length and no commitment: insufficient integrity metadata
  for fail-closed recovery and verification.
- Put physical node IDs into the checkpoint-state commitment: compaction or
  relocation would change logical integrity metadata even when bytes do not.
- Hash the complete canonical JSON on every append: preserves v1 behavior but
  keeps foreground work O(parent).
- Remove channel-version records and store only checkpoint roots: loses explicit
  historical version parentage used by branching/reclamation semantics.

### FORMAT IMPACT

None yet on authoritative stores. Public `crate::format::VERSION` remains 1.
`CheckpointStore::open` continues to accept only the existing v1 manifest.
These staged records become public Format-v2 bytes only after the separate
`T2W2` WAL, sealed representation, manifest dispatch, migration, and crash
matrix are implemented and accepted.

## Canonical checkpoint schema

The current canonical read semantics remain:

```text
{"identity":<identity>,"messages":[<messages>]}
```

and, when a result channel exists:

```text
{"identity":<identity>,"messages":[<messages>],"result":<result>}
```

`messages` contains the already-comma-separated logical message sequence stored
by its persistent root. An absent messages version therefore means an empty
message sequence, not a different JSON schema.

For schema id `1`, exact canonical length is:

```text
27
+ identity_root.logical_len
+ messages_root.logical_len_or_zero
+ if result exists { 10 + result_root.logical_len } else { 0 }
```

The constants are the exact byte widths of the fixed JSON framing.

## Checkpoint-state commitment

For schema id `1`:

```text
SHA256(
  "tulya-checkpoint-v2/state\0"
  || schema_id:u32_le
  || canonical_logical_len:u64_le
  || identity_len:u64_le
  || identity_commitment:[32]
  || messages_present:u8
  || if present { messages_len:u64_le || messages_commitment:[32] }
  || result_present:u8
  || if present { result_len:u64_le || result_commitment:[32] }
)
```

The commitment intentionally excludes:

- root/node IDs;
- payload offsets;
- WAL offsets;
- segment generation; and
- compression/block placement.

Those are physical representation details. The channel root commitments already
bind logical content and the balanced structural metadata required by Format v2.

## `T2V2` version record

Fixed width: 72 bytes.

```text
0..4    magic = T2V2
4..8    version id (u32 LE)
8..12   parent version id, or 0xffffffff (u32 LE)
12..16  flags = 0
16..72  canonical T2R2 root record
```

Requirements:

- version IDs are sequential in publication order;
- a present parent version is strictly smaller than the version id;
- the embedded `T2R2` decoder must accept the root;
- reserved flags fail closed.

## `T2P2` checkpoint record

Fixed prefix: 88 bytes, followed by exact UTF-8 identifier bytes.

```text
0..4    magic = T2P2
4..8    complete record bytes (u32 LE)
8..12   checkpoint number (u32 LE)
12..16  identity version (u32 LE)
16..20  messages version, or 0xffffffff
20..24  result version, or 0xffffffff
24..28  thread-id byte length (u32 LE)
28..32  checkpoint-id byte length (u32 LE)
32..36  parent-checkpoint-id byte length (u32 LE)
36..40  canonical-state schema id = 1
40..48  canonical logical state length (u64 LE)
48..80  checkpoint-state commitment
80..84  flags = 0
84..88  reserved = 0
88..     thread bytes || checkpoint-id bytes || parent-id bytes
```

Requirements:

- thread and checkpoint IDs are non-empty and bounded;
- parent ID may be empty to represent no parent;
- all identifier bytes are UTF-8;
- every referenced channel version is below the transaction/recovery version
  watermark;
- unknown schema IDs, flags, reserved fields, malformed lengths, and trailing
  bytes fail closed.

## WAL/publication boundary for the next unit

The future Format-v2 WAL transaction will use distinct `T2W2` framing and carry
only the append-local delta:

```text
new payload bytes
new T2N2 node records
new T2V2 version records
one T2P2 checkpoint record
optional request-id bytes
transaction SHA-256
```

A metadata-only fork is allowed to contain zero new payload bytes and zero new
nodes/versions while publishing a new checkpoint that references already
committed versions. This preserves the O(1)-metadata fork target.

`T2W2` recovery must validate topology against committed watermarks before any
transaction becomes authoritative. The v1 parser must never accept `T2W2`, and
the v2 parser must never treat `T2W1` as v2.
