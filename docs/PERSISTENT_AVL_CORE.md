# Persistent AVL core invariants

Status: pure in-memory Format-v2 core. This is not yet a writable on-disk
format and does not change the public Format-v1 compatibility contract.

The implementation lives in `src/persistent_sequence/avl.rs` and is informed by
`Vedsaga/Tulya-MDL-Lean/formal/Tulya/Incremental/PersistentAVLFinalAPI.lean`.
That correspondence is architectural and theorem-guided; it is not a claim of
a mechanical refinement proof for the Rust implementation.

## Decision ledger

### DECISION

Use an immutable append-only arena of Format-v2 leaves and branches. An edit
returns a new root and never mutates a node reachable from an older root.
Concatenation descends only the taller AVL spine and restores balance with
single or double rotations.

### WHY

The production locality target requires a child checkpoint to reuse unchanged
parent structure. Path copying provides that sharing directly: an append
allocates one new leaf plus only the changed AVL path. Historical roots remain
valid because existing nodes are immutable.

The general primitive is `concat(left, right)`, even though the first caller is
append-at-end. This keeps the balancing rule representation-local and gives
later insert/replace work a correct primitive without placing tree rotations in
checkpoint semantics.

### ALTERNATIVES REJECTED

- Rebuild a balanced tree from all leaves after each append: exact but O(parent).
- Keep the Format-v1 left-deep chain and add a depth cache: does not provide the
  required range locality or bounded depth.
- Mutate nodes in place during rotation: breaks historical-root immutability.
- Put rotations in `checkpoint_store/store.rs`: couples semantic checkpoint
  behavior to a physical representation and makes future migration harder.

### FORMAT IMPACT

None yet. The core uses the staged `T2N2`/`T2R2` metadata definitions in memory.
No manifest, WAL, sealed-segment, or public format-version field is changed by
this unit.

## Required invariants

For every retained root produced by the core:

1. **Historical immutability** — later edits cannot alter bytes reachable from
   an earlier root.
2. **Exact sequence semantics** — in-order leaves concatenate to exactly the
   logical byte sequence represented by the root.
3. **Stored logical length** — every root/node length equals the sum of its
   represented bytes; every branch left-length equals its left subtree length.
4. **AVL balance** — child heights differ by at most one; branch height is one
   plus the maximum child height.
5. **Commitment agreement** — leaf commitments recompute from exact payload
   bytes and branch commitments recompute from child metadata.
6. **Physical/logical separation** — physical node IDs and payload offsets do
   not enter the logical commitment.
7. **Failure atomicity** — a failed append leaves both payload and node arenas at
   their pre-call lengths.
8. **Local allocation** — a successful append allocates one leaf plus O(height)
   path/rotation nodes; it never copies the complete parent tree.
9. **Bounded range navigation** — a range read follows stored subtree lengths
   rather than recursively measuring unchanged subtrees.

## Test contract

The pure-core unit tests must cover:

- thousands of deterministic sequential appends with a logarithmic height
  bound and per-append fresh-node bound;
- retained historical roots after later appends;
- appending two sibling branches from the same historical root;
- exact ranges crossing leaf and rotation boundaries;
- explicit single and double rotation shapes;
- full metadata/commitment verification of retained roots; and
- append-error rollback with no arena growth.

These tests establish the data-structure contract only. They do not establish
on-disk durability, crash atomicity, migration correctness, or production
range-I/O complexity. Those remain later gates.
