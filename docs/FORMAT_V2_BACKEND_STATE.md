# Format v2 backend semantic state

Status: staged internal design. This layer has no filesystem or public
`CheckpointStore` authority yet. Public `crate::format::VERSION` remains 1.

## Decision ledger

**DECISION**

Represent the recoverable Format-v2 backend as one semantic
`V2CommittedState` reconstructed from:

```text
optional validated T2S2 schema-2 sealed base
                    +
       validated T2C2 + T2E2 hot suffix
                    ↓
      one V2CommittedState
```

The state owns the exact payload/node arena, ordered version table, ordered live
checkpoint table and identity index, active request ledger, retired request
ledger, and deleted-checkpoint identity set.

A canonical export of that state produces either:

- no `T2S2` artifact for a truly new empty state;
- a live `T2S2` snapshot containing the complete current semantic base; or
- a tombstone-only `T2S2` snapshot when all live tree geometry has been
  reclaimed but retired/deleted identities remain authoritative.

**WHY**

The v1 `Manifest`/`StoreState` types encode the released v1 physical stream
layout. Reusing them as containers for v2 balanced-tree state would blur
semantic metadata with physical representation and make recovery dependent on
v1 assumptions.

The backend boundary gives public open/recovery dispatch a representation it
can construct without invoking any v1 node, segment, state-hash, or stream
parser. It also establishes the exact state that later v2 append, seal,
compaction, fsck, and migration code must preserve.

Deleted checkpoint tombstones are part of semantic state. Zero payload/node/
version/checkpoint geometry therefore does not imply a brand-new store: a
`tombstone-only` base may still reject stale checkpoint resurrection.

**ALTERNATIVES REJECTED**

- Convert v2 state into v1 `StoreState`: rejected because it couples the new
  representation to released v1 physical semantics.
- Reconstruct a sealed base by synthesizing historical `T2C2` commits: rejected
  because a semantic snapshot does not preserve original transaction grouping
  and should not fabricate WAL history.
- Treat a zero-geometry tombstone state as empty: rejected because it forgets
  logical deletion authority.
- Trust `T2S2`'s outer digest and populate maps without semantic validation:
  rejected because snapshot decode must complete its independent image,
  topology, commitment, ledger, and tombstone checks first.

**FORMAT IMPACT**

None beyond the separately documented staged `T2S2` schema-2 correction. This
module creates no new persisted record family. Format v1 remains unchanged.

## Import sequence

For a present sealed base:

1. Decode and semantically validate `T2S2` schema 2.
2. If live tree state exists, materialize the already-validated `T2I2`
   payload/node arrays.
3. Rebuild the checkpoint identity index from ordered `T2P2` records.
4. Rebuild active and retired request maps.
5. Rebuild the deleted-checkpoint identity set.
6. Re-check fixed-width geometry conversion.
7. Feed that exact state into the accepted v2 hot-WAL scanner.
8. Require every hot commit's encoded base geometry to equal the current
   reconstructed geometry before apply.

For no sealed base, recovery begins with `V2CommittedState::default()`.

## Export sequence

Before snapshotting, the backend checks that the checkpoint identity index has
exactly one correct ordinal for every live checkpoint and that no live key is
also tombstoned.

For live state it then:

1. creates one `T2I2` image whose retained-root table is the complete ordered
   version-root table;
2. converts active/retired request maps to canonical snapshot records;
3. converts deleted checkpoint identities to `T2X2` tombstone records; and
4. delegates canonical ordering and semantic verification to the `T2S2`
   encoder.

For zero live geometry, active requests and any residual sequence state are an
error. If no retired/deleted identities exist, no snapshot is emitted. If
retired/deleted identities remain, a tombstone-only `T2S2` is emitted.

## Fail-atomic request retirement

**DECISION**

Moving a request identity from the active ledger to the retired ledger is a
prepare-then-commit semantic transition:

```text
validate request id
reject if retired identity already exists
read active record without mutation
allocate/copy retired key
reserve retired-map insertion capacity
remove active record
insert retired record
```

Every fallible validation/allocation step occurs before the active ledger is
mutated. Once the active record is removed, the remaining retired-ledger insert
uses already-owned key bytes and already-reserved map capacity.

**WHY**

Normal validated v2 states keep active and retired request sets disjoint, but
the mutation helper itself must not rely on that invariant to remain
fail-atomic. The prior implementation removed the active entry first and only
then discovered an already-retired identity, returning an error after partially
changing semantic state.

Allocation is part of the same boundary: merely checking the retired map before
removal is insufficient if key allocation or map growth can still fail after
the active entry has been removed.

The internal `V2ApplyError::Capacity` class represents a pre-mutation
reservation failure. It does not change the public v1 error taxonomy and is not
yet part of a public v2 API.

**ALTERNATIVES REJECTED**

- Remove active first and restore it on error: rejected because rollback itself
  adds mutation complexity and another allocation-sensitive path.
- Assume active/retired disjointness makes the helper safe: rejected because
  helpers should fail closed even when handed internally inconsistent state.
- Insert retired first and remove active second: rejected because a successful
  insert followed by any later error would temporarily create overlapping
  ledgers and complicate invariants.
- Ignore allocation failure as practically impossible: rejected for a storage
  transition whose purpose is preserving deletion/idempotency authority.

**FORMAT IMPACT**

None. No `T2S2`, `T2D2`, WAL, manifest, or Format-v1 bytes change. This is
an in-memory semantic-transition hardening only.

**ACCEPTED EVIDENCE**

Rust 1.80 formatting and strict Clippy passed; the focused `apply_v2` suite
passed 7/7 and the full library suite passed 110/110. The overlap regression
proves a failed retirement leaves both request ledgers unchanged.

## Prepared subtree deletion

**DECISION**

A v2 subtree deletion is prepared as a complete replacement of all
checkpoint-visible semantic ledgers before any committed state is mutated:

```text
validate source checkpoint/index/request/tombstone invariants
find target + descendants by prior-parent topology
build deletion mask
fallibly clone retained checkpoint table
fallibly build retained checkpoint ordinal index
fallibly build active request ledger with remapped ordinals
fallibly build retired request ledger including deleted requests
fallibly build deleted-checkpoint tombstone set
                    |
                    v
          V2PreparedSubtreeDelete
                    |
                    v
apply: replace prepared ledgers only
```

Preparation takes `&self`, so validation or allocation failure cannot mutate
semantic state. The apply phase performs no intended allocation, validation,
or filesystem I/O.

Every active request whose checkpoint is deleted moves to the retired ledger
with the same operation digest. Active requests for retained checkpoints keep
their identity/digest but receive the retained checkpoint's new ordinal.

Deleting a checkpoint deletes every later same-thread descendant whose parent
chain reaches the target. Sibling branches and other thread roots remain live.

If at least one checkpoint survives, this semantic unit deliberately retains
the existing payload/node/version arena, including unreachable historical
sequence records. Logical deletion therefore does not depend on immediate
physical compaction.

If no live checkpoint survives, apply clears payload, nodes, and versions so
the backend can emit the already-defined canonical tombstone-only `T2S2`
representation.

**WHY**

Checkpoint tombstones and request retirement are one logical authority
transition. Publishing one without the other could either resurrect a deleted
checkpoint through request retry or incorrectly retire a request while its
checkpoint remains live.

Building complete replacement ledgers avoids rollback logic and avoids
fallible insertion after semantic mutation starts. It also makes retained
request-ordinal remapping explicit after checkpoint-vector compaction.

Keeping unreachable immutable arena/version history after a partial deletion
is correct but not space-optimal. This unit establishes deletion semantics
before physical compaction, matching the repository invariant that logical
delete precedes reclamation.

**ALTERNATIVES REJECTED**

- Tombstone the checkpoint first, then retire requests one at a time: rejected
  because any later failure leaves a mixed deletion/idempotency state.
- Mutate active maps in place and roll back on allocation failure: rejected
  because rollback adds another failure-sensitive transition.
- Rebuild the persistent-sequence arena in the same unit: rejected because
  semantic deletion and physical compaction have different correctness
  boundaries and should be tested independently.
- Delete only the requested checkpoint while retaining descendants: rejected
  because retained children would reference a deleted parent and violate
  checkpoint topology.
- Forget request IDs when deleting the last checkpoint: rejected because a
  tombstone-only store must still reject stale exact retries and conflicting
  request reuse.

**FORMAT IMPACT**

None. The staged schema-2 `T2S2` already carries active requests, retired
requests, and deleted checkpoint identities. No record family or byte
interpretation changes.

**ACCEPTED EVIDENCE**

This unit is accepted only after focused tests prove:

- preparation itself leaves source state unchanged;
- subtree deletion removes target + descendants but preserves siblings/other
  roots;
- retained active-request ordinals are remapped exactly;
- deleted checkpoint requests become retired with their original digests;
- preparation failure on inconsistent request ledgers changes no semantic
  state;
- snapshot export/reopen preserves deletion and retired-request authority; and
- deleting the final live checkpoint produces a valid tombstone-only backend
  that reopens with zero sequence geometry.

Reviewer inspected commit `e556c2fd189ad8c9d9bc5c2d7599eec4e91c24ca`
and confirmed it is formatter-only. Validation passed:

```text
cargo fmt --all -- --check                         PASS
cargo clippy --lib --features local-server
  --locked -- -D warnings                         PASS

persistent_sequence::apply_v2                     9/9
persistent_sequence::backend_v2                   7/7
persistent_sequence::commit_v2                    4/4
persistent_sequence::publication_v2               3/3
full library                                      113/113
```

This accepts the staged semantic transition only. It does not make Format v2
authoritative in CheckpointStore. It does not implement physical
compaction/reclamation. It does not change persisted format bytes.

## Acceptance properties

The focused backend tests require:

- `sealed base + hot suffix` exports exactly the same canonical semantic state
  as replaying the equivalent complete hot history from empty state;
- a brand-new empty state exports no snapshot and can accept the first commit;
- a hot suffix encoded against the wrong base geometry fails closed;
- retired request identity survives seal/reopen;
- request retirement is fail-atomic when active/retired ledgers are internally inconsistent;
- a tombstone-only base survives reopen and blocks reuse of the deleted
  checkpoint identity; and
- corrupt sealed bytes are rejected before hot replay.

## Current boundary

This layer still does not:

- read/write files;
- define the public v2 manifest;
- publish `T2S2` atomically;
- recycle `hot.wal` after a v2 seal;
- publish the prepared deletion as a durable production operation;
- physically compact unreachable v2 arena/version history after partial deletion;
- migrate a v1 directory;
- route public `CheckpointStore` methods to a v2 backend; or
- claim crash durability for live I/O.

Those remain subsequent production-readiness units.
