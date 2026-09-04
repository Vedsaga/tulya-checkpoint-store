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
- implement production deletion/compaction;
- migrate a v1 directory;
- route public `CheckpointStore` methods to a v2 backend; or
- claim crash durability for live I/O.

Those remain subsequent production-readiness units.
