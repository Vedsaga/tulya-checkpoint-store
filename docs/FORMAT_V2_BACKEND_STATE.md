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

## Acceptance properties

The focused backend tests require:

- `sealed base + hot suffix` exports exactly the same canonical semantic state
  as replaying the equivalent complete hot history from empty state;
- a brand-new empty state exports no snapshot and can accept the first commit;
- a hot suffix encoded against the wrong base geometry fails closed;
- retired request identity survives seal/reopen;
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
