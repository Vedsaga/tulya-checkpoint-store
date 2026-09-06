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

## Compaction reachability plan

**DECISION**

Physical reachability is computed as a deterministic plan over source
identifiers before any remapping or reclamation exists:

```text
seed required versions from every live checkpoint
  identity_version + messages_version/result_version when present
retain every transitive parent_version ancestor (iterative worklist)
traverse the AVL DAG from every retained version root (iterative worklist)
retain every reachable node exactly once
record every retained leaf payload range verbatim
              |
              v
    V2CompactionPlan
```

The plan exposes retained source version IDs ascending, retained source node
IDs ascending, and retained source payload ranges ordered by original offset,
then length. It contains old/source identifiers only: no compacted IDs, no
replacement nodes or versions, no apply step, and no publication.

Version planning fails closed on a checkpoint reference to a nonexistent
version, a version ID that disagrees with its vector coordinate, a missing or
non-prior parent, or an invalid conversion boundary. Node traversal fails
closed on a root or branch child outside the node table, a child that is not
topologically prior to its parent, or a leaf whose range overflows or leaves
the payload arena. The planner receives `&V2CommittedState` only, so every
such failure leaves committed state untouched by construction; there is no
rollback path because no semantic mutation ever starts.

Payload ranges are recorded per retained leaf without merging or
deduplication. Canonical appends allocate disjoint delta ranges, so overlaps
can only arise from a malformed arena; preserving them verbatim lets the later
remapping unit reject or handle them explicitly.

**WHY**

Logical deletion deliberately precedes physical reclamation, so unreachable
arena/version history accumulates after partial deletion. Reclaiming bytes
requires first agreeing, in an auditable unit, on exactly which physical state
remains semantically required. Separating the read-only reachability analysis
from the destructive remapping keeps each side independently reviewable and
testable: the plan can be asserted exactly (source IDs and ranges) without
reasoning about replacement coordinates.

**ALTERNATIVES REJECTED**

- Compact the arena in the same unit as planning: rejected because analysis
  and destructive remapping have different correctness boundaries.
- Retain only directly referenced checkpoint versions without ancestry:
  rejected because historical versions remain addressable history until
  remapping proves otherwise.
- Merge or deduplicate payload ranges during planning: rejected because
  merging presumes a remapping policy this unit must not invent.
- Mutate `V2CommittedState` in place while traversing: rejected because a
  read-only `&self` plan is fail-atomic by construction.

**FORMAT IMPACT**

None. This unit computes physical reachability only; it does not reclaim
bytes; it does not change logical deletion authority; it does not publish
Format v2; source IDs remain source IDs; remapping is the next unit.

## Prepared compact replacement

**DECISION**

The accepted reachability plan is converted into a fully prepared dense
replacement before any mutation exists:

```text
source state + reachability plan
              |
              v
deterministic old->new version/node/payload mappings
              |
              v
compact payload (dense repack, no gaps)
compact node table (canonical rebuild, no holes)
compact version table (sequential IDs, remapped parents/roots)
checkpoints with remapped physical version IDs
              |
              v
    V2PreparedCompaction
```

Retained old node/version IDs arrive ascending, so the first retained source
object becomes compact `0`, the next compact `1`, and so on. Branch children
are topologically prior, so ascending order also guarantees each child mapping
already exists when its parent is rebuilt. Retained leaf ranges repack in plan
order with `new_offset = current compact payload length`; gaps from deleted
leaves disappear, while any overlap, duplicate, backward move, overflow, or
out-of-bounds reference fails closed instead of being silently merged.

Every retained object is rebuilt with the canonical constructors, never copied
and patched: leaves via `V2NodeRecord::leaf(new_offset, bytes)`, branches via
`V2NodeRecord::branch(new_left_root, new_right_root)`, versions via
`V2VersionRecord::new(new_id, remapped_parent, remapped_root)`. Each rebuild
must preserve source height, logical length, and commitment (plus explicit
`left_len` agreement for branches); each version root must match its source
root triple, so a root is never trusted merely because its `node_id`
resolves. Each rebuilt checkpoint recomputes `checkpoint_state_metadata` from
the remapped compact roots and must reproduce both the source state and the
source operation digest, proving physical compaction cannot alter logical
request identity.

Checkpoint order and logical identity are unchanged, so ordinals, active
request ordinals, retired digests, and tombstones need no replacement and are
left completely untouched for the later apply step. Preparation takes
`&V2CommittedState` only and uses checked arithmetic with explicit fallible
reservation throughout, so failure leaves committed state untouched by
construction.

**WHY**

Copying reachable bytes without re-deriving them would preserve any latent
structural corruption (wrong commitments, wrong lengths, root disagreement)
into the compacted representation. Canonical reconstruction turns preparation
into the semantic-verification boundary the reachability plan deliberately
deferred: the compact state is a fresh valid physical representation whose
logical commitments are exactly identical. Keeping apply/publication in later
units preserves the prepare-then-apply discipline used by deletion and request
retirement.

**ALTERNATIVES REJECTED**

- Copy source records and rewrite IDs in place: rejected because patching
  cannot detect commitment/length/root disagreement.
- Merge overlapping payload ranges during repack: rejected because merging
  presumes a policy for malformed arenas; rejection is the fail-closed answer.
- Remap request ledgers and tombstones into the prepared object: rejected
  because unchanged checkpoint order leaves their coordinates valid.
- Mutate committed state during preparation: rejected because `&self`
  preparation is fail-atomic by construction.

**FORMAT IMPACT**

None. The staged `T2S2`/`T2I2` layout itself is unchanged; this prepares a
different but semantically equivalent compact physical representation. It does
not publish Format v2 and does not change logical deletion authority;
apply/publication remain later units.

## Atomic compaction apply

**DECISION**

The prepared replacement becomes a usable operation through exactly one
exclusive-borrow entry point:

```text
compact_v2_state(&mut V2CommittedState)
    |
    | prepare_v2_compaction(&*state)?
    | private infallible apply
    v
compact committed semantic state
```

No independently callable `apply(prepared)` exists: a preparation created at
time T can never overwrite a state that has since advanced, because no
prepared object ever escapes the call. The private apply destructures the
prepared object and replaces only payload, nodes, versions, and checkpoints;
ordinals, active/retired requests, and tombstones stay untouched because
checkpoint order is unchanged. Past successful preparation there is no
`Result`-producing operation, no rollback path, and no fallible allocation:
assignments, destructuring, and drops only. `Clone` is deliberately absent
from the prepared type so the replacement behaves as a single-use owned
transition rather than something casually duplicable.

The compacted state must export as valid `T2S2`, reopen exactly (geometry,
checkpoint identities/order/commitments, request ordinals/digests, retired and
tombstone authority, conflict behavior), accept new appends from compacted
coordinates with the standard transaction machinery, and compact idempotently.
Tombstone-only states compact as a physical no-op with authority preserved.

**WHY**

Preparation safety is worthless if a stale prepared object can be applied to
the wrong state generation. A fingerprint/generation protocol would add a new
failure-prone comparison; structural safety (prepare and apply fused under one
`&mut` borrow) removes the hazard class entirely. Proving export/reopen
equivalence plus continued appendability is what promotes the compacted state
from "decodable bytes" to "operating state".

**ALTERNATIVES REJECTED**

- Public `apply_prepared_compaction(state, prepared)`: rejected because an
  independently applicable preparation can go stale.
- Source fingerprint/generation check on apply: rejected as a fragile
  compensation for an API shape that should not exist.
- Snapshot-byte comparison for idempotence: rejected because semantic-state
  equality is the meaningful invariant; byte freezing belongs to no new
  format vector here.
- Remapping ledgers during apply: rejected because unchanged checkpoint order
  keeps every ledger coordinate valid.

**FORMAT IMPACT**

None. This is still staged internal Format-v2 behavior: no filesystem
publication, no manifest authority, no WAL recycling, no migration, and no
Format-v2 production activation.

## Immutable compact authority candidate

**DECISION**

The fully encoded compact `T2S2` authority candidate is prepared from
`&V2CommittedState` without mutating the authoritative state:

```text
authoritative state
        |
        | validate checkpoint index (existing backend validator)
        | prepare compact replacement (accepted unit)
        | build canonical T2I2 image from compact tables
        | serialize authoritative source ledgers
        | encode schema-2 T2S2
        v
compact artifact bytes
```

The existing `compact_v2_state(&mut)` remains the pure staged
semantic/in-memory operation; the new
`prepare_compacted_v2_sealed_artifact(&)` is the immutable pre-publication
preparation. The second is not implemented by cloning and compacting the whole
state: it consumes the preparation result directly. The compact checkpoint
table is used with the authoritative source ledgers (active requests with
exact IDs/digests/unchanged ordinals, retired requests, tombstones), because
unchanged checkpoint order keeps every ledger coordinate valid. A corrupt
checkpoint index fails closed up front rather than being silently repaired by
ordinal reconstruction. Truly empty states yield no artifact; tombstone-only
states yield an authoritative tombstone-only artifact.

**WHY**

Mutating live memory to the compact form before the new disk authority is
durable would leave memory "new" while disk authority is still "old" across
any publication failure. Preparing the exact candidate bytes immutably keeps
the old authority intact through every pre-publication failure, so the later
publisher only needs write/sync/publish followed by memory adoption.

**ALTERNATIVES REJECTED**

- Compact live memory first, then publish: rejected because a publication
  failure splits memory-new from disk-old authority.
- Clone the whole state, compact the clone, export the clone: rejected as
  unnecessary full-state duplication hiding the preparation boundary.
- Rebuilding ledgers from compact order instead of serializing source
  ledgers: rejected because identical order makes them the same data with
  more code.
- Rewriting the snapshot encoder for this unit: rejected; canonical encoding
  and its validators are reused unchanged.

**FORMAT IMPACT**

None. No record family, layout, or byte interpretation changes; the staged
`T2S2` schema-2 carries the compact physical representation with unchanged
ledger semantics. Disk publication and manifest authority are still later
units.

## Accepted semantic-compaction evidence

```text
cargo fmt --all -- --check                         PASS
cargo clippy --lib --features local-server
  --locked -- -D warnings                         PASS

persistent_sequence::compaction_v2               24/24
persistent_sequence::backend_v2                   7/7
persistent_sequence::apply_v2                     9/9
persistent_sequence::conformance_v2               9/9
full library                                      146/146
```

This accepts the pure semantic compaction pipeline (logical deletion,
reachability planning, canonical dense preparation, exclusive prepare/apply,
export/reopen, continued append). Durable publication remains a later unit.

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
