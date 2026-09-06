# HN Launch Execution Handoff

Status: implementation handoff
Target branch: feat/production-readiness
Authored against documentation head: be55c2f37a1579698713743d93b02bd1f9540ae0
Last audited code head before the documentation-only readiness commits:
9f5224b9d65362a3470fa11459ff3b9bf794edac

This document is the implementation handoff for turning Tulya from its current
storage-engine/shadow-integration state into a usable experimental LangGraph
checkpointer that is ready for a final benchmark campaign, user outreach, and a
Hacker News launch.

It is intentionally operational. It tells an implementation agent what to
change, in what order, what not to build, what evidence is required, and when
the implementation phase is finished.

The broader production contract remains in docs/PRODUCTION_READINESS.md. This
document is the shorter execution path to the next commercial milestone.

---

## 1. Mission

Finish the minimum technically serious product that can support this public
position:

> Tulya is an installable experimental LangGraph checkpoint saver backed by an
> embedded Rust persistent-history engine. It is authoritative rather than a
> shadow, passes the documented LangGraph base conformance and restart/durability
> tests, and has reproducible benchmark evidence for the first released storage
> format.

The implementation agent must make the repository reach the
IMPLEMENTATION-COMPLETE / BENCHMARK-READY gate in Section 16.

Do not run or publish the final claim benchmark until that gate is green.

After the gate is green, run the benchmark campaign in Section 17. Only after
the benchmark evidence is reviewed should the project approach users and
publish on Hacker News.

---

## 2. Non-goals for this milestone

Do not expand the milestone with work that is not required for the first usable
LangGraph release.

The following are explicitly deferred unless they are trivial consequences of
required work:

- multi-writer storage;
- distributed consensus;
- network filesystem support;
- remote database/server semantics;
- exact Rust-to-Lean mechanical refinement;
- proof of concrete CPU memory ordering;
- universal filesystem/device durability claims;
- power-cut or controller-cache claims;
- secure physical deletion;
- information-theoretically optimal storage;
- optimized DeltaChannel history;
- copy_thread;
- delete_for_runs;
- prune;
- broad Windows/macOS support unless the wheel/platform is explicitly claimed;
- migration from the unreleased prototype store;
- preserving the current prototype Format-v1 fixture as a public compatibility
  contract.

Do not implement optional LangGraph capabilities merely because methods exist
upstream. Unsupported optional capability is better than incorrect capability.

---

## 3. Source-of-truth hierarchy

When repository documents disagree, use this order until the milestone is
complete:

1. this execution handoff for implementation order and first-release decisions;
2. docs/PRODUCTION_READINESS.md for acceptance invariants and later production
   requirements;
3. the current upstream LangGraph checkpoint interface and official conformance
   package for adapter behavior;
4. the Lean reference model for storage semantics;
5. older format/design documents only when they do not conflict with the first
   four items.

Known stale conflict at handoff:

docs/FORMAL_MODEL_CORRESPONDENCE.md still describes the existing prototype
Format v1 as frozen and staged v2 as a future public Format v2. That is no
longer the release decision.

The current release decision is:

- the existing left-deep prototype format has no compatibility promise;
- the staged balanced design becomes the first released Format v1;
- no prototype-v1 to release-v1 migration is required.

The agent must reconcile stale documentation as part of Phase 0 and Phase 1.

---

## 4. Architecture decisions that are already made

Do not reopen these decisions without discovering a concrete correctness
problem.

### 4.1 One domain-neutral Rust storage engine

The Rust core is a persistent history engine. LangGraph-specific concepts stay
in the LangGraph adapter/schema layer.

The storage core must continue to support:

- immutable historical versions;
- branch from an old version;
- sibling branches;
- persistent structural sharing;
- exact byte reconstruction;
- bounded/local reads;
- single serialized writer;
- safe readers/snapshots;
- durable commit/recovery;
- logical deletion and later reclamation.

Do not hard-code LangGraph Python object semantics into the low-level AVL/node
format.

### 4.2 The staged balanced implementation becomes release Format v1

Current staged modules under src/persistent_sequence include:

- avl.rs
- format_v2.rs
- image_v2.rs
- transaction_v2.rs
- commit_v2.rs
- hot_frame_v2.rs
- apply_v2.rs
- recovery_v2.rs
- snapshot_v2.rs
- publication_v2.rs
- backend_v2.rs
- compaction_v2.rs
- conformance_v2.rs

These are the basis of the first released format.

Do not invest further in the legacy left-deep writer except to delete,
quarantine, or keep a temporary test oracle during the transition.

Do not perform a large cosmetic v2-to-v1 rename before the staged design is
integrated and green. After integration is stable and before the golden release
fixture is frozen, perform one deliberate naming/wire-format cleanup so the
public release has a coherent Format-v1 identity.

The preferred final state is:

- public manifest format_version = 1;
- one writable/readable release-format path;
- one golden release Format-v1 fixture;
- no public promise for the previous prototype bytes;
- staged-v2 wording removed from user-facing documentation.

If internal T2-prefixed wire tags are retained, document that they are internal
record-schema generation tags and not the public format version. Prefer
renaming them before freeze if doing so does not introduce correctness risk.
Once the release fixture is frozen, do not rename persisted bytes casually.

### 4.3 Correctness-first LangGraph storage

Tulya must round-trip arbitrary supported LangGraph checkpoint/channel values
exactly through the configured LangGraph serializer.

Message append structural sharing is an optimization.

A channel/value that Tulya cannot optimize must fall back to exact opaque typed
storage rather than fail or silently change semantics.

### 4.4 One authoritative saver

The launch product is a real TulyaSaver.

TulyaShadowSaver may remain as an evaluation tool, but it cannot be the launch
path and cannot be used for final product benchmark claims.

### 4.5 One process, one Tulya writer

The first claimed scope is:

- one host;
- one Tulya writer process;
- local filesystem;
- concurrent LangGraph tasks/readers made safe inside that process.

Do not claim multiple independent writer processes.

---

## 5. Storage invariants that may not regress

Every implementation phase must preserve these properties.

### Durable acknowledgement

A successful mutating operation implies the required state can be recovered
after immediate process death and reopen.

### Atomic publication

Recovery exposes exactly the previous committed state or exactly the new
committed state, never a mixture.

### Safe retry

The same durable request identity plus the same logical operation must not
duplicate the mutation.

The same identity plus different logical bytes/digest must fail explicitly.

A retired durable identity must not silently become a fresh operation.

### Historical immutability

A child append, sibling branch, delete, compaction, or reclaim must not silently
change another retained historical checkpoint.

### Local-change locality

For parent logical size n and changed/new payload delta:

    local append work ~= O(log n + delta)
    foreground physical writes ~= O(log n + delta)
    range read k bytes ~= O(log n + k)
    fork from a retained root ~= O(1) metadata or equivalent

A fixed 1 KiB append must not require reading/hashing the complete 1 GiB parent.

### Fail closed

Unknown version/features, malformed records, impossible topology, overflow,
invalid lengths/offsets, bad commitments/checksums, and unsupported authority
must fail explicitly.

### Delete logically first

After delete_thread succeeds, deleted checkpoints and pending writes are not
visible. Physical reclaim may be later.

A stale operation referencing a deleted checkpoint must not resurrect it.

---

# EXECUTION PHASES

## 6. Phase 0 — Restore a trustworthy baseline and remove contradictory plans

Priority: P0
Expected economic value: very high
Dependency: none

### Required work

1. Fix the current Rust Clippy failures that prevented the Rust job from running
   the remaining test sequence on code head 9f5224b.

2. Run the complete existing Rust suite after Clippy is clean.

3. Ensure all fault-injection integration tests that are intended to protect the
   release path actually execute in CI. The current workflow explicitly runs
   crash_matrix with fault-injection, but files such as hot_wal_faults.rs and
   publication_faults.rs must not remain accidentally outside the release gate.

4. Add branch protection / required checks to main if repository permissions
   permit it. If automation cannot configure repository protection, record this
   as the only manual repository-setting action required before release.

5. Reconcile stale documents:
   - docs/FORMAL_MODEL_CORRESPONDENCE.md
   - docs/FORMAT_V2_AUTHORITY.md
   - docs/FORMAT_V2_CONFORMANCE.md
   - docs/FORMAT.md
   - any comments in src/persistent_sequence.rs

   They must stop treating the unreleased prototype format as a compatibility
   promise.

6. Do not delete useful prototype tests until the release path has equivalent or
   stronger tests. Rename/quarantine them so it is obvious they are not the
   public format contract.

### Exit criteria

All of the following must pass at the same code commit:

    cargo fmt --all -- --check

    cargo clippy --all-targets --features local-server --locked -- -D warnings

    cargo test --all-targets --features local-server --locked -- --test-threads=1

    cargo test --locked --features fault-injection -- --test-threads=1

    cargo package --locked

No new allow(dead_code), allow(clippy::...), panic/unwrap exceptions, or skipped
tests may be introduced merely to make the gate green without an explicit
justification.

---

## 7. Phase 1 — Make the balanced persistent engine the real CheckpointStore

Priority: P1
Expected economic value: highest
Dependency: Phase 0

This is the most important storage task.

The repository already contains substantial staged implementation. The task is
not to invent a third design. The task is to make the staged balanced design
the only shipping release-format path.

### 7.1 Persistent sequence production seam

Current src/persistent_sequence.rs only exposes read-oriented LegacyV1 roots.

Extend the real production seam as actual callers land. It must support, at
minimum:

- append/create from an optional parent root;
- exact logical_len;
- exact read_range;
- bounded streaming of a range/full value;
- structural verify;
- root metadata sufficient to avoid whole-parent work.

The release root must carry or resolve without whole-parent reconstruction:

- logical byte length;
- physical root identity;
- representation/version;
- subtree metadata needed for range navigation;
- structural/content commitment required by the release format.

Remove the LegacyV1-only assumption from shipping callers.

### 7.2 Wire staged AVL into CheckpointStore

Replace the current left-deep message/state history path in
src/checkpoint_store/store.rs with the balanced persistent sequence.

Required semantic cases:

- create root;
- append to latest;
- append from an old historical root;
- create sibling children from one parent;
- read full value;
- read arbitrary range;
- seal;
- reopen eager;
- reopen lazy where supported;
- continue after reopen;
- verify/fsck;
- logical delete;
- compact/reclaim retained state.

No checkpoint operation may depend on reconstructing the complete unchanged
parent merely to calculate child metadata.

### 7.3 Release authority and hot WAL

Integrate the staged transaction/commit/hot-frame/recovery state machine into
the actual writable store.

The authoritative unit must include durable request identity semantics.

Bare structural transaction records must never become client-visible authority
merely because they are individually well formed.

Required request behavior:

    active ID + same logical digest      => replay existing result
    active ID + different digest         => conflict
    retired ID + same logical digest     => retired / no mutation
    retired ID + different digest        => conflict
    unknown ID                            => new mutation

The release path must survive:

- complete commit;
- short/torn final commit;
- reserve zeroes;
- corrupt complete commit;
- duplicate physical retry;
- reopen after acknowledged commit;
- reopen after interrupted unacknowledged commit.

### 7.4 Sealed snapshot / generation integration

Make the staged sealed representation part of normal seal/reopen.

A snapshot must preserve:

- persistent sequence image;
- versions;
- live checkpoints;
- parent relationships;
- active request ledger;
- retired request ledger;
- deleted checkpoint tombstones;
- enough geometry/metadata for bounded reopen.

Deleting the final live checkpoint must still allow a valid authoritative
tombstone/retired-identity state.

### 7.5 Delete, tombstones, compaction

Connect release-format deletion to the semantic behavior already represented by
the staged code and Lean reference.

Required behavior:

- subtree delete identifies same-thread descendants correctly;
- surviving sibling histories remain exact;
- deleted identities become durable tombstones;
- associated active request identities are retired rather than silently freed;
- survivor request ordinals/references remain valid after remap;
- no stale child of a deleted checkpoint can resurrect deleted history;
- compaction/remapping may change physical placement but not logical checkpoint
  identity, state commitment, operation identity, or retained reads.

### 7.6 Remove/quarantine prototype writer

Once the release-format path passes all equivalent tests:

- remove the old left-deep writable path from normal CheckpointStore creation;
- remove the whole-parent XXH3 append dependency from the release path;
- remove migration obligations from the prototype;
- make unsupported prototype directories fail clearly, or keep an explicitly
  private diagnostic opener if useful during development;
- do not pretend prototype stores are supported release stores.

### 7.7 Freeze release Format v1 only at the end of this phase

Do not freeze the golden fixture before the integrated path passes:

- root/append/branch;
- seal/reopen;
- crash recovery;
- request identity;
- delete/tombstone;
- compaction;
- corruption rejection;
- locality instrumentation.

Then:

- set/document public release format_version = 1;
- perform final v2-to-release-v1 naming cleanup;
- generate the committed release Format-v1 fixture;
- make that fixture immutable from this point onward;
- update FORMAT.md to describe only the released contract plus clearly marked
  internal implementation detail.

### Phase 1 exit criteria

The normal public Rust CheckpointStore must no longer use the prototype
left-deep writer for newly created stores.

A new release-format store must pass all existing semantic/history tests plus
new release-format tests for:

- parent preservation;
- sibling branches;
- exact historical reconstruction;
- arbitrary range reads;
- seal/reopen;
- crash old-or-new;
- request replay/conflict/retirement;
- delete/tombstone/reopen;
- compaction preservation;
- malformed input fail-closed.

---

## 8. Phase 2 — Close the practical Lean-to-Rust boundary

Priority: P1
Dependency: Phase 1 integration can proceed in parallel, closure before format
freeze

Do not attempt a complete theorem that arbitrary Rust source refines Lean. That
is not required for this milestone.

Use the Lean repository as a specification and deterministic conformance oracle.

Primary Lean reference:

    formal/Tulya/Incremental/PersistentAVLFinalAPI.lean

Important storage boundaries to carry into Rust:

- persistent AVL/path-copy correctness;
- old-root preservation;
- logarithmic height/update allocation;
- O(log n + output) range reads;
- O(log n + inserted payload) fresh physical work;
- canonical bounded parsing;
- old-or-new recovery;
- single-writer/many-reader publication model;
- reclamation protected by live/recovery roots;
- bounded restart/base advancement;
- durable request identity semantics;
- tombstone/deletion semantics.

### 8.1 Keep and expand language-neutral conformance vectors

Current Rust fixture:

    src/persistent_sequence/format_v2_conformance.json

Current runner:

    src/persistent_sequence/conformance_v2.rs

Current Lean-side schema/reference:

    formal/Tulya/Incremental/FORMAT_V2_CONFORMANCE_VECTORS.md

The release fixture must cover at least:

- leaf and branch canonical structural bytes;
- root bytes;
- old-root preservation;
- sibling independence;
- noncanonical node/root rejection;
- operation-digest inputs;
- requestless commit;
- requestful commit;
- request active replay;
- request active conflict;
- request retired same-digest;
- request retired conflict;
- snapshot with live checkpoint;
- tombstone-only snapshot;
- active/retired overlap rejection;
- complete hot-WAL recovery;
- torn-final recovery;
- corrupt/bare/duplicate retry rejection;
- subtree delete;
- survivor remap;
- compaction logical invariance.

When staged v2 becomes release Format v1, rename the fixture/schema deliberately
and update both repositories together. Do not retain a fixture name that implies
a public v2 release unless it is explicitly documented as an internal generation
identifier.

### 8.2 Do not overclaim what Lean proves

Lean does not currently prove:

- the Rust implementation refines the Lean machine;
- concrete Rust atomic memory ordering;
- a particular filesystem implements Device.flush semantics;
- SHA-256/checksum security/injectivity as a mathematical fact;
- real-world latency/throughput;
- LangGraph BaseCheckpointSaver semantics;
- Python/PyO3 behavior.

The release documentation must therefore say:

    informed by / corresponds to / checked against a Lean reference model

and not:

    formally verified Rust storage

unless a separate actual Rust refinement artifact is later completed.

### 8.3 Concrete integrity choice

Before release-format freeze, explicitly document:

- which digest/checksum protects structural nodes;
- which protects commits/snapshots;
- whether the digest is for accidental corruption, adversarial tampering, or
  operation identity;
- whether full logical-byte verification is separate from structural
  commitment.

Do not rely on an unstated "hashes never collide" theorem.

### Phase 2 exit criteria

- Lean/Rust conformance fixture passes at the release-format code commit.
- FORMAT/FORMAL_MODEL_CORRESPONDENCE accurately describe the proof boundary.
- No claim says Rust is formally verified.
- Every storage invariant used by the product has either:
  1. a direct release-format test,
  2. a Lean reference + Rust vector,
  3. a documented platform premise,
  or a combination of those.

---

## 9. Phase 3 — Prove locality and scale before building product polish

Priority: P2
Dependency: Phase 1 release-format integration

This phase is engineering evidence, not the final competitive benchmark.

### 9.1 Instrument operation work

Add diagnostics that can report per operation:

- physical/store bytes read;
- physical/store bytes written;
- persistent nodes touched/read;
- persistent nodes allocated;
- logical bytes appended;
- temporary bytes allocated where practical;
- sync/durability time;
- total operation wall time.

Instrumentation must be available to benchmarks without changing storage
semantics.

### 9.2 Local append scaling

Create a reproducible benchmark/test that builds approximately:

- 10 MiB logical parent;
- 100 MiB logical parent;
- 1 GiB logical parent;

and appends the same 1 KiB delta.

The release gate is not a specific latency number.

The release gate is that bytes read/written, nodes touched, CPU work and
incremental memory do not scale linearly with unchanged parent size.

A 10x or 100x increase in parent size must not produce a comparable increase in
unchanged-parent I/O.

### 9.3 Range-read scaling

For large checkpoints, read 4 KiB at:

- start;
- 25 percent;
- 50 percent;
- 75 percent;
- end.

Exercise:

- latest root;
- retained old root;
- sibling root;
- sealed state;
- reopen.

Record unrelated bytes/nodes touched.

### 9.4 Deep history structural tests

Required PR/CI structural coverage:

- enough appends to assert the balancing invariant;
- max tree depth checked against the stated AVL/logarithmic property;
- branch from old roots.

Required benchmark/manual evidence before final launch benchmark:

- 1,000 checkpoints;
- 10,000 checkpoints;
- 100,000 checkpoints.

One-million-checkpoint evidence may remain nightly/manual and is not a blocker
for HN alpha unless smaller tests expose scaling risk.

### Phase 3 exit criteria

A machine-readable locality evidence file exists for the exact release-format
commit and demonstrates the locality invariant.

If this phase fails, stop. Fix the storage path before writing the Python
product layer around it.

---

## 10. Phase 4 — Build the installable Python package

Priority: P3
Dependency: storage path stable enough to bind

Normal Python users must not need:

- Rust installed;
- cargo;
- a Tulya CLI subprocess.

Use PyO3/maturin or an equivalent direct in-process binding.

Preferred package name:

    langgraph-checkpoint-tulya

Preferred Python import surface:

    from langgraph_checkpoint_tulya import TulyaSaver

Exact package naming may change only if packaging conflicts require it.

### Required binding capabilities

Expose the smallest Rust API needed by TulyaSaver:

- open/create store;
- close;
- put checkpoint record;
- get exact checkpoint record;
- list checkpoint records;
- put pending write;
- read pending writes;
- delete thread/checkpoint namespace as required by LangGraph semantics;
- stats/verify operations useful for diagnostics.

Do not expose physical AVL/node internals to Python.

### Blocking behavior

Rust filesystem operations are blocking.

Async Python methods must not block the event loop.

Use one of:

- PyO3 calls that release the GIL plus asyncio.to_thread / executor wrapper;
- another explicit safe blocking boundary.

Do not build a second async storage state machine in Python.

### Wheel CI

At minimum test supported Python versions:

- 3.10;
- 3.11;
- 3.12;
- 3.13.

Only publish/claim platforms actually exercised in CI/release evidence.

Each wheel test starts from a clean environment and performs:

- pip install wheel;
- import package;
- create/open Tulya store;
- put/get round trip;
- close/reopen round trip.

### Phase 4 exit criteria

A clean Python environment can install the built wheel and exercise Tulya with
no Rust toolchain and no CLI subprocess.

---

## 11. Phase 5 — Implement authoritative TulyaSaver

Priority: P4/P5
Dependency: Phase 4 binding

Re-check current upstream interfaces before implementation and before release.

Audit snapshot used when this plan was written:

- langgraph 1.2.11;
- langgraph-checkpoint 4.2.0;
- langgraph-checkpoint-conformance 0.0.2;
- Python 3.10 through 3.13.

Do not assume these versions remain current.

### 11.1 Required identity model

Checkpoint identity must preserve LangGraph's effective key space:

    thread_id
    checkpoint_ns
    checkpoint_id

Do not collapse namespaces.

Store parent checkpoint identity/config explicitly enough to reconstruct the
CheckpointTuple contract exactly.

### 11.2 Serializer rule

Use the saver serializer configured by LangGraph.

Arbitrary checkpoint/channel values must use the serializer's typed encoding
and round-trip exactly.

Do not assume JSON.

Do not assume only messages.

Do not inspect/transform encrypted or custom serializer payloads unless the
serializer contract explicitly requires it.

Recommended correctness-first structure:

- exact opaque serialized checkpoint value;
- exact opaque serialized metadata;
- exact pending-write typed values;
- stable identity/parent/index metadata;
- optional optimized structural representation for append-local channels.

Optimization must never be required for correctness.

### 11.3 Required BaseCheckpointSaver surface

Implement Tulya-backed versions of every current official base capability.

At the audit snapshot this includes the required base behavior corresponding to:

- put / aput;
- put_writes / aput_writes;
- get_tuple / aget_tuple;
- list / alist;
- delete_thread / adelete_thread.

Re-check the upstream conformance package rather than copying this list
blindly.

### 11.4 get_tuple

Return exactly the expected:

- config;
- checkpoint;
- metadata;
- parent_config;
- pending_writes.

Historical checkpoint lookup must work after process reopen.

Latest-checkpoint lookup must follow upstream config semantics.

### 11.5 list

Match current upstream behavior for:

- thread;
- checkpoint namespace;
- before;
- limit;
- metadata filter;
- ordering.

Do not invent approximate filtering.

### 11.6 pending writes

This is a hard launch requirement.

Persist writes with enough identity to implement current upstream idempotency
and resume semantics, including the current task identity/path/index rules and
special write-index behavior.

Before coding, inspect the installed upstream checkpoint package for the exact
current WRITES_IDX_MAP / duplicate-write contract.

Required behaviors include:

- successful node writes survive process restart;
- retries do not duplicate writes that upstream considers idempotent;
- a partial superstep can resume without re-running already successful work;
- checkpoint deletion removes/hides associated pending writes according to the
  upstream contract;
- stale pending writes cannot resurrect a deleted checkpoint lineage.

### 11.7 delete_thread

After success:

- checkpoints for the targeted LangGraph thread scope are no longer visible;
- pending writes are no longer visible;
- reopen preserves deletion;
- stale operations cannot resurrect deleted checkpoint identities.

Physical reclaim may be deferred.

### 11.8 Optional capabilities

Do not override/advertise optional methods unless they are correct and pass the
official tests.

For first HN release it is acceptable to omit:

- copy_thread;
- delete_for_runs;
- prune;
- optimized delta_channel_history.

### Phase 5 exit criteria

TulyaSaver performs all required base operations with no authoritative
InMemory/SQLite/Postgres saver underneath it.

TulyaShadowSaver is not used in the primary-saver tests.

---

## 12. Phase 6 — Official LangGraph conformance

Priority: P6
Dependency: Phase 5

Add the official langgraph-checkpoint-conformance package to CI.

### Required policy

- all base capability tests must pass;
- every optional capability detected/advertised by Tulya must pass;
- save a machine-readable or durable text report as release evidence;
- failure is a release blocker.

### Version matrix

Define:

- minimum supported checkpoint package;
- minimum supported LangGraph package if directly imported;
- locked release versions;
- latest-compatible canary.

CI should contain:

1. minimum supported matrix;
2. locked release matrix;
3. scheduled latest canary.

Do not make the latest canary silently redefine a released support promise.

### Phase 6 exit criteria

Official base conformance is green from the installed Tulya wheel.

---

## 13. Phase 7 — Real graph durability tests

Priority: P6
Dependency: Phase 5, can run alongside Phase 6

Unit conformance is insufficient for a storage product.

Create real StateGraph integration tests using TulyaSaver directly.

Required graph scenarios:

- sync invoke;
- async invoke;
- normal streaming path;
- multiple successive invocations on one thread;
- multiple independent threads;
- multiple checkpoint namespaces/subgraphs;
- historical checkpoint lookup;
- update/fork from an old checkpoint;
- sibling branches;
- close/reopen and continue;
- interrupt then resume;
- node failure after another node produced a pending write;
- resume without duplicating the already-completed work;
- arbitrary non-message channel;
- message/object serializer round-trip;
- custom serializer-supported values;
- encrypted serializer opaque round-trip where upstream permits;
- delete_thread then reopen.

### Python-level crash tests

At minimum add subprocess tests for:

1. acknowledged checkpoint -> immediate process kill -> reopen -> checkpoint is
   present exactly once;

2. acknowledged pending write -> immediate kill -> reopen -> write survives and
   is not duplicated;

3. interrupted/unacknowledged mutation -> reopen -> state is exactly old or new,
   never malformed/mixed;

4. branch child acknowledged -> kill -> reopen -> parent and sibling historical
   reads remain exact.

These tests must go through the installed Python package/TulyaSaver layer.

### Phase 7 exit criteria

All graph-level durability tests pass repeatedly on the supported release
platform in CI.

---

## 14. Phase 8 — Complete fault/security coverage required before benchmark

Priority: P7
Dependency: release format + saver

This is not the full production-candidate fault campaign, but the first user
release must not leave obvious release-path holes.

### Required before benchmark

Ensure the release-format paths are covered by live fault injection for at
least:

- hot checkpoint append;
- pending-write append;
- sync/fsync failure;
- short write;
- ENOSPC before bytes;
- ENOSPC after partial write;
- seal/snapshot publication;
- manifest/authority publication;
- rename failure;
- directory-sync failure;
- delete/tombstone publication;
- compaction publication if compaction is enabled for users.

Required outcome classes:

    Rejected
      => no new visible state; old state exact

    Committed
      => exact new state survives reopen

    Indeterminate
      => Python exposes an actionable indeterminate/recovery-required error and
         safe retry resolves to zero-or-one logical mutation

### Corruption

Release-format tests must include representative:

- truncation;
- bit flip;
- invalid length;
- integer overflow;
- invalid root/node reference;
- bad digest/checksum;
- malformed snapshot;
- malformed commit;
- unsupported version/feature.

No panic/UB/silent clamping.

### Dependency/security checks

Add at least:

- cargo audit and/or cargo deny with an explicit policy;
- Python dependency audit for the wheel environment.

Document reviewed exceptions rather than silently ignoring them.

### Phase 8 exit criteria

Release-format fault and corruption tests run in required CI and are green.

Long fuzzing, broad filesystem qualification, and external security review
remain production-candidate work unless already cheap to add.

---

## 15. Phase 9 — Convert benchmark harness to the actual product

Priority: P7
Dependency: TulyaSaver stable

The existing branch-forest benchmark is useful but final public product claims
must run through the shipping primary saver.

### Required changes

Add a benchmark arm that:

- installs/imports the release Tulya wheel;
- constructs TulyaSaver;
- stores/retrieves the actual LangGraph checkpoint schema;
- uses the same durability policy intended for users;
- closes/reopens the saver;
- reconstructs every historical checkpoint exactly.

The low-level Rust public API benchmark may remain as an engine diagnostic, but
it is not the headline LangGraph product arm.

### Comparator fairness

Keep current strong comparators where meaningful:

- LangGraph SQLite;
- LangGraph SQLite/DeltaChannel;
- normalized/content-addressed delta baseline;
- PostgreSQL when deployment semantics are honestly comparable;
- packed Git as a storage-shape comparator with its durability caveat.

Every result must state the durability policy.

Do not compare one backend's relation-only bytes to another backend's entire
directory and call it equivalent storage.

### Required output fields

For every claim-bearing arm preserve:

- exact semantic reconstruction pass/fail;
- exact corpus hash;
- exact code/release commit;
- package versions;
- platform/filesystem;
- durability policy;
- physical allocated bytes;
- logical/file bytes;
- WAL/temp bytes where relevant;
- append p50/p95/p99;
- historical full-read p50/p95/p99;
- 4 KiB range-read p50/p95/p99 where meaningful;
- reopen latency;
- peak RSS;
- CPU;
- maintenance cost if triggered.

### No inherited headline

Do not preserve the existing 12.38x result merely because it was previously
published in repository evidence.

The final release format and real LangGraph schema may change the result.

Publish the new measurement, including regressions.

### Phase 9 exit criteria

The benchmark harness can run end to end using only the installed wheel and
TulyaSaver, and produces a machine-readable summary without requiring the
shadow adapter.

Do not yet treat the numbers as public release evidence until Section 16 is
green.

---

# GATES

## 16. IMPLEMENTATION-COMPLETE / BENCHMARK-READY gate

This is the point where the implementation agent may stop feature work and hand
the repository to the benchmark/release owner.

Every item is mandatory.

### Repository

- [ ] exact candidate commit has green required CI;
- [ ] fmt/clippy/tests/package are green;
- [ ] full intended fault-injection suite runs;
- [ ] no known release-blocking corruption/data-loss bug;
- [ ] main required checks/protection configured or one explicit manual action
      remains.

### Release Format v1

- [ ] staged balanced sequence is the real CheckpointStore path;
- [ ] prototype left-deep writer is not used for new stores;
- [ ] O(parent) whole-parent append hashing is gone from release path;
- [ ] exact branch/history/range semantics pass;
- [ ] hot commit/recovery is integrated;
- [ ] seal/snapshot/reopen is integrated;
- [ ] active/retired request ledger is durable;
- [ ] deletion/tombstones are durable;
- [ ] compaction preserves logical state if exposed;
- [ ] release Format-v1 golden fixture is frozen;
- [ ] stale prototype-format compatibility language is removed.

### Lean / conformance boundary

- [ ] release-format Lean/Rust vectors pass;
- [ ] storage correspondence document matches the new release format;
- [ ] integrity primitives and assumptions are documented;
- [ ] no formal-verification overclaim.

### Locality

- [ ] 10 MiB + 1 KiB evidence;
- [ ] 100 MiB + 1 KiB evidence;
- [ ] 1 GiB + 1 KiB evidence;
- [ ] counters demonstrate no linear unchanged-parent read/hash behavior;
- [ ] range locality evidence exists;
- [ ] balancing/deep-history structural tests pass.

### Python package

- [ ] wheel builds;
- [ ] clean install works;
- [ ] no Rust runtime dependency;
- [ ] no CLI subprocess dependency;
- [ ] supported Python version matrix passes.

### TulyaSaver

- [ ] primary/authoritative saver;
- [ ] arbitrary serializer-supported values round-trip;
- [ ] namespaces preserved;
- [ ] parent config preserved;
- [ ] pending writes implemented;
- [ ] list semantics implemented;
- [ ] delete_thread implemented;
- [ ] sync and async behavior safe.

### LangGraph validation

- [ ] official base conformance passes;
- [ ] every advertised optional capability passes;
- [ ] real StateGraph sync/async tests pass;
- [ ] interrupt/resume passes;
- [ ] failed-superstep pending-write resume passes;
- [ ] time travel/fork/sibling branches pass;
- [ ] namespace/subgraph tests pass;
- [ ] delete/reopen passes;
- [ ] Python ack-to-kill-to-reopen passes.

### Benchmark harness

- [ ] headline Tulya benchmark arm uses installed TulyaSaver;
- [ ] comparator durability assumptions are explicit;
- [ ] exact reconstruction checks are mandatory;
- [ ] machine-readable evidence output is ready;
- [ ] benchmark does not reuse prototype headline numbers.

When every box above is checked, declare:

    IMPLEMENTATION COMPLETE — READY FOR FINAL BENCHMARK

Do not add more storage features before running the benchmark unless the
benchmark exposes a correctness or severe performance defect.

---

## 17. Final benchmark campaign

Run only after Section 16.

### Campaign A — storage locality

Use the frozen release-format commit.

Record:

- 10 MiB / 100 MiB / 1 GiB parent + 1 KiB append;
- read/write/node amplification;
- latency percentiles;
- CPU;
- incremental RSS;
- sync time.

### Campaign B — real LangGraph branch history

Run the pinned public branch-forest/OpenHands workload through installed
TulyaSaver and all selected comparators.

Verify every historical checkpoint before and after reopen.

### Campaign C — second independent workload

Before making broad performance language, add at least one materially different
real workload.

It should differ in one or more of:

- channel/value mix;
- branch shape;
- message sizes;
- state sizes;
- checkpoint count.

Do not tune the implementation after observing a reserved holdout and then
report the holdout as independent evidence.

### Benchmark acceptance

There is no requirement that Tulya win every metric.

The benchmark is publishable if:

- semantics are equivalent for the claim being made;
- measurements are reproducible;
- Tulya demonstrates a meaningful advantage for the target workload;
- losses are visible;
- no result depends on the discarded prototype format;
- no result bypasses TulyaSaver.

If the final product is materially worse than the old prototype result, report
the new truth and diagnose it before launch.

---

## 18. User-approach / Hacker News gate

After the final benchmark is reviewed, all of the following should be true:

- one-command/normal pip installation;
- README quickstart uses TulyaSaver directly;
- users do not need a second authoritative saver;
- official conformance evidence is linked;
- benchmark reproduction instructions are linked;
- benchmark claims point to exact release evidence;
- single-host/single-writer/local-filesystem scope is explicit;
- unsupported LangGraph optional capabilities are explicit;
- backup recommendation is explicit;
- no production-ready claim;
- no formally-verified-Rust claim;
- no universal X-times-better claim.

Recommended release wording:

> Tulya is an experimental embedded LangGraph checkpoint saver for branch-heavy
> histories. It uses a Rust persistent-history engine, passes the documented
> base LangGraph conformance and restart/durability tests, and ships with
> reproducible benchmark evidence for the tested workload.

Recommended HN framing:

> We built a branch-aware checkpoint store for agent histories in Rust, informed
> by a Lean reference model. It is now a real LangGraph saver rather than a
> shadow. Here are the crash tests, conformance results and reproducible
> benchmark. We are looking for LangGraph workloads with large/branching
> histories to break it.

Do not use "production-ready" at this milestone.

---

# IMPLEMENTATION GUIDANCE

## 19. File-level map

This section is directional, not a prohibition against moving code when a
cleaner implementation requires it.

### Storage integration

Primary files/modules:

- src/persistent_sequence.rs
- src/persistent_sequence/avl.rs
- src/persistent_sequence/format_v2.rs
- src/persistent_sequence/transaction_v2.rs
- src/persistent_sequence/commit_v2.rs
- src/persistent_sequence/hot_frame_v2.rs
- src/persistent_sequence/apply_v2.rs
- src/persistent_sequence/recovery_v2.rs
- src/persistent_sequence/snapshot_v2.rs
- src/persistent_sequence/publication_v2.rs
- src/persistent_sequence/backend_v2.rs
- src/persistent_sequence/compaction_v2.rs
- src/checkpoint_store/store.rs
- src/checkpoint_store/state.rs
- src/checkpoint_store/transaction.rs
- src/checkpoint_store/manifest.rs
- src/checkpoint_store/fsck.rs
- src/checkpoint_store/lazy.rs

### Existing fault/recovery tests to preserve/extend

- tests/crash_matrix.rs
- tests/hot_wal_faults.rs
- tests/publication_faults.rs
- tests/artifact_publication_faults.rs
- tests/message_append_streaming.rs

### Formal/vector boundary

- src/persistent_sequence/conformance_v2.rs
- src/persistent_sequence/format_v2_conformance.json
- docs/FORMAL_MODEL_CORRESPONDENCE.md
- docs/FORMAT_V2_CONFORMANCE.md

Corresponding Lean material is in the separate Tulya-MDL-Lean repository under
formal/Tulya/Incremental.

### LangGraph

Existing shadow files:

- integrations/langgraph/tulya_shadow.py
- integrations/langgraph/test_shadow_smoke.py
- integrations/langgraph/README.md

Create a real package rather than endlessly expanding the shadow module.

Suggested structure:

    python/
      pyproject.toml
      src/langgraph_checkpoint_tulya/
        __init__.py
        saver.py
        errors.py

or an equivalent maturin layout.

The exact package layout may follow standard maturin conventions.

### CI

- .github/workflows/ci.yml

Split jobs when useful so one lint failure does not hide independent evidence.

Suggested required jobs by benchmark-ready stage:

- rust-format-lint;
- rust-tests;
- rust-faults;
- rust-package;
- python-wheel;
- langgraph-conformance;
- langgraph-durability;
- security.

### Benchmark

- benchmarks/branch_forest/
- benchmarks/evidence/
- docs/BENCHMARKS.md

Keep engine diagnostic arms distinct from the final TulyaSaver product arm.

---

## 20. Error behavior

Do not reduce storage outcomes to generic IOError.

The Python layer should distinguish at least:

- not found;
- deleted;
- request conflict;
- retired request / stale retry where relevant;
- malformed/corrupt store;
- unsupported format;
- writer already locked;
- rejected mutation known not committed;
- durability indeterminate / recovery required.

An indeterminate durability result must tell the caller that retry using the
same request/checkpoint identity is the safe resolution mechanism.

Do not tell callers "write failed" when the OS result means it may have become
durable.

---

## 21. Observability needed for benchmark and early users

Before benchmark, expose enough diagnostics to answer:

- how many checkpoints/writes committed;
- bytes read/written;
- sequence nodes touched/allocated;
- durable sync latency;
- logical versus physical bytes;
- WAL bytes;
- sealed bytes;
- reopen/recovery time;
- range-read amplification;
- latest verification/fsck result if recorded.

Do not log checkpoint payloads by default.

The Python API may expose a compact stats method. Users should not need the
development-only HTTP evaluator.

---

## 22. Required documentation updates before benchmark

The implementation agent must update documentation as code changes land.

Required files:

- README.md
- docs/PRODUCTION_READINESS.md
- docs/FORMAL_MODEL_CORRESPONDENCE.md
- docs/FORMAT.md
- docs/BENCHMARKS.md
- integrations/langgraph/README.md or its replacement package README
- SECURITY.md
- CHANGELOG.md

Delete or clearly label stale "future Format v2" documents once the staged
format is promoted.

Historical design documents may remain if their first paragraph clearly says
they describe a superseded prototype/staging phase.

Do not let two documents disagree about which bytes are release Format v1.

---

## 23. Agent working rules

The implementation agent should follow these rules throughout the milestone.

1. Work in small reviewable commits/PRs even if the current production-readiness
   PR is large.

2. Do not mark a readiness checkbox complete because code exists behind
   allow(dead_code). It is complete only when the shipping path calls it and the
   acceptance evidence passes.

3. Prefer tests that fail before the implementation change.

4. Never weaken an invariant to satisfy a test.

5. Never delete a fault/corruption test merely because the new format makes the
   old fixture inconvenient. Replace it with equivalent release-format coverage.

6. Do not optimize only the messages channel in a way that makes arbitrary
   LangGraph state incorrect.

7. Do not make benchmark-specific shortcuts in the production API.

8. Do not tune against the reserved benchmark holdout.

9. Do not add unsupported marketing claims to README.

10. If upstream LangGraph changed materially, adapt to the current official
    contract and record the exact tested versions.

11. If a Lean/Rust mismatch is found, do not choose the implementation silently.
    Record the decision and update the reference/vector or explicitly document
    why the release contract differs.

12. Keep storage format changes and Python product changes separable enough that
    failures can be localized.

---

## 24. Completion evidence layout

When the implementation gate is complete, create a release-candidate evidence
directory similar to:

    benchmarks/evidence/releases/v0.1.0-alpha.1/
      git_commit.json
      release_format.json
      rust_toolchain.json
      python_matrix.json
      langgraph_versions.json
      langgraph_conformance.json
      graph_durability.json
      lean_rust_vectors.json
      crash_matrix.json
      io_fault_matrix.json
      locality_append_scaling.json
      range_read_scaling.json
      deep_history.json
      security_checks.json
      benchmark_harness_smoke.json

The final benchmark later adds:

      benchmark_openhands.json
      benchmark_second_workload.json
      claim_registry.md

Every public benchmark number must be traceable to one of these files or to an
equivalent immutable release artifact.

---

## 25. Final handoff report required from the implementation agent

When Section 16 is complete, the agent must produce a short final report with
exactly these categories:

### Candidate commit

Full Git commit SHA.

### Release format

- format version;
- golden fixture path/hash;
- prototype compatibility policy.

### CI

List required jobs and links/IDs.

### Locality evidence

Summarize 10 MiB, 100 MiB and 1 GiB fixed-delta amplification results.

### Python artifact

- wheel/package version;
- supported Python versions/platform;
- clean-install command.

### LangGraph

- exact langgraph version;
- exact langgraph-checkpoint version;
- exact conformance version;
- conformance result;
- optional capabilities intentionally unsupported.

### Durability

Summarize:

- ack -> kill -> reopen;
- pending-write restart;
- branch restart;
- delete restart;
- fault-injection result.

### Lean correspondence

- exact Lean commit used;
- vector fixture/schema version;
- known unproved platform boundaries.

### Benchmark readiness

State either:

    READY FOR FINAL BENCHMARK

or list exact blockers.

Do not use "production-ready" in this report.

---

## 26. After Hacker News

Only after the first real users begin exercising Tulya should the project spend
heavily on production-candidate work not already demanded by failures.

The next likely priorities are:

- supported backup/restore command and destructive restore drill;
- long fuzz/property campaigns;
- explicit ext4/XFS platform qualification;
- maintenance ENOSPC campaigns;
- 1m-history scale evidence;
- broader wheel/platform support based on demand;
- optional LangGraph capabilities based on demand;
- independent storage/recovery/FFI review;
- independent benchmark reproduction;
- at least one real external primary-saver pilot.

That later work is covered by docs/PRODUCTION_READINESS.md.

The first milestone is simpler:

    finish one excellent storage path
      -> expose it as one real TulyaSaver
      -> prove compatibility/durability
      -> benchmark the actual product
      -> approach users
      -> publish the evidence
