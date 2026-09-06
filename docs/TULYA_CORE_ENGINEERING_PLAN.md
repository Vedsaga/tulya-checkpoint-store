# Tulya Core Engineering Execution Plan

Status: **authoritative engineering execution plan**
Repository: `Vedsaga/tulya-checkpoint-store`
Target development line: `feat/production-readiness`
Lean reference repository: `Vedsaga/Tulya-MDL-Lean`
Lean reference SHA at authoring: `f05ab7629f2b7c3581e8605f2de11b340261f84e`

This document supersedes the former HN/LangGraph launch handoff for implementation order.

Benchmark execution is intentionally separated into
[`docs/TULYA_BENCHMARK_EXECUTION_PLAN.md`](TULYA_BENCHMARK_EXECUTION_PLAN.md).
Do not put benchmark campaign work into this plan except for implementation
instrumentation and the final benchmark-readiness gate.

---

## 1. Mission

Build one clean embedded domain-neutral persistent state-history engine, reuse
the mature Rust storage/fault-testing work already in this repository, delete
superseded legacy/checkpoint-shaped storage paths as replacements become
authoritative, and freeze the first public Format v1 only after the generic
operation/lifecycle grammar is complete.

Target architecture:

```text
                    adapters
          /            |             \
 checkpoint adapter   RL adapter    future adapters
          \            |             /
                   tulya-core
                       |
             PersistentHistoryStore
                       |
          persistent editable sequence
                       |
        incremental physical persistence
                       |
         snapshot / WAL / recovery
                       |
             retention / GC / compaction
```

Required dependency direction:

```text
adapter crate -> tulya-core
```

Forbidden dependency direction:

```text
tulya-core -> checkpoint / LangGraph / framework semantics
```

---

## 2. Decisions already made

Do not reopen these unless a concrete correctness problem is discovered.

1. **No new repository.** Preserve Git history, mature fault tests, staged
   balanced-storage code, benchmark assets, and implementation provenance.
2. **Create a clean `tulya-core` crate inside this repository after P1.4.**
3. **Zero external users.** The unreleased prototype disk format has no
   compatibility promise and does not require migration.
4. The staged balanced design becomes the first released **Format v1**, after a
   deliberate final naming/wire-format cleanup.
5. Lean is a **reference/specification/conformance source**, not a claim that
   Rust is mechanically verified.
6. Core vocabulary is domain-neutral. It must not contain `thread_id`,
   `checkpoint_id`, `messages`, `task_id`, or other adapter concepts.
7. Stable logical IDs are separate from physical positions.
8. First release scope is embedded/local and single writer. Do not add
   distributed consensus or multiwriter storage.
9. Do not build another framework adapter before the core benchmark gate.
10. Do not keep obsolete code merely for safety. Git history is the archive.

---

## 3. Source-of-truth hierarchy

When documents disagree, use this order:

1. **This document** for Rust engineering order and hard gates.
2. `docs/TULYA_BENCHMARK_EXECUTION_PLAN.md` for benchmark methodology/order
   only.
3. `docs/PRODUCTION_READINESS.md` for still-applicable durability/failure
   invariants.
4. Current Rust code and tests for actual implemented behavior.
5. Lean reference paths listed in Section 6 for semantic guidance.
6. Older format/checkpoint documents only where they do not conflict with the
   above.

The old HN/LangGraph execution handoff is superseded and must not be used as an
implementation source of truth.

---

## 4. Current work state

### Remote-verified state

At authoring, the pushed `feat/production-readiness` ref is:

```text
81cbaf329dbd64f70ea9c1b247433c2b4a8f25e7
feat(storage): add generic durable history authority with replay
```

Remote-verified completed slices through that SHA:

- P0 production-readiness gate.
- P1.1 balanced sequence seam.
- P1.2 domain-neutral `PersistentHistoryStore` and CheckpointStore candidate.
- P1.3 monotonic IDs, generic operation digest, durable history WAL, request
  replay/conflict/retirement, poison, recovery from genesis.

### Agent-reported local state after remote head

The active Rust agent reports:

- P1.4-C1 `3372c35`: opaque bindings + binding-aware digest +
  idempotent `create_history_with_binding`.
- P1.4-C2 `9789a69`: generic `HistorySnapshot` codec reusing balanced image
  machinery.
- P1.4-C3a active: generic history manifest, generation filenames, writable WAL
  process lock.
- P1.4-C3b active: history authority open/seal/recycle/publication.
- P1.4-C4 next: CheckpointStore adapter maps, first-use retry, reopen, writer
  ownership integration.

These later SHAs were not visible on the remote branch when this document was
written. **Do not redo them.** When they are pushed, verify exact SHAs and tests
before continuing.

---

## 5. Mandatory engineering rules

Every slice must obey all of these.

### 5.1 No parallel authority

There must be exactly one authoritative core path for each semantic operation.
A temporary candidate path is allowed only until the replacement gate is green;
then the superseded authority must be deleted or reduced to adapter-only code.

### 5.2 Stable logical identity

`HistoryId` and `VersionId` are logical identities. GC, snapshot replacement,
segment rollover, and compaction must never renumber a retained logical version.

### 5.3 Prepare before authority

All fallible semantic validation/reservation needed to definitely reject an
operation must occur before authority-changing I/O where possible.

### 5.4 Failure classes remain explicit

Preserve the distinction between:

- definite rejection / no authority change;
- durability indeterminate / reopen required;
- committed authority followed by failed maintenance / reopen required.

### 5.5 Fail closed

Malformed IDs, roots, parents, lengths, checksums, generation references,
request records, feature bits, and cross-references must fail closed.

### 5.6 No full-parent hidden work

A local edit to a large historical state must not quietly read/hash/rewrite the
unchanged parent state in an adapter or storage helper.

### 5.7 Test knowledge survives code deletion

Before deleting an old subsystem, identify the invariant covered by its tests
and migrate/recreate that invariant against the new core.

### 5.8 Strict production lint policy

Production code remains:

```text
no unsafe
no unwrap/expect
no panic/todo/unimplemented
-D warnings
```

---

## 6. Lean reference map

Use these as semantic/reference guidance. Do not mechanically port Lean types or
historical bytes.

### 6.1 Final review boundary

- `formal/Tulya/Incremental/PersistentAVLFinalAPI.lean`

### 6.2 Persistent local edit

- `formal/Tulya/Incremental/PersistentAVLEdit.lean`
- `formal/Tulya/Incremental/PersistentAVLGrammarEditBridge.lean`
- `formal/Tulya/Incremental/PersistentAVLWorkBounds.lean`

Relevant concept: persistent insert/delete via split/concat/rebalance, exact old
root preservation, logarithmic structural work plus inserted payload.

### 6.3 Zero-content fork/publication

- `formal/Tulya/Incremental/PersistentAVLGrammarLifecycleBridge.lean`
- `formal/Tulya/Incremental/PersistentAVLGrammarBridgeAudit.md`

Relevant theorem/reference name:
`publishHousing_zero_content_growth`.

### 6.4 Incremental physical persistence

- `formal/Tulya/Incremental/PersistentAVLIncrementalSegmentCertificate.lean`
- `formal/Tulya/Incremental/PersistentAVLIncrementalSegmentBoundary.md`
- `formal/Tulya/Incremental/PersistentAVLIncrementalSegmentAudit.lean`

Relevant concepts: fresh payload/node records, checked widths, segment rollover,
foreground byte bounds, old/new crash recovery, malformed-record rejection.

### 6.5 Bounded restart and receipt horizon

- `formal/Tulya/Incremental/PersistentAVLBoundedRestartCertificate.lean`
- `formal/Tulya/Incremental/BOUNDED_RESTART_AUDIT.md`
- `formal/Tulya/Incremental/PersistentAVLReceiptRetention.lean`

Relevant concepts: canonical base replacement, bounded suffix replay, old/new
publication, cleanup, stable catalogue identities, bounded request-receipt
horizon.

### 6.6 Retention, expiration, reclamation, compaction

- `formal/Tulya/Incremental/PersistentAVLWorkBounds.lean`
- `formal/Tulya/Incremental/PersistentAVLCrashRecovery.lean`
- `formal/Tulya/Incremental/PersistentAVLSWMRAudit.md`

Relevant concepts: logical expiration separate from physical reclaim,
`boundedGCWork`, protected roots, compaction preserving decode.

### 6.7 Stable-local adapters

- `formal/Tulya/Incremental/PersistentAVLStableLocalAdapter.lean`
- `formal/Tulya/Incremental/PersistentAVLStableLocalAdapter.md`
- `docs/tulya-stable-local-adapter-spec.md`
- `formal/Tulya/Incremental/PersistentAVLAutomaticAdapter.lean`

Relevant concepts: exact encode/decode, exact translated edit semantics, bounded
translated operation count and inserted payload relative to an independently
defined semantic change measure.

### 6.8 Release-format discipline and vectors

- `formal/Tulya/Incremental/PersistentAVLV1FormatAndFailureCertificate.lean`
- `formal/Tulya/Incremental/V1_FORMAT_FAILURE_AUDIT.md`

Reuse the failure/format methodology, not the superseded historical V1 bytes.

### 6.9 Deferred capabilities

Do not block the first flagship benchmark on these:

- exact comparison/LCE:
  `PersistentAVLComparison.lean`,
  `PersistentAVLCompressedComparisonAudit.md`;
- compressed-base/automatic compressed history:
  `PersistentAVLAutomaticCompressedHistoryCertificate.lean`;
- full fine-grained reader pin/epoch realization;
- CPU memory-order refinement;
- formal Rust refinement;
- secure deletion;
- multiwriter/distributed storage.

---

# 7. Ordered task graph

The agent must execute the tasks in this exact order unless a STOP condition in
Section 19 applies.

```text
E0  Finish P1.4 authority/bounded reopen
 |
E1  Extract tulya-core crate (behavior preserving)
 |
E2  Persistent splice/insert/delete
 |
E3  Zero-content fork/publication
 |
E4  Retention/expiration + bounded request receipts
 |
E5  Generic GC + stable-ID compaction
 |
E6  Incremental physical persistence + physical reads
 |
E7  Stable-local adapter discipline
 |
E8  Delete/genericize superseded legacy/staged code
 |
E9  Freeze release Format v1 + Lean/Rust vectors
 |
E10 Engineering-complete / benchmark-ready gate
```

**Do not start E(n+1) until E(n) acceptance is green.**

---

# 8. E0 — Finish P1.4

## Objective

Close generic base/suffix authority, durable adapter binding, first-use
idempotency, and core writer ownership before any crate move.

## Required work

Finish the currently active C3/C4 implementation:

1. Generic manifest codec and generation naming.
2. Generic writable WAL ownership/process lock.
3. Generic sealed snapshot authority.
4. Exact represented-WAL-prefix coordinate.
5. Publication sequence:
   - build next snapshot;
   - write complete bytes;
   - sync snapshot;
   - publish final snapshot artifact;
   - directory sync where required;
   - publish manifest/authority;
   - make authority durable;
   - only then recycle represented WAL prefix.
6. Open must select exactly one valid authority and replay only the suffix not
   represented by the selected snapshot.
7. Corrupt complete authoritative snapshot/manifest fails closed.
8. Incomplete/unpublished artifact cannot displace last durable authority.
9. Opaque adapter binding survives reopen.
10. First adapter operation retry cannot create a duplicate logical HistoryId.
11. Generic core owns/enforces single-writer authority.
12. Second writable open fails explicitly.

## Acceptance

- snapshot roundtrip;
- snapshot + suffix;
- branch reopen;
- bindings reconstruct exactly;
- request ledgers reconstruct exactly;
- next-ID counters restore correctly;
- crash cut before/within/after snapshot publication;
- crash cut after authority but before/during WAL recycle;
- corrupt snapshot/root/parent/ID/counter/ledger fail closed;
- bounded suffix demonstrated;
- second writer rejected;
- CheckpointStore candidate uses generic authority with no legacy transaction
  dual-write for candidate operation;
- full gates green.

## Lean references

Section 6.5 and 6.6.

---

# 9. E1 — Extract `tulya-core` crate

## Objective

Create a compile-time architectural boundary without changing behavior.

## Prerequisite

E0 accepted.

## Required workspace shape

Preferred:

```text
crates/
  tulya-core/
  tulya-checkpoint/
```

A minimally invasive equivalent is acceptable if Cargo enforces the same
dependency direction.

## Move into `tulya-core`

Domain-neutral pieces only:

- `PersistentHistoryStore`;
- `HistoryId`, `VersionId`, `Version`;
- generic request ledger/digest;
- opaque binding semantics;
- generic snapshot/manifest/authority/WAL/recovery;
- `BalancedSequence`;
- AVL/root/range/image primitives;
- generic durability error classes where not checkpoint-specific.

## Keep outside core

- thread/checkpoint IDs;
- checkpoint number/namespace;
- messages/identity/result roles;
- LangGraph or Python behavior;
- same-thread subtree-delete policy;
- checkpoint canonical JSON;
- checkpoint-specific tombstones/ordinals.

## Staged code rule

For every staged v2 module, classify:

- **MOVE**: already generic;
- **HARVEST**: algorithm/framing useful, record vocabulary must be rewritten;
- **ADAPTER**: checkpoint-only;
- **DELETE-LATER**: superseded after replacement.

Record the classification in the E1 report.

## Acceptance

- `tulya-core` builds/tests independently;
- checkpoint crate depends on core;
- no inverse dependency;
- no checkpoint vocabulary in core;
- no semantic wire-format change in E1;
- all existing generic and adapter tests green;
- fault suite green.

---

# 10. E2 — Persistent local splice/edit

## Objective

Upgrade the engine from append-history to fully persistent locally editable
state.

## Required core semantic operation

Implement a canonical operation equivalent to:

```rust
splice(parent_version, offset, delete_len, inserted_bytes, request_id?)
```

Append may be a convenience wrapper over splice.

Must support:

- append;
- insert;
- delete;
- equal-length replace;
- shorter replace;
- longer replace;
- editing any retained historical version;
- sibling edits from the same historical parent.

## Implementation requirement

Use persistent structural editing: split/concat/rebalance/path-copy or a
semantically equivalent balanced method.

Forbidden implementations:

- read whole parent then rewrite complete state;
- adapter-level delta replay that does not make the new Version root represent
  the complete logical state;
- parent-size hashing to validate every local edit.

## Durable grammar

The generic operation digest and WAL record must bind the exact mutation:

- operation kind;
- history;
- parent;
- offset;
- delete length;
- inserted length;
- inserted bytes;
- relevant binding bytes.

A changed coordinate under the same request ID must conflict.

## Tests

At minimum:

- insert/delete/replace at start/middle/end;
- append through edit grammar;
- invalid offset/range;
- overflow;
- old root exact after descendant edit;
- branch from old version then edit;
- two sibling edits;
- cross-history parent rejected;
- reopen exact;
- request replay/conflict;
- corruption fail closed;
- 100 MiB+ parent / 4 KiB mutation locality regression.

## Lean references

Section 6.2.

---

# 11. E3 — Zero-content fork

## Objective

Create a new durable logical Version using an existing persistent root without
adding content nodes/payload.

## Required semantic operation

Equivalent to:

```rust
fork(history, parent_version, request_id?, binding?) -> Version
```

Result requirements:

- new stable VersionId;
- parent = source VersionId;
- root exactly equals source root;
- zero payload bytes added;
- zero content-tree nodes allocated.

Do not simulate fork with dummy bytes.

## Tests

- one fork;
- 1,000 forks from the same historical version;
- all VersionIds distinct;
- roots equal source root;
- zero content allocation counters;
- durable replay/conflict;
- close/reopen exact;
- fork an old version after later descendants exist.

## Lean references

Section 6.3.

---

# 12. E4 — Retention/expiration and bounded receipts

## Objective

Separate logical lifecycle from physical storage lifecycle and prevent request
metadata from growing forever.

## Required version states

At minimum:

- retained;
- expired;
- protected while required by current authority/maintenance.

Core operation:

```text
expire(VersionId)
```

Expiration means the version is no longer available for new logical acquisition
according to core policy. It does **not** imply immediate physical deletion.

Adapter-specific branch/subtree deletion must translate to a set of generic
VersionIds to expire.

## Request receipt horizon

Introduce explicit request-receipt retention policy.

Required semantics:

- retained receipt + same digest -> replay;
- retained receipt + different digest -> conflict;
- retired retained receipt + same digest -> retired/no mutation;
- retired retained receipt + different digest -> conflict;
- expired receipt must never be mistaken for a retained receipt;
- semantics after receipt expiry must be documented and deterministic.

Snapshot/restart metadata must be bounded by policy rather than lifetime request
count.

## Tests

- expire leaf;
- expire internal historical version;
- sibling retained;
- parent retained as needed by retained descendant semantics;
- receipt horizon boundary;
- snapshot/reopen of retained/expired status;
- large request-count ledger remains bounded.

## Lean references

Sections 6.5 and 6.6.

---

# 13. E5 — Generic GC and compaction

## Objective

Reclaim physical storage for expired history while preserving every retained
logical identity and byte result.

## First-release concurrency scope

Quiescent GC is acceptable:

```text
one writer
no active reader sessions during physical reclamation
```

Do not block E5 on full fine-grained reader epochs.

## Protected roots

At minimum protect:

- all retained versions;
- currently authoritative snapshot/base roots;
- current publication candidate/recovery fallback roots required by crash
  semantics.

## GC requirements

- trace each shared physical node once;
- reclaim only unreachable content;
- shared nodes survive if any retained root reaches them;
- no retained version changes decode;
- recovery metadata needed by valid authority survives.

## Compaction requirements

Physical relocation may change:

- node offsets;
- payload offsets;
- segment IDs;
- table positions.

It must not change:

- HistoryId;
- VersionId;
- parent VersionId;
- logical bytes;
- request/binding semantics.

If dense `versions[VersionId]` prevents relocation, introduce an explicit
logical-ID-to-record lookup/index. Do not renumber VersionIds.

## Tests

- create large branch set;
- expire 90%;
- GC;
- compact;
- verify survivors;
- close/reopen;
- branch from survivor after compaction;
- shared-content retention;
- physical locations change while logical IDs do not;
- crash/failure cuts around replacement publication.

## Lean references

Section 6.6.

---

# 14. E6 — Incremental physical persistence and physical reads

## Objective

Make locality true in the durable backend, not only in the in-memory AVL.

## Required foreground property

A local mutation writes only:

- new payload;
- path-copy balanced-tree nodes;
- bounded commit/catalogue metadata;
- required authority/WAL bytes.

It must not rewrite/read/hash the unchanged parent state proportional to parent
logical size.

## Staged Rust donor audit

Audit:

- `format_v2.rs`
- `image_v2.rs`
- `transaction_v2.rs`
- `hot_frame_v2.rs`
- `recovery_v2.rs`
- `backend_v2.rs`
- `snapshot_v2.rs`
- `publication_v2.rs`
- `compaction_v2.rs`
- `conformance_v2.rs`

Reuse sound mechanics, but remove checkpoint-specific record vocabulary from
core.

Do not create a third independent tree serialization unless existing generic
image machinery is demonstrably unsuitable.

## Physical random read requirement

A historical range read after reopen must be capable of:

```text
VersionId
 -> root descriptor
 -> O(log n) physical node navigation
 -> requested payload block(s)
```

without materializing the complete retained database into RAM first.

## Required counters

Separate algorithmic from physical counters.

Algorithmic:

- nodes inspected;
- nodes allocated.

Physical:

- node bytes read;
- node bytes written;
- payload bytes read;
- payload bytes written;
- metadata bytes read/written;
- WAL bytes written;
- snapshot bytes written;
- sync/fsync count;
- sync time.

## Acceptance

For fixed 4 KiB mutation, increasing parent from 10 MiB -> 100 MiB -> 1 GiB
must not make physical foreground work proportional to parent size.

Historical small range reads must likewise not require parent-sized physical
reads.

## Lean references

Section 6.4.

---

# 15. E7 — Stable-local adapter discipline

## Objective

Define how adapters prove correctness and demonstrate that they preserve
Tulya's locality advantage.

Do **not** create a broad plugin framework.

Each serious adapter must define:

- deterministic canonical encoding;
- exact decoding;
- semantic edit;
- translation to Tulya edit operation(s);
- independently chosen semantic change measure.

Required correctness:

```text
decode(encode(x)) == x

apply_tulya_edits(encode(x), translate(x,e))
==
encode(apply_semantic_edit(x,e))
```

Required diagnostics:

- total canonical bytes;
- semantic delta units;
- canonical bytes changed;
- Tulya operation count;
- inserted bytes;
- deleted bytes;
- unrelated bytes touched;
- locality ratio = changed canonical bytes / total canonical bytes.

Reject/redesign adapters where small semantic changes routinely cause global
canonical rewrites.

## Lean references

Section 6.7.

---

# 16. E8 — Remove stale/legacy/parallel architecture

## Objective

Converge to one clean engine before release freeze.

### Mandatory stale-code classification

For every old/staged module classify:

- KEEP — still authoritative;
- MOVE — generic and belongs in core;
- ADAPTER — framework/checkpoint concern;
- HARVEST — keep algorithm/tests, delete old wrapper;
- DELETE — superseded/unreachable.

### Expected deletion/genericization targets

When replacements are authoritative, remove as applicable:

- LegacyV1 left-deep shipping writer;
- old whole-parent hashing paths;
- old candidate dual-authority logic;
- obsolete `history.wal` staging authority if replaced by generation authority;
- checkpoint-shaped staged backend records that have generic replacements;
- duplicate recovery/publication implementations;
- dead migration/compatibility code;
- unused feature gates;
- stale v2/T2 names that are not deliberately retained;
- obsolete docs describing a no-longer-existing authority.

### Rule

Every new generic subsystem must name the old subsystem it replaces. A slice is
not complete if it leaves an obsolete parallel authority indefinitely.

---

# 17. E9 — Freeze release Format v1 and Lean/Rust conformance

## Prerequisite

Do not begin freeze until E2-E8 semantics required by the public core are
settled.

Format v1 must be able to represent, directly or through the selected canonical
authority model:

- history creation/binding;
- stable versions and parents;
- append/splice mutation;
- zero-content fork;
- request receipts and retention policy;
- retained/expired lifecycle state needed for recovery;
- persistent roots;
- snapshot/base generation;
- bounded hot suffix;
- metadata needed for safe GC/compaction.

## Release-format contract

Deliberately define:

- magic;
- major/minor format version;
- profile ID;
- byte order where relevant;
- mandatory feature mask;
- optional feature mask;
- fixed-width field contract;
- integrity/checksum algorithm ID;
- declared lengths;
- reserved fields;
- complete input consumption;
- overflow/exhaustion behavior;
- unknown mandatory feature rejection;
- optional-feature behavior;
- no legacy reinterpretation/fallback.

Interim tags such as `THL1`, `THLF`, `THS1`, `T2I2`, and
`tulya-history/v1/...` must be deliberately accepted, renamed, or replaced
before freeze. No accidental compatibility promise.

## Lean/Rust conformance

Do not claim Rust is formally verified.

Create language-neutral vectors derived from the release semantic contract and
Lean reference where applicable.

Cover at minimum:

- valid image/root/version records;
- append/splice;
- fork;
- request replay/conflict;
- receipt expiry;
- retention/expiration;
- base + suffix recovery;
- overflow;
- malformed lengths;
- invalid IDs/parents;
- corrupt integrity bytes;
- truncation/trailing bytes;
- unsupported features;
- old/new publication;
- cleanup/recycle.

Run the vectors against Rust in CI.

## Lean references

Sections 6.5 and 6.8.

---

# 18. E10 — Engineering-complete / benchmark-ready gate

Engineering is benchmark-ready only when all of the following are true:

- [ ] `tulya-core` exists as the primary generic engine crate.
- [ ] No checkpoint/framework vocabulary exists in core.
- [ ] Balanced persistent historical roots are authoritative.
- [ ] Arbitrary local splice/insert/delete is durable.
- [ ] Zero-content fork is durable.
- [ ] HistoryId/VersionId are stable through snapshot/GC/compaction.
- [ ] Incremental physical persistence is authoritative.
- [ ] Small historical reads are physically local after reopen.
- [ ] Restart uses verified base + bounded suffix, not lifetime replay.
- [ ] Request receipt metadata is policy-bounded.
- [ ] Generic logical expiration exists.
- [ ] Physical GC/reclamation exists.
- [ ] Compaction preserves logical IDs and bytes.
- [ ] Crash-safe authority publication/recycle is fault-tested.
- [ ] Generic single-writer ownership is enforced.
- [ ] Physical I/O counters are trustworthy.
- [ ] Release Format v1 is deliberately frozen.
- [ ] Release Lean/Rust conformance vectors pass.
- [ ] Legacy/candidate/staged duplicate authorities are removed.
- [ ] fmt/clippy/all-targets/fault/package/CI gates are green.

Only after this gate passes should the public benchmark campaign in
`TULYA_BENCHMARK_EXECUTION_PLAN.md` begin.

Private diagnostic microbenchmarks are allowed before E10 only to catch
regressions and validate locality assumptions. They are not public evidence.

---

# 19. STOP conditions

Stop and report instead of improvising if any of these occur:

### S1 — Core dependency inversion

A required core implementation would need to import checkpoint/framework
semantics.

### S2 — Lean semantic conflict

A proposed generic edit/lifecycle behavior conflicts with a Lean invariant rather
than merely choosing a different Rust representation.

### S3 — Stable-ID conflict

Planned compaction/reclamation cannot preserve stable VersionId semantics.

### S4 — Data-structure mismatch

Physical locality cannot be achieved with the existing persistent AVL design
without fundamentally replacing the data structure.

### S5 — Authority mismatch

Snapshot/base advancement cannot provide old-or-new recovery without a
fundamentally different publication authority.

### S6 — Premature format freeze

A required operation would force an incompatible release-format change because
the current grammar was frozen too early.

### S7 — Duplicate implementation growth

A new implementation would create a third long-lived authority for the same
semantic state instead of replacing/genericizing an existing path.

---

# 20. Required validation commands

Unless a slice explicitly requires a stronger command, run:

```bash
cargo fmt --all -- --check

cargo clippy --all-targets --features local-server --locked -- -D warnings

cargo test --all-targets --features local-server --locked -- --test-threads=1

cargo test --locked --features fault-injection -- --test-threads=1

cargo package --locked
```

After workspace extraction, run equivalent workspace-wide forms and ensure both
core and adapter packages are covered.

No slice is accepted with ignored lint/test failures.

---

# 21. Required agent report template

Every completed slice report must include exactly these headings:

1. **Task ID**
2. **Commit SHA(s)**
3. **Authoritative path after this slice**
4. **Dependency direction**
5. **Persisted grammar changes**
6. **Stable-ID implications**
7. **Crash/durability semantics**
8. **Code moved/genericized**
9. **Code deleted**
10. **Stale/parallel code remaining**
11. **Tests added/migrated**
12. **Locality evidence/counters**
13. **fmt**
14. **clippy**
15. **all-target tests**
16. **fault-injection**
17. **package**
18. **exact CI run + head SHA**
19. **Lean reference consulted**
20. **Lean/Rust mismatch or none**
21. **Format-v1 decision forced by this slice**
22. **Next task ID**

Do not report a task as complete if its acceptance gate is only planned.

---

# 22. Immediate instruction to the active Rust agent

The next action is unambiguous:

```text
CURRENT TASK: E0 / finish P1.4

1. Finish C3b authority seal/open/recycle and its crash/corruption tests.
2. Finish C4 adapter binding reconstruction, first-use retry and generic
   single-writer integration.
3. Run all validation gates.
4. Push exact commits.
5. Report using Section 21.
6. STOP for review.

DO NOT:
- start crate extraction;
- start local edit;
- start GC;
- freeze Format v1;
- run/publicize flagship benchmarks.
```

After E0 is independently accepted, execute **E1 core extraction** next.
