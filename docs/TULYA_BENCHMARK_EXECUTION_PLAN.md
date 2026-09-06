# Tulya Benchmark Execution Plan

Status: **authoritative benchmark execution plan**
Repository: `Vedsaga/tulya-checkpoint-store`

Engineering source of truth:
[`docs/TULYA_CORE_ENGINEERING_PLAN.md`](TULYA_CORE_ENGINEERING_PLAN.md)

Historical benchmark evidence:
[`docs/BENCHMARKS.md`](BENCHMARKS.md)

This document is intentionally separate from the engineering roadmap. It defines
when Tulya may be benchmarked publicly, what is measured, benchmark order,
backend fairness rules, artifact capture, and falsification criteria.

Do not use this document to change storage semantics. If a benchmark exposes an
engineering defect, return to the engineering plan.

---

## 1. Purpose

The benchmark program must answer two separate questions in order:

1. **Does the Tulya storage architecture actually win for large state, localized
   mutations, and many retained branches?**
2. **If yes, which real workload/adapter turns that architectural advantage into
   a useful product?**

The benchmark campaign is therefore ordered:

```text
B5  TulyaBranchBench          core architecture
 |
B1  SessionBranchBench       event/session histories
 |
B4  RLStateForkBench         SQLite-page / environment state
 |
B3  WorkspaceDiffBench       workspace/file-tree state
 |
B2  FullStateCheckpointBench structured application state
```

Do not reverse this order merely because an adapter is easier to demo.

---

# 2. Public benchmark hard prerequisite

The public flagship benchmark campaign must NOT begin until engineering task E10
in `TULYA_CORE_ENGINEERING_PLAN.md` is accepted.

Required core capabilities include:

- persistent balanced historical roots;
- arbitrary local splice/insert/delete;
- zero-content fork;
- stable logical VersionIds;
- incremental physical persistence;
- physical historical range reads after reopen;
- bounded restart;
- bounded request-receipt metadata;
- retain/expire;
- actual GC/reclamation;
- compaction preserving logical IDs;
- crash-safe authority publication/recycle;
- physical I/O instrumentation;
- frozen release Format v1;
- Lean/Rust conformance vectors;
- one authoritative generic core.

Private diagnostic microbenchmarks before E10 are allowed only for engineering
regression detection.

Do not publish or use them for product claims.

---

# 3. Benchmark principles

## 3.1 Benchmark physical behavior, not only logical behavior

Every applicable result must distinguish:

- logical state bytes;
- logical changed bytes;
- physical allocated filesystem bytes;
- physical bytes read;
- physical bytes written;
- metadata bytes;
- WAL bytes;
- snapshot/base bytes;
- peak RSS;
- CPU time;
- sync/fsync count;
- sync/fsync time.

Do not use nominal file length as a substitute for allocated bytes when sparse
files or filesystem allocation matter.

## 3.2 Correctness before performance

Every backend must reconstruct the exact expected logical state.

For each retained version that is sampled/verified, compare against a canonical
reference digest.

Verify at minimum:

- before close;
- after reopen;
- after sibling branch creation;
- after expiration;
- after GC;
- after compaction;
- after second reopen.

A performance run with incorrect reconstructed state is invalid.

## 3.3 Same workload semantics

Backends may use their natural implementation, but they must represent the same
logical workload.

Do not give Tulya append-only operations if the benchmark's semantic operation
is a point/range update.

Do not give a competitor weaker durability than Tulya without labeling the
difference.

## 3.4 Separate durability modes

Where practical report:

- **D0**: memory/no durability barrier, if backend meaningfully supports it;
- **D1**: process-crash durability / normal durable commit;
- **D2**: stronger explicit durability mode if the backend exposes one.

Headline comparisons must use semantically comparable durability.

If a backend cannot match the selected durability mode, say so rather than
silently weakening it.

## 3.5 Avoid hidden warmup advantages

Report separately:

- warm in-process;
- cold process reopen;
- cold page-cache when reproducibly measurable.

Never mix them in one latency distribution.

## 3.6 Report distributions

For latency report at least:

- p50;
- p95;
- p99;
- max;
- operation count.

Do not publish only best-case or mean latency.

---

# 4. Dedicated benchmark environment

Headline public runs should use a dedicated, documented machine rather than
GitHub-hosted CI.

Record:

- CPU model;
- physical/logical core count;
- RAM;
- storage device/model;
- filesystem;
- mount options relevant to durability;
- Linux/kernel version;
- Rust version;
- compiler profile;
- repository SHA;
- dependency lockfile hash;
- benchmark configuration;
- power/performance governor where relevant;
- free disk space before run.

CI remains useful for deterministic small regression benchmarks, not headline
performance numbers.

---

# 5. Common benchmark harness contract

All Tulya benchmark adapters should expose an internal semantic interface
approximately equivalent to:

```text
prepare_base(base_state)

fork(parent_version)

mutate(version, operation)

read(version, query)

expire(version)

gc()

compact()

seal()

close()

reopen()

verify(version)

stats()
```

The exact Rust trait/API is not a public contract and should not force core
design changes.

B5 must call `tulya-core` directly. It must not require CheckpointStore or a
framework adapter.

---

# 6. Required Tulya instrumentation

Before B5 public execution, Tulya must expose trustworthy per-run/per-operation
counters.

Algorithmic:

- AVL/tree nodes inspected;
- AVL/tree nodes allocated;
- resulting tree height where useful.

Physical:

- node bytes read;
- node bytes written;
- payload bytes read;
- payload bytes written;
- metadata bytes read;
- metadata bytes written;
- WAL bytes written;
- snapshot/base bytes written;
- sync count;
- sync duration;
- allocated filesystem bytes before/after;
- reclaimed allocated bytes.

Process:

- wall-clock latency;
- CPU time where available;
- peak/incremental RSS.

Do not label logical arena traversal as physical I/O.

---

# 7. Adapter locality diagnostics

For B1/B2/B3/B4, every Tulya adapter must report the stable-local diagnostics
defined by the engineering plan.

At minimum:

```text
semantic_delta_units
logical_state_bytes
canonical_changed_bytes
Tulya_operation_count
Tulya_inserted_bytes
Tulya_deleted_bytes
unrelated_bytes_touched
locality_ratio = canonical_changed_bytes / logical_state_bytes
```

This is required to distinguish:

```text
Tulya core lost
```

from:

```text
adapter destroyed locality before Tulya saw the edit
```

Small semantic edits that cause global canonical rewrites must be disclosed.

Lean conceptual reference for adapter methodology:

- `formal/Tulya/Incremental/PersistentAVLStableLocalAdapter.lean`
- `formal/Tulya/Incremental/PersistentAVLStableLocalAdapter.md`
- `docs/tulya-stable-local-adapter-spec.md`

---

# 8. B5 — TulyaBranchBench

## 8.1 Goal

Test the core architectural thesis directly:

> Large parent states can be forked into many retained descendants, changed
> locally, read historically, expired, reclaimed, compacted and reopened with
> work/storage substantially tied to changed/shared structure rather than
> parent-size copies or lifetime replay.

This is the flagship falsification benchmark.

## 8.2 Required state sizes

At minimum:

```text
10 MiB
100 MiB
1 GiB
```

Add larger states only after the required matrix is stable.

## 8.3 Required mutation sizes

At minimum:

```text
1 KiB
10 KiB
100 KiB
1 MiB
```

Include both replacement and insertion/deletion where competitor semantics
permit fair representation.

## 8.4 Required branch fanout

```text
1
10
100
1,000
10,000
100,000
```

Higher fanout may be added if runtime/storage permits.

## 8.5 Required topology

Run separate scenarios:

### Flat fanout

Many children from one historical parent.

### Deep chain

Repeated mutation from the latest version.

### Bushy tree

Repeated branching from multiple historical generations.

### Old-version refork

Build a history, then return to an older retained version and create a new large
fanout.

### MCTS-like

Repeated selection of retained historical states with multiple short descendant
rollouts.

## 8.6 Required reads

Measure:

- latest read;
- random historical read;
- random small range read;
- old-parent read after many descendants;
- sibling reads;
- deep-chain historical read.

Report physical bytes read, not just latency.

## 8.7 Zero-content fork scenario

Before applying any mutation:

```text
1 GiB parent
 -> 1
 -> 100
 -> 10,000
 -> 100,000 forks
```

Measure:

- fork latency;
- metadata bytes/fork;
- content node allocations;
- payload bytes written;
- total allocated bytes.

Expected Tulya semantic invariant:

```text
fork allocates zero content nodes/payload
```

Version/catalogue metadata is not zero and must be reported.

## 8.8 Parent-size locality plot

For fixed 1 KiB and fixed 4 KiB local mutation:

```text
10 MiB parent
100 MiB parent
1 GiB parent
```

Plot:

- p50/p99 mutation latency;
- nodes inspected;
- nodes allocated;
- physical node bytes read/written;
- payload bytes read/written.

The key falsification question:

> Does fixed local mutation cost remain driven primarily by tree height + delta,
> or grow roughly in proportion to parent logical size?

## 8.9 Branch storage plot

For fixed base and mutation geometry, plot allocated physical bytes against:

```text
10
100
1k
10k
100k branches
```

Include:

- logical cumulative state bytes;
- allocated physical bytes;
- metadata bytes;
- bytes/version.

## 8.10 Depth/read plot

Plot historical read p99 and physical bytes read against increasing branch
depth.

Tulya must not quietly become:

```text
base + replay N deltas
```

for historical reads.

## 8.11 Restart plot

Run histories with increasing lifetime operation count while holding retained
live state/suffix policy controlled.

Measure:

- open wall time;
- bytes read;
- snapshot verification time;
- suffix replay count.

The key question is whether restart follows:

```text
verified retained base + bounded suffix
```

rather than total lifetime history.

## 8.12 GC/reclamation scenario

Every relevant large-scale B5 run ends with:

```text
create branches
 -> expire 90%
 -> GC
 -> compact if separate
 -> reopen
 -> verify survivors
 -> measure allocated bytes
```

Repeat with 99% expiry at appropriate scale.

Report:

- before-expiry allocated bytes;
- before-GC allocated bytes;
- after-GC allocated bytes;
- after-compaction allocated bytes;
- bytes reclaimed;
- GC wall/CPU time;
- survivor read latency;
- reopen time.

No storage-efficiency headline is valid without post-GC evidence.

---

# 9. B5 competitor set

Final competitor list should be verified for availability and runnable semantics
at benchmark time.

Preferred categories:

- Tulya;
- Dolt/DoltLite or closest runnable branch-native analogue;
- Git/object-tree baseline where semantically appropriate;
- SQLite snapshot/copy baseline;
- PostgreSQL/database branch/copy baseline where reproducible;
- ProcessFork/reflink/COW baseline where applicable;
- other current BranchBench-relevant systems.

Do not include a competitor solely because it is famous if workload semantics
cannot be reproduced fairly.

For systems not runnable locally, literature-only numbers must be clearly
separated from measured results and never mixed into the same performance table
as if measured on the same machine.

---

# 10. B5 pass/fail interpretation

Do not define success as "wins every cell."

The architecture needs strong evidence for three primary claims.

## Claim A — mutation locality

Fixed small mutation must not scale linearly with unchanged parent size.

## Claim B — branch economics

Large fanout storage should substantially reflect:

```text
shared base + changed content + metadata
```

rather than:

```text
parent size * branch count
```

## Claim C — historical access/restart

Historical reads must not degrade into branch-depth delta replay, and restart
must not degrade into lifetime-log replay.

### Falsification guidance

- No meaningful high-fanout/local-edit advantage -> core thesis weakened.
- Strong microbenchmark win but no real workload has the geometry -> market/ICP
  problem; stay lean.
- Real workload has geometry but adapter destroys locality -> adapter/design
  problem.
- Adapter preserves locality but Tulya loses -> core/storage implementation
  problem.
- Real pilot integrates, sees benchmark win, then stops using Tulya -> strongest
  negative commercial signal.

---

# 11. B1 — SessionBranchBench

## Goal

Measure append-heavy durable agent/session histories with historical branch
creation and reopen.

This is the closest real workload to the original checkpoint product and should
require little/no new core storage functionality after B5.

## Tulya representation

Use a stable event/session adapter where new events are naturally append
operations and branch identity maps to VersionId lineage.

## Required workload

Use a pinned, reproducible public session/agent dataset where possible.

Measure:

- import/build latency;
- append p50/p99;
- branch from historical event;
- branch switch/read;
- cold reopen;
- random historical lookup;
- allocated bytes;
- RSS;
- post-expiration/GC storage.

## Baselines

At minimum include credible native/simple alternatives such as:

- original/native event files or JSONL;
- SQLite normalized/event schema;
- Git/object history if applicable.

Historical Tulya checkpoint benchmarks in `docs/BENCHMARKS.md` are evidence
from the prior append-only architecture and must be labeled historical rather
than silently reused as release-core results.

---

# 12. B4 — RLStateForkBench

## Goal

Test the commercially promising geometry:

```text
large environment/database state
+
small state mutation
+
many rollouts/forks
+
restore historical state
```

## Preferred first representation

SQLite-page-aware state.

Why:

- pages provide stable local replace units;
- dirty-page count is measurable;
- exact database verification is possible;
- Tulya splice maps naturally to page replacement.

## Required Tulya arms

### T1 — opaque/chunk state

A simpler representation used to show what happens without page-local
canonicalization.

### T2 — page-aware stable-local adapter

Translate dirty SQLite pages to bounded local Tulya replacements.

## Required metrics

- forks/sec;
- mutation latency;
- restore latency;
- physical bytes/rollout;
- physical bytes/dirty page;
- dirty pages/operation;
- historical read/restore bytes;
- storage per 1,000 rollouts;
- environment replication seconds per 1,000 rollouts;
- GC/reclamation after discarding losing rollouts;
- reopen and survivor verification.

## Correctness

After every sampled restore, open/verify SQLite database state and compare
semantic result/hash.

## Adapter disclosure

Report:

```text
SQL semantic change
 -> dirty pages
 -> canonical changed bytes
 -> Tulya splice bytes
```

Do not claim row-level locality when SQLite itself dirtied many pages.

---

# 13. B3 — WorkspaceDiffBench

## Goal

Test durable branching workspace/file-tree state.

## Required logical coverage

At minimum:

- tracked files;
- untracked files;
- generated files;
- directories;
- metadata;
- symlinks;
- permissions where platform permits;
- large files;
- SQLite files inside workspace.

## Candidate baselines

Verify availability at execution time, but categories include:

- Git/worktrees;
- reflink/COW copies;
- ProcessFork-like workspace snapshots;
- AgentFS or equivalent current agent filesystem;
- Factory VFS or equivalent current offering;
- full directory copy baseline.

## Required operations

- prepare base workspace;
- fork;
- edit existing file;
- create file;
- delete file;
- rename where representation supports exact semantics;
- restore old branch;
- read changed file;
- read unchanged file;
- directory listing;
- delete 90%/99% branches;
- GC;
- reopen.

## Required metrics

- fork latency;
- mutation latency;
- restore latency;
- unchanged read latency/bytes;
- changed read latency/bytes;
- allocated bytes/branch;
- metadata bytes;
- RSS;
- post-GC disk.

The adapter must expose canonicalization locality diagnostics.

---

# 14. B2 — FullStateCheckpointBench

## Goal

Test applications that conceptually checkpoint a complete structured state but
usually make small semantic changes between versions.

## Required mutation classes

- append-like change;
- point mutation;
- nested-object replacement;
- random local mutation;
- larger regional replacement.

## Tulya representation

Use deterministic canonical structured records/regions that preserve stable
identity and local updates.

Do not serialize the whole object through a format where tiny semantic edits
globally reorder/recompress data unless that behavior is explicitly the
"opaque" arm.

## Baselines

At minimum include:

- full snapshot serialization;
- compressed full snapshot;
- a competent delta baseline;
- SQLite/relational representation where appropriate.

## Required metrics

- checkpoint latency;
- restore latency;
- physical bytes/version;
- changed bytes/version;
- metadata bytes/version;
- RSS;
- historical read;
- branch from old checkpoint;
- post-GC storage.

---

# 15. Benchmark artifact layout

Each public benchmark run should produce a self-contained artifact directory.

Suggested:

```text
benchmarks/results/<benchmark>/<timestamp-or-run-id>/

  environment.json
  git.json
  config.json
  backend_versions.json
  correctness.json
  raw_operations.csv
  aggregate.json
  storage_before.json
  storage_after.json
  plots/
  stdout.log
  stderr.log
  README.md
```

Required hashes:

- repository commit;
- Cargo.lock;
- workload/corpus files;
- benchmark configuration;
- output aggregate files.

Do not manually edit aggregate results after the run without preserving the raw
artifact and documenting transformation.

---

# 16. Reproducibility

Every public benchmark must provide enough information for an independent
engineer to reproduce:

1. exact repository SHA;
2. exact workload/corpus revision;
3. setup command;
4. build command;
5. run command;
6. backend versions;
7. durability settings;
8. hardware/filesystem description;
9. correctness verification command;
10. aggregation/plot command.

Prefer scripts checked into the repo over prose-only procedures.

---

# 17. Statistical protocol

For latency-sensitive operations:

- include warmup policy;
- report number of repetitions;
- report p50/p95/p99/max;
- preserve raw samples;
- separate setup/import from steady-state operations;
- do not drop outliers without a documented mechanical rule.

For very large 100k-branch scenarios where full repetition is expensive, report
fewer complete runs honestly and use repeated smaller cells for variance
characterization.

---

# 18. Storage accounting protocol

For each backend capture:

- logical base size;
- cumulative logical version bytes;
- filesystem apparent bytes;
- filesystem allocated bytes;
- database-reported size if available;
- WAL/log bytes;
- temp/compaction peak bytes;
- post-GC allocated bytes.

Do not use only the final main data file if WAL/temp/index files are part of the
backend's required state.

Peak coexistence during compaction should be reported separately from steady
state.

---

# 19. Memory accounting protocol

At minimum distinguish:

- process baseline RSS;
- peak RSS during operation;
- incremental RSS attributable to benchmark where measurable;
- reopened idle RSS.

Do not compare Tulya peak build RSS to a competitor's idle RSS.

---

# 20. Public presentation rules

## Allowed

- exact measured ratios for the documented workload;
- workload-specific statements;
- plots with raw data available;
- explicit limitations;
- separate algorithmic and physical metrics.

## Not allowed

Do not claim:

- universal storage advantage;
- universal latency advantage;
- formally verified Rust;
- collision-free SHA-256 semantics;
- zero-cost fork if metadata is written;
- zero-copy unless the measured operation truly performs no relevant copy;
- power-loss/controller-cache guarantees not tested;
- competitor behavior from literature as if measured locally;
- ROI/customer savings without customer evidence.

---

# 21. Relationship to Lean

Lean supports the architectural/specification case, not the measured Rust
performance claim.

Appropriate public wording:

> Tulya's storage design is informed by a kernel-checked Lean reference model
> covering persistent edits, historical preservation, work bounds, lifecycle,
> crash recovery and bounded restart. The Rust implementation is not claimed to
> be mechanically verified; release behavior is checked through deterministic
> conformance vectors, fault tests and reproducible physical benchmarks.

Do not convert a Lean asymptotic theorem into a Rust performance claim without
measurement.

---

# 22. Historical benchmark policy

`docs/BENCHMARKS.md` and existing `benchmarks/evidence/` files remain useful
historical evidence.

They must be labeled by:

- old architecture SHA;
- old public API;
- append-oriented workload;
- durability differences;
- known caveats.

Do not delete them simply because the product architecture evolved.

Do not reuse historical headline ratios as release-core B5/B1/B4/B3/B2 results
unless the exact benchmark is rerun against the final engine.

---

# 23. Benchmark task checklist

Execute only after engineering E10.

```text
BM0  Freeze benchmark machine + harness + artifact schema
 |
BM1  Run B5 correctness-only small matrix
 |
BM2  Run B5 full locality/fanout/depth/restart/GC campaign
 |
BM3  Independent review of B5 evidence
 |
BM4  If B5 thesis survives, run B1
 |
BM5  Run B4 stable-local adapter qualification + benchmark
 |
BM6  Run B3 stable-local adapter qualification + benchmark
 |
BM7  Run B2 stable-local adapter qualification + benchmark
 |
BM8  Consolidate public benchmark report
```

Stop after BM3 if B5 does not show a material architectural advantage worth
pursuing.

---

# 24. Required benchmark report template

Each benchmark completion report must contain:

1. benchmark ID;
2. Tulya commit SHA;
3. workload/corpus revision;
4. machine/filesystem;
5. backend versions;
6. durability modes;
7. operation geometry;
8. correctness status;
9. raw artifact path;
10. allocated-byte accounting;
11. latency distributions;
12. physical read/write counters;
13. RSS;
14. restart measurements;
15. GC/post-GC measurements where applicable;
16. adapter locality diagnostics where applicable;
17. competitor caveats;
18. falsification/negative findings;
19. claims supported;
20. claims NOT supported.

---

# 25. Immediate benchmark instruction

Until engineering E10 is accepted:

```text
DO NOT start the public benchmark campaign.

Allowed:
- small diagnostic locality tests;
- counter validation;
- correctness-only harness development that does not distort engineering
  priorities.

Not allowed:
- publish B5 headline numbers;
- build all domain adapters;
- carry old 12.38x or other historical ratios into the new release story
  without rerunning.
```
