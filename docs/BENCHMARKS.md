# Benchmark evidence

Tulya's first public systems claim is deliberately narrow: on one pinned,
natural branch-history workload, it occupied less reopened storage than the
tested checkpoint backends while reconstructing every historical checkpoint
exactly.

## Exact workload contract

The headline result uses only a frozen evaluation subset of
[`nebius/SWE-rebench-openhands-trajectories`](https://huggingface.co/datasets/nebius/SWE-rebench-openhands-trajectories/commit/35455389ab51bf5e2306bfd436ef72d0f98bf882),
not arbitrary application state and not the entire source dataset.

```text
source revision:
35455389ab51bf5e2306bfd436ef72d0f98bf882

frozen evaluation SHA-256:
a931c659530c083933b7da5fd886bcee0068c8c8df3ce57f6aea43fae18df12e
```

Each source row is a complete OpenHands attempt to resolve a software issue.
The freezer preserves normalized system, user, assistant/tool-call, and tool
messages. Every next message becomes one checkpoint whose only operation is
`append_message` against the exact parent history.

| Workload property | Value |
| --- | ---: |
| Software-task instances | 8 |
| Independent attempts | 91 |
| Observed checkpoints | 11,549 |
| Unique checkpoint nodes | 11,383 |
| Messages per attempt | 123 median; 197 p95; 77–201 range |
| Appended-message JSON | 529 B median; 6,018 B p95 |
| Resulting cumulative state | 112,204 B median; 257,973 B p95 |
| Append / resulting-state size | 0.71% median; 8.26% p95 |
| Sum of unique full-snapshot states | 1,382,359,956 logical bytes |
| Reused exact-prefix observations | 166 |

This is a favorable shape for persistent sequences: the parent is usually
large and the next operation is relatively small. Cross-attempt reuse existed,
but it was modest; the result was not produced by a corpus of mostly identical
branches.

The exact machine-readable shape and definitions are in
[`benchmarks/evidence/evaluation_workload_shape.json`](../benchmarks/evidence/evaluation_workload_shape.json).
No published ratio should be transferred to arbitrary blobs, in-place state
replacement, approximate similarity, compressed/encrypted state, or a
different checkpoint geometry without a new same-work measurement.

## Published result

The frozen OpenHands evaluation contains 11,383 unique checkpoints from a
public repeated-attempt coding-agent corpus. All ten claim-bearing arms
reconstructed every checkpoint exactly before and after reopen.

Tulya measured:

- 5,492,736 marginal reopened allocated bytes;
- 306,708 ns durable-append p50;
- 19,417 ns reopened historical-read p50;
- 90,595,328 bytes peak RSS.

The clean public-API reproduction recorded these exact storage points on the
same evaluation corpus:

| Store | Marginal storage after reopen | Comparator / Tulya |
| --- | ---: | ---: |
| **Tulya** | **5.49 MB** | **1.00x** |
| Packed Git | 19.67 MB | 3.58x |
| SQLite content-addressed delta | 37.10 MB | 6.75x |
| SQLite normalized delta | 38.24 MB | 6.96x |
| LangGraph SQLite DeltaChannel | 68.00 MB | 12.38x |

The frozen claim-bearing ratios were 12.38x for LangGraph SQLite
DeltaChannel, 6.75–6.96x for the custom SQLite delta stores, and 3.59x for
packed Git. The custom SQLite delta stores also had 51–77x Tulya's reopened
historical-read p50 in that frozen run.

Safe summary:

> On a pinned public repeated-attempt agent corpus of 11,383 checkpoints,
> Tulya exactly preserved the natural branch DAG while using 12.38x less
> marginal reopened storage than direct LangGraph SQLite DeltaChannel and
> 6.75–6.96x less than the tested custom SQLite delta stores. Those custom
> delta stores had 51–77x higher reopened historical-read p50 on that workload.

## Losses and qualifications

This is a combined online storage/read point, not a universal compression
result:

- Packed Git came closest on storage. It was 3.59x larger and 2.47x slower on
  reopened-read p50 in the frozen run, but its importer did not fsync one
  transaction per checkpoint.
- Tulya used 1.83x the peak RSS of normalized SQLite in the frozen run.
- PostgreSQL relation bytes are not directly comparable with Tulya's complete
  store directory; PostgreSQL database growth and generated WAL are reported
  separately.
- The result does not establish customer ROI, independent-holdout success,
  device-level power-loss safety, or superiority on every workload.

The machine-readable claim is
[`benchmarks/frozen_evidence.json`](../benchmarks/frozen_evidence.json). The
clean same-machine public-API reproduction is
[`benchmarks/evidence/clean_public_api_reproduction.json`](../benchmarks/evidence/clean_public_api_reproduction.json);
it confirms extraction portability but is not the reserved independent
holdout.

## Post-refactor regression rerun

On 2026-08-24, the standalone repository at clean commit `55f26a0` was rerun
against both available engineering partitions through the current public API
adapter and the complete current nine-arm matrix.

| Corpus | Input SHA-256 | Attempts | Unique checkpoints | Result |
| --- | --- | ---: | ---: | --- |
| SWE-agent engineering | `f1872fef22f3cded330c38a6cce4012e92646e7bc01f69d9a3ec8c57f1b95e76` | 147 | 4,070 | All nine arms passed and reconstructed exactly after reopen; Tulya was exact hot, sealed, and reopened. |
| OpenHands engineering | `a006bd2923eae9607cda3dfd1ab64e3117c53f5fbc493e6cf18ecbf7621656d5` | 51 | 7,573 | All nine arms passed and reconstructed exactly after reopen; Tulya was exact hot, sealed, and reopened. |

Portable records:

- [`post_refactor_swe_engineering_55f26a0.json`](../benchmarks/evidence/post_refactor_swe_engineering_55f26a0.json)
- [`post_refactor_openhands_engineering_55f26a0.json`](../benchmarks/evidence/post_refactor_openhands_engineering_55f26a0.json)

Both campaigns recorded a clean tree before and after execution, all comparator
counts equal, all frozen engineering thresholds passing, and
`holdout_accessed: false`. These are same-machine regression checks. They show
that the refactor did not break the tested branch-history contract; they do not
replace an independent reserved-holdout run or prove customer economics.

## Reproduce it

The release smoke benchmark needs no dataset download and makes no performance
claim:

```bash
cargo build --release --locked --bin tulya-checkpoint
python3 benchmarks/release_smoke.py
```

The complete public-corpus runner lives in
[`benchmarks/branch_forest`](../benchmarks/branch_forest/README.md). It includes
raw and Zstandard-compressed full snapshots, normalized and content-addressed
SQLite deltas, direct LangGraph SQLite full and DeltaChannel modes, packed Git,
and direct LangGraph PostgreSQL.

Any new claim-bearing comparison must retain exact checkpoint semantics and
report:

- allocated and file-length bytes for the complete store;
- empty-store baselines and temporary coexistence bytes;
- per-checkpoint durability policy;
- exact reconstruction before and after reopen;
- durable-write and historical-read p50/p95/p99;
- peak RSS, maintenance time, and reopen time;
- Tulya hot, mixed sealed/hot, and fully sealed equilibrium.

Do not compare relation-only PostgreSQL bytes with a complete Tulya directory,
warm reads with controlled-cold reads, or asynchronous/batched durability with
one-fsync-per-checkpoint ingestion without labeling the difference.

## Dataset availability

The headline claim uses this source only:

- [`nebius/SWE-rebench-openhands-trajectories`](https://huggingface.co/datasets/nebius/SWE-rebench-openhands-trajectories)
  at `35455389ab51bf5e2306bfd436ef72d0f98bf882` (about 2.08 GB).

The runner can also freeze a separate SWE-agent source for engineering
comparisons, but results from that source are not the 12.38x headline claim:

- `nebius/SWE-agent-trajectories` at
  `a8a64e57e7bd7ccbd1add6c4f8637c5d3834570b` (about 1.1 GB);

Download and hash them with:

```bash
python3 benchmarks/download_real_data.py --source benchmark-pair --out ./data
```

The downloader pins revisions and writes per-file SHA-256 manifests. Reserved
holdout files are rejected by the normal runner and should be evaluated once,
on an independent machine, without tuning after observation.

## Claim boundary

Do not translate this benchmark into a 100x end-to-end cost claim, universal
superiority to Git or optimized history stores, semantic-memory or model-quality
improvement, sudden-power-loss safety, or customer economics without a measured
customer deployment.
