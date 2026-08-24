# Benchmark evidence

Tulya's first public systems claim is deliberately narrow: on one pinned,
natural branch-history workload, it occupied less reopened storage than the
tested checkpoint backends while reconstructing every historical checkpoint
exactly.

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

The pinned datasets are public but are not bundled into the crate:

- `nebius/SWE-agent-trajectories` at
  `a8a64e57e7bd7ccbd1add6c4f8637c5d3834570b` (about 1.1 GB);
- `nebius/SWE-rebench-openhands-trajectories` at
  `35455389ab51bf5e2306bfd436ef72d0f98bf882` (about 2.08 GB).

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
