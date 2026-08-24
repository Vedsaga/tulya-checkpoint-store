# Benchmark evidence and honest claim boundary

## Claim-bearing public-corpus result

The frozen OpenHands evaluation used a pinned public repeated-attempt coding
agent corpus and a predeclared, previously untouched partition. Ten storage
arms reconstructed all 11,383 unique checkpoints exactly before and after
reopen.

Tulya's measured point was:

- 5,492,736 marginal reopened allocated bytes;
- 306,708 ns durable-append p50;
- 19,417 ns reopened historical-read p50;
- 90,595,328 bytes peak RSS.

Against the same branch DAG, direct LangGraph SQLite DeltaChannel used 12.38x
Tulya's marginal storage. Normalized and content-addressed SQLite delta arms
used 6.96x and 6.75x, with 51.24x and 77.13x Tulya's reopened-read p50.

The important loss is packed Git: it was only 3.59x larger and 2.47x slower on
reopened read p50, although that importer did not fsync one transaction per
checkpoint. Tulya also used 1.83x the peak RSS of normalized SQLite. These
losses make the claim a combined online frontier, not universal compression.

The machine-readable copy, corpus digest, frozen candidate commit, evidence
hashes, and prohibited inferences are in `benchmarks/frozen_evidence.json`.
The unchanged frozen result, claim firewall, and historical crash closeout are
preserved under `benchmarks/evidence/` rather than asking readers to trust a
summary copied into this page.

The published crate also contains the executable reproduction under
`benchmarks/branch_forest/`: raw-source profiling, deterministic partition
freezing, public-API Tulya and normalized-SQLite Rust adapters, direct
LangGraph SQLite/PostgreSQL arms, SQLite full/CAS variants, packed Git, pinned
Python dependencies, and a Docker Compose PostgreSQL service. See its README
for the exact clean-worktree command and holdout firewall.

The standalone public-API extraction was also exercised across all nine
executable arms on the same evaluation corpus and reproduced Tulya's exact
5,492,736-byte retained point with zero reconstruction failures. Its compact
record is `benchmarks/evidence/standalone_public_api_diagnostic.json`. It is
explicitly diagnostic because the extraction worktree was dirty; it closes the
private-adapter portability question but does not replace the frozen
claim-bearing result.

Safe wording:

> On a pinned public repeated-attempt agent corpus and predeclared untouched
> evaluation partition, Tulya exactly preserved the natural branch DAG while
> using 12.38x less marginal reopened storage than direct LangGraph SQLite
> DeltaChannel and 6.75–6.96x less than the tested custom SQLite delta
> baselines, with 51–77x lower historical-read p50 than those custom deltas.

Do not turn this into a 100x end-to-end cost, general semantic-memory, model
quality, power-loss, or universal storage claim.

## Required baseline set

Any new claim-bearing run must preserve all losses and compare exact semantics
against, at minimum:

- raw and independently compressed full checkpoints;
- normalized parent-operation SQLite delta and content-addressed SQLite delta;
- direct LangGraph SQLite full state and DeltaChannel;
- aggressively packed temporal Git;
- DoltLite/Prolly or an equivalent maintained structural-sharing store;
- PostgreSQL where its full physical cluster/WAL accounting is meaningful;
- Tulya hot, mixed sealed/hot, and fully sealed equilibrium.

Report complete allocated and file-length bytes, empty-store baselines,
per-checkpoint durability, exact reopen, historical reads, peak RSS, maintenance
time, and temporary coexistence bytes. Do not compare relation-only PostgreSQL
bytes with Tulya's whole directory or warm reads with controlled-cold reads.

## Fast release regression

The smoke benchmark requires no dataset download and makes no performance
claim:

```bash
cargo build --release --locked --bin tulya-checkpoint
python3 benchmarks/release_smoke.py
```

It checks exactness and retained storage for Tulya, a durable parent-plus-delta
SQLite store, and independently compressed full snapshots on one deterministic
shallow-fork workload. Its ingest timers are explicitly not symmetric.

## Dataset availability

The pinned datasets are public but intentionally not bundled into the crate:

- `nebius/SWE-agent-trajectories` at
  `a8a64e57e7bd7ccbd1add6c4f8637c5d3834570b` (~1.1 GB);
- `nebius/SWE-rebench-openhands-trajectories` at
  `35455389ab51bf5e2306bfd436ef72d0f98bf882` (~2.08 GB).

On the original development machine they already exist under
`/home/vedsaga/rust_projects/tulya-engine/benchmarks/agent_checkpoints/data`, so
no download or Kaggle compute is needed for local reproduction. Other machines
can install `huggingface_hub` and run:

```bash
python3 benchmarks/download_real_data.py --source benchmark-pair --out ./data
```

The downloader pins revisions and writes per-file SHA-256 manifests. The
reserved holdout digest is published, but it should be run once by an
independent evaluator rather than consumed for local tuning.
