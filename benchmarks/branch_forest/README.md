# Branch-forest benchmark

This is the executable reproduction package for Tulya's first systems claim.
It consumes a natural repeated-attempt coding-agent corpus, preserves its exact
branch DAG, performs one durable transaction per checkpoint, reopens every
backend, and reconstructs every historical state before reporting measurements.

The frozen benchmark is not arbitrary state. It canonicalizes an ordered
OpenHands `messages` history, then creates exactly one `append_message`
operation per checkpoint. The published ratios apply to that append-only state
shape. They do not predict storage for opaque full-state blobs or in-place
replacement.

It uses one benchmark contract, `TULYA_BRANCH_FOREST_BENCHMARK_V1`, and the
released Tulya checkpoint format. The Tulya arm uses only the crate's
normal public API; it does not encode private WAL transactions.

## Comparator matrix

The runner executes nine arms with matching logical checkpoint identities:

1. Tulya Checkpoint Store;
2. normalized SQLite parent-plus-operation delta;
3. SQLite content-addressed delta;
4. SQLite Zstandard-compressed full snapshots;
5. SQLite raw full snapshots;
6. direct LangGraph SQLite full-state checkpointing;
7. direct LangGraph SQLite `DeltaChannel` checkpointing;
8. aggressively packed Git history;
9. direct LangGraph PostgreSQL full-state checkpointing.

PostgreSQL relation bytes, database bytes, generated WAL, and settings are
reported separately; relation-only bytes must not be presented as whole-system
storage. Git's importer is packed aggressively but does not claim one `fsync`
per logical checkpoint, so its append time is not durability-equivalent.

## Install

Prerequisites are Rust 1.80 or newer, Python 3.12, Git, and Docker with Compose
for the PostgreSQL arm.

```bash
python3 -m venv .venv-branch-forest
.venv-branch-forest/bin/pip install -r benchmarks/branch_forest/requirements.txt
cargo build --release --locked \
  --example branch_forest_tulya \
  --example branch_forest_sqlite
```

## Obtain and freeze the public corpus

The raw datasets are not redistributed in the crate. Download the two pinned
public sources once, then reuse the locally verified copies across runs.

```bash
.venv-branch-forest/bin/python benchmarks/download_real_data.py \
  --source benchmark-pair --out ./data

.venv-branch-forest/bin/python benchmarks/branch_forest/freeze_corpus.py \
  --source nebius-openhands \
  --data-root ./data \
  --output-dir ./benchmark-results/branch-forest
```

Relative data and output paths are resolved from the command's working
directory. The freezer works both in a Git checkout and an unpacked crate; its
manifest records either the exact Git state or a deterministic packaged-source
tree digest.

The frozen OpenHands evaluation digest used by claim
`TULYA-BF-OH-EVAL-R1` is
`a931c659530c083933b7da5fd886bcee0068c8c8df3ce57f6aea43fae18df12e`.
Verify the generated file before comparing with the published result.

## Run

Use a new output directory. A clean worktree is required for a reproduction
result; `--allow-dirty` explicitly labels the run as an adapter diagnostic.
The runner records the output filesystem and refuses `tmpfs`/`ramfs` by
default, because their append latency is not a durable-storage measurement.
When run from an unpacked published crate rather than its own Git worktree, the
runner records a deterministic source-tree digest and labels the result as a
packaged-source diagnostic. Reserved holdout access still requires a clean,
commit-addressable worktree.

```bash
.venv-branch-forest/bin/python benchmarks/branch_forest/run.py \
  --corpus ./benchmark-results/branch-forest/nebius-openhands/evaluation.jsonl \
  --output-dir ./benchmark-results/openhands-evaluation
```

The runner preserves stdout and stderr for every arm and writes one
`summary.json` containing corpus SHA-256, Git state, binary SHA-256, host
information, exactness, allocated/file-length bytes, durable append latency,
reopen latency, historical-read latency, peak RSS, and comparator ratios.
Create a portable record without absolute store paths or verbose per-file
accounting with:

```bash
python benchmarks/branch_forest/summarize.py \
  ./benchmark-results/openhands-evaluation/summary.json \
  --output ./benchmark-results/openhands-evaluation-evidence.json
```

## Crash-and-resume demo

The crash demo uses the same natural corpus and normal public API. It forces a
userspace process exit after the checkpoint WAL is synchronized but before the
append is published to in-memory state, then reopens the store, detects the
committed prefix, resumes the remaining branches, seals, reconstructs every
checkpoint, and runs independent read-only `fsck`.

```bash
.venv-branch-forest/bin/python benchmarks/branch_forest/crash_demo.py \
  --corpus ./benchmark-results/branch-forest/nebius-openhands/evaluation.jsonl \
  --output-dir ./benchmark-results/crash-demo
```

This establishes process-crash recovery at the named userspace boundary. It
does not establish sudden-power-loss, disk-controller-cache, or torn-sector
safety; the separate Rust test covers all 32 deterministic seal handoff cases.

## Holdout firewall

Files named `holdout.jsonl` are rejected by default. A one-shot holdout run
requires `--authorize-holdout`, its predeclared SHA-256, an evaluator identity,
an independent-machine label, and a clean worktree. Do not run the reserved
holdout on a development machine or tune after observing it.

No benchmark result authorizes claims about 100x customer economics, universal
superiority to Git, device-level power-loss safety, semantic memory, or model
quality.
