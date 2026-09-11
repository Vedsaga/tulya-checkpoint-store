# Frontier Flip Benchmark v1

`TULYA_FRONTIER_FLIP_V1` measures the storage/time tradeoff for online fully-persistent fixed-length bit histories.

Each case starts with an incompressible-looking deterministic base bitstring of `n` bits. Version `j` chooses one prior parent `p < j` and one bit position `i < n`, flips that bit, and creates a new version. Every old version remains readable and may be the parent of a later update.

## Why this benchmark

The companion Lean repository proves the exact finite-family count for the arbitrary-parent forced-flip model:

```text
|H(n,m)| = 2^n * m! * n^m
Info(n,m) = ceil(log2 |H(n,m)|)
```

The RANDOM topology is therefore the headline theorem-backed case. CHAIN, STAR, and BALANCED are useful operational stress shapes, but their parent topology is externally fixed, so the runner uses the smaller constrained-family count `2^n * n^m` and labels those ratios separately.

## Comparators in v1

The v1 runner deliberately uses algorithmic endpoints rather than unrelated application databases:

1. `information-floor` — the counting lower bound; not executable.
2. `packed-parent-position-log` — raw base plus bit-packed parent/position records. Near the space endpoint, but historical access walks the parent path.
3. `raw-full-snapshot-analytical` — one complete copy per version. Constant direct access, maximal duplication. The exact byte count is computed instead of writing terabytes of redundant data.
4. `chunked-cow-tree-4k` — 4 KiB SHA-256-deduplicated chunks under a persistent path-copy tree. This is the simple chunked-CoW alternative reviewers should compare against Tulya.
5. `tulya-balanced-durable` — the current durable `tulya-core` persistent-history backend, using one-byte replacement splices against arbitrary historical parents, followed by seal and reopen.

Git pack and application-level systems such as AgentFS/LangGraph are intentionally not in this first frontier suite. Git adds an offline global repacking regime, while AgentFS/LangGraph add filesystem/framework semantics that are not part of the abstract persistent-sequence problem. They should be separate product-level follow-up benchmarks once the core frontier is measured.

## Deterministic data

The base is generated in 1 MiB SHAKE-256 blocks from the benchmark name, seed, and block number. Parent and bit choices use independent SplitMix64 streams. Every case writes:

```text
base.bin
history.jsonl
manifest.json
result.json
```

The manifest records the base SHA-256 and both information charges.

## Run

From the repository root:

```bash
python3 benchmarks/frontier_flip/run.py \
  --profile quick \
  --output-dir benchmark-results/frontier-flip-v1-quick
```

`quick` runs four 1 MiB / 1,000-update histories: CHAIN, STAR, BALANCED, and RANDOM.

A single theorem-backed RANDOM case can be run with:

```bash
python3 benchmarks/frontier_flip/run.py \
  --base-bytes 1048576 \
  --updates 1000 \
  --topology random \
  --seed 1 \
  --output-dir benchmark-results/frontier-flip-v1-random
```

For a CI-sized diagnostic:

```bash
python3 benchmarks/frontier_flip/run.py --profile smoke
```

Larger profiles are available:

```text
standard: 1 MiB and 64 MiB; 1K and 10K updates; four topologies; seed 1
full:     1 MiB, 64 MiB, 1 GiB; 1K, 10K, 100K updates; four topologies; seeds 1,2,3
```

`full` is intentionally expensive. Run individual cases if RAM or disk are limited.

## What to send back for analysis

The only file needed for first-pass analysis is:

```text
benchmark-results/frontier-flip-v1-quick/summary.json
```

For the strongest first result, also run the single RANDOM case above and send its `summary.json`.

## Interpretation

The primary space metric is:

```text
actual durable/storage-model bits
---------------------------------
family information bits
```

Only the RANDOM ratio is currently tied directly to the existing Lean arbitrary-parent counting theorem. The other topologies are stress tests with a separately computed fixed-parent family charge.

This benchmark does not yet prove a global space-time optimum, machine-level cell-probe bounds, or superiority over every storage system. It is designed to answer the narrower empirical question: how much storage overhead does the current fast Tulya representation pay above the relevant information endpoint, and what access/update behavior is obtained for that overhead?
