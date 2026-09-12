# Large Branching State Benchmark

`TULYA_LARGE_BRANCHING_STATE_V1` models a large persistent state with localized equal-length edits and arbitrary historical branching.

This benchmark is economic/engineering evidence, not an information-theoretic theorem. Its reference denominator is:

```
raw changed-payload floor = base bytes + all replacement payload bytes
```

The edit corpus is generated separately and is not counted as Tulya or CoW backend storage. The same bytes and version graph feed every executable backend.

## Default large case

- 1 GiB base
- 40 KiB edit per version (4 KiB aligned)
- 10,000 versions
- RANDOM arbitrary-parent topology
- seed 1

Backends:

1. raw changed-payload floor;
2. raw delta log (payload + compact parent/offset metadata, historical replay);
3. analytical full snapshots (never materialized);
4. 4 KiB persistent CoW tree with batch range path-copy, no per-update fsync/WAL;
5. Tulya durable balanced persistent history.

Measured values include storage, update latency, 4 KiB historical read latency, full-edit historical read latency, node allocation, seal/reopen, and correctness.

Run:

```bash
python3 benchmarks/large_branching_state/run.py \
  --base-bytes 1073741824 \
  --edit-bytes 40960 \
  --updates 10000 \
  --topology random \
  --seed 1 \
  --output-dir benchmark-results/large-branching-state-v1 \
  --cleanup-heavy
```

`--cleanup-heavy` removes `base.bin`, `edits.bin`, the CoW store, and the Tulya database only after JSON results have been written. `history.jsonl`, `manifest.json`, `result.json`, and top-level `summary.json` remain.

If a run is interrupted before cleanup, remove its output directory manually after preserving any result JSON:

```bash
rm -rf benchmark-results/large-branching-state-v1
```

The runner performs a conservative disk-free preflight and refuses to start when the configured case exceeds that budget.

## Real traces

The synthetic corpus deliberately isolates the storage primitive. A later trace adapter should preserve the same result schema while replacing generated offsets/edit bytes with real workspace changes (for example coding-agent/SWE-bench workspace diffs). Do not interpret the synthetic 40 KiB distribution as evidence that a particular market has that exact edit distribution.
