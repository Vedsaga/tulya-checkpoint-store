# Tulya Checkpoint Store

A focused Rust checkpoint store for retaining exact, branch-aware,
append-only agent message histories with structural sharing.

The current product probe provides:

- one durable writer with a preinitialized hot WAL;
- exact root, child and sibling reconstruction;
- immutable sealed generations and hot-prefix reclaim;
- exact full-store verification and deterministic export/import;
- a `tulya-checkpoint` CLI;
- a fail-open LangGraph shadow adapter.

It does not claim to be a complete LangGraph saver. Pending writes, arbitrary
channel mutation, async APIs, multi-writer coordination, deletion policy,
encryption policy and literal sudden-power-loss behavior remain outside the
current adapter contract.

## Build and test

```bash
cargo test --release --locked --lib
cargo build --release --locked --bin tulya-checkpoint
```

## CLI smoke test

```bash
printf '["root"]' | target/release/tulya-checkpoint \
  --db ./demo put --thread-id t --checkpoint-id root --checkpoint-no 0
printf '["child"]' | target/release/tulya-checkpoint \
  --db ./demo put --thread-id t --checkpoint-id child --checkpoint-no 1 \
  --parent-checkpoint-id root
target/release/tulya-checkpoint --db ./demo verify
target/release/tulya-checkpoint --db ./demo export > export.json
```

The benchmark harness, frozen datasets, comparator implementations, raw result
manifests and claim ledger remain in the source `tulya-engine` repository so
this repository stays product-focused.
