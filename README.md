# Tulya Checkpoint Store

> [!WARNING]
> **Alpha / design-partner release.** Tulya is pre-production, single-writer
> storage. Run it fail-open beside an authoritative backend while evaluating
> it. Process-crash recovery is tested; sudden-power-loss safety is not yet
> claimed.

**Keep every branch. Store the shared past once.**

Stateful workloads retry, fork, explore alternatives, and preserve history.
Full snapshots repeatedly store the same prefix; delta logs can make old
states costly or difficult to reconstruct. Tulya is an embedded Rust store
designed for both sides of that trade-off: structural sharing across branches,
exact historical reads, durable append, restart recovery, and independent
verification.

On a frozen public branch forest of **11,383 checkpoints**, Tulya used
**12.38x less marginal reopened storage** than direct LangGraph SQLite
DeltaChannel. Stronger custom SQLite delta baselines narrowed the advantage to
**6.75–6.96x**, and aggressively packed Git narrowed it to **3.59x**. Those are
reproducible results for one pinned workload, not a claim about every workload.

## Why Tulya

- **Branch without copying the past:** root, child, and sibling histories share
  structure while retaining stable checkpoint identities.
- **Read the state that was actually saved:** reconstruct exact historical
  bytes instead of returning a summary or approximate semantic memory.
- **Inspect failures independently:** a read-only `fsck`, golden format
  fixture, and deterministic crash matrix make correctness testable outside
  the writer.
- **Embed a focused storage primitive:** the Rust API is intentionally small;
  agent runtimes and LangGraph are the first integration wedge, not the limit
  of the model.
- **Learn one format:** the alpha exposes one **Tulya checkpoint format**, not
  a menu of v3/v4/v5 product variants. Its compatibility integer is an internal
  safety guard.

Tulya is a potential fit when many states share long prefixes: agent retries,
workflow forks, search or simulation trees, and local versioned state. It is
not a general database, distributed checkpoint service, or semantic-memory
layer.

## Five-minute start

```bash
cargo run --locked --example branching_history -- /tmp/tulya-example
cargo run --locked --bin tulya-checkpoint -- \
  --db /tmp/tulya-example fsck
```

The example creates a root and two sibling branches, reconstructs both, seals
the store, and runs the read-only integrity checker.

The core library API is intentionally small:

```rust
use serde_json::json;
use tulya_checkpoint_store::{CheckpointStore, CheckpointStoreConfig};

let mut store = CheckpointStore::open("./history", CheckpointStoreConfig::default())?;
store.append_messages_checkpoint("thread", "root", 0, None, &[json!("root")])?;
store.append_messages_checkpoint("thread", "left", 1, Some("root"), &[json!("left")])?;
store.append_messages_checkpoint("thread", "right", 1, Some("root"), &[json!("right")])?;
assert_ne!(
    store.read_checkpoint("thread", "left")?,
    store.read_checkpoint("thread", "right")?,
);
# Ok::<(), Box<dyn std::error::Error>>(())
```

Physical lifecycle and maintenance types are under
`tulya_checkpoint_store::admin`.

## What is implemented

- single-writer durable append through a preinitialized hot WAL;
- exact root, child, and sibling reconstruction;
- immutable compressed generations, route indexes, and WAL-prefix reclaim;
- eager and bounded lazy sealed-history readers;
- idempotent request keys and durable deletion tombstones;
- golden on-disk fixture and compatibility test;
- independent read-only `fsck`;
- deterministic 32-case process-crash recovery matrix;
- deterministic export/import for the append-only message integration schema;
- restart-safe sync/async LangGraph shadow integration;
- executable nine-arm public-corpus benchmark and frozen evidence with losses.

## Verification

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked -- --test-threads=1
cargo test --locked --features fault-injection \
  --test crash_matrix -- --test-threads=1
cargo package --locked
```

Build the release CLI before the LangGraph smoke test:

```bash
cargo build --release --locked --bin tulya-checkpoint
PYTHONPATH=integrations/langgraph python -m unittest -v \
  integrations/langgraph/test_shadow_smoke.py
```

## Evidence and boundaries

On the frozen public OpenHands evaluation, Tulya exactly retained 11,383
branch-DAG checkpoints and measured 12.38x less marginal reopened storage than
direct LangGraph SQLite DeltaChannel. Custom SQLite deltas narrowed the storage
gap to 6.75–6.96x, and packed Git narrowed it to 3.59x. Read the full
qualifications and strongest-baseline requirements in
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md). The complete corpus-freezing and
comparator runner is in
[`benchmarks/branch_forest/`](benchmarks/branch_forest/README.md); copied result
summaries are not the only evidence shipped in the crate.

Process-crash testing does not imply sudden-power-loss safety. The LangGraph
adapter is a verified shadow, not a replacement saver: the primary remains
authoritative for reads and pending writes. See
[`docs/RECOVERY.md`](docs/RECOVERY.md),
[`docs/FORMAT.md`](docs/FORMAT.md), and
[`integrations/langgraph/README.md`](integrations/langgraph/README.md).

For a low-risk workload-owner evaluation, use the predeclared measurement
contract in [`docs/DESIGN_PARTNER.md`](docs/DESIGN_PARTNER.md).
The implemented-versus-unproved boundary is recorded in
[`docs/RELEASE_AUDIT.md`](docs/RELEASE_AUDIT.md).
The sequencing decision—including why a fresh Lean-to-Rust rewrite is parked—is
in [`docs/ROADMAP.md`](docs/ROADMAP.md).
