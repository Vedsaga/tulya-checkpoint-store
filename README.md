# Tulya Checkpoint Store

**An embedded Rust store for histories that grow by appending—and sometimes
fork from an exact earlier checkpoint.**

> **Status: alpha.** Tulya is embedded and single-writer. The local HTTP server
> is an unauthenticated evaluator, not a production service. Try Tulya beside
> your existing database; do not make it your only copy of production data yet.

## See it work locally

Start with the included branching history:

```bash
git clone https://github.com/Vedsaga/tulya-checkpoint-store.git
cd tulya-checkpoint-store
TULYA_DEMO=$(mktemp -d)
cargo run --release --locked --features local-server --bin tulya-local -- \
  --db "$TULYA_DEMO/store" \
  --import-jsonl examples/sample_history.jsonl
```

Then open [http://127.0.0.1:3210](http://127.0.0.1:3210).

The dashboard reads live facts from the store: checkpoints, threads, branch
points, tree depth, logical state bytes, allocated filesystem bytes, hot/sealed
state, request counters, errors, and the last append durability time. The same
process exposes JSON at `/api/stats` and Prometheus text at `/metrics`.

![Tulya shares an unchanged prefix between two exact branches.](docs/assets/branch-sharing.svg)

The picture is illustrative. The benchmark below uses a pinned public dataset.

## Try your own history

Tulya does not accept arbitrary snapshots and guess how they are related. You
provide the relationship explicitly as append-delta JSONL, one checkpoint per
line, with every parent before its children:

```json
{"thread_id":"case-42","checkpoint_id":"root","checkpoint_no":0,"parent_checkpoint_id":null,"messages":[{"role":"user","content":"Investigate the timeout"}]}
{"thread_id":"case-42","checkpoint_id":"retry-a","checkpoint_no":1,"parent_checkpoint_id":"root","messages":[{"role":"assistant","content":"Try a longer timeout"}]}
{"thread_id":"case-42","checkpoint_id":"retry-b","checkpoint_no":1,"parent_checkpoint_id":"root","messages":[{"role":"assistant","content":"Optimize the query"}]}
```

`messages` contains only values newly appended to the selected parent—not the
complete accumulated state. Save rows like these as `my-history.jsonl`, then
import the file into the running local server:

```bash
curl --fail --silent --show-error \
  -H 'content-type: application/x-ndjson' \
  --data-binary @my-history.jsonl \
  http://127.0.0.1:3210/api/import | jq
```

After that import, append another checkpoint directly from a backend:

```bash
curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  --data '{
    "thread_id":"case-42",
    "checkpoint_id":"validated",
    "checkpoint_no":2,
    "parent_checkpoint_id":"retry-b",
    "messages":[{"role":"tool","content":"Query now finishes in 120 ms"}]
  }' \
  http://127.0.0.1:3210/api/checkpoints | jq
```

Read the exact reconstructed checkpoint:

```bash
curl --fail --silent --show-error \
  -H 'content-type: application/json' \
  --data '{"thread_id":"case-42","checkpoint_id":"validated"}' \
  http://127.0.0.1:3210/api/read | jq .state
```

The importer is incrementally durable, not all-or-nothing: if line 100 is
invalid, the first 99 accepted checkpoints remain committed. Use a new empty
store when repeating an import. The complete API and evaluation procedure are
in [`docs/EVALUATING_TULYA.md`](docs/EVALUATING_TULYA.md).

## When does this representation fit?

Tulya is currently built and tested for this exact relationship:

```text
child state = exact parent history + one or more appended values
```

It is a plausible fit when histories are long, appended values are relatively
small, old checkpoints remain readable, and retries fork from exact parents.

It is not the current fit for unrelated snapshots, large in-place replacement,
similarity hidden inside compressed or encrypted blobs, arbitrary LangGraph
channel mutation, distributed writers, or network-database replacement. The
generic identity API can retain caller-supplied bytes, but the published
storage result does not apply to similarity hidden inside those bytes.

## Measured evidence

The public result uses only a frozen evaluation partition of
[`nebius/SWE-rebench-openhands-trajectories`](https://huggingface.co/datasets/nebius/SWE-rebench-openhands-trajectories/commit/35455389ab51bf5e2306bfd436ef72d0f98bf882).
It contains 11,383 unique checkpoints from 91 OpenHands attempts on eight
software tasks. Each successive message became one append-only checkpoint.

```text
source revision: 35455389ab51bf5e2306bfd436ef72d0f98bf882
evaluation SHA-256: a931c659530c083933b7da5fd886bcee0068c8c8df3ce57f6aea43fae18df12e
```

![Selected storage-comparable arms from the clean public-API reproduction.](docs/assets/benchmark-storage.svg)

The chart shows five selected storage-comparable arms from the complete
nine-arm clean public-API reproduction. Every arm reconstructed every
checkpoint exactly before and after reopen. On that workload, Tulya used 5.49
MB of marginal reopened allocated storage; direct LangGraph SQLite
DeltaChannel used 12.38x more and the tested custom SQLite delta stores used
6.75–6.96x more.

These numbers are not universal:

- Packed Git was the closest storage comparator at 3.58x Tulya, and its import
  did not fsync one transaction per checkpoint.
- Tulya used about 1.86x the peak RSS of normalized SQLite in the same clean
  reproduction.
- The reproduction ran on the same machine and is not the reserved independent
  holdout.
- The result does not establish customer savings. The local dashboard reports
  facts about your Tulya store; it does not invent a comparator or ROI number.

See [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for workload geometry, every
backend, durability settings, losses, environment, evidence hashes, and the
post-refactor SWE-agent/OpenHands regression reruns.

## Embed it in Rust

```rust
use serde_json::json;
use tulya_checkpoint_store::{CheckpointStore, CheckpointStoreConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = CheckpointStore::open(
        "./history",
        CheckpointStoreConfig::default(),
    )?;

    store.append_messages_checkpoint(
        "case-42",
        "root",
        0,
        None,
        &[json!({"role": "user", "content": "Investigate the timeout"})],
    )?;
    store.append_messages_checkpoint(
        "case-42",
        "retry-a",
        1,
        Some("root"),
        &[json!({"role": "assistant", "content": "Try a longer timeout"})],
    )?;

    let state = store.read_checkpoint("case-42", "retry-a")?;
    println!("{}", String::from_utf8(state)?);
    Ok(())
}
```

## What exists today?

- Exact root, child, and sibling reconstruction.
- Single-writer append with one `sync_data` barrier per acknowledged
  transaction.
- Immutable compressed generations, reclaim, and subtree pruning.
- Independent read-only `fsck`.
- A deterministic 32-case process-crash matrix on the tested Linux/filesystem
  stack.
- One public checkpoint format.
- A synchronous/asynchronous LangGraph shadow adapter.
- The loopback local API, strict JSONL importer, live dashboard, and metrics
  endpoint shown above.

The LangGraph adapter mirrors one append-only `messages` channel while the
existing saver remains authoritative. Tulya is not a drop-in LangGraph saver.
The process-crash tests do not establish sudden-power-loss,
drive-controller-cache, torn-sector, multi-writer, or distributed-consensus
safety.

## Verify the release

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --features local-server --locked -- -D warnings
cargo test --all-targets --features local-server --locked -- --test-threads=1
cargo test --locked --features fault-injection \
  --test crash_matrix -- --test-threads=1
cargo package --locked
```

The small correctness-only smoke benchmark needs no external dataset:

```bash
cargo build --release --locked --bin tulya-checkpoint
python3 benchmarks/release_smoke.py
```

## Documentation

- [Evaluate Tulya and use the local API](docs/EVALUATING_TULYA.md)
- [Exact benchmark and reproduction](docs/BENCHMARKS.md)
- [On-disk format](docs/FORMAT.md)
- [Durability, recovery, and crash testing](docs/RECOVERY.md)
- [LangGraph shadow integration](integrations/langgraph/README.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)

Tulya is MIT licensed.
