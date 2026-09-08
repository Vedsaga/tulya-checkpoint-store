# Tulya

**Persistent history for large application state.**

Tulya is an embedded Rust storage engine for applications that keep many
versions of a large state, branch from old versions, and usually change only a
small part at a time.

The idea is simple:

> A small edit to a huge project should not require rebuilding the whole project.

Tulya uses a persistent balanced tree, so versions share unchanged content.

```text
V0  large project
├── V1  small edit
├── V2  another small edit
└── V3  fork of V0
    └── V4  edit on that branch
```

## Measured on a public dataset

We have already benchmarked an earlier Tulya checkpoint workload on a frozen
public dataset:

**Dataset:** [nebius/SWE-rebench-openhands-trajectories](https://huggingface.co/datasets/nebius/SWE-rebench-openhands-trajectories/commit/35455389ab51bf5e2306bfd436ef72d0f98bf882)

```text
dataset revision:
35455389ab51bf5e2306bfd436ef72d0f98bf882

evaluation SHA-256:
a931c659530c083933b7da5fd886bcee0068c8c8df3ce57f6aea43fae18df12e

11,383 unique checkpoints
91 OpenHands attempts
8 software tasks
```

Each successive message was stored as one append-only checkpoint. Every tested
backend had to reconstruct every checkpoint exactly before and after reopen.

![Selected storage-comparable arms from the public reproduction.](docs/assets/benchmark-storage.svg)

On that workload, Tulya used **5.49 MB of marginal reopened allocated storage**.

Compared with that Tulya result:

```text
LangGraph SQLite DeltaChannel     12.38x more storage
custom SQLite delta stores       6.75–6.96x more
Packed Git                       3.58x more
```

Tulya also used about **1.86x the peak RSS of normalized SQLite** in that
reproduction, so this is not a claim that Tulya won every metric.

Full methodology, durability settings, backend definitions, environment, hashes,
and caveats are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

**Important:** that benchmark measures the older append/checkpoint workload. It
does not yet prove the stronger claim we are currently building toward: that a
small local edit to a huge durable state performs only local physical I/O. That
is the active E6 milestone, followed by the new branch-heavy benchmark campaign.

## What Tulya is for

Tulya is aimed at **large versioned state with lots of branching and small local
changes**.

Examples:

- CAD and engineering projects;
- simulation state;
- robotics application state;
- scientific software;
- AI-native authoring and agent state;
- any workload that repeatedly forks a large state and changes a small part.

Old retained versions remain readable. A branch is a new logical version, not a
copy of the whole state.

## Why not just use SQLite?

SQLite is excellent for rows, tables, indexes, and queries.

Tulya is not trying to replace SQL.

The interesting case for Tulya looks more like:

```text
400 MB project
40 KB edit
50,000 retained versions
frequent branches from old versions
```

If your state is naturally relational, use SQLite.

If your native object is a large evolving state and you want cheap historical
versions and branches, that is the problem Tulya is built around.

## What works today

The generic `tulya-core` engine currently has:

- immutable historical versions;
- stable `HistoryId` and `VersionId`;
- local insert / delete / replace through persistent splice;
- zero-content fork: a new version can reuse the exact same persistent root;
- edits from any retained historical version;
- crash-safe single-writer authority;
- sealed snapshots plus a bounded hot WAL suffix;
- bounded request replay / conflict receipts;
- one-way logical version expiration;
- quiescent GC and physical compaction;
- stable logical version IDs across physical relocation;
- exact historical reads and verification.

The older checkpoint/LangGraph code remains as an adapter and evaluation
surface. It is no longer the architecture of the core.

## What we are building now

The current milestone is **incremental physical persistence**.

The persistent tree already performs local structural edits. We are moving that
same locality into the durable backend so this:

```text
1 GiB parent
   +
4 KiB edit
```

can become:

```text
read the affected tree path
+
read bounded boundary leaves
+
write the changed payload
+
write path-copy nodes
+
write small authority metadata
```

instead of reading or rewriting the unchanged 1 GiB.

We are not claiming that physical result until the implementation and locality
tests are complete.

The public on-disk Format v1 is also not frozen yet.

## Status

Tulya is under active development.

Current scope:

- embedded / local;
- single writer;
- no distributed consensus;
- no multi-writer storage;
- no production stability promise yet.

Do not use it as the only copy of important production data.

## Architecture

```text
              adapters
           /     |      \
   checkpoint   RL    future
          \      |      /
             tulya-core
                 |
      PersistentHistoryStore
                 |
       persistent AVL sequence
                 |
     physical persistence
                 |
     WAL / snapshot / recovery
                 |
       expiration / GC
```

Core code deliberately contains no LangGraph or checkpoint vocabulary.

## Build and test

```bash
git clone https://github.com/Vedsaga/tulya-checkpoint-store.git
cd tulya-checkpoint-store

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --features local-server --locked -- -D warnings
cargo test --workspace --all-targets --features local-server --locked -- --test-threads=1
cargo test --workspace --locked --features fault-injection -- --test-threads=1
cargo package -p tulya-core --locked
```

## Formal work

Tulya also has a Lean reference/specification project covering persistent AVL
edits, branching, lifecycle, reclamation, crash-recovery models, and physical
persistence properties.

Lean is used as a specification and conformance reference.

**The Rust implementation is not claimed to be formally verified.**

## Project docs

- [Core engineering plan](docs/TULYA_CORE_ENGINEERING_PLAN.md)
- [Benchmark execution plan](docs/TULYA_BENCHMARK_EXECUTION_PLAN.md)
- [Production-readiness invariants](docs/PRODUCTION_READINESS.md)
- [Benchmark methodology and historical results](docs/BENCHMARKS.md)
- [LangGraph integration](integrations/langgraph/README.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)

## License

MIT.
