# Tulya

**Persistent history for large application state.**

Tulya is an embedded Rust storage engine for applications that:

- keep many historical versions of a large state;
- branch from old versions;
- change only a small part of the state at a time.

The simple idea is:

> A small edit to a huge project should not require rebuilding the whole project.

Tulya stores state in a persistent balanced tree. New versions share unchanged
content with older versions. A local edit creates new content only for the
changed region and the tree path needed to reach it.

```text
V0  large project
├── V1  small edit
├── V2  another small edit
└── V3  fork of V0
    └── V4  edit on that branch
```

Old retained versions stay readable. Branches are first-class versions, not
copies of the whole state.

## Why not just use SQLite?

SQLite is excellent for rows, tables, indexes, and queries.

Tulya is aimed at a different shape of problem: **large versioned state with
lots of branching and small local changes**.

Examples we care about include:

- CAD / engineering projects;
- simulation state;
- robotics application state;
- scientific software;
- AI-native authoring and agent state;
- any workload that repeatedly forks a large state and changes a small part.

If your application is naturally relational, use SQLite. Tulya is not trying to
be a SQL database.

## What works today

The generic `tulya-core` engine currently has:

- immutable historical versions;
- stable `HistoryId` and `VersionId`;
- local insert / delete / replace through persistent splice;
- zero-content fork: a new version can point at the exact same persistent root;
- edits from any retained historical version;
- crash-safe single-writer authority;
- sealed snapshots plus a bounded hot WAL suffix;
- bounded request replay / conflict receipts;
- one-way logical version expiration;
- quiescent GC and physical compaction;
- stable logical version IDs across physical relocation;
- exact historical reads and verification.

The repository also contains the older checkpoint/LangGraph adapter and demo.
That adapter is now a client of the generic core, not the identity of Tulya.

## What is still being built

The main active storage milestone is **incremental physical persistence**.

The in-memory persistent tree already performs local structural edits. The next
step is making the durable backend preserve that locality after reopen:

```text
1 GiB parent
   +
4 KiB edit
   ↓
read the affected tree path
write the new payload + path-copy nodes
do not read or rewrite the unchanged 1 GiB
```

Until that physical-I/O work and the benchmark campaign are complete, we do
**not** claim that a 4 KiB durable edit to a 1 GiB state has a specific disk-I/O
cost or performance advantage.

The public on-disk Format v1 is also **not frozen yet**.

## Status

Tulya is under active development.

Current scope:

- embedded / local;
- single writer;
- no distributed consensus;
- no multi-writer storage;
- no production stability promise yet.

Do not use it as the only copy of important production data.

## How the core is structured

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
- [Historical benchmark work](docs/BENCHMARKS.md)
- [LangGraph integration](integrations/langgraph/README.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)

## License

MIT.
