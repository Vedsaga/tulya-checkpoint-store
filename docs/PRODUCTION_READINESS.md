# Production readiness plan

This document is the release contract for taking `tulya-checkpoint-store` from
its current research/alpha state to a production-quality embedded LangGraph
checkpointer.

It is intentionally stricter than "the tests pass" or "the benchmark looks
good." Storage software earns trust through explicit semantics, bounded work,
failure testing, compatibility, independent review, and reproducible evidence.

No checklist can guarantee that nobody will object to a storage engine. The
goal here is stronger and more useful: every reasonable correctness,
durability, compatibility, locality, packaging, and benchmark objection should
have a concrete test or an explicitly documented scope boundary.

Do not use the word **production-ready** for Tulya until the production gate at
the end of this document is satisfied for a named platform/filesystem scope.

---

## 1. Current baseline

Baseline audited while writing this plan:

- repository head: `8cc5928569016a1baddc991beb91a4f830d0db9a`;
- public format: `tulya-checkpoint-store`, `format_version = 1`;
- embedded Rust, single writer;
- hot WAL + immutable sealed generations + manifest authority;
- process-locking, reopen/recovery, idempotent request identities;
- exact historical reconstruction and sibling branches;
- range-read API and lazy sealed reader;
- read-only `fsck`, verification, reclaim, and subtree pruning;
- deterministic 32-case process-crash matrix on the tested Linux/filesystem
  stack;
- benchmark harness with exact reconstruction checks and multiple comparators;
- LangGraph **shadow** adapter only; the primary saver remains authoritative;
- the shadow adapter shells out to a CLI process for Tulya operations;
- current LangGraph integration is pinned to `langgraph==1.2.10` and Python
  3.12 in CI.

Known architectural gaps at this baseline:

1. `append_messages_checkpoint` reconstructs the complete parent canonical
   state to derive its length/hash. A tiny append to a very large parent can
   therefore perform work proportional to the complete parent.
2. Repeated message appends build a left-deep binary history rather than a
   balanced persistent sequence. Range APIs exist, but a logarithmic/local
   complexity guarantee is not established.
3. Tulya is not a primary `BaseCheckpointSaver`: checkpoint reads, pending
   writes, list/delete semantics, arbitrary channels, DeltaChannel behavior,
   and other LangGraph operations are still delegated to another saver.
4. There is no PyO3/maturin Python package or wheel; the integration launches a
   subprocess per mirrored checkpoint.
5. Official LangGraph checkpoint conformance is not in CI.
6. Live I/O error injection (`ENOSPC`, short write, `EIO`, failed sync, failed
   rename/directory sync) is not yet a complete production matrix.
7. Fuzzing, supported-platform durability evidence, backup/restore, migration
   testing for a future format, and independent review are not complete.
8. At the time this plan was written, the latest GitHub Actions run on `main`
   was red and its jobs failed before executing workflow steps. Restore a green
   required CI baseline before interpreting later results.

Upstream LangGraph snapshot checked on 2026-09-02:

- `langgraph` source version: `1.2.11`;
- `langgraph-checkpoint` source version: `4.2.0`;
- `langgraph-checkpoint-conformance` source version: `0.0.2`;
- LangGraph/checkpoint packages require Python `>=3.10`; the LangGraph source
  advertises Python 3.10, 3.11, 3.12, and 3.13.

These upstream versions are a snapshot, not a permanent compatibility promise.
Re-check upstream before each release.

---

## 2. Production scope we are trying to earn

The first production claim should be deliberately narrow:

> **Tulya is a production-quality embedded LangGraph checkpoint saver for a
> single host and a single Tulya writer process, on explicitly tested local
> filesystems, with concurrent LangGraph tasks/readers serialized or served
> safely inside that process.**

This claim does **not** imply:

- distributed consensus;
- multiple independent writer processes sharing one store;
- network-filesystem safety;
- remote database service semantics;
- Byzantine/tamper-proof storage;
- controller/firmware behavior beyond the documented filesystem/device flush
  contract;
- a Rust implementation mechanically proved equivalent to the Lean model;
- universal performance superiority over SQLite/Postgres/other checkpointers.

A precise supported scope is stronger than pretending Tulya solves deployment
modes it has not tested.

---

## 3. Non-negotiable invariants

Every production design and test should reduce to these invariants.

### 3.1 Durable acknowledgement

```text
put/put_writes returns success
            =>
required durable bytes are recoverable after immediate process death/reopen
```

No successful acknowledgement may depend only on process memory.

### 3.2 Atomic publication

After an interrupted write/recovery, client-visible state is either the exact
previous committed state or the exact new committed state. Never expose a
mixture.

### 3.3 Safe retry

A retry of the same logical request after timeout/crash/unknown acknowledgement
must not create a duplicate logical checkpoint or duplicate pending write.
Reusing the same identity for different bytes must fail explicitly.

### 3.4 Historical immutability

Publishing, deleting, compacting, or branching must not silently change the
bytes returned for another retained checkpoint.

### 3.5 Local-change locality

For parent logical size `n` and changed/new payload `delta`, a local append must
not require work proportional to all unchanged parent bytes.

The production target is approximately:

```text
append CPU / store reads / incremental RAM = O(log n + delta)
foreground physical writes               = O(log n + delta)
range read of k bytes                     = O(log n + k)
fork from retained root                   = O(1) metadata or equivalent
```

Exact constants are implementation-dependent; linear parent reconstruction is
not acceptable.

### 3.6 Correctness before optimization

A LangGraph channel that does not match Tulya's append optimization must still
round-trip exactly through an opaque typed-value representation. Optimization
may fail; checkpoint correctness must not.

### 3.7 Fail closed on incompatible/corrupt bytes

Unknown format versions/features, invalid lengths/offsets, checksum failures,
integer overflow, impossible parent topology, and malformed records must never
be guessed or silently clamped.

### 3.8 Deletion is logical first, reclamation later

Once LangGraph `delete_thread` succeeds, the deleted checkpoints/writes are no
longer visible. Physical reclamation may happen later. Stale operations that
refer to deleted checkpoint IDs must not resurrect them accidentally.

A deliberate new root checkpoint for a reused `thread_id` may be allowed if the
LangGraph contract requires it; distinguish this from a stale operation that
references a deleted checkpoint.

---

## 4. Release levels

### Level 0 — research alpha (current class)

Useful for evaluation and shadow deployment. No production recommendation.

### Level 1 — LangGraph community alpha

Safe to publish as an experimental primary saver and ask the community to try
it. Requires full **base** LangGraph conformance, direct Python/Rust binding,
correct pending writes, arbitrary-channel correctness, restart testing, and a
green CI matrix.

### Level 2 — production candidate

All community-alpha gates plus locality redesign, large/deep-history tests,
I/O fault injection, fuzzing, migration/format discipline, backup/restore,
platform-specific durability testing, and current strong comparator benchmarks.

### Level 3 — production-ready for the documented scope

All production-candidate gates plus independent review/reproduction, release
supply-chain checks, operational docs, and at least one external real-world
pilot whose data can be independently verified/restored.

Do not silently weaken a lower-level gate to reach a higher label.

---

# PART A — STORAGE ENGINE WORK

## 5. Gate zero: restore a trustworthy repository baseline

Before architectural work:

- [ ] Latest `main` GitHub Actions run is green.
- [ ] Determine why the current jobs fail before steps and fix the workflow or
      repository runner/billing/configuration issue.
- [ ] Require the Rust, LangGraph, packaged-artifact, and later conformance/wheel
      jobs on protected `main`.
- [ ] No release from a dirty tree or a commit whose required checks are absent.
- [ ] Keep Rust MSRV explicit and test it separately from current stable Rust.
- [ ] Add dependency/security checks (`cargo audit` and/or `cargo deny`) and a
      Python dependency audit once the wheel package exists.
- [ ] If the crate remains free of local `unsafe`, add `#![forbid(unsafe_code)]`
      or document the exact reason this cannot be done.
- [ ] Do not describe the Rust implementation itself as formally verified
      unless an actual Rust/Lean refinement exists.

Current verification commands remain required:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --features local-server --locked -- -D warnings
cargo test --all-targets --features local-server --locked -- --test-threads=1
cargo test --locked --features fault-injection \
  --test crash_matrix -- --test-threads=1
cargo package --locked
```

Add the later gates in this document to CI rather than replacing these.

---

## 6. Remove whole-parent work from append

### Current problem

The current message append path reconstructs the selected parent canonical JSON
in order to derive the complete resulting state's length/hash, even though the
physical transaction writes only the new message body plus new tree nodes.

For a 1 GiB parent + 1 KiB append this can still imply roughly 1 GiB of read,
hash, and temporary allocation work.

### Required design

A checkpoint record/root must contain enough metadata to construct a child from
its parent root without materializing the complete parent value.

At minimum the parent-access path should expose:

```text
PersistentRoot
├── logical byte length / subtree lengths
├── child/node location
├── schema/representation version
└── structural/content commitment as required by the format
```

Then append becomes:

```text
lookup parent metadata/root
        -> serialize only new values
        -> persistent_sequence.append(parent_root, delta)
        -> create new checkpoint metadata/root
        -> durable commit
```

Do not fake an "incremental xxh3 of concatenation" if the hash primitive does
not mathematically support it. Choose a sound design:

- a composable tree/Merkle commitment;
- a foreground structural commitment plus optional full-byte verification hash
  computed outside the latency-critical append path; or
- another explicitly specified construction.

If this changes Format v1 semantics, introduce Format v2; do not reinterpret
existing v1 bytes.

### Acceptance test

Add an instrumented benchmark, for example
`benchmarks/locality/append_scaling.py`, that builds parents of approximately:

```text
10 MiB
100 MiB
1 GiB
10 GiB (where CI/hardware permits; nightly/manual is acceptable)
```

and appends the same 1 KiB logical delta.

Record at minimum:

- store bytes read during the append;
- store bytes written;
- nodes/blocks touched;
- wall-clock p50/p95/p99;
- CPU time;
- peak incremental RSS;
- fsync/durability time separately.

For fixed `delta`, the read/write/RSS counters must demonstrate non-linear-in-
parent-size behavior. A 10x or 100x larger unchanged parent must not cause a
corresponding 10x or 100x increase in bytes read or temporary RAM.

Keep machine-readable results in `benchmarks/evidence/`.

---

## 7. Replace the left-deep append history with a balanced persistent sequence

### Current problem

Repeated append currently produces a shape equivalent to:

```text
root_N
├── root_(N-1)
└── new_delta_N
```

Repeated indefinitely, this can create depth proportional to history length.
The range API is real, but this shape can make first-touch size computation and
navigation increasingly expensive.

### Required internal abstraction

Introduce a narrow internal interface before changing callers:

```text
PersistentSequence
├── append(parent_root, bytes) -> new_root
├── read_range(root, offset, length)
├── logical_len(root)
├── iterate/stream_range(...)
└── verify(root)
```

The implementation may be a persistent AVL/rope/B-tree/chunk tree or another
balanced structure. The product requirement is the bound, not the data
structure's name.

Store subtree byte lengths directly in internal nodes so a range read does not
have to recursively discover lengths through a long history before navigating.

### Required properties

- [ ] Source root remains exactly readable after child append.
- [ ] Sibling branches share unchanged nodes.
- [ ] Tree depth remains logarithmic/bounded by a stated invariant.
- [ ] `read_range` navigates by subtree lengths without reconstructing full
      checkpoints.
- [ ] Traversal cannot stack-overflow on a valid production-size history;
      prefer iterative traversal where practical.
- [ ] Sealing/reopen preserves the same roots/bytes.
- [ ] Compaction/reclamation preserves all retained roots.

### Deep-history acceptance matrix

Run at least:

```text
1,000 checkpoints
10,000 checkpoints
100,000 checkpoints
1,000,000 checkpoints (nightly/manual if necessary)
```

Measure append, random historical range read, latest read, open/reopen, RSS,
index bytes, and maximum tree depth.

A million-checkpoint test is not required on every PR, but a smaller structural
test must enforce the balancing invariant on every PR.

---

## 8. Random/range-read production gate

The current repo already has `read_checkpoint_range`; production readiness
requires proving its locality empirically and structurally.

For checkpoints of 10 MiB, 100 MiB, 1 GiB, and larger where available, read a
4 KiB range at:

```text
start
25%
50%
75%
end
```

Repeat for:

- latest checkpoint;
- old retained checkpoint;
- sibling branch;
- hot WAL state;
- fully sealed state;
- lazy reopen.

Record blocks/nodes/physical bytes read in addition to latency. The amount of
unrelated data touched must not grow linearly with total logical state size.

Also test:

- zero-length reads at exact end;
- out-of-range/overflow requests fail explicitly;
- ranges crossing leaf/chunk boundaries;
- ranges crossing hot/sealed boundaries if the representation permits this;
- streaming large output without allocating the full checkpoint.

---

## 9. Open/recovery scaling

A large logical history must not be reconstructed merely to open the store.

Required tests:

- [ ] lazy open never materializes all payload/node streams;
- [ ] hot suffix is replayed correctly on top of a lazy sealed base;
- [ ] open RSS is reported as metadata/index cost, not hidden logical-state
      materialization;
- [ ] 100k/1m checkpoint open-time and RSS are measured;
- [ ] startup after an interrupted seal/WAL recycle remains bounded and exact;
- [ ] opening corrupted authoritative metadata fails closed;
- [ ] uncommitted/torn suffix bytes never become authoritative.

If metadata itself becomes too large, add a lazy/mmap/packed metadata index
rather than accepting unbounded Python/Rust heap growth.

---

## 10. Format compatibility and migration

Format v1 already has a compatibility promise. Preserve it.

- [ ] Never change the meaning/layout of released v1 bytes silently.
- [ ] Keep `tests/fixtures/format-v1` immutable.
- [ ] If balanced nodes/new commitments require incompatible bytes, create
      public Format v2.
- [ ] Unknown major format/version must fail closed with an actionable error.
- [ ] Add a committed golden v2 fixture.
- [ ] Implement `v1 -> v2` migration if v2 becomes the writable format.
- [ ] Migration must be crash safe: after a crash, either the old valid store or
      the fully migrated valid store remains authoritative.
- [ ] Verify every checkpoint before and after migration byte-for-byte through
      public reads.
- [ ] Test migration with branches, pending LangGraph writes, deleted records,
      hot suffix, fully sealed store, and large histories.
- [ ] Document downgrade behavior. Never let an older writer reinterpret newer
      format bytes.

A production release must include a format-compatibility matrix in its release
evidence.

---

## 11. Live I/O failure matrix

Process `SIGKILL` testing is necessary but not sufficient. Add an injectable I/O
backend around every state-changing filesystem primitive so tests can return
realistic live failures while the process remains alive.

Inject at least:

```text
write: complete
write: short count
write: ENOSPC
write: EIO
sync_data/fsync: error
rename: error
directory sync: error
open/create/truncate: error
cancellation/early return where applicable
```

Exercise these during:

- hot checkpoint append;
- pending-write append;
- seal segment publication;
- route publication;
- manifest publication;
- WAL recycle;
- logical deletion/tombstone publication;
- compaction/reclaim;
- migration;
- backup creation.

Required results:

```text
Rejected
    => no newly visible checkpoint/write; old committed state exact

Committed
    => exact new state survives immediate reopen

Indeterminate (only where the OS result cannot tell whether durability happened)
    => retrying the same logical request resolves to exactly zero or one commit
```

Expose an actionable Python exception for an indeterminate outcome rather than
pretending failure means "definitely not committed." The checkpoint ID/request
digest must make safe retry possible.

Test "same ID, different payload" as an explicit conflict.

---

## 12. Disk-full and maintenance behavior

Production stores frequently fail in maintenance, not foreground append.

Test near-full filesystems for:

- append;
- seal;
- manifest update;
- WAL replacement;
- delete/prune;
- compaction;
- migration;
- backup.

Maintenance must not destroy old authority before replacement authority is
durable.

Where maintenance needs temporary coexistence space, report/estimate it. Prefer
a preflight check and an explicit "insufficient free space for compaction"
error instead of discovering ENOSPC after destructive steps.

After any failed maintenance operation:

- old retained checkpoints remain readable;
- no deleted checkpoint is resurrected incorrectly;
- reopen/fsck gives a deterministic answer;
- retry is safe.

---

## 13. Corruption and parser hardening

For every persisted record family, create deterministic corruption tests:

- bit flip;
- truncation at every meaningful boundary;
- oversized length;
- integer overflow;
- wrong checksum;
- invalid UTF-8 identifier where UTF-8 is required;
- invalid parent/version/root reference;
- stale/duplicate record;
- invalid zstd block;
- path traversal/absolute path in manifest file names;
- unexpected extra bytes.

`open` and `fsck` must fail safely without panic, UB, or silently returning
incorrect checkpoint bytes.

Add fuzzing:

```text
cargo-fuzz targets:
  manifest parser
  hot-WAL transaction parser
  sealed segment/index parser
  checkpoint/root/node parser
  migration parser
```

Add property/state-machine tests generating random valid histories containing:

```text
append
branch from old checkpoint
range read
seal
reopen
kill/recover
logical delete
gc/compact
```

and compare the public result to a simple in-memory reference model.

Record longer fuzz campaigns before a production release, not only a one-minute
CI smoke run.

---

## 14. Backup and restore

Production readiness requires a supported backup procedure, not "copy the
folder and hope."

Implement and document at least an offline/stable-snapshot backup first.
Preferably add a library/CLI operation that captures a defined authority point.

Acceptance test:

```text
create many branches/checkpoints + pending writes
        -> take supported backup
        -> destroy original directory
        -> restore elsewhere
        -> fsck
        -> compare every checkpoint + metadata + pending write
        -> continue writing successfully
```

Also test:

- backup after crash/reopen;
- backup of mixed sealed + hot state;
- backup during/around maintenance according to the supported contract;
- corrupt/incomplete backup rejection;
- store identity behavior when a backup is restored as the same store vs copied
  as an explicitly independent store.

Document what an operator must copy atomically if no online backup API is
provided.

---

## 15. Concurrency and process model

The first production release may remain single writer, but it must be safe and
clear.

Inside one Python process:

- [ ] concurrent sync calls from threads are serialized safely;
- [ ] async tasks do not block the event loop for long filesystem work;
- [ ] reads during writes observe a committed snapshot, never private state;
- [ ] no data races or mutable aliasing cross the FFI boundary;
- [ ] cancellation cannot publish a half-commit;
- [ ] all locks have a documented ordering to avoid deadlock.

Across processes:

- [ ] second writer receives a deterministic `StoreBusy`/writer-lock error;
- [ ] supported read-only tools/readers coexist safely with the writer according
      to the reclaim-generation protocol;
- [ ] do not support NFS/network filesystems until explicitly tested;
- [ ] document fork behavior; do not allow a Python process to fork and keep
      using inherited writable handles unless that path is explicitly made
      safe.

If later using custom lock-free atomics, add Loom/model concurrency testing.
Do not add complexity solely to look sophisticated; a well-audited mutex is
preferable to an unreviewed lock-free protocol.

---

# PART B — LANGGRAPH PRODUCT WORK

## 16. Target Python package

The community-facing interface should look like normal LangGraph:

```bash
pip install langgraph-checkpoint-tulya
```

```python
from langgraph_checkpoint_tulya import TulyaSaver

with TulyaSaver("./agent-state") as saver:
    graph = builder.compile(checkpointer=saver)
```

Async usage must be supported as well.

Implementation requirements:

- [ ] PyO3 + maturin (or another direct in-process binding with equivalent
      properties);
- [ ] no CLI subprocess per checkpoint;
- [ ] no shell parsing in the data path;
- [ ] blocking Rust filesystem work releases the GIL;
- [ ] async methods use a safe worker/thread integration or native async bridge
      so they do not block the event loop;
- [ ] installed-wheel tests execute the wheel, not the source tree;
- [ ] no Rust toolchain is required for normal users of published wheels.

Initial package CI target:

```text
Python 3.10
Python 3.11
Python 3.12
Python 3.13
```

Start with Linux x86_64 as the narrowest support claim if necessary, then add
Linux aarch64/macOS arm64/x86_64 only after their full tests pass. Do not imply
Windows support until Windows wheels and filesystem tests exist.

---

## 17. LangGraph storage model

Do not force complete LangGraph state into the current `messages` schema.
Persist LangGraph's actual model.

### Checkpoint identity

Use a structured key, not ambiguous string concatenation:

```text
(thread_id, checkpoint_ns, checkpoint_id)
```

### Checkpoint record

Persist enough information to reconstruct the exact upstream `CheckpointTuple`:

```text
Checkpoint
├── v
├── id
├── ts
├── channel_versions
├── versions_seen
├── updated_channels
└── channel value references

CheckpointMetadata
├── source
├── step
├── parents
├── run_id
├── counters_since_delta_snapshot
└── arbitrary/custom metadata keys

Tuple relation
├── config
├── parent_config
└── pending writes
```

Do not drop unknown/custom metadata keys merely because Tulya does not
understand them.

### Channel-value storage

Use LangGraph's serializer boundary:

```text
Python value
 -> saver.serde.dumps_typed(value)
 -> (type tag, opaque bytes)
 -> Tulya
```

Read through `loads_typed`.

Rust should not need to understand `AIMessage`, `ToolMessage`, Pydantic,
datetime, custom LangChain objects, or encrypted payloads.

Store changed channels using `new_versions`; reuse unchanged channel-version
references rather than rewriting every value.

A useful key is conceptually:

```text
(thread_id, checkpoint_ns, channel_name, channel_version)
```

unless a content-addressed scheme is proved equivalent.

Required behaviors:

- channel added later works;
- unchanged channel is recovered from its old version;
- updated channel returns the new value while old checkpoint returns the old
  value;
- channel removed from `channel_versions` is not resurrected from historical
  storage;
- arbitrary non-append channel values use exact opaque replacement;
- an append-optimized channel may use the persistent sequence only when the
  optimization's precondition is actually satisfied.

---

## 18. Pending writes (`put_writes`) are mandatory

Pending writes are part of LangGraph durable execution, not optional metrics.
Implement a separate durable representation keyed conceptually by:

```text
(thread_id,
 checkpoint_ns,
 checkpoint_id,
 task_id,
 write_index)
```

Store at least:

```text
task_path
channel
typed serializer tag
opaque serialized bytes
```

Required semantics:

- multiple writes from one task;
- multiple tasks for one checkpoint;
- task ID preserved;
- channel/value exact round-trip;
- task path accepted/preserved as required internally;
- special LangGraph error/interrupt channels behave correctly;
- duplicate `(task_id, write_index)` is idempotent;
- retry after **close/reopen** is also idempotent (extra Tulya gate beyond a
  same-process test);
- pending writes are isolated by namespace;
- a newly created checkpoint starts with its own fresh pending-write set;
- deleting a thread removes/invisibilizes its writes too.

Do not publish a primary TulyaSaver before this is complete.

---

## 19. Implement the five required LangGraph base capabilities

Current `langgraph-checkpoint-conformance` requires these async surfaces:

| Capability | Required method |
| --- | --- |
| put | `aput` |
| put_writes | `aput_writes` |
| get_tuple | `aget_tuple` |
| list | `alist` |
| delete_thread | `adelete_thread` |

Implement the sync counterparts too for normal `BaseCheckpointSaver` usage.

### `put` / `aput`

Must preserve:

- returned config contains `thread_id`, `checkpoint_ns`, `checkpoint_id`;
- exact checkpoint ID and channel values;
- channel versions;
- `versions_seen` keys **and values**;
- metadata including custom keys/run ID;
- root and child namespaces;
- multiple checkpoints per thread;
- multiple threads without interference;
- parent config;
- incremental channel update/reuse;
- newly added channel;
- removed channel semantics;
- `updated_channels` and any upstream fields added to the current checkpoint
  schema.

### `get_tuple` / `aget_tuple`

Must support:

- missing thread/checkpoint returns `None`;
- no checkpoint ID means newest checkpoint in that thread+namespace;
- explicit checkpoint ID returns that exact historical checkpoint;
- exact config structure;
- all current `Checkpoint` fields;
- metadata;
- exact `parent_config`;
- pending writes;
- namespace isolation.

### `list` / `alist`

Must support:

- all checkpoints for a thread+namespace;
- thread isolation;
- namespace isolation;
- newest-first ordering;
- metadata filtering, including multiple and custom keys;
- `before` pagination cursor;
- `limit`;
- `before + limit`;
- empty results;
- listed tuples include pending writes.

Do not implement list by reconstructing every large checkpoint payload just to
filter metadata.

### `delete_thread` / `adelete_thread`

Must:

- make all checkpoints unavailable;
- remove/invisibilize pending writes;
- cover all namespaces under the thread;
- preserve all other threads;
- treat deletion of a missing thread as a no-op if upstream requires it;
- reject stale writes against deleted checkpoint IDs;
- remain exact after close/reopen;
- avoid synchronous whole-store rewrite if logical tombstone + later GC can
  satisfy the same semantics more safely.

---

## 20. Official LangGraph conformance is a hard gate

Upstream source:

`langchain-ai/langgraph/libs/checkpoint-conformance`

The suite explicitly says it validates custom `BaseCheckpointSaver`
implementations and that base capabilities are required. Use it directly; do
not create a Tulya-specific definition of compatibility.

Add a test such as:

```python
import pytest
from langgraph.checkpoint.conformance import checkpointer_test, validate


@checkpointer_test(name="TulyaSaver")
async def tulya_checkpointer():
    # create isolated temporary store
    # yield a fresh async-capable TulyaSaver
    # close and clean up after yield
    yield saver


@pytest.mark.asyncio
async def test_langgraph_checkpoint_conformance():
    report = await validate(tulya_checkpointer)
    report.print_report()
    assert report.passed_all_base()
    assert report.passed_all()  # all extended capabilities we advertise/override
```

The second assertion is important: if Tulya overrides an optional capability,
its official tests must pass. Do not override an optional method just to return
`NotImplementedError`, because capability detection may treat the override as
support.

CI should save `report.to_dict()` as an artifact so each release has a
machine-readable conformance record.

### Upstream base conformance currently exercises

At the snapshot checked for this document, official tests include:

- checkpoint/channel round-trip;
- channel versions and `versions_seen`;
- metadata/custom metadata preservation;
- namespaces;
- parent config;
- incremental channel updates, new channels, removed channels;
- latest vs exact checkpoint lookup;
- pending writes and write idempotency;
- special ERROR/INTERRUPT writes;
- list ordering/filtering/pagination/limit;
- thread deletion including writes and namespaces.

Re-read upstream tests before every release; this list is informative, not a
forked specification.

---

## 21. Go beyond official conformance

Passing official conformance is necessary, not sufficient for Tulya production
claims. Add these explicit tests even if upstream does not yet enforce them.

### Restart idempotency

```text
put_writes / put
 -> durable commit
 -> close or kill process before caller confidently records success
 -> reopen
 -> repeat identical request
 -> exactly one logical result
```

### Conflicting retry

Same checkpoint/request identity with different serialized bytes must fail.

### Crash through the Python API

Run a real LangGraph/Tulya process, inject a crash at every Rust durability
boundary, restart Python, and verify the graph/checkpoint tuple—not merely an
internal Rust struct.

### Acknowledgement test

```text
TulyaSaver.aput returns
 -> immediately SIGKILL process
 -> reopen
 -> exact checkpoint is present
```

Repeat for `aput_writes`.

### Namespace + subgraph stress

Use real nested/subgraph namespaces and concurrently active threads.

### Serializer compatibility

Test at minimum:

- primitive scalar/list/dict/bytes-like supported values;
- LangChain message objects used by real graphs;
- custom values supported by LangGraph's configured serializer;
- strict msgpack allowlisting via `with_allowlist`/current upstream mechanism;
- `EncryptedSerializer` or current upstream encryption path: Tulya stores opaque
  ciphertext/type bytes and round-trips them without bypassing serializer
  security.

### Delete/recreate race

Test stale operations racing with `delete_thread`. A stale child of a deleted
checkpoint must not resurrect deleted history. If a completely new root for the
same `thread_id` is valid upstream behavior, test that separately.

### Long metadata

Nested custom metadata and `counters_since_delta_snapshot` must survive reopen
and filtering where applicable.

---

## 22. Real LangGraph graph-level matrix

Unit conformance alone is not enough. Run actual `StateGraph` workloads.

Required before community alpha:

- [ ] sync `invoke`;
- [ ] async `ainvoke`;
- [ ] streaming path used by a normal graph;
- [ ] multiple successive invocations on one thread;
- [ ] multiple threads;
- [ ] historical checkpoint lookup/time travel;
- [ ] fork/update from an old checkpoint;
- [ ] sibling branches survive restart;
- [ ] interrupt + resume;
- [ ] node failure after another node wrote successfully; pending writes allow
      resume without duplicating completed work;
- [ ] subgraph/checkpoint namespaces;
- [ ] close/reopen and continue;
- [ ] immediate kill after acknowledged checkpoint then resume graph;
- [ ] `delete_thread` followed by reopen;
- [ ] arbitrary non-message channels.

Required before a full-featured production claim:

- [ ] DeltaChannel graph behavior;
- [ ] long DeltaChannel ancestor chain bounded by upstream snapshot semantics;
- [ ] copy/prune/delete-for-runs if Tulya advertises those capabilities;
- [ ] concurrency stress with many async graph tasks sharing one saver instance.

---

## 23. Extended LangGraph capabilities

Current upstream treats these as optional and auto-detected:

```text
delete_for_runs
copy_thread
prune
delta_channel_history
```

Rules:

1. Community alpha may omit them, but documentation must say so.
2. If a method is overridden/advertised, `report.passed_all()` must remain true.
3. The extra-mile target for a mature TulyaSaver is FULL conformance for every
   capability that remains part of the upstream contract.
4. Re-check beta surfaces before each release; do not freeze today's
   DeltaChannel internals as permanent Tulya format semantics.

### DeltaChannel caution

Upstream explicitly warns that naïve pruning/copying can break DeltaChannel
reconstruction because state may depend on ancestor `checkpoint_writes` up to a
nearest snapshot seed.

Therefore:

- do not implement `prune(keep_latest)` by simply deleting intermediate
  ancestors;
- either preserve the required ancestor/write chain, synthesize a safe fresh
  snapshot using upstream semantics, or leave prune unsupported;
- `copy_thread` must copy enough ancestry/writes to reconstruct every delta
  channel;
- `delete_for_runs` must not silently remove history still required by a live
  thread.

Correctly unsupported is better than incorrectly "supported."

---

## 24. LangGraph compatibility/version matrix

Stop pinning integration confidence to a single exact LangGraph version.

For each Tulya release, define:

```text
minimum supported langgraph-checkpoint version
minimum supported langgraph version (if the integration imports it directly)
latest tested stable version
Python versions
```

CI strategy:

1. **minimum matrix** — oldest version Tulya promises;
2. **locked current matrix** — versions used for release evidence;
3. **latest-canary** — scheduled CI against latest compatible LangGraph and
   conformance package; failure warns maintainers but does not silently change a
   released support promise.

At the time of this document the upstream repo reports LangGraph `1.2.11`,
`langgraph-checkpoint` `4.2.0`, and Python 3.10-3.13. Re-check rather than copying
these numbers forever.

---

# PART C — PRODUCT HARDENING

## 25. Performance and Pareto-frontier evidence

Production quality and product differentiation are separate gates. Tulya may be
correct yet not worth adopting.

After the locality refactor, rerun all existing evidence and add larger-state
workloads.

Always compare against current strong baselines, not only naïve full snapshots:

- current LangGraph SQLite saver;
- current LangGraph SQLite/DeltaChannel behavior;
- current PostgresSaver where deployment semantics are comparable;
- normalized/content-addressed delta baseline;
- packed Git where relevant;
- any new mainstream embedded saver that becomes a realistic alternative.

For every arm report:

```text
exact semantics preserved?
durability policy
physical allocated bytes
file/logical bytes
WAL/temp coexistence bytes
append p50/p95/p99
historical full-read p50/p95/p99
4 KiB range-read p50/p95/p99
open/reopen time
peak RSS
CPU
maintenance/prune/compaction time
```

Rules:

- same durability semantics or label the difference visibly;
- same retained history and deletion semantics;
- compare complete store cost, not a relation/file subset against another
  backend's whole directory;
- verify every checkpoint before and after reopen;
- publish losses;
- do not tune against the reserved holdout after seeing it;
- add at least one independent second real workload before broad claims;
- do not preserve a storage-ratio headline if the new correct LangGraph schema
  changes that result—rerun and report the new truth.

The key large-state benchmark is:

```text
1 GiB logical parent + 1 KiB append
```

and its scaling curve. Storage savings alone are insufficient if append CPU/RAM
still scale with the complete parent.

---

## 26. Retention, delete, GC, and compaction

LangGraph-visible deletion should be cheap and atomic. Physical reclaim may be
asynchronous/explicit.

Required tests:

- delete one thread with branches and pending writes;
- sibling/other thread exact preservation;
- delete after reopen;
- crash before/after deletion authority publication;
- stale request after delete;
- repeated delete is safe;
- reclaim later actually reduces allocated storage;
- concurrent read lease prevents early file reclamation;
- crash during compaction returns old or new valid authority;
- ENOSPC during compaction leaves old state valid;
- no request/checkpoint identity can be reused ambiguously after physical
  reclaim.

If tombstone growth is unbounded, define a safe retirement/checkpointing policy
before production.

---

## 27. Security/threat model

Document a concrete threat model.

Initial production scope may say:

- store directory is trusted local storage;
- Tulya detects accidental corruption but is not an adversarial tamper-proof
  database;
- no network listener is part of the production saver;
- local evaluator HTTP remains development-only;
- payload confidentiality comes from the configured LangGraph serializer or
  filesystem/disk encryption; Tulya metadata may remain visible;
- malformed on-disk bytes are untrusted parser input and must never cause UB.

Add:

- `cargo audit`/`cargo deny` policy;
- Python dependency audit;
- no secrets/payload contents in logs/metrics by default;
- limits on IDs, record sizes, decompression output, metadata nesting where
  Tulya itself parses it;
- symlink/path-escape review for store-owned files;
- security advisory process tested/documented;
- release SBOM/attestation if practical.

---

## 28. Operational observability

Expose machine-readable counters suitable for diagnosing storage behavior
without logging checkpoint payloads:

- committed checkpoints/writes;
- failed/rejected/indeterminate operations;
- durable sync latency;
- bytes read/written per operation;
- nodes/blocks touched;
- WAL used/reserved bytes;
- sealed generation bytes;
- logical vs allocated bytes;
- open/recovery time;
- lazy cache hit/miss/bytes-read;
- range-read amplification;
- compaction/prune reclaimed and temporary coexistence bytes;
- fsck last result where an operator records it.

The embedded library should expose stats directly; a production user must not
need the unauthenticated HTTP evaluator.

---

## 29. Supported platform/filesystem matrix

A production claim must name the tested environment.

Example initial scope (choose only after actually testing):

```text
Linux x86_64
local ext4
local XFS
single writer process
```

For each supported filesystem run:

- normal unit/integration suite;
- process crash matrix;
- live I/O fault matrix where injectable;
- seal/recycle/compaction crash tests;
- backup/restore;
- large-history reopen;
- wheel-level LangGraph crash/reopen tests.

If macOS/APFS wheels are published as supported, run equivalent filesystem
semantics there. If Windows is not tested, say unsupported rather than merely
publishing an untested wheel.

Document the durability assumption: the OS/filesystem/device must honor the
sync/flush contract. Do not claim survival against hardware that lies about
durable flushes.

An extra-mile power-cut campaign can use a VM/device fault harness, but it must
be reported as evidence for the exact tested stack, not universal hardware
proof.

---

## 30. Supply-chain and release engineering

Before production release:

- [ ] all required CI green on release commit;
- [ ] branch protection requires those checks;
- [ ] crate and wheel are built from the tagged commit;
- [ ] packaged crate tests run from the unpacked artifact;
- [ ] wheel tests run from installed wheels in clean environments;
- [ ] Rust MSRV + stable tested;
- [ ] Python 3.10-3.13 matrix for supported wheels;
- [ ] `cargo audit`/`cargo deny` policy passes or documented exception is
      reviewed;
- [ ] Python dependency audit passes or exception is reviewed;
- [ ] licenses are compatible;
- [ ] release notes include format compatibility/migration requirements;
- [ ] checksums/attestations/SBOM published where practical;
- [ ] rollback procedure documented.

Do not make the Rust toolchain a runtime dependency for Python users.

---

## 31. Independent validation

AI-generated or AI-assisted code is not disqualified, but it increases the
importance of external auditability.

Before broad production recommendation obtain:

- [ ] independent human review of file format, WAL/authority ordering, recovery,
      compaction, deletion, and FFI boundary;
- [ ] independent reproduction of the main benchmark on another machine;
- [ ] at least one external fuzzing/security review or equivalent adversarial
      testing effort;
- [ ] at least one real external pilot using Tulya as primary checkpoint storage
      with backups/restore tested;
- [ ] no unresolved known data-loss/corruption bug in the claimed supported
      configuration.

Record reviewer findings and fixes. Do not use "battle-tested" merely because
CI is large.

Critical subsystems should remain small enough to audit. Prefer one clear state
machine/invariant document over layers of generated wrappers and repeated
claims.

---

# PART D — CI AND EVIDENCE

## 32. Target CI layout

A mature workflow should contain logically separate required jobs.

### Rust core

```text
fmt
clippy -D warnings
unit/integration tests
format-v1/v2 golden fixtures
state-machine/property tests
process crash matrix
live I/O fault matrix
package-from-artifact smoke
```

### Security

```text
cargo audit / cargo deny
Python dependency audit
fuzz-target compile + short smoke
optional sanitizer/Miri checks where useful
```

### Python wheel matrix

For each supported Python/platform combination:

```text
build wheel
install wheel into clean venv
import
basic TulyaSaver round trip
sync StateGraph smoke
async StateGraph smoke
```

### LangGraph conformance

Run against the release lock and supported-version matrix:

```text
official base conformance: MUST PASS
official all-detected capabilities: MUST PASS
machine-readable report artifact: REQUIRED
```

### LangGraph durability

```text
pending-write failure/resume
restart idempotency
ack -> immediate kill -> reopen
branch/time travel after reopen
delete + reopen
namespace/subgraph cases
```

### Scheduled/nightly

```text
1m checkpoint deep-history test
1 GiB/10 GiB locality scaling where infrastructure permits
long fuzz campaigns/latest-canary compatibility
independent large benchmark matrix
```

Nightly failures must be triaged before release even if they are too expensive
for every PR.

---

## 33. Release evidence directory

For every candidate release, preserve machine-readable evidence, for example:

```text
benchmarks/evidence/releases/v0.X.Y/
├── git_commit.json
├── rust_toolchain.json
├── python_matrix.json
├── langgraph_versions.json
├── conformance.json
├── crash_matrix.json
├── io_failure_matrix.json
├── locality_append_scaling.json
├── range_read_scaling.json
├── deep_history.json
├── benchmark_comparators.json
├── backup_restore.json
├── format_compatibility.json
└── platform_matrix.json
```

A marketing/README number must point to a reproducible evidence record.

---

# PART E — IMPLEMENTATION ORDER

## 34. Recommended PR sequence

Do this in dependency order; do not polish an API around an execution path we
already know must change.

### PR 0 — CI baseline

- restore green GitHub Actions;
- protect `main`/require checks;
- add this production-readiness doc to release discipline.

### PR 1 — failing locality instrumentation

- add bytes-read/bytes-written/nodes-touched counters;
- add 10 MiB/100 MiB/1 GiB + 1 KiB append benchmark;
- demonstrate the current O(parent) behavior so the regression is measurable.

### PR 2 — `PersistentSequence` abstraction

- isolate append/range/length/stream interfaces from checkpoint metadata;
- no format change yet unless necessary.

### PR 3 — balanced persistent sequence

- subtree byte lengths;
- bounded depth invariant tests;
- branch and range-read tests.

### PR 4 — remove parent reconstruction from append

- derive child metadata without reading full parent;
- choose a sound structural/content commitment;
- make 1 GiB + 1 KiB locality test pass.

### PR 5 — format decision

- if v1 can remain valid, add compatibility tests;
- otherwise freeze v1, add v2 + golden fixture + atomic migration.

### PR 6 — Python package / PyO3 binding

- `langgraph-checkpoint-tulya` package;
- direct in-process Rust binding;
- sync/async wrapper;
- wheel smoke.

### PR 7 — exact LangGraph checkpoint schema

- checkpoint key/metadata/parents/namespaces;
- typed opaque channel-value storage;
- serializer round trips;
- reuse unchanged channel versions.

### PR 8 — primary base saver reads/writes

- `put/aput`;
- `get_tuple/aget_tuple`;
- `list/alist`.

### PR 9 — pending writes + delete

- `put_writes/aput_writes` including restart idempotency;
- `delete_thread/adelete_thread`;
- stale-write-after-delete protection.

### PR 10 — official conformance

- add `langgraph-checkpoint-conformance`;
- require `passed_all_base()`;
- require `passed_all()` for every advertised/detected extended capability;
- test supported LangGraph/Python matrix.

### PR 11 — graph-level durability suite

- real StateGraph sync/async;
- interrupt/resume;
- partial-superstep pending writes;
- time travel/fork;
- namespaces/subgraphs;
- Python-level kill/reopen.

### PR 12 — live I/O fault matrix

- ENOSPC/short write/EIO/sync/rename/dirsync;
- indeterminate outcome + safe retry semantics;
- apply to foreground and maintenance.

### PR 13 — fuzz/security hardening

- cargo-fuzz + random state-machine tests;
- dependency audits;
- parser/path/decompression limits.

### PR 14 — backup/restore and migration operations

- supported backup command/API;
- destructive restore drill;
- migration crash matrix if Format v2 exists.

### PR 15 — production evidence

- independent second workload;
- strong current LangGraph comparators;
- supported filesystem/platform runs;
- external review/reproduction.

Extended LangGraph `copy_thread`, `delete_for_runs`, `prune`, and optimized
`delta_channel_history` can be implemented after base conformance, but any one
that is advertised must pass upstream tests. The extra-mile target is full
conformance without compromising DeltaChannel correctness.

---

# PART F — DEFINITIONS OF DONE

## 35. Community-alpha gate

All must be true:

- [ ] green required CI on `main`;
- [ ] direct `pip install` wheel, no Rust toolchain/CLI subprocess required;
- [ ] real primary `TulyaSaver`, no authoritative SQLite/Postgres shadow;
- [ ] arbitrary channel values round-trip through LangGraph serializer;
- [ ] `put`, `put_writes`, `get_tuple`, `list`, `delete_thread` implemented by
      Tulya;
- [ ] sync + async graph execution;
- [ ] official LangGraph base conformance passes;
- [ ] all advertised optional capabilities pass official tests;
- [ ] restart/branch continuation;
- [ ] pending-write failure/resume;
- [ ] acknowledgement -> kill -> reopen tests;
- [ ] clear single-writer/local-filesystem alpha scope;
- [ ] README still says experimental/alpha and recommends backups.

At this point it is reasonable to approach the LangGraph community for testers.

---

## 36. Production-candidate gate

Everything in community alpha plus:

- [ ] no full-parent reconstruction on local append;
- [ ] balanced persistent sequence/depth invariant;
- [ ] local append and range-read scaling evidence including 1 GiB parent;
- [ ] 100k/1m history scaling evidence;
- [ ] live I/O failure matrix;
- [ ] maintenance/ENOSPC crash safety;
- [ ] fuzz/property testing campaign;
- [ ] format compatibility + migration tests if applicable;
- [ ] backup/restore drill;
- [ ] supported platform/filesystem matrix;
- [ ] current DeltaChannel/Postgres/SQLite comparator rerun;
- [ ] installed-wheel tests for supported Python versions;
- [ ] no known unresolved corruption/data-loss bug in supported scope.

---

## 37. Production-ready gate

Everything above plus:

- [ ] release commit and artifacts fully green/reproducible;
- [ ] official conformance record archived;
- [ ] independent storage/recovery/FFI review completed and findings resolved or
      documented;
- [ ] independent benchmark reproduction completed;
- [ ] external real-world primary-saver pilot completed with successful
      backup/restore/reopen evidence;
- [ ] operator docs cover backup, restore, upgrade, migration, fsck, disk-full,
      errors, and recovery;
- [ ] security/support scope is explicit;
- [ ] performance claims point to current release evidence, including losses;
- [ ] no claim of multi-writer/distributed/power-loss behavior outside what was
      actually tested.

Only then use language such as:

> **Production-ready for the documented single-host, single-writer,
> local-filesystem deployment scope.**

Do not use "battle-tested," "formally verified Rust storage," or universal
"X times better" claims unless separate evidence specifically justifies them.

---

## 38. The standard to keep after release

Production readiness is not a one-time certificate.

For every storage-format, durability, LangGraph-contract, GC, or concurrency
change:

1. identify which invariant above can be affected;
2. add/fail a test before changing the implementation where practical;
3. rerun official LangGraph conformance;
4. rerun the relevant crash/I/O/migration/locality matrix;
5. preserve old format fixtures;
6. update release evidence;
7. report regressions and losses, not only improvements.

The project should optimize for being easy to audit and falsify. The strongest
answer to skepticism is not a larger codebase; it is a small set of clear
invariants backed by reproducible evidence.