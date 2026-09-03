# Rust / Lean storage correspondence

This document records design correspondence between `tulya-checkpoint-store`
and the Lean development in `Vedsaga/Tulya-MDL-Lean`.

It is a traceability document, **not** a claim that the Rust implementation is
formally verified or mechanically proved to refine the Lean model.

## Reference boundary

The primary public Lean reference is:

```text
formal/Tulya/Incremental/PersistentAVLFinalAPI.lean
```

That API exports proved boundaries for persistent edits, random access and work
bounds, preservation of historical versions, persistence/recovery, lifecycle
management, and related storage properties.

The Lean repository also contains a Rust implementation plan in:

```text
docs/rust-persistent-storage-implementation-plan.md
```

Its guidance that semantically different persistent identifiers and lengths
remain distinct fixed-width types is followed here where it fits the existing
checkpoint-store format.

## Current correspondence

| Rust concept | Lean reference concept | Relationship |
| --- | --- | --- |
| `persistent_sequence::PersistentRoot` | retained/published persistent AVL root | Design analogue. Rust currently wraps the released Format-v1 root node ID plus logical length; no refinement proof exists. |
| `persistent_sequence::LogicalLength` | logical sequence length / bounded random-access domain | Fixed-width Rust boundary corresponding to a logical natural-number length. Machine conversion remains checked. |
| `persistent_sequence::PersistentSequence::append` | persistent edit / historical edit | Intended semantic boundary: produce a new root without changing retained roots. Complexity guarantees are not yet claimed for the Format-v1 adapter. |
| `PersistentSequence::read_range` / `stream_range` | random-access extraction | Intended to return exactly the selected logical bytes without requiring full-root materialization. |
| `PersistentSequence::verify` | validity / verifier boundary | Structural verification hook. Exact Format-v2 commitment rules are intentionally not frozen yet. |
| `CheckpointStore` single-writer ownership | SWMR writer boundary | Operational analogue only. The existing Rust process-locking implementation is not claimed to refine the Lean SWMR machine. |

## Decision ledger: persistent-sequence seam

### Decision

Introduce an internal representation-neutral `PersistentSequence` contract
before replacing the legacy append tree.

The initial contract separates:

- physical root identity;
- logical byte length;
- representation identity;
- append semantics;
- range/stream access;
- structural verification.

### Why

The released Format-v1 store exposes version roots as raw node IDs. Its current
range path discovers internal subtree lengths recursively and caches them in
memory. Repeated message appends also create a left-deep binary history.

Changing callers directly to a balanced tree would couple checkpoint semantics
to a new representation and make format compatibility harder to reason about.
A narrow sequence seam lets us first adapt Format v1 exactly, then introduce a
balanced writable representation behind the same semantic boundary.

### Compatibility rule

Format-v1 bytes and their meaning are frozen. The first adapter must preserve
all current v1 reads, writes, recovery behavior, hashes, and fixtures.

If persisted subtree lengths, balancing metadata, or a new commitment scheme
cannot be encoded without changing v1 semantics, the writable redesign must be
Format v2.

### Commitment rule

This first contract does **not** invent an incremental XXH3 construction or
claim that the existing checkpoint hash is a composable subtree commitment.

Before a new writable format is accepted, we must choose and specify a sound
commitment strategy (or explicitly separate structural commitment from an
optional full-byte verification hash), persist the required metadata, and test
failure-closed decoding.

### Complexity rule

The semantic trait by itself does not establish production locality. The
Format-v1 adapter may retain legacy traversal costs while compatibility is
being isolated.

The balanced writable implementation must later demonstrate the production
bounds from `docs/PRODUCTION_READINESS.md`, including append and range-read work
that is not linear in unchanged parent size.

## Planned implementation sequence

1. Compile the internal sequence contract without changing Format-v1 bytes.
2. Adapt existing v1 root lookup, logical-length discovery, and range reads to
   typed `PersistentRoot` / `LogicalLength` boundaries.
3. Route message append construction through the sequence abstraction while
   keeping the existing v1 encoder byte-for-byte compatible.
4. Add structural tests that expose the current left-deep depth as a legacy
   property rather than an accepted production invariant.
5. Design the balanced representation and commitment metadata; introduce
   Format v2 if the persisted representation changes.
6. Add balancing, historical-preservation, range-locality, reopen, corruption,
   and scaling evidence before checking the corresponding production-readiness
   gates.

## Claim discipline

Until a real Rust/Lean refinement artifact exists, documentation and source
comments must use wording such as "informed by", "design analogue", or
"corresponds to" rather than "proved", "verified Rust", or "formally verified
implementation".
