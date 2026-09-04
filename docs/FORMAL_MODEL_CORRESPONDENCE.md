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
| `PersistentSequence::logical_len` | stored/logical sequence length | Active compatibility boundary. Format v1 may still derive the value through legacy traversal; a production writable representation must make parent/root metadata sufficient for local append and navigation. |
| `PersistentSequence::read_range` | random-access extraction | Active compatibility boundary returning exactly the selected logical bytes without full-root output materialization. Format v1 still retains legacy topology costs. |
| planned sequence append | persistent edit / historical edit | Required production target, not yet an active trait method. It will be added when a writable representation and real checkpoint caller exist. |
| planned bounded stream / structural verify | bounded extraction / validity and verifier boundary | Required production targets, not yet active trait methods. Exact Format-v2 commitment and verification rules remain intentionally unfrozen. |
| `CheckpointStore` single-writer ownership | SWMR writer boundary | Operational analogue only. The existing Rust process-locking implementation is not claimed to refine the Lean SWMR machine. |

## Decision ledger: persistent-sequence seam

### Decision

Introduce an internal representation-neutral persistent-sequence boundary before
replacing the legacy append tree. Keep the executable trait limited to methods
with real production callers; extend it as append, bounded streaming, and
structural verification implementations land.

The current seam separates:

- physical root identity;
- logical byte length;
- representation identity;
- exact range access.

The production target additionally requires append semantics, bounded streaming,
and structural verification, but those operations are not carried as unused
trait methods merely to mirror the final target interface.

### Why

The released Format-v1 store exposes version roots as raw node IDs. Its current
range path discovers internal subtree lengths recursively and caches them in
memory. Repeated message appends also create a left-deep binary history.

Changing callers directly to a balanced tree would couple checkpoint semantics
to a new representation and make format compatibility harder to reason about.
A narrow sequence seam lets us first adapt Format v1 exactly, then introduce a
balanced writable representation behind the same semantic boundary.

Keeping only exercised operations in the active trait also preserves the value
of strict `dead_code` checks and follows the project's no-speculative-abstraction
rule. The full production interface remains an acceptance target in
`docs/PRODUCTION_READINESS.md`, not a reason to keep unused executable surface.

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

### Format-v1 message-append compatibility step

The Format-v1 message append path now derives child logical length from persisted
`CheckpointInfo.logical_state_len` and feeds the legacy whole-state XXH3 through
bounded checkpoint-range chunks. It no longer reconstructs the complete parent
canonical JSON in one temporary vector merely to derive the child metadata.

This is a bounded-incremental-RAM compatibility improvement, **not** completion
of the append-locality gate. Format v1 still persists one XXH3-64 over the full
canonical checkpoint. Because that value is not a composable commitment for
concatenation, preserving released v1 hash semantics still requires O(parent)
read/hash work for a child append.

Accordingly, the production locality redesign requires a new writable format
with persisted subtree lengths plus a composable structural/content commitment.
That is a Format-v2 concern; the existing v1 `state_hash` field must not be
reinterpreted as such a commitment.

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
3. Bound Format-v1 message-append temporary materialization while preserving its
   exact whole-state XXH3 semantics; this step does not satisfy append locality.
4. Design the writable Format-v2 root/node metadata and composable commitment,
   then add the real sequence append operation with its checkpoint caller.
5. Add structural tests that expose the current left-deep depth as a legacy
   property rather than an accepted production invariant.
6. Implement the balanced representation with persisted subtree lengths and
   Format-v2 commitment metadata.
7. Add bounded streaming and structural verification to the sequence boundary
   as their production implementations land.
8. Add balancing, historical-preservation, range-locality, reopen, corruption,
   and scaling evidence before checking the corresponding production-readiness
   gates.

## Claim discipline

Until a real Rust/Lean refinement artifact exists, documentation and source
comments must use wording such as "informed by", "design analogue", or
"corresponds to" rather than "proved", "verified Rust", or "formally verified
implementation".
