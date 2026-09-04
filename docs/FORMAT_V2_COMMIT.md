# Format v2 durable commit (`T2C2`)

Status: staged internal design. No `T2C2`/`T2W2` bytes are yet authoritative
checkpoint-store bytes, and public `crate::format::VERSION` remains 1.

`T2W2` is the already-accepted append-local structural transaction. `T2C2` is
the durable publication envelope that adds request/retry identity without
reinterpreting the frozen `T2W2` grammar.

## Decision ledger

**DECISION**

Publish a future Format-v2 mutation as one `T2C2` record containing exactly one
canonical `T2W2` transaction plus an optional byte-string request identity.
For requestful commits, the envelope also stores a SHA-256 logical-operation
digest. The complete envelope has its own SHA-256 integrity digest.

**WHY**

Safe retry requires request identity to survive process death and recovery. It
cannot be reconstructed from process memory after an unknown acknowledgement.
At the same time, `T2W2` was already frozen and tested as the append-local
payload/node/version/checkpoint grammar. Mutating its accepted bytes merely to
add retry metadata would weaken the format discipline we are trying to earn.

The outer commit boundary also gives recovery one explicit rule: a bare `T2W2`
record is structural data, not an authoritative client-visible commit. Only a
complete, validated `T2C2` can publish it.

**ALTERNATIVES REJECTED**

- Change the accepted `T2W2` header in place: rejected because it silently
  changes already-frozen staged bytes.
- Keep request IDs only in memory: rejected because retries after crash or
  unknown acknowledgement would not be safe.
- Use the `T2W2` byte digest as the request operation digest: rejected because
  it includes physical watermarks/allocator choices rather than only logical
  request semantics.
- Write an unbound sidecar request record after `T2W2`: rejected because a
  crash between records would create an ambiguous authority boundary.

**FORMAT IMPACT**

New Format-v2 bytes only. Released Format v1 is unchanged. `T2W2` schema 1 is
also unchanged and becomes an inner structural record of `T2C2`.

The preinitialized hot file adds a separate physical `T2E2` completion footer
after each `T2C2`. That footer does not change `T2C2` semantics or bytes; it
proves that the sequential hot-WAL write reached the end of the exact logical
commit. See `FORMAT_V2_RECOVERY.md`.

## Canonical `T2C2` envelope

All integers are little-endian. Schema 1 uses an 88-byte header:

```text
0   .. 4    magic = "T2C2"
4   .. 8    total commit bytes, u32
8   .. 12   commit schema = 1, u32
12  .. 16   flags = 0, u32
16  .. 20   inner T2W2 bytes, u32
20  .. 24   request-id bytes, u32
24  .. 56   logical-operation digest, [32]u8
56  .. 88   commit digest, [32]u8
```

The body is exactly:

```text
[one complete T2W2 transaction]
[request-id bytes]
```

The commit digest is:

```text
SHA256(
    "tulya-checkpoint-v2/commit\0" ||
    header[0..56] ||
    body
)
```

The digest field itself is excluded.

Request IDs are opaque bytes, not UTF-8 strings. A present request ID must be
1..=4096 bytes. Requestless commits use `request_id_len = 0` and an all-zero
logical-operation digest as the only canonical encoding.

## Logical-operation digest

For a requestful checkpoint commit:

```text
SHA256(
    "tulya-checkpoint-v2/checkpoint-operation\0" ||
    commit_schema:u32_le ||
    checkpoint_no:u32_le ||
    len(thread_id):u32_le || thread_id ||
    len(checkpoint_id):u32_le || checkpoint_id ||
    len(parent_checkpoint_id_or_empty):u32_le || parent_checkpoint_id_or_empty ||
    canonical_logical_len:u64_le ||
    checkpoint_state_commitment:[32]u8
)
```

The digest deliberately excludes version IDs, node IDs, payload offsets, WAL
offsets, and generation/segment placement. Those are physical allocation
choices. The checkpoint-state commitment already binds the semantic channel
roots/presence; apply validation must prove that commitment against the actual
resolved versions before publication.

The first committed golden operation vector for:

```text
checkpoint_no = 1
thread_id = "thread"
checkpoint_id = "cp-1"
parent = none
identity bytes = "abc"
messages = none
result = none
```

is:

```text
6e0363445809801219e0a146b177509d7a74ff7cbff1ee35013d94ad433e9eda
```

## Retry ledger semantics

Committed state maintains an active request ledger:

```text
request_id -> (operation_digest, checkpoint_ordinal)
```

and deletion/compaction may move the identity into a retired ledger:

```text
request_id -> operation_digest
```

Classification occurs before attempting a new physical publication:

```text
unknown request id
    -> NEW

active id + same digest
    -> REPLAY existing checkpoint; write nothing

active id + different digest
    -> explicit request conflict

retired id + same digest
    -> RETIRED; write nothing and never resurrect deleted history

retired id + different digest
    -> explicit request conflict
```

This matches the existing v1 safety intent: retries after timeout/crash/unknown
acknowledgement do not duplicate a logical checkpoint, and identity reuse for
different bytes fails explicitly.

Requestless internal/migration commits remain representable, but they do not
provide a retry identity. Production LangGraph mutations that require safe
retry must use requestful commits.

## Apply validation against committed state

The append-local `T2W2` validator can prove facts involving only new records.
Before a new `T2C2` mutates committed state, the apply layer additionally
resolves every old reference against the current committed metadata.

It must prove all of the following before mutating any state:

1. the inner `T2W2` starting geometry exactly equals current committed geometry;
2. every new branch resolves both children (old or new), and reconstructing the
   branch from those children exactly reproduces length, height, split, and
   commitment;
3. every new version parent exists;
4. every new version root resolves to an existing old/new node and exactly
   matches that node's root metadata;
5. every checkpoint channel version resolves;
6. recomputing checkpoint-state metadata from those resolved roots exactly
   matches the stored `T2P2` state commitment/length;
7. the `(thread_id, checkpoint_id)` is not already committed;
8. any parent checkpoint exists in the same thread; and
9. request classification is `NEW` (unless the call is being returned as a
   replay/retired no-op).

Validation completes before payload/nodes/versions/checkpoint/request-ledger
state is extended. A validation error therefore has no partial logical apply.

## Recovery / authority rule

For a future v2 hot WAL, `T2C2` is the logical authority unit, not bare `T2W2`.
The physical hot file additionally requires the last-written `T2E2` completion
footer before recovery may consider that `T2C2` complete.

Recovery should:

1. prove the `T2C2 + T2E2` physical frame reached its canonical end;
2. validate `T2C2` length/schema/integrity;
3. validate its request metadata and logical-operation digest;
4. decode and validate the inner `T2W2` against the encoded starting geometry;
5. classify the request identity against the reconstructed ledger;
6. resolve old references against already committed state;
7. apply all logical state atomically; then
8. advance the scanner by the complete physical frame length.

A final zero-padded `T2C2`/`T2E2` write that never reaches its canonical footer
is an uncommitted suffix. Once the exact footer is present, any commit or apply
validation failure is corruption and fails closed. A complete inner `T2W2`
without its enclosing complete `T2C2` must never become client-visible.

## Current boundary

This unit is still pure/staged. It does **not** yet:

- write `T2C2 + T2E2` through `CheckpointStore::hot.wal`;
- change manifest/public format version;
- implement v1 -> v2 migration;
- replace sealed Format-v1 streams;
- wire request retirement to production subtree/thread deletion; or
- claim crash durability before the live WAL writer/recovery path is integrated
  and fault-tested.