# Manifest authority failure boundary

Status: accepted internal resilience contract. Rust 1.80 formatting and strict
feature-enabled Clippy passed; error classification passed 8/8, the committed
maintenance invariant passed, and the full library suite passed 109/109. Live
manifest/rename/directory-sync and WAL-recycle fault evidence is staged
separately in `docs/LIVE_PUBLICATION_FAULT_INJECTION.md`.

## DECISION

The manifest is the logical publication authority for sealed generations,
pruned generations, and store re-identification.

Immutable segment and route files are durable prerequisites, but they do not
change logical authority by themselves. A seal/prune/reidentify operation
changes logical authority only through manifest publication.

The runtime therefore distinguishes three states:

```text
old-authority
    |
    | build/sync/publish segment + route
    | write/sync manifest temporary file
    v
manifest-ready
    |
    | manifest rename + parent-directory durability
    v
new-authority
    |
    | WAL recycle / old-file reclaim / lazy-reader refresh / metrics
    v
maintenance-complete
```

Manifest rename or parent-directory sync failure is classified as
`DurabilityIndeterminate` because the caller cannot safely infer whether the
new manifest became durable authority. The writer is poisoned and must reopen.

Once manifest publication returns success, the new authority is known committed.
The in-process `CheckpointStore.manifest` is updated immediately before any
fallible post-commit maintenance.

A later maintenance failure is classified as `RecoveryRequired` with
`RecoveryRequired::authority_committed() == true`. This tells the caller that
the requested logical authority transition is already committed even though the
current writer must reopen before another mutation.

## WHY

Previously, `seal_through` could durably publish a new manifest and then fail
while collecting storage metrics, recycling the WAL, reopening the recycled WAL,
or refreshing a lazy reader. The function would return an error while the
in-memory store still held the old manifest.

That creates two ambiguities:

1. the caller cannot tell whether retrying the logical operation would duplicate
   work or conflict with already-committed authority;
2. read-only process state such as the sealed checkpoint watermark can disagree
   with the manifest already published on disk.

The authority update must therefore happen at the actual logical commit point,
not at the end of all maintenance.

## FAILURE CLASSES

Before manifest authority:

- validation, encoding, allocation, route construction, manifest construction, and manifest serialization are completed before the first named immutable artifact is published;
- segment/route construction failure before named publication: definite failure, writer remains usable unless the underlying path separately poisoned it;
- once a segment/route final or temporary generation artifact may exist, any later definite pre-authority failure returns `RecoveryRequired` with `authority_committed() == false`, because reopen cleanup is required before that generation name is reused;
- this includes segment/route named publication failures and generation-manifest write/flush/tmp-file-sync failures;
- manifest rename or parent-directory-sync uncertainty remains a separate `DurabilityIndeterminate` authority case.

At manifest authority:

- manifest rename error: `DurabilityIndeterminate`;
- manifest parent-directory sync error: `DurabilityIndeterminate`;
- the writer is poisoned because reopen is required to resolve authority.

After successful manifest authority:

- WAL recycle failure;
- recycled-WAL reopen/seek failure;
- lazy sealed-reader refresh failure;
- post-prune obsolete-generation reclaim failure;
- post-commit storage-accounting failure;

are returned as `RecoveryRequired` with
`authority_committed() == true`.

A subsequent mutation attempted on that poisoned handle returns ordinary
`RecoveryRequired` for the handle itself. Callers should use the original
post-commit error to learn that the logical operation was already committed.

## ORDERING CHANGES

### Seal

```text
prepare immutable generation
prepare route bytes + route metadata
prepare next manifest + serialized manifest bytes
publish segment
publish route
collect coexistence accounting
publish prepared manifest authority
update self.manifest                 <-- logical commit reflected in process
recycle WAL
reopen recycled WAL
refresh lazy base if applicable
collect final storage accounting
return SealReport
```

Any failure after named immutable publication begins but before manifest
authority requires reopen if authority is still definitely old, because orphan
generation files may need normalization.

### Prune

```text
build replacement live generation
prepare next manifest + serialized manifest bytes
precompute fallible report counts
publish replacement segment
publish replacement route
collect coexistence accounting
publish prepared manifest authority
update self.manifest + self.state    <-- deletion authority reflected in process
reclaim obsolete generations
collect final storage accounting
return PruneReport
```

### Re-identification

```text
construct next manifest
publish manifest authority
update self.manifest + self.store_id
return StoreId
```

There is no fallible work after successful re-identification authority.

## ALTERNATIVES REJECTED

### Keep assigning the in-memory manifest at the end of seal

Rejected because disk authority can already be newer when post-commit
maintenance fails.

### Return generic I/O for every failure

Rejected because a caller must distinguish an old-authority failure,
indeterminate manifest publication, and a definitely committed authority whose
maintenance failed.

### Treat WAL recycle as part of logical seal commit authority

Rejected because recovery already derives authority from the manifest and can
normalize a WAL that still contains the sealed prefix. Recycle is required
physical maintenance, not the logical checkpoint publication decision.

### Ignore post-commit maintenance errors

Rejected because hiding recycle/reclaim/reader-refresh failures prevents
operators from knowing the current writer should be reopened.

## FORMAT IMPACT

None. Manifest bytes, segment bytes, route bytes, WAL bytes, and public format
version remain unchanged. This unit changes runtime ordering and error
classification only.

## ACCEPTANCE

This internal boundary is accepted only after:

- strict formatting and Clippy pass;
- error-classification tests prove the committed-authority bit;
- a store-level test proves post-commit maintenance poisons later mutation while
  retaining readable committed state;
- the full library suite and existing crash matrix remain green.

The accepted manifest/WAL-recycle syscall-result matrix is documented in
`docs/LIVE_PUBLICATION_FAULT_INJECTION.md`. Immutable segment/route publication
faults are staged separately in
`docs/LIVE_IMMUTABLE_ARTIFACT_FAULT_INJECTION.md`.
