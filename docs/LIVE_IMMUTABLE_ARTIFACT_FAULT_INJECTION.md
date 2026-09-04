# Live immutable segment/route fault injection

Status: staged production-resilience evidence. Local Rust 1.80 verification is
required before this unit is accepted.

## DECISION

Immutable segment and route files are prerequisites for a future manifest, but
they are not logical authority by themselves.

A failure while publishing a named segment or route therefore has two separate
properties:

1. the logical seal/prune operation is definitely **not committed**, because the
   manifest was not changed;
2. the current writer is **not safe to retry in place**, because an immutable
   generation file or temporary file may already exist.

The runtime returns `RecoveryRequired` with
`RecoveryRequired::authority_committed() == false` and poisons the writer.

After close/reopen, the old authoritative manifest is loaded first and
`reclaim_unreferenced_generation_files` removes any segment/route final or
temporary files that are not referenced by that manifest. Only then may the
same generation number be reused.

## WHY

On some filesystems/platforms, renaming a newly generated file over an existing
same-name destination is not portable. A failed segment or route publication
can leave one of these shapes:

```text
.segment-g.tmp only
segment final only
segment final + .route-g.tmp
segment final + route final
```

Allowing same-handle retry would make correctness depend on replacement
semantics of the host filesystem.

Reopen provides a deterministic normalization point: the manifest decides which
generation artifacts are live, and all other generation files are reclaimed.

## LIVE FAULT CASES

The existing feature-gated publication channel supports:

```text
segment-sync-eio-after
segment-rename-eio-before
segment-rename-eio-after
segment-dir-sync-eio-after
route-sync-eio-after
route-rename-eio-before
route-rename-eio-after
route-dir-sync-eio-after
```

Each “after” case performs the real filesystem operation first and then returns
a synthetic EIO. This deliberately creates physical states that are possible
when an application observes failure after the kernel/filesystem may already
have completed work.

## ACCEPTANCE INVARIANTS

The live seal matrix in `tests/artifact_publication_faults.rs` checks every
case above.

For every case:

- `seal_through` returns `RecoveryRequired`;
- `authority_committed() == false`;
- the recovery context identifies the segment or route path that failed;
- the in-process sealed checkpoint watermark remains unchanged;
- exact checkpoint reads still reflect the previously committed hot state;
- every later mutation on that handle is blocked;
- reopen recovers the old manifest authority;
- all checkpoint bytes remain exact;
- `verify_all` reports no failures;
- reopen removes every unreferenced generation-one final/tmp artifact;
- the same generation can then be sealed successfully after reopen.

The matrix intentionally covers orphan states from both segment and route
publication, including the case where segment publication succeeded but route
publication failed.

## PRODUCTION SEMANTICS

The poisoning rule is not limited to synthetic faults.

In seal and prune, any error returned while publishing the finalized segment or
route is converted to pre-authority `RecoveryRequired`. This includes actual
sync, rename, directory-sync, create, write, and flush errors that reach those
publication calls.

Errors that occur earlier while constructing/encoding a segment before named
publication starts remain ordinary definite failures and may be retried if the
writer itself has not been poisoned.

## ALTERNATIVES REJECTED

### Retry the same generation immediately

Rejected because replacement behavior for an orphan same-name final file is not
portable across supported filesystems/platforms.

### Delete the orphan immediately and continue on the same handle

Rejected because cleanup itself is fallible and would add a second authority
decision inside an already-failed publication path.

### Mark artifact rename failures as durability-indeterminate

Rejected because segment/route files do not become logical authority until a
manifest references them. The logical operation is definitely uncommitted even
when the physical artifact state is uncertain.

### Ignore orphan files until a later background cleanup

Rejected for the writer path because a subsequent seal may need to reuse the
same generation name. Reopen cleanup is required before mutation resumes.

## FORMAT IMPACT

None. No segment, route, manifest, WAL, checkpoint, or public format bytes
change.

## BOUNDARY OF THIS EVIDENCE

This unit exercises segment/route publication through the normal seal path.

The prune path uses the same production publication/poisoning helpers, but its
deletion/tombstone semantics and replacement-generation fault matrix remain a
separate acceptance unit.

Old-generation unlink/reclaim failures, backup/restore, migration, and the v2
public-authority path also remain outstanding.
