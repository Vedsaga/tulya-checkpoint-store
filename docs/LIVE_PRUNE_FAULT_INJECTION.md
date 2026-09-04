# Live prune/deletion/reclaim fault injection

Status: staged production-resilience evidence. Local Rust 1.80 verification is
required before this unit is accepted.

## DECISION

Checkpoint deletion is logical first and physical reclamation later.

For a subtree prune, the replacement generation is built from retained state and
the next manifest carries:

- the replacement segment/route set;
- durable checkpoint tombstones;
- durable retired request identities.

The new manifest is the deletion authority. Old-generation unlinking is
maintenance after that authority transition.

The live matrix therefore classifies faults by authority, not by syscall name:

```text
replacement preparation
        |
        v
segment/route publication
        |
        v
generation-manifest publication
        |
        +-- authority unresolved --> DurabilityIndeterminate
        |
        v
deletion authority committed
        |
        v
old-generation reclaim
        |
        +-- failure --> RecoveryRequired(authority_committed = true)
```

## PRE-AUTHORITY REPLACEMENT FAILURES

The prune matrix reuses the live segment/route fault cases:

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

It also covers:

```text
manifest-sync-eio-after
```

For all of these cases, deletion authority is definitely still old.

Because named replacement artifacts may already exist, the writer returns
`RecoveryRequired` with `authority_committed() == false`, blocks further
mutation, and requires reopen cleanup before generation reuse.

After reopen the matrix requires:

- manifest generation remains the old generation;
- the candidate checkpoint is still readable;
- its original request ID still resolves as already committed;
- every unreferenced replacement generation artifact is removed;
- the prune can then be retried successfully.

## MANIFEST AUTHORITY UNCERTAINTY

The matrix covers both possible recovered authorities:

```text
manifest-rename-eio-before
manifest-rename-eio-after
manifest-dir-sync-eio-after
```

`manifest-rename-eio-before` is deliberately injected before the real rename.
The public result is still `DurabilityIndeterminate`, and reopen resolves to
old authority.

The “after” cases perform the real rename or directory sync first and then
surface an injected EIO. Reopen therefore resolves to new deletion authority.

The current process does not publish `self.manifest` / compacted state after an
indeterminate manifest error. Reopen is the authority resolver.

## POST-AUTHORITY RECLAIM FAILURES

The feature-gated publication channel adds:

```text
prune-reclaim-delete-eio-before
prune-reclaim-delete-eio-after
prune-reclaim-dir-sync-eio-after
```

These occur only after the replacement manifest is successfully published and
`CheckpointStore` has installed the compacted state in process memory.

Expected behavior:

- prune returns `RecoveryRequired`;
- `authority_committed() == true`;
- the recovery context path identifies the store directory whose obsolete
  generation maintenance failed;
- the deleted checkpoint is already invisible in the current process;
- all later mutations on that handle are blocked;
- reopen retains deletion authority regardless of how much of the old
  generation was physically removed;
- reopen completes cleanup of any remaining unreferenced old-generation files.

The three cases deliberately exercise:

```text
old segment + old route both still present
one old artifact may already be deleted
both old artifacts deleted and directory sync completed
```

## ANTI-RESURRECTION INVARIANTS

After deletion authority is recovered, every case must prove:

1. reading the deleted checkpoint returns `CheckpointDeleted`;
2. appending the same deleted checkpoint identity returns
   `CheckpointDeleted`;
3. retrying the exact request ID whose checkpoint was deleted returns
   `CheckpointDeleted`, not “already committed” and not a new append;
4. reusing that retired request ID for different bytes returns
   `RequestIdConflict`;
5. retained sibling checkpoints remain byte-for-byte exact;
6. `verify_all` reports no failures;
7. a new retained-lineage checkpoint can be appended after reopen.

This is the production v1 evidence for the readiness invariant:

```text
logical delete succeeds
    => deleted checkpoint/request identity cannot become live again
       merely because physical reclaim was interrupted
```

## ORDERING

All fallible semantic/metadata preparation for prune is completed before the
first named replacement artifact is published:

```text
compact retained state
build replacement segment temporary file
build route bytes/metadata
build next manifest
serialize next manifest
compute report counts
publish replacement segment
publish replacement route
collect coexistence accounting
publish prepared manifest bytes
install new manifest + compacted state
reclaim old generation
collect final storage accounting
```

If a definite failure occurs after named replacement publication but before
manifest authority, the writer requires reopen so orphan replacement files can
be removed portably.

## ALTERNATIVES REJECTED

### Delete old files before publishing tombstones

Rejected because a crash could physically destroy the old authoritative state
before the replacement deletion authority exists.

### Treat reclaim failure as deletion failure

Rejected because the tombstone/retired-request manifest is already the durable
logical authority.

### Let the poisoned handle continue because reads look correct

Rejected because the process has observed incomplete physical maintenance.
Further mutation must wait for reopen normalization.

### Reuse a retired request ID for the same bytes

Rejected because that would resurrect a logically deleted checkpoint through
idempotency retry semantics.

### Make replacement-file cleanup best-effort before retrying in place

Rejected because cleanup is itself fallible and same-name replacement semantics
are not portable across filesystems.

## FORMAT IMPACT

None. No persisted Format v1 bytes change. The existing manifest tombstone and
retired-request fields retain their semantics.

## BOUNDARY OF THIS EVIDENCE

This unit covers production Format v1 subtree prune/deletion and old-generation
reclaim.

It does not make staged Format v2 the production authority, does not complete
backup/restore or migration evidence, and does not prove platform-specific
unlink/directory durability beyond the tested filesystem scope.

The staged v2 request-retirement helper still requires its separate fail-atomic
cleanup before v2 deletion is wired into production.
