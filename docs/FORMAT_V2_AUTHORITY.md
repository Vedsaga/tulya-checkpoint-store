# Format-v2 authority and recovery dispatch

Status: staged design. Public Format v1 remains the only writable store format.
This unit defines how recovery will identify a future public Format v2 without
reinterpreting any existing v1 manifest, WAL, segment, or checkpoint bytes.

## Decision ledger

**DECISION**

Introduce a pure public-format authority probe before changing production
recovery. The probe classifies:

- public manifest `format_version = 1` as Format v1;
- public manifest `format_version = 2` as Format v2;
- an empty/zero-filled hot-WAL prefix as no hot record;
- `T2W1` as the Format-v1 authoritative hot transaction;
- `T2C2` as the Format-v2 authoritative hot commit; and
- bare `T2W2` as invalid authority.

A manifest/WAL pair is accepted only when both select the same public format.

**WHY**

The current production loader has one global public format version and routes
all open/recovery through the Format-v1 manifest/WAL implementation. A future
v2 writer must not make the old parser guess whether new bytes are compatible.
Recovery needs one small, fail-closed discriminator before it enters either
representation-specific parser.

`T2W2` remains an inner structural record. Safe retry metadata and the client-
visible commit boundary live in `T2C2`, so recovery must never publish a bare
`T2W2` merely because its own digest is valid.

**ALTERNATIVES REJECTED**

- Change `crate::format::VERSION` to 2 before migration/recovery exists:
  rejected because new stores would claim a format the production writer cannot
  yet safely create and existing code would stop expressing the current v1
  authority accurately.
- Teach the existing v1 parser to infer new semantics from record flags:
  rejected because released v1 bytes must retain exactly their old meaning.
- Treat any recognized WAL magic as authoritative independent of the manifest:
  rejected because a stale/mismatched hot file must not override the manifest's
  selected public format.
- Treat bare `T2W2` as equivalent to `T2C2`: rejected because that would discard
  the durable request/idempotency publication boundary.

**FORMAT IMPACT**

None in this unit. No persistent bytes are written differently. Public
`crate::format::VERSION` remains 1. Version 2 is only recognized by the staged
probe so later recovery can dispatch explicitly once a v2 manifest writer and
migration protocol exist.

## Probe contract

The manifest probe reads only the public compatibility discriminator:

```text
format         == "tulya-checkpoint-store"
format_version == 1 | 2
```

Any other public format name, missing discriminator, malformed JSON, or unknown
version fails closed.

Pre-release private manifest revisions are intentionally outside this pure
public-format probe. The existing compatibility loader continues to own those
until the production dispatcher is integrated. A future integration must keep
that compatibility path explicit rather than pretending private revisions are
public Format v2.

## Hot-WAL authority

The first logical hot bytes determine the outer authority record:

```text
0000...  -> empty hot suffix
T2W1     -> Format-v1 transaction WAL
T2C2     -> Format-v2 durable commit WAL
T2W2     -> reject: inner structural transaction is not authoritative
other    -> reject: unsupported WAL magic
```

The store physically preinitializes unused WAL capacity with zeroes. Therefore
an all-zero first logical prefix is the canonical empty state; recovery must use
the logical tail/manifest geometry rather than treating the reserve capacity as
committed data.

## Recovery dispatch matrix

```text
manifest v1 + empty/T2W1 -> v1 transaction recovery
manifest v2 + empty/T2C2 -> v2 commit recovery
manifest v1 + T2C2       -> fail closed
manifest v2 + T2W1       -> fail closed
bare T2W2                -> fail closed before dispatch
unknown manifest version -> fail closed before dispatch
unknown WAL magic        -> fail closed before dispatch
```

An empty hot suffix is interpreted according to the manifest version. It does
not downgrade or upgrade the store.

## Migration authority rule

A later v1 -> v2 migration must publish the v2 manifest only after all bytes
required for a valid v2 authoritative state are durable. Until that manifest
publication succeeds, the old v1 manifest remains authoritative and recovery
must continue selecting the v1 parser.

The migration implementation must also define what happens to an existing v1
hot suffix before the v2 manifest is published. It may be converted into v2
commits or first sealed/normalized under v1, but a public v2 manifest must never
point at a `T2W1` hot suffix.

## Current boundary

This unit deliberately does not yet:

- change `crate::format::VERSION` from 1;
- write a public Format-v2 manifest;
- route `CheckpointStore::open` through the new dispatcher;
- write `T2C2` into `hot.wal`;
- define the sealed Format-v2 generation container;
- implement v1 -> v2 migration; or
- change any existing Format-v1 fixture or compatibility promise.

The next unit should connect this probe to a versioned manifest-loading
abstraction while preserving the current pre-release/v1 loader byte-for-byte,
then add an isolated v2 manifest schema only after that dispatch path is green.
