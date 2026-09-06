# Format-v2 conformance vectors

src/persistent_sequence/format_v2_conformance.json is the first
language-neutral fixture at the Lean/Rust boundary.  The Rust unit runner in
conformance_v2.rs parses it with serde_json and checks:

- canonical T2N2/T2R2 leaf and balanced-branch structural commitment bytes;
- historical-root preservation and independent sibling-branch commitments;
- fail-closed noncanonical node/root metadata;
- dense compaction remapping that preserves live checkpoint identities and
  operation digests;
- canonical T2S2 live-ledger and tombstone-only snapshot bytes plus reopen;
- the tombstone-only 128-byte SHA-256 input, including the domain separator,
  64-byte header prefix, and `T2X2` record;
- complete-frame and torn-final-frame hot-WAL recovery outcomes;
- fail-closed recovery for reserve garbage, bare structural records,
  corrupted complete frames, and duplicate physical retries;
- the frozen SHA-256 logical-operation digest for the initial checkpoint;
- digest stability when the physical identity version is relocated;
- requestless `T2C2` commits carrying the canonical zero operation digest;
- requestful `T2C2` commits carrying the logical operation digest;
- active same-digest replay;
- active different-digest conflict;
- retired same-digest no-resurrection;
- retired different-digest conflict; and
- unknown identity classification as new;
- active/retired request-ID overlap rejected by the sealed-snapshot validator;
- subtree deletion that retires deleted-checkpoint request identities and
  remaps surviving request ordinals; and
- deleting the last checkpoint while preserving a tombstone-only state.

The fixture schema is intentionally narrow.  Lean's
PersistentAVLV2RequestLedger.lean provides the symbolic request-ledger
semantics and theorems.  PersistentAVLV2SnapshotWireReference.lean
reconstructs the exact tombstone-only snapshot digest input and its geometry;
it does not claim to compute Rust's SHA-256 bytes.
The runner therefore proves finite executable agreement for these cases, not
full Rust refinement or filesystem durability.

The deletion cases intentionally exercise the existing pure v2 apply
boundary.  They do not make subtree deletion a public Format-v2 operation:
the v2 implementation remains staged behind persistent_sequence and public
checkpoint-store Format v1 is unchanged.

This fixture is a conformance boundary and test artifact, not a format
migration.

There are currently no external Format-v2 consumers.  The `v1`/`v2` names are
internal on-disk authority boundaries: v1 is the current public implementation
and v2 is staged behind `persistent_sequence`.  A v1-to-v2 migration and
dual-version recovery are therefore deferred product work, not a current
release gate; they become necessary only when v2 is selected as the public
writable format or existing v1 stores must be upgraded.

> PRE-USER RELEASE DECISION (2026-09-06): per docs/HN_LAUNCH_EXECUTION_HANDOFF.md
> there are no external v1 stores, so no v1-to-v2 migration is required at all;
> pre-release v1 bytes fail explicitly as unsupported.  The `v1`/`v2` tags above
> are internal staging names: at release-format freeze the staged balanced
> design becomes release Format v1 in one deliberate naming/wire cleanup, and
> this document follows that rename.
