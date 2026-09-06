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
semantics and theorems; it does not claim to compute Rust's SHA-256 bytes.
The runner therefore proves finite executable agreement for these cases, not
full Rust refinement or filesystem durability.

The deletion cases intentionally exercise the existing pure v2 apply
boundary.  They do not make subtree deletion a public Format-v2 operation:
the v2 implementation remains staged behind persistent_sequence and public
checkpoint-store Format v1 is unchanged.

This fixture is a conformance boundary and test artifact, not a format
migration.
