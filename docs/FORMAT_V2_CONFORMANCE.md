# Format-v2 conformance vectors

src/persistent_sequence/format_v2_conformance.json is the first
language-neutral fixture at the Lean/Rust boundary.  The Rust unit runner in
conformance_v2.rs parses it with serde_json and checks:

- the frozen SHA-256 logical-operation digest for the initial checkpoint;
- digest stability when the physical identity version is relocated;
- active same-digest replay;
- active different-digest conflict;
- retired same-digest no-resurrection;
- retired different-digest conflict; and
- unknown identity classification as new.

The fixture schema is intentionally narrow.  Lean's
PersistentAVLV2RequestLedger.lean provides the symbolic request-ledger
semantics and theorems; it does not claim to compute Rust's SHA-256 bytes.
The runner therefore proves finite executable agreement for these cases, not
full Rust refinement or filesystem durability.

The v2 implementation remains staged behind persistent_sequence; public
checkpoint-store Format v1 is unchanged.  This fixture is a conformance
boundary and test artifact, not a format migration.
