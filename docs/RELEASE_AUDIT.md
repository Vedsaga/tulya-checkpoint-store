# Release-candidate audit

This is the evidence boundary for the first product. Tulya exposes one public
checkpoint format, one core API, and one CLI. It does not expose the private
development revision labels used before extraction.

## Implemented and testable here

- Durable append, exact branch reconstruction, seal/reopen, pruning, and
  generation-safe reclaim.
- A manifest compatibility guard, one golden store, and an independent
  read-only `fsck`.
- A 32-boundary deterministic process-crash matrix plus a real-corpus
  crash/resume demonstration.
- Sync and async fail-open LangGraph shadowing while the existing saver remains
  authoritative.
- An executable public-corpus matrix covering Tulya, normalized SQLite,
  content-addressed SQLite, raw/zstd full snapshots, direct LangGraph SQLite
  full and delta modes, packed Git, and direct LangGraph PostgreSQL full state.
- Frozen evidence that preserves strong competing results and negative
  findings rather than presenting only Tulya wins.
- Rust 1.80 formatting, lint, unit/integration, crash, consumer-package, and
  CI workflows.

## Not established by this repository

- Sudden-power-loss durability. Current fault injection proves process-crash
  behavior at named boundaries.
- Controlled-cold latency across several machines/filesystems.
- Arbitrary LangGraph channel/schema replacement. The integration is a shadow
  for the append-only message projection.
- End-to-end agent-cost savings, semantic memory, Frames, model quality, RL
  data quality, or bug-discovery accuracy.
- External adoption. Three workload-owner pilots and at least one measured
  external economic result are still commercial gates, not code tasks.

## Honest release decision

The repository can become a public technical release after a clean-worktree
reproduction and an independent reserved-holdout run. It cannot yet support a
claim of customer-validated economics. Use `PILOT_SCORECARD.md` for that gate;
do not manufacture a testimonial from internal measurements.
