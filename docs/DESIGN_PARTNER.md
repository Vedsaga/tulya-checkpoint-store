# Design-partner pilot

The first commercial question is narrow: does exact branch history remove
meaningful retained-state or replay cost in a workload someone already pays to
run?

A useful pilot shadows an existing checkpoint backend for two weeks without
changing authoritative reads. Before starting, freeze:

- workload owner and current backend/version;
- checkpoint count, branch/fork geometry, logical bytes, and retention period;
- whole-store allocated bytes including WAL/sidecars;
- durable-write and historical-read latency protocol;
- replay/recomputation events and their actual compute cost;
- correctness, privacy, deletion, and rollback requirements.

Tulya runs fail-open as a shadow. The pilot passes only if every mirrored
checkpoint verifies, restart continues existing branches, operational overhead
is acceptable, and the measured savings exceed a pre-agreed threshold. A
failure or a packed-Git/SQLite win is still a valid result and must be
published internally unchanged.

Record the frozen contract and final decision in
[`PILOT_SCORECARD.md`](PILOT_SCORECARD.md). The workload owner—not the Tulya
author—must sign off on the primary-backend accounting, the net saving, and
any quote proposed for public use.

What a pilot does not test: Frames, semantic code understanding, model quality,
bug discovery, RL data quality, or a 100x end-to-end business claim.
