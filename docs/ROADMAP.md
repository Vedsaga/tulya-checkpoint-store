# Release roadmap

## Now: harden and release the existing store

The current Rust implementation already contains the commercially relevant
branch-history mechanism and has real public-corpus evidence. Rewriting it from
scratch from the Lean model would reset integration, recovery, format, and
benchmark confidence without answering the business question.

The 0.1 release gate is therefore the current implementation plus the single
public checkpoint format,
fsck, crash recovery, compatibility fixture, CI, a verified framework shadow,
and honest evidence. No Kriya–Karaka Frame, RL-data, bug-discovery, binary
provenance, or semantic-code-memory claim belongs in this release.

## Immediately after release

1. Have an independent evaluator run the reserved OpenHands holdout once on a
   second Linux/filesystem stack; preserve a failure unchanged.
2. Recruit three workload owners for fail-open shadow pilots using
   `DESIGN_PARTNER.md`; do not optimize to one customer's private trace before
   freezing the metric contract.
3. Measure total storage/replay dollars, not only compression ratios.
4. Add arbitrary-schema and atomic bulk import only when a real adapter needs
   them.
5. Test portability and controlled-cold behavior before broad performance
   wording. Add literal power-loss testing only if the target deployment
   requires that promise.

## Lean boundary

The Lean development remains a design/proof asset, not an automatic proof of
the Rust implementation. Apply a proved invariant to Rust only through a small
specified boundary, differential/property tests, and an explicit refinement
argument. A fresh Lean-to-Rust branch is justified later only if it predicts a
measurable win the current system cannot reach—density, asymptotic edit cost,
or balancing—not as a cleanup exercise before users exist.
