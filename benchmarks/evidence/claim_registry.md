# Branch-forest claim registry r1

Date: 2026-08-23

This is the public-claim firewall for the first Tulya wedge.

## Authorized measured claim

Claim ID `TULYA-BF-OH-EVAL-R1`:

> On a pinned public repeated-attempt OpenHands corpus and a predeclared
> untouched evaluation partition, production Tulya exactly preserved 11,383
> unique branch-DAG checkpoints while using 12.38x less marginal reopened
> storage than direct LangGraph SQLite DeltaChannel and 6.75–6.96x less than
> the tested custom SQLite delta baselines, with 51–77x lower reopened
> historical-read p50 than those custom deltas.

Required qualifier: the result is specific to the frozen workload,
implementations, durability settings, machine and warm/reopened read protocol.

Primary evidence:

- result note: `REAL_WORLD_REPEATED_ATTEMPT_BRANCH_FOREST_EVALUATION_R1_RESULT.md`;
- raw summary SHA-256:
  `cba877506091b1b6720b5cdad71b1f1d3ae0d74eff72cd33fb7d5b59d4462607`;
- evaluation corpus SHA-256:
  `a931c659530c083933b7da5fd886bcee0068c8c8df3ce57f6aea43fae18df12e`;
- frozen evaluation code: `4cf590f951dd4b0173ed100d4ab09fa4dd19eae9`.

## Supporting engineering claims

| claim ID | safe statement | state |
|---|---|---|
| `TULYA-BF-TWO-FAMILY-R1` | Zero reconstruction failures across the complete ten-arm SWE-agent and OpenHands engineering matrices. | measured |
| `TULYA-CRASH-PROCESS-R2` | Current production store body passed 32 forced process-exit cases across two seal generations, plus append after reclaim/reopen. | measured |
| `TULYA-PACKAGE-R1` | Root Cargo package is self-contained after separating two private Kramanvaya research binaries; `cargo package --locked --no-default-features` passes. | measured |
| `TULYA-SHADOW-R0` | A real LangGraph `StateGraph` with SQLite authoritative can shadow a root and two siblings into Tulya, verify, seal and verify again. | integration smoke |
| `TULYA-EXPORT-R0` | Append-only message stores export and import deterministically with byte-identical re-export in the three-checkpoint branch fixture. | integration smoke |

## Important losses and boundaries

- Packed Git was only 3.59x larger and 2.47x slower on reopened historical-read
  p50. It is the strongest storage-only warning, although the importer did not
  fsync one transaction per checkpoint.
- The first SWE evaluation missed the frozen RAM threshold at 2.035x. It remains
  preserved; the adapter memory retention bug was diagnosed before r1.
- The public high-level message API reconstructs the full parent to derive its
  hash and length. It is strict but O(history); the benchmark adapter used the
  optimized production transaction encoder directly.
- The Python shadow adapter starts one CLI process per mirrored checkpoint. Its
  end-to-end latency is not a product latency claim.

## Prohibited claims

Do not say:

- `100x` lower end-to-end customer cost;
- universally smaller than Git, CAS, databases or every optimized history;
- literal power-loss/device-cache safety;
- multi-writer, distributed or network-service readiness;
- full LangGraph saver replacement;
- Kriya-Karaka Frame, semantic-memory, bug-finding or model-quality benefit;
- customer ROI before a workload owner supplies its geometry and economics.

## Fundraising language

The honest sentence is:

> We have a falsified-and-reproduced systems wedge, not yet a proven business:
> exact online branch history occupies a materially better combined durability,
> retained-storage and historical-read point than the tested agent-framework
> and custom delta baselines. We are seeking design partners to measure whether
> that frontier removes real retained-state or replay cost.
