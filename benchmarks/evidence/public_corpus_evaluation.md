# Real-world repeated-attempt branch forest evaluation r1 — result

Date: 2026-08-23

Frozen evaluation head: `4cf590f951dd4b0173ed100d4ab09fa4dd19eae9`

Corpus: untouched OpenHands evaluation partition

Corpus SHA-256:
`a931c659530c083933b7da5fd886bcee0068c8c8df3ce57f6aea43fae18df12e`

Decision: **PASS every frozen evaluation threshold on the first r1 run.**

The worktree was clean before and after. All ten retained arms completed and
reconstructed every one of 11,383 canonical checkpoints exactly before and
after reopen. The run accessed neither reserved holdout.

Binary SHA-256:

- Tulya: `877afd0b525dfacacf93406807b93182bf18141bb02786cc1b446e34dfb12ebe`
- normalized SQLite: `97dafd14e4df25d653e689cd09484e6b946ef8fe51cbf609933c431f72595ba8`

## Frozen-threshold outcome

| threshold | observed | result |
|---|---:|---:|
| all exactness/count/reference checks | zero failures | PASS |
| direct LangGraph Delta marginal storage / Tulya >= 10x | 12.38x | PASS |
| normalized SQLite marginal storage / Tulya >= 3x | 6.96x | PASS |
| CAS SQLite marginal storage / Tulya >= 3x | 6.75x | PASS |
| normalized SQLite reopened-read p50 / Tulya >= 20x | 51.24x | PASS |
| CAS SQLite reopened-read p50 / Tulya >= 20x | 77.13x | PASS |
| Tulya peak RSS / normalized SQLite peak RSS <= 2.0x | 1.830x | PASS |

Tulya used 5,492,736 marginal reopened bytes, 90,595,328 B peak RSS,
306,708 ns durable-append p50, and 19,417 ns reopened historical-read p50.

## Required interpretation

This result authorizes the following narrow benchmark claim:

> On a pinned public repeated-attempt agent corpus and predeclared untouched
> evaluation partition, production Tulya exactly preserved the natural branch
> DAG while using 12.38x less marginal reopened storage than direct LangGraph
> SQLite DeltaChannel and 6.75–6.96x less than the tested custom SQLite delta
> baselines, with 51–77x lower historical-read p50 than those custom deltas.

The claim is specific to the frozen workload, implementations, durability
settings and machine. It is not evidence of a 100x end-to-end customer saving,
general semantic memory, a Frame advantage, or universal superiority over
versioned storage. Packed Git was only 3.59x larger and its historical-read p50
was 2.47x slower, although it did not provide one fsynced transaction per
checkpoint in this importer.

## Next gate

Before a production-durability or investor-grade external claim:

1. run the predeclared SIGKILL/process-crash and injected-write-boundary matrix;
2. package a minimal installable backend and direct LangGraph shadow adapter;
3. have a second machine or independent evaluator run a reserved holdout once;
4. publish the raw manifest, commands, hashes, failures and limitations.

Neither holdout is authorized for local tuning or repeated runs.
