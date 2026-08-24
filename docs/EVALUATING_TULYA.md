# Evaluate Tulya on your workload

Tulya is an alpha. The safest useful evaluation mirrors checkpoints beside an
existing authoritative backend, fails open if Tulya is unavailable, and makes
no production read path depend on the result.

The first question is deliberately narrow:

> Does exact branch history reduce retained-state or replay cost in a workload
> someone already pays to run?

## A suitable pilot

A strong design-partner workload has long histories, repeated prefixes,
retries or forks from older checkpoints, and a real need to reconstruct
historical state exactly. Embedded single-writer storage must be acceptable for
the shadow deployment.

The current LangGraph adapter mirrors one append-only message channel. It is
not suitable for evaluating arbitrary channel mutation, pending writes,
distributed writers, or drop-in saver replacement.

Run the shadow for a pre-agreed interval—two weeks is a useful default—and
freeze the measurement contract before observing Tulya's result.

## Freeze before the run

- Workload owner and current backend/version:
- Tulya commit and crate version:
- Machine, filesystem, and container labels:
- Start/end time and retention interval:
- Checkpoint count, branch count, and maximum fork fan-out:
- Logical historical bytes retained:
- Privacy, deletion, and rollback requirements:
- Minimum agreed economic win:
- Maximum allowed shadow overhead:

## Same-work accounting

- Primary whole-store allocated bytes, including WAL and sidecars:
- Tulya whole-store allocated bytes, including hot/sealed coexistence:
- Primary and Tulya durable-write p50/p95/p99:
- Primary and Tulya historical-read p50/p95/p99:
- Primary and Tulya peak RSS:
- Maintenance wall time and temporary peak bytes:
- Replay or recomputation events avoided:
- Measured compute or operator cost avoided:
- Tulya compute, storage, and operational cost added:

Do not compare relation-only bytes with a complete store directory, different
retention intervals, different checkpoint sets, or different durability
policies without labeling the difference.

## Correctness and recovery

- Mirrored checkpoints:
- Byte or hash mismatches:
- Process restarts exercised:
- Exact continuation after restart:
- Independent `tulya-checkpoint fsck` report attached:
- Primary remained authoritative throughout:

The pilot fails correctness if any mirrored checkpoint differs. Storage or
latency savings cannot compensate for an exactness failure.

## Decision

- Pre-agreed threshold passed:
- Net measured saving:
- Observed limitations or regressions:
- Workload owner will continue, stop, or extend:
- Quote authorized for public use, including exact wording and scope:

The workload owner—not the Tulya author—must confirm primary-backend
accounting, net savings, and any public quotation. A comparator win or a failed
pilot is still useful evidence and must not be silently discarded.

Do not translate a storage ratio into end-to-end savings unless the workload
owner's measured bills or resource costs support that calculation.

Copy this template into a private pilot record. Do not commit customer names,
contacts, traces, or confidential measurements to the public repository. To
explore a design-partner evaluation, open a GitHub issue containing only a
non-sensitive description of the workload shape.
