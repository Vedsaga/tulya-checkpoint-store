# Shadow-pilot scorecard

Use this document unchanged for every design partner. Fill the contract before
the pilot starts, then attach machine-readable measurements and commands. A
blank field is not a pass.

## Frozen before the run

- Workload owner and contact:
- Primary backend and exact version:
- Tulya commit and crate version:
- Machine/filesystem/container labels:
- Start/end time and retention interval:
- Privacy/deletion requirements:
- Rollback trigger:
- Minimum agreed economic win:
- Maximum allowed shadow overhead:

## Same-work accounting

- Logical checkpoint count:
- Unique branches and maximum fork fan-out:
- Logical historical bytes retained:
- Primary whole-store allocated bytes, including WAL and sidecars:
- Tulya whole-store allocated bytes, including hot/sealed coexistence:
- Primary and Tulya durable-write p50/p95/p99:
- Primary and Tulya historical-read p50/p95/p99:
- Primary and Tulya peak RSS:
- Maintenance wall time and temporary peak bytes:
- Replay/recomputation events avoided:
- Measured compute or operator dollars avoided:
- Tulya compute, storage, and operational dollars added:

## Correctness and recovery

- Mirrored checkpoints:
- Byte/hash mismatches:
- Process restarts exercised:
- Exact continuation after restart:
- Independent `tulya-checkpoint fsck` report attached:
- Primary remained authoritative throughout:

## Decision

- Pre-agreed threshold passed:
- Net measured saving:
- Observed limitations or regressions:
- Workload owner will continue, stop, or extend:
- Quote authorized for public use (exact text and scope):

Do not translate storage ratios into end-to-end savings unless the workload
owner's bills or measured resource costs support that calculation.
