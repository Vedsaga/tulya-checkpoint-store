# Evaluate Tulya on your workload

Tulya is an alpha. The safest useful evaluation mirrors checkpoints beside an
existing authoritative backend, fails open if Tulya is unavailable, and makes
no production read path depend on the result.

## Local API and dashboard

The optional `tulya-local` binary is a convenience evaluator around the same
Rust `CheckpointStore` used by the CLI and benchmark adapter. It owns one
writer, serves a dashboard, and accepts JSON on loopback:

```bash
TULYA_EVAL=$(mktemp -d)
cargo run --release --locked --features local-server --bin tulya-local -- \
  --db "$TULYA_EVAL/store" \
  --import-jsonl examples/sample_history.jsonl
```

Open `http://127.0.0.1:3210`. The server refuses a non-loopback bind unless
`--allow-non-loopback` is supplied. That override does not add authentication
or TLS; it should only be used on an otherwise trusted network. Startup
`--import-jsonl` requires an empty store; use the HTTP import endpoint for an
incremental batch after startup.

### Accepted checkpoint JSONL

Each non-empty line must be one JSON object with exactly these fields:

| Field | Meaning |
| --- | --- |
| `thread_id` | Non-empty logical history identifier. |
| `checkpoint_id` | Non-empty checkpoint identifier, unique within the thread. |
| `checkpoint_no` | Unsigned 32-bit sequence number expected by the adapter. |
| `parent_checkpoint_id` | Earlier checkpoint in the same thread, or `null` for a root. |
| `messages` | Non-empty array containing only values newly appended to the parent. |

Parents must appear before children. Values inside `messages` may be any JSON;
Tulya does not require the `role`/`content` shape used by the sample. Unknown
top-level fields, empty deltas, missing parents, and malformed topology are
rejected.

Batch import is incrementally durable rather than transactional across the
entire request. If an error occurs, the response names the failing line and
the number of earlier checkpoints that remain committed. The request body is
limited to 64 MiB; use direct checkpoint calls or multiple topologically
ordered batches for a larger evaluation.

### HTTP surface

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Live local dashboard. |
| `GET` | `/api/health` | Process status and explicit limitations. |
| `GET` | `/api/stats` | Store, workload-shape, and process-lifetime counters. |
| `GET` | `/api/checkpoints?limit=100` | Recent committed checkpoint metadata. |
| `GET` | `/metrics` | Prometheus text exposition. |
| `POST` | `/api/checkpoints` | Append one checkpoint using the JSONL object schema. |
| `POST` | `/api/import` | Import append-delta JSONL. |
| `POST` | `/api/read` | Reconstruct one checkpoint from `thread_id` and `checkpoint_id`. |
| `POST` | `/api/verify` | Reconstruct and hash-check every committed checkpoint. |
| `POST` | `/api/seal` | Seal all currently committed checkpoints. |

The dashboard intentionally does not calculate “savings.” It shows the sum of
exact logical checkpoint-state lengths and the complete Tulya directory's
allocated bytes as separately labeled facts. An economic claim requires the
same checkpoint set, retention, durability policy, and whole-store accounting
from the existing backend.

The service has no authentication, TLS, authorization, rate limiting,
multi-process coordination, stable network-API guarantee, or persistent
metrics history. Metrics reset when the process restarts. Keep the primary
backend authoritative during evaluation.

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
