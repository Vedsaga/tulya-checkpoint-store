#!/usr/bin/env python3
"""Packed Git retained-history baseline for the natural branch forest.

This is a storage/read comparator, not a durable-append comparator. It bulk
imports one full ``state.json`` tree per logical checkpoint, preserves the
frozen parent DAG, retains every attempt tip as a ref, aggressively packs the
repository, and verifies every historical state before and after packing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, BinaryIO


AUTHOR = b"Tulya Benchmark <benchmark@example.invalid>"
BASE_TIMESTAMP = 1_700_000_000


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def run(
    command: list[str], cwd: Path, *, input_bytes: bytes | None = None
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        command,
        cwd=cwd,
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )


def run_text(command: list[str], cwd: Path) -> str:
    return run(command, cwd).stdout.decode("utf-8", errors="replace").strip()


def allocated_bytes(path: Path) -> int:
    return int(getattr(path.stat(), "st_blocks", 0)) * 512


def storage(root: Path) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_file():
            files.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "file_length_bytes": path.stat().st_size,
                    "allocated_bytes": allocated_bytes(path),
                }
            )
    return {
        "file_count": len(files),
        "file_length_bytes": sum(row["file_length_bytes"] for row in files),
        "allocated_bytes": sum(row["allocated_bytes"] for row in files),
        "files": files,
    }


def percentile(values: list[int], fraction: float) -> int:
    ordered = sorted(values)
    if not ordered:
        return 0
    if len(ordered) == 1:
        return ordered[0]
    position = fraction * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return int(round(ordered[lower] * (1 - weight) + ordered[upper] * weight))


def latency_stats(values: list[int]) -> dict[str, int]:
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "mean_ns": int(round(sum(values) / len(values))),
        "p50_ns": percentile(values, 0.50),
        "p95_ns": percentile(values, 0.95),
        "p99_ns": percentile(values, 0.99),
        "max_ns": max(values),
    }


def load_corpus(path: Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        raise RuntimeError("empty branch-forest corpus")
    return rows


def expected_state(nodes: dict[str, dict[str, Any]], checkpoint_id: str) -> bytes:
    messages: list[Any] = []
    cursor: str | None = checkpoint_id
    while cursor is not None:
        node = nodes[cursor]
        operations = node["operations"]
        if len(operations) != 1 or operations[0].get("op") != "append_message":
            raise RuntimeError(f"unsupported operation at {cursor}")
        messages.append(operations[0]["value"])
        cursor = node["parent_checkpoint_id"]
    messages.reverse()
    return canonical_json_bytes({"messages": messages})


def write_data(stream: BinaryIO, data: bytes) -> None:
    stream.write(f"data {len(data)}\n".encode())
    stream.write(data)
    stream.write(b"\n")


def fast_import(
    repo: Path, corpus: list[dict[str, Any]], marks_path: Path
) -> tuple[dict[str, int], int, int]:
    process = subprocess.Popen(
        [
            "git",
            "fast-import",
            "--quiet",
            "--date-format=raw",
            f"--export-marks={marks_path}",
        ],
        cwd=repo,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    if process.stdin is None or process.stderr is None:
        raise RuntimeError("failed to open git fast-import")
    mark_by_checkpoint: dict[str, int] = {}
    checkpoint_count = 0
    attempt_count = 0
    output = process.stdin
    try:
        for instance in corpus:
            nodes = {
                str(node["checkpoint_id"]): node for node in instance["nodes"]
            }
            for node in instance["nodes"]:
                checkpoint_id = str(node["checkpoint_id"])
                checkpoint_count += 1
                mark = checkpoint_count
                mark_by_checkpoint[checkpoint_id] = mark
                raw = expected_state(nodes, checkpoint_id)
                commit_message = canonical_json_bytes(
                    {
                        "canonical_checkpoint_id": checkpoint_id,
                        "instance_id": instance["instance_id"],
                    }
                )
                timestamp = BASE_TIMESTAMP + mark
                output.write(b"commit refs/heads/import\n")
                output.write(f"mark :{mark}\n".encode())
                output.write(b"author " + AUTHOR + f" {timestamp} +0000\n".encode())
                output.write(
                    b"committer " + AUTHOR + f" {timestamp} +0000\n".encode()
                )
                write_data(output, commit_message)
                parent = node["parent_checkpoint_id"]
                if parent is not None:
                    output.write(f"from :{mark_by_checkpoint[str(parent)]}\n".encode())
                output.write(b"deleteall\nM 100644 inline state.json\n")
                write_data(output, raw)
                output.write(b"\n")

        for instance in corpus:
            for attempt in instance["attempts"]:
                path = attempt["checkpoint_path"]
                mark = mark_by_checkpoint[str(path[-1])]
                output.write(f"reset refs/attempts/{attempt['attempt_id']}\n".encode())
                output.write(f"from :{mark}\n\n".encode())
                attempt_count += 1
        output.write(b"reset refs/heads/import\n\ndone\n")
        output.flush()
        output.close()
        return_code = process.wait()
        stderr = process.stderr.read().decode("utf-8", errors="replace")
        if return_code != 0:
            raise RuntimeError(f"git fast-import exited {return_code}: {stderr}")
    except BaseException:
        process.kill()
        raise
    return mark_by_checkpoint, checkpoint_count, attempt_count


def parse_marks(path: Path) -> dict[int, str]:
    marks: dict[int, str] = {}
    for line in path.read_text().splitlines():
        mark, object_id = line.split()
        marks[int(mark.removeprefix(":"))] = object_id
    return marks


def verify(
    repo: Path,
    corpus: list[dict[str, Any]],
    mark_by_checkpoint: dict[str, int],
    object_by_mark: dict[int, str],
) -> dict[str, Any]:
    process = subprocess.Popen(
        ["git", "cat-file", "--batch"],
        cwd=repo,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdin is None or process.stdout is None or process.stderr is None:
        raise RuntimeError("failed to open git cat-file batch")
    failures = 0
    reads: list[int] = []
    checkpoint_count = 0
    try:
        for instance in corpus:
            for node in instance["nodes"]:
                checkpoint_id = str(node["checkpoint_id"])
                commit = object_by_mark[mark_by_checkpoint[checkpoint_id]]
                started = time.perf_counter_ns()
                process.stdin.write(f"{commit}:state.json\n".encode())
                process.stdin.flush()
                header = process.stdout.readline().decode("ascii", errors="strict").strip()
                parts = header.split()
                if len(parts) != 3 or parts[1] != "blob":
                    raise RuntimeError(f"unexpected git cat-file header: {header}")
                size = int(parts[2])
                raw = process.stdout.read(size)
                if process.stdout.read(1) != b"\n":
                    raise RuntimeError("git cat-file record missing delimiter")
                reads.append(time.perf_counter_ns() - started)
                checkpoint_count += 1
                if (
                    len(raw) != int(node["logical_state_len"])
                    or hashlib.sha256(raw).hexdigest()
                    != node["logical_state_sha256"]
                ):
                    failures += 1
        process.stdin.close()
        return_code = process.wait()
        stderr = process.stderr.read().decode("utf-8", errors="replace")
        if return_code != 0:
            raise RuntimeError(f"git cat-file exited {return_code}: {stderr}")
    except BaseException:
        process.kill()
        raise
    return {
        "checkpoint_count": checkpoint_count,
        "state_failures": failures,
        "exact": failures == 0,
        "read_latency": latency_stats(reads),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--repo", type=Path, required=True)
    args = parser.parse_args()

    corpus = load_corpus(args.corpus.resolve())
    repo = args.repo.resolve()
    if repo.exists():
        shutil.rmtree(repo)
    repo.mkdir(parents=True)
    run(["git", "init", "--bare", "--quiet", "."], repo)
    run(["git", "config", "core.logAllRefUpdates", "false"], repo)
    empty = storage(repo)
    marks_path = repo.parent / f".{repo.name}-marks"
    if marks_path.exists():
        marks_path.unlink()

    import_started = time.perf_counter_ns()
    mark_by_checkpoint, checkpoint_count, attempt_count = fast_import(
        repo, corpus, marks_path
    )
    import_duration_ns = time.perf_counter_ns() - import_started
    object_by_mark = parse_marks(marks_path)
    marks_path.unlink()
    active = storage(repo)
    active_verification = verify(
        repo, corpus, mark_by_checkpoint, object_by_mark
    )
    ref_count = int(
        run_text(
            ["git", "for-each-ref", "--format=%(refname)", "refs/attempts"], repo
        ).count("\n")
        + (1 if attempt_count else 0)
    )

    maintenance_started = time.perf_counter_ns()
    run(["git", "pack-refs", "--all", "--prune"], repo)
    run(["git", "reflog", "expire", "--expire=now", "--all"], repo)
    run(["git", "gc", "--aggressive", "--prune=now"], repo)
    maintenance_duration_ns = time.perf_counter_ns() - maintenance_started
    run(["git", "fsck", "--full", "--strict"], repo)
    packed = storage(repo)
    reopened_verification = verify(
        repo, corpus, mark_by_checkpoint, object_by_mark
    )

    passed = (
        active_verification["exact"]
        and reopened_verification["exact"]
        and len(object_by_mark) == checkpoint_count
        and ref_count == attempt_count
    )
    result = {
        "format_version": 1,
        "contract": "TULYA_BRANCH_FOREST_BENCHMARK_V1",
        "backend": "git-full-snapshot-aggressive-pack",
        "checkpoint_node_count": checkpoint_count,
        "attempt_ref_count": attempt_count,
        "pass": passed,
        "claims": {
            "performance_claim": False,
            "durable_append_claim": False,
            "storage_advantage_claim": False,
            "holdout_accessed": args.corpus.name == "holdout.jsonl",
        },
        "durability": {
            "mode": "bulk fast-import; not one durable transaction per checkpoint",
        },
        "versions": {"git": run_text(["git", "--version"], repo)},
        "latency": {
            "bulk_import_duration_ns": import_duration_ns,
            "maintenance_duration_ns": maintenance_duration_ns,
        },
        "exactness": {
            "active": active_verification,
            "reopened_after_aggressive_pack": reopened_verification,
        },
        "storage": {"empty": empty, "active": active, "reopened": packed},
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
