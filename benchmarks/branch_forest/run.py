#!/usr/bin/env python3
"""Run the reproducible Tulya branch-forest comparator matrix."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


TULYA_BIN = "branch_forest_tulya"
SQLITE_BIN = "branch_forest_sqlite"
LANGGRAPH_SCRIPT = "langgraph_sqlite.py"
SQLITE_VARIANTS_SCRIPT = "sqlite_variants.py"
GIT_SCRIPT = "git_history.py"
POSTGRES_SCRIPT = "postgres.py"
RESERVED_HOLDOUT_SHA256 = {
    "757298cb58495825032afb0e236f626592af06c3af3f46224c6f269be5f8e548",
    "e1008af40c89525e549c1137534e2d8e53e559e4c4bede465d078af5e900b472",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def git_text(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout.strip()


def git_metadata(repo: Path) -> dict[str, Any]:
    probe = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if probe.returncode != 0:
        return {"available": False, "head": None, "status": []}
    root = Path(probe.stdout.strip()).resolve()
    if root != repo.resolve():
        return {
            "available": False,
            "head": None,
            "status": [],
            "ignored_parent_worktree": str(root),
        }
    status = git_text(repo, "status", "--short")
    return {
        "available": True,
        "root": str(root),
        "head": git_text(repo, "rev-parse", "HEAD"),
        "status": status.splitlines() if status else [],
    }


def source_tree_identity(repo: Path) -> dict[str, Any]:
    excluded_roots = {
        ".git",
        "target",
        "benchmark-results",
        "data",
        "results",
    }
    files = []
    for path in repo.rglob("*"):
        relative = path.relative_to(repo)
        if not path.is_file() or relative.parts[0] in excluded_roots:
            continue
        if "__pycache__" in relative.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        files.append((relative, path))
    digest = hashlib.sha256()
    total_bytes = 0
    for relative, path in sorted(files, key=lambda item: item[0].as_posix()):
        relative_bytes = relative.as_posix().encode("utf-8")
        digest.update(len(relative_bytes).to_bytes(8, "big"))
        digest.update(relative_bytes)
        size = path.stat().st_size
        digest.update(size.to_bytes(8, "big"))
        total_bytes += size
        with path.open("rb") as source:
            for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
                digest.update(block)
    return {
        "sha256": digest.hexdigest(),
        "file_count": len(files),
        "total_bytes": total_bytes,
    }


def rss_bytes(pid: int) -> int:
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        return 0
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return 0


def monitor(pid: int, stop: threading.Event, values: list[int]) -> None:
    peak = 0
    while not stop.wait(0.01):
        peak = max(peak, rss_bytes(pid))
    values.append(max(peak, rss_bytes(pid)))


def run_arm(
    *,
    name: str,
    command: list[str],
    cwd: Path,
    output_dir: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    started_ns = time.monotonic_ns()
    process = subprocess.Popen(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stop = threading.Event()
    peaks: list[int] = []
    watcher = threading.Thread(target=monitor, args=(process.pid, stop, peaks), daemon=True)
    watcher.start()
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
    finally:
        stop.set()
        watcher.join(timeout=2)
    (output_dir / f"{name}.stdout").write_text(stdout)
    (output_dir / f"{name}.stderr").write_text(stderr)
    values = [line for line in stdout.splitlines() if line.strip()]
    parsed: dict[str, Any] = {}
    if values:
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = json.loads(values[-1])
    return {
        "command": command,
        "returncode": process.returncode,
        "wall_elapsed_ns": time.monotonic_ns() - started_ns,
        "peak_rss_bytes": peaks[0] if peaks else 0,
        "result": parsed,
    }


def nested(value: dict[str, Any], *keys: str) -> Any:
    current: Any = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def ratio(numerator: Any, denominator: Any) -> float | None:
    try:
        denominator_number = float(denominator)
        if denominator_number == 0:
            return None
        return float(numerator) / denominator_number
    except (TypeError, ValueError):
        return None


def at_least(value: float | None, minimum: float) -> bool:
    return value is not None and value >= minimum


def at_most(value: float | None, maximum: float) -> bool:
    return value is not None and value <= maximum


def filesystem_info(path: Path) -> dict[str, Any]:
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    completed = subprocess.run(
        ["findmnt", "-T", str(probe), "-n", "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    fields = completed.stdout.strip().split(maxsplit=3)
    if completed.returncode != 0 or len(fields) < 3:
        return {
            "available": False,
            "error": completed.stderr.strip() or "findmnt returned no filesystem",
        }
    return {
        "available": True,
        "target": fields[0],
        "source": fields[1],
        "type": fields[2],
        "options": fields[3] if len(fields) == 4 else "",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results/branch-forest"),
    )
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument(
        "--allow-volatile-filesystem",
        action="store_true",
        help="Permit tmpfs/ramfs diagnostics; their latency is never claim-bearing",
    )
    parser.add_argument(
        "--authorize-holdout",
        action="store_true",
        help="Allow exactly one reserved holdout run after the candidate is frozen",
    )
    parser.add_argument(
        "--expected-corpus-sha256",
        help="Required exact corpus digest for an authorized holdout run",
    )
    parser.add_argument(
        "--evaluator",
        help="Required independent evaluator name/handle for an authorized holdout run",
    )
    parser.add_argument(
        "--machine-label",
        help="Independent-machine label recorded in an authorized holdout result",
    )
    args = parser.parse_args()

    bench = Path(__file__).resolve().parent
    repo = bench.parent.parent
    corpus = args.corpus.resolve()
    corpus_sha256 = sha256_file(corpus)
    is_holdout = (
        corpus.name == "holdout.jsonl" or corpus_sha256 in RESERVED_HOLDOUT_SHA256
    )
    if is_holdout and not args.authorize_holdout:
        raise RuntimeError(
            "reserved holdout requires --authorize-holdout, its frozen SHA-256, "
            "and an independent evaluator"
        )
    if is_holdout:
        if corpus.name != "holdout.jsonl":
            raise RuntimeError(
                "the reserved holdout digest was detected under a different name; "
                "restore the canonical holdout.jsonl filename before authorization"
            )
        if args.allow_dirty:
            raise RuntimeError("authorized holdout cannot use --allow-dirty")
        if not args.expected_corpus_sha256 or corpus_sha256 != args.expected_corpus_sha256:
            raise RuntimeError("authorized holdout corpus SHA-256 is absent or mismatched")
        if not args.evaluator or not args.machine_label:
            raise RuntimeError(
                "authorized holdout requires --evaluator and --machine-label"
            )
    elif args.authorize_holdout:
        raise RuntimeError("--authorize-holdout is valid only for holdout.jsonl")
    output_dir = args.output_dir.resolve()
    output_filesystem = filesystem_info(output_dir.parent)
    volatile_filesystem = output_filesystem.get("type") in {"tmpfs", "ramfs"}
    if volatile_filesystem and not args.allow_volatile_filesystem:
        raise RuntimeError(
            "benchmark output is on a volatile filesystem; choose durable storage or "
            "pass --allow-volatile-filesystem for a non-claim diagnostic"
        )
    if is_holdout and volatile_filesystem:
        raise RuntimeError("an authorized holdout cannot run on tmpfs or ramfs")
    git_before = git_metadata(repo)
    source_before = source_tree_identity(repo)
    if git_before["status"] and not args.allow_dirty:
        raise RuntimeError("a claim-bearing reproduction requires a clean worktree")
    if is_holdout and not git_before["available"]:
        raise RuntimeError(
            "an authorized holdout requires a clean, commit-addressable Git worktree"
        )
    if output_dir.exists():
        raise RuntimeError("output directory already exists; choose a new path")
    output_dir.mkdir(parents=True)

    build = subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--locked",
            "--example",
            TULYA_BIN,
            "--example",
            SQLITE_BIN,
        ],
        cwd=repo,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    (output_dir / "build.stdout").write_text(build.stdout)
    (output_dir / "build.stderr").write_text(build.stderr)
    if build.returncode != 0:
        raise RuntimeError("branch-forest adapters did not build")

    binary_root = repo / "target/release/examples"
    tulya = run_arm(
        name="tulya",
        command=[
            str(binary_root / TULYA_BIN),
            "--db",
            str(output_dir / "tulya-store"),
            "--corpus",
            str(corpus),
        ],
        cwd=bench,
        output_dir=output_dir,
        timeout_seconds=args.timeout_seconds,
    )
    sqlite = run_arm(
        name="sqlite",
        command=[
            str(binary_root / SQLITE_BIN),
            "--db",
            str(output_dir / "sqlite-store"),
            "--corpus",
            str(corpus),
        ],
        cwd=bench,
        output_dir=output_dir,
        timeout_seconds=args.timeout_seconds,
    )
    langgraph_full = run_arm(
        name="langgraph_full",
        command=[
            sys.executable,
            str(bench / LANGGRAPH_SCRIPT),
            "--mode",
            "full",
            "--db",
            str(output_dir / "langgraph-full-store"),
            "--corpus",
            str(corpus),
        ],
        cwd=bench,
        output_dir=output_dir,
        timeout_seconds=args.timeout_seconds,
    )
    langgraph_delta = run_arm(
        name="langgraph_delta",
        command=[
            sys.executable,
            str(bench / LANGGRAPH_SCRIPT),
            "--mode",
            "delta",
            "--db",
            str(output_dir / "langgraph-delta-store"),
            "--corpus",
            str(corpus),
        ],
        cwd=bench,
        output_dir=output_dir,
        timeout_seconds=args.timeout_seconds,
    )
    variant_arms: dict[str, dict[str, Any]] = {}
    for mode in ("cas-delta", "snapshot-zstd", "snapshot-raw"):
        arm_name = mode.replace("-", "_")
        variant_arms[arm_name] = run_arm(
            name=arm_name,
            command=[
                sys.executable,
                str(bench / SQLITE_VARIANTS_SCRIPT),
                "--mode",
                mode,
                "--db",
                str(output_dir / f"{mode}-store"),
                "--corpus",
                str(corpus),
            ],
            cwd=bench,
            output_dir=output_dir,
            timeout_seconds=args.timeout_seconds,
        )
    git = run_arm(
        name="git",
        command=[
            sys.executable,
            str(bench / GIT_SCRIPT),
            "--repo",
            str(output_dir / "git-store"),
            "--corpus",
            str(corpus),
        ],
        cwd=bench,
        output_dir=output_dir,
        timeout_seconds=args.timeout_seconds,
    )
    postgres_full = run_arm(
        name="postgres_full",
        command=[
            sys.executable,
            str(bench / POSTGRES_SCRIPT),
            "--mode",
            "full",
            "--corpus",
            str(corpus),
        ],
        cwd=bench,
        output_dir=output_dir,
        timeout_seconds=args.timeout_seconds,
    )
    tulya_result = tulya["result"]
    sqlite_result = sqlite["result"]
    langgraph_full_result = langgraph_full["result"]
    langgraph_delta_result = langgraph_delta["result"]
    variant_results = {
        name: arm["result"] for name, arm in variant_arms.items()
    }
    git_result = git["result"]
    postgres_full_result = postgres_full["result"]
    tulya_total = nested(tulya_result, "storage", "reopened", "allocated_bytes")
    tulya_empty = nested(tulya_result, "storage", "empty", "allocated_bytes")
    sqlite_total = nested(sqlite_result, "storage", "reopened", "allocated_bytes")
    sqlite_empty = nested(sqlite_result, "storage", "empty", "allocated_bytes")
    tulya_marginal = int(tulya_total or 0) - int(tulya_empty or 0)
    sqlite_marginal = int(sqlite_total or 0) - int(sqlite_empty or 0)
    langgraph_full_total = nested(
        langgraph_full_result, "storage", "reopened", "allocated_bytes"
    )
    langgraph_full_empty = nested(
        langgraph_full_result, "storage", "empty", "allocated_bytes"
    )
    langgraph_delta_total = nested(
        langgraph_delta_result, "storage", "reopened", "allocated_bytes"
    )
    langgraph_delta_empty = nested(
        langgraph_delta_result, "storage", "empty", "allocated_bytes"
    )
    langgraph_full_marginal = int(langgraph_full_total or 0) - int(
        langgraph_full_empty or 0
    )
    langgraph_delta_marginal = int(langgraph_delta_total or 0) - int(
        langgraph_delta_empty or 0
    )
    variant_marginals: dict[str, int] = {}
    for name, result in variant_results.items():
        total = nested(result, "storage", "reopened", "allocated_bytes")
        empty_size = nested(result, "storage", "empty", "allocated_bytes")
        variant_marginals[name] = int(total or 0) - int(empty_size or 0)
    git_total = nested(git_result, "storage", "reopened", "allocated_bytes")
    git_empty = nested(git_result, "storage", "empty", "allocated_bytes")
    git_marginal = int(git_total or 0) - int(git_empty or 0)
    postgres_full_total = nested(
        postgres_full_result, "storage", "reopened", "allocated_bytes"
    )
    postgres_full_empty = nested(
        postgres_full_result, "storage", "empty", "allocated_bytes"
    )
    postgres_full_marginal = int(postgres_full_total or 0) - int(
        postgres_full_empty or 0
    )
    all_results = (
        sqlite_result,
        langgraph_full_result,
        langgraph_delta_result,
        git_result,
        postgres_full_result,
        *variant_results.values(),
    )
    counts_equal = all(
        result.get("checkpoint_node_count")
        == tulya_result.get("checkpoint_node_count")
        and result.get("attempt_ref_count") == tulya_result.get("attempt_ref_count")
        for result in all_results
    )
    both_exact = (
        tulya["returncode"] == 0
        and sqlite["returncode"] == 0
        and langgraph_full["returncode"] == 0
        and langgraph_delta["returncode"] == 0
        and git["returncode"] == 0
        and postgres_full["returncode"] == 0
        and tulya_result.get("pass") is True
        and sqlite_result.get("pass") is True
        and langgraph_full_result.get("pass") is True
        and langgraph_delta_result.get("pass") is True
        and git_result.get("pass") is True
        and postgres_full_result.get("pass") is True
        and all(
            arm["returncode"] == 0 and variant_results[name].get("pass") is True
            for name, arm in variant_arms.items()
        )
        and all(
            nested(result, "claims", "holdout_accessed") is is_holdout
            for result in (tulya_result, *all_results)
        )
        and counts_equal
    )
    tulya_read_p50 = nested(
        tulya_result, "exactness", "reopened", "read_latency", "p50_ns"
    )
    langgraph_delta_storage_ratio = ratio(langgraph_delta_marginal, tulya_marginal)
    sqlite_storage_ratio = ratio(sqlite_marginal, tulya_marginal)
    cas_storage_ratio = ratio(variant_marginals["cas_delta"], tulya_marginal)
    sqlite_read_ratio = ratio(
        nested(sqlite_result, "exactness", "reopened", "read_latency", "p50_ns"),
        tulya_read_p50,
    )
    cas_read_ratio = ratio(
        nested(
            variant_results["cas_delta"],
            "exactness",
            "reopened",
            "read_latency",
            "p50_ns",
        ),
        tulya_read_p50,
    )
    tulya_sqlite_rss_ratio = ratio(
        tulya.get("peak_rss_bytes"), sqlite.get("peak_rss_bytes")
    )
    frozen_thresholds = {
        "all_exact": both_exact,
        "langgraph_delta_storage_at_least_10x": at_least(
            langgraph_delta_storage_ratio, 10.0
        ),
        "sqlite_storage_at_least_3x": at_least(sqlite_storage_ratio, 3.0),
        "cas_storage_at_least_3x": at_least(cas_storage_ratio, 3.0),
        "sqlite_read_at_least_20x": at_least(sqlite_read_ratio, 20.0),
        "cas_read_at_least_20x": at_least(cas_read_ratio, 20.0),
        "tulya_sqlite_peak_rss_at_most_2x": at_most(
            tulya_sqlite_rss_ratio, 2.0
        ),
    }
    frozen_thresholds_pass = all(frozen_thresholds.values())
    campaign_pass = both_exact and (not is_holdout or frozen_thresholds_pass)
    summary = {
        "format_version": 1,
        "contract": "TULYA_BRANCH_FOREST_BENCHMARK_V1",
        "phase": (
            "independent-holdout"
            if is_holdout
            else "adapter-diagnostic"
            if args.allow_dirty
            else "packaged-source-diagnostic"
            if not git_before["available"]
            else "clean-reproduction"
        ),
        "pass": campaign_pass,
        "performance_claim": (
            is_holdout and frozen_thresholds_pass and not volatile_filesystem
        ),
        "storage_advantage_claim": (
            is_holdout and frozen_thresholds_pass and not volatile_filesystem
        ),
        "holdout_accessed": is_holdout,
        "holdout_authorization": {
            "authorized": is_holdout and args.authorize_holdout,
            "expected_corpus_sha256": args.expected_corpus_sha256,
            "evaluator": args.evaluator,
            "machine_label": args.machine_label,
            "frozen_thresholds": frozen_thresholds,
            "frozen_thresholds_pass": frozen_thresholds_pass,
        },
        "corpus": {
            "path": str(corpus),
            "size_bytes": corpus.stat().st_size,
            "sha256": corpus_sha256,
        },
        "arms": {
            "tulya": tulya,
            "sqlite": sqlite,
            "langgraph_full": langgraph_full,
            "langgraph_delta": langgraph_delta,
            "git": git,
            "postgres_full": postgres_full,
            **variant_arms,
        },
        "comparison": {
            "counts_equal": counts_equal,
            "tulya_marginal_reopened_allocated_bytes": tulya_marginal,
            "sqlite_marginal_reopened_allocated_bytes": sqlite_marginal,
            "sqlite_over_tulya_marginal_storage_ratio": ratio(
                sqlite_marginal, tulya_marginal
            ),
            "langgraph_full_marginal_reopened_allocated_bytes": langgraph_full_marginal,
            "langgraph_delta_marginal_reopened_allocated_bytes": langgraph_delta_marginal,
            "langgraph_full_over_tulya_marginal_storage_ratio": ratio(
                langgraph_full_marginal, tulya_marginal
            ),
            "langgraph_delta_over_tulya_marginal_storage_ratio": ratio(
                langgraph_delta_marginal, tulya_marginal
            ),
            "git_marginal_reopened_allocated_bytes": git_marginal,
            "git_over_tulya_marginal_storage_ratio": ratio(
                git_marginal, tulya_marginal
            ),
            "git_over_tulya_reopened_read_p50_ratio": ratio(
                nested(
                    git_result,
                    "exactness",
                    "reopened_after_aggressive_pack",
                    "read_latency",
                    "p50_ns",
                ),
                nested(
                    tulya_result,
                    "exactness",
                    "reopened",
                    "read_latency",
                    "p50_ns",
                ),
            ),
            "postgres_full_relation_marginal_allocated_bytes": postgres_full_marginal,
            "postgres_full_database_growth_bytes": (
                int(
                    nested(
                        postgres_full_result,
                        "storage",
                        "database_size_final_bytes",
                    )
                    or 0
                )
                - int(
                    nested(
                        postgres_full_result,
                        "storage",
                        "database_size_baseline_bytes",
                    )
                    or 0
                )
            ),
            "postgres_full_wal_generated_workload_bytes": nested(
                postgres_full_result,
                "storage",
                "wal_generated_workload_bytes",
            ),
            "postgres_storage_ratio_comparable_to_tulya": False,
            "postgres_full_over_tulya_durable_append_p50_ratio": ratio(
                nested(
                    postgres_full_result,
                    "latency",
                    "durable_append",
                    "p50_ns",
                ),
                nested(
                    tulya_result,
                    "latency",
                    "durable_append",
                    "p50_ns",
                ),
            ),
            "postgres_full_over_tulya_reopened_read_p50_ratio": ratio(
                nested(
                    postgres_full_result,
                    "exactness",
                    "reopened",
                    "read_latency",
                    "p50_ns",
                ),
                nested(
                    tulya_result,
                    "exactness",
                    "reopened",
                    "read_latency",
                    "p50_ns",
                ),
            ),
            **{
                f"{name}_marginal_reopened_allocated_bytes": marginal
                for name, marginal in variant_marginals.items()
            },
            **{
                f"{name}_over_tulya_marginal_storage_ratio": ratio(
                    marginal, tulya_marginal
                )
                for name, marginal in variant_marginals.items()
            },
            "sqlite_over_tulya_total_storage_ratio": ratio(sqlite_total, tulya_total),
            "sqlite_over_tulya_durable_append_p50_ratio": ratio(
                nested(sqlite_result, "latency", "durable_append", "p50_ns"),
                nested(tulya_result, "latency", "durable_append", "p50_ns"),
            ),
            "sqlite_over_tulya_reopened_read_p50_ratio": ratio(
                nested(sqlite_result, "exactness", "reopened", "read_latency", "p50_ns"),
                nested(tulya_result, "exactness", "reopened", "read_latency", "p50_ns"),
            ),
            "tulya_over_sqlite_peak_rss_ratio": ratio(
                tulya.get("peak_rss_bytes"), sqlite.get("peak_rss_bytes")
            ),
            "tulya_over_langgraph_full_peak_rss_ratio": ratio(
                tulya.get("peak_rss_bytes"), langgraph_full.get("peak_rss_bytes")
            ),
            "tulya_over_langgraph_delta_peak_rss_ratio": ratio(
                tulya.get("peak_rss_bytes"), langgraph_delta.get("peak_rss_bytes")
            ),
            **{
                f"{name}_over_tulya_reopened_read_p50_ratio": ratio(
                    nested(result, "exactness", "reopened", "read_latency", "p50_ns"),
                    nested(tulya_result, "exactness", "reopened", "read_latency", "p50_ns"),
                )
                for name, result in variant_results.items()
            },
            **{
                f"{name}_over_tulya_durable_append_p50_ratio": ratio(
                    nested(result, "latency", "durable_append", "p50_ns"),
                    nested(tulya_result, "latency", "durable_append", "p50_ns"),
                )
                for name, result in variant_results.items()
            },
        },
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "output_filesystem": output_filesystem,
            "volatile_filesystem": volatile_filesystem,
        },
        "source": {
            "tree_before_build": source_before,
            "git": {
                **git_before,
                "status_before": git_before["status"],
                "status_after": git_metadata(repo)["status"],
            },
        },
        "binaries": {
            TULYA_BIN: sha256_file(binary_root / TULYA_BIN),
            SQLITE_BIN: sha256_file(binary_root / SQLITE_BIN),
        },
        "next_gate": (
            "Preserve and publish this one-shot independent holdout result without tuning."
            if is_holdout
            else "Commit the release candidate, then reproduce the pinned evaluation on a clean worktree. Keep the holdout untouched."
            if args.allow_dirty
            else "Match this packaged source-tree digest to the published artifact, then reproduce the pinned evaluation from a clean release commit."
            if not git_before["available"]
            else "Have an independent evaluator run the reserved holdout once from this clean release commit, without tuning after observation."
        ),
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))
    if not campaign_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
