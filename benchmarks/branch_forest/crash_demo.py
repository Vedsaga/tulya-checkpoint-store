#!/usr/bin/env python3
"""Crash and resume a natural Tulya branch forest at a durability boundary."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any


CRASH_ENV = "TULYA_CHECKPOINT_STORE_CRASH_POINT"
CRASH_POINT = "after-hot-sync-before-memory-publication"
CRASH_EXIT_CODE = 86
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


def run_capture(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def parse_json(stdout: str, label: str) -> dict[str, Any]:
    try:
        value = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{label} did not emit JSON") from error
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} JSON is not an object")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    corpus = args.corpus.resolve()
    corpus_sha256 = sha256_file(corpus)
    if corpus.name == "holdout.jsonl" or corpus_sha256 in RESERVED_HOLDOUT_SHA256:
        raise RuntimeError("the crash demo must not consume a reserved holdout")
    output = args.output_dir.resolve()
    if output.exists():
        raise RuntimeError("output directory already exists; choose a new path")
    output.mkdir(parents=True)

    bench = Path(__file__).resolve().parent
    repo = bench.parent.parent
    if not args.skip_build:
        build = run_capture(
            [
                "cargo",
                "build",
                "--release",
                "--locked",
                "--features",
                "fault-injection",
                "--example",
                "branch_forest_tulya",
                "--bin",
                "tulya-checkpoint",
            ],
            cwd=repo,
        )
        (output / "build.stdout").write_text(build.stdout)
        (output / "build.stderr").write_text(build.stderr)
        if build.returncode != 0:
            raise RuntimeError("fault-injection demo binaries did not build")

    adapter = repo / "target/release/examples/branch_forest_tulya"
    cli = repo / "target/release/tulya-checkpoint"
    store = output / "store"
    base_command = [str(adapter), "--db", str(store), "--corpus", str(corpus)]

    crash_env = os.environ.copy()
    crash_env[CRASH_ENV] = CRASH_POINT
    crashed = run_capture(base_command, cwd=repo, env=crash_env)
    (output / "crash.stdout").write_text(crashed.stdout)
    (output / "crash.stderr").write_text(crashed.stderr)

    resumed = run_capture([*base_command, "--resume"], cwd=repo)
    (output / "resume.stdout").write_text(resumed.stdout)
    (output / "resume.stderr").write_text(resumed.stderr)
    resume_result = parse_json(resumed.stdout, "resume") if resumed.returncode == 0 else {}

    checked = run_capture([str(cli), "--db", str(store), "fsck"], cwd=repo)
    (output / "fsck.stdout").write_text(checked.stdout)
    (output / "fsck.stderr").write_text(checked.stderr)
    fsck_result = parse_json(checked.stdout, "fsck") if checked.returncode == 0 else {}

    exactness = resume_result.get("exactness", {})
    passed = (
        crashed.returncode == CRASH_EXIT_CODE
        and resumed.returncode == 0
        and resume_result.get("pass") is True
        and resume_result.get("resume", {}).get("existing_checkpoint_count", 0) > 0
        and exactness.get("hot", {}).get("exact") is True
        and exactness.get("sealed", {}).get("exact") is True
        and exactness.get("reopened", {}).get("exact") is True
        and checked.returncode == 0
        and fsck_result.get("ok") is True
        and fsck_result.get("read_only") is True
    )
    result = {
        "contract": "TULYA_BRANCH_FOREST_CRASH_DEMO_V1",
        "pass": passed,
        "corpus": {
            "path": str(corpus),
            "size_bytes": corpus.stat().st_size,
            "sha256": corpus_sha256,
        },
        "crash": {
            "point": CRASH_POINT,
            "expected_exit_code": CRASH_EXIT_CODE,
            "actual_exit_code": crashed.returncode,
        },
        "resume": resume_result.get("resume"),
        "checkpoint_count": resume_result.get("checkpoint_node_count"),
        "exactness": exactness,
        "fsck": fsck_result,
        "scope": "userspace process exit after WAL sync and before memory publication; not sudden power loss",
    }
    (output / "summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
