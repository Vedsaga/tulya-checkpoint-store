#!/usr/bin/env python3
"""Reduce a raw branch-forest result to a portable evidence record."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def nested(value: dict[str, Any], *keys: str) -> Any:
    current: Any = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def arm_record(arm: dict[str, Any]) -> dict[str, Any]:
    result = arm.get("result", {})
    exactness = result.get("exactness", {})
    total = nested(result, "storage", "reopened", "allocated_bytes")
    empty = nested(result, "storage", "empty", "allocated_bytes")
    marginal = None
    if isinstance(total, int) and isinstance(empty, int):
        marginal = total - empty
    return {
        "backend": result.get("backend"),
        "pass": result.get("pass"),
        "returncode": arm.get("returncode"),
        "checkpoint_node_count": result.get("checkpoint_node_count"),
        "attempt_ref_count": result.get("attempt_ref_count"),
        "peak_rss_bytes": arm.get("peak_rss_bytes"),
        "wall_elapsed_ns": arm.get("wall_elapsed_ns"),
        "marginal_reopened_allocated_bytes": marginal,
        "durable_append_p50_ns": nested(
            result, "latency", "durable_append", "p50_ns"
        ),
        "reopened_read_p50_ns": nested(
            result, "exactness", "reopened", "read_latency", "p50_ns"
        )
        or nested(
            result,
            "exactness",
            "reopened_after_aggressive_pack",
            "read_latency",
            "p50_ns",
        ),
        "exactness": {
            state: {
                "checkpoint_count": record.get("checkpoint_count"),
                "exact": record.get("exact"),
                "state_failures": record.get("state_failures", 0),
                "internal_failures": record.get("internal_failures", 0),
                "metadata_failures": record.get("metadata_failures", 0),
            }
            for state, record in sorted(exactness.items())
            if isinstance(record, dict) and "exact" in record
        },
        "claims": result.get("claims"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    source = args.summary.resolve()
    summary = json.loads(source.read_text())
    if summary.get("pass") is not True:
        raise RuntimeError("refusing to summarize a non-passing campaign")

    corpus = summary.get("corpus", {})
    compact = {
        "evidence_schema": 1,
        "contract": summary.get("contract"),
        "phase": summary.get("phase"),
        "pass": summary.get("pass"),
        "performance_claim": summary.get("performance_claim"),
        "storage_advantage_claim": summary.get("storage_advantage_claim"),
        "holdout_accessed": summary.get("holdout_accessed"),
        "raw_summary_sha256": sha256_file(source),
        "corpus": {
            "name": Path(str(corpus.get("path", ""))).name,
            "size_bytes": corpus.get("size_bytes"),
            "sha256": corpus.get("sha256"),
        },
        "arms": {
            name: arm_record(arm)
            for name, arm in sorted(summary.get("arms", {}).items())
        },
        "comparison": summary.get("comparison"),
        "environment": summary.get("environment"),
        "source": {
            "tree_before_build": nested(summary, "source", "tree_before_build"),
            "git": {
                "available": nested(summary, "source", "git", "available"),
                "head": nested(summary, "source", "git", "head"),
                "clean_before": not bool(
                    nested(summary, "source", "git", "status_before")
                ),
                "clean_after": not bool(
                    nested(summary, "source", "git", "status_after")
                ),
            },
        },
        "next_gate": summary.get("next_gate"),
    }
    rendered = json.dumps(compact, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
