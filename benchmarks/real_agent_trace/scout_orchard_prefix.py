#!/usr/bin/env python3
"""Fail-fast scout for exact reusable execution prefixes in Orchard SWE rollouts.

This intentionally does NOT benchmark Tulya. It asks a prior economic question:
when the same SWE task is sampled multiple times, how much exact tool execution
prefix is duplicated across the observed rollouts?

Rows are fetched from the public Hugging Face Dataset Viewer API in small pages,
so the 9+ GiB Orchard corpus is not downloaded locally.
"""
from __future__ import annotations

import argparse
import json
import statistics
import urllib.parse
import urllib.request
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Any

BENCHMARK = "TULYA_ORCHARD_PREFIX_SCOUT_V1"
API = "https://datasets-server.huggingface.co/rows"


def fetch_rows(offset: int, length: int, timeout: int) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode(
        {
            "dataset": "microsoft/Orchard",
            "config": "swe",
            "split": "train",
            "offset": offset,
            "length": length,
        }
    )
    request = urllib.request.Request(
        f"{API}?{query}",
        headers={"User-Agent": "tulya-orchard-prefix-scout/1"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise RuntimeError(f"unexpected Dataset Viewer response keys: {sorted(payload)}")
    return rows


def decode_jsonish(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def row_payload(wrapper: dict[str, Any]) -> dict[str, Any]:
    row = wrapper.get("row", wrapper)
    if not isinstance(row, dict):
        raise RuntimeError("Dataset Viewer row is not an object")
    return row


def metadata_of(row: dict[str, Any]) -> dict[str, Any]:
    metadata = decode_jsonish(row.get("metadata", {}))
    return metadata if isinstance(metadata, dict) else {}


def messages_of(row: dict[str, Any]) -> list[dict[str, Any]]:
    messages = decode_jsonish(row.get("messages", []))
    if not isinstance(messages, list):
        return []
    return [m for m in messages if isinstance(m, dict)]


def canonical_args(value: Any) -> str:
    parsed = decode_jsonish(value)
    if isinstance(parsed, (dict, list, int, float, bool)) or parsed is None:
        return json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return str(parsed)


def action_sequence(row: dict[str, Any]) -> list[str]:
    """Flatten exact assistant tool calls, ignoring generated call IDs."""
    actions: list[str] = []
    for message in messages_of(row):
        if message.get("role") != "assistant":
            continue
        calls = message.get("tool_calls") or []
        if not isinstance(calls, list):
            continue
        for call in calls:
            if not isinstance(call, dict):
                continue
            function = call.get("function") or {}
            if not isinstance(function, dict):
                continue
            name = str(function.get("name", ""))
            args = canonical_args(function.get("arguments", ""))
            actions.append(f"{name}\0{args}")
    return actions


def common_prefix(a: list[str], b: list[str]) -> int:
    n = 0
    for left, right in zip(a, b):
        if left != right:
            break
        n += 1
    return n


def all_common_prefix(sequences: list[list[str]]) -> int:
    if not sequences:
        return 0
    limit = min(map(len, sequences))
    for i in range(limit):
        first = sequences[0][i]
        if any(seq[i] != first for seq in sequences[1:]):
            return i
    return limit


def trie_unique_actions(sequences: list[list[str]]) -> int:
    """Number of action executions under ideal exact-prefix sharing."""
    root: dict[str, dict] = {}
    nodes = 0
    for seq in sequences:
        cur = root
        for action in seq:
            nxt = cur.get(action)
            if nxt is None:
                nxt = {}
                cur[action] = nxt
                nodes += 1
            cur = nxt
    return nodes


def quantiles(values: list[int]) -> dict[str, float | int]:
    if not values:
        return {"count": 0, "min": 0, "mean": 0.0, "median": 0.0, "max": 0}
    return {
        "count": len(values),
        "min": min(values),
        "mean": statistics.fmean(values),
        "median": statistics.median(values),
        "max": max(values),
    }


def analyze(instance_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda row: (
            int(metadata_of(row).get("sample_idx", 0)),
            str(metadata_of(row).get("model", "")),
        ),
    )
    sequences = [action_sequence(row) for row in ordered]
    sample_idxs = [metadata_of(row).get("sample_idx") for row in ordered]
    pair_prefixes = [common_prefix(sequences[i], sequences[j]) for i, j in combinations(range(len(sequences)), 2)]
    flat = sum(map(len, sequences))
    shared = trie_unique_actions(sequences)
    avoidable = flat - shared
    return {
        "instance_id": instance_id,
        "attempt_count": len(ordered),
        "sample_idxs": sample_idxs,
        "tool_actions_per_attempt": [len(seq) for seq in sequences],
        "all_attempt_common_prefix_actions": all_common_prefix(sequences),
        "pairwise_common_prefix_actions": quantiles(pair_prefixes),
        "pairs_prefix_ge_1": sum(v >= 1 for v in pair_prefixes),
        "pairs_prefix_ge_2": sum(v >= 2 for v in pair_prefixes),
        "pairs_prefix_ge_5": sum(v >= 5 for v in pair_prefixes),
        "flat_tool_action_executions": flat,
        "exact_prefix_tree_action_executions": shared,
        "exact_prefix_avoidable_action_executions": avoidable,
        "exact_prefix_avoidable_fraction": (avoidable / flat) if flat else 0.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark-results/orchard-prefix-scout"))
    parser.add_argument("--target-instances", type=int, default=8)
    parser.add_argument("--min-attempts", type=int, default=2)
    parser.add_argument("--page-length", type=int, default=20)
    parser.add_argument("--max-rows", type=int, default=500)
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()

    if not 1 <= args.page_length <= 100:
        parser.error("--page-length must be in [1,100]")
    if args.min_attempts < 2:
        parser.error("--min-attempts must be >= 2")

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    fetched = 0
    offset = 0
    while fetched < args.max_rows:
        length = min(args.page_length, args.max_rows - fetched)
        wrappers = fetch_rows(offset, length, args.timeout)
        if not wrappers:
            break
        for wrapper in wrappers:
            row = row_payload(wrapper)
            metadata = metadata_of(row)
            instance_id = str(metadata.get("instance_id") or row.get("instance_id") or "")
            if instance_id:
                groups[instance_id].append(row)
        fetched += len(wrappers)
        offset += len(wrappers)
        ready = [rows for rows in groups.values() if len(rows) >= args.min_attempts]
        if len(ready) >= args.target_instances:
            break
        if len(wrappers) < length:
            break

    candidates = [
        (instance_id, rows)
        for instance_id, rows in groups.items()
        if len(rows) >= args.min_attempts
    ]
    candidates.sort(key=lambda item: (-len(item[1]), item[0]))
    selected = candidates[: args.target_instances]
    analyses = [analyze(instance_id, rows) for instance_id, rows in selected]

    flat = sum(item["flat_tool_action_executions"] for item in analyses)
    shared = sum(item["exact_prefix_tree_action_executions"] for item in analyses)
    avoidable = flat - shared
    all_pair_prefixes: list[int] = []
    for instance_id, rows in selected:
        sequences = [action_sequence(row) for row in rows]
        all_pair_prefixes.extend(
            common_prefix(sequences[i], sequences[j])
            for i, j in combinations(range(len(sequences)), 2)
        )

    summary = {
        "benchmark": BENCHMARK,
        "source": "microsoft/Orchard SWE via Hugging Face Dataset Viewer API",
        "rows_fetched": fetched,
        "unique_instances_seen": len(groups),
        "instances_with_multiple_rollouts_seen": len(candidates),
        "instances_analyzed": len(analyses),
        "metric_scope": "Exact assistant tool-call prefixes only; shared prompts are excluded. This is an upper bound on avoidable observed tool executions under exact-prefix branching, not a Tulya benchmark and not a dollar estimate.",
        "aggregate": {
            "flat_tool_action_executions": flat,
            "exact_prefix_tree_action_executions": shared,
            "exact_prefix_avoidable_action_executions": avoidable,
            "exact_prefix_avoidable_fraction": (avoidable / flat) if flat else 0.0,
            "pairwise_common_prefix_actions": quantiles(all_pair_prefixes),
            "pairs_prefix_ge_1": sum(v >= 1 for v in all_pair_prefixes),
            "pairs_prefix_ge_2": sum(v >= 2 for v in all_pair_prefixes),
            "pairs_prefix_ge_5": sum(v >= 5 for v in all_pair_prefixes),
        },
        "instances": analyses,
        "decision_hint": {
            "weak": "Near-zero exact action-prefix reuse means current independent SWE rollouts do not naturally create much reusable execution prefix.",
            "strong": "Large exact-prefix avoidable fraction means branch-from-checkpoint could eliminate repeated observed tool execution, subject to checkpoint cost and environment determinism.",
        },
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = args.output_dir / "summary.json"
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output)
    if not analyses:
        print("No repeated instances found within scan budget; increase --max-rows.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
