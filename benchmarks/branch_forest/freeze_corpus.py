#!/usr/bin/env python3
"""Freeze deterministic natural branch forests before running any backend."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from profile_source import (
    SOURCES,
    canonical_bytes,
    checkpoint_accounting,
    iter_rows,
    normalize_trajectory,
    parquet_paths,
    sha256_file,
)
from run import git_metadata, source_tree_identity


# Frozen before the standalone extraction. Changing this value changes every
# published partition and invalidates the recorded corpus digests.
SELECTION_SEED = "real-world-repeated-attempt-branch-forest-r0-v1"
MIN_ATTEMPTS = 4
PARTITIONS = (
    ("adapter", 1 * 1024 * 1024, 1),
    ("engineering", 128 * 1024 * 1024, 4),
    ("evaluation", 512 * 1024 * 1024, 8),
    ("holdout", 512 * 1024 * 1024, 8),
)


@dataclass
class InstanceStats:
    attempts: int = 0
    checkpoints: int = 0
    observed_cumulative_full_state_bytes: int = 0


def load_source(data_root: Path, source: str) -> tuple[Path, dict[str, Any], list[Path]]:
    dataset_dir = data_root / source
    manifest_path = dataset_dir / "source_manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(f"missing {manifest_path}; download the pinned source first")
    manifest = json.loads(manifest_path.read_text())
    return dataset_dir, manifest, parquet_paths(dataset_dir, manifest)


def source_columns(paths: list[Path], source: str) -> list[str]:
    spec = SOURCES[source]
    available = set(pq.ParquetFile(paths[0]).schema_arrow.names)
    missing = set(spec["required"]) - available
    if missing:
        raise RuntimeError(f"missing required columns: {sorted(missing)}")
    return sorted(set(spec["required"]))


def attempt_metadata(
    *,
    source: str,
    row: dict[str, Any],
    source_ordinal: int,
    relative_path: str,
    revision: str,
) -> tuple[str, str, bool]:
    instance_id = row.get("instance_id")
    if not isinstance(instance_id, str) or not instance_id:
        raise ValueError("invalid instance_id")
    if SOURCES[source]["kind"] == "openhands":
        raw_attempt_id = row.get("trajectory_id")
        solved = int(row.get("resolved") or 0) == 1
    else:
        raw_attempt_id = None
        solved = bool(row.get("target"))
    if isinstance(raw_attempt_id, str) and raw_attempt_id:
        attempt_id = raw_attempt_id
    else:
        attempt_id = hashlib.sha256(
            f"{revision}\0{relative_path}\0{source_ordinal}".encode()
        ).hexdigest()
    return instance_id, attempt_id, solved


def profile_instances(
    *,
    source: str,
    dataset_dir: Path,
    manifest: dict[str, Any],
    paths: list[Path],
    columns: list[str],
) -> tuple[dict[str, InstanceStats], dict[str, int]]:
    instances: dict[str, InstanceStats] = {}
    counters: Counter[str] = Counter()
    revision = str(manifest["resolved_revision"])
    for source_ordinal, (path, row) in enumerate(iter_rows(paths, columns)):
        counters["source_rows"] += 1
        try:
            instance_id, _attempt_id, _solved = attempt_metadata(
                source=source,
                row=row,
                source_ordinal=source_ordinal,
                relative_path=str(path.relative_to(dataset_dir)),
                revision=revision,
            )
            messages = normalize_trajectory(row.get("trajectory"), str(SOURCES[source]["kind"]))
            _final, cumulative, _payload, digests = checkpoint_accounting(messages)
        except Exception:
            counters["invalid_rows"] += 1
            continue
        stats = instances.setdefault(instance_id, InstanceStats())
        stats.attempts += 1
        stats.checkpoints += len(digests)
        stats.observed_cumulative_full_state_bytes += cumulative
        counters["valid_rows"] += 1
        if counters["source_rows"] % 5000 == 0:
            print(
                f"profiled {counters['source_rows']} rows / {len(instances)} instances",
                flush=True,
            )
    return instances, dict(counters)


def select_partitions(
    *,
    revision: str,
    instances: dict[str, InstanceStats],
) -> tuple[dict[str, str], dict[str, Any]]:
    eligible = [
        (instance_id, stats)
        for instance_id, stats in instances.items()
        if stats.attempts >= MIN_ATTEMPTS
    ]
    eligible.sort(
        key=lambda item: hashlib.sha256(
            f"{SELECTION_SEED}\0{revision}\0{item[0]}".encode()
        ).digest()
    )
    selected: dict[str, str] = {}
    summary: dict[str, Any] = {}
    cursor = 0
    for name, target_bytes, minimum_instances in PARTITIONS:
        chosen: list[tuple[str, InstanceStats]] = []
        observed_bytes = 0
        while observed_bytes < target_bytes or len(chosen) < minimum_instances:
            if cursor >= len(eligible):
                raise RuntimeError(f"source exhausted while selecting {name}")
            instance_id, stats = eligible[cursor]
            cursor += 1
            chosen.append((instance_id, stats))
            selected[instance_id] = name
            observed_bytes += stats.observed_cumulative_full_state_bytes
        summary[name] = {
            "target_observed_cumulative_full_state_bytes": target_bytes,
            "instance_count": len(chosen),
            "attempt_count": sum(stats.attempts for _, stats in chosen),
            "observed_checkpoint_count": sum(stats.checkpoints for _, stats in chosen),
            "observed_cumulative_full_state_bytes": observed_bytes,
            "instance_id_sha256": [
                hashlib.sha256(instance_id.encode()).hexdigest()
                for instance_id, _ in chosen
            ],
        }
    return selected, {
        "selection_seed": SELECTION_SEED,
        "minimum_attempts_per_instance": MIN_ATTEMPTS,
        "eligible_instance_count": len(eligible),
        "partitions": summary,
    }


def materialize_selected(
    *,
    source: str,
    dataset_dir: Path,
    manifest: dict[str, Any],
    paths: list[Path],
    columns: list[str],
    selected: dict[str, str],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    rows: dict[str, dict[str, list[dict[str, Any]]]] = {
        name: {} for name, _target, _minimum in PARTITIONS
    }
    revision = str(manifest["resolved_revision"])
    seen_attempt_ids: set[str] = set()
    for source_ordinal, (path, row) in enumerate(iter_rows(paths, columns)):
        instance_id = row.get("instance_id")
        if not isinstance(instance_id, str) or instance_id not in selected:
            continue
        try:
            instance_id, attempt_id, solved = attempt_metadata(
                source=source,
                row=row,
                source_ordinal=source_ordinal,
                relative_path=str(path.relative_to(dataset_dir)),
                revision=revision,
            )
            messages = normalize_trajectory(row.get("trajectory"), str(SOURCES[source]["kind"]))
        except Exception as exc:
            raise RuntimeError(
                f"selected source row became invalid at ordinal {source_ordinal}"
            ) from exc
        if attempt_id in seen_attempt_ids:
            attempt_id = hashlib.sha256(
                f"{attempt_id}\0{source_ordinal}".encode()
            ).hexdigest()
        seen_attempt_ids.add(attempt_id)
        fingerprint = hashlib.sha256(
            canonical_bytes(
                {
                    "attempt_id": attempt_id,
                    "instance_id": instance_id,
                    "messages": messages,
                    "solved": solved,
                }
            )
        ).hexdigest()
        rows[selected[instance_id]].setdefault(instance_id, []).append(
            {
                "attempt_id": attempt_id,
                "messages": messages,
                "solved": solved,
                "source_row_fingerprint": fingerprint,
            }
        )
    missing = set(selected) - {
        instance_id
        for cohort in rows.values()
        for instance_id in cohort
    }
    if missing:
        raise RuntimeError(f"failed to materialize {len(missing)} selected instances")
    return rows


def freeze_instance(
    *,
    source: str,
    revision: str,
    instance_id: str,
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    node_by_edge: dict[tuple[str | None, str], dict[str, Any]] = {}
    node_order: list[dict[str, Any]] = []
    output_attempts: list[dict[str, Any]] = []
    observed_cumulative = 0
    attempts.sort(key=lambda item: str(item["attempt_id"]))

    for attempt in attempts:
        parent_id: str | None = None
        visible: list[dict[str, Any]] = []
        path_ids: list[str] = []
        for sequence_no, message in enumerate(attempt["messages"]):
            encoded_message = canonical_bytes(message)
            message_sha = hashlib.sha256(encoded_message).hexdigest()
            edge = (parent_id, message_sha)
            visible.append(message)
            state = canonical_bytes({"messages": visible})
            observed_cumulative += len(state)
            node = node_by_edge.get(edge)
            if node is None:
                checkpoint_id = "cp-" + hashlib.sha256(
                    canonical_bytes(
                        {
                            "instance_id": instance_id,
                            "message_sha256": message_sha,
                            "parent_checkpoint_id": parent_id,
                        }
                    )
                ).hexdigest()
                node = {
                    "checkpoint_id": checkpoint_id,
                    "parent_checkpoint_id": parent_id,
                    "sequence_no": sequence_no,
                    "operations": [{"op": "append_message", "value": message}],
                    "logical_state_len": len(state),
                    "logical_state_sha256": hashlib.sha256(state).hexdigest(),
                    "observations": 0,
                }
                node_by_edge[edge] = node
                node_order.append(node)
            else:
                expected_operation = [{"op": "append_message", "value": message}]
                if (
                    node["operations"] != expected_operation
                    or node["logical_state_len"] != len(state)
                    or node["logical_state_sha256"] != hashlib.sha256(state).hexdigest()
                ):
                    raise RuntimeError("SHA-256 edge collision or inconsistent shared prefix")
            node["observations"] = int(node["observations"]) + 1
            parent_id = str(node["checkpoint_id"])
            path_ids.append(parent_id)
        output_attempts.append(
            {
                "attempt_id": attempt["attempt_id"],
                "solved": attempt["solved"],
                "source_row_fingerprint": attempt["source_row_fingerprint"],
                "checkpoint_path": path_ids,
            }
        )

    unique_cumulative = sum(int(node["logical_state_len"]) for node in node_order)
    return {
        "format_version": 1,
        "source": source,
        "resolved_revision": revision,
        "instance_id": instance_id,
        "attempts": output_attempts,
        "nodes": node_order,
        "stats": {
            "attempt_count": len(output_attempts),
            "solved_attempt_count": sum(bool(item["solved"]) for item in output_attempts),
            "observed_checkpoint_count": sum(
                len(item["checkpoint_path"]) for item in output_attempts
            ),
            "unique_checkpoint_node_count": len(node_order),
            "shared_checkpoint_observations": sum(
                len(item["checkpoint_path"]) for item in output_attempts
            )
            - len(node_order),
            "observed_cumulative_full_state_bytes": observed_cumulative,
            "unique_node_cumulative_full_state_bytes": unique_cumulative,
        },
    }


def write_partition(
    *,
    path: Path,
    source: str,
    revision: str,
    instances: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    stats: Counter[str] = Counter()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as output:
        for instance_id in sorted(instances):
            row = freeze_instance(
                source=source,
                revision=revision,
                instance_id=instance_id,
                attempts=instances[instance_id],
            )
            output.write(canonical_bytes(row) + b"\n")
            stats["instance_count"] += 1
            for key, value in row["stats"].items():
                stats[key] += int(value)
    return {
        "path": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "stats": dict(stats),
    }


def verify_partition(path: Path, expected_stats: dict[str, Any]) -> dict[str, Any]:
    failures = 0
    aggregate: Counter[str] = Counter()
    with path.open("r", encoding="utf-8") as source:
        for line_no, line in enumerate(source, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            nodes: dict[str, dict[str, Any]] = {}
            state_messages: dict[str, list[dict[str, Any]]] = {}
            state_lengths: dict[str, int] = {}
            for node in row.get("nodes", []):
                checkpoint_id = node.get("checkpoint_id")
                parent_id = node.get("parent_checkpoint_id")
                operations = node.get("operations")
                if (
                    not isinstance(checkpoint_id, str)
                    or checkpoint_id in nodes
                    or (parent_id is not None and parent_id not in nodes)
                    or not isinstance(operations, list)
                    or len(operations) != 1
                    or not isinstance(operations[0], dict)
                    or operations[0].get("op") != "append_message"
                ):
                    failures += 1
                    continue
                parent_messages = [] if parent_id is None else state_messages[parent_id]
                messages = [*parent_messages, operations[0].get("value")]
                state = canonical_bytes({"messages": messages})
                if (
                    node.get("sequence_no") != len(messages) - 1
                    or node.get("logical_state_len") != len(state)
                    or node.get("logical_state_sha256")
                    != hashlib.sha256(state).hexdigest()
                ):
                    failures += 1
                nodes[checkpoint_id] = node
                state_messages[checkpoint_id] = messages
                state_lengths[checkpoint_id] = len(state)

            observations: Counter[str] = Counter()
            solved = 0
            observed_bytes = 0
            observed_checkpoints = 0
            for attempt in row.get("attempts", []):
                solved += int(bool(attempt.get("solved")))
                parent_id: str | None = None
                for checkpoint_id in attempt.get("checkpoint_path", []):
                    node = nodes.get(checkpoint_id)
                    if node is None or node.get("parent_checkpoint_id") != parent_id:
                        failures += 1
                        break
                    observations[checkpoint_id] += 1
                    observed_bytes += state_lengths[checkpoint_id]
                    observed_checkpoints += 1
                    parent_id = checkpoint_id
            for checkpoint_id, node in nodes.items():
                if node.get("observations") != observations[checkpoint_id]:
                    failures += 1

            actual = {
                "attempt_count": len(row.get("attempts", [])),
                "solved_attempt_count": solved,
                "observed_checkpoint_count": observed_checkpoints,
                "unique_checkpoint_node_count": len(nodes),
                "shared_checkpoint_observations": observed_checkpoints - len(nodes),
                "observed_cumulative_full_state_bytes": observed_bytes,
                "unique_node_cumulative_full_state_bytes": sum(state_lengths.values()),
            }
            if actual != row.get("stats"):
                failures += 1
            aggregate["instance_count"] += 1
            for key, value in actual.items():
                aggregate[key] += value
    if dict(aggregate) != expected_stats:
        failures += 1
    result = {
        "instance_rows_checked": aggregate["instance_count"],
        "checkpoint_nodes_checked": aggregate["unique_checkpoint_node_count"],
        "attempt_paths_checked": aggregate["attempt_count"],
        "failures": failures,
        "exact": failures == 0,
    }
    if failures:
        raise RuntimeError(f"frozen partition verification failed for {path}: {result}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=sorted(SOURCES), required=True)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("results/branch-forest"),
    )
    args = parser.parse_args()

    bench = Path(__file__).resolve().parent
    repo = bench.parent.parent
    data_root = args.data_root.resolve()
    output_dir = args.output_dir.resolve()
    git_before = git_metadata(repo)
    source_before = source_tree_identity(repo)
    source_output = output_dir / args.source
    dataset_dir, source_manifest, paths = load_source(data_root, args.source)
    columns = source_columns(paths, args.source)
    instances, scan = profile_instances(
        source=args.source,
        dataset_dir=dataset_dir,
        manifest=source_manifest,
        paths=paths,
        columns=columns,
    )
    selected, selection = select_partitions(
        revision=str(source_manifest["resolved_revision"]),
        instances=instances,
    )
    materialized = materialize_selected(
        source=args.source,
        dataset_dir=dataset_dir,
        manifest=source_manifest,
        paths=paths,
        columns=columns,
        selected=selected,
    )

    files: dict[str, Any] = {}
    for name, _target, _minimum in PARTITIONS:
        files[name] = write_partition(
            path=source_output / f"{name}.jsonl",
            source=args.source,
            revision=str(source_manifest["resolved_revision"]),
            instances=materialized[name],
        )
        files[name]["verification"] = verify_partition(
            source_output / f"{name}.jsonl",
            files[name]["stats"],
        )

    manifest = {
        "format_version": 1,
        "contract": "TULYA_BRANCH_FOREST_CORPUS_V1",
        "profile_and_freeze_only": True,
        "backend_executed": False,
        "new_llm_inference": False,
        "source": args.source,
        "repo_id": source_manifest["repo_id"],
        "resolved_revision": source_manifest["resolved_revision"],
        "source_manifest_sha256": sha256_file(dataset_dir / "source_manifest.json"),
        "scan": scan,
        "selection": selection,
        "files": files,
        "source_provenance": {
            "tree_before_freeze": source_before,
            "git": git_before,
        },
        "claims": {
            "natural_repeated_attempts": True,
            "exact_shared_prefixes_only": True,
            "performance_measured": False,
            "storage_advantage_measured": False,
            "holdout_performance_consumed": False,
        },
    }
    manifest_path = source_output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
