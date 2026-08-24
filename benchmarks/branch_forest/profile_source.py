#!/usr/bin/env python3
"""Profile natural repeated-attempt branch geometry without running a backend."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import pyarrow.parquet as pq


SOURCES = {
    "nebius-swe-agent": {
        "kind": "swe-agent",
        "required": {"instance_id", "target", "trajectory"},
    },
    "nebius-openhands": {
        "kind": "openhands",
        "required": {
            "trajectory_id",
            "instance_id",
            "resolved",
            "trajectory",
        },
    },
}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = int(round(fraction * (len(ordered) - 1)))
    return ordered[index]


def distribution(values: list[int]) -> dict[str, int]:
    return {
        "count": len(values),
        "min": min(values) if values else 0,
        "p25": percentile(values, 0.25),
        "p50": percentile(values, 0.50),
        "p75": percentile(values, 0.75),
        "p90": percentile(values, 0.90),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values) if values else 0,
    }


def normalize_swe_agent_message(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("SWE-agent message is not an object")
    role = raw.get("role")
    if not isinstance(role, str) or not role:
        raise ValueError("SWE-agent message role is invalid")
    if role == "system" and raw.get("system_prompt") not in (None, ""):
        content = raw.get("system_prompt")
    else:
        content = raw.get("text")
    if content is None:
        content = ""
    if not isinstance(content, str):
        content = str(content)
    return {"content": content, "role": role}


def normalize_openhands_message(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("OpenHands message is not an object")
    role = raw.get("role")
    if not isinstance(role, str) or not role:
        raise ValueError("OpenHands message role is invalid")
    if role == "assistant":
        keys = ("role", "content", "tool_calls")
    elif role == "tool":
        keys = ("role", "content", "name", "tool_call_id")
    else:
        keys = ("role", "content")
    return {key: raw.get(key) for key in keys}


def normalize_trajectory(raw: Any, kind: str) -> list[dict[str, Any]]:
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, list):
        raise ValueError("trajectory is not a list")
    normalizer = (
        normalize_swe_agent_message
        if kind == "swe-agent"
        else normalize_openhands_message
    )
    return [normalizer(message) for message in raw]


def checkpoint_accounting(
    messages: list[dict[str, Any]],
) -> tuple[int, int, int, list[bytes]]:
    empty_state = {"messages": []}
    empty_len = len(canonical_bytes(empty_state))
    prefix_payload = 0
    cumulative_full_state_bytes = 0
    message_bytes = 0
    message_digests: list[bytes] = []
    for index, message in enumerate(messages):
        encoded = canonical_bytes(message)
        message_bytes += len(encoded)
        prefix_payload += len(encoded)
        if index:
            prefix_payload += 1
        cumulative_full_state_bytes += empty_len + prefix_payload
        message_digests.append(hashlib.sha256(encoded).digest())
    final_state_len = empty_len + prefix_payload
    return final_state_len, cumulative_full_state_bytes, message_bytes, message_digests


@dataclass
class InstanceProfile:
    attempts: int = 0
    solved_attempts: int = 0
    checkpoints: int = 0
    final_state_bytes: int = 0
    cumulative_full_state_bytes: int = 0
    common_prefix: list[bytes] = field(default_factory=list)

    def add(
        self,
        solved: bool,
        message_digests: list[bytes],
        final_state_len: int,
        cumulative_full_state_bytes: int,
    ) -> None:
        self.attempts += 1
        self.solved_attempts += int(solved)
        self.checkpoints += len(message_digests)
        self.final_state_bytes += final_state_len
        self.cumulative_full_state_bytes += cumulative_full_state_bytes
        if self.attempts == 1:
            self.common_prefix = list(message_digests)
            return
        shared = min(len(self.common_prefix), len(message_digests))
        index = 0
        while index < shared and self.common_prefix[index] == message_digests[index]:
            index += 1
        del self.common_prefix[index:]


def parquet_paths(dataset_dir: Path, manifest: dict[str, Any]) -> list[Path]:
    result = [
        dataset_dir / str(item["path"])
        for item in manifest.get("files", [])
        if str(item.get("path", "")).endswith(".parquet")
    ]
    if not result or any(not path.is_file() for path in result):
        raise RuntimeError("source manifest does not resolve to local Parquet files")
    return result


def iter_rows(paths: Iterable[Path], columns: list[str]):
    for path in paths:
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=128, columns=columns):
            for row in batch.to_pylist():
                yield path, row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=sorted(SOURCES), required=True)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--out", type=Path)
    parser.add_argument(
        "--verify-files",
        action="store_true",
        help="rehash every downloaded input before profiling",
    )
    args = parser.parse_args()

    spec = SOURCES[args.source]
    dataset_dir = args.data_root / args.source
    manifest_path = dataset_dir / "source_manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(f"missing {manifest_path}; download the pinned source first")
    manifest = json.loads(manifest_path.read_text())
    paths = parquet_paths(dataset_dir, manifest)

    if args.verify_files:
        expected = {
            str(item["path"]): str(item["sha256"])
            for item in manifest.get("files", [])
            if str(item.get("path", "")).endswith(".parquet")
        }
        for path in paths:
            relative = str(path.relative_to(dataset_dir))
            actual = sha256_file(path)
            if actual != expected.get(relative):
                raise RuntimeError(f"SHA-256 mismatch for {relative}")

    first_schema = pq.ParquetFile(paths[0]).schema_arrow
    available = set(first_schema.names)
    missing = set(spec["required"]) - available
    if missing:
        raise RuntimeError(f"missing required columns: {sorted(missing)}")
    columns = sorted(set(spec["required"]))

    instances: dict[str, InstanceProfile] = {}
    attempt_ids: set[str] = set()
    valid_attempts = 0
    message_depths: list[int] = []
    final_state_sizes: list[int] = []
    invalid_rows = 0
    duplicate_attempt_ids = 0
    total_message_bytes = 0
    source_rows = 0

    for path, row in iter_rows(paths, columns):
        source_rows += 1
        try:
            instance_id = row.get("instance_id")
            if not isinstance(instance_id, str) or not instance_id:
                raise ValueError("invalid instance_id")
            if spec["kind"] == "openhands":
                raw_attempt_id = row.get("trajectory_id")
                solved = int(row.get("resolved") or 0) == 1
            else:
                raw_attempt_id = None
                solved = bool(row.get("target"))
            if isinstance(raw_attempt_id, str) and raw_attempt_id:
                attempt_id = raw_attempt_id
            else:
                relative = str(path.relative_to(dataset_dir))
                attempt_id = hashlib.sha256(
                    f"{manifest['resolved_revision']}\0{relative}\0{source_rows - 1}".encode()
                ).hexdigest()
            duplicate_attempt = attempt_id in attempt_ids
            if duplicate_attempt:
                duplicate_attempt_ids += 1
                attempt_id = hashlib.sha256(
                    f"{attempt_id}\0{source_rows - 1}".encode()
                ).hexdigest()
            messages = normalize_trajectory(row.get("trajectory"), str(spec["kind"]))
            final_len, cumulative_len, message_bytes, digests = checkpoint_accounting(messages)
        except Exception:
            invalid_rows += 1
            continue

        profile = instances.setdefault(instance_id, InstanceProfile())
        attempt_ids.add(attempt_id)
        valid_attempts += 1
        profile.add(solved, digests, final_len, cumulative_len)
        message_depths.append(len(messages))
        final_state_sizes.append(final_len)
        total_message_bytes += message_bytes

        if source_rows % 5000 == 0:
            print(
                f"profiled {source_rows} rows / {len(instances)} instances",
                flush=True,
            )

    attempt_counts = [profile.attempts for profile in instances.values()]
    checkpoints_per_instance = [profile.checkpoints for profile in instances.values()]
    cumulative_bytes_per_instance = [
        profile.cumulative_full_state_bytes for profile in instances.values()
    ]
    branchable = [profile for profile in instances.values() if profile.attempts >= 2]
    common_prefixes = [len(profile.common_prefix) for profile in branchable]
    branch_checkpoints = sum(profile.checkpoints for profile in branchable)
    branch_full_state_bytes = sum(
        profile.cumulative_full_state_bytes for profile in branchable
    )
    fanout_thresholds = {
        str(threshold): sum(1 for value in attempt_counts if value >= threshold)
        for threshold in (2, 4, 8, 16, 32, 64)
    }
    top_instances = sorted(
        (
            {
                "instance_id_sha256": hashlib.sha256(instance_id.encode()).hexdigest(),
                "attempts": profile.attempts,
                "checkpoints": profile.checkpoints,
                "common_prefix_messages": len(profile.common_prefix),
            }
            for instance_id, profile in instances.items()
        ),
        key=lambda item: (-int(item["attempts"]), str(item["instance_id_sha256"])),
    )[:25]

    result = {
        "format_version": 1,
        "contract": "TULYA_BRANCH_FOREST_PROFILE_V1",
        "profile_only": True,
        "new_llm_inference": False,
        "source": args.source,
        "repo_id": manifest["repo_id"],
        "resolved_revision": manifest["resolved_revision"],
        "source_manifest_sha256": sha256_file(manifest_path),
        "parquet_file_count": len(paths),
        "source_rows": source_rows,
        "valid_attempts": valid_attempts,
        "unique_attempt_ids": len(attempt_ids),
        "invalid_rows": invalid_rows,
        "duplicate_attempt_ids": duplicate_attempt_ids,
        "instance_count": len(instances),
        "branchable_instance_count": len(branchable),
        "fanout_threshold_instance_counts": fanout_thresholds,
        "attempts_per_instance": distribution(attempt_counts),
        "checkpoints_per_instance": distribution(checkpoints_per_instance),
        "observed_cumulative_full_state_bytes_per_instance": distribution(
            cumulative_bytes_per_instance
        ),
        "messages_per_attempt": distribution(message_depths),
        "final_state_bytes_per_attempt": distribution(final_state_sizes),
        "common_prefix_messages_for_branchable_instances": distribution(common_prefixes),
        "total_message_bytes": total_message_bytes,
        "total_checkpoint_count": sum(profile.checkpoints for profile in instances.values()),
        "total_cumulative_full_state_bytes": sum(
            profile.cumulative_full_state_bytes for profile in instances.values()
        ),
        "branchable_checkpoint_count": branch_checkpoints,
        "branchable_cumulative_full_state_bytes": branch_full_state_bytes,
        "top_fanout_instances": top_instances,
    }
    output = args.out or Path("results/branch-forest") / (
        f"{args.source}-profile.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
