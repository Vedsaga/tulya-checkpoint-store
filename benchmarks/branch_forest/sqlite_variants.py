#!/usr/bin/env python3
"""Strong SQLite storage variants for the frozen natural branch forest."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import shutil
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Any

import zstandard as zstd


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> bytes:
    return hashlib.sha256(value).digest()


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
        "mean_ns": int(round(statistics.mean(values))),
        "p50_ns": percentile(values, 0.50),
        "p95_ns": percentile(values, 0.95),
        "p99_ns": percentile(values, 0.99),
        "max_ns": max(values),
    }


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


def load_corpus(path: Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        raise RuntimeError("empty branch-forest corpus")
    return rows


def node_maps(corpus: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        str(instance["instance_id"]): {
            str(node["checkpoint_id"]): node for node in instance["nodes"]
        }
        for instance in corpus
    }


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


def configure(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute("PRAGMA foreign_keys=ON")


def create_schema(connection: sqlite3.Connection, mode: str) -> None:
    if mode == "cas-delta":
        connection.executescript(
            """
            CREATE TABLE blob(
                digest BLOB PRIMARY KEY,
                operation_json BLOB NOT NULL
            ) WITHOUT ROWID;
            CREATE TABLE node(
                instance_id TEXT NOT NULL,
                checkpoint_id TEXT NOT NULL,
                parent_checkpoint_id TEXT,
                sequence_no INTEGER NOT NULL,
                operation_digest BLOB NOT NULL REFERENCES blob(digest),
                logical_state_len INTEGER NOT NULL,
                logical_state_sha256 BLOB NOT NULL,
                PRIMARY KEY(instance_id, checkpoint_id)
            ) WITHOUT ROWID;
            CREATE INDEX node_parent ON node(instance_id, parent_checkpoint_id);
            """
        )
    else:
        connection.executescript(
            """
            CREATE TABLE node(
                instance_id TEXT NOT NULL,
                checkpoint_id TEXT NOT NULL,
                parent_checkpoint_id TEXT,
                sequence_no INTEGER NOT NULL,
                payload BLOB NOT NULL,
                logical_state_len INTEGER NOT NULL,
                logical_state_sha256 BLOB NOT NULL,
                PRIMARY KEY(instance_id, checkpoint_id)
            ) WITHOUT ROWID;
            CREATE INDEX node_parent ON node(instance_id, parent_checkpoint_id);
            """
        )
    connection.execute(
        """
        CREATE TABLE attempt(
            instance_id TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            tip_checkpoint_id TEXT NOT NULL,
            path_length INTEGER NOT NULL,
            PRIMARY KEY(instance_id, attempt_id)
        ) WITHOUT ROWID
        """
    )
    connection.commit()


def persist(
    connection: sqlite3.Connection,
    corpus: list[dict[str, Any]],
    mode: str,
    compressor: zstd.ZstdCompressor,
) -> tuple[list[int], int, int]:
    maps = node_maps(corpus)
    writes: list[int] = []
    checkpoint_count = 0
    attempt_count = 0
    for instance in corpus:
        instance_id = str(instance["instance_id"])
        nodes = maps[instance_id]
        for node in instance["nodes"]:
            checkpoint_id = str(node["checkpoint_id"])
            operation = node["operations"]
            if len(operation) != 1 or operation[0].get("op") != "append_message":
                raise RuntimeError(f"unsupported operation at {checkpoint_id}")
            started = time.perf_counter_ns()
            connection.execute("BEGIN IMMEDIATE")
            if mode == "cas-delta":
                operation_bytes = canonical_json_bytes(operation[0])
                digest = sha256_bytes(operation_bytes)
                connection.execute(
                    "INSERT OR IGNORE INTO blob(digest, operation_json) VALUES (?, ?)",
                    (digest, operation_bytes),
                )
                connection.execute(
                    """
                    INSERT INTO node VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        instance_id,
                        checkpoint_id,
                        node["parent_checkpoint_id"],
                        int(node["sequence_no"]),
                        digest,
                        int(node["logical_state_len"]),
                        bytes.fromhex(node["logical_state_sha256"]),
                    ),
                )
            else:
                state = expected_state(nodes, checkpoint_id)
                payload = state if mode == "snapshot-raw" else compressor.compress(state)
                connection.execute(
                    "INSERT INTO node VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        instance_id,
                        checkpoint_id,
                        node["parent_checkpoint_id"],
                        int(node["sequence_no"]),
                        payload,
                        len(state),
                        sha256_bytes(state),
                    ),
                )
            connection.commit()
            writes.append(time.perf_counter_ns() - started)
            checkpoint_count += 1

        connection.execute("BEGIN IMMEDIATE")
        for attempt in instance["attempts"]:
            path = attempt["checkpoint_path"]
            connection.execute(
                "INSERT INTO attempt VALUES (?, ?, ?, ?)",
                (instance_id, attempt["attempt_id"], path[-1], len(path)),
            )
            attempt_count += 1
        connection.commit()
    return writes, checkpoint_count, attempt_count


def read_state(
    connection: sqlite3.Connection,
    mode: str,
    instance_id: str,
    checkpoint_id: str,
    decompressor: zstd.ZstdDecompressor,
) -> bytes:
    if mode != "cas-delta":
        row = connection.execute(
            """
            SELECT payload, logical_state_len FROM node
            WHERE instance_id=? AND checkpoint_id=?
            """,
            (instance_id, checkpoint_id),
        ).fetchone()
        if row is None:
            raise RuntimeError(f"missing checkpoint {checkpoint_id}")
        payload = bytes(row[0])
        return (
            payload
            if mode == "snapshot-raw"
            else decompressor.decompress(payload, max_output_size=int(row[1]))
        )

    rows = connection.execute(
        """
        WITH RECURSIVE chain(parent_checkpoint_id, operation_digest, depth) AS (
            SELECT parent_checkpoint_id, operation_digest, 0 FROM node
            WHERE instance_id=?1 AND checkpoint_id=?2
            UNION ALL
            SELECT node.parent_checkpoint_id, node.operation_digest, chain.depth + 1
            FROM node JOIN chain
              ON node.instance_id=?1 AND node.checkpoint_id=chain.parent_checkpoint_id
        )
        SELECT blob.operation_json FROM chain
        JOIN blob ON blob.digest=chain.operation_digest
        ORDER BY chain.depth DESC
        """,
        (instance_id, checkpoint_id),
    ).fetchall()
    messages = []
    for row in rows:
        operation = json.loads(bytes(row[0]))
        messages.append(operation["value"])
    return canonical_json_bytes({"messages": messages})


def verify(
    connection: sqlite3.Connection,
    corpus: list[dict[str, Any]],
    mode: str,
    decompressor: zstd.ZstdDecompressor,
) -> dict[str, Any]:
    failures = 0
    reads: list[int] = []
    checkpoint_count = 0
    for instance in corpus:
        instance_id = str(instance["instance_id"])
        for node in instance["nodes"]:
            started = time.perf_counter_ns()
            state = read_state(
                connection,
                mode,
                instance_id,
                str(node["checkpoint_id"]),
                decompressor,
            )
            reads.append(time.perf_counter_ns() - started)
            checkpoint_count += 1
            if (
                len(state) != int(node["logical_state_len"])
                or hashlib.sha256(state).hexdigest()
                != node["logical_state_sha256"]
            ):
                failures += 1
    return {
        "checkpoint_count": checkpoint_count,
        "state_failures": failures,
        "exact": failures == 0,
        "read_latency": latency_stats(reads),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument(
        "--mode", choices=("cas-delta", "snapshot-raw", "snapshot-zstd"), required=True
    )
    args = parser.parse_args()

    corpus = load_corpus(args.corpus.resolve())
    root = args.db.resolve()
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    db_path = root / "baseline.sqlite"
    compressor = zstd.ZstdCompressor(level=1)
    decompressor = zstd.ZstdDecompressor()

    connection = sqlite3.connect(db_path)
    configure(connection)
    create_schema(connection, args.mode)
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    connection.close()
    empty = storage(root)

    connection = sqlite3.connect(db_path)
    configure(connection)
    writes, checkpoint_count, attempt_count = persist(
        connection, corpus, args.mode, compressor
    )
    active = storage(root)
    active_verification = verify(connection, corpus, args.mode, decompressor)
    node_rows = int(connection.execute("SELECT count(*) FROM node").fetchone()[0])
    attempt_rows = int(connection.execute("SELECT count(*) FROM attempt").fetchone()[0])
    blob_rows = (
        int(connection.execute("SELECT count(*) FROM blob").fetchone()[0])
        if args.mode == "cas-delta"
        else None
    )
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    checkpointed = storage(root)
    connection.close()

    reopened_connection = sqlite3.connect(db_path)
    configure(reopened_connection)
    reopened_verification = verify(
        reopened_connection, corpus, args.mode, decompressor
    )
    reopened = storage(root)
    reopened_connection.close()

    passed = (
        active_verification["exact"]
        and reopened_verification["exact"]
        and node_rows == checkpoint_count
        and attempt_rows == attempt_count
    )
    result = {
        "format_version": 1,
        "contract": "TULYA_BRANCH_FOREST_BENCHMARK_V1",
        "backend": f"sqlite-{args.mode}",
        "mode": args.mode,
        "checkpoint_node_count": checkpoint_count,
        "attempt_ref_count": attempt_count,
        "content_blob_count": blob_rows,
        "pass": passed,
        "claims": {
            "performance_claim": False,
            "storage_advantage_claim": False,
            "holdout_accessed": args.corpus.name == "holdout.jsonl",
        },
        "durability": {
            "journal_mode": "WAL",
            "synchronous": "FULL",
            "transaction_per_checkpoint": True,
            "wal_autocheckpoint": 0,
        },
        "versions": {
            "sqlite": sqlite3.sqlite_version,
            "zstandard": importlib.metadata.version("zstandard"),
            "zstd_level": 1,
        },
        "latency": {"durable_append": latency_stats(writes)},
        "exactness": {
            "active": active_verification,
            "reopened": reopened_verification,
        },
        "storage": {
            "empty": empty,
            "active": active,
            "checkpointed": checkpointed,
            "reopened": reopened,
        },
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
