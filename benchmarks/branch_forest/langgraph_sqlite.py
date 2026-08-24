#!/usr/bin/env python3
"""Direct LangGraph SQLite baseline for the frozen natural branch forest.

This uses the public ``CompiledStateGraph.invoke`` API with an input-only graph.
LangGraph therefore stores its input and loop checkpoints for every logical
checkpoint while retaining native parent/fork semantics. Two modes are supported:

* ``full``: the normal BinaryOperatorAggregate list channel;
* ``delta``: LangGraph's beta DeltaChannel with bounded replay snapshots.

Messages are supplied as canonical JSON strings.  This avoids LangChain message
object metadata (including generated IDs) changing the frozen logical payload.
Every read parses the strings back into the contract's ``{"messages": [...]}``
state before checking its exact length and SHA-256.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import operator
import shutil
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Annotated, Any, Sequence, TypedDict

from langgraph.channels import DeltaChannel
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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
    stat = path.stat()
    return int(getattr(stat, "st_blocks", 0)) * 512


def storage(root: Path) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
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


def delta_reducer(current: list[str], batches: Sequence[Any]) -> list[str]:
    """Append batched list updates without nesting them."""
    result = list(current)
    for batch in batches:
        if isinstance(batch, list):
            result.extend(str(value) for value in batch)
        else:
            result.append(str(batch))
    return result


class FullState(TypedDict):
    messages: Annotated[list[str], operator.add]


class DeltaState(TypedDict):
    messages: Annotated[
        list[str], DeltaChannel(delta_reducer, list, snapshot_frequency=1000)
    ]


def build_graph(saver: SqliteSaver, mode: str) -> Any:
    builder = StateGraph(FullState if mode == "full" else DeltaState)
    builder.add_edge(START, END)
    return builder.compile(checkpointer=saver)


def open_graph(db_path: Path, mode: str) -> tuple[sqlite3.Connection, SqliteSaver, Any]:
    connection = sqlite3.connect(db_path, check_same_thread=False)
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    saver = SqliteSaver(connection)
    graph = build_graph(saver, mode)
    # Keep attempt references in the same durable database. LangGraph models
    # checkpoint ancestry, but it has no first-class attempt-name-to-tip table.
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS branch_forest_attempt_refs (
            instance_id TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            canonical_tip_checkpoint_id TEXT NOT NULL,
            langgraph_tip_checkpoint_id TEXT NOT NULL,
            path_length INTEGER NOT NULL,
            PRIMARY KEY (instance_id, attempt_id)
        )
        """
    )
    connection.commit()
    return connection, saver, graph


def config(thread_id: str, checkpoint_id: str | None = None) -> dict[str, Any]:
    configurable: dict[str, Any] = {"thread_id": thread_id, "checkpoint_ns": ""}
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


def expected_state(node_by_id: dict[str, dict[str, Any]], checkpoint_id: str) -> bytes:
    messages: list[Any] = []
    cursor: str | None = checkpoint_id
    while cursor is not None:
        node = node_by_id[cursor]
        operation = node["operations"]
        if len(operation) != 1 or operation[0].get("op") != "append_message":
            raise RuntimeError(f"unsupported operation at {cursor}")
        messages.append(operation[0]["value"])
        cursor = node["parent_checkpoint_id"]
    messages.reverse()
    return canonical_json_bytes({"messages": messages})


def projected_state(values: dict[str, Any]) -> bytes:
    encoded = values.get("messages", [])
    if not isinstance(encoded, list):
        raise RuntimeError("LangGraph messages channel is not a list")
    messages = [json.loads(value) for value in encoded]
    return canonical_json_bytes({"messages": messages})


def verify(
    *, graph: Any, corpus: list[dict[str, Any]], configs: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    failures = 0
    latencies: list[int] = []
    for instance in corpus:
        node_by_id = {node["checkpoint_id"]: node for node in instance["nodes"]}
        for node in instance["nodes"]:
            checkpoint_id = node["checkpoint_id"]
            started = time.perf_counter_ns()
            actual = projected_state(graph.get_state(configs[checkpoint_id]).values)
            latencies.append(time.perf_counter_ns() - started)
            if (
                len(actual) != int(node["logical_state_len"])
                or sha256_bytes(actual) != node["logical_state_sha256"]
                or actual != expected_state(node_by_id, checkpoint_id)
            ):
                failures += 1
    return {
        "checkpoint_count": len(configs),
        "state_failures": failures,
        "exact": failures == 0,
        "read_latency": latency_stats(latencies),
    }


def load_corpus(path: Path) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        raise RuntimeError("empty branch-forest corpus")
    return rows


def recover_configs(
    saver: SqliteSaver, corpus: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    recovered: dict[str, dict[str, Any]] = {}
    for instance in corpus:
        thread_id = str(instance["instance_id"])
        for checkpoint in saver.list(config(thread_id)):
            logical_id = checkpoint.metadata.get("canonical_checkpoint_id")
            if logical_id is None or checkpoint.metadata.get("source") != "loop":
                continue
            logical_id = str(logical_id)
            if logical_id in recovered:
                raise RuntimeError(f"duplicate LangGraph loop checkpoint for {logical_id}")
            recovered[logical_id] = checkpoint.config
    return recovered


def package_version(distribution: str) -> str:
    try:
        return importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return "not-installed"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--mode", choices=("full", "delta"), required=True)
    args = parser.parse_args()

    corpus_path = args.corpus.resolve()
    root = args.db.resolve()
    corpus = load_corpus(corpus_path)
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    db_path = root / "langgraph.sqlite"

    empty_connection, _, _ = open_graph(db_path, args.mode)
    empty_connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    empty_connection.close()
    empty = storage(root)

    connection, saver, graph = open_graph(db_path, args.mode)
    checkpoint_configs: dict[str, dict[str, Any]] = {}
    append_latencies: list[int] = []
    attempt_ref_count = 0
    for instance in corpus:
        thread_id = str(instance["instance_id"])
        for node in instance["nodes"]:
            checkpoint_id = str(node["checkpoint_id"])
            parent_id = node["parent_checkpoint_id"]
            parent_config = (
                config(thread_id)
                if parent_id is None
                else checkpoint_configs[str(parent_id)]
            )
            operation = node["operations"]
            if len(operation) != 1 or operation[0].get("op") != "append_message":
                raise RuntimeError(f"unsupported operation at {checkpoint_id}")
            message = canonical_json_bytes(operation[0]["value"]).decode("utf-8")
            invocation_config = dict(parent_config)
            invocation_config["metadata"] = {
                "canonical_checkpoint_id": checkpoint_id
            }
            started = time.perf_counter_ns()
            graph.invoke({"messages": [message]}, invocation_config)
            append_latencies.append(time.perf_counter_ns() - started)
            saved = graph.get_state(config(thread_id)).config
            checkpoint_configs[checkpoint_id] = saved

        for attempt in instance["attempts"]:
            path = attempt["checkpoint_path"]
            connection.execute(
                """
                INSERT INTO branch_forest_attempt_refs
                    (instance_id, attempt_id, canonical_tip_checkpoint_id,
                     langgraph_tip_checkpoint_id, path_length)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    thread_id,
                    attempt["attempt_id"],
                    path[-1],
                    checkpoint_configs[path[-1]]["configurable"]["checkpoint_id"],
                    len(path),
                ),
            )
            attempt_ref_count += 1
    connection.commit()
    active = storage(root)
    active_verification = verify(
        graph=graph, corpus=corpus, configs=checkpoint_configs
    )
    checkpoint_rows = connection.execute("SELECT count(*) FROM checkpoints").fetchone()[0]
    write_rows = connection.execute("SELECT count(*) FROM writes").fetchone()[0]
    ref_rows = connection.execute(
        "SELECT count(*) FROM branch_forest_attempt_refs"
    ).fetchone()[0]
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    checkpointed = storage(root)
    connection.close()

    reopened_connection, reopened_saver, reopened_graph = open_graph(db_path, args.mode)
    reopened_configs = recover_configs(reopened_saver, corpus)
    reopened_verification = verify(
        graph=reopened_graph, corpus=corpus, configs=reopened_configs
    )
    reopened = storage(root)
    reopened_connection.close()

    exact = (
        active_verification["exact"]
        and reopened_verification["exact"]
        and len(reopened_configs) == len(checkpoint_configs)
        and checkpoint_rows == 2 * len(checkpoint_configs)
        and ref_rows == attempt_ref_count
    )
    result = {
        "format_version": 1,
        "contract": "TULYA_BRANCH_FOREST_BENCHMARK_V1",
        "backend": f"langgraph-sqlite-{args.mode}",
        "mode": args.mode,
        "checkpoint_node_count": len(checkpoint_configs),
        "attempt_ref_count": attempt_ref_count,
        "physical_checkpoint_rows": checkpoint_rows,
        "physical_write_rows": write_rows,
        "pass": exact,
        "claims": {
            "performance_claim": False,
            "storage_advantage_claim": False,
            "holdout_accessed": args.corpus.name == "holdout.jsonl",
        },
        "durability": {
            "journal_mode": "WAL",
            "synchronous": "FULL",
            "transaction_per_checkpoint": "LangGraph invoke public API",
            "wal_autocheckpoint": 0,
        },
        "versions": {
            "langgraph": package_version("langgraph"),
            "langgraph_checkpoint": package_version("langgraph-checkpoint"),
            "langgraph_checkpoint_sqlite": package_version(
                "langgraph-checkpoint-sqlite"
            ),
            "sqlite": sqlite3.sqlite_version,
        },
        "latency": {"durable_append": latency_stats(append_latencies)},
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
    if not exact:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
