#!/usr/bin/env python3
"""Direct LangGraph PostgreSQL baseline for the natural branch forest."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import psycopg
from langgraph.checkpoint.postgres import PostgresSaver

from langgraph_sqlite import (
    build_graph,
    canonical_json_bytes,
    config,
    latency_stats,
    load_corpus,
    package_version,
    projected_state,
    recover_configs,
    sha256_bytes,
)


POSTGRES_TABLES = (
    "checkpoint_migrations",
    "checkpoints",
    "checkpoint_blobs",
    "checkpoint_writes",
    "branch_forest_attempt_refs",
)


def run(command: list[str], cwd: Path, *, check: bool = True) -> None:
    subprocess.run(
        command,
        cwd=cwd,
        check=check,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def wait_for_postgres(url: str, timeout_seconds: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(url, autocommit=True) as connection:
                connection.execute("SELECT 1")
            return
        except Exception as error:  # pragma: no cover - service startup race
            last_error = error
            time.sleep(0.25)
    raise RuntimeError(f"PostgreSQL did not become ready: {last_error}")


def scalar(connection: psycopg.Connection[Any], sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = connection.execute(sql, params).fetchone()
    return None if row is None else row[0]


def relation_storage(connection: psycopg.Connection[Any]) -> dict[str, Any]:
    tables: list[dict[str, Any]] = []
    total = 0
    for table in POSTGRES_TABLES:
        relation = f"public.{table}"
        exists = scalar(connection, "SELECT to_regclass(%s)::text", (relation,))
        size = (
            0
            if exists is None
            else int(
                scalar(
                    connection,
                    "SELECT pg_total_relation_size(%s::regclass)",
                    (relation,),
                )
            )
        )
        tables.append({"table": table, "exists": exists is not None, "bytes": size})
        total += size
    return {
        "allocated_bytes": total,
        "file_length_bytes": total,
        "file_count": None,
        "tables": tables,
        "scope": "pg_total_relation_size: heap + TOAST + indexes",
    }


def lsn(connection: psycopg.Connection[Any]) -> str:
    return str(scalar(connection, "SELECT pg_current_wal_lsn()::text"))


def lsn_diff(connection: psycopg.Connection[Any], newer: str, older: str) -> int:
    return int(
        scalar(
            connection,
            "SELECT pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)",
            (newer, older),
        )
    )


def postgres_settings(connection: psycopg.Connection[Any]) -> dict[str, str]:
    names = (
        "server_version",
        "fsync",
        "synchronous_commit",
        "full_page_writes",
        "wal_compression",
    )
    return {name: str(scalar(connection, f"SHOW {name}")) for name in names}


def verify(
    graph: Any,
    corpus: list[dict[str, Any]],
    configs: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    failures = 0
    failure_samples: list[dict[str, Any]] = []
    reads: list[int] = []
    checkpoint_count = 0
    for instance in corpus:
        for node in instance["nodes"]:
            checkpoint_id = str(node["checkpoint_id"])
            started = time.perf_counter_ns()
            raw = projected_state(graph.get_state(configs[checkpoint_id]).values)
            reads.append(time.perf_counter_ns() - started)
            checkpoint_count += 1
            actual_hash = sha256_bytes(raw)
            if (
                len(raw) != int(node["logical_state_len"])
                or actual_hash != node["logical_state_sha256"]
            ):
                failures += 1
                if len(failure_samples) < 10:
                    failure_samples.append(
                        {
                            "instance_id": instance["instance_id"],
                            "checkpoint_id": checkpoint_id,
                            "parent_checkpoint_id": node["parent_checkpoint_id"],
                            "expected_len": int(node["logical_state_len"]),
                            "actual_len": len(raw),
                            "expected_sha256": node["logical_state_sha256"],
                            "actual_sha256": actual_hash,
                        }
                    )
    return {
        "checkpoint_count": checkpoint_count,
        "state_failures": failures,
        "failure_samples": failure_samples,
        "exact": failures == 0,
        "read_latency": latency_stats(reads),
    }


def run_backend(
    *, corpus: list[dict[str, Any]], url: str, mode: str, holdout_accessed: bool
) -> dict[str, Any]:
    with PostgresSaver.from_conn_string(url) as saver:
        saver.setup()
        graph = build_graph(saver, mode)
        with psycopg.connect(url, autocommit=True) as connection:
            connection.execute(
                """
                CREATE TABLE branch_forest_attempt_refs(
                    instance_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    canonical_tip_checkpoint_id TEXT NOT NULL,
                    langgraph_tip_checkpoint_id TEXT NOT NULL,
                    path_length INTEGER NOT NULL,
                    PRIMARY KEY(instance_id, attempt_id)
                )
                """
            )
            empty = relation_storage(connection)
            baseline_database_bytes = int(
                scalar(connection, "SELECT pg_database_size(current_database())")
            )
            baseline_lsn = lsn(connection)
            settings = postgres_settings(connection)

        configs: dict[str, dict[str, Any]] = {}
        writes: list[int] = []
        attempt_count = 0
        for instance in corpus:
            thread_id = str(instance["instance_id"])
            for node in instance["nodes"]:
                checkpoint_id = str(node["checkpoint_id"])
                parent_id = node["parent_checkpoint_id"]
                parent_config = (
                    config(thread_id)
                    if parent_id is None
                    else configs[str(parent_id)]
                )
                operations = node["operations"]
                if len(operations) != 1 or operations[0].get("op") != "append_message":
                    raise RuntimeError(f"unsupported operation at {checkpoint_id}")
                message = canonical_json_bytes(operations[0]["value"]).decode("utf-8")
                invocation_config = dict(parent_config)
                invocation_config["metadata"] = {
                    "canonical_checkpoint_id": checkpoint_id
                }
                started = time.perf_counter_ns()
                graph.invoke({"messages": [message]}, invocation_config)
                writes.append(time.perf_counter_ns() - started)
                configs[checkpoint_id] = graph.get_state(config(thread_id)).config

            refs = []
            for attempt in instance["attempts"]:
                path = attempt["checkpoint_path"]
                refs.append(
                    (
                        thread_id,
                        attempt["attempt_id"],
                        path[-1],
                        configs[path[-1]]["configurable"]["checkpoint_id"],
                        len(path),
                    )
                )
                attempt_count += 1
            with psycopg.connect(url) as connection:
                with connection.cursor() as cursor:
                    cursor.executemany(
                        """
                        INSERT INTO branch_forest_attempt_refs VALUES (%s,%s,%s,%s,%s)
                        """,
                        refs,
                    )

        active_verification = verify(graph, corpus, configs)
        with psycopg.connect(url, autocommit=True) as connection:
            active = relation_storage(connection)
            workload_lsn = lsn(connection)
            checkpoint_started = time.perf_counter_ns()
            connection.execute("CHECKPOINT")
            checkpoint_duration_ns = time.perf_counter_ns() - checkpoint_started
            checkpointed_lsn = lsn(connection)
            checkpointed = relation_storage(connection)
            checkpoint_rows = int(scalar(connection, "SELECT count(*) FROM checkpoints"))
            write_rows = int(
                scalar(connection, "SELECT count(*) FROM checkpoint_writes")
            )
            attempt_rows = int(
                scalar(connection, "SELECT count(*) FROM branch_forest_attempt_refs")
            )
            database_bytes = int(
                scalar(connection, "SELECT pg_database_size(current_database())")
            )
            wal_workload = lsn_diff(connection, workload_lsn, baseline_lsn)
            wal_checkpoint = lsn_diff(connection, checkpointed_lsn, workload_lsn)

    with PostgresSaver.from_conn_string(url) as reopened_saver:
        reopened_graph = build_graph(reopened_saver, mode)
        reopened_configs = recover_configs(reopened_saver, corpus)
        reopened_verification = verify(reopened_graph, corpus, reopened_configs)
        with psycopg.connect(url, autocommit=True) as connection:
            reopened = relation_storage(connection)

    logical_count = len(configs)
    passed = (
        active_verification["exact"]
        and reopened_verification["exact"]
        and len(reopened_configs) == logical_count
        and checkpoint_rows == 2 * logical_count
        and attempt_rows == attempt_count
    )
    return {
        "format_version": 1,
        "contract": "TULYA_BRANCH_FOREST_BENCHMARK_V1",
        "backend": f"langgraph-postgres-{mode}",
        "mode": mode,
        "checkpoint_node_count": logical_count,
        "attempt_ref_count": attempt_count,
        "physical_checkpoint_rows": checkpoint_rows,
        "physical_write_rows": write_rows,
        "pass": passed,
        "claims": {
            "performance_claim": False,
            "storage_advantage_claim": False,
            "holdout_accessed": holdout_accessed,
        },
        "durability": {
            "fsync": settings["fsync"],
            "synchronous_commit": settings["synchronous_commit"],
            "full_page_writes": settings["full_page_writes"],
        },
        "versions": {
            "postgres": settings["server_version"],
            "langgraph": package_version("langgraph"),
            "langgraph_checkpoint_postgres": package_version(
                "langgraph-checkpoint-postgres"
            ),
        },
        "latency": {
            "durable_append": latency_stats(writes),
            "checkpoint_duration_ns": checkpoint_duration_ns,
        },
        "exactness": {
            "active": active_verification,
            "reopened": reopened_verification,
        },
        "storage": {
            "empty": empty,
            "active": active,
            "checkpointed": checkpointed,
            "reopened": reopened,
            "database_size_baseline_bytes": baseline_database_bytes,
            "database_size_final_bytes": database_bytes,
            "wal_generated_workload_bytes": wal_workload,
            "wal_generated_checkpoint_bytes": wal_checkpoint,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--mode", choices=("full", "delta"), default="delta")
    parser.add_argument(
        "--postgres-url",
        default="postgresql://postgres:postgres@127.0.0.1:55432/postgres?sslmode=disable",
    )
    args = parser.parse_args()

    bench = Path(__file__).resolve().parent
    corpus = load_corpus(args.corpus.resolve())
    run(["docker", "compose", "down", "-v", "--remove-orphans"], bench, check=False)
    try:
        run(["docker", "compose", "up", "-d", "postgres"], bench)
        wait_for_postgres(args.postgres_url)
        result = run_backend(
            corpus=corpus,
            url=args.postgres_url,
            mode=args.mode,
            holdout_accessed=args.corpus.name == "holdout.jsonl",
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        if not result["pass"]:
            raise SystemExit(1)
    finally:
        run(
            ["docker", "compose", "down", "-v", "--remove-orphans"],
            bench,
            check=False,
        )


if __name__ == "__main__":
    main()
