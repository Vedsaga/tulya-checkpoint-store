#!/usr/bin/env python3
"""Reproducible storage smoke: Tulya vs durable SQLite delta vs full snapshots.

This synthetic workload is a release regression test, not claim-bearing
evidence. The frozen public-corpus evidence and stronger comparator matrix are
documented in ``docs/BENCHMARKS.md``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import time
import zlib
from pathlib import Path
from typing import Any


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()


def allocated_bytes(path: Path) -> int:
    return sum(
        entry.stat().st_blocks * 512
        for entry in path.rglob("*")
        if entry.is_file()
    )


def deterministic_text(label: str, size: int) -> str:
    output = bytearray()
    counter = 0
    while len(output) < size:
        output.extend(hashlib.sha256(f"{label}:{counter}".encode()).hexdigest().encode())
        counter += 1
    return output[:size].decode()


def workload(
    threads: int, branches_per_thread: int, base_bytes: int, delta_bytes: int
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for thread in range(threads):
        thread_id = f"thread-{thread:04}"
        base = deterministic_text(f"base-{thread}", base_bytes)
        rows.append(
            {
                "thread_id": thread_id,
                "checkpoint_id": "root",
                "checkpoint_no": 0,
                "parent_checkpoint_id": None,
                "messages": [base],
                "delta": [base],
            }
        )
        for branch in range(branches_per_thread):
            delta = deterministic_text(f"delta-{thread}-{branch}", delta_bytes)
            rows.append(
                {
                    "thread_id": thread_id,
                    "checkpoint_id": f"branch-{branch:06}",
                    "checkpoint_no": 1,
                    "parent_checkpoint_id": "root",
                    "messages": [base, delta],
                    "delta": [delta],
                }
            )
    return rows


def run_tulya(binary: Path, root: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    empty = root / "empty"
    store = root / "store"
    subprocess.run([binary, "--db", empty, "stats"], check=True, stdout=subprocess.PIPE)
    empty_bytes = allocated_bytes(empty)
    export = {
        "format": "tulya-append-only-message-export",
        "format_version": 1,
        "checkpoints": [
            {
                "thread_id": row["thread_id"],
                "checkpoint_id": row["checkpoint_id"],
                "checkpoint_no": row["checkpoint_no"],
                "parent_checkpoint_id": row["parent_checkpoint_id"],
                "state": {"identity": None, "messages": row["messages"]},
            }
            for row in rows
        ],
    }
    started = time.perf_counter_ns()
    imported = subprocess.run(
        [binary, "--db", store, "import"],
        input=canonical(export),
        check=True,
        stdout=subprocess.PIPE,
    )
    subprocess.run(
        [binary, "--db", store, "seal"], check=True, stdout=subprocess.PIPE
    )
    elapsed_ns = time.perf_counter_ns() - started
    result = json.loads(imported.stdout)
    verified = json.loads(
        subprocess.run(
            [binary, "--db", store, "fsck"], check=True, stdout=subprocess.PIPE
        ).stdout
    )
    total = allocated_bytes(store)
    return {
        "checkpoint_count": result["checkpoint_count"],
        "exact": verified["failures"] == 0,
        "total_allocated_bytes": total,
        "empty_allocated_bytes": empty_bytes,
        "marginal_allocated_bytes": total - empty_bytes,
        "import_and_seal_elapsed_ns": elapsed_ns,
    }


def setup_sqlite(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute(
        "CREATE TABLE checkpoints (thread TEXT, id TEXT, parent TEXT, delta BLOB, "
        "state_sha256 BLOB, PRIMARY KEY(thread,id)) WITHOUT ROWID"
    )
    connection.commit()
    return connection


def run_sqlite_delta(root: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    empty_root = root / "empty"
    store_root = root / "store"
    empty_root.mkdir(parents=True)
    store_root.mkdir(parents=True)
    empty = setup_sqlite(empty_root / "history.sqlite")
    empty.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    empty.close()
    empty_bytes = allocated_bytes(empty_root)

    connection = setup_sqlite(store_root / "history.sqlite")
    started = time.perf_counter_ns()
    expected: dict[tuple[str, str], bytes] = {}
    for row in rows:
        state = canonical({"identity": None, "messages": row["messages"]})
        connection.execute(
            "INSERT INTO checkpoints VALUES (?,?,?,?,?)",
            (
                row["thread_id"],
                row["checkpoint_id"],
                row["parent_checkpoint_id"],
                canonical(row["delta"]),
                hashlib.sha256(state).digest(),
            ),
        )
        connection.commit()
        expected[(row["thread_id"], row["checkpoint_id"])] = state
    elapsed_ns = time.perf_counter_ns() - started
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    connection.close()

    reopened = sqlite3.connect(store_root / "history.sqlite")
    exact = True
    for key, wanted in expected.items():
        chain: list[list[str]] = []
        cursor: str | None = key[1]
        while cursor is not None:
            record = reopened.execute(
                "SELECT parent,delta,state_sha256 FROM checkpoints WHERE thread=? AND id=?",
                (key[0], cursor),
            ).fetchone()
            if record is None:
                exact = False
                break
            chain.append(json.loads(record[1]))
            cursor = record[0]
        messages: list[str] = []
        for delta in reversed(chain):
            messages.extend(delta)
        actual = canonical({"identity": None, "messages": messages})
        if actual != wanted:
            exact = False
            break
    reopened.close()
    total = allocated_bytes(store_root)
    return {
        "checkpoint_count": len(rows),
        "exact": exact,
        "total_allocated_bytes": total,
        "empty_allocated_bytes": empty_bytes,
        "marginal_allocated_bytes": total - empty_bytes,
        "durable_ingest_elapsed_ns": elapsed_ns,
        "representation": "parent pointer plus append delta; WAL truncated at equilibrium",
    }


def run_full_snapshots(root: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    root.mkdir(parents=True)
    data_path = root / "snapshots.zlib"
    index_path = root / "index.jsonl"
    expected: list[bytes] = []
    started = time.perf_counter_ns()
    with data_path.open("wb") as data, index_path.open("wb") as index:
        for row in rows:
            state = canonical({"identity": None, "messages": row["messages"]})
            encoded = zlib.compress(state, level=6)
            offset = data.tell()
            data.write(encoded)
            data.flush()
            os.fsync(data.fileno())
            index.write(
                canonical(
                    {
                        "offset": offset,
                        "length": len(encoded),
                        "sha256": hashlib.sha256(state).hexdigest(),
                    }
                )
                + b"\n"
            )
            index.flush()
            os.fsync(index.fileno())
            expected.append(state)
    elapsed_ns = time.perf_counter_ns() - started
    exact = True
    with data_path.open("rb") as data:
        for line, wanted in zip(index_path.read_bytes().splitlines(), expected):
            record = json.loads(line)
            data.seek(record["offset"])
            actual = zlib.decompress(data.read(record["length"]))
            if actual != wanted or hashlib.sha256(actual).hexdigest() != record["sha256"]:
                exact = False
                break
    return {
        "checkpoint_count": len(rows),
        "exact": exact,
        "total_allocated_bytes": allocated_bytes(root),
        "empty_allocated_bytes": 0,
        "marginal_allocated_bytes": allocated_bytes(root),
        "durable_ingest_elapsed_ns": elapsed_ns,
        "representation": "independently compressed full state; one fsync per data and index append",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/tulya-checkpoint"))
    parser.add_argument("--output", type=Path, default=Path("benchmark-results/release-smoke"))
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--branches-per-thread", type=int, default=100)
    parser.add_argument("--base-bytes", type=int, default=64 * 1024)
    parser.add_argument("--delta-bytes", type=int, default=4 * 1024)
    args = parser.parse_args()
    if args.output.exists():
        shutil.rmtree(args.output)
    args.output.mkdir(parents=True)
    rows = workload(
        args.threads, args.branches_per_thread, args.base_bytes, args.delta_bytes
    )
    result = {
        "format_version": 1,
        "claim_bearing": False,
        "workload": {
            "threads": args.threads,
            "branches_per_thread": args.branches_per_thread,
            "base_bytes": args.base_bytes,
            "delta_bytes": args.delta_bytes,
            "checkpoint_count": len(rows),
        },
        "arms": {
            "tulya": run_tulya(args.binary.resolve(), args.output / "tulya", rows),
            "sqlite_delta": run_sqlite_delta(args.output / "sqlite-delta", rows),
            "full_snapshots_zlib": run_full_snapshots(
                args.output / "full-snapshots", rows
            ),
        },
        "limitations": [
            "synthetic shallow-fork workload",
            "storage and exactness regression only; ingest timings are not symmetric",
            "OS cache uncontrolled",
            "use docs/BENCHMARKS.md for frozen public-corpus evidence and losses",
        ],
    }
    result["pass"] = all(arm["exact"] for arm in result["arms"].values())
    output = args.output / "summary.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
