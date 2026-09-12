#!/usr/bin/env python3
"""Durable CoW vs Tulya on the large branching-state workload.

This is a fail-fast engineering control. The CoW arm publishes each version by:

1. appending any fresh 4 KiB chunks and path-copied tree nodes;
2. fsyncing the chunk and node files;
3. appending a fixed-width (version,parent,root) record; and
4. fsyncing the root log before the update is considered complete.

That gives a clear durable publication point, but deliberately does NOT claim
Tulya-equivalent integrity: there are no per-record commitments, WAL protocol,
manifest generations, corruption detection, or crash-fuzz proof here.
"""
from __future__ import annotations

import argparse
from array import array
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import time

# Running this file directly places its directory on sys.path.
import run as bench

ROOT_RECORD = struct.Struct("<QQQ")  # version, parent, root node id


def percentile(values: list[int], p: int) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[(len(ordered) - 1) * p // 100]


def latency(values: list[int]) -> dict:
    return {
        "count": len(values),
        "p50_ns": percentile(values, 50),
        "p95_ns": percentile(values, 95),
        "p99_ns": percentile(values, 99),
        "max_ns": max(values, default=0),
    }


class DurableCowTree:
    """4 KiB persistent CoW tree with ordered per-version fsync publication."""

    def __init__(self, directory: Path, base_path: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory
        self.chunk_path = directory / "chunks.bin"
        self.node_path = directory / "nodes.bin"
        self.root_path = directory / "roots.log"
        self.chunk_file = self.chunk_path.open("w+b")
        self.node_file = self.node_path.open("w+b")
        self.root_file = self.root_path.open("w+b")
        self.left = array("Q")
        self.right = array("Q")
        self.roots = array("Q")
        self.by_hash: dict[bytes, int] = {}

        count = base_path.stat().st_size // bench.CHUNK
        if count <= 0 or count & (count - 1):
            raise ValueError("durable CoW requires a power-of-two number of 4 KiB chunks")
        self.depth = count.bit_length() - 1

        leaves: list[int] = []
        with base_path.open("rb") as source:
            for _ in range(count):
                data = source.read(bench.CHUNK)
                if len(data) != bench.CHUNK:
                    raise RuntimeError("short base chunk")
                leaves.append(self.node(self.intern(data), bench.LEAF))
        level = leaves
        while len(level) > 1:
            level = [self.node(level[i], level[i + 1]) for i in range(0, len(level), 2)]
        root = level[0]
        self.roots.append(root)
        self._sync_content()
        self.root_file.write(ROOT_RECORD.pack(0, bench.MASK64, root))
        self.root_file.flush()
        os.fsync(self.root_file.fileno())

    def node(self, left: int, right: int) -> int:
        node_id = len(self.left)
        self.left.append(left)
        self.right.append(right)
        self.node_file.write(struct.pack("<QQ", left, right))
        return node_id

    def intern(self, data: bytes) -> int:
        digest = bench.hashlib.sha256(data).digest()
        found = self.by_hash.get(digest)
        if found is not None:
            self.chunk_file.seek(found * bench.CHUNK)
            if self.chunk_file.read(bench.CHUNK) == data:
                return found
        chunk_id = len(self.by_hash)
        self.chunk_file.seek(0, os.SEEK_END)
        self.chunk_file.write(data)
        self.by_hash[digest] = chunk_id
        return chunk_id

    def replace_contiguous(
        self,
        node: int,
        depth: int,
        start: int,
        first: int,
        chunks: list[int],
    ) -> int:
        end = first + len(chunks)
        span = 1 << depth
        if end <= start or first >= start + span:
            return node
        if depth == 0:
            return self.node(chunks[start - first], bench.LEAF)
        half = span >> 1
        left = self.left[node]
        right = self.right[node]
        new_left = self.replace_contiguous(left, depth - 1, start, first, chunks)
        new_right = self.replace_contiguous(right, depth - 1, start + half, first, chunks)
        return self.node(new_left, new_right)

    def _sync_content(self) -> None:
        self.chunk_file.flush()
        self.node_file.flush()
        os.fsync(self.chunk_file.fileno())
        os.fsync(self.node_file.fileno())

    def replace_durable(self, op: bench.Op, data: bytes) -> None:
        first = op.offset // bench.CHUNK
        chunks = [
            self.intern(data[i:i + bench.CHUNK])
            for i in range(0, len(data), bench.CHUNK)
        ]
        root = self.replace_contiguous(
            self.roots[op.parent], self.depth, 0, first, chunks
        )
        self._sync_content()
        self.root_file.write(ROOT_RECORD.pack(op.version, op.parent, root))
        self.root_file.flush()
        os.fsync(self.root_file.fileno())
        self.roots.append(root)

    def close(self) -> None:
        for handle in (self.chunk_file, self.node_file, self.root_file):
            if not handle.closed:
                handle.close()

    def storage_bytes(self) -> int:
        for handle in (self.chunk_file, self.node_file, self.root_file):
            handle.flush()
        return sum(
            path.stat().st_size
            for path in (self.chunk_path, self.node_path, self.root_path)
        )


class ReopenedDurableCow:
    def __init__(self, directory: Path, depth: int) -> None:
        self.directory = directory
        self.depth = depth
        self.chunk_file = (directory / "chunks.bin").open("rb")
        self.left = array("Q")
        self.right = array("Q")
        self.roots = array("Q")

        with (directory / "nodes.bin").open("rb") as nodes:
            while True:
                record = nodes.read(16)
                if not record:
                    break
                if len(record) != 16:
                    raise RuntimeError("durable CoW node tail is torn")
                left, right = struct.unpack("<QQ", record)
                self.left.append(left)
                self.right.append(right)

        with (directory / "roots.log").open("rb") as roots:
            expected_version = 0
            while True:
                record = roots.read(ROOT_RECORD.size)
                if not record:
                    break
                if len(record) != ROOT_RECORD.size:
                    # An interrupted *uncommitted* append is ignored. Every
                    # acknowledged version ended with fsync of a full record.
                    break
                version, parent, root = ROOT_RECORD.unpack(record)
                if version != expected_version:
                    raise RuntimeError("durable CoW root log is not dense")
                if version == 0:
                    if parent != bench.MASK64:
                        raise RuntimeError("durable CoW root parent is invalid")
                elif parent >= version:
                    raise RuntimeError("durable CoW parent is not prior")
                if root >= len(self.left):
                    raise RuntimeError("durable CoW root references absent node")
                self.roots.append(root)
                expected_version += 1

    def chunk_for(self, root: int, index: int) -> int:
        node = root
        for level in range(self.depth - 1, -1, -1):
            node = self.right[node] if ((index >> level) & 1) else self.left[node]
        if self.right[node] != bench.LEAF:
            raise RuntimeError("bad durable CoW leaf")
        return self.left[node]

    def read_range(self, version: int, offset: int, length: int) -> bytes:
        out = bytearray()
        end = offset + length
        pos = offset
        while pos < end:
            chunk_index = pos // bench.CHUNK
            within = pos % bench.CHUNK
            take = min(bench.CHUNK - within, end - pos)
            chunk_id = self.chunk_for(self.roots[version], chunk_index)
            self.chunk_file.seek(chunk_id * bench.CHUNK + within)
            data = self.chunk_file.read(take)
            if len(data) != take:
                raise RuntimeError("short durable CoW chunk read")
            out.extend(data)
            pos += take
        return bytes(out)

    def close(self) -> None:
        self.chunk_file.close()


def durable_cow(
    case: Path,
    base_path: Path,
    edits_path: Path,
    ops: list[bench.Op],
    edit_bytes: int,
    floor: int,
) -> dict:
    directory = case / "durable-cow-tree"
    tree = DurableCowTree(directory, base_path)
    update_times: list[int] = []
    with edits_path.open("rb") as edits:
        for op in ops:
            edits.seek((op.version - 1) * edit_bytes)
            payload = edits.read(edit_bytes)
            if len(payload) != edit_bytes:
                raise RuntimeError("short edit corpus read")
            started = time.perf_counter_ns()
            tree.replace_durable(op, payload)
            update_times.append(time.perf_counter_ns() - started)
    storage = tree.storage_bytes()
    depth = tree.depth
    node_count = len(tree.left)
    chunk_count = len(tree.by_hash)
    tree.close()

    started = time.perf_counter_ns()
    reopened = ReopenedDurableCow(directory, depth)
    reopen_ns = time.perf_counter_ns() - started
    failures = 0
    read4_times: list[int] = []
    read_edit_times: list[int] = []
    with edits_path.open("rb") as edits:
        for version in bench.sampled_versions(len(ops)):
            op = ops[version - 1]
            edits.seek((version - 1) * edit_bytes)
            expected = edits.read(edit_bytes)
            started = time.perf_counter_ns()
            actual4 = reopened.read_range(version, op.offset, bench.CHUNK)
            read4_times.append(time.perf_counter_ns() - started)
            started = time.perf_counter_ns()
            actual = reopened.read_range(version, op.offset, edit_bytes)
            read_edit_times.append(time.perf_counter_ns() - started)
            failures += int(actual4 != expected[:bench.CHUNK])
            failures += int(actual != expected)
    reopened.close()

    return {
        "backend": "durable-chunked-cow-tree-4k",
        "exact": failures == 0,
        "verification_failures": failures,
        "storage_bytes": storage,
        "storage_ratio_to_changed_payload_floor": storage / floor,
        "tree_depth": depth,
        "tree_nodes": node_count,
        "unique_chunks": chunk_count,
        "root_record_bytes": ROOT_RECORD.size,
        "update_replace_durable": latency(update_times),
        "reopen_ns": reopen_ns,
        "historical_read_4k": latency(read4_times),
        "historical_read_edit": latency(read_edit_times),
        "durability_protocol": "fsync chunks+nodes, append root record, fsync root log per committed version",
        "integrity_scope": "control only: no commitments, WAL generations, corruption detection, or crash-fuzz validation",
    }


def cleanup(case: Path) -> None:
    for name in ("base.bin", "edits.bin", "durable-cow-tree", "tulya"):
        path = case / name
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-bytes", type=int, default=1 << 30)
    parser.add_argument("--edit-bytes", type=int, default=40 << 10)
    parser.add_argument("--updates", type=int, default=10_000)
    parser.add_argument(
        "--topology",
        choices=["chain", "star", "balanced", "random"],
        default="random",
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmark-results/large-branching-state-durable"),
    )
    parser.add_argument("--cleanup-heavy", action="store_true")
    args = parser.parse_args()

    if args.base_bytes <= 0 or args.base_bytes % bench.CHUNK:
        parser.error("base bytes must be a positive multiple of 4096")
    if args.edit_bytes <= 0 or args.edit_bytes % bench.CHUNK:
        parser.error("edit bytes must be a positive multiple of 4096")
    chunks = args.base_bytes // bench.CHUNK
    if chunks & (chunks - 1):
        parser.error("base must contain a power-of-two number of 4 KiB chunks")
    if args.edit_bytes > args.base_bytes:
        parser.error("edit bytes cannot exceed base bytes")

    repo = Path(__file__).resolve().parents[2]
    output = args.output_dir if args.output_dir.is_absolute() else repo / args.output_dir
    case = output / (
        f"b{args.base_bytes}-e{args.edit_bytes}-m{args.updates}-{args.topology}-s{args.seed}"
    )
    if case.exists():
        shutil.rmtree(case)
    case.mkdir(parents=True)

    floor = args.base_bytes + args.edit_bytes * args.updates
    # Conservative rather than predictive: corpus copies + both backends +
    # temporary/WAL headroom. Refuse early rather than fill the user's disk.
    required = max(4 << 30, floor * 6)
    free = shutil.disk_usage(case).free
    if free < required:
        raise SystemExit(
            f"refusing benchmark: need conservatively {required} free bytes, have {free}"
        )

    base_path = case / "base.bin"
    edits_path = case / "edits.bin"
    history_path = case / "history.jsonl"
    base_sha = bench.ensure_base(base_path, args.base_bytes, args.seed)
    edits_sha = bench.ensure_edits(edits_path, args.edit_bytes, args.updates, args.seed)
    ops = bench.generate_history(
        history_path,
        args.base_bytes,
        args.edit_bytes,
        args.updates,
        args.topology,
        args.seed,
    )
    manifest = {
        "benchmark": "TULYA_LARGE_BRANCHING_STATE_DURABLE_CONTROL_V1",
        "base_bytes": args.base_bytes,
        "edit_bytes": args.edit_bytes,
        "updates": args.updates,
        "versions": args.updates + 1,
        "topology": args.topology,
        "seed": args.seed,
        "base_sha256": base_sha,
        "edits_sha256": edits_sha,
        "changed_payload_floor_bytes": floor,
        "disk_free_bytes_at_start": free,
        "disk_conservative_peak_requirement_bytes": required,
    }
    (case / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )

    print("[durable-control] running durable 4 KiB CoW", file=sys.stderr, flush=True)
    cow = durable_cow(case, base_path, edits_path, ops, args.edit_bytes, floor)

    print("[durable-control] running Tulya", file=sys.stderr, flush=True)
    binary = bench.build_tulya(repo)
    tulya = bench.run_tulya(binary, case, base_path, history_path, edits_path, floor)

    result = {
        "manifest": manifest,
        "durable_cow": cow,
        "tulya": tulya,
        "comparison": {
            "tulya_storage_over_durable_cow": tulya["storage_bytes"] / cow["storage_bytes"],
            "tulya_update_p50_over_durable_cow": (
                tulya["latency"]["update_splice"]["p50_ns"]
                / cow["update_replace_durable"]["p50_ns"]
            ),
            "scope_warning": "CoW control matches ordered fsync publication but not Tulya integrity/crash-detection semantics.",
        },
    }
    (case / "result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    summary = {
        "benchmark": "TULYA_LARGE_BRANCHING_STATE_DURABLE_CONTROL_V1",
        "results": [result],
    }
    output.mkdir(parents=True, exist_ok=True)
    summary_path = output / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

    if args.cleanup_heavy:
        cleanup(case)
    print(summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
