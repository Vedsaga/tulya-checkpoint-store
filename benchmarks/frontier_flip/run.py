#!/usr/bin/env python3
"""TULYA_FRONTIER_FLIP_V1.

A deterministic forced-bit-flip benchmark tied to the Lean finite-family count

    2^n * m! * n^m

for n initial bits and m arbitrary-parent flips.  RANDOM is the theorem-backed
headline topology because it exercises the arbitrary-parent family.  CHAIN,
STAR, and BALANCED are operational stress shapes; their constrained-family
information charge omits the m! parent-choice factor.

The runner emits one summary.json with:
  * exact family information charges;
  * a bit-packed parent+position history-log baseline;
  * an analytical full-snapshot endpoint;
  * a 4 KiB chunk-deduplicated persistent-tree baseline; and
  * the durable Tulya balanced-history arm.

No third-party Python packages are required.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import struct
import subprocess
import sys
import time
from array import array
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

BENCHMARK = "TULYA_FRONTIER_FLIP_V1"
MASK64 = (1 << 64) - 1
CHUNK_SIZE = 4096
LEAF_SENTINEL = MASK64


class SplitMix64:
    def __init__(self, seed: int) -> None:
        self.state = seed & MASK64

    def next(self) -> int:
        self.state = (self.state + 0x9E3779B97F4A7C15) & MASK64
        z = self.state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
        return (z ^ (z >> 31)) & MASK64


@dataclass(frozen=True)
class FlipOp:
    version: int
    parent: int
    bit: int


class BitWriter:
    def __init__(self, path: Path) -> None:
        self.file = path.open("wb")
        self.current = 0
        self.used = 0
        self.bit_count = 0

    def write(self, value: int, width: int) -> None:
        if width < 0 or value < 0 or (width and value >= (1 << width)):
            raise ValueError("value does not fit bit field")
        for shift in range(width - 1, -1, -1):
            self.current = (self.current << 1) | ((value >> shift) & 1)
            self.used += 1
            self.bit_count += 1
            if self.used == 8:
                self.file.write(bytes([self.current]))
                self.current = 0
                self.used = 0

    def close(self) -> None:
        if self.used:
            self.current <<= 8 - self.used
            self.file.write(bytes([self.current]))
        self.file.close()

    def __enter__(self) -> "BitWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def ceil_log2_int(value: int) -> int:
    if value <= 1:
        return 0
    return (value - 1).bit_length()


def theorem_family_info_bits(n_bits: int, updates: int) -> int:
    """ceil(log2(2^n * m! * n^m)) without materialising 2^n."""
    if n_bits <= 0 or updates < 0:
        raise ValueError("n_bits must be positive and updates non-negative")
    if updates == 0:
        return n_bits
    tail = math.factorial(updates) * pow(n_bits, updates)
    return n_bits + ceil_log2_int(tail)


def fixed_parent_family_info_bits(n_bits: int, updates: int) -> int:
    """Exact count when parent topology is externally fixed: 2^n * n^m."""
    if updates == 0:
        return n_bits
    return n_bits + ceil_log2_int(pow(n_bits, updates))


def shake_block(seed: int, block: int, length: int) -> bytes:
    tag = f"{BENCHMARK}:base:{seed}:{block}".encode("ascii")
    return hashlib.shake_256(tag).digest(length)


def ensure_base(path: Path, size: int, seed: int) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if size <= 0 or size % CHUNK_SIZE:
        raise ValueError("base size must be a positive multiple of 4096")
    digest = hashlib.sha256()
    with path.open("wb") as handle:
        remaining = size
        block = 0
        while remaining:
            take = min(1 << 20, remaining)
            payload = shake_block(seed, block, take)
            handle.write(payload)
            digest.update(payload)
            remaining -= take
            block += 1
    return digest.hexdigest()


def parent_for(topology: str, version: int, rng: SplitMix64) -> int:
    if topology == "chain":
        return version - 1
    if topology == "star":
        return 0
    if topology == "balanced":
        return (version - 1) // 2
    if topology == "random":
        return rng.next() % version
    raise ValueError(f"unknown topology: {topology}")


def generate_history(path: Path, n_bits: int, updates: int, topology: str, seed: int) -> list[FlipOp]:
    parent_rng = SplitMix64(seed ^ 0xA0761D6478BD642F)
    bit_rng = SplitMix64(seed ^ 0xE7037ED1A0B428DB)
    ops: list[FlipOp] = []
    with path.open("w", encoding="utf-8") as handle:
        for version in range(1, updates + 1):
            parent = parent_for(topology, version, parent_rng)
            bit = bit_rng.next() % n_bits
            op = FlipOp(version=version, parent=parent, bit=bit)
            ops.append(op)
            handle.write(json.dumps(op.__dict__, sort_keys=True, separators=(",", ":")) + "\n")
    return ops


def load_base_bit(base: bytes, bit: int) -> int:
    return (base[bit // 8] >> (bit % 8)) & 1


def expected_bit(base: bytes, ops: list[FlipOp], version: int, bit: int) -> tuple[int, int]:
    value = load_base_bit(base, bit)
    steps = 0
    while version:
        op = ops[version - 1]
        steps += 1
        if op.bit == bit:
            value ^= 1
        version = op.parent
    return value, steps


def verification_queries(ops: list[FlipOp], n_bits: int, samples: int = 256) -> list[tuple[int, int]]:
    version_count = len(ops) + 1
    count = max(1, min(samples, version_count))
    queries: list[tuple[int, int]] = []
    for index in range(count):
        version = version_count - 1 if count == 1 else index * (version_count - 1) // (count - 1)
        bit = (0x9E3779B97F4A7C15 % n_bits) if version == 0 else ops[version - 1].bit
        queries.append((version, bit))
    return queries


def ratio(storage_bytes: int, info_bits: int) -> float:
    return (storage_bytes * 8) / info_bits


def packed_history_log(case_dir: Path, base_bytes: int, n_bits: int, ops: list[FlipOp], info_bits: int) -> dict:
    path = case_dir / "packed-history-log.bin"
    bit_width = ceil_log2_int(n_bits)
    with BitWriter(path) as writer:
        # Canonical benchmark header; charged as physical overhead.
        for byte in b"TFF1":
            writer.write(byte, 8)
        writer.write(n_bits, 64)
        writer.write(len(ops), 64)
        for op in ops:
            parent_width = ceil_log2_int(op.version)
            writer.write(op.parent, parent_width)
            writer.write(op.bit, bit_width)
        operation_bits = writer.bit_count
    bytes_on_disk = base_bytes + path.stat().st_size
    queries = verification_queries(ops, n_bits)
    steps = [expected_bit(b"\0" * base_bytes, ops, version, bit)[1] for version, bit in queries]
    return {
        "backend": "packed-parent-position-log",
        "storage_bytes": bytes_on_disk,
        "storage_ratio_to_info": ratio(bytes_on_disk, info_bits),
        "encoded_operation_bits_including_header": operation_bits,
        "historical_access_steps": {
            "samples": len(steps),
            "mean": (sum(steps) / len(steps)) if steps else 0.0,
            "max": max(steps, default=0),
        },
        "note": "Near-space endpoint; historical access walks parent edges.",
    }


def snapshot_endpoint(base_bytes: int, updates: int, info_bits: int) -> dict:
    storage = base_bytes * (updates + 1)
    return {
        "backend": "raw-full-snapshot-analytical",
        "storage_bytes": storage,
        "storage_ratio_to_info": ratio(storage, info_bits),
        "historical_access_steps": 1,
        "materialized": False,
        "note": "Exact analytical byte count; intentionally not written to disk.",
    }


class ChunkedPersistentTree:
    """4 KiB chunk-deduplicated path-copy tree.

    This is deliberately simple and transparent rather than a tuned database.
    It represents the standard alternative reviewers will ask about: fixed
    chunks plus a persistent tree of chunk references.  Chunks are SHA-256
    deduplicated; tree nodes are fixed 16-byte (left,right) records and are
    path-copied without node hash-consing.
    """

    def __init__(self, directory: Path, base_path: Path) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.chunk_path = directory / "chunks.bin"
        self.nodes_path = directory / "nodes.bin"
        self.roots_path = directory / "roots.bin"
        self.left = array("Q")
        self.right = array("Q")
        self.roots = array("Q")
        self.digest_to_chunk: dict[bytes, int] = {}
        self.chunk_file = self.chunk_path.open("w+b")

        base_size = base_path.stat().st_size
        if base_size % CHUNK_SIZE:
            raise ValueError("chunked baseline requires a 4096-byte-aligned base")
        self.chunk_count = base_size // CHUNK_SIZE
        if self.chunk_count <= 0 or self.chunk_count & (self.chunk_count - 1):
            raise ValueError("chunked baseline requires a power-of-two chunk count")
        self.depth = self.chunk_count.bit_length() - 1

        leaves: list[int] = []
        with base_path.open("rb") as source:
            for _ in range(self.chunk_count):
                payload = source.read(CHUNK_SIZE)
                if len(payload) != CHUNK_SIZE:
                    raise RuntimeError("short base chunk")
                chunk = self._intern_chunk(payload)
                leaves.append(self._node(chunk, LEAF_SENTINEL))
        level = leaves
        while len(level) > 1:
            level = [self._node(level[i], level[i + 1]) for i in range(0, len(level), 2)]
        self.roots.append(level[0])

    def _node(self, left: int, right: int) -> int:
        node_id = len(self.left)
        self.left.append(left)
        self.right.append(right)
        return node_id

    def _intern_chunk(self, payload: bytes) -> int:
        digest = hashlib.sha256(payload).digest()
        existing = self.digest_to_chunk.get(digest)
        if existing is not None:
            self.chunk_file.seek(existing * CHUNK_SIZE)
            if self.chunk_file.read(CHUNK_SIZE) == payload:
                return existing
        chunk_id = len(self.digest_to_chunk)
        self.chunk_file.seek(0, os.SEEK_END)
        self.chunk_file.write(payload)
        self.digest_to_chunk[digest] = chunk_id
        return chunk_id

    def _chunk_for(self, root: int, chunk_index: int) -> tuple[int, list[tuple[int, int]]]:
        node = root
        path: list[tuple[int, int]] = []
        for level in range(self.depth - 1, -1, -1):
            direction = (chunk_index >> level) & 1
            left = self.left[node]
            right = self.right[node]
            if direction == 0:
                path.append((0, right))
                node = left
            else:
                path.append((1, left))
                node = right
        if self.right[node] != LEAF_SENTINEL:
            raise RuntimeError("chunk tree leaf marker is corrupt")
        return self.left[node], path

    def flip(self, parent_version: int, bit: int) -> None:
        parent_root = self.roots[parent_version]
        byte_index = bit // 8
        chunk_index = byte_index // CHUNK_SIZE
        byte_in_chunk = byte_index % CHUNK_SIZE
        mask = 1 << (bit % 8)
        chunk_id, path = self._chunk_for(parent_root, chunk_index)
        self.chunk_file.seek(chunk_id * CHUNK_SIZE)
        payload = bytearray(self.chunk_file.read(CHUNK_SIZE))
        if len(payload) != CHUNK_SIZE:
            raise RuntimeError("chunk store read is short")
        payload[byte_in_chunk] ^= mask
        new_chunk = self._intern_chunk(bytes(payload))
        node = self._node(new_chunk, LEAF_SENTINEL)
        for direction, sibling in reversed(path):
            node = self._node(node, sibling) if direction == 0 else self._node(sibling, node)
        self.roots.append(node)

    def read_bit(self, version: int, bit: int) -> int:
        byte_index = bit // 8
        chunk_index = byte_index // CHUNK_SIZE
        byte_in_chunk = byte_index % CHUNK_SIZE
        chunk_id, _ = self._chunk_for(self.roots[version], chunk_index)
        self.chunk_file.seek(chunk_id * CHUNK_SIZE + byte_in_chunk)
        byte = self.chunk_file.read(1)
        if len(byte) != 1:
            raise RuntimeError("chunk store bit read is short")
        return (byte[0] >> (bit % 8)) & 1

    def finish(self) -> int:
        self.chunk_file.flush()
        os.fsync(self.chunk_file.fileno())
        self.chunk_file.close()
        with self.nodes_path.open("wb") as handle:
            for left, right in zip(self.left, self.right):
                handle.write(struct.pack("<QQ", left, right))
        with self.roots_path.open("wb") as handle:
            self.roots.tofile(handle)
        return self.chunk_path.stat().st_size + self.nodes_path.stat().st_size + self.roots_path.stat().st_size


def chunked_cow_baseline(case_dir: Path, base_path: Path, base: bytes, ops: list[FlipOp], n_bits: int, info_bits: int) -> dict:
    directory = case_dir / "chunked-cow-tree"
    started = time.perf_counter_ns()
    tree = ChunkedPersistentTree(directory, base_path)
    for op in ops:
        tree.flip(op.parent, op.bit)
    build_ns = time.perf_counter_ns() - started
    queries = verification_queries(ops, n_bits)
    failures = 0
    read_times: list[int] = []
    for version, bit in queries:
        expected, _ = expected_bit(base, ops, version, bit)
        before = time.perf_counter_ns()
        actual = tree.read_bit(version, bit)
        read_times.append(time.perf_counter_ns() - before)
        failures += int(actual != expected)
    node_count = len(tree.left)
    chunk_count = len(tree.digest_to_chunk)
    storage = tree.finish()
    return {
        "backend": "chunked-cow-tree-4k",
        "exact": failures == 0,
        "verification_failures": failures,
        "storage_bytes": storage,
        "storage_ratio_to_info": ratio(storage, info_bits),
        "chunk_size": CHUNK_SIZE,
        "unique_chunks": chunk_count,
        "tree_nodes": node_count,
        "tree_depth": tree.depth,
        "build_ns": build_ns,
        "historical_bit_read_ns": {
            "samples": len(read_times),
            "p50": sorted(read_times)[len(read_times) // 2] if read_times else 0,
            "max": max(read_times, default=0),
        },
        "note": "4 KiB chunk dedup + persistent path-copy tree; no per-update fsync/WAL.",
    }


def build_tulya(repo_root: Path) -> Path:
    subprocess.run(
        ["cargo", "build", "--release", "--locked", "--example", "frontier_flip_tulya"],
        cwd=repo_root,
        check=True,
    )
    suffix = ".exe" if os.name == "nt" else ""
    binary = repo_root / "target" / "release" / "examples" / f"frontier_flip_tulya{suffix}"
    if not binary.exists():
        raise FileNotFoundError(binary)
    return binary


def run_tulya(binary: Path, case_dir: Path, base_path: Path, history_path: Path, info_bits: int) -> dict:
    db = case_dir / "tulya"
    command = [
        str(binary),
        "--db",
        str(db),
        "--base",
        str(base_path),
        "--history",
        str(history_path),
        "--fresh",
    ]
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    result = json.loads(completed.stdout)
    storage = int(result["storage"]["post_seal_file_bytes"])
    result["storage_bytes"] = storage
    result["storage_ratio_to_info"] = ratio(storage, info_bits)
    result["command"] = command
    return result


def case_manifest(base_bytes: int, updates: int, topology: str, seed: int, base_sha256: str) -> dict:
    n_bits = base_bytes * 8
    theorem_bits = theorem_family_info_bits(n_bits, updates)
    topology_bits = theorem_bits if topology == "random" else fixed_parent_family_info_bits(n_bits, updates)
    return {
        "benchmark": BENCHMARK,
        "base_bytes": base_bytes,
        "base_bits": n_bits,
        "updates": updates,
        "versions": updates + 1,
        "topology": topology,
        "seed": seed,
        "base_sha256": base_sha256,
        "lean_arbitrary_parent_family_info_bits": theorem_bits,
        "topology_family_info_bits": topology_bits,
        "headline_ratio_is_lean_theorem_backed": topology == "random",
        "lean_count": "2^n * m! * n^m",
        "fixed_parent_count": "2^n * n^m",
    }


def run_case(
    repo_root: Path,
    binary: Path,
    output_root: Path,
    base_bytes: int,
    updates: int,
    topology: str,
    seed: int,
) -> dict:
    name = f"b{base_bytes}-m{updates}-{topology}-s{seed}"
    case_dir = output_root / name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    base_path = case_dir / "base.bin"
    history_path = case_dir / "history.jsonl"
    base_sha = ensure_base(base_path, base_bytes, seed)
    ops = generate_history(history_path, base_bytes * 8, updates, topology, seed)
    manifest = case_manifest(base_bytes, updates, topology, seed, base_sha)
    (case_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    # Verification and the chunked comparator need the bytes in memory.  The
    # full profile is intentionally demanding; users can run cases separately
    # if 1 GiB resident base state is too large for their machine.
    base = base_path.read_bytes()
    denominator = int(manifest["topology_family_info_bits"])

    baselines = [
        {
            "backend": "information-floor",
            "storage_bits": denominator,
            "storage_bytes_ceiling": (denominator + 7) // 8,
            "storage_ratio_to_info": 1.0,
            "note": "Counting lower bound / ideal endpoint, not an executable store.",
        },
        packed_history_log(case_dir, base_bytes, base_bytes * 8, ops, denominator),
        snapshot_endpoint(base_bytes, updates, denominator),
        chunked_cow_baseline(case_dir, base_path, base, ops, base_bytes * 8, denominator),
    ]
    tulya = run_tulya(binary, case_dir, base_path, history_path, denominator)
    result = {
        "manifest": manifest,
        "baselines": baselines,
        "tulya": tulya,
    }
    (case_dir / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def profile_cases(profile: str) -> Iterable[tuple[int, int, str, int]]:
    if profile == "smoke":
        sizes, updates, topologies, seeds = [64 << 10], [128], ["random"], [1]
    elif profile == "quick":
        sizes, updates, topologies, seeds = [1 << 20], [1000], ["chain", "star", "balanced", "random"], [1]
    elif profile == "standard":
        sizes, updates, topologies, seeds = [1 << 20, 64 << 20], [1000, 10000], ["chain", "star", "balanced", "random"], [1]
    elif profile == "full":
        sizes = [1 << 20, 64 << 20, 1 << 30]
        updates = [1000, 10000, 100000]
        topologies = ["chain", "star", "balanced", "random"]
        seeds = [1, 2, 3]
    else:
        raise ValueError(profile)
    for size in sizes:
        for count in updates:
            for topology in topologies:
                for seed in seeds:
                    yield size, count, topology, seed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["smoke", "quick", "standard", "full"], default="quick")
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark-results/frontier-flip-v1"))
    parser.add_argument("--base-bytes", type=int)
    parser.add_argument("--updates", type=int)
    parser.add_argument("--topology", choices=["chain", "star", "balanced", "random"])
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    output_root = args.output_dir if args.output_dir.is_absolute() else repo_root / args.output_dir
    output_root.mkdir(parents=True, exist_ok=True)
    binary = build_tulya(repo_root)

    if any(value is not None for value in (args.base_bytes, args.updates, args.topology)):
        if args.base_bytes is None or args.updates is None or args.topology is None:
            parser.error("--base-bytes, --updates, and --topology must be supplied together")
        cases = [(args.base_bytes, args.updates, args.topology, args.seed)]
    else:
        cases = list(profile_cases(args.profile))

    results = []
    for base_bytes, updates, topology, seed in cases:
        print(
            f"[{BENCHMARK}] base={base_bytes} updates={updates} topology={topology} seed={seed}",
            file=sys.stderr,
            flush=True,
        )
        results.append(
            run_case(repo_root, binary, output_root, base_bytes, updates, topology, seed)
        )

    summary = {
        "benchmark": BENCHMARK,
        "profile": args.profile,
        "case_count": len(results),
        "results": results,
    }
    summary_path = output_root / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
