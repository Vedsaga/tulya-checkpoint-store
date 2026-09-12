#!/usr/bin/env python3
"""Large branching-state benchmark for Tulya.

Models a large immutable base with localized equal-length edits and persistent
branching.  This is an economic/storage benchmark, not an information-theory
claim: the main denominator is raw changed payload (base + edit bytes).
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

BENCHMARK = "TULYA_LARGE_BRANCHING_STATE_V1"
MASK64 = (1 << 64) - 1
CHUNK = 4096
LEAF = MASK64


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
class Op:
    version: int
    parent: int
    offset: int
    length: int


class BitWriter:
    def __init__(self, path: Path) -> None:
        self.f = path.open("wb")
        self.cur = 0
        self.used = 0
        self.bits = 0

    def write(self, value: int, width: int) -> None:
        for shift in range(width - 1, -1, -1):
            self.cur = (self.cur << 1) | ((value >> shift) & 1)
            self.used += 1
            self.bits += 1
            if self.used == 8:
                self.f.write(bytes([self.cur]))
                self.cur = self.used = 0

    def close(self) -> None:
        if self.used:
            self.cur <<= 8 - self.used
            self.f.write(bytes([self.cur]))
        self.f.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def clog2(n: int) -> int:
    return 0 if n <= 1 else (n - 1).bit_length()


def ensure_base(path: Path, size: int, seed: int) -> str:
    if size <= 0 or size % CHUNK:
        raise ValueError("base bytes must be a positive multiple of 4096")
    digest = hashlib.sha256()
    with path.open("wb") as f:
        remaining = size
        block = 0
        while remaining:
            take = min(1 << 20, remaining)
            data = hashlib.shake_256(f"{BENCHMARK}:base:{seed}:{block}".encode()).digest(take)
            f.write(data)
            digest.update(data)
            remaining -= take
            block += 1
    return digest.hexdigest()


def ensure_edits(path: Path, edit_bytes: int, updates: int, seed: int) -> str:
    digest = hashlib.sha256()
    with path.open("wb") as f:
        for version in range(1, updates + 1):
            data = hashlib.shake_256(f"{BENCHMARK}:edit:{seed}:{version}".encode()).digest(edit_bytes)
            f.write(data)
            digest.update(data)
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
    raise ValueError(topology)


def generate_history(path: Path, base_bytes: int, edit_bytes: int, updates: int, topology: str, seed: int) -> list[Op]:
    if edit_bytes <= 0 or edit_bytes % CHUNK or edit_bytes > base_bytes:
        raise ValueError("edit bytes must be a positive 4096-byte multiple no larger than base")
    slots = (base_bytes - edit_bytes) // CHUNK + 1
    prng = SplitMix64(seed ^ 0xA0761D6478BD642F)
    where = SplitMix64(seed ^ 0xE7037ED1A0B428DB)
    out: list[Op] = []
    with path.open("w", encoding="utf-8") as f:
        for version in range(1, updates + 1):
            op = Op(version, parent_for(topology, version, prng), (where.next() % slots) * CHUNK, edit_bytes)
            out.append(op)
            f.write(json.dumps(op.__dict__, sort_keys=True, separators=(",", ":")) + "\n")
    return out


def sampled_versions(updates: int, samples: int = 128) -> list[int]:
    count = min(max(1, samples), updates)
    if count == 1:
        return [updates]
    return [1 + i * (updates - 1) // (count - 1) for i in range(count)]


def ancestry_steps(ops: list[Op], version: int) -> int:
    steps = 0
    while version:
        steps += 1
        version = ops[version - 1].parent
    return steps


def raw_delta_baseline(case: Path, base_bytes: int, edits_bytes: int, ops: list[Op]) -> dict:
    metadata = case / "delta-metadata.bin"
    offset_slots = max(1, base_bytes // CHUNK)
    with BitWriter(metadata) as w:
        for b in b"TLS1":
            w.write(b, 8)
        w.write(base_bytes, 64)
        w.write(edits_bytes, 64)
        w.write(len(ops), 64)
        for op in ops:
            w.write(op.parent, clog2(op.version))
            w.write(op.offset // CHUNK, clog2(offset_slots))
        metadata_bits = w.bits
    storage = base_bytes + edits_bytes + metadata.stat().st_size
    steps = [ancestry_steps(ops, v) for v in sampled_versions(len(ops))]
    return {
        "backend": "raw-delta-log",
        "storage_bytes": storage,
        "storage_ratio_to_changed_payload_floor": storage / (base_bytes + edits_bytes),
        "metadata_bits": metadata_bits,
        "historical_access_steps": {"samples": len(steps), "mean": sum(steps) / len(steps), "max": max(steps)},
        "note": "Raw edit payload plus compact parent/offset log; no direct historical index or durability protocol.",
    }


def snapshots(base_bytes: int, updates: int) -> dict:
    return {
        "backend": "raw-full-snapshot-analytical",
        "storage_bytes": base_bytes * (updates + 1),
        "materialized": False,
        "note": "Analytical only; never written to disk.",
    }


class CowTree:
    def __init__(self, root: Path, base_path: Path) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.chunk_path = root / "chunks.bin"
        self.node_path = root / "nodes.bin"
        self.root_path = root / "roots.bin"
        self.chunk_file = self.chunk_path.open("w+b")
        self.left = array("Q")
        self.right = array("Q")
        self.roots = array("Q")
        self.by_hash: dict[bytes, int] = {}
        count = base_path.stat().st_size // CHUNK
        if count <= 0 or count & (count - 1):
            raise ValueError("CoW baseline requires a power-of-two number of 4 KiB chunks")
        self.depth = count.bit_length() - 1
        leaves: list[int] = []
        with base_path.open("rb") as f:
            for _ in range(count):
                data = f.read(CHUNK)
                leaves.append(self.node(self.intern(data), LEAF))
        level = leaves
        while len(level) > 1:
            level = [self.node(level[i], level[i + 1]) for i in range(0, len(level), 2)]
        self.roots.append(level[0])

    def node(self, left: int, right: int) -> int:
        i = len(self.left)
        self.left.append(left)
        self.right.append(right)
        return i

    def intern(self, data: bytes) -> int:
        digest = hashlib.sha256(data).digest()
        found = self.by_hash.get(digest)
        if found is not None:
            self.chunk_file.seek(found * CHUNK)
            if self.chunk_file.read(CHUNK) == data:
                return found
        i = len(self.by_hash)
        self.chunk_file.seek(0, os.SEEK_END)
        self.chunk_file.write(data)
        self.by_hash[digest] = i
        return i

    def replace_contiguous(self, node: int, depth: int, start: int, first: int, chunks: list[int]) -> int:
        end = first + len(chunks)
        span = 1 << depth
        if end <= start or first >= start + span:
            return node
        if depth == 0:
            return self.node(chunks[start - first], LEAF)
        half = span >> 1
        left = self.left[node]
        right = self.right[node]
        nl = self.replace_contiguous(left, depth - 1, start, first, chunks)
        nr = self.replace_contiguous(right, depth - 1, start + half, first, chunks)
        return self.node(nl, nr)

    def replace(self, parent_version: int, offset: int, data: bytes) -> None:
        first = offset // CHUNK
        chunks = [self.intern(data[i:i + CHUNK]) for i in range(0, len(data), CHUNK)]
        self.roots.append(self.replace_contiguous(self.roots[parent_version], self.depth, 0, first, chunks))

    def chunk_for(self, root: int, index: int) -> int:
        node = root
        for level in range(self.depth - 1, -1, -1):
            node = self.right[node] if ((index >> level) & 1) else self.left[node]
        if self.right[node] != LEAF:
            raise RuntimeError("bad CoW leaf")
        return self.left[node]

    def read_range(self, version: int, offset: int, length: int) -> bytes:
        out = bytearray()
        end = offset + length
        pos = offset
        while pos < end:
            chunk_index = pos // CHUNK
            within = pos % CHUNK
            take = min(CHUNK - within, end - pos)
            chunk_id = self.chunk_for(self.roots[version], chunk_index)
            self.chunk_file.seek(chunk_id * CHUNK + within)
            out.extend(self.chunk_file.read(take))
            pos += take
        return bytes(out)

    def finish(self) -> int:
        self.chunk_file.flush()
        os.fsync(self.chunk_file.fileno())
        self.chunk_file.close()
        with self.node_path.open("wb") as f:
            for left, right in zip(self.left, self.right):
                f.write(struct.pack("<QQ", left, right))
        with self.root_path.open("wb") as f:
            self.roots.tofile(f)
        return self.chunk_path.stat().st_size + self.node_path.stat().st_size + self.root_path.stat().st_size


def pct(values: list[int], p: int) -> int:
    s = sorted(values)
    return 0 if not s else s[(len(s) - 1) * p // 100]


def lat(values: list[int]) -> dict:
    return {"count": len(values), "p50_ns": pct(values, 50), "p95_ns": pct(values, 95), "p99_ns": pct(values, 99), "max_ns": max(values, default=0)}


def cow_baseline(case: Path, base_path: Path, edits_path: Path, ops: list[Op], edit_bytes: int, floor: int) -> dict:
    started = time.perf_counter_ns()
    tree = CowTree(case / "chunked-cow-tree", base_path)
    with edits_path.open("rb") as edits:
        for op in ops:
            edits.seek((op.version - 1) * edit_bytes)
            tree.replace(op.parent, op.offset, edits.read(edit_bytes))
    build_ns = time.perf_counter_ns() - started
    failures = 0
    r4: list[int] = []
    redit: list[int] = []
    with edits_path.open("rb") as edits:
        for version in sampled_versions(len(ops)):
            op = ops[version - 1]
            edits.seek((version - 1) * edit_bytes)
            expected = edits.read(edit_bytes)
            t = time.perf_counter_ns(); actual4 = tree.read_range(version, op.offset, CHUNK); r4.append(time.perf_counter_ns() - t)
            t = time.perf_counter_ns(); actual = tree.read_range(version, op.offset, edit_bytes); redit.append(time.perf_counter_ns() - t)
            failures += int(actual4 != expected[:CHUNK]) + int(actual != expected)
    nodes = len(tree.left)
    chunks = len(tree.by_hash)
    storage = tree.finish()
    return {
        "backend": "chunked-cow-tree-4k",
        "exact": failures == 0,
        "verification_failures": failures,
        "storage_bytes": storage,
        "storage_ratio_to_changed_payload_floor": storage / floor,
        "tree_depth": tree.depth,
        "tree_nodes": nodes,
        "unique_chunks": chunks,
        "build_ns": build_ns,
        "historical_read_4k": lat(r4),
        "historical_read_edit": lat(redit),
        "note": "4 KiB persistent CoW tree with batch path-copy for one contiguous edit; no per-update fsync/WAL.",
    }


def build_tulya(repo: Path) -> Path:
    subprocess.run(["cargo", "build", "--release", "--example", "large_branching_state_tulya"], cwd=repo, check=True)
    return repo / "target" / "release" / "examples" / "large_branching_state_tulya"


def run_tulya(binary: Path, case: Path, base: Path, history: Path, edits: Path, floor: int) -> dict:
    db = case / "tulya"
    command = [str(binary), "--db", str(db), "--base", str(base), "--history", str(history), "--edits", str(edits), "--fresh"]
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    result = json.loads(completed.stdout)
    storage = int(result["storage"]["post_seal_file_bytes"])
    result["storage_bytes"] = storage
    result["storage_ratio_to_changed_payload_floor"] = storage / floor
    result["command"] = command
    return result


def cleanup_heavy(case: Path) -> None:
    for name in ("base.bin", "edits.bin", "chunked-cow-tree", "tulya"):
        p = case / name
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base-bytes", type=int, default=1 << 30)
    p.add_argument("--edit-bytes", type=int, default=40 << 10)
    p.add_argument("--updates", type=int, default=10_000)
    p.add_argument("--topology", choices=["chain", "star", "balanced", "random"], default="random")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--output-dir", type=Path, default=Path("benchmark-results/large-branching-state-v1"))
    p.add_argument("--cleanup-heavy", action="store_true")
    args = p.parse_args()

    if args.base_bytes % CHUNK or args.edit_bytes % CHUNK:
        p.error("base and edit sizes must be multiples of 4096")
    if args.base_bytes // CHUNK & (args.base_bytes // CHUNK - 1):
        p.error("base must contain a power-of-two number of 4 KiB chunks for the CoW comparator")

    repo = Path(__file__).resolve().parents[2]
    out = args.output_dir if args.output_dir.is_absolute() else repo / args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    corpus_bytes = args.updates * args.edit_bytes
    conservative_peak = 6 * args.base_bytes + 8 * corpus_bytes
    free = shutil.disk_usage(out).free
    if free < conservative_peak:
        p.error(f"disk preflight failed: free={free} bytes, conservative requirement={conservative_peak} bytes")

    name = f"b{args.base_bytes}-e{args.edit_bytes}-m{args.updates}-{args.topology}-s{args.seed}"
    case = out / name
    if case.exists():
        shutil.rmtree(case)
    case.mkdir(parents=True)
    base = case / "base.bin"
    edits = case / "edits.bin"
    history = case / "history.jsonl"
    base_sha = ensure_base(base, args.base_bytes, args.seed)
    edits_sha = ensure_edits(edits, args.edit_bytes, args.updates, args.seed)
    ops = generate_history(history, args.base_bytes, args.edit_bytes, args.updates, args.topology, args.seed)
    floor = args.base_bytes + corpus_bytes
    manifest = {
        "benchmark": BENCHMARK,
        "base_bytes": args.base_bytes,
        "edit_bytes": args.edit_bytes,
        "updates": args.updates,
        "versions": args.updates + 1,
        "topology": args.topology,
        "seed": args.seed,
        "base_sha256": base_sha,
        "edits_sha256": edits_sha,
        "changed_payload_floor_bytes": floor,
        "changed_payload_floor_note": "Raw base plus all replacement payload bytes; not an information-theoretic lower bound.",
        "disk_free_bytes_at_start": free,
        "disk_conservative_peak_requirement_bytes": conservative_peak,
    }
    (case / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    binary = build_tulya(repo)
    baselines = [
        {"backend": "changed-payload-floor", "storage_bytes": floor, "storage_ratio_to_changed_payload_floor": 1.0},
        raw_delta_baseline(case, args.base_bytes, corpus_bytes, ops),
        snapshots(args.base_bytes, args.updates),
        cow_baseline(case, base, edits, ops, args.edit_bytes, floor),
    ]
    tulya = run_tulya(binary, case, base, history, edits, floor)
    result = {"manifest": manifest, "baselines": baselines, "tulya": tulya}
    (case / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    summary = {"benchmark": BENCHMARK, "case_count": 1, "results": [result]}
    summary_path = out / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    if args.cleanup_heavy:
        cleanup_heavy(case)
    print(summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
