#!/usr/bin/env python3
"""Fail-fast scout for real multi-attempt SWE-bench trajectories.

Uses only Python stdlib and anonymous S3 HTTP. It lists a public SWE-bench
submission prefix, downloads a small bounded sample of trajectory/log files,
and emits a summary describing file sizes and JSON structure. This is a scout,
not yet a Tulya-vs-baseline benchmark.
"""
from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path
from typing import Any

BUCKET = "swe-bench-submissions"
BASE = f"https://{BUCKET}.s3.amazonaws.com/"
DEFAULT_SUBMISSION = "verified/20250616_Skywork-SWE-32B+TTS_Bo8"


def fetch(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "tulya-trace-scout/1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def list_prefix(prefix: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        q = {"list-type": "2", "prefix": prefix}
        if token:
            q["continuation-token"] = token
        url = BASE + "?" + urllib.parse.urlencode(q)
        root = ET.fromstring(fetch(url))
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        for item in root.findall("s3:Contents", ns):
            key = item.findtext("s3:Key", default="", namespaces=ns)
            size = int(item.findtext("s3:Size", default="0", namespaces=ns))
            if key and not key.endswith("/"):
                out.append({"key": key, "size": size})
        truncated = root.findtext("s3:IsTruncated", default="false", namespaces=ns) == "true"
        if not truncated:
            break
        token = root.findtext("s3:NextContinuationToken", default="", namespaces=ns)
        if not token:
            raise RuntimeError("S3 listing was truncated without a continuation token")
    return out


def s3_object_url(key: str) -> str:
    return BASE + urllib.parse.quote(key, safe="/")


def summarize_json_bytes(data: bytes) -> dict[str, Any]:
    text = data.decode("utf-8", errors="replace")
    result: dict[str, Any] = {"bytes": len(data), "text_lines": text.count("\n") + (1 if text else 0)}
    parsed: Any = None
    mode = None
    try:
        parsed = json.loads(text)
        mode = "json"
    except json.JSONDecodeError:
        rows = []
        ok = True
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                ok = False
                break
        if ok and rows:
            parsed = rows
            mode = "jsonl"
    result["parse_mode"] = mode or "text"
    if isinstance(parsed, dict):
        result["top_type"] = "object"
        result["top_keys"] = sorted(parsed.keys())[:80]
        for k, v in parsed.items():
            if isinstance(v, list):
                result.setdefault("list_fields", {})[k] = len(v)
    elif isinstance(parsed, list):
        result["top_type"] = "array"
        result["top_length"] = len(parsed)
        keys = Counter()
        for row in parsed[:200]:
            if isinstance(row, dict):
                keys.update(row.keys())
        result["row_keys"] = [k for k, _ in keys.most_common(80)]
    else:
        result["top_type"] = type(parsed).__name__ if parsed is not None else "text"
    return result


def choose_samples(items: list[dict[str, Any]], max_files: int, max_file_bytes: int) -> list[dict[str, Any]]:
    eligible = [x for x in items if 0 < x["size"] <= max_file_bytes]
    # Prefer a spread rather than only the lexicographically first few files.
    eligible.sort(key=lambda x: x["key"])
    if len(eligible) <= max_files:
        return eligible
    if max_files <= 1:
        return [eligible[len(eligible) // 2]]
    idxs = [i * (len(eligible) - 1) // (max_files - 1) for i in range(max_files)]
    return [eligible[i] for i in idxs]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--submission", default=DEFAULT_SUBMISSION)
    p.add_argument("--output-dir", type=Path, default=Path("benchmark-results/real-agent-trace-scout"))
    p.add_argument("--max-sample-files", type=int, default=12)
    p.add_argument("--max-file-bytes", type=int, default=8 << 20)
    p.add_argument("--max-total-download-bytes", type=int, default=48 << 20)
    args = p.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    sections: dict[str, Any] = {}
    total_downloaded = 0

    for kind in ("trajs", "logs"):
        prefix = f"{args.submission}/{kind}/"
        items = list_prefix(prefix)
        sizes = [x["size"] for x in items]
        section: dict[str, Any] = {
            "prefix": prefix,
            "file_count": len(items),
            "total_bytes": sum(sizes),
            "min_bytes": min(sizes) if sizes else 0,
            "max_bytes": max(sizes) if sizes else 0,
            "sample_files": [],
        }
        samples = choose_samples(items, args.max_sample_files, args.max_file_bytes)
        sample_dir = args.output_dir / kind
        sample_dir.mkdir(exist_ok=True)
        for item in samples:
            if total_downloaded + item["size"] > args.max_total_download_bytes:
                break
            data = fetch(s3_object_url(item["key"]))
            total_downloaded += len(data)
            local = sample_dir / Path(item["key"]).name
            local.write_bytes(data)
            entry = {"key": item["key"], "declared_size": item["size"], "local_file": str(local)}
            entry.update(summarize_json_bytes(data))
            section["sample_files"].append(entry)
        sections[kind] = section

    summary = {
        "benchmark": "TULYA_REAL_AGENT_TRACE_SCOUT_V1",
        "submission": args.submission,
        "source": "public anonymous S3 SWE-bench submission artifacts",
        "downloaded_bytes": total_downloaded,
        "sections": sections,
        "next_gate": "Inspect sampled trajectory schema, then compute per-instance multi-attempt common-prefix duplication before implementing a storage adapter.",
    }
    out = args.output_dir / "summary.json"
    out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
