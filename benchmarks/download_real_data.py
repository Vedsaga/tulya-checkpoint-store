#!/usr/bin/env python3
"""Download and checksum the public real-world corpora used by the benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from huggingface_hub import HfApi, snapshot_download

SOURCES: dict[str, dict[str, Any]] = {
    "nebius-swe-agent": {
        "repo_id": "nebius/SWE-agent-trajectories",
        # Commit that introduced the current 12-shard 1.11 GB dataset. Pinning
        # the data revision prevents a future dataset update from silently
        # changing benchmark inputs.
        "revision": "a8a64e57e7bd7ccbd1add6c4f8637c5d3834570b",
        "allow_patterns": ["data/*.parquet", "README.md"],
        "role": "primary",
        "license": "CC-BY-4.0",
    },
    "nebius-openhands": {
        "repo_id": "nebius/SWE-rebench-openhands-trajectories",
        # The dataset is a single root-level Parquet file. Pin the verified
        # dataset-card revision so an independent-corpus run never follows a
        # moving main branch.
        "revision": "35455389ab51bf5e2306bfd436ef72d0f98bf882",
        "allow_patterns": ["trajectories.parquet", "README.md", "LICENSE"],
        "role": "independent-agent-framework-validation",
        "license": "CC-BY-4.0",
    },
    "trace-commons": {
        "repo_id": "trace-commons/agent-traces",
        # Trace Commons is actively growing. If --revision is omitted we
        # resolve main once, write the exact SHA to source_manifest.json, and
        # every benchmark result carries that SHA. Pass that SHA on subsequent
        # reproductions.
        "revision": None,
        "allow_patterns": ["data/*.parquet", "README.md"],
        "role": "secondary-real-usage-validation",
        "license": "CC-BY-4.0",
    },
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def corpus_files(local_dir: Path) -> list[Path]:
    """Return only files that are benchmark input, never local cache/metadata."""
    result: list[Path] = []
    readme = local_dir / "README.md"
    if readme.is_file():
        result.append(readme)
    data_dir = local_dir / "data"
    if data_dir.is_dir():
        result.extend(sorted(path for path in data_dir.glob("*.parquet") if path.is_file()))
    result.extend(sorted(path for path in local_dir.glob("*.parquet") if path.is_file()))
    return result


def download(
    source: str,
    root: Path,
    revision_override: str | None,
    *,
    manifest_only: bool = False,
) -> dict[str, Any]:
    spec = SOURCES[source]
    repo_id = str(spec["repo_id"])
    revision = revision_override or spec["revision"]
    if revision is None:
        revision = HfApi().dataset_info(repo_id).sha
    if not revision:
        raise RuntimeError(f"could not resolve revision for {repo_id}")

    local_dir = root / source
    local_dir.mkdir(parents=True, exist_ok=True)
    if not manifest_only:
        snapshot_download(
            repo_id=repo_id,
            repo_type="dataset",
            revision=revision,
            allow_patterns=list(spec["allow_patterns"]),
            local_dir=local_dir,
        )

    input_files = corpus_files(local_dir)
    parquet_files = [path for path in input_files if path.suffix == ".parquet"]
    if not parquet_files:
        raise RuntimeError(f"download produced no Parquet shards for {repo_id}")

    files = []
    total_bytes = 0
    for path in input_files:
        size = path.stat().st_size
        total_bytes += size
        files.append(
            {
                "path": str(path.relative_to(local_dir)),
                "bytes": size,
                "sha256": sha256_file(path),
            }
        )

    manifest = {
        "source": source,
        "role": spec["role"],
        "license": spec["license"],
        "repo_id": repo_id,
        "resolved_revision": revision,
        "download_bytes": total_bytes,
        "parquet_shard_count": len(parquet_files),
        "files": files,
    }
    (local_dir / "source_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=(*SOURCES.keys(), "benchmark-pair", "all"),
        default="nebius-swe-agent",
    )
    parser.add_argument("--out", type=Path, default=Path("data"))
    parser.add_argument(
        "--revision",
        help="override revision (valid only when downloading one source)",
    )
    parser.add_argument(
        "--manifest-only",
        action="store_true",
        help="checksum already-local inputs without contacting the dataset host",
    )
    args = parser.parse_args()

    if args.source in {"benchmark-pair", "all"} and args.revision:
        parser.error("--revision can only be used when downloading one source")

    if args.source == "benchmark-pair":
        selected = ["nebius-swe-agent", "nebius-openhands"]
    elif args.source == "all":
        selected = list(SOURCES)
    else:
        selected = [args.source]
    results = [
        download(name, args.out, args.revision, manifest_only=args.manifest_only)
        for name in selected
    ]
    print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
