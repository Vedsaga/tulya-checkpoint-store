from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected old block exactly once, found {count}")
    p.write_text(text.replace(old, new, 1))


EXAMPLE = "examples/frontier_flip_tulya.rs"
RUNNER = "benchmarks/frontier_flip/run.py"

replace_once(
    EXAMPLE,
    '''fn directory_file_bytes(root: &Path) -> Result<u64, Box<dyn Error>> {
    fn visit(path: &Path, total: &mut u64) -> Result<(), Box<dyn Error>> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                visit(&entry.path(), total)?;
            } else if metadata.is_file() {
                *total = total
                    .checked_add(metadata.len())
                    .ok_or("directory byte count overflow")?;
            }
        }
        Ok(())
    }

    let mut total = 0u64;
    visit(root, &mut total)?;
    Ok(total)
}
''',
    '''fn directory_storage_breakdown(root: &Path) -> Result<Value, Box<dyn Error>> {
    #[derive(Default)]
    struct Totals {
        total: u64,
        payload: u64,
        nodes: u64,
        wal: u64,
        snapshot: u64,
        manifest: u64,
        lock: u64,
        other: u64,
        payload_files: u64,
        node_files: u64,
    }

    fn checked_add(slot: &mut u64, value: u64) -> Result<(), Box<dyn Error>> {
        *slot = slot
            .checked_add(value)
            .ok_or("directory byte count overflow")?;
        Ok(())
    }

    fn category(name: &str) -> &'static str {
        if name.starts_with("content-") && name.ends_with(".payload") {
            "payload"
        } else if name.starts_with("content-") && name.ends_with(".nodes") {
            "nodes"
        } else if name.starts_with("history-snap-") && name.ends_with(".ths") {
            "snapshot"
        } else if name.starts_with("history-") && name.ends_with(".wal") {
            "wal"
        } else if name == "history-manifest.json" {
            "manifest"
        } else if name == "history.lock" {
            "lock"
        } else {
            "other"
        }
    }

    fn visit(
        root: &Path,
        path: &Path,
        totals: &mut Totals,
        files: &mut Vec<Value>,
    ) -> Result<(), Box<dyn Error>> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                visit(root, &entry.path(), totals, files)?;
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            let bytes = metadata.len();
            checked_add(&mut totals.total, bytes)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let class = category(&name);
            match class {
                "payload" => {
                    checked_add(&mut totals.payload, bytes)?;
                    totals.payload_files += 1;
                }
                "nodes" => {
                    checked_add(&mut totals.nodes, bytes)?;
                    totals.node_files += 1;
                }
                "wal" => checked_add(&mut totals.wal, bytes)?,
                "snapshot" => checked_add(&mut totals.snapshot, bytes)?,
                "manifest" => checked_add(&mut totals.manifest, bytes)?,
                "lock" => checked_add(&mut totals.lock, bytes)?,
                _ => checked_add(&mut totals.other, bytes)?,
            }
            let relative = entry
                .path()
                .strip_prefix(root)?
                .to_string_lossy()
                .into_owned();
            files.push(json!({
                "path": relative,
                "bytes": bytes,
                "category": class,
            }));
        }
        Ok(())
    }

    let mut totals = Totals::default();
    let mut files = Vec::new();
    visit(root, root, &mut totals, &mut files)?;
    files.sort_by(|left, right| {
        left["path"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["path"].as_str().unwrap_or_default())
    });
    Ok(json!({
        "total_bytes": totals.total,
        "payload_file_bytes": totals.payload,
        "node_file_bytes": totals.nodes,
        "wal_file_bytes": totals.wal,
        "snapshot_file_bytes": totals.snapshot,
        "manifest_file_bytes": totals.manifest,
        "lock_file_bytes": totals.lock,
        "other_file_bytes": totals.other,
        "payload_file_count": totals.payload_files,
        "node_file_count": totals.node_files,
        "files": files,
    }))
}
''',
)

replace_once(
    EXAMPLE,
    '''    let pre_seal_file_bytes = directory_file_bytes(&args.db)?;
    let seal_started = Instant::now();
    let seal = authority.seal()?;
    let seal_ns = seal_started.elapsed().as_nanos();
    let post_seal_file_bytes = directory_file_bytes(&args.db)?;
''',
    '''    let pre_seal_storage = directory_storage_breakdown(&args.db)?;
    let pre_seal_file_bytes = pre_seal_storage["total_bytes"]
        .as_u64()
        .ok_or("pre-seal storage total is not u64")?;
    let seal_started = Instant::now();
    let seal = authority.seal()?;
    let seal_ns = seal_started.elapsed().as_nanos();
    let post_seal_storage = directory_storage_breakdown(&args.db)?;
    let post_seal_file_bytes = post_seal_storage["total_bytes"]
        .as_u64()
        .ok_or("post-seal storage total is not u64")?;
''',
)

replace_once(
    EXAMPLE,
    '''        "storage": {
            "pre_seal_file_bytes": pre_seal_file_bytes,
            "post_seal_file_bytes": post_seal_file_bytes,
        },
''',
    '''        "storage": {
            "pre_seal_file_bytes": pre_seal_file_bytes,
            "post_seal_file_bytes": post_seal_file_bytes,
            "pre_seal_breakdown": pre_seal_storage,
            "post_seal_breakdown": post_seal_storage,
        },
''',
)

replace_once(
    RUNNER,
    '''    result["storage_bytes"] = storage
    result["storage_ratio_to_info"] = ratio(storage, info_bits)
    result["command"] = command
''',
    '''    result["storage_bytes"] = storage
    result["storage_ratio_to_info"] = ratio(storage, info_bits)
    base_bytes = int(result["base_bytes"])
    history_info_bits = info_bits - int(result["base_bits"])
    result["storage_bytes_above_base"] = storage - base_bytes
    result["history_information_bits"] = history_info_bits
    result["history_information_bytes_ceiling"] = (history_info_bits + 7) // 8
    result["storage_above_base_ratio_to_history_info"] = (
        ratio(storage - base_bytes, history_info_bits) if history_info_bits else 0.0
    )
    result["command"] = command
''',
)

replace_once(
    RUNNER,
    '''        "topology_family_info_bits": topology_bits,
        "headline_ratio_is_lean_theorem_backed": topology == "random",
''',
    '''        "topology_family_info_bits": topology_bits,
        "base_information_bits": n_bits,
        "history_information_bits": topology_bits - n_bits,
        "history_information_bytes_ceiling": (topology_bits - n_bits + 7) // 8,
        "headline_ratio_is_lean_theorem_backed": topology == "random",
''',
)

print("frontier storage breakdown instrumentation applied exactly")
