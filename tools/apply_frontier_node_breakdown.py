from pathlib import Path

path = Path("examples/frontier_flip_tulya.rs")
text = path.read_text()

def replace_once(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected exactly one match, got {count}: {old[:80]!r}")
    text = text.replace(old, new, 1)

replace_once(
    'use tulya_core::persistent_history::{CommitOutcome, Version};\n',
    'use tulya_core::persistent_history::{CommitOutcome, Version};\n\nconst PHYSICAL_NODE_HEADER_BYTES: u64 = 16;\nconst PHYSICAL_NODE_RECORD_BYTES: u64 = 72;\n',
)

replace_once(
    'fn expected_bit(base: &[u8], ops: &[FlipOp], mut version: u64, bit: u64) -> u8 {\n',
    '''fn physical_node_file_bytes(root: &Path) -> Result<u64, Box<dyn Error>> {\n    let mut total = 0u64;\n    for entry in fs::read_dir(root)? {\n        let entry = entry?;\n        let metadata = entry.metadata()?;\n        if !metadata.is_file() {\n            continue;\n        }\n        let name = entry.file_name();\n        let name = name.to_string_lossy();\n        if name.starts_with("content-") && name.ends_with(".nodes") {\n            total = total\n                .checked_add(metadata.len())\n                .ok_or("node file byte count overflow")?;\n        }\n    }\n    Ok(total)\n}\n\nfn physical_node_record_count(bytes: u64) -> Result<u64, Box<dyn Error>> {\n    if bytes < PHYSICAL_NODE_HEADER_BYTES {\n        return Err("physical node file is shorter than its header".into());\n    }\n    let body = bytes - PHYSICAL_NODE_HEADER_BYTES;\n    if body % PHYSICAL_NODE_RECORD_BYTES != 0 {\n        return Err("physical node file body is not record aligned".into());\n    }\n    Ok(body / PHYSICAL_NODE_RECORD_BYTES)\n}\n\nfn node_allocation_json(values: &[u64]) -> Value {\n    let mut sorted = values.to_vec();\n    sorted.sort_unstable();\n    let percentile = |percent: usize| -> u64 {\n        if sorted.is_empty() {\n            return 0;\n        }\n        sorted[(sorted.len() - 1) * percent / 100]\n    };\n    let total: u64 = values.iter().copied().sum();\n    json!({\n        "count": values.len(),\n        "total": total,\n        "mean": if values.is_empty() { 0.0 } else { total as f64 / values.len() as f64 },\n        "min": sorted.first().copied().unwrap_or(0),\n        "p50": percentile(50),\n        "p95": percentile(95),\n        "p99": percentile(99),\n        "max": sorted.last().copied().unwrap_or(0),\n    })\n}\n\nfn expected_bit(base: &[u8], ops: &[FlipOp], mut version: u64, bit: u64) -> u8 {\n''',
)

replace_once(
    '''    let mut versions = Vec::with_capacity(ops.len() + 1);\n    versions.push(root);\n    let mut update_latencies = Vec::with_capacity(ops.len());\n    let mut parent_read_latencies = Vec::with_capacity(ops.len());\n\n    for op in &ops {\n''',
    '''    let initial_node_file_bytes = physical_node_file_bytes(&args.db)?;\n    let initial_node_records = physical_node_record_count(initial_node_file_bytes)?;\n    let mut previous_node_records = initial_node_records;\n\n    let mut versions = Vec::with_capacity(ops.len() + 1);\n    versions.push(root);\n    let mut update_latencies = Vec::with_capacity(ops.len());\n    let mut parent_read_latencies = Vec::with_capacity(ops.len());\n    let mut fresh_node_records_per_update = Vec::with_capacity(ops.len());\n\n    for op in &ops {\n''',
)

replace_once(
    '''        versions.push(child);\n    }\n\n    let pre_seal_storage = directory_storage_breakdown(&args.db)?;\n''',
    '''        versions.push(child);\n\n        // Measure after the timed splice so filesystem metadata inspection does\n        // not contaminate update latency. Physical nodes are append-only.\n        let node_file_bytes = physical_node_file_bytes(&args.db)?;\n        let node_records = physical_node_record_count(node_file_bytes)?;\n        let fresh = node_records\n            .checked_sub(previous_node_records)\n            .ok_or("physical node record count regressed during append-only updates")?;\n        fresh_node_records_per_update.push(fresh);\n        previous_node_records = node_records;\n    }\n\n    let final_node_file_bytes = physical_node_file_bytes(&args.db)?;\n    let final_node_records = physical_node_record_count(final_node_file_bytes)?;\n    let fresh_node_records_total = final_node_records\n        .checked_sub(initial_node_records)\n        .ok_or("final physical node count is below the initial tree")?;\n    let observed_fresh_total: u64 = fresh_node_records_per_update.iter().copied().sum();\n    if observed_fresh_total != fresh_node_records_total {\n        return Err("per-update node allocation accounting does not sum to final growth".into());\n    }\n\n    let pre_seal_storage = directory_storage_breakdown(&args.db)?;\n''',
)

replace_once(
    '''        "storage": {\n            "pre_seal_file_bytes": pre_seal_file_bytes,\n            "post_seal_file_bytes": post_seal_file_bytes,\n            "pre_seal_breakdown": pre_seal_storage,\n            "post_seal_breakdown": post_seal_storage,\n        },\n''',
    '''        "storage": {\n            "pre_seal_file_bytes": pre_seal_file_bytes,\n            "post_seal_file_bytes": post_seal_file_bytes,\n            "pre_seal_breakdown": pre_seal_storage,\n            "post_seal_breakdown": post_seal_storage,\n        },\n        "node_allocation": {\n            "record_size_bytes": PHYSICAL_NODE_RECORD_BYTES,\n            "file_header_bytes": PHYSICAL_NODE_HEADER_BYTES,\n            "initial_node_file_bytes": initial_node_file_bytes,\n            "initial_node_records": initial_node_records,\n            "final_node_file_bytes": final_node_file_bytes,\n            "final_node_records": final_node_records,\n            "fresh_node_records_total": fresh_node_records_total,\n            "fresh_node_bytes_total": fresh_node_records_total * PHYSICAL_NODE_RECORD_BYTES,\n            "per_update_fresh_node_records": node_allocation_json(&fresh_node_records_per_update),\n        },\n''',
)

path.write_text(text)
print("frontier node allocation instrumentation applied exactly")
