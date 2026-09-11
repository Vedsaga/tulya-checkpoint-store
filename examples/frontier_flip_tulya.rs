use std::error::Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use serde_json::{json, Value};
use tulya_core::persistent_history::authority::{open_history_authority, WritableHistoryAuthority};
use tulya_core::persistent_history::{CommitOutcome, Version};

#[derive(Parser, Debug)]
#[command(name = "frontier-flip-tulya")]
struct Args {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    base: PathBuf,
    #[arg(long)]
    history: PathBuf,
    #[arg(long, default_value_t = 256)]
    verify_samples: usize,
    #[arg(long, default_value_t = false)]
    fresh: bool,
}

#[derive(Debug, Clone, Copy)]
struct FlipOp {
    version: u64,
    parent: u64,
    bit: u64,
}

fn parse_history(path: &Path, bit_len: u64) -> Result<Vec<FlipOp>, Box<dyn Error>> {
    let mut ops = Vec::new();
    for (line_no, line) in BufReader::new(File::open(path)?).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: Value = serde_json::from_str(&line)
            .map_err(|error| format!("invalid history line {}: {error}", line_no + 1))?;
        let version = row
            .get("version")
            .and_then(Value::as_u64)
            .ok_or("history row missing version")?;
        let parent = row
            .get("parent")
            .and_then(Value::as_u64)
            .ok_or("history row missing parent")?;
        let bit = row
            .get("bit")
            .and_then(Value::as_u64)
            .ok_or("history row missing bit")?;
        let expected = u64::try_from(ops.len())? + 1;
        if version != expected {
            return Err(format!(
                "history versions must be dense and ordered: expected {expected}, got {version}"
            )
            .into());
        }
        if parent >= version {
            return Err(format!("version {version} has non-prior parent {parent}").into());
        }
        if bit >= bit_len {
            return Err(format!("version {version} bit {bit} exceeds bit length {bit_len}").into());
        }
        ops.push(FlipOp {
            version,
            parent,
            bit,
        });
    }
    Ok(ops)
}

fn committed_version(outcome: CommitOutcome) -> Result<Version, Box<dyn Error>> {
    outcome
        .version()
        .ok_or_else(|| "benchmark operation unexpectedly resolved to a retired receipt".into())
}

fn percentile(values: &[u128], percent: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let last = sorted.len() - 1;
    sorted[last * percent / 100]
}

fn latency_json(values: &[u128]) -> Value {
    json!({
        "count": values.len(),
        "p50_ns": percentile(values, 50),
        "p95_ns": percentile(values, 95),
        "p99_ns": percentile(values, 99),
        "max_ns": values.iter().copied().max().unwrap_or(0),
    })
}

fn directory_file_bytes(root: &Path) -> Result<u64, Box<dyn Error>> {
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

fn expected_bit(base: &[u8], ops: &[FlipOp], mut version: u64, bit: u64) -> u8 {
    let byte = base[(bit / 8) as usize];
    let mut value = (byte >> (bit % 8)) & 1;
    while version != 0 {
        let op = ops[(version - 1) as usize];
        if op.bit == bit {
            value ^= 1;
        }
        version = op.parent;
    }
    value
}

fn verification_queries(ops: &[FlipOp], bit_len: u64, samples: usize) -> Vec<(u64, u64)> {
    let version_count = ops.len() + 1;
    let count = samples.max(1).min(version_count);
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let version = if count == 1 {
            u64::try_from(version_count - 1).unwrap_or(u64::MAX)
        } else {
            u64::try_from(index * (version_count - 1) / (count - 1)).unwrap_or(u64::MAX)
        };
        let bit = if version == 0 {
            (0x9e37_79b9_7f4a_7c15u64 % bit_len.max(1)).min(bit_len.saturating_sub(1))
        } else {
            ops[(version - 1) as usize].bit
        };
        out.push((version, bit));
    }
    out
}

fn run(args: &Args) -> Result<Value, Box<dyn Error>> {
    if args.fresh && args.db.exists() {
        fs::remove_dir_all(&args.db)?;
    }
    fs::create_dir_all(&args.db)?;

    let base = fs::read(&args.base)?;
    if base.is_empty() {
        return Err("base state must be non-empty".into());
    }
    let bit_len = u64::try_from(base.len())?
        .checked_mul(8)
        .ok_or("base bit length overflow")?;
    let ops = parse_history(&args.history, bit_len)?;

    let open_started = Instant::now();
    let mut authority = WritableHistoryAuthority::open(&args.db)?;
    let open_ns = open_started.elapsed().as_nanos();
    let history_id = authority.create_history(None)?;

    let root_started = Instant::now();
    let root = committed_version(authority.splice(history_id, None, 0, 0, &base, None, None)?)?;
    let root_create_ns = root_started.elapsed().as_nanos();
    if root.id().id() != 0 {
        return Err("fresh Tulya history did not assign root version id 0".into());
    }

    let mut versions = Vec::with_capacity(ops.len() + 1);
    versions.push(root);
    let mut update_latencies = Vec::with_capacity(ops.len());
    let mut parent_read_latencies = Vec::with_capacity(ops.len());

    for op in &ops {
        let parent = versions
            .get(usize::try_from(op.parent)?)
            .copied()
            .ok_or("history parent is absent")?;
        let byte_offset = op.bit / 8;
        let mask = 1u8 << (op.bit % 8);

        let read_started = Instant::now();
        let mut current = Vec::with_capacity(1);
        authority
            .store()
            .read(parent, byte_offset, 1, &mut current)?;
        parent_read_latencies.push(read_started.elapsed().as_nanos());
        if current.len() != 1 {
            return Err("Tulya parent-byte read returned the wrong length".into());
        }
        let next = [current[0] ^ mask];

        let update_started = Instant::now();
        let child = committed_version(authority.splice(
            history_id,
            Some(parent.id()),
            byte_offset,
            1,
            &next,
            None,
            None,
        )?)?;
        update_latencies.push(update_started.elapsed().as_nanos());
        if child.id().id() != op.version {
            return Err(format!(
                "Tulya version identity mismatch: corpus {}, Tulya {}",
                op.version,
                child.id().id()
            )
            .into());
        }
        versions.push(child);
    }

    let pre_seal_file_bytes = directory_file_bytes(&args.db)?;
    let seal_started = Instant::now();
    let seal = authority.seal()?;
    let seal_ns = seal_started.elapsed().as_nanos();
    let post_seal_file_bytes = directory_file_bytes(&args.db)?;
    drop(authority);

    let reopen_started = Instant::now();
    let reopened = open_history_authority(&args.db)?;
    let reopen_ns = reopen_started.elapsed().as_nanos();
    if reopened.store.version_count() != ops.len() + 1 {
        return Err(format!(
            "reopened version count mismatch: expected {}, got {}",
            ops.len() + 1,
            reopened.store.version_count()
        )
        .into());
    }

    let queries = verification_queries(&ops, bit_len, args.verify_samples);
    let mut query_latencies = Vec::with_capacity(queries.len());
    let mut verification_failures = 0u64;
    for (version_id, bit) in &queries {
        let version = reopened
            .store
            .lookup_version(tulya_core::persistent_history::VersionId::new(*version_id))?;
        let byte_offset = *bit / 8;
        let started = Instant::now();
        let mut bytes = Vec::with_capacity(1);
        reopened.store.read(version, byte_offset, 1, &mut bytes)?;
        query_latencies.push(started.elapsed().as_nanos());
        let actual = (bytes[0] >> (*bit % 8)) & 1;
        let expected = expected_bit(&base, &ops, *version_id, *bit);
        if actual != expected {
            verification_failures += 1;
        }
    }

    Ok(json!({
        "benchmark": "TULYA_FRONTIER_FLIP_V1",
        "backend": "tulya-balanced-durable",
        "base_bytes": base.len(),
        "base_bits": bit_len,
        "updates": ops.len(),
        "versions": ops.len() + 1,
        "exact": verification_failures == 0,
        "verification": {
            "sample_count": queries.len(),
            "failures": verification_failures,
        },
        "storage": {
            "pre_seal_file_bytes": pre_seal_file_bytes,
            "post_seal_file_bytes": post_seal_file_bytes,
        },
        "latency": {
            "open_ns": open_ns,
            "root_create_ns": root_create_ns,
            "parent_byte_read": latency_json(&parent_read_latencies),
            "update_splice": latency_json(&update_latencies),
            "seal_ns": seal_ns,
            "reopen_ns": reopen_ns,
            "historical_bit_read": latency_json(&query_latencies),
        },
        "seal": {
            "generation": seal.generation,
            "snapshot_len": seal.snapshot_len,
            "represented_wal_end": seal.represented_wal_end,
            "recycled_hot": seal.recycled_hot,
        },
        "reopen": {
            "generation": reopened.generation,
            "snapshot_versions": reopened.stats.snapshot_versions,
            "suffix_bytes": reopened.stats.suffix_bytes,
        }
    }))
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    println!("{}", serde_json::to_string_pretty(&run(&args)?)?);
    Ok(())
}
