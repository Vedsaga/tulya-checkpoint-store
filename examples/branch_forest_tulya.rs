use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tulya_checkpoint_store::{admin::StoreStorage, CheckpointStore, CheckpointStoreConfig};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Parser, Debug)]
#[command(name = "branch-forest-tulya")]
struct Args {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    corpus: PathBuf,
    /// Continue an interrupted run by skipping the exact committed prefix.
    #[arg(long)]
    resume: bool,
}

#[derive(Debug)]
struct PlanNode {
    checkpoint_no: u32,
    thread_id: String,
    checkpoint_id: String,
    parent_checkpoint_id: Option<String>,
    parent_index: Option<usize>,
    message_bytes: Vec<u8>,
    external_state_len: u64,
    external_state_sha256: String,
    internal_state_len: u64,
    internal_state_hash: u64,
}

#[derive(Clone, Debug)]
struct AttemptRef {
    instance_id: String,
    attempt_id: String,
    solved: bool,
    source_row_fingerprint: String,
    tip_checkpoint_id: String,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn state_bytes(
    plans: &[PlanNode],
    parent_index: Option<usize>,
    message: &[u8],
    expected_external_len: usize,
) -> (Vec<u8>, Vec<u8>) {
    let mut ancestry = Vec::new();
    let mut current = parent_index;
    while let Some(index) = current {
        ancestry.push(index);
        current = plans[index].parent_index;
    }
    ancestry.reverse();
    let mut body = Vec::with_capacity(expected_external_len);
    for index in ancestry {
        if !body.is_empty() {
            body.push(b',');
        }
        body.extend_from_slice(&plans[index].message_bytes);
    }
    if !body.is_empty() {
        body.push(b',');
    }
    body.extend_from_slice(message);

    let mut external = Vec::with_capacity(expected_external_len);
    external.extend_from_slice(b"{\"messages\":[");
    external.extend_from_slice(&body);
    external.extend_from_slice(b"]}");
    let mut internal = Vec::with_capacity(external.len() + 16);
    internal.extend_from_slice(b"{\"identity\":null,\"messages\":[");
    internal.extend_from_slice(&body);
    internal.extend_from_slice(b"]}");
    (external, internal)
}

fn parse_plan(path: &Path) -> Result<(Vec<PlanNode>, Vec<AttemptRef>), Box<dyn Error>> {
    let file = File::open(path)?;
    let mut plans: Vec<PlanNode> = Vec::new();
    let mut attempts = Vec::new();
    let mut global_ids: HashMap<(String, String), usize> = HashMap::new();

    for (line_index, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: Value = serde_json::from_str(&line)
            .map_err(|error| format!("invalid corpus line {}: {error}", line_index + 1))?;
        let instance_id = row
            .get("instance_id")
            .and_then(Value::as_str)
            .ok_or("corpus row missing instance_id")?
            .to_owned();
        let nodes = row
            .get("nodes")
            .and_then(Value::as_array)
            .ok_or("corpus row missing nodes")?;
        for node in nodes {
            let checkpoint_id = node
                .get("checkpoint_id")
                .and_then(Value::as_str)
                .ok_or("node missing checkpoint_id")?
                .to_owned();
            let parent_checkpoint_id = node
                .get("parent_checkpoint_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let parent_index = parent_checkpoint_id
                .as_ref()
                .map(|parent| {
                    global_ids
                        .get(&(instance_id.clone(), parent.clone()))
                        .copied()
                        .ok_or("node parent is absent or not topologically prior")
                })
                .transpose()?;
            let operation = node
                .get("operations")
                .and_then(Value::as_array)
                .and_then(|items| (items.len() == 1).then(|| &items[0]))
                .ok_or("node must have exactly one operation")?;
            if operation.get("op").and_then(Value::as_str) != Some("append_message") {
                return Err("node operation is not append_message".into());
            }
            let message = operation
                .get("value")
                .and_then(Value::as_object)
                .ok_or("append_message operation missing value")?;
            let message_bytes = serde_json::to_vec(message)?;
            let external_state_len = node
                .get("logical_state_len")
                .and_then(Value::as_u64)
                .ok_or("node missing logical_state_len")?;
            let external_state_sha256 = node
                .get("logical_state_sha256")
                .and_then(Value::as_str)
                .ok_or("node missing logical_state_sha256")?
                .to_owned();
            let (external, internal) = state_bytes(
                &plans,
                parent_index,
                &message_bytes,
                usize::try_from(external_state_len)?,
            );
            if u64::try_from(external.len())? != external_state_len
                || sha256_hex(&external) != external_state_sha256
            {
                return Err("corpus node state does not match its frozen length/hash".into());
            }
            let checkpoint_no = u32::try_from(plans.len())?;
            let index = plans.len();
            if global_ids
                .insert((instance_id.clone(), checkpoint_id.clone()), index)
                .is_some()
            {
                return Err("duplicate checkpoint identity".into());
            }
            plans.push(PlanNode {
                checkpoint_no,
                thread_id: instance_id.clone(),
                checkpoint_id,
                parent_checkpoint_id,
                parent_index,
                message_bytes,
                external_state_len,
                external_state_sha256,
                internal_state_len: u64::try_from(internal.len())?,
                internal_state_hash: xxh3_64(&internal),
            });
        }

        for attempt in row
            .get("attempts")
            .and_then(Value::as_array)
            .ok_or("corpus row missing attempts")?
        {
            let path = attempt
                .get("checkpoint_path")
                .and_then(Value::as_array)
                .ok_or("attempt missing checkpoint_path")?;
            let tip_checkpoint_id = path
                .last()
                .and_then(Value::as_str)
                .ok_or("attempt path is empty")?
                .to_owned();
            attempts.push(AttemptRef {
                instance_id: instance_id.clone(),
                attempt_id: attempt
                    .get("attempt_id")
                    .and_then(Value::as_str)
                    .ok_or("attempt missing attempt_id")?
                    .to_owned(),
                solved: attempt
                    .get("solved")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                source_row_fingerprint: attempt
                    .get("source_row_fingerprint")
                    .and_then(Value::as_str)
                    .ok_or("attempt missing source_row_fingerprint")?
                    .to_owned(),
                tip_checkpoint_id,
            });
        }
    }
    if plans.is_empty() || attempts.is_empty() {
        return Err("branch forest is empty".into());
    }
    Ok((plans, attempts))
}

fn storage_json(storage: StoreStorage) -> Value {
    json!({
        "file_length_bytes": storage.file_length_bytes,
        "allocated_bytes": storage.allocated_bytes,
        "file_count": storage.file_count,
    })
}

fn process_memory_json() -> Value {
    let Ok(status) = fs::read_to_string("/proc/self/status") else {
        return json!({"available": false});
    };
    let mut rss_bytes = None;
    let mut hwm_bytes = None;
    for line in status.lines() {
        let parse = |prefix: &str| -> Option<u64> {
            line.strip_prefix(prefix)?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
                .and_then(|kib| kib.checked_mul(1024))
        };
        rss_bytes = rss_bytes.or_else(|| parse("VmRSS:"));
        hwm_bytes = hwm_bytes.or_else(|| parse("VmHWM:"));
    }
    json!({
        "available": rss_bytes.is_some() && hwm_bytes.is_some(),
        "rss_bytes": rss_bytes,
        "peak_rss_bytes": hwm_bytes,
    })
}

fn latency_json(values: &[u128]) -> Value {
    if values.is_empty() {
        return json!({"count": 0, "p50_ns": 0, "p95_ns": 0, "p99_ns": 0, "max_ns": 0});
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let last = sorted.len() - 1;
    json!({
        "count": sorted.len(),
        "p50_ns": sorted[last * 50 / 100],
        "p95_ns": sorted[last * 95 / 100],
        "p99_ns": sorted[last * 99 / 100],
        "max_ns": sorted[last],
    })
}

fn persist_attempt_refs(db: &Path, attempts: &[AttemptRef]) -> Result<u64, Box<dyn Error>> {
    let path = db.join("attempt-refs.jsonl");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    for attempt in attempts {
        let row = json!({
            "attempt_id": attempt.attempt_id,
            "instance_id": attempt.instance_id,
            "solved": attempt.solved,
            "source_row_fingerprint": attempt.source_row_fingerprint,
            "tip_checkpoint_id": attempt.tip_checkpoint_id,
        });
        file.write_all(&serde_json::to_vec(&row)?)?;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    Ok(fs::metadata(path)?.len())
}

fn verify(
    store: &CheckpointStore,
    db: &Path,
    plans: &[PlanNode],
    attempts: &[AttemptRef],
) -> Result<Value, Box<dyn Error>> {
    let internal = store.verify_all()?;
    let checkpoints = store.checkpoints();
    let mut metadata_failures = 0u64;
    let mut state_failures = 0u64;
    let mut read_latencies = Vec::with_capacity(plans.len());
    if checkpoints.len() != plans.len() {
        metadata_failures += 1;
    }
    for plan in plans {
        let Some(info) = checkpoints.get(usize::try_from(plan.checkpoint_no)?) else {
            metadata_failures += 1;
            continue;
        };
        if info.checkpoint_no != plan.checkpoint_no
            || info.thread_id != plan.thread_id
            || info.checkpoint_id != plan.checkpoint_id
            || info.parent_checkpoint_id != plan.parent_checkpoint_id
            || info.logical_state_len != plan.internal_state_len
            || info.state_hash != plan.internal_state_hash
        {
            metadata_failures += 1;
        }
        let started = Instant::now();
        let actual = store.read_checkpoint(&plan.thread_id, &plan.checkpoint_id)?;
        read_latencies.push(started.elapsed().as_nanos());
        let value: Value = serde_json::from_slice(&actual)?;
        if value.get("identity") != Some(&Value::Null) {
            state_failures += 1;
            continue;
        }
        let projected = serde_json::to_vec(&json!({
            "messages": value.get("messages").cloned().unwrap_or(Value::Null),
        }))?;
        if u64::try_from(projected.len())? != plan.external_state_len
            || sha256_hex(&projected) != plan.external_state_sha256
        {
            state_failures += 1;
        }
    }
    let attempt_path = db.join("attempt-refs.jsonl");
    let persisted_attempts = BufReader::new(File::open(attempt_path)?)
        .lines()
        .map_while(Result::ok)
        .filter(|line| !line.trim().is_empty())
        .count();
    Ok(json!({
        "exact": internal.failures == 0
            && metadata_failures == 0
            && state_failures == 0
            && persisted_attempts == attempts.len(),
        "internal_failures": internal.failures,
        "metadata_failures": metadata_failures,
        "state_failures": state_failures,
        "checkpoint_count": checkpoints.len(),
        "attempt_ref_count": persisted_attempts,
        "read_latency": latency_json(&read_latencies),
    }))
}

fn reset_db(path: &Path) -> Result<(), Box<dyn Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn run(args: &Args) -> Result<Value, Box<dyn Error>> {
    let (mut plans, attempts) = parse_plan(&args.corpus)?;
    let mut phase_memory = serde_json::Map::new();
    phase_memory.insert("after_parse".to_owned(), process_memory_json());
    if !args.resume {
        reset_db(&args.db)?;
    }
    let empty_path = args.db.with_extension("empty-baseline");
    reset_db(&empty_path)?;
    let empty = CheckpointStore::open(&empty_path, CheckpointStoreConfig::default())?;
    let empty_storage = storage_json(empty.storage()?);
    drop(empty);
    reset_db(&empty_path)?;

    let mut store = CheckpointStore::open(&args.db, CheckpointStoreConfig::default())?;
    phase_memory.insert("after_store_open".to_owned(), process_memory_json());
    let existing_checkpoint_count = store.checkpoint_count();
    if existing_checkpoint_count > plans.len() {
        return Err("store contains more checkpoints than the corpus".into());
    }
    let mut append_latencies = Vec::with_capacity(plans.len());
    let mut fork_latencies = Vec::new();
    let mut child_counts: HashMap<Option<usize>, usize> = HashMap::new();
    for plan in plans.iter().take(existing_checkpoint_count) {
        *child_counts.entry(plan.parent_index).or_insert(0) += 1;
    }

    for plan in plans.iter_mut().skip(existing_checkpoint_count) {
        let message_bytes = std::mem::take(&mut plan.message_bytes);
        let message: Value = serde_json::from_slice(&message_bytes)?;
        let started = Instant::now();
        store.append_messages_checkpoint(
            &plan.thread_id,
            &plan.checkpoint_id,
            plan.checkpoint_no,
            plan.parent_checkpoint_id.as_deref(),
            &[message],
        )?;
        let elapsed = started.elapsed().as_nanos();
        append_latencies.push(elapsed);
        let prior_children = child_counts.entry(plan.parent_index).or_insert(0);
        if plan.parent_index.is_some() && *prior_children > 0 {
            fork_latencies.push(elapsed);
        }
        *prior_children += 1;
    }
    phase_memory.insert("after_hot_ingest".to_owned(), process_memory_json());

    let attempt_ref_bytes = persist_attempt_refs(&args.db, &attempts)?;
    let hot_storage = storage_json(store.storage()?);
    let hot_exactness = verify(&store, &args.db, &plans, &attempts)?;
    phase_memory.insert("after_hot_verify".to_owned(), process_memory_json());
    let seal_started = Instant::now();
    let seal_report = store.seal_through(u64::try_from(plans.len())?)?;
    let seal_duration_ns = seal_started.elapsed().as_nanos();
    let sealed_storage = storage_json(store.storage()?);
    phase_memory.insert("after_seal".to_owned(), process_memory_json());
    let sealed_exactness = verify(&store, &args.db, &plans, &attempts)?;
    phase_memory.insert("after_sealed_verify".to_owned(), process_memory_json());
    drop(store);
    phase_memory.insert("after_hot_store_drop".to_owned(), process_memory_json());
    let reopen_started = Instant::now();
    let reopened = CheckpointStore::open(&args.db, CheckpointStoreConfig::default())?;
    let reopen_duration_ns = reopen_started.elapsed().as_nanos();
    let reopened_storage = storage_json(reopened.storage()?);
    phase_memory.insert("after_reopen".to_owned(), process_memory_json());
    let reopened_exactness = verify(&reopened, &args.db, &plans, &attempts)?;
    phase_memory.insert("after_reopened_verify".to_owned(), process_memory_json());
    let pass = hot_exactness["exact"] == Value::Bool(true)
        && sealed_exactness["exact"] == Value::Bool(true)
        && reopened_exactness["exact"] == Value::Bool(true);

    Ok(json!({
        "format_version": 1,
        "contract": "TULYA_BRANCH_FOREST_BENCHMARK_V1",
        "backend": "production-tulya-checkpoint-store",
        "pass": pass,
        "checkpoint_node_count": plans.len(),
        "attempt_ref_count": attempts.len(),
        "attempt_ref_bytes": attempt_ref_bytes,
        "resume": {
            "requested": args.resume,
            "existing_checkpoint_count": existing_checkpoint_count,
            "new_checkpoint_count": plans.len() - existing_checkpoint_count,
        },
        "process_memory_by_phase": phase_memory,
        "storage": {
            "empty": empty_storage,
            "hot": hot_storage,
            "sealed": sealed_storage,
            "reopened": reopened_storage,
        },
        "latency": {
            "durable_append": latency_json(&append_latencies),
            "durable_fork_append": latency_json(&fork_latencies),
            "seal_duration_ns": seal_duration_ns,
            "reopen_duration_ns": reopen_duration_ns,
        },
        "seal": {
            "checkpoint_count": seal_report.checkpoint_count,
            "newly_sealed_wal_bytes": seal_report.newly_sealed_wal_bytes,
            "hot_suffix_logical_bytes": seal_report.hot_suffix_logical_bytes,
        },
        "exactness": {
            "hot": hot_exactness,
            "sealed": sealed_exactness,
            "reopened": reopened_exactness,
        },
        "claims": {
            "performance_claim": false,
            "storage_advantage_claim": false,
            "holdout_accessed": args.corpus.file_name().and_then(|name| name.to_str()) == Some("holdout.jsonl"),
            "process_crash_claim": false,
            "power_loss_claim": false,
        },
    }))
}

fn main() {
    let args = Args::parse();
    match run(&args) {
        Ok(result) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&result).unwrap_or_default()
            );
            if result["pass"] != Value::Bool(true) {
                std::process::exit(1);
            }
        }
        Err(error) => {
            println!("{}", json!({"pass": false, "error": error.to_string()}));
            std::process::exit(1);
        }
    }
}
