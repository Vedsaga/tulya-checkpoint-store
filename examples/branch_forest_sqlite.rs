use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Parser;
use rusqlite::{params, Connection, TransactionBehavior};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[derive(Parser, Debug)]
#[command(name = "branch-forest-sqlite")]
struct Args {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    corpus: PathBuf,
}

#[derive(Clone, Debug)]
struct PlanNode {
    instance_id: String,
    checkpoint_id: String,
    parent_checkpoint_id: Option<String>,
    sequence_no: u64,
    operation_json: Vec<u8>,
    state_len: u64,
    state_sha256: String,
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

fn parse_plan(path: &Path) -> Result<(Vec<PlanNode>, Vec<AttemptRef>), Box<dyn Error>> {
    let mut nodes = Vec::new();
    let mut attempts = Vec::new();
    let mut ids: HashMap<(String, String), ()> = HashMap::new();
    for (line_no, line) in BufReader::new(File::open(path)?).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: Value = serde_json::from_str(&line)
            .map_err(|error| format!("invalid corpus line {}: {error}", line_no + 1))?;
        let instance_id = row
            .get("instance_id")
            .and_then(Value::as_str)
            .ok_or("corpus row missing instance_id")?
            .to_owned();
        for node in row
            .get("nodes")
            .and_then(Value::as_array)
            .ok_or("corpus row missing nodes")?
        {
            let checkpoint_id = node
                .get("checkpoint_id")
                .and_then(Value::as_str)
                .ok_or("node missing checkpoint_id")?
                .to_owned();
            let parent_checkpoint_id = node
                .get("parent_checkpoint_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(parent) = parent_checkpoint_id.as_ref() {
                if !ids.contains_key(&(instance_id.clone(), parent.clone())) {
                    return Err("node parent is absent or not topologically prior".into());
                }
            }
            if ids
                .insert((instance_id.clone(), checkpoint_id.clone()), ())
                .is_some()
            {
                return Err("duplicate checkpoint identity".into());
            }
            let operation = node
                .get("operations")
                .and_then(Value::as_array)
                .and_then(|items| (items.len() == 1).then(|| &items[0]))
                .ok_or("node must have exactly one operation")?;
            if operation.get("op").and_then(Value::as_str) != Some("append_message") {
                return Err("node operation is not append_message".into());
            }
            nodes.push(PlanNode {
                instance_id: instance_id.clone(),
                checkpoint_id,
                parent_checkpoint_id,
                sequence_no: node
                    .get("sequence_no")
                    .and_then(Value::as_u64)
                    .ok_or("node missing sequence_no")?,
                operation_json: serde_json::to_vec(operation)?,
                state_len: node
                    .get("logical_state_len")
                    .and_then(Value::as_u64)
                    .ok_or("node missing logical_state_len")?,
                state_sha256: node
                    .get("logical_state_sha256")
                    .and_then(Value::as_str)
                    .ok_or("node missing logical_state_sha256")?
                    .to_owned(),
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
                tip_checkpoint_id: path
                    .last()
                    .and_then(Value::as_str)
                    .ok_or("attempt path is empty")?
                    .to_owned(),
            });
        }
    }
    if nodes.is_empty() || attempts.is_empty() {
        return Err("branch forest is empty".into());
    }
    Ok((nodes, attempts))
}

fn configure(conn: &Connection) -> Result<(), Box<dyn Error>> {
    let mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    if mode.to_ascii_lowercase() != "wal" {
        return Err("SQLite did not enter WAL mode".into());
    }
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.pragma_update(None, "wal_autocheckpoint", 0i64)?;
    conn.pragma_update(None, "foreign_keys", true)?;
    Ok(())
}

fn create_schema(conn: &Connection) -> Result<(), Box<dyn Error>> {
    conn.execute_batch(
        "CREATE TABLE node(\
             instance_id TEXT NOT NULL,\
             checkpoint_id TEXT NOT NULL,\
             parent_checkpoint_id TEXT,\
             sequence_no INTEGER NOT NULL,\
             operation_json BLOB NOT NULL,\
             logical_state_len INTEGER NOT NULL,\
             logical_state_sha256 TEXT NOT NULL,\
             PRIMARY KEY(instance_id, checkpoint_id)\
         ) WITHOUT ROWID;\
         CREATE INDEX node_parent ON node(instance_id, parent_checkpoint_id);\
         CREATE TABLE attempt(\
             instance_id TEXT NOT NULL,\
             attempt_id TEXT NOT NULL,\
             solved INTEGER NOT NULL,\
             source_row_fingerprint TEXT NOT NULL,\
             tip_checkpoint_id TEXT NOT NULL,\
             PRIMARY KEY(instance_id, attempt_id)\
         ) WITHOUT ROWID;",
    )?;
    Ok(())
}

fn allocated_bytes(path: &Path) -> u64 {
    let Ok(metadata) = fs::metadata(path) else {
        return 0;
    };
    #[cfg(unix)]
    {
        metadata.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        metadata.len()
    }
}

fn storage_json(directory: &Path) -> Result<Value, Box<dyn Error>> {
    let mut file_count = 0u64;
    let mut file_length_bytes = 0u64;
    let mut allocated = 0u64;
    if directory.is_dir() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                file_count += 1;
                file_length_bytes += entry.metadata()?.len();
                allocated += allocated_bytes(&entry.path());
            }
        }
    }
    Ok(json!({
        "file_count": file_count,
        "file_length_bytes": file_length_bytes,
        "allocated_bytes": allocated,
    }))
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

fn reconstruct_state(
    conn: &Connection,
    instance_id: &str,
    checkpoint_id: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut statement = conn.prepare(
        "WITH RECURSIVE chain(parent_checkpoint_id, operation_json, depth) AS (
             SELECT parent_checkpoint_id, operation_json, 0
             FROM node WHERE instance_id=?1 AND checkpoint_id=?2
             UNION ALL
             SELECT node.parent_checkpoint_id, node.operation_json, chain.depth+1
             FROM node JOIN chain
               ON node.instance_id=?1 AND node.checkpoint_id=chain.parent_checkpoint_id
         ) SELECT operation_json FROM chain ORDER BY depth DESC",
    )?;
    let mut query = statement.query(params![instance_id, checkpoint_id])?;
    let mut messages = Vec::new();
    while let Some(row) = query.next()? {
        let operation_bytes: Vec<u8> = row.get(0)?;
        let operation: Value = serde_json::from_slice(&operation_bytes)?;
        if operation.get("op").and_then(Value::as_str) != Some("append_message") {
            return Err("persisted operation is not append_message".into());
        }
        messages.push(
            operation
                .get("value")
                .cloned()
                .ok_or("persisted operation missing value")?,
        );
    }
    Ok(serde_json::to_vec(&json!({"messages": messages}))?)
}

fn verify(
    conn: &Connection,
    nodes: &[PlanNode],
    attempts: &[AttemptRef],
) -> Result<Value, Box<dyn Error>> {
    let mut failures = 0u64;
    let mut reads = Vec::with_capacity(nodes.len());
    for node in nodes {
        let started = Instant::now();
        let state = reconstruct_state(conn, &node.instance_id, &node.checkpoint_id)?;
        reads.push(started.elapsed().as_nanos());
        if u64::try_from(state.len())? != node.state_len || sha256_hex(&state) != node.state_sha256
        {
            failures += 1;
        }
    }
    let node_count: i64 = conn.query_row("SELECT COUNT(*) FROM node", [], |row| row.get(0))?;
    let attempt_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM attempt", [], |row| row.get(0))?;
    Ok(json!({
        "exact": failures == 0
            && usize::try_from(node_count)? == nodes.len()
            && usize::try_from(attempt_count)? == attempts.len(),
        "state_failures": failures,
        "checkpoint_count": node_count,
        "attempt_ref_count": attempt_count,
        "read_latency": latency_json(&reads),
    }))
}

fn reset(path: &Path) -> Result<(), Box<dyn Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn empty_baseline(directory: &Path) -> Result<Value, Box<dyn Error>> {
    reset(directory)?;
    let path = directory.join("state.sqlite");
    let connection = Connection::open(path)?;
    configure(&connection)?;
    create_schema(&connection)?;
    let _: (i64, i64, i64) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    drop(connection);
    storage_json(directory)
}

fn run(args: &Args) -> Result<Value, Box<dyn Error>> {
    let (nodes, attempts) = parse_plan(&args.corpus)?;
    let empty_dir = args.db.with_extension("empty-baseline");
    let empty_storage = empty_baseline(&empty_dir)?;
    reset(&empty_dir)?;
    fs::remove_dir(&empty_dir)?;
    reset(&args.db)?;
    let db_path = args.db.join("state.sqlite");
    let mut connection = Connection::open(&db_path)?;
    configure(&connection)?;
    create_schema(&connection)?;
    let mut write_latencies = Vec::with_capacity(nodes.len());

    for node in &nodes {
        let started = Instant::now();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO node(\
                 instance_id, checkpoint_id, parent_checkpoint_id, sequence_no,\
                 operation_json, logical_state_len, logical_state_sha256\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                node.instance_id,
                node.checkpoint_id,
                node.parent_checkpoint_id,
                i64::try_from(node.sequence_no)?,
                node.operation_json,
                i64::try_from(node.state_len)?,
                node.state_sha256,
            ],
        )?;
        transaction.commit()?;
        write_latencies.push(started.elapsed().as_nanos());
    }
    let attempt_transaction =
        connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for attempt in &attempts {
        attempt_transaction.execute(
            "INSERT INTO attempt(\
                 instance_id, attempt_id, solved, source_row_fingerprint, tip_checkpoint_id\
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                attempt.instance_id,
                attempt.attempt_id,
                attempt.solved,
                attempt.source_row_fingerprint,
                attempt.tip_checkpoint_id,
            ],
        )?;
    }
    attempt_transaction.commit()?;
    let active_storage = storage_json(&args.db)?;
    let hot_exactness = verify(&connection, &nodes, &attempts)?;
    let checkpoint_started = Instant::now();
    let checkpoint_result: (i64, i64, i64) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    let checkpoint_duration_ns = checkpoint_started.elapsed().as_nanos();
    let checkpointed_storage = storage_json(&args.db)?;
    drop(connection);
    let reopen_started = Instant::now();
    let reopened = Connection::open(&db_path)?;
    reopened.pragma_update(None, "query_only", true)?;
    let reopen_duration_ns = reopen_started.elapsed().as_nanos();
    let reopened_exactness = verify(&reopened, &nodes, &attempts)?;
    let reopened_storage = storage_json(&args.db)?;
    let pass = hot_exactness["exact"] == Value::Bool(true)
        && reopened_exactness["exact"] == Value::Bool(true)
        && checkpoint_result.0 == 0;

    Ok(json!({
        "format_version": 1,
        "contract": "TULYA_BRANCH_FOREST_BENCHMARK_V1",
        "backend": "sqlite-normalized-parent-operation",
        "pass": pass,
        "checkpoint_node_count": nodes.len(),
        "attempt_ref_count": attempts.len(),
        "durability": {
            "journal_mode": "WAL",
            "synchronous": "FULL",
            "transaction_per_checkpoint": true,
            "wal_autocheckpoint": 0,
        },
        "storage": {
            "empty": empty_storage,
            "active": active_storage,
            "checkpointed": checkpointed_storage,
            "reopened": reopened_storage,
        },
        "latency": {
            "durable_append": latency_json(&write_latencies),
            "checkpoint_duration_ns": checkpoint_duration_ns,
            "reopen_duration_ns": reopen_duration_ns,
        },
        "wal_checkpoint": {
            "busy": checkpoint_result.0,
            "log_frames": checkpoint_result.1,
            "checkpointed_frames": checkpoint_result.2,
        },
        "exactness": {
            "active": hot_exactness,
            "reopened": reopened_exactness,
        },
        "claims": {
            "performance_claim": false,
            "storage_advantage_claim": false,
            "holdout_accessed": args.corpus.file_name().and_then(|name| name.to_str()) == Some("holdout.jsonl"),
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
