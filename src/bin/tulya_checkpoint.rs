//! Command-line surface for the production branch-aware message checkpoint store.

use std::error::Error;
use std::io::{self, Read};
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use serde_json::{json, Value};
use tulya_checkpoint_store::{CheckpointStore, CheckpointStoreConfig};

#[derive(Debug, Parser)]
#[command(
    name = "tulya-checkpoint",
    about = "Durable branch-aware checkpoint store for append-only agent messages"
)]
struct Args {
    #[arg(long)]
    db: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Append one checkpoint. Read a JSON array of newly appended messages from stdin.
    Put {
        #[arg(long)]
        thread_id: String,
        #[arg(long)]
        checkpoint_id: String,
        #[arg(long)]
        checkpoint_no: u32,
        #[arg(long)]
        parent_checkpoint_id: Option<String>,
    },
    /// Read one exact canonical checkpoint as JSON.
    Get {
        #[arg(long)]
        thread_id: String,
        #[arg(long)]
        checkpoint_id: String,
    },
    /// List committed checkpoint metadata in commit order.
    List,
    /// Verify all committed logical-state lengths and hashes.
    Verify,
    /// Report complete store-file accounting and lifecycle counters.
    Stats,
    /// Seal all checkpoints through the requested global count.
    Seal {
        #[arg(long)]
        through: Option<u64>,
    },
    /// Export every checkpoint and exact canonical state in commit order.
    Export,
    /// Import a deterministic export into an empty store. Read JSON from stdin.
    Import,
}

fn read_json_input() -> Result<Value, Box<dyn Error>> {
    let mut input = Vec::new();
    io::stdin().read_to_end(&mut input)?;
    Ok(serde_json::from_slice(&input)?)
}

fn read_message_delta() -> Result<Vec<Value>, Box<dyn Error>> {
    let value = read_json_input()?;
    value
        .as_array()
        .cloned()
        .ok_or_else(|| "stdin must contain a JSON array of newly appended messages".into())
}

fn canonical_messages(state: &Value) -> Result<&[Value], Box<dyn Error>> {
    let object = state
        .as_object()
        .ok_or("checkpoint state must be a JSON object")?;
    if object.len() != 2 || object.get("identity") != Some(&Value::Null) {
        return Err("checkpoint state must contain only identity:null and messages".into());
    }
    object
        .get("messages")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| "checkpoint state messages must be a JSON array".into())
}

fn run(args: Args) -> Result<Value, Box<dyn Error>> {
    let mut store = CheckpointStore::open(&args.db, CheckpointStoreConfig::default())?;
    match args.command {
        Command::Put {
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
        } => {
            let messages = read_message_delta()?;
            let report = store.append_messages_checkpoint(
                &thread_id,
                &checkpoint_id,
                checkpoint_no,
                parent_checkpoint_id.as_deref(),
                &messages,
            )?;
            Ok(json!({
                "ok": true,
                "operation": "put",
                "thread_id": thread_id,
                "checkpoint_id": checkpoint_id,
                "checkpoint_count": store.checkpoint_count(),
                "transaction_bytes": report.transaction_bytes,
                "logical_tail_bytes": report.logical_tail_bytes,
                "capacity_bytes": report.capacity_bytes,
                "write_ns": report.write_ns,
                "sync_data_ns": report.sync_data_ns,
            }))
        }
        Command::Get {
            thread_id,
            checkpoint_id,
        } => {
            let state = store.read_checkpoint(&thread_id, &checkpoint_id)?;
            let state: Value = serde_json::from_slice(&state)?;
            Ok(json!({
                "ok": true,
                "operation": "get",
                "thread_id": thread_id,
                "checkpoint_id": checkpoint_id,
                "state": state,
            }))
        }
        Command::List => Ok(json!({
            "ok": true,
            "operation": "list",
            "checkpoints": store
                .checkpoints()
                .iter()
                .map(|checkpoint| json!({
                    "ordinal": checkpoint.ordinal,
                    "thread_id": checkpoint.thread_id,
                    "checkpoint_no": checkpoint.checkpoint_no,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "parent_checkpoint_id": checkpoint.parent_checkpoint_id,
                    "logical_state_len": checkpoint.logical_state_len,
                    "state_hash_xxh3_64": checkpoint.state_hash,
                }))
                .collect::<Vec<_>>(),
        })),
        Command::Verify => {
            let report = store.verify_all()?;
            Ok(json!({
                "ok": report.failures == 0,
                "operation": "verify",
                "checkpoint_count": report.checkpoint_count,
                "failures": report.failures,
            }))
        }
        Command::Stats => {
            let storage = store.storage()?;
            Ok(json!({
                "ok": true,
                "operation": "stats",
                "checkpoint_count": store.checkpoint_count(),
                "version_count": store.version_count(),
                "sealed_checkpoint_count": store.sealed_checkpoint_count(),
                "hot_logical_bytes": store.hot_logical_bytes(),
                "hot_capacity_bytes": store.hot_capacity_bytes()?,
                "storage": {
                    "file_length_bytes": storage.file_length_bytes,
                    "allocated_bytes": storage.allocated_bytes,
                    "file_count": storage.file_count,
                },
            }))
        }
        Command::Seal { through } => {
            let target = through.unwrap_or(u64::try_from(store.checkpoint_count())?);
            let report = store.seal_through(target)?;
            Ok(json!({
                "ok": true,
                "operation": "seal",
                "generation": report.generation,
                "checkpoint_count": report.checkpoint_count,
                "newly_sealed_wal_bytes": report.newly_sealed_wal_bytes,
                "hot_suffix_logical_bytes": report.hot_suffix_logical_bytes,
                "reclaimed_allocated_bytes": report.reclaimed.allocated_bytes,
            }))
        }
        Command::Export => {
            let mut checkpoints = Vec::with_capacity(store.checkpoint_count());
            for checkpoint in store.checkpoints() {
                let state =
                    store.read_checkpoint(&checkpoint.thread_id, &checkpoint.checkpoint_id)?;
                checkpoints.push(json!({
                    "thread_id": checkpoint.thread_id,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "checkpoint_no": checkpoint.checkpoint_no,
                    "parent_checkpoint_id": checkpoint.parent_checkpoint_id,
                    "state": serde_json::from_slice::<Value>(&state)?,
                }));
            }
            Ok(json!({
                "format": "tulya-append-only-message-export",
                "format_version": 1,
                "checkpoint_count": checkpoints.len(),
                "checkpoints": checkpoints,
            }))
        }
        Command::Import => {
            if store.checkpoint_count() != 0 {
                return Err("import requires an empty Tulya store".into());
            }
            let export = read_json_input()?;
            if export.get("format")
                != Some(&Value::String(
                    "tulya-append-only-message-export".to_owned(),
                ))
                || export.get("format_version") != Some(&Value::from(1))
            {
                return Err("stdin is not a Tulya append-only message export v1".into());
            }
            let records = export
                .get("checkpoints")
                .and_then(Value::as_array)
                .ok_or("export checkpoints must be a JSON array")?;
            for record in records {
                let thread_id = record
                    .get("thread_id")
                    .and_then(Value::as_str)
                    .ok_or("export checkpoint thread_id must be a string")?;
                let checkpoint_id = record
                    .get("checkpoint_id")
                    .and_then(Value::as_str)
                    .ok_or("export checkpoint checkpoint_id must be a string")?;
                let checkpoint_no = u32::try_from(
                    record
                        .get("checkpoint_no")
                        .and_then(Value::as_u64)
                        .ok_or("export checkpoint checkpoint_no must be an unsigned integer")?,
                )?;
                let parent_checkpoint_id = match record.get("parent_checkpoint_id") {
                    None | Some(Value::Null) => None,
                    Some(value) => Some(
                        value
                            .as_str()
                            .ok_or("export parent_checkpoint_id must be a string or null")?,
                    ),
                };
                let state = record
                    .get("state")
                    .ok_or("export checkpoint state is absent")?;
                let messages = canonical_messages(state)?;
                let parent_len = if let Some(parent_id) = parent_checkpoint_id {
                    let parent = store.read_checkpoint(thread_id, parent_id)?;
                    let parent: Value = serde_json::from_slice(&parent)?;
                    let parent_messages = canonical_messages(&parent)?;
                    if messages.len() < parent_messages.len()
                        || messages[..parent_messages.len()] != *parent_messages
                    {
                        return Err(format!(
                            "checkpoint {checkpoint_id} does not append to parent {parent_id}"
                        )
                        .into());
                    }
                    parent_messages.len()
                } else {
                    0
                };
                let delta = &messages[parent_len..];
                if delta.is_empty() {
                    return Err(format!(
                        "checkpoint {checkpoint_id} has no append-only message delta"
                    )
                    .into());
                }
                store.append_messages_checkpoint(
                    thread_id,
                    checkpoint_id,
                    checkpoint_no,
                    parent_checkpoint_id,
                    delta,
                )?;
                let reconstructed = store.read_checkpoint(thread_id, checkpoint_id)?;
                if reconstructed != serde_json::to_vec(state)? {
                    return Err(format!(
                        "checkpoint {checkpoint_id} failed exact import verification"
                    )
                    .into());
                }
            }
            let report = store.verify_all()?;
            Ok(json!({
                "ok": report.failures == 0,
                "operation": "import",
                "checkpoint_count": report.checkpoint_count,
                "failures": report.failures,
            }))
        }
    }
}

fn main() {
    let args = Args::parse();
    match run(args) {
        Ok(value) => println!("{}", serde_json::to_string(&value).unwrap_or_default()),
        Err(error) => {
            println!("{}", json!({"ok": false, "error": error.to_string()}));
            std::process::exit(1);
        }
    }
}
