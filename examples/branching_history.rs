use std::error::Error;
use std::path::PathBuf;

use serde_json::json;
use tulya_checkpoint_store::{fsck, CheckpointStore, CheckpointStoreConfig};

fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./tulya-example-store"));
    if path.exists() {
        return Err(format!("example output already exists: {}", path.display()).into());
    }

    let config = CheckpointStoreConfig {
        wal_segment_bytes: 64 * 1024,
        preinit_chunk_bytes: 16 * 1024,
        sealed_block_size: 4 * 1024,
        ..CheckpointStoreConfig::default()
    };
    let mut store = CheckpointStore::open(&path, config)?;
    store.append_messages_checkpoint(
        "support-case-42",
        "root",
        0,
        None,
        &[json!({"role": "user", "content": "Investigate the timeout"})],
    )?;
    store.append_messages_checkpoint(
        "support-case-42",
        "increase-timeout",
        1,
        Some("root"),
        &[json!({"role": "assistant", "content": "Try a longer timeout"})],
    )?;
    store.append_messages_checkpoint(
        "support-case-42",
        "fix-query",
        1,
        Some("root"),
        &[json!({"role": "assistant", "content": "Optimize the query"})],
    )?;
    store.seal_through(3)?;

    let first_branch = store.read_checkpoint("support-case-42", "increase-timeout")?;
    let second_branch = store.read_checkpoint("support-case-42", "fix-query")?;
    assert_ne!(first_branch, second_branch);
    println!("first branch: {}", String::from_utf8(first_branch)?);
    println!("second branch: {}", String::from_utf8(second_branch)?);
    drop(store);

    let report = fsck(&path)?;
    println!(
        "Format v{}: {} checkpoints, {} failures, {} bytes on disk",
        report.format_version, report.checkpoint_count, report.failures, report.hot_physical_bytes
    );
    Ok(())
}
