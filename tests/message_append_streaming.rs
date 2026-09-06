use serde_json::json;
use tulya_checkpoint_store::{CheckpointStore, CheckpointStoreConfig, CheckpointStoreRecoveryMode};

#[test]
fn message_append_streaming_hash_matches_full_v1_state_across_chunks(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 4096,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
    };

    let parent_message = json!({
        "role": "user",
        "content": "a".repeat(3 * 64 * 1024),
    });
    let child_message = json!({
        "role": "assistant",
        "content": "child",
    });
    let expected_child = serde_json::to_vec(&json!({
        "identity": null,
        "messages": [parent_message.clone(), child_message.clone()],
    }))?;

    let mut store = CheckpointStore::open(temp.path(), config)?;
    store.append_messages_checkpoint(
        "thread",
        "parent",
        0,
        None,
        std::slice::from_ref(&parent_message),
    )?;
    store.append_messages_checkpoint(
        "thread",
        "child",
        1,
        Some("parent"),
        std::slice::from_ref(&child_message),
    )?;

    assert_eq!(store.read_checkpoint("thread", "child")?, expected_child);
    assert_eq!(store.verify_all()?.failures, 0);

    store.seal_through(2)?;
    drop(store);

    let reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(reopened.read_checkpoint("thread", "child")?, expected_child);
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}
