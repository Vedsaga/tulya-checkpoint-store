use super::*;
use std::sync::{Mutex, MutexGuard, OnceLock};

fn nested_process_test_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("nested checkpoint-store process test guard was poisoned")
}

fn root_record(id: u32, root: u64, parent: Option<u32>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&ROOT_MAGIC);
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&root.to_le_bytes());
    out.extend_from_slice(&parent.unwrap_or(NONE_PARENT).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    let hash = xxh3_64(&out);
    out.extend_from_slice(&hash.to_le_bytes());
    out
}

fn checkpoint_record(
    checkpoint_no: u32,
    identity_version: u32,
    thread: &str,
    checkpoint_id: &str,
    parent: Option<&str>,
    canonical: &[u8],
) -> Vec<u8> {
    let parent = parent.unwrap_or("");
    let total = CHECKPOINT_PREFIX_SIZE + thread.len() + checkpoint_id.len() + parent.len() + 8;
    let mut out = Vec::new();
    out.extend_from_slice(&CHECKPOINT_MAGIC);
    out.extend_from_slice(&u32::try_from(total).unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&checkpoint_no.to_le_bytes());
    out.extend_from_slice(&identity_version.to_le_bytes());
    out.extend_from_slice(&NONE_VERSION.to_le_bytes());
    out.extend_from_slice(&NONE_VERSION.to_le_bytes());
    out.extend_from_slice(&u32::try_from(thread.len()).unwrap_or(0).to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(checkpoint_id.len())
            .unwrap_or(0)
            .to_le_bytes(),
    );
    out.extend_from_slice(&u32::try_from(parent.len()).unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&u64::try_from(canonical.len()).unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&xxh3_64(canonical).to_le_bytes());
    out.extend_from_slice(thread.as_bytes());
    out.extend_from_slice(checkpoint_id.as_bytes());
    out.extend_from_slice(parent.as_bytes());
    let hash = xxh3_64(&out);
    out.extend_from_slice(&hash.to_le_bytes());
    out
}

fn transaction(
    version_start: u32,
    checkpoint_count: u32,
    byte_start: u64,
    node_start: u64,
    identity: &[u8],
    checkpoint_id: &str,
    parent: Option<&str>,
) -> Vec<u8> {
    transaction_for_thread(
        version_start,
        checkpoint_count,
        byte_start,
        node_start,
        identity,
        "thread",
        checkpoint_id,
        parent,
    )
}

fn transaction_for_thread(
    version_start: u32,
    checkpoint_count: u32,
    byte_start: u64,
    node_start: u64,
    identity: &[u8],
    thread: &str,
    checkpoint_id: &str,
    parent: Option<&str>,
) -> Vec<u8> {
    let root_id = node_start;
    let mut slot = [0u8; COMPACT_NODE_SIZE];
    slot[..8].copy_from_slice(&byte_start.to_le_bytes());
    slot[8..12].copy_from_slice(&u32::try_from(identity.len()).unwrap_or(0).to_le_bytes());
    slot[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
    let mut canonical = Vec::new();
    canonical.extend_from_slice(b"{\"identity\":");
    canonical.extend_from_slice(identity);
    canonical.extend_from_slice(b",\"messages\":[]}");
    let cp = checkpoint_record(
        checkpoint_count - 1,
        version_start,
        thread,
        checkpoint_id,
        parent,
        &canonical,
    );
    let root = root_record(version_start, root_id, version_start.checked_sub(1));
    let total =
        TX_HEADER_SIZE + identity.len() + slot.len() + root.len() + cp.len() + TX_CHECKSUM_SIZE;
    let mut out = Vec::new();
    out.extend_from_slice(&TX_MAGIC);
    out.extend_from_slice(&u32::try_from(total).unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&version_start.to_le_bytes());
    out.extend_from_slice(&(version_start + 1).to_le_bytes());
    out.extend_from_slice(&checkpoint_count.to_le_bytes());
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&byte_start.to_le_bytes());
    out.extend_from_slice(&u64::try_from(identity.len()).unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&node_start.to_le_bytes());
    out.extend_from_slice(&1u32.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&u32::try_from(cp.len()).unwrap_or(0).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(identity);
    out.extend_from_slice(&slot);
    out.extend_from_slice(&root);
    out.extend_from_slice(&cp);
    let hash = xxh3_64(&out);
    out.extend_from_slice(&hash.to_le_bytes());
    out
}

#[test]
fn append_seal_reopen_append_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 4096,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
    };
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    store.append_encoded_transaction(&tx1)?;
    let expected1 = b"{\"identity\":{},\"messages\":[]}";
    assert_eq!(store.read_checkpoint("thread", "cp-1")?, expected1);
    let seal = store.seal_through(1)?;
    assert_eq!(seal.hot_suffix_logical_bytes, 0);
    assert_eq!(store.verify_all()?.failures, 0);
    drop(store);

    let mut reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, expected1);
    let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
    reopened.append_encoded_transaction(&tx2)?;
    let expected2 = b"{\"identity\":{\"x\":1},\"messages\":[]}";
    assert_eq!(reopened.read_checkpoint("thread", "cp-2")?, expected2);
    assert_eq!(reopened.verify_all()?.failures, 0);
    assert_eq!(reopened.sealed_checkpoint_count(), 1);
    assert!(reopened.hot_capacity_bytes()? >= config.wal_segment_bytes);
    Ok(())
}

#[test]
fn semantic_checkpoint_append_round_trips_through_seal_and_reopen(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 4096,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
    };
    let identity = br#"{"files":[{"path":"src/lib.rs","length":16}]}"#;
    let canonical = {
        let mut value = Vec::new();
        value.extend_from_slice(b"{\"identity\":");
        value.extend_from_slice(identity);
        value.extend_from_slice(b",\"messages\":[]}");
        value
    };
    let mut store = CheckpointStore::open(temp.path(), config)?;
    store.append_checkpoint("repo", "commit-0", 7, None, identity)?;
    assert_eq!(store.read_checkpoint("repo", "commit-0")?, canonical);
    assert_eq!(
        store.read_checkpoint_range("repo", "commit-0", 12, 16)?,
        identity[..16]
    );
    store.seal_through(1)?;
    drop(store);

    let reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(reopened.read_checkpoint("repo", "commit-0")?, canonical);
    assert_eq!(reopened.checkpoints()[0].checkpoint_no, 7);
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn message_checkpoint_append_preserves_branches_and_multiple_roots(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 4096,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
    };
    let a = json!({"role": "user", "content": "root"});
    let b1 = json!({"role": "assistant", "content": "left-1"});
    let b2 = json!({"role": "tool", "content": "left-2"});
    let c = json!({"role": "assistant", "content": "right"});
    let x = json!({"role": "user", "content": "second-thread"});

    let mut store = CheckpointStore::open(temp.path(), config)?;
    store.append_messages_checkpoint("thread-1", "A", 0, None, std::slice::from_ref(&a))?;
    store.append_messages_checkpoint("thread-1", "B", 1, Some("A"), &[b1.clone(), b2.clone()])?;
    store.append_messages_checkpoint("thread-1", "C", 2, Some("A"), std::slice::from_ref(&c))?;
    store.append_messages_checkpoint("thread-2", "X", 3, None, std::slice::from_ref(&x))?;

    assert_eq!(
        store.read_checkpoint("thread-1", "A")?,
        serde_json::to_vec(&json!({"identity": null, "messages": [a.clone()]}))?
    );
    assert_eq!(
        store.read_checkpoint("thread-1", "B")?,
        serde_json::to_vec(&json!({"identity": null, "messages": [a.clone(), b1, b2]}))?
    );
    assert_eq!(
        store.read_checkpoint("thread-1", "C")?,
        serde_json::to_vec(&json!({"identity": null, "messages": [a, c]}))?
    );
    assert_eq!(
        store.read_checkpoint("thread-2", "X")?,
        serde_json::to_vec(&json!({"identity": null, "messages": [x]}))?
    );
    assert_eq!(store.checkpoint_count(), 4);
    assert_eq!(store.version_count(), 5);
    assert_eq!(store.verify_all()?.failures, 0);
    assert!(store
        .append_messages_checkpoint("thread-1", "empty", 4, Some("B"), &[])
        .is_err());

    store.seal_through(4)?;
    drop(store);
    let reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(reopened.checkpoint_count(), 4);
    assert_eq!(reopened.version_count(), 5);
    assert_eq!(reopened.verify_all()?.failures, 0);
    assert_eq!(
        reopened.read_checkpoint("thread-1", "B")?,
        serde_json::to_vec(&json!({
            "identity": null,
            "messages": [
                {"role": "user", "content": "root"},
                {"role": "assistant", "content": "left-1"},
                {"role": "tool", "content": "left-2"}
            ]
        }))?
    );
    Ok(())
}

#[test]
fn subtree_prune_reclaims_deleted_branch_and_preserves_sibling(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 4096,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
    };
    let a = b"{}".to_vec();
    let b = b"{\"branch\":\"base\"}".to_vec();
    let mut c = Vec::with_capacity(256 * 1024);
    c.extend_from_slice(b"{\"blob\":\"");
    let mut noise = 0x9e37_79b9_u32;
    for _ in 0..(256 * 1024) {
        noise = noise.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        c.push(b'a' + u8::try_from((noise >> 24) % 26)?);
    }
    c.extend_from_slice(b"\"}");
    let d = b"{\"branch\":\"sibling\"}".to_vec();
    let canonical = |identity: &[u8]| {
        let mut value = Vec::new();
        value.extend_from_slice(b"{\"identity\":");
        value.extend_from_slice(identity);
        value.extend_from_slice(b",\"messages\":[]}");
        value
    };
    let mut byte_start = 0u64;
    let mut node_start = 0u64;
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let tx_a = transaction_for_thread(0, 1, byte_start, node_start, &a, "thread", "A", None);
    store.append_encoded_transaction(&tx_a)?;
    byte_start += u64::try_from(a.len())?;
    node_start += 1;
    let tx_b = transaction_for_thread(1, 2, byte_start, node_start, &b, "thread", "B", Some("A"));
    store.append_encoded_transaction(&tx_b)?;
    byte_start += u64::try_from(b.len())?;
    node_start += 1;
    let tx_c = transaction_for_thread(2, 3, byte_start, node_start, &c, "thread", "C", Some("B"));
    store.append_encoded_transaction_with_request_id(b"request-c", &tx_c)?;
    byte_start += u64::try_from(c.len())?;
    node_start += 1;
    let tx_d = transaction_for_thread(3, 4, byte_start, node_start, &d, "thread", "D", Some("B"));
    store.append_encoded_transaction(&tx_d)?;
    store.seal_through(4)?;
    let before = store.storage()?.allocated_bytes;
    let store_id = store.store_id();

    let report = store.delete_checkpoint_subtree("thread", "C")?;
    assert_eq!(report.deleted_checkpoint_count, 1);
    assert_eq!(report.retained_checkpoint_count, 3);
    assert!(report.reclaimed.allocated_bytes < before);
    assert_eq!(store.store_id(), store_id);
    assert_eq!(store.read_checkpoint("thread", "A")?, canonical(&a));
    assert_eq!(store.read_checkpoint("thread", "B")?, canonical(&b));
    assert_eq!(store.read_checkpoint("thread", "D")?, canonical(&d));
    assert!(matches!(
        store.read_checkpoint("thread", "C"),
        Err(CheckpointStoreError::CheckpointDeleted)
    ));
    assert!(matches!(
        store.append_encoded_transaction_with_request_id(b"request-c", &tx_c),
        Err(CheckpointStoreError::CheckpointDeleted)
    ));
    let conflicting_c = transaction_for_thread(
        2,
        3,
        u64::try_from(a.len() + b.len())?,
        2,
        b"{\"different\":true}",
        "thread",
        "C",
        Some("B"),
    );
    assert!(matches!(
        store.append_encoded_transaction_with_request_id(b"request-c", &conflicting_c),
        Err(CheckpointStoreError::RequestIdConflict)
    ));
    assert_eq!(store.verify_all()?.failures, 0);
    drop(store);

    let mut reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(reopened.store_id(), store_id);
    assert_eq!(reopened.read_checkpoint("thread", "D")?, canonical(&d));
    assert!(matches!(
        reopened.read_checkpoint("thread", "C"),
        Err(CheckpointStoreError::CheckpointDeleted)
    ));
    let deleted_append = reopened.append_encoded_transaction(&tx_c);
    assert!(
        matches!(deleted_append, Err(CheckpointStoreError::CheckpointDeleted)),
        "unexpected append result after prune: {deleted_append:?}"
    );
    let next = transaction(
        u32::try_from(reopened.version_count())?,
        u32::try_from(reopened.checkpoint_count() + 1)?,
        u64::try_from(reopened.state.arena_bytes.len())?,
        u64::try_from(reopened.state.compact_nodes.len() / COMPACT_NODE_SIZE)?,
        b"{\"post\":true}",
        "E",
        Some("D"),
    );
    reopened.append_encoded_transaction(&next)?;
    reopened.seal_through(4)?;
    assert_eq!(reopened.verify_all()?.failures, 0);
    drop(reopened);
    let mut final_reopen = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(
        final_reopen.read_checkpoint("thread", "E")?,
        canonical(b"{\"post\":true}")
    );
    assert_eq!(final_reopen.verify_all()?.failures, 0);
    let second_report = final_reopen.delete_checkpoint_subtree("thread", "B")?;
    assert_eq!(second_report.deleted_checkpoint_count, 3);
    assert_eq!(second_report.retained_checkpoint_count, 1);
    assert_eq!(final_reopen.read_checkpoint("thread", "A")?, canonical(&a));
    assert!(matches!(
        final_reopen.read_checkpoint("thread", "B"),
        Err(CheckpointStoreError::CheckpointDeleted)
    ));
    assert!(matches!(
        final_reopen.read_checkpoint("thread", "E"),
        Err(CheckpointStoreError::CheckpointDeleted)
    ));
    assert!(matches!(
        final_reopen.append_encoded_transaction_with_request_id(b"request-c", &tx_c),
        Err(CheckpointStoreError::CheckpointDeleted)
    ));
    assert_eq!(final_reopen.verify_all()?.failures, 0);
    drop(final_reopen);
    let final_pruned = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(final_pruned.read_checkpoint("thread", "A")?, canonical(&a));
    assert!(matches!(
        final_pruned.read_checkpoint("thread", "D"),
        Err(CheckpointStoreError::CheckpointDeleted)
    ));
    let mut lazy = LazyCheckpointStore::open(temp.path())?;
    assert_eq!(lazy.read_checkpoint("thread", "A")?, canonical(&a));
    assert!(matches!(
        lazy.read_checkpoint("thread", "D"),
        Err(CheckpointStoreError::CheckpointDeleted)
    ));
    Ok(())
}

#[test]
fn subtree_prune_requires_a_sealed_store() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let tx = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    store.append_encoded_transaction(&tx)?;
    assert!(matches!(
        store.delete_checkpoint_subtree("thread", "cp-1"),
        Err(CheckpointStoreError::PruneRequiresSealedStore)
    ));
    assert_eq!(
        store.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    Ok(())
}

#[test]
fn subtree_prune_staging_failure_preserves_old_authority() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let tx = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    store.append_encoded_transaction(&tx)?;
    store.seal_through(1)?;
    drop(store);

    fs::create_dir(temp.path().join("structured-g000002.t3s"))?;
    let mut reopened = CheckpointStore::open(temp.path(), config)?;
    assert!(reopened
        .delete_checkpoint_subtree("thread", "cp-1")
        .is_err());
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    drop(reopened);

    let mut recovered = CheckpointStore::open(temp.path(), config)?;
    assert!(!temp.path().join(".structured-g000002.t3s.tmp").exists());
    fs::remove_dir(temp.path().join("structured-g000002.t3s"))?;
    let report = recovered.delete_checkpoint_subtree("thread", "cp-1")?;
    assert_eq!(report.deleted_checkpoint_count, 1);
    assert!(matches!(
        recovered.read_checkpoint("thread", "cp-1"),
        Err(CheckpointStoreError::CheckpointDeleted)
    ));
    Ok(())
}

#[test]
fn bounded_wal_lifecycle_seals_at_transaction_boundaries() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 4096,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
    };
    let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
    let policy = BoundedWalLifecyclePolicy::new(
        u64::try_from(tx1.len() + tx2.len() - 1)?,
        u64::try_from(tx1.len() + tx2.len())?,
    )?;
    let mut store = CheckpointStore::open(temp.path(), config)?;

    let first = store.append_encoded_transaction_with_bounded_lifecycle(&tx1, policy)?;
    assert!(first.automatic_seal.is_none());
    assert_eq!(store.hot_logical_bytes(), u64::try_from(tx1.len())?);
    let second = store.append_encoded_transaction_with_bounded_lifecycle(&tx2, policy)?;
    assert_eq!(
        second.automatic_seal.map(|seal| seal.checkpoint_count),
        Some(1)
    );
    assert_eq!(store.sealed_checkpoint_count(), 1);
    assert_eq!(store.hot_logical_bytes(), u64::try_from(tx2.len())?);
    drop(store);

    let reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-2")?,
        b"{\"identity\":{\"x\":1},\"messages\":[]}"
    );
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn bounded_wal_lifecycle_reopens_each_automatic_cycle() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
    let tx3 = transaction(2, 3, 9, 2, b"{\"x\":2}", "cp-3", Some("cp-2"));
    let policy = BoundedWalLifecyclePolicy::new(
        u64::try_from(tx1.len() + tx2.len() - 1)?,
        u64::try_from(tx1.len() + tx2.len())?,
    )?;

    let mut store = CheckpointStore::open(temp.path(), config)?;
    store.append_encoded_transaction_with_bounded_lifecycle(&tx1, policy)?;
    let second = store.append_encoded_transaction_with_bounded_lifecycle(&tx2, policy)?;
    assert!(second.automatic_seal.is_some());
    drop(store);

    let mut reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-2")?,
        b"{\"identity\":{\"x\":1},\"messages\":[]}"
    );
    assert_eq!(reopened.verify_all()?.failures, 0);
    let third = reopened.append_encoded_transaction_with_bounded_lifecycle(&tx3, policy)?;
    assert!(third.automatic_seal.is_some());
    drop(reopened);

    let final_reopen = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(final_reopen.sealed_checkpoint_count(), 2);
    assert_eq!(
        final_reopen.read_checkpoint("thread", "cp-3")?,
        b"{\"identity\":{\"x\":2},\"messages\":[]}"
    );
    assert_eq!(final_reopen.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn bounded_wal_lifecycle_seal_failure_backpressures_without_append(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
    let policy = BoundedWalLifecyclePolicy::new(
        u64::try_from(tx1.len())?,
        u64::try_from(tx1.len() + tx2.len())?,
    )?;
    let mut store = CheckpointStore::open(temp.path(), config)?;
    store.append_encoded_transaction_with_bounded_lifecycle(&tx1, policy)?;
    let hot_before = store.hot_logical_bytes();
    fs::create_dir(temp.path().join(".structured-g000001.t3s.tmp"))?;

    let error = store
        .append_encoded_transaction_with_bounded_lifecycle(&tx2, policy)
        .expect_err("seal failure must backpressure the next transaction");
    assert!(matches!(error, CheckpointStoreError::Io(_)));
    assert_eq!(store.hot_logical_bytes(), hot_before);
    assert_eq!(store.checkpoint_count(), 1);
    fs::remove_dir(temp.path().join(".structured-g000001.t3s.tmp"))?;
    drop(store);

    let reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn bounded_wal_lifecycle_rejects_oversize_before_wal_change(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    let size = u64::try_from(transaction.len())?;
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let policy = BoundedWalLifecyclePolicy::new(size - 1, size - 1)?;
    let error = store
        .append_encoded_transaction_with_bounded_lifecycle(&transaction, policy)
        .expect_err("transaction above hard limit must be rejected");
    assert!(matches!(error, CheckpointStoreError::Format(_)));
    assert_eq!(store.hot_logical_bytes(), 0);
    assert_eq!(store.checkpoint_count(), 0);

    let policy = BoundedWalLifecyclePolicy::new(size - 1, size)?;
    let report = store.append_encoded_transaction_with_bounded_lifecycle(&transaction, policy)?;
    assert!(report.automatic_seal.is_none());
    assert_eq!(store.hot_logical_bytes(), size);
    Ok(())
}

#[test]
fn writer_lock_rejects_second_open_and_releases_on_drop() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let store = CheckpointStore::open(temp.path(), config)?;
    let second = CheckpointStore::open(temp.path(), config);
    assert!(matches!(
        second,
        Err(CheckpointStoreError::WriterAlreadyOpen)
    ));
    drop(store);

    let reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn writer_lock_releases_after_owner_process_exit() -> Result<(), Box<dyn std::error::Error>> {
    let _nested_process_guard = nested_process_test_guard();
    if std::env::var_os("TULYA_WRITER_LOCK_CHILD").is_some() {
        let path = std::env::var_os("TULYA_WRITER_LOCK_PATH")
            .ok_or_else(|| "writer-lock child path is missing".to_owned())?;
        let store = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
        assert_eq!(store.verify_all()?.failures, 0);
        std::process::exit(0);
    }

    let temp = tempfile::tempdir()?;
    let status = std::process::Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("checkpoint_store::tests::writer_lock_releases_after_owner_process_exit")
        .arg("--test-threads=1")
        .env("TULYA_WRITER_LOCK_CHILD", "1")
        .env("TULYA_WRITER_LOCK_PATH", temp.path())
        .status()?;
    assert!(status.success());

    let reopened = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn checkpoint_identity_rejects_empty_and_oversized_values_before_wal_append(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let oversized = "x".repeat(MAX_CHECKPOINT_IDENTIFIER_BYTES + 1);
    let invalid_transactions = [
        transaction_for_thread(0, 1, 0, 0, b"{}", "", "cp-1", None),
        transaction_for_thread(0, 1, 0, 0, b"{}", "thread", "", None),
        transaction_for_thread(0, 1, 0, 0, b"{}", &oversized, "cp-1", None),
        transaction_for_thread(0, 1, 0, 0, b"{}", "thread", &oversized, None),
        transaction_for_thread(0, 1, 0, 0, b"{}", "thread", "cp-1", Some(&oversized)),
    ];

    for transaction in invalid_transactions {
        let before = store.hot_logical_bytes();
        let result = store.append_encoded_transaction(&transaction);
        assert!(matches!(result, Err(CheckpointStoreError::Format(_))));
        assert_eq!(store.hot_logical_bytes(), before);
        assert_eq!(store.checkpoint_count(), 0);
    }
    Ok(())
}

#[test]
fn checkpoint_identity_parser_rejects_malformed_persisted_records() {
    let empty_thread = checkpoint_record(0, 0, "", "cp-1", None, b"{}");
    assert!(parse_checkpoint_record(&empty_thread, 1, 0).is_err());
    let oversized = "x".repeat(MAX_CHECKPOINT_IDENTIFIER_BYTES + 1);
    let oversized_checkpoint = checkpoint_record(0, 0, "thread", &oversized, None, b"{}");
    assert!(parse_checkpoint_record(&oversized_checkpoint, 1, 0).is_err());
}

#[test]
fn transaction_parser_rejects_zero_checkpoint_count_without_underflow() {
    let mut transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    transaction[16..20].copy_from_slice(&0u32.to_le_bytes());
    let checksum = xxh3_64(&transaction[..transaction.len() - TX_CHECKSUM_SIZE]);
    let checksum_start = transaction.len() - TX_CHECKSUM_SIZE;
    transaction[checksum_start..].copy_from_slice(&checksum.to_le_bytes());

    assert!(matches!(
        parse_transaction_unchecked(&transaction, 0),
        Err(CheckpointStoreError::Format(message))
            if message == "transaction checkpoint count must be non-zero"
    ));
}

#[test]
fn transaction_parser_rejects_descending_version_topology_before_body_walk() {
    let mut transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    transaction[8..12].copy_from_slice(&2u32.to_le_bytes());
    transaction[12..16].copy_from_slice(&1u32.to_le_bytes());
    transaction[20..24].copy_from_slice(&0u32.to_le_bytes());
    let checksum = xxh3_64(&transaction[..transaction.len() - TX_CHECKSUM_SIZE]);
    let checksum_start = transaction.len() - TX_CHECKSUM_SIZE;
    transaction[checksum_start..].copy_from_slice(&checksum.to_le_bytes());

    assert!(matches!(
        parse_transaction_unchecked(&transaction, 0),
        Err(CheckpointStoreError::Format(message))
            if message == "transaction version topology is inconsistent"
    ));
}

#[test]
fn store_id_persists_and_pre_release_manifest_is_upgraded() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let store = CheckpointStore::open(temp.path(), config)?;
    let initial_id = store.store_id();
    assert_eq!(initial_id.to_hex().len(), 32);
    assert_eq!(StoreId::from_hex(&initial_id.to_hex())?, initial_id);
    drop(store);

    let reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(reopened.store_id(), initial_id);
    drop(reopened);

    let manifest_path = temp.path().join(MANIFEST_FILE);
    let mut legacy: Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
    legacy["format"] = json!(PRE_RELEASE_MANIFEST_FORMAT);
    legacy["format_version"] = json!(PRE_RELEASE_REVISION_INITIAL);
    legacy
        .as_object_mut()
        .ok_or("manifest is not a JSON object")?
        .remove("store_id");
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&legacy)?)?;
    let legacy_store = CheckpointStore::open(temp.path(), config)?;
    let migrated_id = legacy_store.store_id();
    assert_ne!(migrated_id, initial_id);
    drop(legacy_store);
    let upgraded: Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
    assert_eq!(upgraded["format"], json!(MANIFEST_FORMAT));
    assert_eq!(upgraded["format_version"], json!(MANIFEST_FORMAT_VERSION));

    let migrated = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(migrated.store_id(), migrated_id);
    Ok(())
}

#[test]
fn copied_store_reidentification_preserves_checkpoint_and_request_identity(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let original_id = store.store_id();
    let transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    store.append_encoded_transaction_with_request_id(b"request-1", &transaction)?;
    let expected_state = store.read_checkpoint("thread", "cp-1")?;
    let copied_store_id = store.reidentify_copied_store()?;
    assert_ne!(copied_store_id, original_id);
    assert_eq!(store.read_checkpoint("thread", "cp-1")?, expected_state);
    assert_eq!(store.verify_all()?.failures, 0);
    assert!(matches!(
        store.append_encoded_transaction_with_request_id(b"request-1", &transaction)?,
        CheckpointStoreAppendOutcome::AlreadyCommitted
    ));
    drop(store);

    let reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(reopened.store_id(), copied_store_id);
    assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, expected_state);
    Ok(())
}

#[test]
fn malformed_store_id_is_rejected_without_regeneration() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let store = CheckpointStore::open(temp.path(), config)?;
    drop(store);
    let manifest_path = temp.path().join(MANIFEST_FILE);
    let mut manifest: Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
    manifest["store_id"] = json!("not-a-store-id");
    std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

    assert!(matches!(
        CheckpointStore::open(temp.path(), config),
        Err(CheckpointStoreError::Format(_))
    ));
    Ok(())
}

#[test]
fn fsck_is_independent_read_only_and_checks_hot_and_sealed_history(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 64,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
    };
    let mut store = CheckpointStore::open(temp.path(), config)?;
    store.append_messages_checkpoint("thread", "root", 0, None, &[json!("root")])?;
    store.seal_through(1)?;
    store.append_messages_checkpoint("thread", "child", 1, Some("root"), &[json!("child")])?;
    drop(store);

    type FileSnapshot = Vec<(String, Vec<u8>)>;
    let snapshot = || -> Result<FileSnapshot, Box<dyn std::error::Error>> {
        let mut files = std::fs::read_dir(temp.path())?
            .map(|entry| {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                let bytes = if entry.file_type()?.is_file() {
                    std::fs::read(entry.path())?
                } else {
                    Vec::new()
                };
                Ok((name, bytes))
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        files.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(files)
    };
    let before = snapshot()?;
    let report = fsck(temp.path())?;
    let after = snapshot()?;

    assert_eq!(before, after);
    assert_eq!(report.format_version, crate::format::VERSION);
    assert_eq!(report.checkpoint_count, 2);
    assert_eq!(report.sealed_segment_count, 1);
    assert!(report.hot_logical_bytes > 0);
    assert_eq!(report.trailing_nonzero_bytes, 0);
    assert_eq!(report.failures, 0);
    Ok(())
}

#[test]
fn fsck_rejects_corrupted_sealed_data() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
    store.append_messages_checkpoint("thread", "root", 0, None, &[json!("root")])?;
    store.seal_through(1)?;
    drop(store);

    let segment = temp.path().join("structured-g000001.t3s");
    let mut bytes = std::fs::read(&segment)?;
    let last = bytes.last_mut().ok_or("sealed segment is empty")?;
    *last ^= 0xff;
    std::fs::write(segment, bytes)?;
    assert!(matches!(
        fsck(temp.path()),
        Err(CheckpointStoreError::Format(_))
    ));
    Ok(())
}

#[test]
fn positional_reads_are_exact_without_mutating_file_cursor(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("positional-read.bin");
    std::fs::write(&path, b"abcdef")?;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(5))?;
    let mut bytes = [0u8; 3];
    read_file_exact_at(&file, &mut bytes, 1)?;
    assert_eq!(&bytes, b"bcd");
    assert_eq!(file.stream_position()?, 5);
    Ok(())
}

#[test]
fn lazy_payload_fast_path_uses_the_declared_cap() {
    assert!(lazy_payload_fast_path_allowed(0));
    assert!(lazy_payload_fast_path_allowed(LAZY_EAGER_PAYLOAD_CAP_BYTES));
    assert!(!lazy_payload_fast_path_allowed(
        LAZY_EAGER_PAYLOAD_CAP_BYTES + 1
    ));
}

#[test]
fn lazy_payload_fast_path_falls_back_above_cap() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 16 * 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 64 * 1024,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ClonePayload,
    };
    let identity_len = usize::try_from(LAZY_EAGER_PAYLOAD_CAP_BYTES)? + 1024;
    let mut identity = Vec::with_capacity(identity_len);
    identity.push(b'\"');
    for index in 0..identity_len.saturating_sub(2) {
        let value = (index as u64)
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        identity.push(b'a' + u8::try_from(value % 26)?);
    }
    identity.push(b'\"');

    let mut store = CheckpointStore::open(temp.path(), config)?;
    store.append_encoded_transaction(&transaction(0, 1, 0, 0, &identity, "cp-1", None))?;
    store.seal_through(1)?;
    drop(store);

    let mut lazy = LazyCheckpointStore::open(temp.path())?;
    assert!(!lazy.payload_fast_path_active());
    assert!(lazy.cached_block_count() <= LazyCheckpointStore::cache_capacity_blocks());
    assert_eq!(lazy.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn request_id_retry_survives_seal_and_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 4096,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
    };
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let first = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    assert!(matches!(
        store.append_encoded_transaction_with_request_id(b"", &first),
        Err(CheckpointStoreError::Format(_))
    ));
    let oversized_request = vec![b'x'; MAX_REQUEST_ID_BYTES + 1];
    assert!(matches!(
        store.append_encoded_transaction_with_request_id(&oversized_request, &first),
        Err(CheckpointStoreError::Format(_))
    ));
    let first_outcome = store.append_encoded_transaction_with_request_id(b"request-1", &first)?;
    assert!(matches!(
        first_outcome,
        CheckpointStoreAppendOutcome::Appended(_)
    ));
    let logical_tail = store.hot_logical_bytes();
    let checkpoint_count = store.checkpoint_count();
    assert!(matches!(
        store.append_encoded_transaction_with_request_id(b"request-1", &first)?,
        CheckpointStoreAppendOutcome::AlreadyCommitted
    ));
    assert_eq!(store.hot_logical_bytes(), logical_tail);
    assert_eq!(store.checkpoint_count(), checkpoint_count);

    let conflicting = transaction(0, 1, 0, 0, b"{\"x\":1}", "cp-1", None);
    assert!(matches!(
        store.append_encoded_transaction_with_request_id(b"request-1", &conflicting),
        Err(CheckpointStoreError::RequestIdConflict)
    ));
    assert_eq!(store.hot_logical_bytes(), logical_tail);
    assert_eq!(store.checkpoint_count(), checkpoint_count);

    store.seal_through(1)?;
    drop(store);
    let mut reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(reopened.verify_all()?.failures, 0);
    assert!(matches!(
        reopened.append_encoded_transaction_with_request_id(b"request-1", &first)?,
        CheckpointStoreAppendOutcome::AlreadyCommitted
    ));

    let second = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
    assert!(matches!(
        reopened.append_encoded_transaction_with_request_id(b"request-2", &second)?,
        CheckpointStoreAppendOutcome::Appended(_)
    ));
    assert_eq!(reopened.checkpoint_count(), 2);
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn malformed_request_route_suffix_is_rejected_on_reopen() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig::default();
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    let _ = store.append_encoded_transaction_with_request_id(b"request-1", &transaction)?;
    store.seal_through(1)?;
    drop(store);

    let route_path = temp.path().join("route-g000001.t3r");
    let mut route = std::fs::read(&route_path)?;
    route.truncate(route.len().saturating_sub(1));
    std::fs::write(&route_path, route)?;
    assert!(CheckpointStore::open(temp.path(), config).is_err());
    Ok(())
}

#[cfg(feature = "bench-diagnostics")]
#[test]
fn durable_append_recovery_after_publication_crash() -> Result<(), Box<dyn std::error::Error>> {
    let _nested_process_guard = nested_process_test_guard();
    if std::env::var_os("TULYA_DURABLE_APPEND_CHILD").is_some() {
        let path = std::env::var_os("TULYA_DURABLE_APPEND_PATH")
            .ok_or_else(|| "durable-append child path is missing".to_owned())?;
        let mut store = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
        let transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        let _ = store.append_encoded_transaction_with_request_id(b"crash-request", &transaction)?;
        return Err("diagnostic child did not exit at the publication boundary".into());
    }

    let temp = tempfile::tempdir()?;
    let status = std::process::Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("checkpoint_store::tests::durable_append_recovery_after_publication_crash")
        .arg("--test-threads=1")
        .env("TULYA_DURABLE_APPEND_CHILD", "1")
        .env("TULYA_DURABLE_APPEND_PATH", temp.path())
        .env(
            "TULYA_CHECKPOINT_STORE_CRASH_POINT",
            "after-hot-sync-before-memory-publication",
        )
        .status()?;
    assert_eq!(status.code(), Some(86));

    let mut reopened = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
    assert_eq!(reopened.checkpoint_count(), 1);
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    assert_eq!(reopened.verify_all()?.failures, 0);
    assert!(matches!(
        reopened.append_encoded_transaction_with_request_id(
            b"crash-request",
            &transaction(0, 1, 0, 0, b"{}", "cp-1", None),
        )?,
        CheckpointStoreAppendOutcome::AlreadyCommitted
    ));
    Ok(())
}

#[cfg(feature = "bench-diagnostics")]
#[test]
fn store_id_manifest_replacement_crash_recovery() -> Result<(), Box<dyn std::error::Error>> {
    let _nested_process_guard = nested_process_test_guard();
    if std::env::var_os("TULYA_STORE_ID_CHILD").is_some() {
        let path = std::env::var_os("TULYA_STORE_ID_PATH")
            .ok_or_else(|| "StoreId child path is missing".to_owned())?;
        let mut store = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
        assert_eq!(store.store_id(), None);
        let _ = store.reidentify_copied_store()?;
        return Err("diagnostic child did not exit at the StoreId manifest boundary".into());
    }

    let boundaries = [
        ("after-manifest-write", false),
        ("after-manifest-sync", false),
        ("after-manifest-rename", true),
        ("after-manifest-dir-sync", true),
    ];
    for (boundary, new_authority) in boundaries {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let store = CheckpointStore::open(temp.path(), config)?;
        drop(store);
        let manifest_path = temp.path().join(MANIFEST_FILE);
        let mut legacy: Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        legacy["format"] = json!(PRE_RELEASE_MANIFEST_FORMAT);
        legacy["format_version"] = json!(PRE_RELEASE_REVISION_INITIAL);
        legacy
            .as_object_mut()
            .ok_or("manifest is not a JSON object")?
            .remove("store_id");
        std::fs::write(&manifest_path, serde_json::to_vec_pretty(&legacy)?)?;

        let status = std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("checkpoint_store::tests::store_id_manifest_replacement_crash_recovery")
            .arg("--test-threads=1")
            .env("TULYA_STORE_ID_CHILD", "1")
            .env("TULYA_STORE_ID_PATH", temp.path())
            .env("TULYA_CHECKPOINT_STORE_CRASH_POINT", boundary)
            .status()?;
        assert_eq!(status.code(), Some(86), "boundary {boundary}");

        let reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(
            reopened.store_id().is_some(),
            new_authority,
            "boundary {boundary}"
        );
    }
    Ok(())
}

#[cfg(feature = "bench-diagnostics")]
#[test]
fn subtree_prune_crash_recovery() -> Result<(), Box<dyn std::error::Error>> {
    let _nested_process_guard = nested_process_test_guard();
    if std::env::var_os("TULYA_SUBTREE_PRUNE_CHILD").is_some() {
        let path = std::env::var_os("TULYA_SUBTREE_PRUNE_PATH")
            .ok_or_else(|| "subtree-prune child path is missing".to_owned())?;
        let mut store = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
        let _ = store.delete_checkpoint_subtree("thread", "C")?;
        return Err("diagnostic child did not exit at the subtree-prune boundary".into());
    }

    let boundaries = [
        ("after-prune-segment-write", false),
        ("after-segment-sync", false),
        ("after-segment-rename", false),
        ("after-segment-dir-sync", false),
        ("after-route-write", false),
        ("after-route-sync", false),
        ("after-route-rename", false),
        ("after-route-dir-sync", false),
        ("after-manifest-write", false),
        ("after-manifest-sync", false),
        ("after-manifest-rename", true),
        ("after-manifest-dir-sync", true),
        ("after-prune-manifest-authority", true),
        ("after-prune-old-file-delete", true),
    ];
    for (boundary, new_authority) in boundaries {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let tx_a = transaction_for_thread(0, 1, 0, 0, b"{}", "thread", "A", None);
        store.append_encoded_transaction(&tx_a)?;
        let tx_b = transaction_for_thread(1, 2, 2, 1, b"{\"base\":true}", "thread", "B", Some("A"));
        store.append_encoded_transaction(&tx_b)?;
        let tx_c =
            transaction_for_thread(2, 3, 15, 2, b"{\"deleted\":true}", "thread", "C", Some("B"));
        store.append_encoded_transaction_with_request_id(b"request-c", &tx_c)?;
        let tx_d =
            transaction_for_thread(3, 4, 31, 3, b"{\"sibling\":true}", "thread", "D", Some("B"));
        store.append_encoded_transaction(&tx_d)?;
        store.seal_through(4)?;
        drop(store);

        let status = std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("checkpoint_store::tests::subtree_prune_crash_recovery")
            .arg("--test-threads=1")
            .env("TULYA_SUBTREE_PRUNE_CHILD", "1")
            .env("TULYA_SUBTREE_PRUNE_PATH", temp.path())
            .env("TULYA_CHECKPOINT_STORE_CRASH_POINT", boundary)
            .status()?;
        assert_eq!(status.code(), Some(86), "boundary {boundary}");

        let mut reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.verify_all()?.failures, 0, "boundary {boundary}");
        assert_eq!(
            reopened.read_checkpoint("thread", "A")?,
            b"{\"identity\":{},\"messages\":[]}"
        );
        assert_eq!(
            reopened.read_checkpoint("thread", "B")?,
            b"{\"identity\":{\"base\":true},\"messages\":[]}"
        );
        assert_eq!(
            reopened.read_checkpoint("thread", "D")?,
            b"{\"identity\":{\"sibling\":true},\"messages\":[]}"
        );
        if new_authority {
            assert!(matches!(
                reopened.read_checkpoint("thread", "C"),
                Err(CheckpointStoreError::CheckpointDeleted)
            ));
            assert!(matches!(
                reopened.append_encoded_transaction_with_request_id(b"request-c", &tx_c),
                Err(CheckpointStoreError::CheckpointDeleted)
            ));
            assert_eq!(reopened.manifest.generation, 2);
            assert_eq!(reopened.manifest.segments.len(), 1);
            assert_eq!(reopened.manifest.routes.len(), 1);
            assert!(!temp.path().join("structured-g000001.t3s").exists());
            assert!(!temp.path().join("route-g000001.t3r").exists());
        } else {
            assert_eq!(
                reopened.read_checkpoint("thread", "C")?,
                b"{\"identity\":{\"deleted\":true},\"messages\":[]}"
            );
            assert!(matches!(
                reopened.append_encoded_transaction_with_request_id(b"request-c", &tx_c)?,
                CheckpointStoreAppendOutcome::AlreadyCommitted
            ));
            assert_eq!(reopened.manifest.generation, 1);
            assert!(temp.path().join("structured-g000001.t3s").exists());
            assert!(temp.path().join("route-g000001.t3r").exists());
        }
        drop(reopened);
    }
    Ok(())
}

#[test]
fn lazy_sealed_reader_round_trip_and_bounded_cache() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 32,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ClonePayload,
    };
    let mut store = CheckpointStore::open(temp.path(), config)?;
    let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    store.append_encoded_transaction(&tx1)?;
    store.seal_through(1)?;
    drop(store);

    let mut lazy = LazyCheckpointStore::open(temp.path())?;
    assert!(lazy.payload_fast_path_active());
    assert_eq!(lazy.checkpoint_count(), 1);
    assert_eq!(lazy.version_count(), 1);
    assert_eq!(
        lazy.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    assert_eq!(lazy.verify_all()?.failures, 0);
    assert!(lazy.cached_block_count() <= LazyCheckpointStore::cache_capacity_blocks());
    assert!(lazy.read_metrics().cache_misses > 0);

    let mut ownership_config = config;
    ownership_config.recovery_mode = CheckpointStoreRecoveryMode::ReusePayload;
    let ownership_reopened = CheckpointStore::open(temp.path(), ownership_config)?;
    assert_eq!(
        ownership_reopened.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    assert_eq!(ownership_reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn lazy_open_does_not_create_a_writable_store_manifest() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let _reader = LazyCheckpointStore::open(temp.path())?;
    assert!(!temp.path().join(MANIFEST_FILE).exists());
    Ok(())
}

#[test]
fn write_capable_lazy_tier_reads_sealed_and_hot_overlay_across_reopen(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 32,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::Lazy,
    };
    let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
    let tx3 = transaction(2, 3, 9, 2, b"{\"x\":2}", "cp-3", Some("cp-2"));

    let mut store = CheckpointStore::open(temp.path(), config)?;
    store.append_encoded_transaction(&tx1)?;
    store.seal_through(1)?;
    store.append_encoded_transaction(&tx2)?;
    assert_eq!(store.checkpoint_count(), 2);
    assert_eq!(
        store.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    assert_eq!(
        store.read_checkpoint("thread", "cp-2")?,
        b"{\"identity\":{\"x\":1},\"messages\":[]}"
    );
    assert_eq!(store.verify_all()?.failures, 0);
    drop(store);

    let mut reopened = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-2")?,
        b"{\"identity\":{\"x\":1},\"messages\":[]}"
    );
    assert_eq!(reopened.verify_all()?.failures, 0);
    reopened.seal_through(2)?;
    reopened.append_encoded_transaction(&tx3)?;
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-3")?,
        b"{\"identity\":{\"x\":2},\"messages\":[]}"
    );
    drop(reopened);

    let mut final_reopen = CheckpointStore::open(temp.path(), config)?;
    assert_eq!(final_reopen.checkpoint_count(), 3);
    assert_eq!(final_reopen.sealed_checkpoint_count(), 2);
    assert_eq!(
        final_reopen.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    assert_eq!(
        final_reopen.read_checkpoint("thread", "cp-2")?,
        b"{\"identity\":{\"x\":1},\"messages\":[]}"
    );
    assert_eq!(
        final_reopen.read_checkpoint("thread", "cp-3")?,
        b"{\"identity\":{\"x\":2},\"messages\":[]}"
    );
    assert_eq!(final_reopen.verify_all()?.failures, 0);
    final_reopen.seal_through(3)?;
    assert!(matches!(
        final_reopen.delete_checkpoint_subtree("thread", "cp-1"),
        Err(CheckpointStoreError::PruneRequiresEagerRecovery)
    ));
    Ok(())
}

#[test]
fn write_capable_lazy_tier_preserves_request_retry_after_reopen(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 32,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::Lazy,
    };
    let tx = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
    let mut store = CheckpointStore::open(temp.path(), config)?;
    assert!(matches!(
        store.append_encoded_transaction_with_request_id(b"lazy-request", &tx)?,
        CheckpointStoreAppendOutcome::Appended(_)
    ));
    store.seal_through(1)?;
    drop(store);

    let mut reopened = CheckpointStore::open(temp.path(), config)?;
    assert!(matches!(
        reopened.append_encoded_transaction_with_request_id(b"lazy-request", &tx)?,
        CheckpointStoreAppendOutcome::AlreadyCommitted
    ));
    assert_eq!(
        reopened.read_checkpoint("thread", "cp-1")?,
        b"{\"identity\":{},\"messages\":[]}"
    );
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn checkpoint_range_matches_full_read_for_eager_and_lazy_modes(
) -> Result<(), Box<dyn std::error::Error>> {
    for recovery_mode in [
        CheckpointStoreRecoveryMode::ReusePayload,
        CheckpointStoreRecoveryMode::Lazy,
    ] {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 32,
            zstd_level: 1,
            recovery_mode,
        };
        let tx1 = transaction(0, 1, 0, 0, b"{\"x\":1}", "cp-1", None);
        let tx2 = transaction(1, 2, 7, 1, b"{\"x\":2}", "cp-2", Some("cp-1"));
        let mut store = CheckpointStore::open(temp.path(), config)?;
        store.append_encoded_transaction(&tx1)?;
        store.seal_through(1)?;
        store.append_encoded_transaction(&tx2)?;
        drop(store);

        let store = CheckpointStore::open(temp.path(), config)?;
        for checkpoint in store.checkpoints() {
            let full = store.read_checkpoint(&checkpoint.thread_id, &checkpoint.checkpoint_id)?;
            let length = u64::try_from(full.len())?;
            for (offset, range_length) in [
                (0, 0),
                (0, 1),
                (1, length.saturating_sub(2)),
                (length / 2, 2),
                (length.saturating_sub(2), 2),
                (length, 0),
            ] {
                let end = offset
                    .checked_add(range_length)
                    .ok_or("test range overflow")?;
                let actual = store.read_checkpoint_range(
                    &checkpoint.thread_id,
                    &checkpoint.checkpoint_id,
                    offset,
                    range_length,
                )?;
                assert_eq!(
                    actual,
                    full[usize::try_from(offset)?..usize::try_from(end)?]
                );
            }
            assert!(matches!(
                store.read_checkpoint_range(
                    &checkpoint.thread_id,
                    &checkpoint.checkpoint_id,
                    length + 1,
                    0,
                ),
                Err(CheckpointStoreError::Format(_))
            ));
            assert!(matches!(
                store.read_checkpoint_range(
                    &checkpoint.thread_id,
                    &checkpoint.checkpoint_id,
                    length,
                    1,
                ),
                Err(CheckpointStoreError::Format(_))
            ));
            assert!(matches!(
                store.read_checkpoint_range(
                    &checkpoint.thread_id,
                    &checkpoint.checkpoint_id,
                    u64::MAX,
                    1,
                ),
                Err(CheckpointStoreError::Format(_))
            ));
        }
    }
    Ok(())
}

#[test]
fn lazy_checkpoint_range_reads_above_payload_fast_path_cap(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let config = CheckpointStoreConfig {
        wal_segment_bytes: 16 * 1024 * 1024,
        preinit_chunk_bytes: 64 * 1024,
        sealed_block_size: 64 * 1024,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::Lazy,
    };
    let blob_len = 8 * 1024 * 1024 + 1;
    let blob_prefix = b"{\"blob\":\"";
    let blob_suffix = b"\"}";
    let mut identity = Vec::with_capacity(blob_prefix.len() + blob_len + blob_suffix.len());
    identity.extend_from_slice(blob_prefix);
    identity.resize(blob_prefix.len() + blob_len, b'x');
    identity.extend_from_slice(blob_suffix);
    let tx = transaction(0, 1, 0, 0, &identity, "large", None);

    let mut store = CheckpointStore::open(temp.path(), config)?;
    store.append_encoded_transaction(&tx)?;
    store.seal_through(1)?;
    drop(store);

    let store = CheckpointStore::open(temp.path(), config)?;
    let checkpoint = store
        .checkpoints()
        .first()
        .ok_or("large checkpoint metadata missing")?;
    let total = checkpoint.logical_state_len;
    let prefix = b"{\"identity\":{\"blob\":\"";
    assert_eq!(
        store.read_checkpoint_range("thread", "large", 0, u64::try_from(prefix.len())?)?,
        prefix
    );
    let blob_start = u64::try_from(prefix.len())?;
    assert_eq!(
        store.read_checkpoint_range("thread", "large", blob_start + 1024, 128)?,
        vec![b'x'; 128]
    );
    let suffix = b",\"messages\":[]}";
    let suffix_start = total
        .checked_sub(u64::try_from(suffix.len())?)
        .ok_or("large checkpoint suffix underflow")?;
    assert_eq!(
        store.read_checkpoint_range(
            "thread",
            "large",
            suffix_start,
            u64::try_from(suffix.len())?
        )?,
        suffix
    );
    assert_eq!(store.verify_all()?.failures, 0);
    Ok(())
}
