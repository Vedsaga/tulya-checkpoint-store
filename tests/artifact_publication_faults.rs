#![cfg(feature = "fault-injection")]

use std::error::Error;
use std::ffi::OsString;
use std::fs;

use tulya_checkpoint_store::{
    CheckpointStore, CheckpointStoreConfig, CheckpointStoreError, CheckpointStoreFailureKind,
};

const PUBLICATION_IO_FAULT_ENV: &str = "TULYA_CHECKPOINT_STORE_PUBLICATION_IO_FAULT";
const WAL_IO_FAULT_ENV: &str = "TULYA_CHECKPOINT_STORE_WAL_IO_FAULT";

struct EnvGuard {
    key: &'static str,
    previous: Option<OsString>,
}

impl EnvGuard {
    fn clear(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        std::env::remove_var(key);
        Self { key, previous }
    }

    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

struct TwoCheckpointStore {
    temp: tempfile::TempDir,
    store: CheckpointStore,
    first: Vec<u8>,
    second: Vec<u8>,
}

fn open_two_checkpoint_store() -> Result<TwoCheckpointStore, Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let mut store = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
    store.append_checkpoint("thread", "cp-1", 1, None, b"{\"value\":1}")?;
    store.append_checkpoint("thread", "cp-2", 2, Some("cp-1"), b"{\"value\":2}")?;
    let first = store.read_checkpoint("thread", "cp-1")?;
    let second = store.read_checkpoint("thread", "cp-2")?;
    Ok(TwoCheckpointStore {
        temp,
        store,
        first,
        second,
    })
}

fn expect_error<T>(
    result: Result<T, CheckpointStoreError>,
) -> Result<CheckpointStoreError, Box<dyn Error>> {
    match result {
        Ok(_) => Err("fault-injected operation unexpectedly succeeded".into()),
        Err(error) => Ok(error),
    }
}

fn assert_no_generation_one_orphans(temp: &tempfile::TempDir) -> Result<(), Box<dyn Error>> {
    for entry in fs::read_dir(temp.path())? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or("generation artifact filename is not UTF-8")?;
        assert!(
            !name.contains("g000001"),
            "reopen left an unreferenced generation-one artifact: {name}"
        );
    }
    Ok(())
}

fn run_case(fault: &str, expected_prefix: &str) -> Result<(), Box<dyn Error>> {
    let TwoCheckpointStore {
        temp,
        mut store,
        first,
        second,
    } = open_two_checkpoint_store()?;

    let error = {
        let _fault = EnvGuard::set(PUBLICATION_IO_FAULT_ENV, fault);
        expect_error(store.seal_through(1))?
    };

    assert_eq!(
        error.failure_kind(),
        CheckpointStoreFailureKind::RecoveryRequired
    );
    let context = error
        .recovery_required()
        .ok_or("missing pre-authority recovery context")?;
    assert!(!context.authority_committed());
    let name = context
        .path()
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or("recovery artifact path has no UTF-8 filename")?;
    assert!(name.starts_with(expected_prefix), "unexpected artifact path: {name}");

    assert_eq!(store.sealed_checkpoint_count(), 0);
    assert_eq!(store.read_checkpoint("thread", "cp-1")?, first);
    assert_eq!(store.read_checkpoint("thread", "cp-2")?, second);

    let blocked = expect_error(store.append_checkpoint(
        "thread",
        "blocked",
        3,
        Some("cp-2"),
        b"{\"blocked\":true}",
    ))?;
    assert_eq!(
        blocked.failure_kind(),
        CheckpointStoreFailureKind::RecoveryRequired
    );
    assert!(!blocked
        .recovery_required()
        .ok_or("missing poisoned-writer recovery context")?
        .authority_committed());
    drop(store);

    let mut reopened = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
    assert_eq!(reopened.sealed_checkpoint_count(), 0);
    assert_eq!(reopened.checkpoint_count(), 2);
    assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, first);
    assert_eq!(reopened.read_checkpoint("thread", "cp-2")?, second);
    assert_eq!(reopened.verify_all()?.failures, 0);
    assert_no_generation_one_orphans(&temp)?;

    reopened.seal_through(1)?;
    assert_eq!(reopened.sealed_checkpoint_count(), 1);
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn live_segment_and_route_faults_reopen_cleanly_before_retry() -> Result<(), Box<dyn Error>> {
    let _clean_publication = EnvGuard::clear(PUBLICATION_IO_FAULT_ENV);
    let _clean_wal = EnvGuard::clear(WAL_IO_FAULT_ENV);

    for (fault, expected_prefix) in [
        ("segment-sync-eio-after", "structured-g"),
        ("segment-rename-eio-before", "structured-g"),
        ("segment-rename-eio-after", "structured-g"),
        ("segment-dir-sync-eio-after", "structured-g"),
        ("route-sync-eio-after", "route-g"),
        ("route-rename-eio-before", "route-g"),
        ("route-rename-eio-after", "route-g"),
        ("route-dir-sync-eio-after", "route-g"),
    ] {
        run_case(fault, expected_prefix)?;
    }

    Ok(())
}
