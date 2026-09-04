#![cfg(feature = "fault-injection")]

use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::path::Path;

use tulya_checkpoint_store::{
    CheckpointStore, CheckpointStoreConfig, CheckpointStoreError, CheckpointStoreFailureKind,
    CheckpointStoreRecoveryMode, DurabilityOperation,
};

const WAL_IO_FAULT_ENV: &str = "TULYA_CHECKPOINT_STORE_WAL_IO_FAULT";

struct FaultEnv {
    previous: Option<OsString>,
}

impl FaultEnv {
    fn clear() -> Self {
        let previous = std::env::var_os(WAL_IO_FAULT_ENV);
        std::env::remove_var(WAL_IO_FAULT_ENV);
        Self { previous }
    }

    fn set(value: &str) -> Self {
        let previous = std::env::var_os(WAL_IO_FAULT_ENV);
        std::env::set_var(WAL_IO_FAULT_ENV, value);
        Self { previous }
    }
}

impl Drop for FaultEnv {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var(WAL_IO_FAULT_ENV, value),
            None => std::env::remove_var(WAL_IO_FAULT_ENV),
        }
    }
}

fn expect_error<T>(
    result: Result<T, CheckpointStoreError>,
) -> Result<CheckpointStoreError, Box<dyn Error>> {
    match result {
        Ok(_) => Err("fault-injected operation unexpectedly succeeded".into()),
        Err(error) => Ok(error),
    }
}

fn assert_kind(error: &CheckpointStoreError, expected: CheckpointStoreFailureKind) {
    assert_eq!(error.failure_kind(), expected);
}

fn canonical(identity: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"{\"identity\":");
    bytes.extend_from_slice(identity);
    bytes.extend_from_slice(b",\"messages\":[]}");
    bytes
}

fn hot_prefix(path: &Path, length: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let bytes = fs::read(path.join("hot.wal"))?;
    let end = length.min(bytes.len());
    let prefix = bytes
        .get(..end)
        .ok_or("hot WAL prefix range is outside file bytes")?;
    Ok(prefix.to_vec())
}

fn small_reserve_config() -> CheckpointStoreConfig {
    CheckpointStoreConfig {
        wal_segment_bytes: 1024,
        preinit_chunk_bytes: 256,
        sealed_block_size: 4096,
        zstd_level: 1,
        recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
    }
}

#[test]
fn live_file_backed_hot_wal_faults_recover_to_exact_state() -> Result<(), Box<dyn Error>> {
    let _clean_fault_environment = FaultEnv::clear();

    // Real short writes must be completed by the foreground commit loop.
    {
        let temp = tempfile::tempdir()?;
        let mut store = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        {
            let _fault = FaultEnv::set("short-write=7");
            store.append_checkpoint("thread", "short", 1, None, b"{}")?;
        }
        drop(store);

        let reopened = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        assert_eq!(
            reopened.read_checkpoint("thread", "short")?,
            canonical(b"{}")
        );
        assert_eq!(reopened.verify_all()?.failures, 0);
    }

    // ENOSPC before the first record byte is a definite I/O failure and the
    // same writer may retry after the fault disappears.
    {
        let temp = tempfile::tempdir()?;
        let mut store = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        let before = hot_prefix(temp.path(), 256)?;
        let error = {
            let _fault = FaultEnv::set("write-enospc-after=0");
            expect_error(store.append_checkpoint("thread", "retry", 1, None, b"{}"))?
        };
        assert_kind(&error, CheckpointStoreFailureKind::Capacity);
        assert_eq!(hot_prefix(temp.path(), 256)?, before);

        store.append_checkpoint("thread", "retry", 1, None, b"{}")?;
        assert_eq!(store.read_checkpoint("thread", "retry")?, canonical(b"{}"));
    }

    // ENOSPC after a real partial write leaves a torn physical suffix. The
    // current handle is poisoned; reopen ignores the torn suffix and permits
    // the logical operation to be retried from the recovered tail.
    {
        let temp = tempfile::tempdir()?;
        let mut store = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        let error = {
            let _fault = FaultEnv::set("write-enospc-after=16");
            expect_error(store.append_checkpoint("thread", "partial", 1, None, b"{}"))?
        };
        assert_kind(&error, CheckpointStoreFailureKind::RecoveryRequired);
        assert!(hot_prefix(temp.path(), 16)?.iter().any(|byte| *byte != 0));

        let blocked =
            expect_error(store.append_checkpoint("thread", "blocked", 2, None, b"{}"))?;
        assert_kind(&blocked, CheckpointStoreFailureKind::RecoveryRequired);
        let local_missing = expect_error(store.read_checkpoint("thread", "partial"))?;
        assert_kind(&local_missing, CheckpointStoreFailureKind::Stale);
        drop(store);

        let mut reopened = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        let recovered_missing = expect_error(reopened.read_checkpoint("thread", "partial"))?;
        assert_kind(&recovered_missing, CheckpointStoreFailureKind::Stale);
        reopened.append_checkpoint("thread", "partial", 1, None, b"{}")?;
        assert_eq!(
            reopened.read_checkpoint("thread", "partial")?,
            canonical(b"{}")
        );
        assert_eq!(reopened.verify_all()?.failures, 0);
    }

    // A flush error is indeterminate because the complete record already
    // reached the live file. Reopen must resolve to a valid old-or-new state,
    // never a partially published in-memory state.
    {
        let temp = tempfile::tempdir()?;
        let mut store = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        let error = {
            let _fault = FaultEnv::set("flush-eio-after");
            expect_error(store.append_checkpoint("thread", "flush", 1, None, b"{}"))?
        };
        assert_kind(
            &error,
            CheckpointStoreFailureKind::DurabilityIndeterminate,
        );
        assert_eq!(
            error
                .durability_indeterminate()
                .ok_or("missing flush durability context")?
                .operation(),
            DurabilityOperation::WalFlush
        );
        let blocked =
            expect_error(store.append_checkpoint("thread", "blocked", 2, None, b"{}"))?;
        assert_kind(&blocked, CheckpointStoreFailureKind::RecoveryRequired);
        drop(store);

        let mut reopened = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        match reopened.read_checkpoint("thread", "flush") {
            Ok(bytes) => assert_eq!(bytes, canonical(b"{}")),
            Err(error) if error.failure_kind() == CheckpointStoreFailureKind::Stale => {
                reopened.append_checkpoint("thread", "flush", 1, None, b"{}")?;
            }
            Err(error) => return Err(error.into()),
        }
        assert_eq!(
            reopened.read_checkpoint("thread", "flush")?,
            canonical(b"{}")
        );
        assert_eq!(reopened.verify_all()?.failures, 0);
    }

    // This injected error is returned only after the real sync_data succeeds.
    // The caller receives an indeterminate result, the handle is poisoned, and
    // reopen proves the transaction was in fact durable.
    {
        let temp = tempfile::tempdir()?;
        let mut store = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        let error = {
            let _fault = FaultEnv::set("sync-eio-after");
            expect_error(store.append_checkpoint("thread", "synced", 1, None, b"{}"))?
        };
        assert_kind(
            &error,
            CheckpointStoreFailureKind::DurabilityIndeterminate,
        );
        assert_eq!(
            error
                .durability_indeterminate()
                .ok_or("missing sync durability context")?
                .operation(),
            DurabilityOperation::WalSyncData
        );
        let local_missing = expect_error(store.read_checkpoint("thread", "synced"))?;
        assert_kind(&local_missing, CheckpointStoreFailureKind::Stale);
        drop(store);

        let reopened = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        assert_eq!(
            reopened.read_checkpoint("thread", "synced")?,
            canonical(b"{}")
        );
        assert_eq!(reopened.verify_all()?.failures, 0);
    }

    // Reserve growth mutates file length before zero-fill/sync. Failure after
    // set_len therefore poisons the writer even though no transaction byte was
    // written. Reopen derives the old logical tail and safely reuses the larger
    // physical reserve.
    {
        let temp = tempfile::tempdir()?;
        let config = small_reserve_config();
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let before_capacity = store.hot_capacity_bytes()?;
        let identity = format!("\"{}\"", "x".repeat(4096)).into_bytes();
        let error = {
            let _fault = FaultEnv::set("reserve-enospc-after-set-len");
            expect_error(store.append_checkpoint("thread", "reserve", 1, None, &identity))?
        };
        assert_kind(&error, CheckpointStoreFailureKind::RecoveryRequired);
        assert!(store.hot_capacity_bytes()? > before_capacity);
        let blocked =
            expect_error(store.append_checkpoint("thread", "blocked", 2, None, b"{}"))?;
        assert_kind(&blocked, CheckpointStoreFailureKind::RecoveryRequired);
        drop(store);

        let mut reopened = CheckpointStore::open(temp.path(), config)?;
        let missing = expect_error(reopened.read_checkpoint("thread", "reserve"))?;
        assert_kind(&missing, CheckpointStoreFailureKind::Stale);
        reopened.append_checkpoint("thread", "reserve", 1, None, &identity)?;
        assert_eq!(
            reopened.read_checkpoint("thread", "reserve")?,
            canonical(&identity)
        );
        assert_eq!(reopened.verify_all()?.failures, 0);
    }

    Ok(())
}
