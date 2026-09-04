#![cfg(feature = "fault-injection")]

use std::error::Error;
use std::ffi::OsString;

use tulya_checkpoint_store::{
    CheckpointStore, CheckpointStoreConfig, CheckpointStoreError, CheckpointStoreFailureKind,
    DurabilityOperation,
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

fn expect_error<T>(
    result: Result<T, CheckpointStoreError>,
) -> Result<CheckpointStoreError, Box<dyn Error>> {
    match result {
        Ok(_) => Err("fault-injected operation unexpectedly succeeded".into()),
        Err(error) => Ok(error),
    }
}

fn assert_blocked(store: &mut CheckpointStore) -> Result<(), Box<dyn Error>> {
    let error = expect_error(store.append_checkpoint(
        "thread",
        "blocked",
        3,
        Some("cp-2"),
        b"{\"blocked\":true}",
    ))?;
    assert_eq!(
        error.failure_kind(),
        CheckpointStoreFailureKind::RecoveryRequired
    );
    let context = error
        .recovery_required()
        .ok_or("missing poisoned-writer recovery context")?;
    assert!(!context.authority_committed());
    Ok(())
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

fn assert_reopened(
    temp: &tempfile::TempDir,
    sealed: u64,
    first: &[u8],
    second: &[u8],
) -> Result<CheckpointStore, Box<dyn Error>> {
    let store = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
    assert_eq!(store.sealed_checkpoint_count(), sealed);
    assert_eq!(store.checkpoint_count(), 2);
    assert_eq!(store.read_checkpoint("thread", "cp-1")?, first);
    assert_eq!(store.read_checkpoint("thread", "cp-2")?, second);
    assert_eq!(store.verify_all()?.failures, 0);
    Ok(store)
}

fn assert_indeterminate(
    error: &CheckpointStoreError,
    operation: DurabilityOperation,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        error.failure_kind(),
        CheckpointStoreFailureKind::DurabilityIndeterminate
    );
    let context = error
        .durability_indeterminate()
        .ok_or("missing durability-indeterminate context")?;
    assert_eq!(context.operation(), operation);
    Ok(())
}

fn run_post_commit_recycle_case(fault: &str) -> Result<(), Box<dyn Error>> {
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
        .ok_or("missing post-authority recovery context")?;
    assert!(context.authority_committed());

    assert_eq!(store.sealed_checkpoint_count(), 1);
    assert_eq!(store.read_checkpoint("thread", "cp-1")?, first);
    assert_eq!(store.read_checkpoint("thread", "cp-2")?, second);
    assert_blocked(&mut store)?;
    drop(store);

    let mut reopened = assert_reopened(&temp, 1, &first, &second)?;
    reopened.seal_through(2)?;
    assert_eq!(reopened.sealed_checkpoint_count(), 2);
    assert_eq!(reopened.verify_all()?.failures, 0);
    Ok(())
}

#[test]
fn live_manifest_and_recycle_faults_recover_exact_authority() -> Result<(), Box<dyn Error>> {
    let _clean_publication = EnvGuard::clear(PUBLICATION_IO_FAULT_ENV);
    let _clean_wal = EnvGuard::clear(WAL_IO_FAULT_ENV);

    // The manifest temporary file was really synced, but authority was never
    // renamed into place. Logical authority is definitely old; however the
    // segment/route finals already exist, so the writer must reopen before
    // reusing the generation name.
    {
        let TwoCheckpointStore {
            temp,
            mut store,
            first,
            second,
        } = open_two_checkpoint_store()?;
        let error = {
            let _fault = EnvGuard::set(PUBLICATION_IO_FAULT_ENV, "manifest-sync-eio-after");
            expect_error(store.seal_through(1))?
        };
        assert_eq!(
            error.failure_kind(),
            CheckpointStoreFailureKind::RecoveryRequired
        );
        assert!(error.durability_indeterminate().is_none());
        assert!(!error
            .recovery_required()
            .ok_or("missing pre-authority manifest recovery context")?
            .authority_committed());
        assert_eq!(store.sealed_checkpoint_count(), 0);
        assert_blocked(&mut store)?;
        drop(store);

        let mut reopened = assert_reopened(&temp, 0, &first, &second)?;
        reopened.append_checkpoint("thread", "cp-3", 3, Some("cp-2"), b"{\"value\":3}")?;
        assert_eq!(reopened.checkpoint_count(), 3);
        assert_eq!(reopened.verify_all()?.failures, 0);
    }

    // Failure reported before the manifest rename: the caller receives an
    // indeterminate result and must reopen. Reopen resolves to old authority.
    {
        let TwoCheckpointStore {
            temp,
            mut store,
            first,
            second,
        } = open_two_checkpoint_store()?;
        let error = {
            let _fault = EnvGuard::set(PUBLICATION_IO_FAULT_ENV, "manifest-rename-eio-before");
            expect_error(store.seal_through(1))?
        };
        assert_indeterminate(&error, DurabilityOperation::Rename)?;
        assert_eq!(store.sealed_checkpoint_count(), 0);
        assert_blocked(&mut store)?;
        drop(store);

        let mut reopened = assert_reopened(&temp, 0, &first, &second)?;
        reopened.seal_through(1)?;
        assert_eq!(reopened.sealed_checkpoint_count(), 1);
    }

    // Failure reported after the real rename but before directory durability:
    // process-local state stays unresolved, while reopen sees new authority.
    {
        let TwoCheckpointStore {
            temp,
            mut store,
            first,
            second,
        } = open_two_checkpoint_store()?;
        let error = {
            let _fault = EnvGuard::set(PUBLICATION_IO_FAULT_ENV, "manifest-rename-eio-after");
            expect_error(store.seal_through(1))?
        };
        assert_indeterminate(&error, DurabilityOperation::Rename)?;
        assert_eq!(store.sealed_checkpoint_count(), 0);
        assert_blocked(&mut store)?;
        drop(store);

        let mut reopened = assert_reopened(&temp, 1, &first, &second)?;
        reopened.seal_through(2)?;
        assert_eq!(reopened.sealed_checkpoint_count(), 2);
    }

    // Directory sync succeeds on the real filesystem and the injected error is
    // then surfaced as indeterminate. Reopen must observe new authority.
    {
        let TwoCheckpointStore {
            temp,
            mut store,
            first,
            second,
        } = open_two_checkpoint_store()?;
        let error = {
            let _fault = EnvGuard::set(PUBLICATION_IO_FAULT_ENV, "manifest-dir-sync-eio-after");
            expect_error(store.seal_through(1))?
        };
        assert_indeterminate(&error, DurabilityOperation::DirectorySync)?;
        assert_eq!(store.sealed_checkpoint_count(), 0);
        assert_blocked(&mut store)?;
        drop(store);

        let reopened = assert_reopened(&temp, 1, &first, &second)?;
        assert_eq!(reopened.verify_all()?.failures, 0);
    }

    // Every remaining case occurs after manifest authority was successfully
    // committed. The original error must say so, the live handle is poisoned,
    // and reopen must recover the same new authority regardless of how far WAL
    // recycle progressed.
    for fault in [
        "wal-recycle-sync-eio-after",
        "wal-recycle-rename-eio-before",
        "wal-recycle-rename-eio-after",
        "wal-recycle-dir-sync-eio-after",
    ] {
        run_post_commit_recycle_case(fault)?;
    }

    Ok(())
}
