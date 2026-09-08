//! Generation authority: sealed snapshots, manifest publication, WAL recycle.
//!
//! Exactly one manifest generation is current. A sealed generation is an
//! immutable snapshot file plus the hot log that continues it; recovery loads
//! the snapshot and replays the current hot file fully. Publication order
//! keeps the old authority complete until the new one is durable:
//!
//! ```text
//! build snapshot(N+1) covering hot-N[..E]
//! write + sync snapshot tmp, verify by strict re-decode, rename, dir sync
//! create empty hot-(N+1) via tmp + rename + dir sync
//! open + lock hot-(N+1) while the manifest still reads N:
//!     failure here is a definite reject, old authority fully operational
//! write + sync manifest tmp {N+1, sealed digest}, rename
//! dirsync: rename ok + dirsync failure is indeterminate (old-or-new),
//!     so the writer poisons and must reopen
//! adopt the already-open hot-(N+1) infallibly; switch generation/stats
//! only now delete superseded hot-N (best effort, reported)
//! ```
//!
//! Every crash cut therefore recovers exactly old authority (manifest N with
//! hot-N suffix) or new authority (manifest N+1 with hot-(N+1) suffix). A
//! torn manifest can only come from hand forgery — publication renames
//! atomically — and fails closed, as does a missing sealed file or any digest
//! or length disagreement. Old snapshots are retained; only the superseded
//! hot log is recycled, and only after the new manifest is durable.

use super::{
    durable_log::{decode_history_log, replay_history_suffix, DurableError, DurableHistoryLog},
    manifest::{
        decode_history_manifest, encode_history_manifest, history_snapshot_filename,
        history_wal_filename, HistoryManifest, ManifestSealed, HISTORY_LOCK_FILE,
        HISTORY_MANIFEST_FILE,
    },
    snapshot::{decode_history_snapshot, encode_history_snapshot},
    CommitOutcome, ExpireOutcome, HistoryError, HistoryId, PersistentHistoryStore, VersionId,
};
use fs4::FileExt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenedHistoryStats {
    pub snapshot_versions: usize,
    pub suffix_bytes: u64,
}

#[derive(Debug)]
pub struct OpenedHistory {
    pub store: PersistentHistoryStore,
    pub generation: u64,
    pub stats: OpenedHistoryStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealSummary {
    pub generation: u64,
    pub represented_wal_end: u64,
    pub snapshot_len: u64,
    pub recycled_hot: bool,
}

/// Single-writer authority over one history directory: a store-wide writer
/// lease, the owned writable hot-log handle, and generation tracking that
/// survives seal transitions.
///
/// Opening a writable authority acquires one store-wide non-blocking writer
/// lease (`history.lock`) held for the authority lifetime, loads the read
/// authority, and opens the current-generation hot log for writing. Every
/// mutation (create, commit, retire) goes through the owned handle — never
/// through ad-hoc unlocked opens — so at most one writable authority exists
/// per directory at any moment, including across seal transitions: the lease
/// is retained while the generation advances and the hot handle switches.
/// A second writable open fails with [`DurableError::AlreadyOpen`]; dropping
/// the authority releases the lease and admits the next writer. Read-only
/// loading via [`open_history_authority`] stays lock-free.
pub struct WritableHistoryAuthority {
    store: PersistentHistoryStore,
    generation: u64,
    stats: OpenedHistoryStats,
    dir: PathBuf,
    // The store-wide writer lease: never read, held purely for its flock
    // lifetime. Dropping the authority releases the lease and admits the
    // next writer.
    _lease: File,
    hot: DurableHistoryLog,
}

impl WritableHistoryAuthority {
    /// Opens the writable authority for a directory: store-wide lease, read
    /// authority, and current-generation writable hot log.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError::AlreadyOpen`] when another writable authority
    /// holds the store-wide lease (or the current hot log), and definite
    /// rejection for any corrupt or unreadable authority. Nothing is mutated
    /// on any error path.
    pub fn open(dir: &Path) -> Result<Self, DurableError> {
        let lease = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(HISTORY_LOCK_FILE))
            .map_err(|_| {
                DurableError::Rejected(HistoryError::Invalid("history writer lease open failed"))
            })?;
        lease.try_lock_exclusive().map_err(|error| {
            if DurableHistoryLog::is_lock_contention(&error) {
                DurableError::AlreadyOpen
            } else {
                DurableError::Rejected(HistoryError::Invalid(
                    "history writer lease acquisition failed",
                ))
            }
        })?;
        let opened = open_history_authority(dir)?;
        let hot = DurableHistoryLog::open_write(&dir.join(history_wal_filename(opened.generation)))
            .map_err(map_hot_open_error)?;
        Ok(Self {
            store: opened.store,
            generation: opened.generation,
            stats: opened.stats,
            dir: dir.to_path_buf(),
            _lease: lease,
            hot,
        })
    }

    /// Borrows the live store. Only shared access escapes: every mutation
    /// flows through the authority-owned writable log.
    pub fn store(&self) -> &PersistentHistoryStore {
        &self.store
    }

    /// Reports the generation the writable hot log belongs to.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Reports the bounded-reopen evidence captured at open (or the last
    /// seal): snapshot versions plus replayed hot suffix bytes.
    pub const fn stats(&self) -> OpenedHistoryStats {
        self.stats
    }

    /// Durably creates a history through the owned writable log, bound to
    /// opaque adapter bytes when supplied. First use stays idempotent by
    /// binding exactly like the underlying core primitive.
    pub fn create_history(&mut self, binding: Option<&[u8]>) -> Result<HistoryId, DurableError> {
        match binding {
            None => self.store.create_history_durable(&mut self.hot),
            Some(bytes) => self
                .store
                .create_history_durable_with_binding(&mut self.hot, bytes),
        }
    }

    /// Durably splices through the owned writable log: the single canonical
    /// content mutation. See [`PersistentHistoryStore::splice`].
    pub fn splice(
        &mut self,
        history: HistoryId,
        parent: Option<VersionId>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, DurableError> {
        self.store.splice_durable(
            &mut self.hot,
            history,
            parent,
            offset,
            delete_len,
            insert,
            request_id,
            binding,
        )
    }

    /// Durably appends through the owned writable log: canonicalizes to a
    /// splice at the parent end with one shared digest and encoding. See
    /// [`PersistentHistoryStore::append`].
    pub fn append(
        &mut self,
        history: HistoryId,
        parent: Option<VersionId>,
        bytes: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, DurableError> {
        self.store
            .append_durable(&mut self.hot, history, parent, bytes, request_id, binding)
    }

    /// Durably forks through the owned writable log: publishes a new
    /// version pointing at the exact existing parent root with zero content
    /// growth. See [`PersistentHistoryStore::fork`].
    pub fn fork(
        &mut self,
        history: HistoryId,
        parent: VersionId,
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, DurableError> {
        self.store
            .fork_durable(&mut self.hot, history, parent, request_id, binding)
    }

    /// Durably expires one known version through the owned writable log:
    /// one-way lifecycle metadata, no cascade, no content change. See
    /// [`PersistentHistoryStore::expire`].
    pub fn expire(&mut self, version: VersionId) -> Result<ExpireOutcome, DurableError> {
        self.store.expire_durable(&mut self.hot, version)
    }

    /// Durably retires a request identity through the owned writable log.
    pub fn retire(&mut self, request_id: &[u8]) -> Result<(), DurableError> {
        self.store.retire_durable(&mut self.hot, request_id)
    }

    /// Seals the current generation while retaining the store-wide lease:
    /// snapshot, next hot log, and manifest publish in publication order,
    /// then the owned handle switches to the next generation with no writer
    /// race — no other authority can exist while this lease is held. A
    /// poisoned store refuses to seal: publishing a snapshot of
    /// indeterminate state would launder it into authority.
    pub fn seal(&mut self) -> Result<SealSummary, DurableError> {
        let dir = self.dir.clone();
        self.seal_with_sync(|| sync_dir(&dir))
    }

    /// Seal orchestration with an injectable manifest directory sync: the
    /// deterministic test seam for the rename-ok/dirsync-fail cut.
    /// Crate-private; external writers use [`seal`](Self::seal).
    pub(crate) fn seal_with_sync(
        &mut self,
        sync_dir_once: impl FnOnce() -> std::io::Result<()>,
    ) -> Result<SealSummary, DurableError> {
        if self.store.is_poisoned() {
            return Err(DurableError::RecoveryRequired);
        }
        let staged = stage_sealed_generation(&self.store, &self.dir)?;
        // Open and lock the next hot log BEFORE the manifest commits: any
        // failure here still sees manifest N, so it stays a definite
        // rejection with the old handle and generation fully operational.
        let next_hot =
            DurableHistoryLog::open_write(&self.dir.join(history_wal_filename(staged.generation)))
                .map_err(map_hot_open_error)?;
        commit_staged_manifest(&mut self.store, &self.dir, &staged, sync_dir_once)?;
        // Infallible adoption: the next handle is already open and locked, so
        // no fallible step remains between durable authority and memory.
        self.hot = next_hot;
        self.generation = staged.generation;
        self.stats = OpenedHistoryStats {
            snapshot_versions: self.store.version_count(),
            suffix_bytes: 0,
        };
        let recycled_hot = recycle_superseded_hot(&self.dir, staged.previous);
        Ok(SealSummary {
            generation: staged.generation,
            represented_wal_end: staged.represented_wal_end,
            snapshot_len: staged.snapshot_len,
            recycled_hot,
        })
    }
}

fn map_hot_open_error(error: std::io::Error) -> DurableError {
    if DurableHistoryLog::is_lock_contention(&error) {
        DurableError::AlreadyOpen
    } else {
        DurableError::Rejected(HistoryError::Invalid("history hot log open failed"))
    }
}

/// Opens the authoritative history for a directory: manifest generation,
/// verified sealed snapshot, and current hot suffix replayed on top.
///
/// A missing manifest means no authority was ever published: generation zero
/// replays the genesis hot log when present, exactly like a fresh store
/// otherwise. Every other absence or disagreement fails closed.
pub fn open_history_authority(dir: &Path) -> Result<OpenedHistory, DurableError> {
    let manifest_path = dir.join(HISTORY_MANIFEST_FILE);
    let manifest_bytes = match fs::read(&manifest_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => {
            return Err(DurableError::Rejected(HistoryError::Invalid(
                "history manifest read failed",
            )));
        }
        Ok(bytes) => Some(bytes),
    };
    let manifest = match manifest_bytes {
        None => None,
        Some(bytes) => Some(decode_history_manifest(&bytes).map_err(DurableError::Rejected)?),
    };
    let generation = manifest.map_or(0, |manifest| manifest.generation());
    let mut store = match manifest.and_then(|manifest| manifest.sealed()) {
        None => {
            if generation != 0 {
                return Err(DurableError::Rejected(HistoryError::Invalid(
                    "history manifest without sealed base must be generation zero",
                )));
            }
            PersistentHistoryStore::new()
        }
        Some(sealed) => {
            let snap_path = dir.join(history_snapshot_filename(generation));
            let bytes = fs::read(&snap_path).map_err(|_| {
                DurableError::Rejected(HistoryError::Invalid("history sealed snapshot is missing"))
            })?;
            if bytes.len() as u64 != sealed.byte_len() {
                return Err(DurableError::Rejected(HistoryError::Invalid(
                    "history sealed snapshot length disagrees with manifest",
                )));
            }
            if super::manifest::sealed_artifact_digest(&bytes) != sealed.sha256() {
                return Err(DurableError::Rejected(HistoryError::Invalid(
                    "history sealed snapshot digest mismatch",
                )));
            }
            let snapshot = decode_history_snapshot(&bytes).map_err(DurableError::Rejected)?;
            if snapshot.generation != generation {
                return Err(DurableError::Rejected(HistoryError::Invalid(
                    "history snapshot generation disagrees with manifest",
                )));
            }
            PersistentHistoryStore::import_snapshot(snapshot).map_err(DurableError::Rejected)?
        }
    };
    let snapshot_versions = store.version_count();
    let hot_path = dir.join(history_wal_filename(generation));
    // A published generation always owns its hot file: the seal creates and
    // syncs the empty next-generation log before the manifest rename, so a
    // missing hot file after publication means external deletion, and
    // treating it as empty would silently drop post-seal versions.
    let suffix = match fs::read(&hot_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if manifest.is_some() {
                return Err(DurableError::Rejected(HistoryError::Invalid(
                    "history hot log for the published generation is missing",
                )));
            }
            Vec::new()
        }
        Err(_) => {
            return Err(DurableError::Rejected(HistoryError::Invalid(
                "history hot log read failed",
            )));
        }
        Ok(bytes) => bytes,
    };
    let suffix_bytes = suffix.len() as u64;
    replay_history_suffix(&mut store, &suffix).map_err(DurableError::Rejected)?;
    Ok(OpenedHistory {
        store,
        generation,
        stats: OpenedHistoryStats {
            snapshot_versions,
            suffix_bytes,
        },
    })
}

/// Seals the current generation: persists a snapshot covering the live state
/// plus the consumed hot prefix, publishes the next manifest generation, and
/// recycles the superseded hot log.
///
/// Test-only full-file driver for crash-cut scenarios; production seals
/// through [`WritableHistoryAuthority::seal`], which additionally opens the
/// next hot log before the manifest commits. Takes the store by exclusive
/// reference so a manifest dirsync failure can poison it: a rename that
/// succeeded with a lost directory sync leaves old-or-new authority behind,
/// which is indeterminate, never a definite rejection.
///
/// All failures before the manifest rename leave the old authority complete
/// and report definite rejection.
#[cfg(test)]
pub(crate) fn seal_history_generation(
    store: &mut PersistentHistoryStore,
    dir: &Path,
) -> Result<SealSummary, DurableError> {
    let staged = stage_sealed_generation(store, dir)?;
    commit_staged_manifest(store, dir, &staged, || sync_dir(dir))?;
    let recycled_hot = recycle_superseded_hot(dir, staged.previous);
    Ok(SealSummary {
        generation: staged.generation,
        represented_wal_end: staged.represented_wal_end,
        snapshot_len: staged.snapshot_len,
        recycled_hot,
    })
}

/// Staged sealed generation: snapshot published and verified, empty
/// next-generation hot log published, everything short of manifest
/// authority. Every failure here predates any new authority and reports
/// definite rejection.
struct StagedSeal {
    previous: u64,
    generation: u64,
    represented_wal_end: u64,
    sealed: ManifestSealed,
    snapshot_len: u64,
}

fn stage_sealed_generation(
    store: &PersistentHistoryStore,
    dir: &Path,
) -> Result<StagedSeal, DurableError> {
    let current = current_generation(dir)?;
    let generation =
        current
            .checked_add(1)
            .ok_or(DurableError::Rejected(HistoryError::Overflow(
                "history generation exceeds u64",
            )))?;
    let hot_current = dir.join(history_wal_filename(current));
    let hot_bytes = match fs::read(&hot_current) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if current == 0 {
                Vec::new()
            } else {
                return Err(DurableError::Rejected(HistoryError::Invalid(
                    "history hot log for the authoritative generation is missing",
                )));
            }
        }
        Err(_) => {
            return Err(DurableError::Rejected(HistoryError::Invalid(
                "history hot log read failed",
            )));
        }
        Ok(bytes) => bytes,
    };
    let (_, represented_wal_end) =
        decode_history_log(&hot_bytes).map_err(DurableError::Rejected)?;

    let snapshot = encode_history_snapshot(store, generation, represented_wal_end)
        .map_err(DurableError::Rejected)?;
    let snap_path = dir.join(history_snapshot_filename(generation));
    let snap_tmp = tmp_path(&snap_path);
    write_file_synced(&snap_tmp, &snapshot).map_err(|_| {
        DurableError::Rejected(HistoryError::Invalid("history seal snapshot write failed"))
    })?;
    let sealed_back = fs::read(&snap_tmp).map_err(|_| {
        DurableError::Rejected(HistoryError::Invalid(
            "history seal snapshot re-read failed",
        ))
    })?;
    if sealed_back != snapshot {
        return Err(DurableError::Rejected(HistoryError::Invalid(
            "history sealed snapshot did not survive its own write",
        )));
    }
    decode_history_snapshot(&sealed_back).map_err(DurableError::Rejected)?;
    publish_file(&snap_tmp, &snap_path, dir).map_err(|_| {
        DurableError::Rejected(HistoryError::Invalid(
            "history seal snapshot publication failed",
        ))
    })?;

    let hot_next = dir.join(history_wal_filename(generation));
    if hot_next.exists() {
        let stale = fs::read(&hot_next).map_err(|_| {
            DurableError::Rejected(HistoryError::Invalid(
                "history next-generation hot log read failed",
            ))
        })?;
        if !stale.is_empty() {
            return Err(DurableError::Rejected(HistoryError::Invalid(
                "history next-generation hot log is not empty",
            )));
        }
    }
    let hot_tmp = tmp_path(&hot_next);
    write_file_synced(&hot_tmp, &[]).map_err(|_| {
        DurableError::Rejected(HistoryError::Invalid("history next hot log write failed"))
    })?;
    publish_file(&hot_tmp, &hot_next, dir).map_err(|_| {
        DurableError::Rejected(HistoryError::Invalid(
            "history next hot log publication failed",
        ))
    })?;

    let sealed = ManifestSealed::for_snapshot(snapshot.len() as u64, &snapshot);
    Ok(StagedSeal {
        previous: current,
        generation,
        represented_wal_end,
        sealed,
        snapshot_len: snapshot.len() as u64,
    })
}

/// Commits a staged seal to manifest authority: tmp write and sync, atomic
/// rename, then the directory sync that makes the rename durable.
///
/// Failure classes are phase-correct: tmp write/sync or rename failure
/// leaves manifest N in place (definite rejection), but a rename that
/// succeeded with a failed directory sync leaves old-or-new authority behind
/// — indeterminate, so the writer poisons and must reopen instead of
/// continuing on an unknown generation. The directory sync runs through the
/// injected hook so tests can deterministically take the rename-ok/sync-fail
/// cut; production passes the real directory sync.
fn commit_staged_manifest(
    store: &mut PersistentHistoryStore,
    dir: &Path,
    staged: &StagedSeal,
    sync_dir_once: impl FnOnce() -> std::io::Result<()>,
) -> Result<(), DurableError> {
    let manifest = HistoryManifest::for_generation(staged.generation, Some(staged.sealed));
    let manifest_path = dir.join(HISTORY_MANIFEST_FILE);
    let manifest_tmp = tmp_path(&manifest_path);
    write_file_synced(&manifest_tmp, &encode_history_manifest(&manifest)).map_err(|_| {
        DurableError::Rejected(HistoryError::Invalid("history manifest write failed"))
    })?;
    std::fs::rename(&manifest_tmp, &manifest_path).map_err(|_| {
        DurableError::Rejected(HistoryError::Invalid("history manifest publication failed"))
    })?;
    sync_dir_once().map_err(|source| {
        store.set_poisoned();
        DurableError::Indeterminate {
            operation: crate::operation::DurabilityOperation::DirectorySync,
            source,
        }
    })
}

/// Recycles the superseded hot log after the new manifest is durable.
/// Best-effort by design: a leftover superseded log is ignored, never read.
fn recycle_superseded_hot(dir: &Path, previous: u64) -> bool {
    match fs::remove_file(dir.join(history_wal_filename(previous))) {
        Ok(()) => {
            let _ = sync_dir(dir);
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => false,
    }
}

fn current_generation(dir: &Path) -> Result<u64, DurableError> {
    let manifest_path = dir.join(HISTORY_MANIFEST_FILE);
    match fs::read(&manifest_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(_) => Err(DurableError::Rejected(HistoryError::Invalid(
            "history manifest read failed",
        ))),
        Ok(bytes) => decode_history_manifest(&bytes)
            .map(|manifest| manifest.generation())
            .map_err(DurableError::Rejected),
    }
}

fn tmp_path(final_path: &Path) -> PathBuf {
    let mut tmp = final_path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

fn write_file_synced(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn publish_file(tmp: &Path, final_path: &Path, dir: &Path) -> std::io::Result<()> {
    std::fs::rename(tmp, final_path)?;
    sync_dir(dir)
}

fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::super::durable_log::DurableHistoryLog;
    use super::super::manifest::ManifestSealed;
    use super::super::snapshot::{decode_history_snapshot, encode_history_snapshot_struct};
    use super::super::{CommitOutcome, HistoryId, VersionId};
    use super::*;

    fn fixture_dir() -> tempfile::TempDir {
        tempfile::TempDir::new().unwrap()
    }

    fn committed(
        store: &mut PersistentHistoryStore,
        history: HistoryId,
        parent: Option<VersionId>,
        payload: &[u8],
    ) -> VersionId {
        match store.append(history, parent, payload, None, None).unwrap() {
            CommitOutcome::Committed(version) => version.id(),
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("fixture commit must create")
            }
        }
    }

    fn durable_committed(
        store: &mut PersistentHistoryStore,
        log: &mut DurableHistoryLog,
        history: HistoryId,
        parent: Option<VersionId>,
        payload: &[u8],
    ) -> VersionId {
        match store
            .append_durable(log, history, parent, payload, None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version.id(),
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("fixture durable commit must create")
            }
        }
    }

    /// Builds a store whose commits are genuinely durable in the generation-0
    /// hot log: three versions across two histories, one bound.
    fn durable_fixture(dir: &Path) -> PersistentHistoryStore {
        let mut store = PersistentHistoryStore::new();
        let mut log = DurableHistoryLog::open(&dir.join(history_wal_filename(0))).unwrap();
        let first = store
            .create_history_durable_with_binding(&mut log, b"thread-a")
            .unwrap();
        let second = store.create_history_durable(&mut log).unwrap();
        let v0 = durable_committed(&mut store, &mut log, first, None, b"aaa");
        let _v1 = durable_committed(&mut store, &mut log, first, Some(v0), b"aaabbb");
        let _v2 = durable_committed(&mut store, &mut log, second, None, b"eee");
        store
    }

    fn assert_same_authority(expected: &PersistentHistoryStore, actual: &PersistentHistoryStore) {
        assert_eq!(actual.versions.len(), expected.versions.len());
        assert_eq!(actual.histories, expected.histories);
        assert_eq!(actual.next_history_id, expected.next_history_id);
        assert_eq!(actual.next_version_id, expected.next_version_id);
        assert_eq!(actual.active_requests, expected.active_requests);
        assert_eq!(actual.retired_requests, expected.retired_requests);
        assert_eq!(actual.receipt_order, expected.receipt_order);
        assert_eq!(actual.expired_versions, expected.expired_versions);
        assert_eq!(actual.history_bindings, expected.history_bindings);
        assert_eq!(actual.version_bindings, expected.version_bindings);
        for version in &expected.versions {
            let reopened = actual.lookup_version(version.id()).unwrap();
            assert_eq!(reopened, *version);
            let mut expected_bytes = Vec::new();
            let mut actual_bytes = Vec::new();
            expected
                .read(
                    *version,
                    0,
                    version.root().logical_len().get(),
                    &mut expected_bytes,
                )
                .unwrap();
            actual
                .read(
                    reopened,
                    0,
                    reopened.root().logical_len().get(),
                    &mut actual_bytes,
                )
                .unwrap();
            assert_eq!(actual_bytes, expected_bytes);
        }
    }

    #[test]
    fn crash_before_any_publication_recovers_genesis_hot() {
        let temp = fixture_dir();
        let store = durable_fixture(temp.path());

        let opened = open_history_authority(temp.path()).unwrap();
        assert_eq!(opened.generation, 0);
        assert_eq!(opened.stats.snapshot_versions, 0);
        let hot_len = std::fs::metadata(temp.path().join(history_wal_filename(0)))
            .unwrap()
            .len();
        assert_eq!(opened.stats.suffix_bytes, hot_len);
        assert_same_authority(&store, &opened.store);
    }

    #[test]
    fn stray_tmp_files_are_ignored_before_rename() {
        let temp = fixture_dir();
        let store = durable_fixture(temp.path());
        // A crash between the tmp write and its rename leaves stray files
        // that must never confer authority.
        std::fs::write(
            temp.path()
                .join(format!("{}.tmp", history_snapshot_filename(1))),
            b"incomplete snapshot bytes",
        )
        .unwrap();
        std::fs::write(
            temp.path().join(format!("{HISTORY_MANIFEST_FILE}.tmp")),
            b"incomplete manifest bytes",
        )
        .unwrap();
        std::fs::write(
            temp.path().join(format!("{}.tmp", history_wal_filename(1))),
            b"incomplete hot bytes",
        )
        .unwrap();

        let opened = open_history_authority(temp.path()).unwrap();
        assert_eq!(opened.generation, 0);
        assert_same_authority(&store, &opened.store);
    }

    #[test]
    fn published_snapshot_without_manifest_stays_on_old_authority() {
        let temp = fixture_dir();
        let store = durable_fixture(temp.path());
        // A sealed file alone is not authority: only the manifest moves it.
        // Garbage bytes prove the file is never even opened pre-publication.
        std::fs::write(
            temp.path().join(history_snapshot_filename(1)),
            b"forged snapshot without manifest",
        )
        .unwrap();
        std::fs::write(temp.path().join(history_wal_filename(1)), b"").unwrap();

        let opened = open_history_authority(temp.path()).unwrap();
        assert_eq!(opened.generation, 0);
        assert_same_authority(&store, &opened.store);
    }

    #[test]
    fn superseded_hot_is_ignored_after_publication() {
        let temp = fixture_dir();
        let mut store = durable_fixture(temp.path());
        let summary = seal_history_generation(&mut store, temp.path()).unwrap();
        assert_eq!(summary.generation, 1);
        assert!(summary.recycled_hot);
        // Resurrect the superseded hot file with plausible bytes: the new
        // authority must never read it again.
        std::fs::write(
            temp.path().join(history_wal_filename(0)),
            b"resurrected hot",
        )
        .unwrap();

        let opened = open_history_authority(temp.path()).unwrap();
        assert_eq!(opened.generation, 1);
        assert_eq!(opened.stats.snapshot_versions, 3);
        assert_eq!(opened.stats.suffix_bytes, 0);
        assert_same_authority(&store, &opened.store);
    }

    #[test]
    fn missing_new_hot_fails_closed() {
        let temp = fixture_dir();
        let mut store = durable_fixture(temp.path());
        seal_history_generation(&mut store, temp.path()).unwrap();
        std::fs::remove_file(temp.path().join(history_wal_filename(1))).unwrap();

        let error = open_history_authority(temp.path()).unwrap_err();
        assert!(matches!(error, DurableError::Rejected(_)));
    }

    #[test]
    fn sealed_snapshot_missing_fails_closed() {
        let temp = fixture_dir();
        let mut store = durable_fixture(temp.path());
        seal_history_generation(&mut store, temp.path()).unwrap();
        std::fs::remove_file(temp.path().join(history_snapshot_filename(1))).unwrap();

        let error = open_history_authority(temp.path()).unwrap_err();
        assert!(matches!(error, DurableError::Rejected(_)));
    }

    #[test]
    fn torn_manifest_fails_closed() {
        let temp = fixture_dir();
        let mut store = durable_fixture(temp.path());
        seal_history_generation(&mut store, temp.path()).unwrap();
        let manifest_path = temp.path().join(HISTORY_MANIFEST_FILE);
        let bytes = std::fs::read(&manifest_path).unwrap();

        std::fs::write(&manifest_path, &bytes[..3]).unwrap();
        assert!(matches!(
            open_history_authority(temp.path()).unwrap_err(),
            DurableError::Rejected(_)
        ));

        std::fs::write(&manifest_path, b"{}").unwrap();
        assert!(matches!(
            open_history_authority(temp.path()).unwrap_err(),
            DurableError::Rejected(_)
        ));

        let mut flipped = bytes.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0xff;
        std::fs::write(&manifest_path, &flipped).unwrap();
        assert!(matches!(
            open_history_authority(temp.path()).unwrap_err(),
            DurableError::Rejected(_)
        ));
    }

    #[test]
    fn snapshot_length_mismatch_fails_closed() {
        let temp = fixture_dir();
        let mut store = durable_fixture(temp.path());
        seal_history_generation(&mut store, temp.path()).unwrap();
        let snap_path = temp.path().join(history_snapshot_filename(1));
        let mut bytes = std::fs::read(&snap_path).unwrap();
        bytes.push(0x00);
        std::fs::write(&snap_path, &bytes).unwrap();

        let error = open_history_authority(temp.path()).unwrap_err();
        assert!(matches!(error, DurableError::Rejected(_)));
    }

    #[test]
    fn snapshot_byte_flip_fails_closed_on_digest() {
        let temp = fixture_dir();
        let mut store = durable_fixture(temp.path());
        seal_history_generation(&mut store, temp.path()).unwrap();
        let snap_path = temp.path().join(history_snapshot_filename(1));
        let mut bytes = std::fs::read(&snap_path).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0x01;
        std::fs::write(&snap_path, &bytes).unwrap();

        let error = open_history_authority(temp.path()).unwrap_err();
        assert!(matches!(error, DurableError::Rejected(_)));
    }

    #[test]
    fn snapshot_generation_mismatch_fails_closed() {
        let temp = fixture_dir();
        let store = durable_fixture(temp.path());
        // A well-formed snapshot for generation 9, published under manifest
        // generation 1 with a matching digest: length and digest agree, so
        // only the generation cross-check can catch it.
        let forged = encode_history_snapshot(&store, 9, 0).unwrap();
        let snap_path = temp.path().join(history_snapshot_filename(1));
        write_file_synced(&snap_path, &forged).unwrap();
        let manifest = HistoryManifest::for_generation(
            1,
            Some(ManifestSealed::for_snapshot(forged.len() as u64, &forged)),
        );
        write_file_synced(
            &temp.path().join(HISTORY_MANIFEST_FILE),
            &encode_history_manifest(&manifest),
        )
        .unwrap();
        std::fs::write(temp.path().join(history_wal_filename(1)), b"").unwrap();

        let error = open_history_authority(temp.path()).unwrap_err();
        assert!(matches!(error, DurableError::Rejected(_)));
    }

    #[test]
    fn corrupt_complete_hot_frame_fails_closed() {
        let temp = fixture_dir();
        let mut store = durable_fixture(temp.path());
        seal_history_generation(&mut store, temp.path()).unwrap();
        let mut log = DurableHistoryLog::open(&temp.path().join(history_wal_filename(1))).unwrap();
        let history = HistoryId::new(0);
        durable_committed(
            &mut store,
            &mut log,
            history,
            Some(VersionId::new(1)),
            b"fff",
        );
        drop(log);
        // Corrupt the frame magic: a complete-but-invalid frame must fail,
        // never be mistaken for a torn tail.
        let hot_path = temp.path().join(history_wal_filename(1));
        let mut bytes = std::fs::read(&hot_path).unwrap();
        assert!(!bytes.is_empty());
        bytes[0] ^= 0xff;
        std::fs::write(&hot_path, &bytes).unwrap();

        let error = open_history_authority(temp.path()).unwrap_err();
        assert!(matches!(error, DurableError::Rejected(_)));
    }

    #[test]
    fn writable_authority_owns_single_writer_across_seal() {
        let temp = fixture_dir();
        let mut first = WritableHistoryAuthority::open(temp.path()).unwrap();
        assert_eq!(first.generation(), 0);
        // A second writable open is rejected while the first authority lives.
        assert!(matches!(
            WritableHistoryAuthority::open(temp.path()),
            Err(DurableError::AlreadyOpen)
        ));
        // Lock-free read-only loading still works beside the writer.
        let read_only = open_history_authority(temp.path()).unwrap();
        assert_eq!(read_only.generation, 0);

        let history = first.create_history(Some(b"thread-a")).unwrap();
        let v0 = match first.append(history, None, b"aaa", None, None).unwrap() {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("authority commit must create")
            }
        };
        let summary = first.seal().unwrap();
        assert_eq!(summary.generation, 1);
        assert!(summary.recycled_hot);
        assert_eq!(first.generation(), 1);
        assert_eq!(first.stats().snapshot_versions, 1);
        assert_eq!(first.stats().suffix_bytes, 0);
        // The new handle was already owned when the manifest committed, and
        // the superseded hot log is gone only after that authority.
        assert!(!temp.path().join(history_wal_filename(0)).exists());
        assert!(temp.path().join(history_wal_filename(1)).exists());
        // The store-wide lease survives the generation transition: the
        // second writer is still rejected after seal.
        assert!(matches!(
            WritableHistoryAuthority::open(temp.path()),
            Err(DurableError::AlreadyOpen)
        ));
        // Post-seal writes land in the new generation through the owned
        // handle, with no unlocked open anywhere in the path.
        let v1 = match first
            .append(history, Some(v0.id()), b"bbb", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("post-seal commit must create")
            }
        };
        drop(first);
        // After release, the next writer opens exactly where the first left
        // off: sealed snapshot plus the bounded post-seal suffix.
        let second = WritableHistoryAuthority::open(temp.path()).unwrap();
        assert_eq!(second.generation(), 1);
        assert_eq!(second.stats().snapshot_versions, 1);
        assert!(second.stats().suffix_bytes > 0);
        let got = second.store().lookup_version(v1.id()).unwrap();
        assert_eq!(got, v1);
        let mut output = Vec::new();
        second.store().read(got, 0, 6, &mut output).unwrap();
        assert_eq!(output, b"aaabbb");
    }

    #[test]
    fn cross_history_snapshot_lineage_fails_authority_open() {
        // History 0 owns V0, history 1 owns V1 as a root; graft V1 under V0
        // and publish with recomputed length and digest. Integrity agrees, so
        // only the lineage cross-check can reject at open.
        let temp = fixture_dir();
        let mut store = PersistentHistoryStore::new();
        let first = store.create_history().unwrap();
        let second = store.create_history().unwrap();
        committed(&mut store, first, None, b"aaa");
        committed(&mut store, second, None, b"bbb");
        let snapshot =
            decode_history_snapshot(&encode_history_snapshot(&store, 1, 0).unwrap()).unwrap();
        let mut forged = snapshot;
        forged.versions[1].parent = Some(VersionId::new(0));
        let forged_bytes = encode_history_snapshot_struct(&forged).unwrap();
        let snap_path = temp.path().join(history_snapshot_filename(1));
        write_file_synced(&snap_path, &forged_bytes).unwrap();
        let manifest = HistoryManifest::for_generation(
            1,
            Some(ManifestSealed::for_snapshot(
                forged_bytes.len() as u64,
                &forged_bytes,
            )),
        );
        write_file_synced(
            &temp.path().join(HISTORY_MANIFEST_FILE),
            &encode_history_manifest(&manifest),
        )
        .unwrap();
        std::fs::write(temp.path().join(history_wal_filename(1)), b"").unwrap();
        assert!(matches!(
            open_history_authority(temp.path()),
            Err(DurableError::Rejected(_))
        ));
    }

    #[test]
    fn seal_rejects_foreign_next_hot_and_keeps_old_authority() {
        // Any next-hot acquisition failure lands before manifest publication:
        // definite rejection, manifest still generation zero, and the old
        // writer keeps operating on its still-current handle.
        for sabotage in ["nonempty", "directory"] {
            let temp = fixture_dir();
            let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
            let history = authority.create_history(Some(b"thread-a")).unwrap();
            let v0 = match authority.append(history, None, b"aaa", None, None).unwrap() {
                CommitOutcome::Committed(version) => version,
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("authority commit must create")
                }
            };
            match sabotage {
                "nonempty" => {
                    std::fs::write(temp.path().join(history_wal_filename(1)), b"foreign bytes")
                        .unwrap();
                }
                _ => {
                    std::fs::create_dir(temp.path().join(history_wal_filename(1))).unwrap();
                }
            }
            let error = authority.seal().unwrap_err();
            assert!(
                matches!(error, DurableError::Rejected(_)),
                "{sabotage}: pre-manifest failure must reject, got {error}"
            );
            assert!(!temp.path().join(HISTORY_MANIFEST_FILE).exists());
            assert_eq!(authority.generation(), 0);
            let v1 = match authority
                .append(history, Some(v0.id()), b"bbb", None, None)
                .unwrap()
            {
                CommitOutcome::Committed(version) => version,
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("old writer must stay operational")
                }
            };
            drop(authority);
            let reopened = open_history_authority(temp.path()).unwrap();
            assert_eq!(reopened.generation, 0);
            let got = reopened.store.lookup_version(v1.id()).unwrap();
            let mut output = Vec::new();
            reopened.store.read(got, 0, 6, &mut output).unwrap();
            assert_eq!(output, b"aaabbb");
        }
    }

    #[test]
    fn manifest_dirsync_failure_poisons_and_reopen_resolves() {
        // Rename-ok/dirsync-fail is old-or-new authority: indeterminate, the
        // writer poisons and must not continue, and reopen resolves a valid
        // authority on whichever side of the cut survived.
        let temp = fixture_dir();
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let history = authority.create_history(Some(b"thread-a")).unwrap();
        let v0 = match authority.append(history, None, b"aaa", None, None).unwrap() {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("authority commit must create")
            }
        };
        let error = authority
            .seal_with_sync(|| {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "injected dirsync fault",
                ))
            })
            .unwrap_err();
        assert!(
            matches!(error, DurableError::Indeterminate { .. }),
            "rename-ok/sync-fail must be indeterminate, got {error}"
        );
        assert!(authority.store().is_poisoned());
        assert!(matches!(
            authority.create_history(None),
            Err(DurableError::RecoveryRequired)
        ));
        assert!(matches!(
            authority.append(history, None, b"zzz", None, None),
            Err(DurableError::RecoveryRequired)
        ));
        assert!(matches!(
            authority.seal(),
            Err(DurableError::RecoveryRequired)
        ));
        drop(authority);
        let reopened = open_history_authority(temp.path()).unwrap();
        match reopened.generation {
            0 => {
                let got = reopened.store.lookup_version(v0.id()).unwrap();
                let mut output = Vec::new();
                reopened.store.read(got, 0, 3, &mut output).unwrap();
                assert_eq!(output, b"aaa");
            }
            1 => {
                assert_eq!(reopened.stats.snapshot_versions, 1);
                let got = reopened.store.lookup_version(v0.id()).unwrap();
                let mut output = Vec::new();
                reopened.store.read(got, 0, 3, &mut output).unwrap();
                assert_eq!(output, b"aaa");
            }
            generation => panic!("reopen must resolve old-or-new authority, got {generation}"),
        }
    }

    #[test]
    fn second_writer_is_rejected_with_contention() {
        let temp = fixture_dir();
        let path = temp.path().join(history_wal_filename(0));
        let first = DurableHistoryLog::open_write(&path).unwrap();
        let error = DurableHistoryLog::open_write(&path).unwrap_err();
        assert!(DurableHistoryLog::is_lock_contention(&error));
        drop(first);
        // Releasing the handle releases the lock: the next writer proceeds.
        let _reopened = DurableHistoryLog::open_write(&path).unwrap();
    }

    #[test]
    fn reopen_reports_bounded_restart_inputs() {
        let temp = fixture_dir();
        let mut store = durable_fixture(temp.path());
        seal_history_generation(&mut store, temp.path()).unwrap();
        let mut log = DurableHistoryLog::open(&temp.path().join(history_wal_filename(1))).unwrap();
        let history = HistoryId::new(0);
        durable_committed(
            &mut store,
            &mut log,
            history,
            Some(VersionId::new(1)),
            b"fff",
        );
        durable_committed(
            &mut store,
            &mut log,
            history,
            Some(VersionId::new(3)),
            b"ggg",
        );
        drop(log);

        let opened = open_history_authority(temp.path()).unwrap();
        assert_eq!(opened.generation, 1);
        // Restart cost is exactly snapshot versions plus hot suffix bytes:
        // both are reported so callers can bound reopen work with seal
        // policy instead of trusting it.
        assert_eq!(opened.stats.snapshot_versions, 3);
        let hot_len = std::fs::metadata(temp.path().join(history_wal_filename(1)))
            .unwrap()
            .len();
        assert!(hot_len > 0);
        assert_eq!(opened.stats.suffix_bytes, hot_len);
        assert_same_authority(&store, &opened.store);
    }

    fn sealed_fixture(dir: &Path) -> PersistentHistoryStore {
        let mut store = PersistentHistoryStore::new();
        let first = store.create_history_with_binding(b"thread-a").unwrap();
        let second = store.create_history().unwrap();
        let v0 = committed(&mut store, first, None, b"aaa");
        let v1 = committed(&mut store, first, Some(v0), b"bbb");
        let _v2 = committed(&mut store, first, Some(v1), b"ccc");
        let _v3 = committed(&mut store, first, Some(v0), b"ddd");
        let _v4 = committed(&mut store, second, None, b"eee");
        let summary = seal_history_generation(&mut store, dir).unwrap();
        assert_eq!(summary.generation, 1);
        store
    }

    #[test]
    fn seal_round_trip_preserves_exact_authority() {
        let temp = fixture_dir();
        let store = sealed_fixture(temp.path());

        let opened = open_history_authority(temp.path()).unwrap();
        assert_eq!(opened.generation, 1);
        assert_eq!(opened.stats.snapshot_versions, 5);
        assert_eq!(opened.stats.suffix_bytes, 0);
        assert_eq!(opened.store.versions.len(), store.versions.len());
        assert_eq!(opened.store.histories, store.histories);
        assert_eq!(opened.store.next_history_id, store.next_history_id);
        assert_eq!(opened.store.next_version_id, store.next_version_id);
        assert_eq!(opened.store.active_requests, store.active_requests);
        assert_eq!(opened.store.retired_requests, store.retired_requests);
        assert_eq!(opened.store.history_bindings, store.history_bindings);
        assert_eq!(opened.store.version_bindings, store.version_bindings);
        for version in &store.versions {
            let reopened = opened.store.lookup_version(version.id()).unwrap();
            assert_eq!(reopened, *version);
            let mut expected = Vec::new();
            let mut actual = Vec::new();
            store
                .read(
                    *version,
                    0,
                    version.root().logical_len().get(),
                    &mut expected,
                )
                .unwrap();
            opened
                .store
                .read(
                    reopened,
                    0,
                    reopened.root().logical_len().get(),
                    &mut actual,
                )
                .unwrap();
            assert_eq!(actual, expected);
        }
        // The superseded genesis hot log is recycled by the seal itself.
        assert!(!temp.path().join(history_wal_filename(0)).exists());
        assert!(temp.path().join(history_wal_filename(1)).exists());
        assert!(temp.path().join(history_snapshot_filename(1)).exists());
        assert!(temp.path().join(HISTORY_MANIFEST_FILE).exists());
    }

    #[test]
    fn snapshot_plus_suffix_reopens_to_full_history() {
        let temp = fixture_dir();
        sealed_fixture(temp.path());

        // Append a genuinely durable suffix to the generation-1 hot log.
        let opened = open_history_authority(temp.path()).unwrap();
        assert_eq!(opened.generation, 1);
        let mut store = opened.store;
        let mut log = DurableHistoryLog::open(&temp.path().join(history_wal_filename(1))).unwrap();
        let history = HistoryId::new(0);
        let base = store.lookup_version(VersionId::new(3)).unwrap();
        let v5 = match store
            .append_durable(&mut log, history, Some(base.id()), b"fff", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("suffix commit must create")
            }
        };
        let v6 = match store
            .append_durable(&mut log, history, Some(v5.id()), b"ggg", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("suffix commit must create")
            }
        };
        drop(store);
        drop(log);

        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.generation, 1);
        assert_eq!(reopened.stats.snapshot_versions, 5);
        assert!(reopened.stats.suffix_bytes > 0);
        assert_eq!(reopened.store.versions.len(), 7);
        let suffix_v5 = reopened.store.lookup_version(v5.id()).unwrap();
        let suffix_v6 = reopened.store.lookup_version(v6.id()).unwrap();
        assert_eq!(suffix_v5.parent(), Some(base.id()));
        assert_eq!(suffix_v6.parent(), Some(v5.id()));
        let mut output = Vec::new();
        reopened
            .store
            .read(
                suffix_v6,
                0,
                suffix_v6.root().logical_len().get(),
                &mut output,
            )
            .unwrap();
        assert_eq!(output, b"aaadddfffggg");
    }

    #[test]
    fn seal_then_splice_suffix_reopens_exact() {
        // Durable splice after a seal lands in the new hot generation and
        // reopens byte-exact: snapshot base plus bounded splice suffix.
        let temp = fixture_dir();
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let history = authority.create_history(Some(b"thread-a")).unwrap();
        let v0 = match authority
            .append(history, None, b"abcdefghij", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("root append must create")
            }
        };
        let summary = authority.seal().unwrap();
        assert_eq!(summary.generation, 1);
        let v1 = match authority
            .splice(history, Some(v0.id()), 5, 2, b"XY", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("suffix splice must create")
            }
        };
        drop(authority);
        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.generation, 1);
        assert_eq!(reopened.stats.snapshot_versions, 1);
        assert!(reopened.stats.suffix_bytes > 0);
        let got = reopened.store.lookup_version(v1.id()).unwrap();
        let mut output = Vec::new();
        reopened
            .store
            .read(got, 0, got.root().logical_len().get(), &mut output)
            .unwrap();
        assert_eq!(output, b"abcdeXYhij");
        // The pre-seal root is untouched by the suffix splice.
        let base = reopened.store.lookup_version(v0.id()).unwrap();
        let mut expected = Vec::new();
        reopened
            .store
            .read(base, 0, base.root().logical_len().get(), &mut expected)
            .unwrap();
        assert_eq!(expected, b"abcdefghij");
    }

    fn authority_forked(
        authority: &mut WritableHistoryAuthority,
        history: HistoryId,
        parent: VersionId,
    ) -> crate::persistent_history::Version {
        match authority.fork(history, parent, None, None).unwrap() {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("authority fork must create")
            }
        }
    }

    #[test]
    fn seal_then_fork_suffix_reopens_exact() {
        // Durable fork after a seal lands in the new hot generation and
        // reopens exact: snapshot base plus a content-free fork suffix whose
        // child shares the sealed parent root.
        let temp = fixture_dir();
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let history = authority.create_history(Some(b"thread-a")).unwrap();
        let v0 = match authority
            .append(history, None, b"fork-suffix-base", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("root append must create")
            }
        };
        let summary = authority.seal().unwrap();
        assert_eq!(summary.generation, 1);
        let v1 = authority_forked(&mut authority, history, v0.id());
        assert_eq!(v1.parent(), Some(v0.id()));
        assert_eq!(v1.root(), v0.root());
        drop(authority);
        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.generation, 1);
        assert_eq!(reopened.stats.snapshot_versions, 1);
        assert!(reopened.stats.suffix_bytes > 0);
        assert_eq!(reopened.store.versions.len(), 2);
        let got = reopened.store.lookup_version(v1.id()).unwrap();
        assert_eq!(got.parent(), Some(v0.id()));
        assert_eq!(
            got.root(),
            reopened.store.lookup_version(v0.id()).unwrap().root()
        );
        let mut output = Vec::new();
        reopened
            .store
            .read(got, 0, got.root().logical_len().get(), &mut output)
            .unwrap();
        assert_eq!(output, b"fork-suffix-base");
    }

    #[test]
    fn fork_then_seal_snapshot_only_reopens_exact() {
        // Fork before seal: the sealed snapshot catalogue alone carries the
        // shared root (no hot suffix), and reopen preserves every forked
        // identity, parent, root, and byte exactly.
        let temp = fixture_dir();
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let history = authority.create_history(Some(b"thread-a")).unwrap();
        let v0 = match authority
            .append(history, None, b"sealed-fork-base", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("root append must create")
            }
        };
        let v1 = authority_forked(&mut authority, history, v0.id());
        let v2 = authority_forked(&mut authority, history, v0.id());
        assert_ne!(v1.id(), v2.id());
        let summary = authority.seal().unwrap();
        assert_eq!(summary.generation, 1);
        drop(authority);
        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.generation, 1);
        assert_eq!(reopened.stats.snapshot_versions, 3);
        assert_eq!(reopened.stats.suffix_bytes, 0);
        for forked in [v1, v2] {
            let got = reopened.store.lookup_version(forked.id()).unwrap();
            assert_eq!(got, forked);
            assert_eq!(got.parent(), Some(v0.id()));
            assert_eq!(
                got.root(),
                reopened.store.lookup_version(v0.id()).unwrap().root()
            );
            let mut output = Vec::new();
            reopened
                .store
                .read(got, 0, got.root().logical_len().get(), &mut output)
                .unwrap();
            assert_eq!(output, b"sealed-fork-base");
            reopened.store.verify(got).unwrap();
        }
    }

    #[test]
    fn expire_reopen_seal_matrix_preserves_lifecycle() {
        // Retain V, expire V, close, reopen: still expired. Seal with the
        // expiration in the suffix, reopen: still expired. Expire, seal,
        // reopen from snapshot only: still expired with zero suffix.
        let temp = fixture_dir();
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let history = authority.create_history(Some(b"thread-a")).unwrap();
        let v0 = match authority
            .append(history, None, b"expire-matrix", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("root append must create")
            }
        };
        let v1 = authority_forked(&mut authority, history, v0.id());
        assert_eq!(
            authority.expire(v0.id()).unwrap(),
            crate::persistent_history::ExpireOutcome::Expired
        );
        assert!(authority.store().is_expired(v0.id()).unwrap());
        drop(authority);
        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.store.is_expired(v0.id()), Ok(true));
        assert_eq!(reopened.store.is_retained(v1.id()), Ok(true));
        // Repeated expiration after reopen writes zero new WAL bytes.
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let hot_len_before =
            std::fs::metadata(temp.path().join(history_wal_filename(reopened.generation)))
                .unwrap()
                .len();
        assert_eq!(
            authority.expire(v0.id()).unwrap(),
            crate::persistent_history::ExpireOutcome::AlreadyExpired
        );
        assert_eq!(
            std::fs::metadata(temp.path().join(history_wal_filename(reopened.generation)))
                .unwrap()
                .len(),
            hot_len_before
        );
        // Seal with the expiration in the suffix, then reopen.
        let summary = authority.seal().unwrap();
        drop(authority);
        let sealed = open_history_authority(temp.path()).unwrap();
        assert_eq!(sealed.generation, summary.generation);
        assert_eq!(sealed.store.is_expired(v0.id()), Ok(true));
        assert_eq!(sealed.stats.suffix_bytes, 0);
    }

    #[test]
    fn expire_suffix_after_seal_reopens_expired() {
        let temp = fixture_dir();
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let history = authority.create_history(Some(b"thread-a")).unwrap();
        let v0 = match authority
            .append(history, None, b"suffix-expire", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("root append must create")
            }
        };
        authority.seal().unwrap();
        assert_eq!(
            authority.expire(v0.id()).unwrap(),
            crate::persistent_history::ExpireOutcome::Expired
        );
        drop(authority);
        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.store.is_expired(v0.id()), Ok(true));
        assert!(reopened.stats.suffix_bytes > 0);
        assert_eq!(
            reopened.store.lookup_version(v0.id()),
            Err(crate::persistent_history::HistoryError::VersionExpired)
        );
    }

    #[test]
    fn expire_moves_receipt_durable_across_seal_reopen() {
        // E4.25 durable half: request R creates V1, expire V1, seal, reopen —
        // R stays retired with identical behavior on both sides of the seal.
        let temp = fixture_dir();
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let history = authority.create_history(Some(b"thread-a")).unwrap();
        let v0 = match authority
            .append(history, None, b"receipt-expire", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("root append must create")
            }
        };
        let v1 = match authority
            .splice(
                history,
                Some(v0.id()),
                7,
                0,
                b"[e]",
                Some(b"req-seal"),
                None,
            )
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("request splice must create")
            }
        };
        authority.expire(v1.id()).unwrap();
        assert_eq!(
            authority.store().request_receipt_status(b"req-seal"),
            crate::persistent_history::RequestReceiptStatus::Retired
        );
        authority.seal().unwrap();
        drop(authority);
        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.store.is_expired(v1.id()), Ok(true));
        assert_eq!(
            reopened.store.request_receipt_status(b"req-seal"),
            crate::persistent_history::RequestReceiptStatus::Retired
        );
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        assert!(matches!(
            authority.splice(
                history,
                Some(v0.id()),
                7,
                0,
                b"[e]",
                Some(b"req-seal"),
                None
            ),
            Ok(CommitOutcome::Retired)
        ));
        assert!(matches!(
            authority.splice(
                history,
                Some(v0.id()),
                7,
                0,
                b"[x]",
                Some(b"req-seal"),
                None
            ),
            Err(
                crate::persistent_history::durable_log::DurableError::Rejected(
                    crate::persistent_history::HistoryError::RequestConflict
                )
            )
        ));
    }

    #[test]
    fn seal_reopen_preserves_horizon_across_capacity() {
        // E4.29 with a real seal: 4100 request forks, seal, close, reopen —
        // exact same count, statuses, order, and retry behavior; evicted
        // receipts stay unknown without resurrection.
        let temp = fixture_dir();
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let history = authority.create_history(Some(b"thread-a")).unwrap();
        let v0 = match authority
            .append(history, None, b"horizon-seal", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("root append must create")
            }
        };
        for index in 0..4100 {
            let request = format!("seal-req-{index:05}");
            match authority
                .fork(history, v0.id(), Some(request.as_bytes()), None)
                .unwrap()
            {
                CommitOutcome::Committed(_) => {}
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("fresh fork must create")
                }
            }
        }
        assert_eq!(authority.store().request_receipt_count(), 4096);
        let order_before: Vec<Vec<u8>> = authority.store().receipt_order.iter().cloned().collect();
        authority.seal().unwrap();
        drop(authority);
        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.stats.snapshot_versions, 4101);
        assert_eq!(reopened.stats.suffix_bytes, 0);
        assert_eq!(reopened.store.request_receipt_count(), 4096);
        assert_eq!(
            reopened
                .store
                .receipt_order
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            order_before
        );
        assert_eq!(
            reopened.store.request_receipt_status(b"seal-req-00003"),
            crate::persistent_history::RequestReceiptStatus::Unknown
        );
        assert!(matches!(
            reopened.store.request_receipt_status(b"seal-req-00004"),
            crate::persistent_history::RequestReceiptStatus::Active(_)
        ));
        // Retained replay and evicted-fresh behavior survive the seal.
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        assert!(matches!(
            authority.fork(history, v0.id(), Some(b"seal-req-00004"), None),
            Ok(CommitOutcome::Replayed(_))
        ));
        assert!(matches!(
            authority.fork(history, v0.id(), Some(b"seal-req-00003"), None),
            Ok(CommitOutcome::Committed(_))
        ));
        assert_eq!(authority.store().request_receipt_count(), 4096);
    }

    #[test]
    fn suffix_replay_reproduces_horizon_across_capacity() {
        // E4.30: seal a base near the horizon, cross the boundary in the hot
        // suffix, reopen — snapshot + suffix replay equals the live horizon
        // exactly, with no resurrected receipts.
        let temp = fixture_dir();
        let mut live = PersistentHistoryStore::new();
        let history = live.create_history().unwrap();
        let v0 = match live
            .append(history, None, b"suffix-horizon", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("root append must create")
            }
        };
        for index in 0..4000 {
            let request = format!("suffix-req-{index:05}");
            match live
                .fork(history, v0.id(), Some(request.as_bytes()), None)
                .unwrap()
            {
                CommitOutcome::Committed(_) => {}
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("fresh fork must create")
                }
            }
        }
        assert_eq!(live.request_receipt_count(), 4000);
        let summary = seal_history_generation(&mut live, temp.path()).unwrap();
        assert_eq!(summary.generation, 1);
        // 200 request forks in the generation-1 hot suffix cross the 4096
        // boundary: the oldest 104 base receipts evict.
        let opened = open_history_authority(temp.path()).unwrap();
        let mut store = opened.store;
        let mut log = DurableHistoryLog::open(&temp.path().join(history_wal_filename(1))).unwrap();
        for index in 4000..4200 {
            let request = format!("suffix-req-{index:05}");
            match store
                .fork_durable(&mut log, history, v0.id(), Some(request.as_bytes()), None)
                .unwrap()
            {
                CommitOutcome::Committed(_) => {}
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("fresh suffix fork must create")
                }
            }
        }
        // The live reference performs the same 200 operations in memory.
        for index in 4000..4200 {
            let request = format!("suffix-req-{index:05}");
            match live
                .fork(history, v0.id(), Some(request.as_bytes()), None)
                .unwrap()
            {
                CommitOutcome::Committed(_) => {}
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("fresh live fork must create")
                }
            }
        }
        assert_eq!(live.request_receipt_count(), 4096);
        drop(store);
        drop(log);
        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.store.request_receipt_count(), 4096);
        assert_eq!(reopened.store.active_requests, live.active_requests);
        assert_eq!(reopened.store.retired_requests, live.retired_requests);
        assert_eq!(reopened.store.receipt_order, live.receipt_order);
        assert_eq!(
            reopened.store.request_receipt_status(b"suffix-req-00000"),
            crate::persistent_history::RequestReceiptStatus::Unknown
        );
        assert_eq!(
            reopened.store.request_receipt_status(b"suffix-req-00103"),
            crate::persistent_history::RequestReceiptStatus::Unknown
        );
        assert!(matches!(
            reopened.store.request_receipt_status(b"suffix-req-00104"),
            crate::persistent_history::RequestReceiptStatus::Active(_)
        ));
    }

    #[test]
    fn thousand_fork_seal_reopen_at_ci_scale() {
        // CI-scale fork fleet through the writable authority: 256 KiB parent,
        // 1,000 durable forks, seal, reopen — every forked root exact.
        let temp = fixture_dir();
        let mut authority = WritableHistoryAuthority::open(temp.path()).unwrap();
        let history = authority.create_history(Some(b"thread-a")).unwrap();
        let parent_payload = vec![0x5Au8; 256 * 1024];
        let v0 = match authority
            .append(history, None, &parent_payload, None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("root append must create")
            }
        };
        let counters_before = authority.store().work_counters();
        for _ in 0..1000 {
            let forked = authority_forked(&mut authority, history, v0.id());
            assert_eq!(forked.parent(), Some(v0.id()));
            assert_eq!(forked.root(), v0.root());
        }
        let counters_after = authority.store().work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        assert_eq!(authority.store().versions.len(), 1001);
        let summary = authority.seal().unwrap();
        assert_eq!(summary.generation, 1);
        drop(authority);
        let reopened = open_history_authority(temp.path()).unwrap();
        assert_eq!(reopened.generation, 1);
        assert_eq!(reopened.stats.snapshot_versions, 1001);
        assert_eq!(reopened.stats.suffix_bytes, 0);
        assert_eq!(reopened.store.versions.len(), 1001);
        let base = reopened.store.lookup_version(v0.id()).unwrap();
        for id in [1u64, 500, 1000] {
            let got = reopened.store.lookup_version(VersionId::new(id)).unwrap();
            assert_eq!(got.parent(), Some(v0.id()));
            assert_eq!(got.root(), base.root());
        }
        let mut head = Vec::new();
        reopened
            .store
            .read(
                reopened.store.lookup_version(VersionId::new(1000)).unwrap(),
                0,
                16,
                &mut head,
            )
            .unwrap();
        assert_eq!(head, &parent_payload[..16]);
    }
}
