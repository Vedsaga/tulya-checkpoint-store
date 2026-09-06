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
//! write + sync manifest tmp {N+1, sealed digest}, rename, dir sync
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
    durable_log::{decode_history_log, replay_history_suffix, DurableError},
    manifest::{
        decode_history_manifest, encode_history_manifest, history_snapshot_filename,
        history_wal_filename, HistoryManifest, HISTORY_MANIFEST_FILE,
    },
    snapshot::{decode_history_snapshot, encode_history_snapshot},
    HistoryError, PersistentHistoryStore,
};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpenedHistoryStats {
    pub(crate) snapshot_versions: usize,
    pub(crate) suffix_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct OpenedHistory {
    pub(crate) store: PersistentHistoryStore,
    pub(crate) generation: u64,
    pub(crate) stats: OpenedHistoryStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SealSummary {
    pub(crate) generation: u64,
    pub(crate) represented_wal_end: u64,
    pub(crate) snapshot_len: u64,
    pub(crate) recycled_hot: bool,
}

/// Opens the authoritative history for a directory: manifest generation,
/// verified sealed snapshot, and current hot suffix replayed on top.
///
/// A missing manifest means no authority was ever published: generation zero
/// replays the genesis hot log when present, exactly like a fresh store
/// otherwise. Every other absence or disagreement fails closed.
pub(crate) fn open_history_authority(dir: &Path) -> Result<OpenedHistory, DurableError> {
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
    let suffix = match fs::read(&hot_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
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
/// The live store is never mutated; the caller advances its generation
/// tracking and write handle to the returned generation. All filesystem
/// failures before manifest publication leave the old authority complete and
/// report definite rejection.
pub(crate) fn seal_history_generation(
    store: &PersistentHistoryStore,
    dir: &Path,
) -> Result<SealSummary, DurableError> {
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

    let manifest = HistoryManifest::for_generation(
        generation,
        Some(super::manifest::ManifestSealed::for_snapshot(
            snapshot.len() as u64,
            &snapshot,
        )),
    );
    let manifest_tmp = tmp_path(&dir.join(HISTORY_MANIFEST_FILE));
    write_file_synced(&manifest_tmp, &encode_history_manifest(&manifest)).map_err(|_| {
        DurableError::Rejected(HistoryError::Invalid("history manifest write failed"))
    })?;
    publish_file(&manifest_tmp, &dir.join(HISTORY_MANIFEST_FILE), dir).map_err(|_| {
        DurableError::Rejected(HistoryError::Invalid("history manifest publication failed"))
    })?;

    let recycled_hot = match fs::remove_file(&hot_current) {
        Ok(()) => {
            let _ = sync_dir(dir);
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => false,
    };
    Ok(SealSummary {
        generation,
        represented_wal_end,
        snapshot_len: snapshot.len() as u64,
        recycled_hot,
    })
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
        match store.commit(history, parent, payload, None, None).unwrap() {
            CommitOutcome::Committed(version) => version.id(),
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("fixture commit must create")
            }
        }
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
        let summary = seal_history_generation(&store, dir).unwrap();
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
            .commit_durable(&mut log, history, Some(base.id()), b"fff", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("suffix commit must create")
            }
        };
        let v6 = match store
            .commit_durable(&mut log, history, Some(v5.id()), b"ggg", None, None)
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
}
