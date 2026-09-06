//! Domain-neutral persistent-history core.
//!
//! Tulya is a persistent-history engine first; LangGraph checkpointing is one
//! adapter workload. This module owns the generic version/history vocabulary
//! that every adapter builds on:
//!
//! ```text
//! adapters (checkpoint store, future)
//!      |
//!      v
//! PersistentHistoryStore
//!      |
//!      v
//! BalancedSequence (persistent sequence)
//! ```
//!
//! The dependency direction is load-bearing: this core knows nothing about
//! threads, checkpoint namespaces, messages, pending writes, or any other
//! adapter concept. A version is an identity plus an optional parent plus a
//! persistent root; payloads are opaque bytes. Adapter identity mapping
//! (for example checkpoint thread to history) lives above this module.
//!
//! Request ledgers, tombstones, snapshots, and reclamation arrive in later
//! slices; this slice covers versioned payload history with exact reads and
//! structural verification only.

use crate::persistent_sequence::{
    BalancedSequence, LogicalLength, PersistentRoot, PersistentSequence, PersistentSequenceAppend,
    SequenceError, SequenceRange, SequenceWorkCounters,
};
use std::collections::HashSet;
use std::fmt;

/// Opaque core-assigned history/object identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct HistoryId(u64);

impl HistoryId {
    pub(crate) const fn id(self) -> u64 {
        self.0
    }
}

/// Opaque core-assigned version identity, dense per store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct VersionId(u64);

impl VersionId {
    pub(crate) const fn id(self) -> u64 {
        self.0
    }
}

/// One committed generic version: its history, identity, optional parent,
/// and persistent root. Payloads stay opaque bytes below the adapter layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Version {
    history: HistoryId,
    id: VersionId,
    parent: Option<VersionId>,
    root: PersistentRoot,
}

impl Version {
    pub(crate) const fn history(self) -> HistoryId {
        self.history
    }

    pub(crate) const fn id(self) -> VersionId {
        self.id
    }

    pub(crate) const fn parent(self) -> Option<VersionId> {
        self.parent
    }

    pub(crate) const fn root(self) -> PersistentRoot {
        self.root
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryError {
    Sequence(SequenceError),
    Invalid(&'static str),
    Overflow(&'static str),
    Capacity(&'static str),
}

impl fmt::Display for HistoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sequence(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) | Self::Capacity(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl std::error::Error for HistoryError {}

impl From<SequenceError> for HistoryError {
    fn from(error: SequenceError) -> Self {
        Self::Sequence(error)
    }
}

/// Domain-neutral store of versioned opaque payload histories over one shared
/// balanced arena. Histories isolate parenthood: a parent version must belong
/// to the same history, so one object's lineage can never silently graft onto
/// another's.
#[derive(Debug, Default)]
pub(crate) struct PersistentHistoryStore {
    backend: BalancedSequence,
    histories: HashSet<HistoryId>,
    versions: Vec<Version>,
}

impl PersistentHistoryStore {
    pub(crate) fn new() -> Self {
        Self {
            backend: BalancedSequence::new(),
            histories: HashSet::new(),
            versions: Vec::new(),
        }
    }

    /// Creates an empty history and returns its core-assigned identity.
    pub(crate) fn create_history(&mut self) -> Result<HistoryId, HistoryError> {
        let id = HistoryId(
            u64::try_from(self.histories.len())
                .map_err(|_| HistoryError::Overflow("persistent history count exceeds u64"))?,
        );
        self.histories
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent history set allocation failed"))?;
        let _ = self.histories.insert(id);
        Ok(id)
    }

    /// Commits `payload` as a new version of `history` under `parent`.
    ///
    /// `None` parent creates a root version. The backend append preserves all
    /// retained history; on any failure no version is recorded.
    pub(crate) fn commit(
        &mut self,
        history: HistoryId,
        parent: Option<VersionId>,
        payload: &[u8],
    ) -> Result<Version, HistoryError> {
        if !self.histories.contains(&history) {
            return Err(HistoryError::Invalid(
                "persistent commit targets an unknown history",
            ));
        }
        let parent_root = match parent {
            None => None,
            Some(id) => {
                let record = self.version_record(id)?;
                if record.history() != history {
                    return Err(HistoryError::Invalid(
                        "persistent parent version belongs to a different history",
                    ));
                }
                Some(record.root())
            }
        };
        let id = VersionId(
            u64::try_from(self.versions.len())
                .map_err(|_| HistoryError::Overflow("persistent version count exceeds u64"))?,
        );
        self.versions
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent version table allocation failed"))?;
        let root = self.backend.append(parent_root, payload)?;
        let version = Version {
            history,
            id,
            parent,
            root,
        };
        self.versions.push(version);
        Ok(version)
    }

    /// Reads an exact byte range of a committed version.
    pub(crate) fn read(
        &self,
        version: Version,
        offset: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), HistoryError> {
        let record = self.committed_version(version)?;
        let range = SequenceRange::new(LogicalLength::new(offset), LogicalLength::new(length))
            .ok_or(HistoryError::Invalid(
                "persistent history read range exceeds u64",
            ))?;
        self.backend.read_range(record.root(), range, output)?;
        Ok(())
    }

    /// Recomputes every reachable node's metadata and commitment.
    pub(crate) fn verify(&self, version: Version) -> Result<(), HistoryError> {
        let record = self.committed_version(version)?;
        self.backend.verify(record.root())?;
        Ok(())
    }

    /// Returns a snapshot of the backend diagnostic work counters.
    pub(crate) fn work_counters(&self) -> SequenceWorkCounters {
        self.backend.work_counters()
    }

    /// Resolves a version identity within an expected history for adapter
    /// reads. The returned root always comes from the committed table, so a
    /// fabricated value fails closed in [`PersistentHistoryStore::read`].
    pub(crate) fn committed_version_for_adapter(
        &self,
        id: VersionId,
        history: HistoryId,
    ) -> Result<Version, HistoryError> {
        let record = self.version_record(id)?;
        if record.history() != history {
            return Err(HistoryError::Invalid(
                "persistent version belongs to a different history",
            ));
        }
        Ok(record)
    }

    fn version_record(&self, id: VersionId) -> Result<Version, HistoryError> {
        let index = usize::try_from(id.id())
            .map_err(|_| HistoryError::Overflow("persistent version identifier exceeds usize"))?;
        let record = self
            .versions
            .get(index)
            .copied()
            .ok_or(HistoryError::Invalid("persistent version is unknown"))?;
        if record.id() != id {
            return Err(HistoryError::Invalid(
                "persistent version disagrees with its table coordinate",
            ));
        }
        Ok(record)
    }

    /// Resolves a caller-held version against the committed table so a
    /// fabricated or stale value fails closed instead of addressing the arena.
    fn committed_version(&self, version: Version) -> Result<Version, HistoryError> {
        let record = self.version_record(version.id())?;
        if record != version {
            return Err(HistoryError::Invalid(
                "persistent version does not match committed history",
            ));
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_full(store: &PersistentHistoryStore, version: Version, len: u64) -> Vec<u8> {
        let mut output = Vec::new();
        store.read(version, 0, len, &mut output).unwrap();
        output
    }

    #[test]
    fn histories_are_isolated_and_versions_link_parents() {
        let mut store = PersistentHistoryStore::new();
        let first = store.create_history().unwrap();
        let second = store.create_history().unwrap();
        assert_ne!(first, second);

        let root_a = store.commit(first, None, b"aaa").unwrap();
        assert_eq!(root_a.history(), first);
        assert_eq!(root_a.parent(), None);
        let child_a = store.commit(first, Some(root_a.id()), b"bbb").unwrap();
        assert_eq!(child_a.parent(), Some(root_a.id()));
        let root_b = store.commit(second, None, b"zzz").unwrap();

        assert_eq!(read_full(&store, root_a, 3), b"aaa");
        assert_eq!(read_full(&store, child_a, 6), b"aaabbb");
        assert_eq!(read_full(&store, root_b, 3), b"zzz");

        // Cross-history grafts fail closed.
        assert_eq!(
            store.commit(second, Some(root_a.id()), b"nope"),
            Err(HistoryError::Invalid(
                "persistent parent version belongs to a different history"
            ))
        );
        // Unknown history and unknown parent fail closed.
        assert_eq!(
            store.commit(HistoryId(999), None, b"nope"),
            Err(HistoryError::Invalid(
                "persistent commit targets an unknown history"
            ))
        );
        assert_eq!(
            store.commit(first, Some(VersionId(999)), b"nope"),
            Err(HistoryError::Invalid("persistent version is unknown"))
        );
        // Failures record no versions.
        assert_eq!(store.versions.len(), 3);
    }

    #[test]
    fn sibling_versions_share_parent_byte_exact() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let parent = store.commit(history, None, b"parent").unwrap();
        let left = store.commit(history, Some(parent.id()), b"-left").unwrap();
        let right = store.commit(history, Some(parent.id()), b"-right").unwrap();
        assert_eq!(left.parent(), Some(parent.id()));
        assert_eq!(right.parent(), Some(parent.id()));
        assert_eq!(read_full(&store, parent, 6), b"parent");
        assert_eq!(read_full(&store, left, 11), b"parent-left");
        assert_eq!(read_full(&store, right, 12), b"parent-right");
        store.verify(parent).unwrap();
        store.verify(left).unwrap();
        store.verify(right).unwrap();
    }

    #[test]
    fn fabricated_versions_fail_closed_on_read_and_verify() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let version = store.commit(history, None, b"data").unwrap();

        let wrong_id = Version {
            history,
            id: VersionId(999),
            parent: None,
            root: version.root(),
        };
        let mut output = Vec::new();
        assert_eq!(
            store.read(wrong_id, 0, 4, &mut output),
            Err(HistoryError::Invalid("persistent version is unknown"))
        );
        assert_eq!(
            store.verify(wrong_id),
            Err(HistoryError::Invalid("persistent version is unknown"))
        );

        let tampered = Version {
            history,
            id: version.id(),
            parent: version.parent(),
            root: PersistentRoot::balanced_v2(0, LogicalLength::new(999)),
        };
        assert_eq!(
            store.read(tampered, 0, 4, &mut output),
            Err(HistoryError::Invalid(
                "persistent version does not match committed history"
            ))
        );
        assert_eq!(
            store.verify(tampered),
            Err(HistoryError::Invalid(
                "persistent version does not match committed history"
            ))
        );
        assert!(output.is_empty());
    }

    #[test]
    fn empty_commit_payload_is_rejected_without_recording() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        assert!(store.commit(history, None, b"").is_err());
        assert_eq!(store.versions.len(), 0);
    }

    #[test]
    fn history_counters_observe_backend_work() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let before = store.work_counters();
        let version = store.commit(history, None, b"payload").unwrap();
        let after_commit = store.work_counters();
        assert_eq!(
            after_commit.payload_bytes_written - before.payload_bytes_written,
            7
        );
        assert!(after_commit.nodes_allocated > before.nodes_allocated);
        // Root creation resolves no parent and copies no spine.
        assert_eq!(after_commit.nodes_inspected, before.nodes_inspected);

        let child = store.commit(history, Some(version.id()), b"more").unwrap();
        let after_child = store.work_counters();
        assert!(after_child.nodes_inspected > after_commit.nodes_inspected);
        assert_eq!(
            after_child.payload_bytes_written - after_commit.payload_bytes_written,
            4
        );

        let mut output = Vec::new();
        store.read(child, 0, 11, &mut output).unwrap();
        let after_read = store.work_counters();
        assert_eq!(output, b"payloadmore");
        assert!(after_read.nodes_read > after_child.nodes_read);
        assert_eq!(
            after_read.payload_bytes_read - after_child.payload_bytes_read,
            11
        );
    }
}
