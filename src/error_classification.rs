//! Stable failure classification for checkpoint-store callers.
//!
//! `CheckpointStoreError` predates the production-resilience work and is a
//! public enum. Adding variants directly would break downstream exhaustive
//! matches, so callers get a separate non-exhaustive classification API while
//! the concrete error remains backward compatible.

use crate::checkpoint_store::CheckpointStoreError;
use serde_json::error::Category as JsonCategory;
use std::error::Error;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

/// Stable behavioral class for a checkpoint-store failure.
///
/// Callers should make retry/repair decisions from this value rather than by
/// parsing diagnostic strings or exhaustively matching the legacy concrete
/// error enum.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStoreFailureKind {
    /// Persisted authoritative bytes are malformed or internally inconsistent.
    Corruption,
    /// Persisted bytes use a format/version this build cannot interpret.
    UnsupportedFormat,
    /// A request identity was reused for a different logical operation.
    RequestConflict,
    /// The requested checkpoint identity was durably deleted.
    Deleted,
    /// The requested live object no longer exists or is otherwise stale.
    Stale,
    /// Another owner currently holds the required exclusive resource.
    LockBusy,
    /// A configured or representable resource/capacity limit was exceeded.
    Capacity,
    /// An I/O operation definitely failed without an indeterminate commit result.
    Io,
    /// Durable commit may have succeeded even though the caller did not receive success.
    DurabilityIndeterminate,
    /// The writer observed a partial/mutating failure and must reopen/recover before reuse.
    RecoveryRequired,
    /// The requested operation violates a documented lifecycle precondition.
    Precondition,
    /// Legacy error site has not yet been migrated to a precise class.
    LegacyUnclassified,
}

/// Durability operation whose failure can leave commit outcome indeterminate.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityOperation {
    /// Flush after a complete WAL record has been written.
    WalFlush,
    /// Data-only durability barrier for a complete WAL record.
    WalSyncData,
    /// Full file durability barrier used by immutable/publication artifacts.
    FileSyncAll,
    /// Atomic publication rename after the staged file is durable.
    Rename,
    /// Parent-directory durability barrier after publication rename.
    DirectorySync,
}

impl fmt::Display for DurabilityOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::WalFlush => "wal-flush",
            Self::WalSyncData => "wal-sync-data",
            Self::FileSyncAll => "file-sync-all",
            Self::Rename => "rename",
            Self::DirectorySync => "directory-sync",
        };
        formatter.write_str(name)
    }
}

/// Typed context retained when durable commit may have happened before an I/O
/// error was observed.
#[derive(Debug)]
pub struct DurabilityIndeterminate {
    operation: DurabilityOperation,
    path: PathBuf,
    source: io::Error,
}

impl DurabilityIndeterminate {
    fn new(operation: DurabilityOperation, path: PathBuf, source: io::Error) -> Self {
        Self {
            operation,
            path,
            source,
        }
    }

    /// Returns the durability stage whose outcome is indeterminate.
    #[must_use]
    pub const fn operation(&self) -> DurabilityOperation {
        self.operation
    }

    /// Returns the filesystem object involved in the durability operation.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the original operating-system I/O error.
    #[must_use]
    pub const fn source_error(&self) -> &io::Error {
        &self.source
    }
}

impl fmt::Display for DurabilityIndeterminate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} outcome is indeterminate for {}: {}",
            self.operation,
            self.path.display(),
            self.source
        )
    }
}

impl Error for DurabilityIndeterminate {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Typed context returned once a mutable writer can no longer safely continue
/// without reconstructing authority from disk.
#[derive(Debug)]
pub struct RecoveryRequired {
    path: PathBuf,
    source: Option<io::Error>,
}

impl RecoveryRequired {
    fn new(path: PathBuf, source: Option<io::Error>) -> Self {
        Self { path, source }
    }

    /// Returns the WAL/filesystem object that must be recovered before reuse.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the original I/O error when the recovery requirement was caused
    /// directly by a failed mutating syscall.
    #[must_use]
    pub fn source_error(&self) -> Option<&io::Error> {
        self.source.as_ref()
    }
}

impl fmt::Display for RecoveryRequired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(source) => write!(
                formatter,
                "writer requires reopen/recovery for {} after I/O failure: {}",
                self.path.display(),
                source
            ),
            None => write!(
                formatter,
                "writer requires reopen/recovery for {} before further mutation",
                self.path.display()
            ),
        }
    }
}

impl Error for RecoveryRequired {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

impl CheckpointStoreError {
    /// Returns the stable behavioral class for this failure.
    ///
    /// Legacy `Format(String)` sites intentionally remain
    /// [`CheckpointStoreFailureKind::LegacyUnclassified`] until each call site
    /// is migrated with enough context to distinguish corruption, unsupported
    /// format, bad input, and capacity errors without string heuristics.
    #[must_use]
    pub fn failure_kind(&self) -> CheckpointStoreFailureKind {
        match self {
            Self::Io(error) if embedded_durability_indeterminate(error).is_some() => {
                CheckpointStoreFailureKind::DurabilityIndeterminate
            }
            Self::Io(error) if embedded_recovery_required(error).is_some() => {
                CheckpointStoreFailureKind::RecoveryRequired
            }
            Self::Io(_) => CheckpointStoreFailureKind::Io,
            Self::Json(error) => match error.classify() {
                JsonCategory::Io => CheckpointStoreFailureKind::Io,
                JsonCategory::Syntax | JsonCategory::Data | JsonCategory::Eof => {
                    CheckpointStoreFailureKind::Corruption
                }
            },
            Self::WriterAlreadyOpen | Self::ReclaimWorkerAlreadyRunning => {
                CheckpointStoreFailureKind::LockBusy
            }
            Self::RequestIdConflict => CheckpointStoreFailureKind::RequestConflict,
            Self::CheckpointNotFound => CheckpointStoreFailureKind::Stale,
            Self::CheckpointDeleted => CheckpointStoreFailureKind::Deleted,
            Self::PruneRequiresSealedStore | Self::PruneRequiresEagerRecovery => {
                CheckpointStoreFailureKind::Precondition
            }
            Self::Format(_) => CheckpointStoreFailureKind::LegacyUnclassified,
        }
    }

    /// Returns durability-indeterminate context when this error represents an
    /// operation whose commit result must be resolved by reopen/recovery.
    #[must_use]
    pub fn durability_indeterminate(&self) -> Option<&DurabilityIndeterminate> {
        match self {
            Self::Io(error) => embedded_durability_indeterminate(error),
            _ => None,
        }
    }

    /// Returns recovery-required context when this writer must not perform
    /// further mutation until the store is reopened and recovered.
    #[must_use]
    pub fn recovery_required(&self) -> Option<&RecoveryRequired> {
        match self {
            Self::Io(error) => embedded_recovery_required(error),
            _ => None,
        }
    }
}

fn embedded_durability_indeterminate(error: &io::Error) -> Option<&DurabilityIndeterminate> {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<DurabilityIndeterminate>())
}

fn embedded_recovery_required(error: &io::Error) -> Option<&RecoveryRequired> {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<RecoveryRequired>())
}

/// Wraps an I/O failure whose commit outcome cannot safely be treated as a
/// definite abort.
pub(crate) fn durability_indeterminate_error(
    operation: DurabilityOperation,
    path: &Path,
    source: io::Error,
) -> CheckpointStoreError {
    let kind = source.kind();
    let context = DurabilityIndeterminate::new(operation, path.to_path_buf(), source);
    CheckpointStoreError::Io(io::Error::new(kind, context))
}

/// Wraps a failure after mutable WAL bytes may have changed, or reports an
/// already-poisoned writer. Callers must reopen/recover before another write.
pub(crate) fn recovery_required_error(
    path: &Path,
    source: Option<io::Error>,
) -> CheckpointStoreError {
    let kind = source
        .as_ref()
        .map_or(io::ErrorKind::Other, io::Error::kind);
    let context = RecoveryRequired::new(path.to_path_buf(), source);
    CheckpointStoreError::Io(io::Error::new(kind, context))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn existing_semantic_errors_have_stable_behavioral_classes() {
        assert_eq!(
            CheckpointStoreError::RequestIdConflict.failure_kind(),
            CheckpointStoreFailureKind::RequestConflict
        );
        assert_eq!(
            CheckpointStoreError::CheckpointDeleted.failure_kind(),
            CheckpointStoreFailureKind::Deleted
        );
        assert_eq!(
            CheckpointStoreError::CheckpointNotFound.failure_kind(),
            CheckpointStoreFailureKind::Stale
        );
        assert_eq!(
            CheckpointStoreError::WriterAlreadyOpen.failure_kind(),
            CheckpointStoreFailureKind::LockBusy
        );
        assert_eq!(
            CheckpointStoreError::PruneRequiresSealedStore.failure_kind(),
            CheckpointStoreFailureKind::Precondition
        );
        assert_eq!(
            CheckpointStoreError::Format("legacy ambiguous site".to_owned()).failure_kind(),
            CheckpointStoreFailureKind::LegacyUnclassified
        );
    }

    #[test]
    fn malformed_json_is_classified_as_corruption_without_string_matching() {
        let json_error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let error = CheckpointStoreError::Json(json_error);
        assert_eq!(error.failure_kind(), CheckpointStoreFailureKind::Corruption);
    }

    #[test]
    fn durability_indeterminate_retains_operation_path_and_os_error() {
        let source = io::Error::from_raw_os_error(28);
        let error = durability_indeterminate_error(
            DurabilityOperation::WalSyncData,
            Path::new("/tmp/hot.wal"),
            source,
        );
        assert_eq!(
            error.failure_kind(),
            CheckpointStoreFailureKind::DurabilityIndeterminate
        );
        let context = error.durability_indeterminate().unwrap();
        assert_eq!(context.operation(), DurabilityOperation::WalSyncData);
        assert_eq!(context.path(), Path::new("/tmp/hot.wal"));
        assert_eq!(context.source_error().raw_os_error(), Some(28));
    }

    #[test]
    fn recovery_required_retains_path_and_optional_source() {
        let error = recovery_required_error(
            Path::new("/tmp/hot.wal"),
            Some(io::Error::from_raw_os_error(5)),
        );
        assert_eq!(
            error.failure_kind(),
            CheckpointStoreFailureKind::RecoveryRequired
        );
        let context = error.recovery_required().unwrap();
        assert_eq!(context.path(), Path::new("/tmp/hot.wal"));
        assert_eq!(
            context.source_error().and_then(io::Error::raw_os_error),
            Some(5)
        );

        let poisoned = recovery_required_error(Path::new("/tmp/hot.wal"), None);
        assert_eq!(
            poisoned.failure_kind(),
            CheckpointStoreFailureKind::RecoveryRequired
        );
        assert!(poisoned
            .recovery_required()
            .unwrap()
            .source_error()
            .is_none());
    }

    #[test]
    fn ordinary_io_error_is_not_misclassified_as_indeterminate_or_recovery_required() {
        let error = CheckpointStoreError::Io(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "permission denied",
        ));
        assert_eq!(error.failure_kind(), CheckpointStoreFailureKind::Io);
        assert!(error.durability_indeterminate().is_none());
        assert!(error.recovery_required().is_none());
    }
}
