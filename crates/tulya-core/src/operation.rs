//! Durability operation taxonomy shared by every Tulya adapter.
//!
//! Moved verbatim from the checkpoint crate's failure classification: the
//! variants describe physical durability barriers, never checkpoint
//! semantics, so the core owns the single definition and adapters reuse it.

use std::fmt;

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
