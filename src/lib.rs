#![forbid(unsafe_code)]
#![allow(clippy::too_many_arguments)]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::unwrap_used
    )
)]

//! Durable branch-aware checkpoint history for stateful systems.
//!
//! This focused crate is extracted from `tulya-engine`. It contains the
//! production hot-WAL, immutable sealed-history, exact historical-read,
//! verification, reclaim and process-locking implementation without the
//! surrounding research modules.

mod checkpoint_store;
mod error_classification;
#[allow(dead_code)]
mod format_authority;
mod hot_wal_commit;
mod persistent_sequence;

// The domain-neutral history core is staged: its CheckpointStore client lands
// in the next slice, which removes this allowance.
#[allow(dead_code)]
mod persistent_history;

pub mod admin;
pub mod format;

pub use checkpoint_store::{
    fsck, CheckpointInfo, CheckpointStore, CheckpointStoreConfig, CheckpointStoreError,
    CheckpointStoreRecoveryMode, FsckReport, HotWalAppendReport, StoreId,
};
pub use error_classification::{
    CheckpointStoreFailureKind, DurabilityIndeterminate, DurabilityOperation, RecoveryRequired,
};
