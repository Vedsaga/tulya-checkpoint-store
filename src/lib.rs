#![allow(clippy::too_many_arguments)]

//! Durable branch-aware checkpoint history for stateful systems.
//!
//! This focused crate is extracted from `tulya-engine`. It contains the
//! production hot-WAL, immutable sealed-history, exact historical-read,
//! verification, reclaim and process-locking implementation without the
//! surrounding research modules.

mod checkpoint_store;
#[allow(dead_code)]
mod format_authority;
mod persistent_sequence;

pub mod admin;
pub mod format;

pub use checkpoint_store::{
    fsck, CheckpointInfo, CheckpointStore, CheckpointStoreConfig, CheckpointStoreError,
    CheckpointStoreRecoveryMode, FsckReport, HotWalAppendReport, StoreId,
};
