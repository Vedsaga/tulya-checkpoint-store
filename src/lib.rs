#![allow(clippy::too_many_arguments)]

//! Durable branch-aware checkpoint history for stateful systems.
//!
//! This focused crate is extracted from `tulya-engine`. It contains the
//! production hot-WAL, immutable sealed-history, exact historical-read,
//! verification, reclaim and process-locking implementation without the
//! surrounding research modules.

mod checkpoint_store;
// Staged production-readiness seam. The next storage task adapts Format v1
// reads/appends behind this contract and removes this temporary dead-code
// allowance without changing released v1 bytes.
#[allow(dead_code)]
mod persistent_sequence;

pub mod admin;
pub mod format;

pub use checkpoint_store::{
    fsck, CheckpointInfo, CheckpointStore, CheckpointStoreConfig, CheckpointStoreError,
    CheckpointStoreRecoveryMode, FsckReport, HotWalAppendReport, StoreId,
};
