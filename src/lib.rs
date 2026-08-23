//! Durable branch-aware checkpoint storage for append-only agent state.
//!
//! This focused crate is extracted from `tulya-engine`. It contains the
//! production hot-WAL, immutable sealed-history, exact historical-read,
//! verification, reclaim and process-locking implementation without the
//! surrounding research modules.

mod checkpoint_store {
    include!("checkpoint_store_impl_body.rs");

    impl PartialEq for Geometry {
        fn eq(&self, other: &Self) -> bool {
            self.byte_len == other.byte_len
                && self.node_count == other.node_count
                && self.wide_count == other.wide_count
                && self.version_count == other.version_count
                && self.checkpoint_count == other.checkpoint_count
        }
    }

    impl Eq for Geometry {}
}

pub use checkpoint_store::{
    BoundedWalAppendReport, BoundedWalLifecyclePolicy, CheckpointInfo, CheckpointReclaimWorker,
    CheckpointReclaimWorkerStats, CheckpointStore, CheckpointStoreAppendOutcome,
    CheckpointStoreConfig, CheckpointStoreError, CheckpointStoreRecoveryMode, HotWalAppendReport,
    LazyCheckpointStore, LazyReadMetrics, PruneReport, SealReport, StoreId, StoreStorage,
    VerificationReport,
};
