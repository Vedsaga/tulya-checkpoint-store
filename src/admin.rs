//! Explicit maintenance and physical-lifecycle APIs.
//!
//! Most applications only need the types re-exported at the crate root. The
//! lower-level WAL, sealing, lazy-reader, pruning, and reclaim controls live
//! here so the primary API remains small.

pub use crate::checkpoint_store::{
    BoundedWalAppendReport, BoundedWalLifecyclePolicy, CheckpointReclaimWorker,
    CheckpointReclaimWorkerStats, HotWalAppendReport, LazyCheckpointStore, LazyReadMetrics,
    PruneReport, SealReport, StoreStorage, VerificationReport,
};
