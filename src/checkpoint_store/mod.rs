// Production-facing durable checkpoint store.
//
// This module composes the frozen T2W1 transaction WAL with the proven
// T3STRS02 structured sealed-segment format. The foreground durability path
// keeps one preinitialized WAL reserve and one `sync_data` barrier per
// acknowledged transaction. Sealing publishes immutable segment + route
// metadata before recycling the represented WAL prefix into a fresh writable
// reserve.
//
// The first implementation deliberately materializes sealed streams into
// memory on reopen. That keeps recovery simple and exact while the physical
// lifecycle is productized; recovery optimization is a separate concern.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::{FileExt as PositionalFileExt, MetadataExt};
#[cfg(windows)]
use std::os::windows::fs::FileExt as PositionalFileExt;

use fs4::FileExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use xxhash_rust::xxh3::{xxh3_64, Xxh3};

use crate::error_classification::{
    durability_indeterminate_error, recovery_required_after_commit_error, DurabilityOperation,
};
use crate::hot_wal_commit::{FileHotWalCommitIo, HotWalCommitter};

mod storage_format;
use storage_format::*;

pub(crate) mod fault_injection;
use fault_injection::*;

mod fsck;
pub use fsck::{fsck, FsckReport};

/// Errors returned by the production checkpoint-store lifecycle.
#[derive(Debug, Error)]
pub enum CheckpointStoreError {
    /// Filesystem or durability operation failed.
    #[error("checkpoint-store I/O error: {0}")]
    Io(#[from] io::Error),
    /// Manifest JSON could not be encoded or decoded.
    #[error("checkpoint-store manifest JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Persisted bytes violate the public format or topology contract.
    #[error("checkpoint-store format error: {0}")]
    Format(String),
    /// Another process or store handle owns the writable store lock.
    #[error("checkpoint store is already open for writing")]
    WriterAlreadyOpen,
    /// A request key was reused for different transaction bytes.
    #[error("checkpoint request id conflicts with a previously committed operation")]
    RequestIdConflict,
    /// A checkpoint key does not exist in the live checkpoint set.
    #[error("checkpoint key was not found")]
    CheckpointNotFound,
    /// A checkpoint key was durably deleted by a prune operation.
    #[error("checkpoint key was deleted")]
    CheckpointDeleted,
    /// A prune operation requires all committed WAL bytes to be sealed first.
    #[error("checkpoint subtree prune requires a fully sealed store")]
    PruneRequiresSealedStore,
    /// Lazy recovery keeps sealed payloads out of the eager compaction path.
    #[error("checkpoint subtree prune requires eager recovery mode")]
    PruneRequiresEagerRecovery,
    /// A second background reclaim worker is already active for the store.
    #[error("checkpoint reclaim worker is already running")]
    ReclaimWorkerAlreadyRunning,
}

fn format_error(message: impl Into<String>) -> CheckpointStoreError {
    CheckpointStoreError::Format(message.into())
}

/// Opaque persistent identity for one logical checkpoint store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoreId([u8; 16]);

impl StoreId {
    /// Returns the raw 128-bit identity bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Returns the canonical 32-character lowercase hexadecimal encoding.
    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(32);
        for byte in self.0 {
            output.push(hex_digit(byte >> 4));
            output.push(hex_digit(byte & 0x0f));
        }
        output
    }

    /// Parses the canonical 32-character hexadecimal encoding.
    pub fn from_hex(value: &str) -> Result<Self, CheckpointStoreError> {
        if value.len() != 32 {
            return Err(format_error(
                "StoreId must contain exactly 32 hex characters",
            ));
        }
        let bytes = value.as_bytes();
        let mut output = [0u8; 16];
        for (index, slot) in output.iter_mut().enumerate() {
            let high = hex_value(
                *bytes
                    .get(index * 2)
                    .ok_or_else(|| format_error("StoreId hex encoding is truncated"))?,
            )?;
            let low = hex_value(
                *bytes
                    .get(index * 2 + 1)
                    .ok_or_else(|| format_error("StoreId hex encoding is truncated"))?,
            )?;
            *slot = (high << 4) | low;
        }
        Ok(Self(output))
    }

    fn generate() -> Result<Self, CheckpointStoreError> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            io::Error::other(format!("operating-system random source failed: {error}"))
        })?;
        Ok(Self(bytes))
    }
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'a' + (value - 10)),
        _ => '?',
    }
}

fn hex_value(value: u8) -> Result<u8, CheckpointStoreError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(format_error("StoreId must use lowercase hexadecimal")),
    }
}

/// Recovery materialization ownership policy for the sealed prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStoreRecoveryMode {
    /// Preserve the historical eager materializer's cloned payload behavior.
    ClonePayload,
    /// Transfer the decoded payload stream into the recovered state.
    ReusePayload,
    /// Keep the sealed prefix on the bounded lazy reader and materialize only
    /// the writable hot suffix into the process-owned overlay.
    Lazy,
}

/// Frozen physical defaults for the writable checkpoint service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointStoreConfig {
    /// Bytes in one fully materialized hot WAL reserve segment.
    pub wal_segment_bytes: u64,
    /// Chunk size used while zero-initializing a new reserve.
    pub preinit_chunk_bytes: usize,
    /// Raw block size for immutable sealed streams.
    pub sealed_block_size: u32,
    /// Zstd level used by the frozen sealed representation.
    pub zstd_level: i32,
    /// Sealed-prefix recovery ownership policy.
    pub recovery_mode: CheckpointStoreRecoveryMode,
}

/// Explicit sequential policy for keeping the logical hot WAL bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedWalLifecyclePolicy {
    /// Seal the committed hot prefix before a transaction would cross this size.
    pub soft_logical_bytes: u64,
    /// Reject a transaction larger than this size before changing the WAL.
    pub hard_logical_bytes: u64,
}

impl BoundedWalLifecyclePolicy {
    /// Creates a policy with a positive soft threshold no larger than the hard limit.
    pub fn new(
        soft_logical_bytes: u64,
        hard_logical_bytes: u64,
    ) -> Result<Self, CheckpointStoreError> {
        let policy = Self {
            soft_logical_bytes,
            hard_logical_bytes,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(self) -> Result<(), CheckpointStoreError> {
        if self.soft_logical_bytes == 0 || self.hard_logical_bytes == 0 {
            return Err(format_error("bounded WAL limits must be positive"));
        }
        if self.soft_logical_bytes > self.hard_logical_bytes {
            return Err(format_error("bounded WAL soft limit exceeds hard limit"));
        }
        Ok(())
    }
}

impl Default for CheckpointStoreConfig {
    fn default() -> Self {
        Self {
            wal_segment_bytes: 32 * 1024 * 1024,
            preinit_chunk_bytes: 1024 * 1024,
            sealed_block_size: 64 * 1024,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
        }
    }
}

struct WriterLock {
    _file: File,
}

struct ReaderReclaimLease {
    _file: File,
}

impl ReaderReclaimLease {
    fn acquire_shared(dir: &Path) -> Result<Self, CheckpointStoreError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join(READER_RECLAIM_LOCK_FILE))?;
        fs4::FileExt::lock_shared(&file)?;
        Ok(Self { _file: file })
    }
}

struct ReaderReclaimGuard {
    _file: File,
}

impl ReaderReclaimGuard {
    fn try_acquire_exclusive(dir: &Path) -> Result<Option<Self>, CheckpointStoreError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join(READER_RECLAIM_LOCK_FILE))?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.raw_os_error() == fs4::lock_contended_error().raw_os_error() =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
}

struct ReclaimWorkerLock {
    _file: File,
}

impl ReclaimWorkerLock {
    fn acquire(dir: &Path) -> Result<Self, CheckpointStoreError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join(RECLAIM_WORKER_LOCK_FILE))?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { _file: file }),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.raw_os_error() == fs4::lock_contended_error().raw_os_error() =>
            {
                Err(CheckpointStoreError::ReclaimWorkerAlreadyRunning)
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Observable counters for one background reclaim worker.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckpointReclaimWorkerStats {
    /// Number of manifest/reclaim polls attempted by the worker.
    pub poll_count: u64,
    /// Polls that acquired the exclusive reclaim gate.
    pub completed_polls: u64,
    /// Polls deferred because a reader lease was active.
    pub deferred_polls: u64,
    /// Filesystem-allocated bytes removed across completed polls.
    pub reclaimed_allocated_bytes: u64,
    /// Number of polls stopped by an I/O or format error.
    pub error_stops: u64,
    /// First worker error, if the worker stopped on an error.
    pub last_error: Option<String>,
}

/// Cooperative background maintenance for obsolete sealed-generation files.
///
/// The worker never publishes manifests, seals WAL bytes, or mutates logical
/// checkpoint state. It only retries the already-authoritative manifest's
/// deferred generation cleanup under the reader/reclaimer gate.
pub struct CheckpointReclaimWorker {
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<CheckpointReclaimWorkerStats>>,
    join: Option<JoinHandle<()>>,
}

impl CheckpointReclaimWorker {
    /// Starts one reclaim worker for a store directory.
    pub fn start(dir: impl AsRef<Path>, interval: Duration) -> Result<Self, CheckpointStoreError> {
        if interval.is_zero() {
            return Err(format_error("reclaim worker interval must be positive"));
        }
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let worker_lock = ReclaimWorkerLock::acquire(&dir)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(Mutex::new(CheckpointReclaimWorkerStats::default()));
        let thread_stop = Arc::clone(&stop);
        let thread_stats = Arc::clone(&stats);
        let join = thread::Builder::new()
            .name("tulya-reclaim-worker".to_owned())
            .spawn(move || {
                let _worker_lock = worker_lock;
                while !thread_stop.load(Ordering::SeqCst) {
                    if !reclaim_worker_poll(&dir, &thread_stats) {
                        break;
                    }
                    thread::sleep(interval);
                }
            })
            .map_err(CheckpointStoreError::Io)?;
        Ok(Self {
            stop,
            stats,
            join: Some(join),
        })
    }

    /// Returns a snapshot of worker progress and the first terminal error.
    #[must_use]
    pub fn snapshot(&self) -> CheckpointReclaimWorkerStats {
        self.stats
            .lock()
            .map(|stats| stats.clone())
            .unwrap_or_default()
    }

    /// Stops and joins the worker, returning its final counters.
    pub fn stop(mut self) -> Result<CheckpointReclaimWorkerStats, CheckpointStoreError> {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| format_error("reclaim worker thread panicked"))?;
        }
        Ok(self.snapshot())
    }
}

impl Drop for CheckpointReclaimWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl WriterLock {
    fn acquire(dir: &Path) -> Result<Self, CheckpointStoreError> {
        let path = dir.join(WRITER_LOCK_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { _file: file }),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.raw_os_error() == fs4::lock_contended_error().raw_os_error() =>
            {
                let _ = fs4::FileExt::unlock(&file);
                Err(CheckpointStoreError::WriterAlreadyOpen)
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Result of one foreground WAL append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HotWalAppendReport {
    /// Logical bytes appended by the acknowledged transaction.
    pub transaction_bytes: u64,
    /// Committed logical tail after the append.
    pub logical_tail_bytes: u64,
    /// Current physical WAL capacity.
    pub capacity_bytes: u64,
    /// Nanoseconds spent writing transaction bytes before the durability call.
    pub write_ns: u128,
    /// Nanoseconds spent in the single `sync_data` durability barrier.
    pub sync_data_ns: u128,
}

/// Result of one append through the explicit bounded WAL lifecycle policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedWalAppendReport {
    /// Durable append result for the new transaction.
    pub append: HotWalAppendReport,
    /// Seal performed before the append, if the soft threshold was crossed.
    pub automatic_seal: Option<SealReport>,
}

/// Result of an idempotent checkpoint append.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointStoreAppendOutcome {
    /// A new request/checkpoint transaction was durably appended.
    Appended(HotWalAppendReport),
    /// The exact request was already committed; no WAL bytes were appended.
    AlreadyCommitted,
}

/// Complete allocated/file-length accounting for a checkpoint-store directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreStorage {
    /// Sum of logical file lengths for regular files in the directory.
    pub file_length_bytes: u64,
    /// Sum of allocated filesystem bytes for regular files in the directory.
    pub allocated_bytes: u64,
    /// Number of regular files counted.
    pub file_count: u64,
}

/// Public checkpoint metadata retained by the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// Stable zero-based checkpoint ordinal in commit order.
    pub ordinal: u32,
    /// Logical thread identifier.
    pub thread_id: String,
    /// Checkpoint number supplied by the adapter.
    pub checkpoint_no: u32,
    /// Stable checkpoint identifier.
    pub checkpoint_id: String,
    /// Parent checkpoint identifier within the same thread, if present.
    pub parent_checkpoint_id: Option<String>,
    /// Version containing the identity channel.
    pub identity_version: u32,
    /// Version containing the concatenated message channel, if present.
    pub messages_version: Option<u32>,
    /// Version containing the result channel, if present.
    pub result_version: Option<u32>,
    /// Canonical reconstructed state length.
    pub logical_state_len: u64,
    /// XXH3-64 of the canonical reconstructed state.
    pub state_hash: u64,
}

#[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
#[derive(Debug, Clone, Copy)]
pub(crate) struct IdentityLeafRef {
    pub(crate) node_id: u64,
    pub(crate) logical_start: u64,
    pub(crate) logical_len: u64,
}

#[allow(dead_code)] // Reserved for a future zero-copy repository adapter feature.
pub(crate) enum IdentityLeafSource<'a> {
    New(&'a [u8]),
    Existing(IdentityLeafRef),
}

/// Physical and logical accounting returned by a subtree-prune operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PruneReport {
    /// Manifest generation published by the replacement live generation.
    pub generation: u64,
    /// Number of checkpoints removed by the selected subtree.
    pub deleted_checkpoint_count: u64,
    /// Number of checkpoints retained after pruning.
    pub retained_checkpoint_count: u64,
    /// Bytes represented by the replacement generation before cleanup.
    pub rewritten_bytes: u64,
    /// Storage before the replacement generation was staged.
    pub before: StoreStorage,
    /// Storage while old and replacement generations coexist.
    pub coexistence: StoreStorage,
    /// Storage after unreferenced old files were removed.
    pub reclaimed: StoreStorage,
}

/// Summary of one seal + manifest publication + WAL recycle cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealReport {
    /// Newly published immutable generation.
    pub generation: u64,
    /// Total sealed checkpoint count after publication.
    pub checkpoint_count: u64,
    /// Number of hot logical WAL bytes represented by this generation.
    pub newly_sealed_wal_bytes: u64,
    /// Remaining committed logical suffix after recycle.
    pub hot_suffix_logical_bytes: u64,
    /// Storage before segment creation.
    pub before: StoreStorage,
    /// Storage after segment/route/manifest publication but before WAL recycle.
    pub coexistence: StoreStorage,
    /// Sampled storage while old and replacement WAL reserves coexist.
    pub recycle_peak: StoreStorage,
    /// Storage after WAL recycle finishes.
    pub reclaimed: StoreStorage,
}

/// Verification result for all currently committed checkpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerificationReport {
    /// Number of checkpoints reconstructed and checked.
    pub checkpoint_count: u64,
    /// Number of length/hash mismatches.
    pub failures: u64,
}

/// Library-owned preinitialized WAL durability primitive.
///
/// The caller supplies the already-validated logical tail. Appends overwrite
/// preinitialized blocks and call exactly one `sync_data` after transaction
/// bytes are written. Segment extension is fully initialized and `sync_all`'d
/// before the transaction is written.
pub struct HotWal {
    path: PathBuf,
    file: File,
    logical_tail: u64,
    config: CheckpointStoreConfig,
    committer: HotWalCommitter,
}

impl HotWal {
    /// Opens or creates a writable preinitialized WAL at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created, initialized, synced, or
    /// positioned at `logical_tail`.
    pub fn open_at(
        path: impl AsRef<Path>,
        logical_tail: u64,
        config: CheckpointStoreConfig,
    ) -> Result<Self, CheckpointStoreError> {
        validate_config(config)?;
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let created = !path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let current_len = file.metadata()?.len();
        if current_len < logical_tail {
            return Err(format_error(
                "physical WAL shorter than committed logical tail",
            ));
        }
        let required_capacity = round_capacity(logical_tail, config.wal_segment_bytes)?;
        if current_len < required_capacity {
            preinitialize_range(
                &mut file,
                current_len,
                required_capacity,
                config.preinit_chunk_bytes,
            )?;
        } else if created {
            file.sync_all()?;
        }
        if created {
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
        }
        file.seek(SeekFrom::Start(logical_tail))?;
        Ok(Self {
            path,
            file,
            logical_tail,
            config,
            committer: HotWalCommitter::default(),
        })
    }

    /// Appends one encoded transaction and makes it durable with `sync_data`.
    ///
    /// # Errors
    ///
    /// Returns an error if capacity initialization, write, or durability sync
    /// fails.
    pub fn append(
        &mut self,
        transaction: &[u8],
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        self.ensure_writable()?;
        let tx_len = u64::try_from(transaction.len())
            .map_err(|_| format_error("transaction length does not fit u64"))?;
        let required_tail = self
            .logical_tail
            .checked_add(tx_len)
            .ok_or_else(|| format_error("WAL logical tail overflow"))?;
        let current_capacity = self.file.metadata()?.len();
        let capacity_bytes = if required_tail > current_capacity {
            let new_capacity = round_capacity(required_tail, self.config.wal_segment_bytes)?;
            if let Err(error) = preinitialize_range(
                &mut self.file,
                current_capacity,
                new_capacity,
                self.config.preinit_chunk_bytes,
            ) {
                return Err(self.recovery_required_after_mutation(error));
            }
            if let Err(source) = self.file.seek(SeekFrom::Start(self.logical_tail)) {
                return Err(self
                    .committer
                    .recovery_required_error(&self.path, Some(source)));
            }
            new_capacity
        } else {
            current_capacity
        };

        let timings = {
            let mut io = FileHotWalCommitIo::new(&mut self.file)?;
            self.committer.commit(&self.path, &mut io, transaction)?
        };
        self.logical_tail = required_tail;
        Ok(HotWalAppendReport {
            transaction_bytes: tx_len,
            logical_tail_bytes: required_tail,
            capacity_bytes,
            write_ns: timings.write_ns,
            sync_data_ns: timings.sync_data_ns,
        })
    }

    /// Returns the committed logical tail inside the physical reserve.
    #[must_use]
    pub fn logical_tail(&self) -> u64 {
        self.logical_tail
    }

    /// Returns the current materialized physical capacity.
    ///
    /// # Errors
    ///
    /// Returns an error if file metadata cannot be read.
    pub fn capacity(&self) -> Result<u64, CheckpointStoreError> {
        Ok(self.file.metadata()?.len())
    }

    fn ensure_writable(&self) -> Result<(), CheckpointStoreError> {
        self.committer.ensure_writable(&self.path)
    }

    fn recovery_required_after_mutation(
        &mut self,
        error: CheckpointStoreError,
    ) -> CheckpointStoreError {
        match error {
            CheckpointStoreError::Io(source) => self
                .committer
                .recovery_required_error(&self.path, Some(source)),
            other => {
                self.committer.mark_recovery_required();
                other
            }
        }
    }

    fn recovery_required_after_committed_authority(
        &mut self,
        error: CheckpointStoreError,
    ) -> CheckpointStoreError {
        self.committer.mark_recovery_required();
        let source = match error {
            CheckpointStoreError::Io(source) => Some(source),
            other => Some(io::Error::other(other.to_string())),
        };
        recovery_required_after_commit_error(&self.path, source)
    }

    fn replace_after_recycle(&mut self, new_tail: u64) -> Result<(), CheckpointStoreError> {
        self.ensure_writable()?;
        let mut file = match OpenOptions::new().read(true).write(true).open(&self.path) {
            Ok(file) => file,
            Err(source) => {
                self.committer.mark_recovery_required();
                return Err(recovery_required_after_commit_error(
                    &self.path,
                    Some(source),
                ));
            }
        };
        if let Err(source) = file.seek(SeekFrom::Start(new_tail)) {
            self.committer.mark_recovery_required();
            return Err(recovery_required_after_commit_error(
                &self.path,
                Some(source),
            ));
        }
        self.file = file;
        self.logical_tail = new_tail;
        Ok(())
    }

    fn poison(&mut self) {
        self.committer.mark_recovery_required();
    }

    #[cfg(test)]
    fn poison_for_test(&mut self) {
        self.poison();
    }
}

/// Production-facing checkpoint-store lifecycle.
///
/// The store owns the hot durability reserve, public-format recovery, immutable
/// segments/routes/manifest, physical WAL recycle, and exact checkpoint reads.
pub struct CheckpointStore {
    dir: PathBuf,
    config: CheckpointStoreConfig,
    manifest: Manifest,
    store_id: StoreId,
    state: StoreState,
    _writer_lock: WriterLock,
    hot: HotWal,
    lazy_base: Option<RefCell<LazyCheckpointStore>>,
    range_sizes: RefCell<Vec<Option<u64>>>,
}

mod store;

/// Read-only metrics collected by the bounded lazy sealed-state reader.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LazyReadMetrics {
    /// Number of block lookups served from the bounded cache.
    pub cache_hits: u64,
    /// Number of block lookups that required a sealed-file read and decode.
    pub cache_misses: u64,
    /// Encoded bytes read from sealed segment payloads.
    pub encoded_bytes_read: u64,
    /// Raw bytes produced by sealed block decompression.
    pub raw_bytes_decompressed: u64,
    /// Number of block-table entry lookups served from the bounded route cache.
    pub block_entry_cache_hits: u64,
    /// Number of block-table entry lookups that read the sealed index.
    pub block_entry_cache_misses: u64,
    /// Bytes returned by sealed block-table entry reads.
    pub block_entry_bytes_read: u64,
}

/// Read-only candidate for the bounded/lazy sealed-state recovery hypothesis.
///
/// This view is intentionally separate from [`CheckpointStore`]. It opens the
/// fully sealed prefix, keeps route/checkpoint/version metadata and compact
/// route/checkpoint/version metadata resident, and reads node topology and the
/// large payload column through a bounded decoded-block cache. It does not
/// own a WAL and cannot append, seal, or recycle; the production writer remains
/// on the existing `CheckpointStore` path while this candidate is measured.
pub struct LazyCheckpointStore {
    _reader_reclaim_lease: ReaderReclaimLease,
    manifest: Manifest,
    deleted_checkpoints: HashSet<(String, String)>,
    segments: Vec<LazySegment>,
    metadata: LazyMetadata,
    cache: HashMap<(usize, usize), Vec<u8>>,
    cache_order: VecDeque<(usize, usize)>,
    block_entries: HashMap<(usize, usize), BlockEntry>,
    block_entry_order: VecDeque<(usize, usize)>,
    cache_capacity: usize,
    payload_fast: Option<Vec<u8>>,
    metrics: LazyReadMetrics,
}

mod lazy;

mod model;
use model::*;

fn load_lazy_segments(
    dir: &Path,
    manifest: &Manifest,
) -> Result<Vec<LazySegment>, CheckpointStoreError> {
    let mut segments = Vec::with_capacity(manifest.segments.len());
    for segment in &manifest.segments {
        let path = dir.join(&segment.file);
        let index = if std::env::var_os("TULYA_LAZY_EAGER_SEGMENT_INDEX_DIAGNOSTIC").is_some() {
            let full = read_segment_index_file(&path)?;
            LazySegmentIndex {
                header: full.header,
                streams: full.streams,
            }
        } else {
            read_lazy_segment_index_file(&path)?
        };
        if index.header.generation != segment.generation
            || index.header.checkpoint_start_count != segment.checkpoint_start_count
            || index.header.checkpoint_end_count != segment.checkpoint_end_count
            || index.header.version_start_count != segment.version_start_count
            || index.header.version_end_count != segment.version_end_count
            || index.header.block_size != segment.block_size
            || index.header.block_count != segment.block_count
            || index.header.index_xxh3_64 != segment.index_xxh3_64
        {
            return Err(format_error("lazy segment index disagrees with manifest"));
        }
        if index.streams.len() != STREAM_NAMES.len() {
            return Err(format_error("lazy segment stream width mismatch"));
        }
        for (stream_id, entry) in index.streams.iter().enumerate() {
            if segment.stream_starts.get(stream_id) != Some(&entry.global_start)
                || segment.stream_ends.get(stream_id) != Some(&entry.global_end)
                || u64::from(entry.first_block) + u64::from(entry.block_count)
                    > u64::from(index.header.block_count)
            {
                return Err(format_error(
                    "lazy segment stream entry disagrees with manifest",
                ));
            }
        }
        segments.push(LazySegment {
            index,
            file: File::open(dir.join(&segment.file))?,
            decompressor: zstd::bulk::Decompressor::new()?,
        });
    }
    Ok(segments)
}

struct HotParse {
    transactions: Vec<ParsedTransaction>,
    logical_tail: u64,
}

fn validate_config(config: CheckpointStoreConfig) -> Result<(), CheckpointStoreError> {
    if config.wal_segment_bytes == 0
        || config.preinit_chunk_bytes == 0
        || config.sealed_block_size == 0
    {
        return Err(format_error("checkpoint-store sizes must be positive"));
    }
    Ok(())
}

fn round_capacity(required: u64, segment: u64) -> Result<u64, CheckpointStoreError> {
    const MIN_RESERVE_BYTES: u64 = 1024 * 1024;

    let floor = required.max(1);
    let initial = segment.min(MIN_RESERVE_BYTES);
    if floor <= segment {
        let mut capacity = initial;
        while capacity < floor {
            let doubled = capacity
                .checked_mul(2)
                .ok_or_else(|| format_error("WAL adaptive capacity overflow"))?;
            if doubled >= segment {
                return Ok(segment);
            }
            capacity = doubled;
        }
        return Ok(capacity);
    }

    let rounded = floor
        .checked_add(segment - 1)
        .ok_or_else(|| format_error("WAL capacity rounding overflow"))?
        / segment;
    rounded
        .checked_mul(segment)
        .ok_or_else(|| format_error("WAL capacity multiplication overflow"))
}

fn preinitialize_range(
    file: &mut File,
    from: u64,
    to: u64,
    chunk_bytes: usize,
) -> Result<(), CheckpointStoreError> {
    if to <= from {
        return Ok(());
    }
    #[cfg(feature = "fault-injection")]
    let fault = configured_wal_io_fault()?;
    file.set_len(to)?;
    #[cfg(feature = "fault-injection")]
    if fault == Some(WalIoFault::ReserveEnospcAfterSetLen) {
        return Err(injected_disk_full_error().into());
    }
    file.seek(SeekFrom::Start(from))?;
    let zeros = vec![0u8; chunk_bytes];
    let mut cursor = from;
    while cursor < to {
        let remaining = to - cursor;
        let write_len = usize::try_from(remaining.min(
            u64::try_from(zeros.len()).map_err(|_| format_error("zero-buffer length overflow"))?,
        ))
        .map_err(|_| format_error("zero-fill write length overflow"))?;
        file.write_all(
            zeros
                .get(..write_len)
                .ok_or_else(|| format_error("zero-fill slice outside buffer"))?,
        )?;
        cursor = cursor
            .checked_add(
                u64::try_from(write_len)
                    .map_err(|_| format_error("zero-fill cursor length overflow"))?,
            )
            .ok_or_else(|| format_error("zero-fill cursor overflow"))?;
    }
    file.flush()?;
    file.sync_all()?;
    Ok(())
}

fn sync_dir(path: &Path) -> Result<(), CheckpointStoreError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn reclaim_unreferenced_generation_files(
    dir: &Path,
    manifest: &Manifest,
) -> Result<bool, CheckpointStoreError> {
    let Some(_reclaim_guard) = ReaderReclaimGuard::try_acquire_exclusive(dir)? else {
        return Ok(false);
    };
    let referenced = manifest
        .segments
        .iter()
        .map(|segment| segment.file.as_str())
        .chain(manifest.routes.iter().map(|route| route.file.as_str()))
        .collect::<HashSet<_>>();
    let mut removed = false;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let generation_artifact = (name.starts_with("structured-g") && name.ends_with(".t3s"))
            || (name.starts_with("route-g") && name.ends_with(".t3r"))
            || (name.starts_with(".structured-g") && name.ends_with(".t3s.tmp"))
            || (name.starts_with(".route-g") && name.ends_with(".t3r.tmp"))
            || name == ".manifest.json.tmp";
        if generation_artifact && !referenced.contains(name) {
            fs::remove_file(path)?;
            removed = true;
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(true)
}

fn record_reclaim_worker_error(
    stats: &Arc<Mutex<CheckpointReclaimWorkerStats>>,
    error: CheckpointStoreError,
) {
    if let Ok(mut stats) = stats.lock() {
        stats.error_stops = stats.error_stops.saturating_add(1);
        if stats.last_error.is_none() {
            stats.last_error = Some(error.to_string());
        }
    }
}

fn reclaim_worker_poll(dir: &Path, stats: &Arc<Mutex<CheckpointReclaimWorkerStats>>) -> bool {
    if let Ok(mut stats) = stats.lock() {
        stats.poll_count = stats.poll_count.saturating_add(1);
    } else {
        return false;
    }
    let before = match tree_storage(dir) {
        Ok(storage) => storage,
        Err(error) => {
            record_reclaim_worker_error(stats, error);
            return false;
        }
    };
    let manifest = match load_manifest(dir) {
        Ok(manifest) => manifest,
        Err(error) => {
            record_reclaim_worker_error(stats, error);
            return false;
        }
    };
    let gate_acquired = match reclaim_unreferenced_generation_files(dir, &manifest) {
        Ok(acquired) => acquired,
        Err(error) => {
            record_reclaim_worker_error(stats, error);
            return false;
        }
    };
    let after = match tree_storage(dir) {
        Ok(storage) => storage,
        Err(error) => {
            record_reclaim_worker_error(stats, error);
            return false;
        }
    };
    if let Ok(mut stats) = stats.lock() {
        if gate_acquired {
            stats.completed_polls = stats.completed_polls.saturating_add(1);
        } else {
            stats.deferred_polls = stats.deferred_polls.saturating_add(1);
        }
        stats.reclaimed_allocated_bytes = stats
            .reclaimed_allocated_bytes
            .saturating_add(before.allocated_bytes.saturating_sub(after.allocated_bytes));
        true
    } else {
        false
    }
}

fn ensure_hot_file_exists(
    path: &Path,
    config: CheckpointStoreConfig,
) -> Result<(), CheckpointStoreError> {
    let hot = HotWal::open_at(path, 0, config)?;
    drop(hot);
    Ok(())
}

#[cfg(unix)]
fn read_file_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    let mut remaining = buffer;
    let mut cursor = offset;
    while !remaining.is_empty() {
        let read = PositionalFileExt::read_at(file, remaining, cursor)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "positional file read ended before the requested range",
            ));
        }
        remaining = remaining.get_mut(read..).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "positional read exceeded buffer",
            )
        })?;
        cursor = cursor
            .checked_add(u64::try_from(read).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "positional read length overflow",
                )
            })?)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "positional read offset overflow",
                )
            })?;
    }
    Ok(())
}

#[cfg(windows)]
fn read_file_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    let mut remaining = buffer;
    let mut cursor = offset;
    while !remaining.is_empty() {
        let read = PositionalFileExt::seek_read(file, remaining, cursor)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "positional file read ended before the requested range",
            ));
        }
        remaining = remaining.get_mut(read..).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "positional read exceeded buffer",
            )
        })?;
        cursor = cursor
            .checked_add(u64::try_from(read).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "positional read length overflow",
                )
            })?)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "positional read offset overflow",
                )
            })?;
    }
    Ok(())
}

mod transaction;
use transaction::*;

mod segment;
use segment::*;

mod state;
use state::*;

mod manifest;
use manifest::*;

fn tmp_path_for(path: &Path) -> Result<PathBuf, CheckpointStoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| format_error("target path has no parent"))?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format_error("target filename is not UTF-8"))?;
    Ok(parent.join(format!(".{name}.tmp")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicationRole {
    Artifact,
    ManifestAuthority,
}

fn publish_existing_tmp_with_role(
    tmp: &Path,
    final_path: &Path,
    role: PublicationRole,
) -> Result<(), CheckpointStoreError> {
    if !tmp.exists() {
        return Err(format_error("staged temporary file is missing"));
    }
    #[cfg(feature = "fault-injection")]
    let publication_fault = if role == PublicationRole::ManifestAuthority {
        configured_publication_io_fault()?
    } else {
        None
    };

    OpenOptions::new()
        .read(true)
        .write(true)
        .open(tmp)?
        .sync_all()?;
    maybe_file_crash(final_path, "sync");

    #[cfg(feature = "fault-injection")]
    if publication_fault == Some(PublicationIoFault::ManifestSyncEioAfter) {
        return Err(injected_io_error().into());
    }

    #[cfg(feature = "fault-injection")]
    if publication_fault == Some(PublicationIoFault::ManifestRenameEioBefore) {
        return Err(durability_indeterminate_error(
            DurabilityOperation::Rename,
            final_path,
            injected_io_error(),
        ));
    }

    if let Err(source) = fs::rename(tmp, final_path) {
        return Err(if role == PublicationRole::ManifestAuthority {
            durability_indeterminate_error(DurabilityOperation::Rename, final_path, source)
        } else {
            source.into()
        });
    }
    maybe_file_crash(final_path, "rename");

    #[cfg(feature = "fault-injection")]
    if publication_fault == Some(PublicationIoFault::ManifestRenameEioAfter) {
        return Err(durability_indeterminate_error(
            DurabilityOperation::Rename,
            final_path,
            injected_io_error(),
        ));
    }

    let parent = final_path
        .parent()
        .ok_or_else(|| format_error("final path has no parent"))?;
    if let Err(error) = sync_dir(parent) {
        return Err(match (role, error) {
            (PublicationRole::ManifestAuthority, CheckpointStoreError::Io(source)) => {
                durability_indeterminate_error(DurabilityOperation::DirectorySync, parent, source)
            }
            (_, other) => other,
        });
    }
    maybe_file_crash(final_path, "dir-sync");

    #[cfg(feature = "fault-injection")]
    if publication_fault == Some(PublicationIoFault::ManifestDirSyncEioAfter) {
        return Err(durability_indeterminate_error(
            DurabilityOperation::DirectorySync,
            parent,
            injected_io_error(),
        ));
    }

    Ok(())
}

fn publish_existing_tmp(tmp: &Path, final_path: &Path) -> Result<(), CheckpointStoreError> {
    publish_existing_tmp_with_role(tmp, final_path, PublicationRole::Artifact)
}

fn staged_write_with_role(
    path: &Path,
    bytes: &[u8],
    role: PublicationRole,
) -> Result<(), CheckpointStoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| format_error("target path has no parent"))?;
    fs::create_dir_all(parent)?;
    let tmp = tmp_path_for(path)?;
    if tmp.exists() {
        fs::remove_file(&tmp)?;
    }
    let mut file = File::create(&tmp)?;
    file.write_all(bytes)?;
    file.flush()?;
    drop(file);
    maybe_file_crash(path, "write");
    publish_existing_tmp_with_role(&tmp, path, role)
}

fn staged_write_new(path: &Path, bytes: &[u8]) -> Result<(), CheckpointStoreError> {
    staged_write_with_role(path, bytes, PublicationRole::Artifact)
}

fn staged_write_manifest(path: &Path, bytes: &[u8]) -> Result<(), CheckpointStoreError> {
    staged_write_with_role(path, bytes, PublicationRole::ManifestAuthority)
}

struct RecycleResult {
    peak: StoreStorage,
}

fn recycle_hot_file(
    dir: &Path,
    hot_path: &Path,
    source_offset: u64,
    logical_tail: u64,
    config: CheckpointStoreConfig,
) -> Result<RecycleResult, CheckpointStoreError> {
    #[cfg(feature = "fault-injection")]
    let publication_fault = configured_publication_io_fault()?;

    if source_offset > logical_tail {
        return Err(format_error(
            "WAL recycle source offset exceeds logical tail",
        ));
    }
    let tmp = tmp_path_for(hot_path)?;
    if tmp.exists() {
        fs::remove_file(&tmp)?;
    }
    let mut source = File::open(hot_path)?;
    source.seek(SeekFrom::Start(source_offset))?;
    let suffix_len = logical_tail - source_offset;
    let mut output = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    let mut limited = source.take(suffix_len);
    let copied = io::copy(&mut limited, &mut output)?;
    if copied != suffix_len {
        return Err(format_error(
            "WAL recycle copied an unexpected suffix length",
        ));
    }
    maybe_crash("after-wal-write");
    let capacity = round_capacity(suffix_len, config.wal_segment_bytes)?;
    preinitialize_range(
        &mut output,
        suffix_len,
        capacity,
        config.preinit_chunk_bytes,
    )?;
    maybe_crash("after-wal-sync");

    #[cfg(feature = "fault-injection")]
    if publication_fault == Some(PublicationIoFault::WalRecycleSyncEioAfter) {
        return Err(injected_io_error().into());
    }

    let peak = tree_storage(dir)?;
    drop(output);

    #[cfg(feature = "fault-injection")]
    if publication_fault == Some(PublicationIoFault::WalRecycleRenameEioBefore) {
        return Err(injected_io_error().into());
    }

    fs::rename(&tmp, hot_path)?;
    maybe_crash("after-wal-rename");

    #[cfg(feature = "fault-injection")]
    if publication_fault == Some(PublicationIoFault::WalRecycleRenameEioAfter) {
        return Err(injected_io_error().into());
    }

    let parent = hot_path
        .parent()
        .ok_or_else(|| format_error("hot WAL path has no parent"))?;
    sync_dir(parent)?;
    maybe_crash("after-wal-dir-sync");

    #[cfg(feature = "fault-injection")]
    if publication_fault == Some(PublicationIoFault::WalRecycleDirSyncEioAfter) {
        return Err(injected_io_error().into());
    }

    Ok(RecycleResult { peak })
}

fn tree_storage(dir: &Path) -> Result<StoreStorage, CheckpointStoreError> {
    let mut result = StoreStorage {
        file_length_bytes: 0,
        allocated_bytes: 0,
        file_count: 0,
    };
    if !dir.exists() {
        return Ok(result);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name() == WRITER_LOCK_FILE
            || entry.file_name() == READER_RECLAIM_LOCK_FILE
            || entry.file_name() == RECLAIM_WORKER_LOCK_FILE
        {
            continue;
        }
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        result.file_count = result.file_count.saturating_add(1);
        result.file_length_bytes = result.file_length_bytes.saturating_add(metadata.len());
        #[cfg(unix)]
        {
            result.allocated_bytes = result
                .allocated_bytes
                .saturating_add(metadata.blocks().saturating_mul(512));
        }
        #[cfg(not(unix))]
        {
            result.allocated_bytes = result.allocated_bytes.saturating_add(metadata.len());
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
