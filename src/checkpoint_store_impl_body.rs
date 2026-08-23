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
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::{FileExt as PositionalFileExt, MetadataExt};
#[cfg(windows)]
use std::os::windows::fs::FileExt as PositionalFileExt;

use fs4::FileExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use xxhash_rust::xxh3::{xxh3_64, Xxh3};

const TX_MAGIC: [u8; 4] = *b"T2W1";
const TX_HEADER_SIZE: usize = 72;
const TX_CHECKSUM_SIZE: usize = 8;
const ROOT_MAGIC: [u8; 4] = *b"T2R1";
const CHECKPOINT_MAGIC: [u8; 4] = *b"T2P1";
const ROOT_RECORD_SIZE: usize = 32;
const CHECKPOINT_PREFIX_SIZE: usize = 52;
const NONE_ROOT: u64 = u64::MAX;
const NONE_VERSION: u32 = u32::MAX;
const NONE_PARENT: u32 = u32::MAX;
const COMPACT_NODE_SIZE: usize = 16;
const WIDE_RECORD_SIZE: usize = 32;
const CANONICAL_STATE_PREFIX_BYTES: u64 = 12;
const CANONICAL_STATE_SUFFIX_BYTES: u64 = 15;
const KIND_LEAF: u32 = 0xD100_0001;
const KIND_BINARY: u32 = 0xD100_0002;
const KIND_WIDE: u32 = 0xD100_0003;
const WIDE_KIND_LEAF: u32 = 1;
const WIDE_KIND_BINARY: u32 = 2;

const MANIFEST_FORMAT: &str = "tulya-r3-structured-segment-manifest-r3";
const MANIFEST_FORMAT_VERSION_LEGACY: u64 = 3;
const MANIFEST_FORMAT_VERSION_STORE_ID: u64 = 4;
const MANIFEST_FORMAT_VERSION_SUBTREE_PRUNE: u64 = 5;
const MANIFEST_FILE: &str = "structured-segment-manifest.json";
const HOT_WAL_FILE: &str = "hot.wal";
const SEGMENT_MAGIC: &[u8; 8] = b"T3STRS02";
const SEGMENT_FORMAT_VERSION: u32 = 2;
const SEGMENT_HEADER_SIZE: usize = 120;
const STREAM_ENTRY_SIZE: usize = 40;
const BLOCK_ENTRY_SIZE: usize = 40;
const ROUTE_MAGIC: &[u8; 8] = b"T3ROUT01";
const ROUTE_FORMAT_VERSION: u32 = 1;
const ROUTE_HEADER_SIZE: usize = 64;
const ROUTE_ENTRY_SIZE: usize = 24;
const WRITER_LOCK_FILE: &str = ".tulya-writer.lock";
const READER_RECLAIM_LOCK_FILE: &str = ".tulya-reader-reclaim.lock";
const RECLAIM_WORKER_LOCK_FILE: &str = ".tulya-reclaim-worker.lock";
const MAX_CHECKPOINT_IDENTIFIER_BYTES: usize = 4096;
const MAX_REQUEST_ID_BYTES: usize = 4096;
const REQUEST_SECTION_MAGIC: &[u8; 8] = b"T2REQ01\0";
const REQUEST_FOOTER_MAGIC: &[u8; 8] = b"T2REQF1\0";
const REQUEST_SECTION_VERSION: u32 = 1;
const REQUEST_SECTION_HEADER_BYTES: usize = 20;
const REQUEST_SECTION_FOOTER_BYTES: usize = 16;
const LAZY_BLOCK_CACHE_CAPACITY: usize = 128;
const LAZY_DIAGNOSTIC_MAX_CACHE_CAPACITY: usize = 4096;
const LAZY_EAGER_PAYLOAD_CAP_BYTES: u64 = 8 * 1024 * 1024;

fn lazy_payload_fast_path_allowed(payload_len: u64) -> bool {
    payload_len <= LAZY_EAGER_PAYLOAD_CAP_BYTES
}

fn lazy_block_cache_capacity() -> Result<usize, CheckpointStoreError> {
    let Some(value) = std::env::var_os("TULYA_LAZY_BLOCK_CACHE_CAPACITY_DIAGNOSTIC") else {
        return Ok(LAZY_BLOCK_CACHE_CAPACITY);
    };
    let value = value
        .to_str()
        .ok_or_else(|| format_error("lazy diagnostic cache capacity is not UTF-8"))?;
    let capacity = value
        .parse::<usize>()
        .map_err(|_| format_error("lazy diagnostic cache capacity is not a number"))?;
    if capacity == 0 || capacity > LAZY_DIAGNOSTIC_MAX_CACHE_CAPACITY {
        return Err(format_error("lazy diagnostic cache capacity is outside bounds"));
    }
    Ok(capacity)
}

const STREAM_NAMES: [&str; 22] = [
    "payload.bin",
    "node_field0_u64.bin",
    "node_field1_u32.bin",
    "node_kind_u8.bin",
    "wide_kind_u8.bin",
    "wide_a_u64.bin",
    "wide_b_u64.bin",
    "wide_c_u64.bin",
    "version_root_u64.bin",
    "version_parent_u32.bin",
    "thread_offsets_u32.bin",
    "thread_bytes.bin",
    "checkpoint_thread_u32.bin",
    "checkpoint_no_u32.bin",
    "checkpoint_id_offsets_u32.bin",
    "checkpoint_id_bytes.bin",
    "checkpoint_parent_ordinal_u32.bin",
    "checkpoint_identity_version_u32.bin",
    "checkpoint_messages_version_u32.bin",
    "checkpoint_result_version_u32.bin",
    "checkpoint_logical_state_len_u64.bin",
    "checkpoint_state_hash_u64.bin",
];
const PAYLOAD: usize = 0;
const NODE_FIELD0: usize = 1;
const NODE_FIELD1: usize = 2;
const NODE_KIND: usize = 3;
const WIDE_KIND: usize = 4;
const WIDE_A: usize = 5;
const WIDE_B: usize = 6;
const WIDE_C: usize = 7;
const VERSION_ROOT: usize = 8;
const VERSION_PARENT: usize = 9;
const THREAD_OFFSETS: usize = 10;
const THREAD_BYTES: usize = 11;
const CP_THREAD: usize = 12;
const CP_NO: usize = 13;
const CP_ID_OFFSETS: usize = 14;
const CP_ID_BYTES: usize = 15;
const CP_PARENT_ORDINAL: usize = 16;
const CP_IDENTITY_VERSION: usize = 17;
const CP_MESSAGES_VERSION: usize = 18;
const CP_RESULT_VERSION: usize = 19;
const CP_LOGICAL_LEN: usize = 20;
const CP_STATE_HASH: usize = 21;

/// Errors returned by the production checkpoint-store lifecycle.
#[derive(Debug, Error)]
pub enum CheckpointStoreError {
    /// Filesystem or durability operation failed.
    #[error("checkpoint-store I/O error: {0}")]
    Io(#[from] io::Error),
    /// Manifest JSON could not be encoded or decoded.
    #[error("checkpoint-store manifest JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Persisted bytes violate the T2W1/T3 format or topology contract.
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
            return Err(format_error("StoreId must contain exactly 32 hex characters"));
        }
        let bytes = value.as_bytes();
        let mut output = [0u8; 16];
        for index in 0..16 {
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
            output[index] = (high << 4) | low;
        }
        Ok(Self(output))
    }

    fn generate() -> Result<Self, CheckpointStoreError> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("operating-system random source failed: {error}"),
            )
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
    /// Raw block size for T3 structured sealed streams.
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
    pub fn new(soft_logical_bytes: u64, hard_logical_bytes: u64) -> Result<Self, CheckpointStoreError> {
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
            .read(true)
            .write(true)
            .open(dir.join(READER_RECLAIM_LOCK_FILE))?;
        file.lock_shared()?;
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
    pub fn start(
        dir: impl AsRef<Path>,
        interval: Duration,
    ) -> Result<Self, CheckpointStoreError> {
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
        self.stats.lock().map(|stats| stats.clone()).unwrap_or_default()
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
            .read(true)
            .write(true)
            .open(path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { _file: file }),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.raw_os_error() == fs4::lock_contended_error().raw_os_error() =>
            {
                let _ = file.unlock();
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStoreAppendOutcome {
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct IdentityLeafRef {
    pub(crate) node_id: u64,
    pub(crate) logical_start: u64,
    pub(crate) logical_len: u64,
}

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
            return Err(format_error("physical WAL shorter than committed logical tail"));
        }
        let required_capacity = round_capacity(logical_tail, config.wal_segment_bytes)?;
        if current_len < required_capacity {
            preinitialize_range(&mut file, current_len, required_capacity, config.preinit_chunk_bytes)?;
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
        })
    }

    /// Appends one encoded transaction and makes it durable with `sync_data`.
    ///
    /// # Errors
    ///
    /// Returns an error if capacity initialization, write, or durability sync
    /// fails.
    pub fn append(&mut self, transaction: &[u8]) -> Result<HotWalAppendReport, CheckpointStoreError> {
        let tx_len = u64::try_from(transaction.len())
            .map_err(|_| format_error("transaction length does not fit u64"))?;
        let required_tail = self
            .logical_tail
            .checked_add(tx_len)
            .ok_or_else(|| format_error("WAL logical tail overflow"))?;
        let current_capacity = self.file.metadata()?.len();
        if required_tail > current_capacity {
            let new_capacity = round_capacity(required_tail, self.config.wal_segment_bytes)?;
            preinitialize_range(
                &mut self.file,
                current_capacity,
                new_capacity,
                self.config.preinit_chunk_bytes,
            )?;
            self.file.seek(SeekFrom::Start(self.logical_tail))?;
        }

        let write_started = Instant::now();
        self.file.write_all(transaction)?;
        self.file.flush()?;
        let write_ns = write_started.elapsed().as_nanos();
        let sync_started = Instant::now();
        self.file.sync_data()?;
        let sync_data_ns = sync_started.elapsed().as_nanos();
        self.logical_tail = required_tail;
        Ok(HotWalAppendReport {
            transaction_bytes: tx_len,
            logical_tail_bytes: required_tail,
            capacity_bytes: self.file.metadata()?.len(),
            write_ns,
            sync_data_ns,
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

    fn replace_after_recycle(&mut self, new_tail: u64) -> Result<(), CheckpointStoreError> {
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.logical_tail = new_tail;
        self.file.seek(SeekFrom::Start(new_tail))?;
        Ok(())
    }
}

/// Production-facing checkpoint-store lifecycle.
///
/// The store owns the hot durability reserve, T2W1 recovery, immutable T3
/// segments/routes/manifest, physical WAL recycle, and exact checkpoint reads.
/// Semantic adapters may currently submit already-encoded T2W1 transactions;
/// they do not own filesystem durability or sealing.
pub struct CheckpointStore {
    dir: PathBuf,
    config: CheckpointStoreConfig,
    manifest: Manifest,
    state: StoreState,
    _writer_lock: WriterLock,
    hot: HotWal,
    lazy_base: Option<RefCell<LazyCheckpointStore>>,
    range_sizes: RefCell<Vec<Option<u64>>>,
}

impl CheckpointStore {
    /// Opens or creates a checkpoint store and reconstructs its complete
    /// committed logical state from sealed streams plus the valid hot suffix.
    ///
    /// If a previous process died after publishing a manifest but before WAL
    /// recycle, `open` recognizes the previous-generation hot geometry and
    /// completes the already-authorized recycle before returning.
    ///
    /// # Errors
    ///
    /// Returns an error if persisted authoritative data is malformed or an I/O
    /// operation fails.
    pub fn open(
        dir: impl AsRef<Path>,
        config: CheckpointStoreConfig,
    ) -> Result<Self, CheckpointStoreError> {
        validate_config(config)?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let writer_lock = WriterLock::acquire(&dir)?;
        let manifest = load_manifest(&dir)?;
        let manifest = ensure_new_store_manifest(&dir, manifest)?;
        let (mut state, lazy_base) = if config.recovery_mode == CheckpointStoreRecoveryMode::Lazy {
            let lazy = LazyCheckpointStore::open_for_writable_store(&dir)?;
            let state = state_from_lazy_reader(&dir, &manifest, &lazy)?;
            (state, Some(RefCell::new(lazy)))
        } else {
            (
                materialize_sealed_state(&dir, &manifest, config.recovery_mode)?,
                None,
            )
        };
        reclaim_unreferenced_generation_files(&dir, &manifest)?;
        let hot_path = dir.join(HOT_WAL_FILE);
        ensure_hot_file_exists(&hot_path, config)?;

        let current_geometry = Geometry::from_manifest(&manifest)?;
        let current_parse = parse_hot_prefix(&hot_path, current_geometry)?;
        let mut normalized_tail = current_parse.logical_tail;
        let mut txs = current_parse.transactions;

        if txs.is_empty() && file_starts_with_tx_magic(&hot_path)? {
            if let Some(previous) = Geometry::previous_generation(&manifest)? {
                let previous_parse = parse_hot_prefix(&hot_path, previous)?;
                if !previous_parse.transactions.is_empty() {
                    let overlap_end = previous_parse
                        .transactions
                        .iter()
                        .filter(|tx| tx.checkpoint_count <= manifest.checkpoint_count)
                        .map(|tx| tx.end_offset)
                        .max()
                        .unwrap_or(0);
                    if overlap_end > 0 {
                        let suffix_len = previous_parse.logical_tail.saturating_sub(overlap_end);
                        recycle_hot_file(
                            &dir,
                            &hot_path,
                            overlap_end,
                            previous_parse.logical_tail,
                            config,
                        )?;
                        let reparsed = parse_hot_prefix(&hot_path, current_geometry)?;
                        if reparsed.logical_tail != suffix_len {
                            return Err(format_error("post-crash WAL normalization suffix mismatch"));
                        }
                        normalized_tail = reparsed.logical_tail;
                        txs = reparsed.transactions;
                    }
                }
            }
        }

        for tx in &txs {
            apply_transaction(&mut state, tx)?;
        }
        let hot = HotWal::open_at(&hot_path, normalized_tail, config)?;
        let store = Self {
            dir,
            config,
            manifest,
            state,
            _writer_lock: writer_lock,
            hot,
            lazy_base,
            range_sizes: RefCell::new(Vec::new()),
        };
        if store.lazy_base.is_none() {
            let roots = store.state.versions.iter().flatten().copied().collect::<Vec<_>>();
            for root in roots {
                store.root_term_size(root)?;
            }
        }
        Ok(store)
    }

    /// Returns this store's persistent identity, if it has been migrated from
    /// the legacy manifest format.
    pub fn store_id(&self) -> Option<StoreId> {
        self.manifest.store_id
    }

    /// Explicitly migrates a legacy manifest to the StoreId manifest revision.
    ///
    /// Calling this on an already-identified store is idempotent and returns
    /// the existing identity without rewriting the manifest.
    pub fn migrate_store_id(&mut self) -> Result<StoreId, CheckpointStoreError> {
        if let Some(store_id) = self.manifest.store_id {
            return Ok(store_id);
        }
        let store_id = StoreId::generate()?;
        let mut next_manifest = self.manifest.clone();
        next_manifest.store_id = Some(store_id);
        staged_write_new(&self.dir.join(MANIFEST_FILE), &manifest_bytes(&next_manifest)?)?;
        self.manifest = next_manifest;
        Ok(store_id)
    }

    /// Rebinds a store to a newly generated identity for an explicitly
    /// independent clone. Checkpoint and request-ledger bytes are unchanged.
    pub fn fork_as_new_store(&mut self) -> Result<StoreId, CheckpointStoreError> {
        let store_id = StoreId::generate()?;
        let mut next_manifest = self.manifest.clone();
        next_manifest.store_id = Some(store_id);
        staged_write_new(&self.dir.join(MANIFEST_FILE), &manifest_bytes(&next_manifest)?)?;
        self.manifest = next_manifest;
        Ok(store_id)
    }

    /// Deletes one checkpoint and all of its descendants in the current
    /// one-parent thread lineage, then rewrites only live reachable history.
    ///
    /// The operation is deliberately restricted to a fully sealed store. The
    /// replacement segment and route become durable before the old generation
    /// is removed, and deleted checkpoint/request identities remain durable
    /// tombstones so they cannot be reused after reopen.
    pub fn delete_checkpoint_subtree(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
    ) -> Result<PruneReport, CheckpointStoreError> {
        validate_checkpoint_identifier(thread_id, "thread id")?;
        validate_checkpoint_identifier(checkpoint_id, "checkpoint id")?;
        if self.hot.logical_tail() != 0 {
            return Err(CheckpointStoreError::PruneRequiresSealedStore);
        }
        if self.lazy_base.is_some() {
            return Err(CheckpointStoreError::PruneRequiresEagerRecovery);
        }
        let target_key = (thread_id.to_owned(), checkpoint_id.to_owned());
        if !self.state.checkpoint_ordinals.contains_key(&target_key) {
            return Err(if self.state.deleted_checkpoints.contains(&target_key) {
                CheckpointStoreError::CheckpointDeleted
            } else {
                CheckpointStoreError::CheckpointNotFound
            });
        }
        let mut deleted_keys = HashSet::new();
        deleted_keys.insert(target_key);
        loop {
            let mut changed = false;
            for checkpoint in &self.state.checkpoints {
                if deleted_keys.contains(&(checkpoint.thread_id.clone(), checkpoint.checkpoint_id.clone())) {
                    continue;
                }
                if checkpoint.thread_id == thread_id
                    && checkpoint
                        .parent_checkpoint_id
                        .as_ref()
                        .is_some_and(|parent| deleted_keys.contains(&(thread_id.to_owned(), parent.clone())))
                {
                    deleted_keys.insert((checkpoint.thread_id.clone(), checkpoint.checkpoint_id.clone()));
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        self.delete_checkpoint_set(deleted_keys)
    }

    /// Compacts an explicitly validated set of checkpoints from a fully
    /// sealed store. Repository adapters use this boundary when their
    /// persisted metadata contains more parent edges than the generic
    /// first-parent checkpoint record.
    pub(crate) fn delete_checkpoint_set(
        &mut self,
        deleted_keys: HashSet<(String, String)>,
    ) -> Result<PruneReport, CheckpointStoreError> {
        if self.hot.logical_tail() != 0 {
            return Err(CheckpointStoreError::PruneRequiresSealedStore);
        }
        if self.lazy_base.is_some() {
            return Err(CheckpointStoreError::PruneRequiresEagerRecovery);
        }
        if deleted_keys.is_empty() {
            return Err(format_error("checkpoint prune set is empty"));
        }
        for key in &deleted_keys {
            if !self.state.checkpoint_ordinals.contains_key(key) {
                return Err(if self.state.deleted_checkpoints.contains(key) {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                });
            }
        }
        if self.manifest.store_id.is_none() {
            self.migrate_store_id()?;
        }

        let before = tree_storage(&self.dir)?;
        let compacted = compact_live_state(&self.state, &deleted_keys)?;
        let generation = self
            .manifest
            .generation
            .checked_add(1)
            .ok_or_else(|| format_error("prune manifest generation overflow"))?;
        let replacement = write_compacted_generation(&self.dir, generation, self.config, &compacted)?;
        let segment_final = self.dir.join(&replacement.finalized.meta.file);
        publish_existing_tmp(&replacement.finalized.tmp_path, &segment_final)?;

        let route_path = self.dir.join(&replacement.route_meta.file);
        staged_write_new(&route_path, &replacement.route_bytes)?;
        let next_manifest = Manifest {
            generation,
            sealed_end_wal_bytes: 0,
            checkpoint_count: u64::try_from(compacted.state.checkpoints.len())
                .map_err(|_| format_error("pruned checkpoint count overflow"))?,
            version_count: u64::try_from(compacted.state.versions.len())
                .map_err(|_| format_error("pruned version count overflow"))?,
            thread_count: u64::try_from(compacted.state.threads.len())
                .map_err(|_| format_error("pruned thread count overflow"))?,
            stream_sizes: replacement.finalized.meta.stream_ends.clone(),
            segments: vec![replacement.finalized.meta.clone()],
            routes: vec![replacement.route_meta.clone()],
            store_id: self.manifest.store_id,
            deleted_checkpoints: compacted
                .deleted_checkpoints
                .iter()
                .map(|(thread_id, checkpoint_id)| CheckpointTombstone {
                    thread_id: thread_id.clone(),
                    checkpoint_id: checkpoint_id.clone(),
                })
                .collect(),
            retired_requests: compacted
                .retired_requests
                .iter()
                .map(|(key, operation_digest)| RetiredRequestRecord {
                    key: key.clone(),
                    operation_digest: *operation_digest,
                })
                .collect(),
        };
        staged_write_new(&self.dir.join(MANIFEST_FILE), &manifest_bytes(&next_manifest)?)?;
        let coexistence = tree_storage(&self.dir)?;

        self.manifest = next_manifest;
        self.state = compacted.state;
        self.range_sizes.borrow_mut().clear();
        reclaim_unreferenced_generation_files(&self.dir, &self.manifest)?;
        let reclaimed = tree_storage(&self.dir)?;
        Ok(PruneReport {
            generation,
            deleted_checkpoint_count: u64::try_from(deleted_keys.len())
                .map_err(|_| format_error("deleted checkpoint count overflow"))?,
            retained_checkpoint_count: u64::try_from(self.state.checkpoints.len())
                .map_err(|_| format_error("retained checkpoint count overflow"))?,
            rewritten_bytes: replacement
                .finalized
                .meta
                .segment_file_bytes
                .checked_add(replacement.route_meta.route_file_bytes)
                .ok_or_else(|| format_error("prune rewritten byte count overflow"))?,
            before,
            coexistence,
            reclaimed,
        })
    }

    /// Drains unreferenced sealed generations when no sealed reader lease is
    /// active. If a reader is still active, this call is safe and leaves the
    /// obsolete files for a later drain.
    pub fn reclaim_deferred_generations(&self) -> Result<StoreStorage, CheckpointStoreError> {
        reclaim_unreferenced_generation_files(&self.dir, &self.manifest)?;
        tree_storage(&self.dir)
    }

    /// Appends one adapter-encoded T2W1 transaction through the production
    /// preinitialized WAL and updates the in-memory committed state after the
    /// durability barrier succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is malformed, does not continue the
    /// current global topology, or cannot be durably written.
    pub fn append_encoded_transaction(
        &mut self,
        transaction: &[u8],
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        let requestless = parse_transaction_unchecked(transaction, 0)?;
        reject_deleted_checkpoint(&self.state, &requestless)?;
        let geometry = Geometry::from_state(&self.state)?;
        let tx = parse_transaction(transaction, geometry, 0)?;
        self.append_parsed_transaction(transaction, tx)
    }

    /// Appends one checkpoint from identity bytes and checkpoint metadata.
    ///
    /// This is the semantic boundary for adapters that should not assemble
    /// private T2W1 records or choose the store's global byte, node, and
    /// version watermarks. The identity bytes are stored as one leaf in the
    /// existing checkpoint format; the canonical reader continues to expose
    /// them through the existing `{"identity":...,"messages":[]}` framing.
    ///
    /// # Errors
    ///
    /// Returns an error if identifiers, topology counters, lengths, or the
    /// parent relationship are invalid, or if the durable append fails.
    pub fn append_checkpoint(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        identity: &[u8],
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        let transaction = encode_single_identity_transaction(
            &geometry,
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            identity,
        )?;
        self.append_encoded_transaction(&transaction)
    }

    /// Appends one checkpoint containing new values for an append-only
    /// `messages` channel while structurally sharing the selected parent's
    /// prior message history.
    ///
    /// This is the production semantic boundary used by message-oriented
    /// agent adapters. The canonical state is
    /// `{"identity":null,"messages":[...]}`. Each supplied value is encoded
    /// once; a child checkpoint adds one leaf plus one binary node regardless
    /// of the parent's logical message-history length.
    ///
    /// The store reconstructs the selected parent before encoding the new
    /// checkpoint and derives the canonical state length/hash itself. This
    /// favors a strict, self-validating first integration over an unchecked
    /// caller-supplied digest. The caller may batch multiple newly appended
    /// message values into one checkpoint, but the batch must not be empty.
    ///
    /// # Errors
    ///
    /// Returns an error if identifiers or parent topology are invalid, if the
    /// store contains incompatible non-message checkpoints, if a message value
    /// exceeds the compact-node length limit, or if the durable append fails.
    pub fn append_messages_checkpoint(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        messages: &[Value],
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        if messages.is_empty() {
            return Err(format_error("message checkpoint delta is empty"));
        }

        let mut message_body = Vec::new();
        for (index, message) in messages.iter().enumerate() {
            if index > 0 {
                message_body.push(b',');
            }
            serde_json::to_writer(&mut message_body, message)?;
        }

        let parent = match parent_checkpoint_id {
            Some(parent_id) => {
                let ordinal = self
                    .state
                    .checkpoint_ordinals
                    .get(&(thread_id.to_owned(), parent_id.to_owned()))
                    .copied()
                    .ok_or(CheckpointStoreError::CheckpointNotFound)?;
                let info = self
                    .state
                    .checkpoints
                    .get(usize::try_from(ordinal).map_err(|_| {
                        format_error("parent checkpoint ordinal exceeds usize")
                    })?)
                    .ok_or_else(|| format_error("parent checkpoint ordinal is absent"))?
                    .clone();
                let messages_version = info.messages_version.ok_or_else(|| {
                    format_error("parent checkpoint has no append-only messages channel")
                })?;
                let messages_root = self.version_root_for_read(messages_version)?;
                Some((info, messages_root, messages_version))
            }
            None => None,
        };

        if parent.is_none() {
            if let Some(first) = self.state.checkpoints.first() {
                let first_state = self.read_checkpoint(&first.thread_id, &first.checkpoint_id)?;
                if !first_state.starts_with(b"{\"identity\":null,") {
                    return Err(format_error(
                        "message checkpoints cannot reuse a non-null identity root",
                    ));
                }
            }
        }

        let mut canonical = if let Some((info, _, _)) = parent.as_ref() {
            let mut prior = self.read_checkpoint(thread_id, &info.checkpoint_id)?;
            if !prior.starts_with(b"{\"identity\":null,\"messages\":[")
                || !prior.ends_with(b"]}")
            {
                return Err(format_error(
                    "parent canonical state is not the append-only message schema",
                ));
            }
            prior.truncate(
                prior
                    .len()
                    .checked_sub(2)
                    .ok_or_else(|| format_error("parent canonical state is truncated"))?,
            );
            prior.push(b',');
            prior.extend_from_slice(&message_body);
            prior.extend_from_slice(b"]}");
            prior
        } else {
            let mut root = Vec::new();
            root.extend_from_slice(b"{\"identity\":null,\"messages\":[");
            root.extend_from_slice(&message_body);
            root.extend_from_slice(b"]}");
            root
        };
        let canonical_state_len = u64::try_from(canonical.len())
            .map_err(|_| format_error("canonical message state length exceeds u64"))?;
        let canonical_state_hash = xxh3_64(&canonical);
        canonical.clear();

        let geometry = self.state.geometry()?;
        let transaction = encode_message_append_transaction(
            &geometry,
            self.state.checkpoints.first(),
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            parent.as_ref().map(|(info, root, version)| {
                (info.identity_version, *root, *version)
            }),
            &message_body,
            canonical_state_len,
            canonical_state_hash,
        )?;
        self.append_encoded_transaction(&transaction)
    }

    pub(crate) fn identity_leaf_refs(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
    ) -> Result<Vec<IdentityLeafRef>, CheckpointStoreError> {
        let ordinal = self
            .state
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or(CheckpointStoreError::CheckpointNotFound)?;
        let checkpoint = self
            .state
            .checkpoints
            .get(usize::try_from(ordinal).map_err(|_| format_error("checkpoint ordinal overflow"))?)
            .ok_or_else(|| format_error("checkpoint ordinal outside state"))?;
        let root = self.version_root_for_read(checkpoint.identity_version)?;
        let mut leaves = Vec::new();
        let identity_len = self.collect_identity_leaf_refs(root, 0, &mut leaves)?;
        let expected_len = checkpoint
            .logical_state_len
            .checked_sub(CANONICAL_STATE_PREFIX_BYTES)
            .and_then(|value| value.checked_sub(CANONICAL_STATE_SUFFIX_BYTES))
            .ok_or_else(|| format_error("canonical checkpoint is shorter than its framing"))?;
        if identity_len != expected_len {
            return Err(format_error("identity leaf lengths disagree with checkpoint metadata"));
        }
        Ok(leaves)
    }

    pub(crate) fn read_identity_leaf(
        &self,
        reference: IdentityLeafRef,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        if reference.node_id >= geometry.node_count {
            return Err(format_error("identity leaf node is outside committed state"));
        }
        let node = self.decode_node_for_read(reference.node_id)?;
        if node.kind != 0 || node.b != reference.logical_len {
            return Err(format_error("identity leaf reference does not identify a leaf"));
        }
        let mut bytes = Vec::new();
        self.append_payload_range(node.a, node.b, &mut bytes)?;
        Ok(bytes)
    }

    fn collect_identity_leaf_refs(
        &self,
        node_id: u64,
        logical_start: u64,
        leaves: &mut Vec<IdentityLeafRef>,
    ) -> Result<u64, CheckpointStoreError> {
        let node = self.decode_node_for_read(node_id)?;
        if node.kind == 0 {
            leaves.push(IdentityLeafRef {
                node_id,
                logical_start,
                logical_len: node.b,
            });
            return Ok(node.b);
        }
        let left_len = self.collect_identity_leaf_refs(node.a, logical_start, leaves)?;
        let right_start = logical_start
            .checked_add(left_len)
            .ok_or_else(|| format_error("identity leaf logical offset overflow"))?;
        let right_len = self.collect_identity_leaf_refs(node.b, right_start, leaves)?;
        Ok(left_len
            .checked_add(right_len)
            .ok_or_else(|| format_error("identity tree length overflow"))?)
    }

    pub(crate) fn append_checkpoint_from_identity_leaves(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        leaves: &[IdentityLeafSource<'_>],
        canonical_state_len: u64,
        canonical_state_hash: u64,
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        let transaction = encode_identity_leaf_transaction(
            &geometry,
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            leaves,
            canonical_state_len,
            canonical_state_hash,
        )?;
        self.append_encoded_transaction(&transaction)
    }

    pub(crate) fn append_checkpoint_from_identity_leaves_with_bounded_lifecycle(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        leaves: &[IdentityLeafSource<'_>],
        canonical_state_len: u64,
        canonical_state_hash: u64,
        policy: BoundedWalLifecyclePolicy,
    ) -> Result<BoundedWalAppendReport, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        let transaction = encode_identity_leaf_transaction(
            &geometry,
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            leaves,
            canonical_state_len,
            canonical_state_hash,
        )?;
        self.append_encoded_transaction_with_bounded_lifecycle(&transaction, policy)
    }

    pub(crate) fn committed_node_count(&self) -> Result<u64, CheckpointStoreError> {
        Ok(self.state.geometry()?.node_count)
    }

    /// Appends one semantic checkpoint through an explicit bounded WAL
    /// lifecycle policy.
    ///
    /// This combines [`Self::append_checkpoint`] with the existing sequential
    /// seal-before-append policy. The transaction is rejected before any WAL
    /// mutation when it exceeds the policy hard limit.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata or topology is invalid, the transaction is
    /// larger than the policy hard limit, or the durable append/seal fails.
    pub fn append_checkpoint_with_bounded_lifecycle(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
        checkpoint_no: u32,
        parent_checkpoint_id: Option<&str>,
        identity: &[u8],
        policy: BoundedWalLifecyclePolicy,
    ) -> Result<BoundedWalAppendReport, CheckpointStoreError> {
        let geometry = self.state.geometry()?;
        let transaction = encode_single_identity_transaction(
            &geometry,
            thread_id,
            checkpoint_id,
            checkpoint_no,
            parent_checkpoint_id,
            identity,
        )?;
        self.append_encoded_transaction_with_bounded_lifecycle(&transaction, policy)
    }

    /// Appends one transaction while applying an explicit sequential hot-WAL bound.
    ///
    /// If the next transaction would cross the soft limit, the already committed
    /// hot prefix is sealed before this transaction is appended. A transaction
    /// larger than the hard limit is rejected before any WAL or seal mutation.
    /// This method performs no background maintenance and does not change the
    /// default [`Self::append_encoded_transaction`] behavior.
    pub fn append_encoded_transaction_with_bounded_lifecycle(
        &mut self,
        transaction: &[u8],
        policy: BoundedWalLifecyclePolicy,
    ) -> Result<BoundedWalAppendReport, CheckpointStoreError> {
        policy.validate()?;
        let transaction_bytes = u64::try_from(transaction.len())
            .map_err(|_| format_error("bounded WAL transaction length exceeds u64"))?;
        if transaction_bytes > policy.hard_logical_bytes {
            return Err(format_error(
                "transaction exceeds bounded WAL hard logical limit",
            ));
        }
        let requestless = parse_transaction_unchecked(transaction, 0)?;
        reject_deleted_checkpoint(&self.state, &requestless)?;
        let geometry = Geometry::from_state(&self.state)?;
        let tx = parse_transaction(transaction, geometry, 0)?;
        let current_hot = self.hot.logical_tail();
        let projected_hot = current_hot
            .checked_add(transaction_bytes)
            .ok_or_else(|| format_error("bounded WAL projected logical tail overflow"))?;
        let automatic_seal = if current_hot > 0 && projected_hot > policy.soft_logical_bytes {
            let checkpoint_count = u64::try_from(self.state.checkpoints.len())
                .map_err(|_| format_error("bounded WAL checkpoint count exceeds u64"))?;
            Some(self.seal_through(checkpoint_count)?)
        } else {
            None
        };
        let append = self.append_parsed_transaction(transaction, tx)?;
        Ok(BoundedWalAppendReport {
            append,
            automatic_seal,
        })
    }

    /// Appends one transaction with a durable request/idempotency key.
    ///
    /// An exact retry returns [`CheckpointStoreAppendOutcome::AlreadyCommitted`]
    /// without appending another WAL record. Reusing the key for different
    /// transaction bytes returns [`CheckpointStoreError::RequestIdConflict`].
    ///
    /// # Errors
    ///
    /// Returns an error if the request key or transaction is malformed, the
    /// transaction conflicts with the current topology, the request key was
    /// reused for different bytes, or durability fails.
    pub fn append_encoded_transaction_with_request_id(
        &mut self,
        request_id: &[u8],
        transaction: &[u8],
    ) -> Result<CheckpointStoreAppendOutcome, CheckpointStoreError> {
        validate_request_id(request_id)?;
        let requestless = parse_transaction_unchecked(transaction, 0)?;
        if requestless.request_id.is_some() {
            return Err(format_error(
                "request-id append expects a requestless transaction",
            ));
        }
        let encoded = encode_transaction_with_request_id(transaction, request_id)?;
        let candidate = parse_transaction_unchecked(&encoded, 0)?;
        if let Some(existing) = self.state.request_records.get(request_id) {
            if existing.operation_digest == candidate.operation_digest {
                return Ok(CheckpointStoreAppendOutcome::AlreadyCommitted);
            }
            return Err(CheckpointStoreError::RequestIdConflict);
        }
        if let Some(existing) = self.state.retired_requests.get(request_id) {
            if existing == &candidate.operation_digest {
                return Err(CheckpointStoreError::CheckpointDeleted);
            }
            return Err(CheckpointStoreError::RequestIdConflict);
        }
        reject_deleted_checkpoint(&self.state, &candidate)?;
        let geometry = Geometry::from_state(&self.state)?;
        let tx = parse_transaction(&encoded, geometry, 0)?;
        let report = self.append_parsed_transaction(&encoded, tx)?;
        Ok(CheckpointStoreAppendOutcome::Appended(report))
    }

    fn append_parsed_transaction(
        &mut self,
        transaction: &[u8],
        tx: ParsedTransaction,
    ) -> Result<HotWalAppendReport, CheckpointStoreError> {
        validate_transaction_against_state(&self.state, &tx)?;
        let report = self.hot.append(transaction)?;
        apply_transaction(&mut self.state, &tx)?;
        Ok(report)
    }

    /// Seals all currently hot transactions through `checkpoint_count`,
    /// publishes immutable T3 segment/route/manifest authority, then recycles
    /// the represented WAL prefix into a fresh preinitialized writable reserve.
    ///
    /// # Errors
    ///
    /// Returns an error if the target is not a strict advancement, the hot WAL
    /// does not contain the requested committed prefix, structured encoding
    /// fails, or any durability step fails.
    pub fn seal_through(
        &mut self,
        checkpoint_count: u64,
    ) -> Result<SealReport, CheckpointStoreError> {
        if checkpoint_count <= self.manifest.checkpoint_count {
            return Err(format_error("seal target must advance checkpoint authority"));
        }
        if checkpoint_count > u64::try_from(self.state.checkpoints.len())
            .map_err(|_| format_error("checkpoint count does not fit u64"))?
        {
            return Err(format_error("seal target exceeds committed checkpoint count"));
        }
        let before = tree_storage(&self.dir)?;
        let hot_path = self.dir.join(HOT_WAL_FILE);
        let base = Geometry::from_manifest(&self.manifest)?;
        let parsed = parse_hot_prefix(&hot_path, base)?;
        let target_index = parsed
            .transactions
            .iter()
            .position(|tx| tx.checkpoint_count == checkpoint_count)
            .ok_or_else(|| format_error("seal target is not present in current hot WAL"))?;
        let selected = &parsed.transactions[..=target_index];
        let source_offset = selected
            .last()
            .map(|tx| tx.end_offset)
            .ok_or_else(|| format_error("empty seal transaction selection"))?;
        let generation = self
            .manifest
            .generation
            .checked_add(1)
            .ok_or_else(|| format_error("manifest generation overflow"))?;
        let mut writer = StreamingSegmentWriter::new(
            &self.dir,
            generation,
            self.manifest.stream_sizes.clone(),
            self.manifest.checkpoint_count,
            self.manifest.version_count,
            self.config.sealed_block_size,
            self.config.zstd_level,
        )?;
        let mut new_threads = Vec::<(String, u32)>::new();
        let mut new_checkpoints = Vec::<(String, String, u32)>::new();
        let mut new_requests = Vec::<RequestRecord>::new();
        let mut next_thread_ordinal = u32::try_from(self.manifest.thread_count)
            .map_err(|_| format_error("thread count exceeds u32"))?;
        for tx in selected {
            write_transaction_to_segment(
                &mut writer,
                tx,
                &self.state,
                &mut next_thread_ordinal,
                &mut new_threads,
                &mut new_checkpoints,
                &mut new_requests,
            )?;
        }
        let version_end = selected
            .last()
            .map(|tx| tx.version_count)
            .ok_or_else(|| format_error("missing selected transaction"))?;
        let new_wal_end = self
            .manifest
            .sealed_end_wal_bytes
            .checked_add(source_offset)
            .ok_or_else(|| format_error("global sealed WAL watermark overflow"))?;
        let finalized = writer.finalize(
            self.manifest.sealed_end_wal_bytes,
            new_wal_end,
            checkpoint_count,
            version_end,
        )?;
        let segment_final = self.dir.join(&finalized.meta.file);
        publish_existing_tmp(&finalized.tmp_path, &segment_final)?;

        let (route_bytes, route_hash) =
            build_route_file(generation, &new_threads, &new_checkpoints, &new_requests)?;
        let route_name = format!("route-g{generation:06}.t3r");
        let route_path = self.dir.join(&route_name);
        staged_write_new(&route_path, &route_bytes)?;
        let route_meta = RouteMeta {
            generation,
            file: route_name,
            thread_entry_count: u64::try_from(new_threads.len())
                .map_err(|_| format_error("thread route count overflow"))?,
            checkpoint_entry_count: u64::try_from(new_checkpoints.len())
                .map_err(|_| format_error("checkpoint route count overflow"))?,
            route_file_bytes: u64::try_from(route_bytes.len())
                .map_err(|_| format_error("route file length overflow"))?,
            route_index_xxh3_64: route_hash,
        };
        let mut segments = self.manifest.segments.clone();
        segments.push(finalized.meta.clone());
        let mut routes = self.manifest.routes.clone();
        routes.push(route_meta);
        let next_manifest = Manifest {
            generation,
            sealed_end_wal_bytes: new_wal_end,
            checkpoint_count,
            version_count: version_end,
            thread_count: u64::from(next_thread_ordinal),
            stream_sizes: finalized.meta.stream_ends.clone(),
            segments,
            routes,
            store_id: self.manifest.store_id,
            deleted_checkpoints: self.manifest.deleted_checkpoints.clone(),
            retired_requests: self.manifest.retired_requests.clone(),
        };
        staged_write_new(&self.dir.join(MANIFEST_FILE), &manifest_bytes(&next_manifest)?)?;
        let coexistence = tree_storage(&self.dir)?;
        let recycle = recycle_hot_file(
            &self.dir,
            &hot_path,
            source_offset,
            parsed.logical_tail,
            self.config,
        )?;
        let suffix_len = parsed.logical_tail.saturating_sub(source_offset);
        self.hot.replace_after_recycle(suffix_len)?;
        self.manifest = next_manifest;
        if let Some(lazy) = &self.lazy_base {
            *lazy.borrow_mut() = LazyCheckpointStore::open_for_writable_store(&self.dir)?;
            self.state.base_geometry = Some(Geometry::from_manifest(&self.manifest)?);
            self.state.arena_bytes.clear();
            self.state.compact_nodes.clear();
            self.state.wide_nodes.clear();
        }
        self.range_sizes.borrow_mut().clear();
        let reclaimed = tree_storage(&self.dir)?;
        Ok(SealReport {
            generation,
            checkpoint_count,
            newly_sealed_wal_bytes: source_offset,
            hot_suffix_logical_bytes: suffix_len,
            before,
            coexistence,
            recycle_peak: recycle.peak,
            reclaimed,
        })
    }

    /// Reads and reconstructs one exact canonical checkpoint state.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint does not exist or persisted DAG bytes
    /// are structurally invalid.
    pub fn read_checkpoint(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let ordinal = self
            .state
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or_else(|| {
                if self
                    .state
                    .deleted_checkpoints
                    .contains(&(thread_id.to_owned(), checkpoint_id.to_owned()))
                {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                }
            })?;
        let index = usize::try_from(ordinal)
            .map_err(|_| format_error("checkpoint ordinal does not fit usize"))?;
        let checkpoint = self
            .state
            .checkpoints
            .get(index)
            .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?;
        if self.lazy_base.is_some() {
            self.reconstruct_checkpoint_lazy(checkpoint)
        } else {
            reconstruct_checkpoint(&self.state, checkpoint)
        }
    }

    /// Reads an exact half-open byte range from one canonical checkpoint.
    ///
    /// The returned bytes are identical to
    /// `read_checkpoint(thread_id, checkpoint_id)?[offset..offset + length]`,
    /// but the complete canonical checkpoint is not first assembled in one
    /// process-owned vector. The current persisted node format may still
    /// require additional topology reads to locate a range.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint does not exist, the range overflows
    /// or is outside the exact canonical state, or persisted DAG bytes are
    /// structurally invalid.
    pub fn read_checkpoint_range(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let ordinal = self
            .state
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or_else(|| {
                if self
                    .state
                    .deleted_checkpoints
                    .contains(&(thread_id.to_owned(), checkpoint_id.to_owned()))
                {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                }
            })?;
        let index = usize::try_from(ordinal)
            .map_err(|_| format_error("checkpoint ordinal does not fit usize"))?;
        let checkpoint = self
            .state
            .checkpoints
            .get(index)
            .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("checkpoint range end overflow"))?;
        if end > checkpoint.logical_state_len {
            return Err(format_error("checkpoint range outside canonical state"));
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        let output_len = usize::try_from(length)
            .map_err(|_| format_error("checkpoint range length exceeds usize"))?;
        let mut output = Vec::with_capacity(output_len);
        self.append_canonical_range(checkpoint, offset, end, &mut output)?;
        if output.len() != output_len {
            return Err(format_error("checkpoint range produced unexpected length"));
        }
        Ok(output)
    }

    pub(crate) fn read_identity_range(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let ordinal = self
            .state
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or_else(|| {
                if self
                    .state
                    .deleted_checkpoints
                    .contains(&(thread_id.to_owned(), checkpoint_id.to_owned()))
                {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                }
            })?;
        let index = usize::try_from(ordinal)
            .map_err(|_| format_error("checkpoint ordinal does not fit usize"))?;
        let checkpoint = self
            .state
            .checkpoints
            .get(index)
            .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?;
        if checkpoint.messages_version.is_some() || checkpoint.result_version.is_some() {
            return Err(format_error(
                "identity range requires a checkpoint without message or result channels",
            ));
        }
        let identity_len = checkpoint
            .logical_state_len
            .checked_sub(CANONICAL_STATE_PREFIX_BYTES)
            .and_then(|value| value.checked_sub(CANONICAL_STATE_SUFFIX_BYTES))
            .ok_or_else(|| format_error("canonical checkpoint is shorter than identity framing"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("identity range end overflow"))?;
        if end > identity_len {
            return Err(format_error("identity range outside canonical identity"));
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        let root = self.version_root_for_read(checkpoint.identity_version)?;
        let output_len =
            usize::try_from(length).map_err(|_| format_error("identity range exceeds usize"))?;
        let mut output = Vec::with_capacity(output_len);
        self.append_root_range_with_known_size(root, identity_len, offset, length, &mut output)?;
        if output.len() != output_len {
            return Err(format_error("identity range produced unexpected length"));
        }
        Ok(output)
    }

    /// Delivers an exact canonical checkpoint range in bounded chunks.
    ///
    /// The callback runs synchronously and the supplied slice is valid only
    /// for the duration of the callback. The callback may return a
    /// `CheckpointStoreError` to stop delivery. This path bounds the temporary
    /// output allocation to `chunk_bytes`, but does not claim that the
    /// filesystem read path touched only sublinear physical blocks.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint/range is invalid, `chunk_bytes` is
    /// zero, persisted DAG bytes are malformed, or the callback fails.
    pub fn stream_checkpoint_range(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
        offset: u64,
        length: u64,
        chunk_bytes: u64,
        sink: &mut dyn FnMut(&[u8]) -> Result<(), CheckpointStoreError>,
    ) -> Result<(), CheckpointStoreError> {
        if chunk_bytes == 0 {
            return Err(format_error("checkpoint stream chunk size must be positive"));
        }
        if length == 0 {
            self.read_checkpoint_range(thread_id, checkpoint_id, offset, 0)?;
            return Ok(());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("checkpoint stream range end overflow"))?;
        let mut cursor = offset;
        while cursor < end {
            let take = chunk_bytes.min(end - cursor);
            let chunk = self.read_checkpoint_range(thread_id, checkpoint_id, cursor, take)?;
            sink(&chunk)?;
            cursor = cursor
                .checked_add(take)
                .ok_or_else(|| format_error("checkpoint stream cursor overflow"))?;
        }
        Ok(())
    }

    pub(crate) fn stream_identity_range(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
        offset: u64,
        length: u64,
        chunk_bytes: u64,
        sink: &mut dyn FnMut(&[u8]) -> Result<(), CheckpointStoreError>,
    ) -> Result<(), CheckpointStoreError> {
        if chunk_bytes == 0 {
            return Err(format_error("identity stream chunk size must be positive"));
        }
        if length == 0 {
            self.read_identity_range(thread_id, checkpoint_id, offset, 0)?;
            return Ok(());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("identity stream range end overflow"))?;
        let mut cursor = offset;
        while cursor < end {
            let take = chunk_bytes.min(end - cursor);
            let chunk = self.read_identity_range(thread_id, checkpoint_id, cursor, take)?;
            sink(&chunk)?;
            cursor = cursor
                .checked_add(take)
                .ok_or_else(|| format_error("identity stream cursor overflow"))?;
        }
        Ok(())
    }

    /// Reconstructs every committed checkpoint and checks stored length/hash.
    ///
    /// # Errors
    ///
    /// Returns an error if the DAG cannot be structurally reconstructed.
    pub fn verify_all(&self) -> Result<VerificationReport, CheckpointStoreError> {
        let mut failures = 0u64;
        for checkpoint in &self.state.checkpoints {
            let bytes = if self.lazy_base.is_some() {
                self.reconstruct_checkpoint_lazy(checkpoint)?
            } else {
                reconstruct_checkpoint(&self.state, checkpoint)?
            };
            let len = u64::try_from(bytes.len())
                .map_err(|_| format_error("reconstructed state length overflow"))?;
            if len != checkpoint.logical_state_len || xxh3_64(&bytes) != checkpoint.state_hash {
                failures = failures.saturating_add(1);
            }
        }
        Ok(VerificationReport {
            checkpoint_count: u64::try_from(self.state.checkpoints.len())
                .map_err(|_| format_error("checkpoint count overflow"))?,
            failures,
        })
    }

    fn reconstruct_checkpoint_lazy(
        &self,
        checkpoint: &CheckpointInfo,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let identity = self.extract_version_lazy(checkpoint.identity_version)?;
        let messages = checkpoint
            .messages_version
            .map(|version| self.extract_version_lazy(version))
            .transpose()?
            .unwrap_or_default();
        let result = checkpoint
            .result_version
            .map(|version| self.extract_version_lazy(version))
            .transpose()?;
        let mut out = Vec::new();
        out.extend_from_slice(b"{\"identity\":");
        out.extend_from_slice(&identity);
        out.extend_from_slice(b",\"messages\":[");
        out.extend_from_slice(&messages);
        out.extend_from_slice(b"]");
        if let Some(result) = result {
            out.extend_from_slice(b",\"result\":");
            out.extend_from_slice(&result);
        }
        out.extend_from_slice(b"}");
        Ok(out)
    }

    fn extract_version_lazy(&self, version: u32) -> Result<Vec<u8>, CheckpointStoreError> {
        let root = self
            .state
            .versions
            .get(usize::try_from(version).map_err(|_| format_error("version index overflow"))?)
            .copied()
            .flatten()
            .ok_or_else(|| format_error("lazy version has no root"))?;
        self.extract_root_lazy(root)
    }

    fn extract_root_lazy(&self, root: u64) -> Result<Vec<u8>, CheckpointStoreError> {
        let mut out = Vec::new();
        let mut stack = vec![root];
        while let Some(node_id) = stack.pop() {
            let node = self.decode_node_lazy(node_id)?;
            if node.kind == 0 {
                let end = node
                    .a
                    .checked_add(node.b)
                    .ok_or_else(|| format_error("lazy leaf byte end overflow"))?;
                let base = self.state.lazy_base_geometry();
                if node.a < base.byte_len {
                    if end > base.byte_len {
                        return Err(format_error("lazy leaf crosses base/overlay boundary"));
                    }
                    let lazy = self
                        .lazy_base
                        .as_ref()
                        .ok_or_else(|| format_error("lazy base reader is missing"))?;
                    lazy.borrow_mut().append_stream_range(
                        PAYLOAD,
                        node.a,
                        node.b,
                        &mut out,
                    )?;
                } else {
                    let start = usize::try_from(node.a - base.byte_len)
                        .map_err(|_| format_error("lazy overlay leaf start overflow"))?;
                    let length = usize::try_from(node.b)
                        .map_err(|_| format_error("lazy overlay leaf length overflow"))?;
                    let end = start
                        .checked_add(length)
                        .ok_or_else(|| format_error("lazy overlay leaf end overflow"))?;
                    out.extend_from_slice(
                        self.state
                            .arena_bytes
                            .get(start..end)
                            .ok_or_else(|| format_error("lazy overlay leaf outside payload"))?,
                    );
                }
            } else {
                stack.push(node.b);
                stack.push(node.a);
            }
        }
        Ok(out)
    }

    fn decode_node_lazy(&self, node_id: u64) -> Result<DecodedNode, CheckpointStoreError> {
        let base = self.state.lazy_base_geometry();
        if node_id < base.node_count {
            let lazy = self
                .lazy_base
                .as_ref()
                .ok_or_else(|| format_error("lazy base reader is missing"))?;
            return lazy.borrow_mut().decode_node(node_id);
        }
        decode_node(&self.state, node_id)
    }

    fn append_canonical_range(
        &self,
        checkpoint: &CheckpointInfo,
        start: u64,
        end: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), CheckpointStoreError> {
        let identity_root = self.version_root_for_read(checkpoint.identity_version)?;
        let identity_len = self.root_term_size(identity_root)?;
        let messages_root = checkpoint
            .messages_version
            .map(|version| self.version_root_for_read(version))
            .transpose()?;
        let messages_len = messages_root
            .map(|root| self.root_term_size(root))
            .transpose()?
            .unwrap_or(0);
        let result_root = checkpoint
            .result_version
            .map(|version| self.version_root_for_read(version))
            .transpose()?;
        let result_len = result_root
            .map(|root| self.root_term_size(root))
            .transpose()?;

        let mut cursor = 0u64;
        cursor = self.append_static_range(
            b"{\"identity\":",
            cursor,
            start,
            end,
            output,
        )?;
        cursor = self.append_root_segment_range(
            identity_root,
            identity_len,
            cursor,
            start,
            end,
            output,
        )?;
        cursor = self.append_static_range(
            b",\"messages\":[",
            cursor,
            start,
            end,
            output,
        )?;
        if let Some(root) = messages_root {
            cursor = self.append_root_segment_range(
                root,
                messages_len,
                cursor,
                start,
                end,
                output,
            )?;
        }
        cursor = self.append_static_range(b"]", cursor, start, end, output)?;
        if let Some(root) = result_root {
            cursor = self.append_static_range(
                b",\"result\":",
                cursor,
                start,
                end,
                output,
            )?;
            cursor = self.append_root_segment_range(
                root,
                result_len.unwrap_or(0),
                cursor,
                start,
                end,
                output,
            )?;
        }
        cursor = self.append_static_range(b"}", cursor, start, end, output)?;
        if cursor != checkpoint.logical_state_len {
            return Err(format_error("canonical checkpoint length disagrees with metadata"));
        }
        Ok(())
    }

    fn append_static_range(
        &self,
        bytes: &[u8],
        segment_start: u64,
        request_start: u64,
        request_end: u64,
        output: &mut Vec<u8>,
    ) -> Result<u64, CheckpointStoreError> {
        let segment_len = u64::try_from(bytes.len())
            .map_err(|_| format_error("canonical static segment length overflow"))?;
        let segment_end = segment_start
            .checked_add(segment_len)
            .ok_or_else(|| format_error("canonical static segment end overflow"))?;
        let overlap_start = request_start.max(segment_start);
        let overlap_end = request_end.min(segment_end);
        if overlap_start < overlap_end {
            let local_start = usize::try_from(overlap_start - segment_start)
                .map_err(|_| format_error("canonical static range start overflow"))?;
            let local_end = usize::try_from(overlap_end - segment_start)
                .map_err(|_| format_error("canonical static range end overflow"))?;
            output.extend_from_slice(
                bytes
                    .get(local_start..local_end)
                    .ok_or_else(|| format_error("canonical static range outside segment"))?,
            );
        }
        Ok(segment_end)
    }

    fn append_root_segment_range(
        &self,
        root: u64,
        root_len: u64,
        segment_start: u64,
        request_start: u64,
        request_end: u64,
        output: &mut Vec<u8>,
    ) -> Result<u64, CheckpointStoreError> {
        let segment_end = segment_start
            .checked_add(root_len)
            .ok_or_else(|| format_error("canonical root segment end overflow"))?;
        let overlap_start = request_start.max(segment_start);
        let overlap_end = request_end.min(segment_end);
        if overlap_start < overlap_end {
            self.append_root_range(
                root,
                overlap_start - segment_start,
                overlap_end - overlap_start,
                output,
            )?;
        }
        Ok(segment_end)
    }

    fn version_root_for_read(&self, version: u32) -> Result<u64, CheckpointStoreError> {
        self.state
            .versions
            .get(usize::try_from(version).map_err(|_| format_error("version index overflow"))?)
            .copied()
            .flatten()
            .ok_or_else(|| format_error("version has no root"))
    }

    fn decode_node_for_read(&self, node_id: u64) -> Result<DecodedNode, CheckpointStoreError> {
        if self.lazy_base.is_some() {
            self.decode_node_lazy(node_id)
        } else {
            decode_node(&self.state, node_id)
        }
    }

    fn root_term_size(&self, root: u64) -> Result<u64, CheckpointStoreError> {
        let index = usize::try_from(root).map_err(|_| format_error("checkpoint root index overflow"))?;
        if let Some(cached) = self.range_sizes.borrow().get(index).and_then(|value| *value) {
            return Ok(cached);
        }
        let node = self.decode_node_for_read(root)?;
        let total = if node.kind == 0 {
            node.b
        } else {
            self.root_term_size(node.a)?
                .checked_add(self.root_term_size(node.b)?)
                .ok_or_else(|| format_error("checkpoint root term size overflow"))?
        };
        let mut cache = self.range_sizes.borrow_mut();
        if cache.len() <= index {
            cache.resize(index + 1, None);
        }
        cache[index] = Some(total);
        Ok(total)
    }

    fn append_root_range(
        &self,
        root: u64,
        offset: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), CheckpointStoreError> {
        let root_len = self.root_term_size(root)?;
        self.append_root_range_with_known_size(root, root_len, offset, length, output)
    }

    fn append_root_range_with_known_size(
        &self,
        root: u64,
        root_len: u64,
        offset: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), CheckpointStoreError> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| format_error("checkpoint root range end overflow"))?;
        if end > root_len {
            return Err(format_error("checkpoint root range outside root"));
        }
        let mut stack = vec![(root, offset, length)];
        while let Some((node_id, request_offset, request_length)) = stack.pop() {
            if request_length == 0 {
                continue;
            }
            let node = self.decode_node_for_read(node_id)?;
            if node.kind == 0 {
                if request_offset >= node.b {
                    return Err(format_error("checkpoint leaf range outside leaf"));
                }
                let take = node.b - request_offset;
                let take = take.min(request_length);
                let payload_start = node
                    .a
                    .checked_add(request_offset)
                    .ok_or_else(|| format_error("checkpoint leaf range start overflow"))?;
                self.append_payload_range(payload_start, take, output)?;
                continue;
            }
            let left_size = self.root_term_size(node.a)?;
            let request_end = request_offset
                .checked_add(request_length)
                .ok_or_else(|| format_error("checkpoint node range end overflow"))?;
            if request_end > left_size {
                let right_offset = request_offset.saturating_sub(left_size);
                let left_take = if request_offset < left_size {
                    left_size - request_offset
                } else {
                    0
                };
                let right_length = request_length.saturating_sub(left_take);
                if right_length > 0 {
                    stack.push((node.b, right_offset, right_length));
                }
            }
            if request_offset < left_size {
                let left_length = (left_size - request_offset).min(request_length);
                if left_length > 0 {
                    stack.push((node.a, request_offset, left_length));
                }
            }
        }
        Ok(())
    }

    fn append_payload_range(
        &self,
        start: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), CheckpointStoreError> {
        let base = self.state.lazy_base_geometry();
        let end = start
            .checked_add(length)
            .ok_or_else(|| format_error("checkpoint payload range end overflow"))?;
        if start < base.byte_len {
            if end > base.byte_len {
                return Err(format_error("checkpoint payload range crosses base/overlay"));
            }
            let lazy = self
                .lazy_base
                .as_ref()
                .ok_or_else(|| format_error("lazy base reader is missing"))?;
            lazy.borrow_mut()
                .append_stream_range(PAYLOAD, start, length, output)?;
        } else {
            let local_start = usize::try_from(start - base.byte_len)
                .map_err(|_| format_error("checkpoint overlay range start overflow"))?;
            let local_length = usize::try_from(length)
                .map_err(|_| format_error("checkpoint overlay range length overflow"))?;
            let local_end = local_start
                .checked_add(local_length)
                .ok_or_else(|| format_error("checkpoint overlay range end overflow"))?;
            output.extend_from_slice(
                self.state
                    .arena_bytes
                    .get(local_start..local_end)
                    .ok_or_else(|| format_error("checkpoint overlay range outside payload"))?,
            );
        }
        Ok(())
    }

    /// Returns complete current directory storage accounting.
    ///
    /// # Errors
    ///
    /// Returns an error if directory metadata cannot be read.
    pub fn storage(&self) -> Result<StoreStorage, CheckpointStoreError> {
        tree_storage(&self.dir)
    }

    /// Returns the current checkpoint count including the hot suffix.
    #[must_use]
    pub fn checkpoint_count(&self) -> usize {
        self.state.checkpoints.len()
    }

    /// Returns the current version-root count including the hot suffix.
    #[must_use]
    pub fn version_count(&self) -> usize {
        self.state.versions.len()
    }

    /// Returns the currently sealed checkpoint prefix length.
    #[must_use]
    pub fn sealed_checkpoint_count(&self) -> u64 {
        self.manifest.checkpoint_count
    }

    /// Returns the committed logical hot-WAL suffix length.
    #[must_use]
    pub fn hot_logical_bytes(&self) -> u64 {
        self.hot.logical_tail()
    }

    /// Returns the current physical hot-WAL capacity.
    ///
    /// # Errors
    ///
    /// Returns an error if WAL metadata cannot be read.
    pub fn hot_capacity_bytes(&self) -> Result<u64, CheckpointStoreError> {
        self.hot.capacity()
    }

    pub(crate) fn lazy_read_metrics(&self) -> Option<LazyReadMetrics> {
        self.lazy_base
            .as_ref()
            .map(|lazy| lazy.borrow().read_metrics())
    }

    /// Returns checkpoint metadata in commit order.
    #[must_use]
    pub fn checkpoints(&self) -> &[CheckpointInfo] {
        &self.state.checkpoints
    }
}

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

impl LazyCheckpointStore {
    /// Opens a fully sealed checkpoint-store prefix without materializing its
    /// payload or node streams into process-owned vectors.
    ///
    /// The current candidate rejects a non-empty hot suffix because its purpose
    /// is to isolate sealed-state recovery mechanics before integrating a lazy
    /// reader with the writable suffix path.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, CheckpointStoreError> {
        Self::open_internal(dir.as_ref(), false)
    }

    fn open_for_writable_store(dir: impl AsRef<Path>) -> Result<Self, CheckpointStoreError> {
        Self::open_internal(dir.as_ref(), true)
    }

    fn open_internal(dir: &Path, allow_hot_suffix: bool) -> Result<Self, CheckpointStoreError> {
        let dir = dir.to_path_buf();
        fs::create_dir_all(&dir)?;
        let reader_reclaim_lease = ReaderReclaimLease::acquire_shared(&dir)?;
        let manifest = load_manifest(&dir)?;
        let deleted_checkpoints = manifest
            .deleted_checkpoints
            .iter()
            .map(|tombstone| {
                (
                    tombstone.thread_id.clone(),
                    tombstone.checkpoint_id.clone(),
                )
            })
            .collect();
        let segments = load_lazy_segments(&dir, &manifest)?;
        let cache_capacity = lazy_block_cache_capacity()?;
        let mut reader = Self {
            _reader_reclaim_lease: reader_reclaim_lease,
            manifest,
            deleted_checkpoints,
            segments,
            metadata: LazyMetadata::default(),
            cache: HashMap::with_capacity(cache_capacity),
            cache_order: VecDeque::with_capacity(cache_capacity),
            block_entries: HashMap::with_capacity(cache_capacity),
            block_entry_order: VecDeque::with_capacity(cache_capacity),
            cache_capacity,
            payload_fast: None,
            metrics: LazyReadMetrics::default(),
        };
        if reader.manifest.generation == 0 {
            return Ok(reader);
        }
        let hot_path = dir.join(HOT_WAL_FILE);
        if !allow_hot_suffix && hot_path.exists() {
            let geometry = Geometry::from_manifest(&reader.manifest)?;
            if !parse_hot_prefix(&hot_path, geometry)?.transactions.is_empty() {
                return Err(format_error(
                    "lazy sealed reader requires a fully sealed store with no hot suffix",
                ));
            }
        }
        reader.metadata = reader.load_metadata()?;
        let payload_len = reader
            .manifest
            .stream_sizes
            .get(PAYLOAD)
            .copied()
            .ok_or_else(|| format_error("lazy payload stream is missing from manifest"))?;
        if lazy_payload_fast_path_allowed(payload_len) {
            reader.payload_fast = Some(reader.read_stream_range(PAYLOAD, 0, payload_len)?);
            reader.cache.clear();
            reader.cache_order.clear();
        }
        Ok(reader)
    }

    /// Returns the number of checkpoints in the sealed prefix.
    #[must_use]
    pub fn checkpoint_count(&self) -> usize {
        self.metadata.checkpoints.len()
    }

    /// Returns the number of versions in the sealed prefix.
    #[must_use]
    pub fn version_count(&self) -> usize {
        self.metadata.versions.len()
    }

    /// Returns checkpoint metadata in commit order.
    #[must_use]
    pub fn checkpoints(&self) -> &[CheckpointInfo] {
        &self.metadata.checkpoints
    }

    /// Returns the bounded cache capacity in blocks.
    #[must_use]
    pub const fn cache_capacity_blocks() -> usize {
        LAZY_BLOCK_CACHE_CAPACITY
    }

    /// Returns the number of currently cached decoded blocks.
    #[must_use]
    pub fn cached_block_count(&self) -> usize {
        self.cache.len()
    }

    /// Returns whether the bounded small-payload fast path is active.
    #[must_use]
    pub fn payload_fast_path_active(&self) -> bool {
        self.payload_fast.is_some()
    }

    /// Returns a snapshot of lazy-read block and byte counters.
    #[must_use]
    pub fn read_metrics(&self) -> LazyReadMetrics {
        self.metrics
    }

    /// Clears the lazy-read block and byte counters without evicting the cache.
    pub fn reset_read_metrics(&mut self) {
        self.metrics = LazyReadMetrics::default();
    }

    /// Reads and reconstructs one exact checkpoint state through the bounded
    /// sealed-block reader.
    pub fn read_checkpoint(
        &mut self,
        thread_id: &str,
        checkpoint_id: &str,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let ordinal = self
            .metadata
            .checkpoint_ordinals
            .get(&(thread_id.to_owned(), checkpoint_id.to_owned()))
            .copied()
            .ok_or_else(|| {
                if self
                    .deleted_checkpoints
                    .contains(&(thread_id.to_owned(), checkpoint_id.to_owned()))
                {
                    CheckpointStoreError::CheckpointDeleted
                } else {
                    CheckpointStoreError::CheckpointNotFound
                }
            })?;
        self.read_checkpoint_ordinal(ordinal)
    }

    /// Reconstructs every sealed checkpoint and checks stored length/hash.
    pub fn verify_all(&mut self) -> Result<VerificationReport, CheckpointStoreError> {
        self.verify_segment_index_checksums()?;
        let mut failures = 0u64;
        for ordinal in 0..self.metadata.checkpoints.len() {
            let checkpoint = self
                .metadata
                .checkpoints
                .get(ordinal)
                .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?
                .clone();
            let bytes = self.reconstruct_checkpoint(&checkpoint)?;
            let len = u64::try_from(bytes.len())
                .map_err(|_| format_error("reconstructed state length overflow"))?;
            if len != checkpoint.logical_state_len || xxh3_64(&bytes) != checkpoint.state_hash {
                failures = failures.saturating_add(1);
            }
        }
        Ok(VerificationReport {
            checkpoint_count: u64::try_from(self.metadata.checkpoints.len())
                .map_err(|_| format_error("checkpoint count overflow"))?,
            failures,
        })
    }

    fn verify_segment_index_checksums(&self) -> Result<(), CheckpointStoreError> {
        for segment in &self.segments {
            let index_bytes_len = segment
                .index
                .header
                .stream_table_bytes
                .checked_add(segment.index.header.block_table_bytes)
                .ok_or_else(|| format_error("lazy segment index length overflow"))?;
            let mut index_bytes = vec![
                0u8;
                usize::try_from(index_bytes_len)
                    .map_err(|_| format_error("lazy segment index length exceeds usize"))?
            ];
            read_file_exact_at(
                &segment.file,
                &mut index_bytes,
                segment.index.header.index_offset,
            )?;
            if xxh3_64(&index_bytes) != segment.index.header.index_xxh3_64 {
                return Err(format_error("lazy segment index checksum mismatch"));
            }
        }
        Ok(())
    }

    fn load_metadata(&mut self) -> Result<LazyMetadata, CheckpointStoreError> {
        let roots = self.read_stream_all(VERSION_ROOT)?;
        let parents = self.read_stream_all(VERSION_PARENT)?;
        if roots.len() % 8 != 0 || parents.len() % 4 != 0 || roots.len() / 8 != parents.len() / 4 {
            return Err(format_error("lazy version column widths disagree"));
        }
        let mut versions = Vec::with_capacity(roots.len() / 8);
        let mut version_parents = Vec::with_capacity(parents.len() / 4);
        for index in 0..roots.len() / 8 {
            let raw_root = read_u64(&roots, index * 8)?;
            let raw_parent = read_u32(&parents, index * 4)?;
            versions.push(if raw_root == NONE_ROOT { None } else { Some(raw_root) });
            version_parents.push(if raw_parent == NONE_PARENT { None } else { Some(raw_parent) });
        }
        if versions.len() != usize::try_from(self.manifest.version_count)
            .map_err(|_| format_error("lazy version count overflow"))?
        {
            return Err(format_error("lazy version count disagrees with manifest"));
        }

        let threads = decode_string_table(
            &self.read_stream_all(THREAD_OFFSETS)?,
            &self.read_stream_all(THREAD_BYTES)?,
        )?;
        if threads.len() != usize::try_from(self.manifest.thread_count)
            .map_err(|_| format_error("lazy thread count overflow"))?
        {
            return Err(format_error("lazy thread count disagrees with manifest"));
        }
        let mut thread_ordinals = HashMap::with_capacity(threads.len());
        for (index, thread) in threads.iter().enumerate() {
            thread_ordinals.insert(
                thread.clone(),
                u32::try_from(index).map_err(|_| format_error("thread ordinal overflow"))?,
            );
        }

        let checkpoint_count = usize::try_from(self.manifest.checkpoint_count)
            .map_err(|_| format_error("lazy checkpoint count overflow"))?;
        let checkpoint_ids = decode_string_table(
            &self.read_stream_all(CP_ID_OFFSETS)?,
            &self.read_stream_all(CP_ID_BYTES)?,
        )?;
        if checkpoint_ids.len() != checkpoint_count {
            return Err(format_error("lazy checkpoint id table count mismatch"));
        }
        let cp_thread = self.read_stream_all(CP_THREAD)?;
        let cp_no = self.read_stream_all(CP_NO)?;
        let cp_parent = self.read_stream_all(CP_PARENT_ORDINAL)?;
        let cp_identity = self.read_stream_all(CP_IDENTITY_VERSION)?;
        let cp_messages = self.read_stream_all(CP_MESSAGES_VERSION)?;
        let cp_result = self.read_stream_all(CP_RESULT_VERSION)?;
        let cp_len = self.read_stream_all(CP_LOGICAL_LEN)?;
        let cp_hash = self.read_stream_all(CP_STATE_HASH)?;
        for (bytes, width) in [
            (&cp_thread, 4usize),
            (&cp_no, 4),
            (&cp_parent, 4),
            (&cp_identity, 4),
            (&cp_messages, 4),
            (&cp_result, 4),
            (&cp_len, 8),
            (&cp_hash, 8),
        ] {
            if bytes.len() != checkpoint_count.checked_mul(width).ok_or_else(|| {
                format_error("lazy checkpoint metadata width overflow")
            })? {
                return Err(format_error("lazy checkpoint metadata width mismatch"));
            }
        }
        let mut checkpoints = Vec::with_capacity(checkpoint_count);
        let mut checkpoint_ordinals = HashMap::with_capacity(checkpoint_count);
        for index in 0..checkpoint_count {
            let thread_ordinal = read_u32(&cp_thread, index * 4)?;
            let thread = threads
                .get(usize::try_from(thread_ordinal).map_err(|_| format_error("thread ordinal overflow"))?)
                .cloned()
                .ok_or_else(|| format_error("lazy checkpoint thread ordinal outside table"))?;
            let parent_ordinal = read_u32(&cp_parent, index * 4)?;
            let parent_checkpoint_id = if parent_ordinal == NONE_PARENT {
                None
            } else {
                let parent_index = usize::try_from(parent_ordinal)
                    .map_err(|_| format_error("lazy parent ordinal overflow"))?;
                if parent_index >= index {
                    return Err(format_error("lazy checkpoint parent is not prior"));
                }
                Some(
                    checkpoint_ids
                        .get(parent_index)
                        .cloned()
                        .ok_or_else(|| format_error("lazy parent checkpoint id missing"))?,
                )
            };
            let optional = |raw: u32| if raw == NONE_VERSION { None } else { Some(raw) };
            let checkpoint_id = checkpoint_ids
                .get(index)
                .cloned()
                .ok_or_else(|| format_error("lazy checkpoint id missing"))?;
            let info = CheckpointInfo {
                ordinal: u32::try_from(index).map_err(|_| format_error("checkpoint ordinal overflow"))?,
                thread_id: thread.clone(),
                checkpoint_no: read_u32(&cp_no, index * 4)?,
                checkpoint_id: checkpoint_id.clone(),
                parent_checkpoint_id,
                identity_version: read_u32(&cp_identity, index * 4)?,
                messages_version: optional(read_u32(&cp_messages, index * 4)?),
                result_version: optional(read_u32(&cp_result, index * 4)?),
                logical_state_len: read_u64(&cp_len, index * 8)?,
                state_hash: read_u64(&cp_hash, index * 8)?,
            };
            checkpoint_ordinals.insert((thread, checkpoint_id), info.ordinal);
            checkpoints.push(info);
        }
        let node_count = self.manifest.stream_sizes[NODE_KIND];
        if self.manifest.stream_sizes[NODE_FIELD0]
            != node_count
                .checked_mul(8)
                .ok_or_else(|| format_error("lazy node field0 width overflow"))?
            || self.manifest.stream_sizes[NODE_FIELD1]
                != node_count
                    .checked_mul(4)
                    .ok_or_else(|| format_error("lazy node field1 width overflow"))?
        {
            return Err(format_error("lazy compact node column widths disagree"));
        }
        let wide_count = self.manifest.stream_sizes[WIDE_KIND];
        for stream_id in [WIDE_A, WIDE_B, WIDE_C] {
            if self.manifest.stream_sizes[stream_id]
                != wide_count
                    .checked_mul(8)
                    .ok_or_else(|| format_error("lazy wide column width overflow"))?
            {
                return Err(format_error("lazy wide node column widths disagree"));
            }
        }
        if std::env::var_os("TULYA_LAZY_EAGER_TOPOLOGY_DIAGNOSTIC").is_some() {
            for stream_id in [NODE_KIND, NODE_FIELD0, NODE_FIELD1, WIDE_KIND, WIDE_A, WIDE_B, WIDE_C] {
                let _ = self.read_stream_all(stream_id)?;
            }
        }
        Ok(LazyMetadata {
            versions,
            version_parents,
            threads,
            thread_ordinals,
            checkpoints,
            checkpoint_ordinals,
        })
    }

    fn read_checkpoint_ordinal(&mut self, ordinal: u32) -> Result<Vec<u8>, CheckpointStoreError> {
        let index = usize::try_from(ordinal).map_err(|_| format_error("checkpoint ordinal overflow"))?;
        let checkpoint = self
            .metadata
            .checkpoints
            .get(index)
            .ok_or_else(|| format_error("checkpoint ordinal outside metadata"))?
            .clone();
        self.reconstruct_checkpoint(&checkpoint)
    }

    fn reconstruct_checkpoint(
        &mut self,
        checkpoint: &CheckpointInfo,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let identity = self.extract_version(checkpoint.identity_version)?;
        let messages = checkpoint
            .messages_version
            .map(|version| self.extract_version(version))
            .transpose()?
            .unwrap_or_default();
        let result = checkpoint
            .result_version
            .map(|version| self.extract_version(version))
            .transpose()?;
        let mut out = Vec::new();
        out.extend_from_slice(b"{\"identity\":");
        out.extend_from_slice(&identity);
        out.extend_from_slice(b",\"messages\":[");
        out.extend_from_slice(&messages);
        out.extend_from_slice(b"]");
        if let Some(result) = result {
            out.extend_from_slice(b",\"result\":");
            out.extend_from_slice(&result);
        }
        out.extend_from_slice(b"}");
        Ok(out)
    }

    fn extract_version(&mut self, version: u32) -> Result<Vec<u8>, CheckpointStoreError> {
        let index = usize::try_from(version).map_err(|_| format_error("version index overflow"))?;
        let root = self
            .metadata
            .versions
            .get(index)
            .copied()
            .flatten()
            .ok_or_else(|| format_error("version has no root"))?;
        self.extract_root(root)
    }

    fn extract_root(&mut self, root: u64) -> Result<Vec<u8>, CheckpointStoreError> {
        let mut out = Vec::new();
        let mut stack = vec![root];
        while let Some(node_id) = stack.pop() {
            let node = self.decode_node(node_id)?;
            if node.kind == 0 {
                let start = node.a;
                let length = node.b;
                let end = start
                    .checked_add(length)
                    .ok_or_else(|| format_error("lazy leaf byte end overflow"))?;
                self.append_stream_range(
                    PAYLOAD,
                    start,
                    end.checked_sub(start).ok_or_else(|| format_error("lazy leaf range underflow"))?,
                    &mut out,
                )?;
            } else {
                stack.push(node.b);
                stack.push(node.a);
            }
        }
        Ok(out)
    }

    fn decode_node(&mut self, node_id: u64) -> Result<DecodedNode, CheckpointStoreError> {
        let node_count = self.manifest.stream_sizes[NODE_KIND];
        if node_id >= node_count {
            return Err(format_error("lazy node id outside committed nodes"));
        }
        let field0_offset = node_id
            .checked_mul(8)
            .ok_or_else(|| format_error("lazy node field0 offset overflow"))?;
        let field1_offset = node_id
            .checked_mul(4)
            .ok_or_else(|| format_error("lazy node field1 offset overflow"))?;
        let kind = self
            .read_stream_range(NODE_KIND, node_id, 1)?
            .first()
            .copied()
            .ok_or_else(|| format_error("lazy node kind missing"))?;
        match kind {
            0 => {
                let field0 = self.read_stream_range(NODE_FIELD0, field0_offset, 8)?;
                let field1 = self.read_stream_range(NODE_FIELD1, field1_offset, 4)?;
                Ok(DecodedNode {
                    kind: 0,
                    a: read_u64(&field0, 0)?,
                    b: u64::from(read_u32(&field1, 0)?),
                })
            }
            1 => {
                let field0 = self.read_stream_range(NODE_FIELD0, field0_offset, 8)?;
                let left_delta = u64::from(read_u32(&field0, 0)?);
                let right_delta = u64::from(read_u32(&field0, 4)?);
                if left_delta == 0 || right_delta == 0 || left_delta > node_id || right_delta > node_id {
                    return Err(format_error("lazy compact binary delta underflow"));
                }
                Ok(DecodedNode { kind: 1, a: node_id - left_delta, b: node_id - right_delta })
            }
            2 => {
                let field0 = self.read_stream_range(NODE_FIELD0, field0_offset, 8)?;
                let wide_index = read_u64(&field0, 0)?;
                if wide_index >= self.manifest.stream_sizes[WIDE_KIND] {
                    return Err(format_error("lazy wide index outside committed records"));
                }
                let wide_offset = wide_index
                    .checked_mul(8)
                    .ok_or_else(|| format_error("lazy wide offset overflow"))?;
                let wide_kind = self
                    .read_stream_range(WIDE_KIND, wide_index, 1)?
                    .first()
                    .copied()
                    .ok_or_else(|| format_error("lazy wide kind missing"))?;
                let wide_a = self.read_stream_range(WIDE_A, wide_offset, 8)?;
                let wide_b = self.read_stream_range(WIDE_B, wide_offset, 8)?;
                match wide_kind {
                    0 => Ok(DecodedNode {
                        kind: 0,
                        a: read_u64(&wide_a, 0)?,
                        b: read_u64(&wide_b, 0)?,
                    }),
                    1 => {
                        let left = read_u64(&wide_a, 0)?;
                        let right = read_u64(&wide_b, 0)?;
                        if left >= node_id || right >= node_id {
                            return Err(format_error("lazy wide binary child not prior"));
                        }
                        Ok(DecodedNode { kind: 1, a: left, b: right })
                    }
                    _ => Err(format_error("lazy wide node kind invalid")),
                }
            }
            _ => Err(format_error("lazy node kind invalid")),
        }
    }

    fn read_stream_all(&mut self, stream_id: usize) -> Result<Vec<u8>, CheckpointStoreError> {
        let length = *self
            .manifest
            .stream_sizes
            .get(stream_id)
            .ok_or_else(|| format_error("lazy stream id outside manifest"))?;
        self.read_stream_range(stream_id, 0, length)
    }

    fn read_stream_range(
        &mut self,
        stream_id: usize,
        start: u64,
        length: u64,
    ) -> Result<Vec<u8>, CheckpointStoreError> {
        let output_capacity = usize::try_from(length)
            .map_err(|_| format_error("lazy stream range exceeds usize"))?;
        let mut output = Vec::with_capacity(output_capacity);
        self.append_stream_range(stream_id, start, length, &mut output)?;
        Ok(output)
    }

    fn append_stream_range(
        &mut self,
        stream_id: usize,
        start: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), CheckpointStoreError> {
        let stream_size = *self
            .manifest
            .stream_sizes
            .get(stream_id)
            .ok_or_else(|| format_error("lazy stream id outside manifest"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| format_error("lazy stream range overflow"))?;
        if end > stream_size {
            return Err(format_error("lazy stream range outside manifest"));
        }
        let output_start = output.len();
        if length == 0 {
            return Ok(());
        }
        if stream_id == PAYLOAD {
            if let Some(payload) = self.payload_fast.as_ref() {
                let start = usize::try_from(start)
                    .map_err(|_| format_error("lazy fast payload start exceeds usize"))?;
                let end = usize::try_from(end)
                    .map_err(|_| format_error("lazy fast payload end exceeds usize"))?;
                output.extend_from_slice(
                    payload
                        .get(start..end)
                        .ok_or_else(|| {
                            format_error("lazy fast payload range outside decoded stream")
                        })?,
                );
                return Ok(());
            }
        }
        let mut cursor = start;
        while cursor < end {
            let (segment_index, stream_entry) = self
                .segments
                .iter()
                .enumerate()
                .find_map(|(index, segment)| {
                    let entry = segment.index.streams.get(stream_id).copied()?;
                    (entry.global_start <= cursor && cursor < entry.global_end)
                        .then_some((index, entry))
                })
                .ok_or_else(|| format_error("lazy stream range has no sealed segment"))?;
            let segment_end = end.min(stream_entry.global_end);
            let local_start = cursor
                .checked_sub(stream_entry.global_start)
                .ok_or_else(|| format_error("lazy stream local offset underflow"))?;
            let local_end = segment_end
                .checked_sub(stream_entry.global_start)
                .ok_or_else(|| format_error("lazy stream local end underflow"))?;
            let block_count = u64::from(stream_entry.block_count);
            if block_count == 0 {
                return Err(format_error("lazy stream has no blocks"));
            }
            let block_size = u64::from(
                self.segments
                    .get(segment_index)
                    .ok_or_else(|| format_error("lazy segment index outside reader"))?
                    .index
                    .header
                    .block_size,
            );
            let mut local_cursor = local_start;
            while local_cursor < local_end {
                let block_position = (local_cursor / block_size).min(block_count - 1);
                let block_index = stream_entry
                    .first_block
                    .checked_add(
                        u32::try_from(block_position)
                            .map_err(|_| format_error("lazy block position overflow"))?,
                    )
                    .ok_or_else(|| format_error("lazy block index overflow"))?;
                let block = self.read_block_entry(segment_index, block_index)?;
                if usize::try_from(block.stream_id).map_err(|_| format_error("lazy block stream id overflow"))? != stream_id {
                    return Err(format_error("lazy block belongs to wrong stream"));
                }
                let block_end_raw = block
                    .raw_offset
                    .checked_add(u64::from(block.raw_len))
                    .ok_or_else(|| format_error("lazy block raw end overflow"))?;
                if block.raw_offset > local_cursor
                    || block_end_raw <= local_cursor
                    || block_end_raw > stream_entry.raw_len
                {
                    return Err(format_error("lazy block exceeds stream raw length"));
                }
                let decoded = self.load_block(
                    segment_index,
                    usize::try_from(block_index)
                        .map_err(|_| format_error("lazy block index exceeds usize"))?,
                    block,
                )?;
                let copy_start = usize::try_from(local_cursor - block.raw_offset)
                    .map_err(|_| format_error("lazy block copy start overflow"))?;
                let copy_end = usize::try_from((local_end.min(block_end_raw)) - block.raw_offset)
                    .map_err(|_| format_error("lazy block copy end overflow"))?;
                output.extend_from_slice(
                    decoded
                        .get(copy_start..copy_end)
                        .ok_or_else(|| format_error("lazy block copy range outside decoded block"))?,
                );
                local_cursor = stream_entry.global_start
                    .checked_add(block_end_raw.min(local_end))
                    .and_then(|global| global.checked_sub(stream_entry.global_start))
                    .ok_or_else(|| format_error("lazy stream cursor overflow"))?;
            }
            cursor = segment_end;
        }
        if output.len().saturating_sub(output_start)
            != usize::try_from(length).map_err(|_| format_error("lazy stream range exceeds usize"))?
        {
            return Err(format_error("lazy stream range returned unexpected length"));
        }
        Ok(())
    }

    fn read_block_entry(
        &mut self,
        segment_index: usize,
        block_index: u32,
    ) -> Result<BlockEntry, CheckpointStoreError> {
        let block_index_usize = usize::try_from(block_index)
            .map_err(|_| format_error("lazy block index exceeds usize"))?;
        let key = (segment_index, block_index_usize);
        if let Some(entry) = self.block_entries.get(&key).copied() {
            self.metrics.block_entry_cache_hits = self.metrics.block_entry_cache_hits.saturating_add(1);
            return Ok(entry);
        }
        let entry = {
            let segment = self
                .segments
                .get(segment_index)
                .ok_or_else(|| format_error("lazy segment index outside reader"))?;
            read_lazy_block_entry(&segment.file, &segment.index, block_index)?
        };
        self.metrics.block_entry_cache_misses = self.metrics.block_entry_cache_misses.saturating_add(1);
        self.metrics.block_entry_bytes_read = self
            .metrics
            .block_entry_bytes_read
            .saturating_add(u64::try_from(BLOCK_ENTRY_SIZE).map_err(|_| format_error("block entry size exceeds u64"))?);
        self.block_entries.insert(key, entry);
        self.block_entry_order.push_back(key);
        if self.block_entries.len() > self.cache_capacity {
            let evicted = self
                .block_entry_order
                .pop_front()
                .ok_or_else(|| format_error("lazy block-entry eviction order is empty"))?;
            self.block_entries.remove(&evicted);
        }
        Ok(entry)
    }

    fn load_block(
        &mut self,
        segment_index: usize,
        block_index: usize,
        block: BlockEntry,
    ) -> Result<&[u8], CheckpointStoreError> {
        let key = (segment_index, block_index);
        if self.cache.contains_key(&key) {
            self.metrics.cache_hits = self.metrics.cache_hits.saturating_add(1);
            return self
                .cache
                .get(&key)
                .map(Vec::as_slice)
                .ok_or_else(|| format_error("lazy cache entry disappeared"));
        }
        let decoded = {
            let segment = self
                .segments
                .get_mut(segment_index)
                .ok_or_else(|| format_error("lazy segment index outside reader"))?;
            let payload_offset = segment
                .index
                .header
                .payload_offset
                .checked_add(block.encoded_offset)
                .ok_or_else(|| format_error("lazy encoded block offset overflow"))?;
            let mut encoded = vec![
                0u8;
                usize::try_from(block.encoded_len)
                    .map_err(|_| format_error("lazy encoded block length overflow"))?
            ];
            read_file_exact_at(&segment.file, &mut encoded, payload_offset)?;
            segment.decompressor.decompress(
                &encoded,
                usize::try_from(block.raw_len)
                    .map_err(|_| format_error("lazy raw block length overflow"))?,
            )?
        };
        if decoded.len() != usize::try_from(block.raw_len).map_err(|_| format_error("lazy raw block length overflow"))?
            || xxh3_64(&decoded) != block.raw_xxh3_64
        {
            return Err(format_error("lazy sealed block decompression/hash mismatch"));
        }
        self.metrics.cache_misses = self.metrics.cache_misses.saturating_add(1);
        self.metrics.encoded_bytes_read = self
            .metrics
            .encoded_bytes_read
            .saturating_add(u64::from(block.encoded_len));
        self.metrics.raw_bytes_decompressed = self
            .metrics
            .raw_bytes_decompressed
            .saturating_add(u64::from(block.raw_len));
        self.cache.insert(key, decoded);
        self.cache_order.push_back(key);
        if self.cache.len() > self.cache_capacity {
            let evicted = self
                .cache_order
                .pop_front()
                .ok_or_else(|| format_error("lazy cache eviction order is empty"))?;
            self.cache.remove(&evicted);
        }
        self.cache
            .get(&key)
            .map(Vec::as_slice)
            .ok_or_else(|| format_error("lazy cache insertion failed"))
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Geometry {
    byte_len: u64,
    node_count: u64,
    wide_count: u64,
    version_count: u64,
    checkpoint_count: u64,
}

impl Geometry {
    fn from_manifest(manifest: &Manifest) -> Result<Self, CheckpointStoreError> {
        if manifest.stream_sizes.len() != STREAM_NAMES.len() {
            return Err(format_error("manifest stream width mismatch"));
        }
        Ok(Self {
            byte_len: manifest.stream_sizes[PAYLOAD],
            node_count: manifest.stream_sizes[NODE_KIND],
            wide_count: manifest.stream_sizes[WIDE_KIND],
            version_count: manifest.version_count,
            checkpoint_count: manifest.checkpoint_count,
        })
    }

    fn previous_generation(manifest: &Manifest) -> Result<Option<Self>, CheckpointStoreError> {
        let Some(last) = manifest.segments.last() else {
            return Ok(None);
        };
        if last.stream_starts.len() != STREAM_NAMES.len() {
            return Err(format_error("last segment stream-start width mismatch"));
        }
        Ok(Some(Self {
            byte_len: last.stream_starts[PAYLOAD],
            node_count: last.stream_starts[NODE_KIND],
            wide_count: last.stream_starts[WIDE_KIND],
            version_count: last.version_start_count,
            checkpoint_count: last.checkpoint_start_count,
        }))
    }

    fn from_state(state: &StoreState) -> Result<Self, CheckpointStoreError> {
        state.geometry()
    }

    fn advance(&mut self, tx: &ParsedTransaction) {
        self.byte_len = tx.byte_end;
        self.node_count = tx.node_end;
        self.wide_count = tx.wide_end;
        self.version_count = tx.version_count;
        self.checkpoint_count = tx.checkpoint_count;
    }
}

#[derive(Debug, Clone)]
struct ParsedTransaction {
    start_offset: u64,
    end_offset: u64,
    version_start: u64,
    version_count: u64,
    checkpoint_count: u64,
    byte_start: u64,
    byte_end: u64,
    bytes: Vec<u8>,
    node_start: u64,
    node_end: u64,
    compact_nodes: Vec<u8>,
    wide_start: u64,
    wide_end: u64,
    wide_nodes: Vec<u8>,
    roots: Vec<Option<u64>>,
    parents: Vec<Option<u32>>,
    checkpoint: CheckpointInfo,
    request_id: Option<Vec<u8>>,
    operation_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestRecord {
    key: Vec<u8>,
    operation_digest: [u8; 32],
    checkpoint_ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckpointTombstone {
    thread_id: String,
    checkpoint_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetiredRequestRecord {
    key: Vec<u8>,
    operation_digest: [u8; 32],
}

#[derive(Default)]
struct StoreState {
    /// Geometry already represented by the lazy sealed base. The byte/node
    /// vectors below contain only the writable overlay when this is present.
    base_geometry: Option<Geometry>,
    arena_bytes: Vec<u8>,
    compact_nodes: Vec<u8>,
    wide_nodes: Vec<u8>,
    versions: Vec<Option<u64>>,
    parents: Vec<Option<u32>>,
    checkpoints: Vec<CheckpointInfo>,
    threads: Vec<String>,
    thread_ordinals: HashMap<String, u32>,
    checkpoint_ordinals: HashMap<(String, String), u32>,
    request_records: HashMap<Vec<u8>, RequestRecord>,
    deleted_checkpoints: HashSet<(String, String)>,
    retired_requests: HashMap<Vec<u8>, [u8; 32]>,
}

impl StoreState {
    fn geometry(&self) -> Result<Geometry, CheckpointStoreError> {
        let base = self.base_geometry.unwrap_or_default();
        Ok(Geometry {
            byte_len: base
                .byte_len
                .checked_add(u64::try_from(self.arena_bytes.len()).map_err(|_| {
                    format_error("overlay arena byte length overflow")
                })?)
                .ok_or_else(|| format_error("combined arena byte length overflow"))?,
            node_count: base
                .node_count
                .checked_add(
                    u64::try_from(self.compact_nodes.len() / COMPACT_NODE_SIZE)
                        .map_err(|_| format_error("overlay node count overflow"))?,
                )
                .ok_or_else(|| format_error("combined node count overflow"))?,
            wide_count: base
                .wide_count
                .checked_add(
                    u64::try_from(self.wide_nodes.len() / WIDE_RECORD_SIZE)
                        .map_err(|_| format_error("overlay wide count overflow"))?,
                )
                .ok_or_else(|| format_error("combined wide count overflow"))?,
            version_count: u64::try_from(self.versions.len())
                .map_err(|_| format_error("version count overflow"))?,
            checkpoint_count: u64::try_from(self.checkpoints.len())
                .map_err(|_| format_error("checkpoint count overflow"))?,
        })
    }

    fn lazy_base_geometry(&self) -> Geometry {
        self.base_geometry.unwrap_or_default()
    }
}

#[derive(Default)]
struct LazyMetadata {
    versions: Vec<Option<u64>>,
    version_parents: Vec<Option<u32>>,
    threads: Vec<String>,
    thread_ordinals: HashMap<String, u32>,
    checkpoints: Vec<CheckpointInfo>,
    checkpoint_ordinals: HashMap<(String, String), u32>,
}

struct LazySegment {
    index: LazySegmentIndex,
    file: File,
    decompressor: zstd::bulk::Decompressor<'static>,
}

struct LazySegmentIndex {
    header: SegmentHeader,
    streams: Vec<StreamEntry>,
}

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
                return Err(format_error("lazy segment stream entry disagrees with manifest"));
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
    file.set_len(to)?;
    file.seek(SeekFrom::Start(from))?;
    let zeros = vec![0u8; chunk_bytes];
    let mut cursor = from;
    while cursor < to {
        let remaining = to - cursor;
        let write_len = usize::try_from(
            remaining.min(
                u64::try_from(zeros.len())
                    .map_err(|_| format_error("zero-buffer length overflow"))?,
            ),
        )
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

fn reclaim_worker_poll(
    dir: &Path,
    stats: &Arc<Mutex<CheckpointReclaimWorkerStats>>,
) -> bool {
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
        remaining = remaining
            .get_mut(read..)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "positional read exceeded buffer"))?;
        cursor = cursor
            .checked_add(u64::try_from(read).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "positional read length overflow")
            })?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "positional read offset overflow"))?;
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
        remaining = remaining
            .get_mut(read..)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "positional read exceeded buffer"))?;
        cursor = cursor
            .checked_add(u64::try_from(read).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "positional read length overflow")
            })?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "positional read offset overflow"))?;
    }
    Ok(())
}

fn read_u32(data: &[u8], offset: usize) -> Result<u32, CheckpointStoreError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| format_error("u32 offset overflow"))?;
    let bytes: [u8; 4] = data
        .get(offset..end)
        .ok_or_else(|| format_error("truncated u32"))?
        .try_into()
        .map_err(|_| format_error("u32 slice width mismatch"))?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(data: &[u8], offset: usize) -> Result<u64, CheckpointStoreError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| format_error("u64 offset overflow"))?;
    let bytes: [u8; 8] = data
        .get(offset..end)
        .ok_or_else(|| format_error("truncated u64"))?
        .try_into()
        .map_err(|_| format_error("u64 slice width mismatch"))?;
    Ok(u64::from_le_bytes(bytes))
}

fn parse_root(
    record: &[u8],
    expected_id: u64,
) -> Result<(Option<u64>, Option<u32>), CheckpointStoreError> {
    if record.len() != ROOT_RECORD_SIZE || record.get(..4) != Some(ROOT_MAGIC.as_slice()) {
        return Err(format_error("invalid root record"));
    }
    if xxh3_64(
        record
            .get(..24)
            .ok_or_else(|| format_error("truncated root checksum input"))?,
    ) != read_u64(record, 24)?
    {
        return Err(format_error("root checksum mismatch"));
    }
    let id = u64::from(read_u32(record, 4)?);
    if id != expected_id {
        return Err(format_error("root id is not sequential"));
    }
    let raw_root = read_u64(record, 8)?;
    let raw_parent = read_u32(record, 16)?;
    let root = if raw_root == NONE_ROOT { None } else { Some(raw_root) };
    let parent = if raw_parent == NONE_PARENT {
        None
    } else if u64::from(raw_parent) < id {
        Some(raw_parent)
    } else {
        return Err(format_error("root parent is not topologically prior"));
    };
    Ok((root, parent))
}

fn parse_checkpoint_record(
    record: &[u8],
    version_count: u64,
    ordinal: u32,
) -> Result<CheckpointInfo, CheckpointStoreError> {
    if record.len() < CHECKPOINT_PREFIX_SIZE + 8
        || record.get(..4) != Some(CHECKPOINT_MAGIC.as_slice())
    {
        return Err(format_error("invalid checkpoint record"));
    }
    if usize::try_from(read_u32(record, 4)?)
        .map_err(|_| format_error("checkpoint record length overflow"))?
        != record.len()
    {
        return Err(format_error("checkpoint record length mismatch"));
    }
    let checksum_at = record.len() - 8;
    if xxh3_64(
        record
            .get(..checksum_at)
            .ok_or_else(|| format_error("checkpoint checksum input outside record"))?,
    ) != read_u64(record, checksum_at)?
    {
        return Err(format_error("checkpoint checksum mismatch"));
    }
    let checkpoint_no = read_u32(record, 8)?;
    let identity = read_u32(record, 12)?;
    let messages = read_u32(record, 16)?;
    let result = read_u32(record, 20)?;
    if u64::from(identity) >= version_count {
        return Err(format_error("identity version out of bounds"));
    }
    let optional_version = |raw: u32| -> Result<Option<u32>, CheckpointStoreError> {
        if raw == NONE_VERSION {
            Ok(None)
        } else if u64::from(raw) < version_count {
            Ok(Some(raw))
        } else {
            Err(format_error("optional checkpoint version out of bounds"))
        }
    };
    let thread_len = usize::try_from(read_u32(record, 24)?)
        .map_err(|_| format_error("thread length overflow"))?;
    let checkpoint_len = usize::try_from(read_u32(record, 28)?)
        .map_err(|_| format_error("checkpoint-id length overflow"))?;
    let parent_len = usize::try_from(read_u32(record, 32)?)
        .map_err(|_| format_error("parent-id length overflow"))?;
    if thread_len == 0 || thread_len > MAX_CHECKPOINT_IDENTIFIER_BYTES {
        return Err(format_error("thread id is empty or exceeds the byte limit"));
    }
    if checkpoint_len == 0 || checkpoint_len > MAX_CHECKPOINT_IDENTIFIER_BYTES {
        return Err(format_error(
            "checkpoint id is empty or exceeds the byte limit",
        ));
    }
    if parent_len > MAX_CHECKPOINT_IDENTIFIER_BYTES {
        return Err(format_error("parent checkpoint id exceeds the byte limit"));
    }
    let logical_state_len = read_u64(record, 36)?;
    let state_hash = read_u64(record, 44)?;
    let payload_end = CHECKPOINT_PREFIX_SIZE
        .checked_add(thread_len)
        .and_then(|n| n.checked_add(checkpoint_len))
        .and_then(|n| n.checked_add(parent_len))
        .ok_or_else(|| format_error("checkpoint payload length overflow"))?;
    if payload_end + 8 != record.len() {
        return Err(format_error("checkpoint payload geometry mismatch"));
    }
    let mut cursor = CHECKPOINT_PREFIX_SIZE;
    let thread_end = cursor + thread_len;
    let thread_id = std::str::from_utf8(
        record
            .get(cursor..thread_end)
            .ok_or_else(|| format_error("thread bytes outside checkpoint record"))?,
    )
    .map_err(|_| format_error("thread id is not UTF-8"))?
    .to_owned();
    cursor = thread_end;
    let checkpoint_end = cursor + checkpoint_len;
    let checkpoint_id = std::str::from_utf8(
        record
            .get(cursor..checkpoint_end)
            .ok_or_else(|| format_error("checkpoint id bytes outside record"))?,
    )
    .map_err(|_| format_error("checkpoint id is not UTF-8"))?
    .to_owned();
    cursor = checkpoint_end;
    let parent_end = cursor + parent_len;
    let parent = std::str::from_utf8(
        record
            .get(cursor..parent_end)
            .ok_or_else(|| format_error("parent id bytes outside record"))?,
    )
    .map_err(|_| format_error("parent checkpoint id is not UTF-8"))?
    .to_owned();
    Ok(CheckpointInfo {
        ordinal,
        thread_id,
        checkpoint_no,
        checkpoint_id,
        parent_checkpoint_id: if parent.is_empty() { None } else { Some(parent) },
        identity_version: identity,
        messages_version: optional_version(messages)?,
        result_version: optional_version(result)?,
        logical_state_len,
        state_hash,
    })
}

fn validate_request_id(request_id: &[u8]) -> Result<(), CheckpointStoreError> {
    if request_id.is_empty() || request_id.len() > MAX_REQUEST_ID_BYTES {
        return Err(format_error("request id is empty or exceeds the byte limit"));
    }
    Ok(())
}

fn validate_checkpoint_identifier(value: &str, label: &str) -> Result<(), CheckpointStoreError> {
    if value.is_empty() || value.len() > MAX_CHECKPOINT_IDENTIFIER_BYTES {
        return Err(format_error(format!("{label} is empty or exceeds the byte limit")));
    }
    Ok(())
}

fn encode_message_append_transaction(
    geometry: &Geometry,
    first_checkpoint: Option<&CheckpointInfo>,
    thread_id: &str,
    checkpoint_id: &str,
    checkpoint_no: u32,
    parent_checkpoint_id: Option<&str>,
    parent: Option<(u32, u64, u32)>,
    message_body: &[u8],
    canonical_state_len: u64,
    canonical_state_hash: u64,
) -> Result<Vec<u8>, CheckpointStoreError> {
    validate_checkpoint_identifier(thread_id, "thread id")?;
    validate_checkpoint_identifier(checkpoint_id, "checkpoint id")?;
    if let Some(parent_id) = parent_checkpoint_id {
        validate_checkpoint_identifier(parent_id, "parent checkpoint id")?;
    }
    if message_body.is_empty() {
        return Err(format_error("message checkpoint body is empty"));
    }

    let version_start = u32::try_from(geometry.version_count)
        .map_err(|_| format_error("version count exceeds the T2W1 u32 limit"))?;
    let checkpoint_count = geometry
        .checkpoint_count
        .checked_add(1)
        .ok_or_else(|| format_error("checkpoint count overflows the T2W1 u64 limit"))?;
    let checkpoint_count = u32::try_from(checkpoint_count)
        .map_err(|_| format_error("checkpoint count exceeds the T2W1 u32 limit"))?;

    let mut payload = Vec::new();
    let mut compact_nodes = Vec::<[u8; COMPACT_NODE_SIZE]>::new();
    let mut roots = Vec::<(u32, u64, Option<u32>)>::new();
    let (identity_version, messages_version, version_count) = if geometry.checkpoint_count == 0 {
        if parent.is_some() || parent_checkpoint_id.is_some() || first_checkpoint.is_some() {
            return Err(format_error("first message checkpoint cannot have a parent"));
        }
        let identity_node = geometry.node_count;
        let messages_node = identity_node
            .checked_add(1)
            .ok_or_else(|| format_error("message node id overflow"))?;
        payload.extend_from_slice(b"null");
        payload.extend_from_slice(message_body);

        let mut identity_leaf = [0u8; COMPACT_NODE_SIZE];
        identity_leaf[..8].copy_from_slice(&geometry.byte_len.to_le_bytes());
        identity_leaf[8..12].copy_from_slice(&4u32.to_le_bytes());
        identity_leaf[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
        compact_nodes.push(identity_leaf);

        let message_offset = geometry
            .byte_len
            .checked_add(4)
            .ok_or_else(|| format_error("message leaf offset overflow"))?;
        let mut message_leaf = [0u8; COMPACT_NODE_SIZE];
        message_leaf[..8].copy_from_slice(&message_offset.to_le_bytes());
        message_leaf[8..12].copy_from_slice(
            &u32::try_from(message_body.len())
                .map_err(|_| format_error("message delta exceeds compact-node length"))?
                .to_le_bytes(),
        );
        message_leaf[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
        compact_nodes.push(message_leaf);

        let messages_version = version_start
            .checked_add(1)
            .ok_or_else(|| format_error("message version id overflow"))?;
        let version_count = messages_version
            .checked_add(1)
            .ok_or_else(|| format_error("message version count overflow"))?;
        roots.push((version_start, identity_node, None));
        roots.push((messages_version, messages_node, None));
        (version_start, messages_version, version_count)
    } else if let Some((identity_version, parent_root, parent_messages_version)) = parent {
        if parent_checkpoint_id.is_none() {
            return Err(format_error("message parent metadata is inconsistent"));
        }
        payload.push(b',');
        payload.extend_from_slice(message_body);

        let leaf_node = geometry.node_count;
        let root_node = leaf_node
            .checked_add(1)
            .ok_or_else(|| format_error("message binary root id overflow"))?;
        let mut message_leaf = [0u8; COMPACT_NODE_SIZE];
        message_leaf[..8].copy_from_slice(&geometry.byte_len.to_le_bytes());
        message_leaf[8..12].copy_from_slice(
            &u32::try_from(payload.len())
                .map_err(|_| format_error("message delta exceeds compact-node length"))?
                .to_le_bytes(),
        );
        message_leaf[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
        compact_nodes.push(message_leaf);

        let left_delta = root_node
            .checked_sub(parent_root)
            .ok_or_else(|| format_error("message parent root is not topologically prior"))?;
        let right_delta = root_node
            .checked_sub(leaf_node)
            .ok_or_else(|| format_error("message leaf is not topologically prior"))?;
        let mut binary = [0u8; COMPACT_NODE_SIZE];
        binary[..4].copy_from_slice(
            &u32::try_from(left_delta)
                .map_err(|_| format_error("message parent delta exceeds compact-node limit"))?
                .to_le_bytes(),
        );
        binary[4..8].copy_from_slice(
            &u32::try_from(right_delta)
                .map_err(|_| format_error("message leaf delta exceeds compact-node limit"))?
                .to_le_bytes(),
        );
        binary[12..16].copy_from_slice(&KIND_BINARY.to_le_bytes());
        compact_nodes.push(binary);

        let version_count = version_start
            .checked_add(1)
            .ok_or_else(|| format_error("message version count overflow"))?;
        roots.push((version_start, root_node, Some(parent_messages_version)));
        (identity_version, version_start, version_count)
    } else {
        if parent_checkpoint_id.is_some() {
            return Err(format_error("message parent checkpoint is absent"));
        }
        let first = first_checkpoint
            .ok_or_else(|| format_error("non-empty store is missing its first checkpoint"))?;
        payload.extend_from_slice(message_body);
        let root_node = geometry.node_count;
        let mut message_leaf = [0u8; COMPACT_NODE_SIZE];
        message_leaf[..8].copy_from_slice(&geometry.byte_len.to_le_bytes());
        message_leaf[8..12].copy_from_slice(
            &u32::try_from(message_body.len())
                .map_err(|_| format_error("message delta exceeds compact-node length"))?
                .to_le_bytes(),
        );
        message_leaf[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
        compact_nodes.push(message_leaf);
        let version_count = version_start
            .checked_add(1)
            .ok_or_else(|| format_error("message version count overflow"))?;
        roots.push((version_start, root_node, None));
        (first.identity_version, version_start, version_count)
    };

    let parent_id = parent_checkpoint_id.unwrap_or("");
    let checkpoint_record_len = CHECKPOINT_PREFIX_SIZE
        .checked_add(thread_id.len())
        .and_then(|length| length.checked_add(checkpoint_id.len()))
        .and_then(|length| length.checked_add(parent_id.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("checkpoint record length overflow"))?;
    let checkpoint_record_len_u32 = u32::try_from(checkpoint_record_len)
        .map_err(|_| format_error("checkpoint record exceeds the u32 length limit"))?;
    let mut checkpoint_record = Vec::with_capacity(checkpoint_record_len);
    checkpoint_record.extend_from_slice(&CHECKPOINT_MAGIC);
    checkpoint_record.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    checkpoint_record.extend_from_slice(&checkpoint_no.to_le_bytes());
    checkpoint_record.extend_from_slice(&identity_version.to_le_bytes());
    checkpoint_record.extend_from_slice(&messages_version.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(
        &u32::try_from(thread_id.len())
            .map_err(|_| format_error("thread id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(checkpoint_id.len())
            .map_err(|_| format_error("checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(parent_id.len())
            .map_err(|_| format_error("parent checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(&canonical_state_len.to_le_bytes());
    checkpoint_record.extend_from_slice(&canonical_state_hash.to_le_bytes());
    checkpoint_record.extend_from_slice(thread_id.as_bytes());
    checkpoint_record.extend_from_slice(checkpoint_id.as_bytes());
    checkpoint_record.extend_from_slice(parent_id.as_bytes());
    checkpoint_record.extend_from_slice(&xxh3_64(&checkpoint_record).to_le_bytes());

    let mut root_records = Vec::with_capacity(
        roots
            .len()
            .checked_mul(ROOT_RECORD_SIZE)
            .ok_or_else(|| format_error("root record bytes overflow"))?,
    );
    for (version, root, parent_version) in roots {
        let mut record = Vec::with_capacity(ROOT_RECORD_SIZE);
        record.extend_from_slice(&ROOT_MAGIC);
        record.extend_from_slice(&version.to_le_bytes());
        record.extend_from_slice(&root.to_le_bytes());
        record.extend_from_slice(&parent_version.unwrap_or(NONE_PARENT).to_le_bytes());
        record.extend_from_slice(&0u32.to_le_bytes());
        record.extend_from_slice(&xxh3_64(&record).to_le_bytes());
        root_records.extend_from_slice(&record);
    }

    let compact_node_bytes = compact_nodes
        .len()
        .checked_mul(COMPACT_NODE_SIZE)
        .ok_or_else(|| format_error("compact message node bytes overflow"))?;
    let total = TX_HEADER_SIZE
        .checked_add(payload.len())
        .and_then(|length| length.checked_add(compact_node_bytes))
        .and_then(|length| length.checked_add(root_records.len()))
        .and_then(|length| length.checked_add(checkpoint_record.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("message transaction length overflow"))?;
    let total_u32 = u32::try_from(total)
        .map_err(|_| format_error("message transaction exceeds the u32 length limit"))?;
    let root_count = version_count
        .checked_sub(version_start)
        .ok_or_else(|| format_error("message root count underflow"))?;
    let mut transaction = Vec::with_capacity(total);
    transaction.extend_from_slice(&TX_MAGIC);
    transaction.extend_from_slice(&total_u32.to_le_bytes());
    transaction.extend_from_slice(&version_start.to_le_bytes());
    transaction.extend_from_slice(&version_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_count.to_le_bytes());
    transaction.extend_from_slice(&root_count.to_le_bytes());
    transaction.extend_from_slice(&geometry.byte_len.to_le_bytes());
    transaction.extend_from_slice(
        &u64::try_from(payload.len())
            .map_err(|_| format_error("message payload length exceeds u64"))?
            .to_le_bytes(),
    );
    transaction.extend_from_slice(&geometry.node_count.to_le_bytes());
    transaction.extend_from_slice(
        &u32::try_from(compact_nodes.len())
            .map_err(|_| format_error("message compact node count exceeds u32"))?
            .to_le_bytes(),
    );
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.wide_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&payload);
    for node in compact_nodes {
        transaction.extend_from_slice(&node);
    }
    transaction.extend_from_slice(&root_records);
    transaction.extend_from_slice(&checkpoint_record);
    transaction.extend_from_slice(&xxh3_64(&transaction).to_le_bytes());
    Ok(transaction)
}

fn encode_single_identity_transaction(
    geometry: &Geometry,
    thread_id: &str,
    checkpoint_id: &str,
    checkpoint_no: u32,
    parent_checkpoint_id: Option<&str>,
    identity: &[u8],
) -> Result<Vec<u8>, CheckpointStoreError> {
    validate_checkpoint_identifier(thread_id, "thread id")?;
    validate_checkpoint_identifier(checkpoint_id, "checkpoint id")?;
    if let Some(parent) = parent_checkpoint_id {
        validate_checkpoint_identifier(parent, "parent checkpoint id")?;
    }

    let version_start = u32::try_from(geometry.version_count)
        .map_err(|_| format_error("version count exceeds the T2W1 u32 limit"))?;
    let version_count = version_start
        .checked_add(1)
        .ok_or_else(|| format_error("version count overflows the T2W1 u32 limit"))?;
    let checkpoint_count = geometry
        .checkpoint_count
        .checked_add(1)
        .ok_or_else(|| format_error("checkpoint count overflows the T2W1 u32 limit"))?;
    let checkpoint_count = u32::try_from(checkpoint_count)
        .map_err(|_| format_error("checkpoint count exceeds the T2W1 u32 limit"))?;
    let identity_len = u64::try_from(identity.len())
        .map_err(|_| format_error("identity length does not fit u64"))?;
    let identity_len_u32 = u32::try_from(identity.len())
        .map_err(|_| format_error("identity length exceeds the compact leaf limit"))?;
    let parent = parent_checkpoint_id.unwrap_or("");
    let canonical_len = u64::try_from(b"{\"identity\":".len())
        .map_err(|_| format_error("canonical prefix length does not fit u64"))?
        .checked_add(identity_len)
        .and_then(|length| length.checked_add(u64::try_from(b",\"messages\":[]}".len()).ok()?))
        .ok_or_else(|| format_error("canonical state length overflow"))?;
    let mut canonical = Vec::new();
    canonical.extend_from_slice(b"{\"identity\":");
    canonical.extend_from_slice(identity);
    canonical.extend_from_slice(b",\"messages\":[]}");
    let state_hash = xxh3_64(&canonical);

    let checkpoint_record_len = CHECKPOINT_PREFIX_SIZE
        .checked_add(thread_id.len())
        .and_then(|length| length.checked_add(checkpoint_id.len()))
        .and_then(|length| length.checked_add(parent.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("checkpoint record length overflow"))?;
    let checkpoint_record_len_u32 = u32::try_from(checkpoint_record_len)
        .map_err(|_| format_error("checkpoint record exceeds the u32 length limit"))?;

    let mut checkpoint_record = Vec::with_capacity(checkpoint_record_len);
    checkpoint_record.extend_from_slice(&CHECKPOINT_MAGIC);
    checkpoint_record.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    checkpoint_record.extend_from_slice(&checkpoint_no.to_le_bytes());
    checkpoint_record.extend_from_slice(&version_start.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(
        &u32::try_from(thread_id.len())
            .map_err(|_| format_error("thread id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(checkpoint_id.len())
            .map_err(|_| format_error("checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(parent.len())
            .map_err(|_| format_error("parent checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(&canonical_len.to_le_bytes());
    checkpoint_record.extend_from_slice(&state_hash.to_le_bytes());
    checkpoint_record.extend_from_slice(thread_id.as_bytes());
    checkpoint_record.extend_from_slice(checkpoint_id.as_bytes());
    checkpoint_record.extend_from_slice(parent.as_bytes());
    checkpoint_record.extend_from_slice(&xxh3_64(&checkpoint_record).to_le_bytes());

    let mut slot = [0u8; COMPACT_NODE_SIZE];
    slot[..8].copy_from_slice(&geometry.byte_len.to_le_bytes());
    slot[8..12].copy_from_slice(&identity_len_u32.to_le_bytes());
    slot[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());

    let root_id = geometry.node_count;
    let root_id_u32 = u32::try_from(version_start)
        .map_err(|_| format_error("root version id exceeds the u32 limit"))?;
    let root_parent = version_start.checked_sub(1).unwrap_or(NONE_PARENT);
    let mut root_record = Vec::with_capacity(ROOT_RECORD_SIZE);
    root_record.extend_from_slice(&ROOT_MAGIC);
    root_record.extend_from_slice(&root_id_u32.to_le_bytes());
    root_record.extend_from_slice(&root_id.to_le_bytes());
    root_record.extend_from_slice(&root_parent.to_le_bytes());
    root_record.extend_from_slice(&0u32.to_le_bytes());
    root_record.extend_from_slice(&xxh3_64(&root_record).to_le_bytes());

    let total = TX_HEADER_SIZE
        .checked_add(identity.len())
        .and_then(|length| length.checked_add(COMPACT_NODE_SIZE))
        .and_then(|length| length.checked_add(ROOT_RECORD_SIZE))
        .and_then(|length| length.checked_add(checkpoint_record.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("transaction length overflow"))?;
    let total_u32 = u32::try_from(total)
        .map_err(|_| format_error("transaction exceeds the u32 length limit"))?;

    let mut transaction = Vec::with_capacity(total);
    transaction.extend_from_slice(&TX_MAGIC);
    transaction.extend_from_slice(&total_u32.to_le_bytes());
    transaction.extend_from_slice(&version_start.to_le_bytes());
    transaction.extend_from_slice(&version_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_count.to_le_bytes());
    transaction.extend_from_slice(&1u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.byte_len.to_le_bytes());
    transaction.extend_from_slice(&identity_len.to_le_bytes());
    transaction.extend_from_slice(&geometry.node_count.to_le_bytes());
    transaction.extend_from_slice(&1u32.to_le_bytes());
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.wide_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(identity);
    transaction.extend_from_slice(&slot);
    transaction.extend_from_slice(&root_record);
    transaction.extend_from_slice(&checkpoint_record);
    transaction.extend_from_slice(&xxh3_64(&transaction).to_le_bytes());
    Ok(transaction)
}

fn encode_identity_leaf_transaction(
    geometry: &Geometry,
    thread_id: &str,
    checkpoint_id: &str,
    checkpoint_no: u32,
    parent_checkpoint_id: Option<&str>,
    leaves: &[IdentityLeafSource<'_>],
    canonical_state_len: u64,
    canonical_state_hash: u64,
) -> Result<Vec<u8>, CheckpointStoreError> {
    validate_checkpoint_identifier(thread_id, "thread id")?;
    validate_checkpoint_identifier(checkpoint_id, "checkpoint id")?;
    if let Some(parent) = parent_checkpoint_id {
        validate_checkpoint_identifier(parent, "parent checkpoint id")?;
    }
    if leaves.is_empty() {
        return Err(format_error("identity tree must contain at least one leaf"));
    }

    let version_start = u32::try_from(geometry.version_count)
        .map_err(|_| format_error("version count exceeds the T2W1 u32 limit"))?;
    let version_count = version_start
        .checked_add(1)
        .ok_or_else(|| format_error("version count overflows the T2W1 u32 limit"))?;
    let checkpoint_count = geometry
        .checkpoint_count
        .checked_add(1)
        .ok_or_else(|| format_error("checkpoint count overflows the T2W1 u64 limit"))?;
    let checkpoint_count = u32::try_from(checkpoint_count)
        .map_err(|_| format_error("checkpoint count exceeds the u32 limit"))?;
    let mut byte_delta = Vec::new();
    let mut leaf_ids = Vec::with_capacity(leaves.len());
    let mut identity_len = 0u64;
    let mut nodes = Vec::<[u8; COMPACT_NODE_SIZE]>::new();
    for leaf in leaves {
        let (node_id, leaf_len) = match leaf {
            IdentityLeafSource::New(bytes) => {
                let offset = geometry
                    .byte_len
                    .checked_add(u64::try_from(byte_delta.len()).map_err(|_| {
                        format_error("identity byte delta length exceeds u64")
                    })?)
                    .ok_or_else(|| format_error("identity leaf offset overflow"))?;
                let leaf_len = u64::try_from(bytes.len())
                    .map_err(|_| format_error("identity leaf length exceeds u64"))?;
                let leaf_len_u32 = u32::try_from(leaf_len)
                    .map_err(|_| format_error("identity leaf length exceeds compact-node limit"))?;
                byte_delta.extend_from_slice(bytes);
                let node_id = geometry
                    .node_count
                    .checked_add(u64::try_from(nodes.len()).map_err(|_| {
                        format_error("identity node count exceeds u64")
                    })?)
                    .ok_or_else(|| format_error("identity node id overflow"))?;
                let mut slot = [0u8; COMPACT_NODE_SIZE];
                slot[..8].copy_from_slice(&offset.to_le_bytes());
                slot[8..12].copy_from_slice(&leaf_len_u32.to_le_bytes());
                slot[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
                nodes.push(slot);
                (node_id, leaf_len)
            }
            IdentityLeafSource::Existing(reference) => {
                if reference.node_id >= geometry.node_count {
                    return Err(format_error("identity reuse node is not committed"));
                }
                (reference.node_id, reference.logical_len)
            }
        };
        identity_len = identity_len
            .checked_add(leaf_len)
            .ok_or_else(|| format_error("identity length overflow"))?;
        leaf_ids.push(node_id);
    }
    let expected_state_len = CANONICAL_STATE_PREFIX_BYTES
        .checked_add(identity_len)
        .and_then(|value| value.checked_add(CANONICAL_STATE_SUFFIX_BYTES))
        .ok_or_else(|| format_error("canonical state length overflow"))?;
    if expected_state_len != canonical_state_len {
        return Err(format_error("canonical state length disagrees with identity leaves"));
    }

    let mut level = leaf_ids;
    while level.len() > 1 {
        let mut next = Vec::with_capacity((level.len() + 1) / 2);
        for pair in level.chunks(2) {
            if pair.len() == 1 {
                next.push(pair[0]);
                continue;
            }
            let node_id = geometry
                .node_count
                .checked_add(u64::try_from(nodes.len()).map_err(|_| {
                    format_error("identity node count exceeds u64")
                })?)
                .ok_or_else(|| format_error("identity node id overflow"))?;
            let left_delta = node_id
                .checked_sub(pair[0])
                .ok_or_else(|| format_error("identity binary left child is not prior"))?;
            let right_delta = node_id
                .checked_sub(pair[1])
                .ok_or_else(|| format_error("identity binary right child is not prior"))?;
            let mut slot = [0u8; COMPACT_NODE_SIZE];
            slot[..4].copy_from_slice(
                &u32::try_from(left_delta)
                    .map_err(|_| format_error("identity binary left delta exceeds u32"))?
                    .to_le_bytes(),
            );
            slot[4..8].copy_from_slice(
                &u32::try_from(right_delta)
                    .map_err(|_| format_error("identity binary right delta exceeds u32"))?
                    .to_le_bytes(),
            );
            slot[12..16].copy_from_slice(&KIND_BINARY.to_le_bytes());
            nodes.push(slot);
            next.push(node_id);
        }
        level = next;
    }
    let root_id = *level
        .first()
        .ok_or_else(|| format_error("identity tree root is missing"))?;

    let parent = parent_checkpoint_id.unwrap_or("");
    let checkpoint_record_len = CHECKPOINT_PREFIX_SIZE
        .checked_add(thread_id.len())
        .and_then(|length| length.checked_add(checkpoint_id.len()))
        .and_then(|length| length.checked_add(parent.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("checkpoint record length overflow"))?;
    let checkpoint_record_len_u32 = u32::try_from(checkpoint_record_len)
        .map_err(|_| format_error("checkpoint record exceeds the u32 length limit"))?;
    let mut checkpoint_record = Vec::with_capacity(checkpoint_record_len);
    checkpoint_record.extend_from_slice(&CHECKPOINT_MAGIC);
    checkpoint_record.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    checkpoint_record.extend_from_slice(&checkpoint_no.to_le_bytes());
    checkpoint_record.extend_from_slice(&version_start.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(&NONE_VERSION.to_le_bytes());
    checkpoint_record.extend_from_slice(
        &u32::try_from(thread_id.len())
            .map_err(|_| format_error("thread id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(checkpoint_id.len())
            .map_err(|_| format_error("checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(
        &u32::try_from(parent.len())
            .map_err(|_| format_error("parent checkpoint id length exceeds the u32 limit"))?
            .to_le_bytes(),
    );
    checkpoint_record.extend_from_slice(&canonical_state_len.to_le_bytes());
    checkpoint_record.extend_from_slice(&canonical_state_hash.to_le_bytes());
    checkpoint_record.extend_from_slice(thread_id.as_bytes());
    checkpoint_record.extend_from_slice(checkpoint_id.as_bytes());
    checkpoint_record.extend_from_slice(parent.as_bytes());
    checkpoint_record.extend_from_slice(&xxh3_64(&checkpoint_record).to_le_bytes());

    let root_parent = version_start.checked_sub(1).unwrap_or(NONE_PARENT);
    let mut root_record = Vec::with_capacity(ROOT_RECORD_SIZE);
    root_record.extend_from_slice(&ROOT_MAGIC);
    root_record.extend_from_slice(&version_start.to_le_bytes());
    root_record.extend_from_slice(&root_id.to_le_bytes());
    root_record.extend_from_slice(&root_parent.to_le_bytes());
    root_record.extend_from_slice(&0u32.to_le_bytes());
    root_record.extend_from_slice(&xxh3_64(&root_record).to_le_bytes());

    let total = TX_HEADER_SIZE
        .checked_add(byte_delta.len())
        .and_then(|length| length.checked_add(nodes.len().checked_mul(COMPACT_NODE_SIZE)?))
        .and_then(|length| length.checked_add(ROOT_RECORD_SIZE))
        .and_then(|length| length.checked_add(checkpoint_record.len()))
        .and_then(|length| length.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("transaction length overflow"))?;
    let total_u32 = u32::try_from(total)
        .map_err(|_| format_error("transaction exceeds the u32 length limit"))?;
    let mut transaction = Vec::with_capacity(total);
    transaction.extend_from_slice(&TX_MAGIC);
    transaction.extend_from_slice(&total_u32.to_le_bytes());
    transaction.extend_from_slice(&version_start.to_le_bytes());
    transaction.extend_from_slice(&version_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_count.to_le_bytes());
    transaction.extend_from_slice(&1u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.byte_len.to_le_bytes());
    transaction.extend_from_slice(
        &u64::try_from(byte_delta.len())
            .map_err(|_| format_error("identity byte delta length exceeds u64"))?
            .to_le_bytes(),
    );
    transaction.extend_from_slice(&geometry.node_count.to_le_bytes());
    transaction.extend_from_slice(
        &u32::try_from(nodes.len()).map_err(|_| format_error("identity node count exceeds u32"))?
            .to_le_bytes(),
    );
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&geometry.wide_count.to_le_bytes());
    transaction.extend_from_slice(&checkpoint_record_len_u32.to_le_bytes());
    transaction.extend_from_slice(&0u32.to_le_bytes());
    transaction.extend_from_slice(&byte_delta);
    for node in nodes {
        transaction.extend_from_slice(&node);
    }
    transaction.extend_from_slice(&root_record);
    transaction.extend_from_slice(&checkpoint_record);
    transaction.extend_from_slice(&xxh3_64(&transaction).to_le_bytes());
    Ok(transaction)
}

fn encode_transaction_with_request_id(
    transaction: &[u8],
    request_id: &[u8],
) -> Result<Vec<u8>, CheckpointStoreError> {
    validate_request_id(request_id)?;
    if transaction.len() < TX_HEADER_SIZE + TX_CHECKSUM_SIZE
        || transaction.get(..4) != Some(TX_MAGIC.as_slice())
    {
        return Err(format_error("invalid requestless transaction"));
    }
    if read_u32(transaction, 68)? != 0 {
        return Err(format_error("requestless transaction has a non-zero request field"));
    }
    let new_len = transaction
        .len()
        .checked_add(request_id.len())
        .ok_or_else(|| format_error("request-bearing transaction length overflow"))?;
    let new_len_u32 = u32::try_from(new_len)
        .map_err(|_| format_error("request-bearing transaction exceeds u32 length"))?;
    let mut encoded = Vec::with_capacity(new_len);
    encoded.extend_from_slice(
        transaction
            .get(..4)
            .ok_or_else(|| format_error("transaction magic missing"))?,
    );
    encoded.extend_from_slice(&new_len_u32.to_le_bytes());
    encoded.extend_from_slice(
        transaction
            .get(8..68)
            .ok_or_else(|| format_error("transaction header missing"))?,
    );
    encoded.extend_from_slice(
        &u32::try_from(request_id.len())
            .map_err(|_| format_error("request id length exceeds u32"))?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(request_id);
    encoded.extend_from_slice(
        transaction
            .get(TX_HEADER_SIZE..transaction.len() - TX_CHECKSUM_SIZE)
            .ok_or_else(|| format_error("transaction body missing"))?,
    );
    let digest = xxh3_64(&encoded);
    encoded.extend_from_slice(&digest.to_le_bytes());
    Ok(encoded)
}

fn parse_transaction_unchecked(
    record: &[u8],
    start_offset: u64,
) -> Result<ParsedTransaction, CheckpointStoreError> {
    parse_transaction_with_geometry(record, None, start_offset)
}

fn parse_transaction(
    record: &[u8],
    geometry: Geometry,
    start_offset: u64,
) -> Result<ParsedTransaction, CheckpointStoreError> {
    parse_transaction_with_geometry(record, Some(geometry), start_offset)
}

fn parse_transaction_with_geometry(
    record: &[u8],
    geometry: Option<Geometry>,
    start_offset: u64,
) -> Result<ParsedTransaction, CheckpointStoreError> {
    if record.len() < TX_HEADER_SIZE + TX_CHECKSUM_SIZE
        || record.get(..4) != Some(TX_MAGIC.as_slice())
    {
        return Err(format_error("invalid T2W1 transaction header"));
    }
    let record_len = usize::try_from(read_u32(record, 4)?)
        .map_err(|_| format_error("transaction length overflow"))?;
    if record_len != record.len() {
        return Err(format_error("transaction length mismatch"));
    }
    if xxh3_64(
        record
            .get(..record.len() - TX_CHECKSUM_SIZE)
            .ok_or_else(|| format_error("transaction checksum input outside record"))?,
    ) != read_u64(record, record.len() - TX_CHECKSUM_SIZE)?
    {
        return Err(format_error("transaction checksum mismatch"));
    }
    let version_start = u64::from(read_u32(record, 8)?);
    let version_count = u64::from(read_u32(record, 12)?);
    let checkpoint_count = u64::from(read_u32(record, 16)?);
    let new_version_count = u64::from(read_u32(record, 20)?);
    let byte_start = read_u64(record, 24)?;
    let byte_delta_len = usize::try_from(read_u64(record, 32)?)
        .map_err(|_| format_error("byte delta length overflow"))?;
    let node_start = read_u64(record, 40)?;
    let compact_node_count = usize::try_from(read_u32(record, 48)?)
        .map_err(|_| format_error("compact node count overflow"))?;
    let wide_node_count = usize::try_from(read_u32(record, 52)?)
        .map_err(|_| format_error("wide node count overflow"))?;
    let wide_start = read_u64(record, 56)?;
    let checkpoint_record_len = usize::try_from(read_u32(record, 64)?)
        .map_err(|_| format_error("checkpoint record length overflow"))?;
    let request_id_len = usize::try_from(read_u32(record, 68)?)
        .map_err(|_| format_error("request id length overflow"))?;
    if request_id_len > MAX_REQUEST_ID_BYTES {
        return Err(format_error("request id exceeds the byte limit"));
    }
    if checkpoint_count == 0 {
        return Err(format_error("transaction checkpoint count must be non-zero"));
    }
    let expected_version_count = version_start
        .checked_add(new_version_count)
        .ok_or_else(|| format_error("version topology overflows u64"))?;
    if version_count != expected_version_count {
        return Err(format_error("transaction version topology is inconsistent"));
    }
    if let Some(geometry) = geometry {
        let expected_checkpoint_count = geometry
            .checkpoint_count
            .checked_add(1)
            .ok_or_else(|| format_error("checkpoint topology overflows u64"))?;
        if version_start != geometry.version_count
            || checkpoint_count != expected_checkpoint_count
            || byte_start != geometry.byte_len
            || node_start != geometry.node_count
            || wide_start != geometry.wide_count
        {
            return Err(format_error("transaction topology/watermark mismatch"));
        }
    }
    let compact_bytes_len = compact_node_count
        .checked_mul(COMPACT_NODE_SIZE)
        .ok_or_else(|| format_error("compact-node byte length overflow"))?;
    let wide_bytes_len = wide_node_count
        .checked_mul(WIDE_RECORD_SIZE)
        .ok_or_else(|| format_error("wide-node byte length overflow"))?;
    let root_bytes_len = usize::try_from(new_version_count)
        .map_err(|_| format_error("new version count overflow"))?
        .checked_mul(ROOT_RECORD_SIZE)
        .ok_or_else(|| format_error("root-record byte length overflow"))?;
    let expected = TX_HEADER_SIZE
        .checked_add(request_id_len)
        .and_then(|n| n.checked_add(byte_delta_len))
        .and_then(|n| n.checked_add(compact_bytes_len))
        .and_then(|n| n.checked_add(wide_bytes_len))
        .and_then(|n| n.checked_add(root_bytes_len))
        .and_then(|n| n.checked_add(checkpoint_record_len))
        .and_then(|n| n.checked_add(TX_CHECKSUM_SIZE))
        .ok_or_else(|| format_error("transaction payload geometry overflow"))?;
    if expected != record.len() {
        return Err(format_error("transaction payload geometry mismatch"));
    }
    let mut cursor = TX_HEADER_SIZE;
    let request_id = if request_id_len == 0 {
        None
    } else {
        let end = cursor
            .checked_add(request_id_len)
            .ok_or_else(|| format_error("request id range overflow"))?;
        let request_id = record
            .get(cursor..end)
            .ok_or_else(|| format_error("request id outside transaction"))?
            .to_vec();
        cursor = end;
        Some(request_id)
    };
    let byte_end_cursor = cursor + byte_delta_len;
    let bytes = record
        .get(cursor..byte_end_cursor)
        .ok_or_else(|| format_error("transaction byte delta outside record"))?
        .to_vec();
    cursor = byte_end_cursor;
    let compact_end_cursor = cursor + compact_bytes_len;
    let compact_nodes = record
        .get(cursor..compact_end_cursor)
        .ok_or_else(|| format_error("compact nodes outside transaction"))?
        .to_vec();
    cursor = compact_end_cursor;
    let wide_end_cursor = cursor + wide_bytes_len;
    let wide_nodes = record
        .get(cursor..wide_end_cursor)
        .ok_or_else(|| format_error("wide nodes outside transaction"))?
        .to_vec();
    cursor = wide_end_cursor;
    let mut roots = Vec::new();
    let mut parents = Vec::new();
    for expected_id in version_start..version_count {
        let root_end = cursor + ROOT_RECORD_SIZE;
        let (root, parent) = parse_root(
            record
                .get(cursor..root_end)
                .ok_or_else(|| format_error("root record outside transaction"))?,
            expected_id,
        )?;
        roots.push(root);
        parents.push(parent);
        cursor = root_end;
    }
    let cp_end = cursor + checkpoint_record_len;
    let ordinal = u32::try_from(checkpoint_count - 1)
        .map_err(|_| format_error("checkpoint ordinal exceeds u32"))?;
    let checkpoint = parse_checkpoint_record(
        record
            .get(cursor..cp_end)
            .ok_or_else(|| format_error("checkpoint record outside transaction"))?,
        version_count,
        ordinal,
    )?;
    cursor = cp_end;
    if cursor != record.len() - TX_CHECKSUM_SIZE {
        return Err(format_error("transaction cursor mismatch"));
    }
    let byte_end = byte_start
        .checked_add(
            u64::try_from(bytes.len()).map_err(|_| format_error("byte delta length overflow"))?,
        )
        .ok_or_else(|| format_error("byte watermark overflow"))?;
    let node_end = node_start
        .checked_add(
            u64::try_from(compact_node_count)
                .map_err(|_| format_error("compact-node count overflow"))?,
        )
        .ok_or_else(|| format_error("node watermark overflow"))?;
    let wide_end = wide_start
        .checked_add(
            u64::try_from(wide_node_count)
                .map_err(|_| format_error("wide-node count overflow"))?,
        )
        .ok_or_else(|| format_error("wide watermark overflow"))?;
    let end_offset = start_offset
        .checked_add(
            u64::try_from(record.len()).map_err(|_| format_error("record length overflow"))?,
        )
        .ok_or_else(|| format_error("transaction file offset overflow"))?;
    Ok(ParsedTransaction {
        start_offset,
        end_offset,
        version_start,
        version_count,
        checkpoint_count,
        byte_start,
        byte_end,
        bytes,
        node_start,
        node_end,
        compact_nodes,
        wide_start,
        wide_end,
        wide_nodes,
        roots,
        parents,
        checkpoint,
        request_id,
        operation_digest: Sha256::digest(record).into(),
    })
}

fn parse_hot_prefix(path: &Path, mut geometry: Geometry) -> Result<HotParse, CheckpointStoreError> {
    let mut file = File::open(path)?;
    let physical = file.metadata()?.len();
    let mut offset = 0u64;
    let mut transactions = Vec::new();
    let minimum = u64::try_from(TX_HEADER_SIZE + TX_CHECKSUM_SIZE)
        .map_err(|_| format_error("minimum transaction size overflow"))?;
    while offset
        .checked_add(minimum)
        .ok_or_else(|| format_error("hot-WAL scan offset overflow"))?
        <= physical
    {
        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0u8; TX_HEADER_SIZE];
        if file.read_exact(&mut header).is_err() {
            break;
        }
        if header.get(..4) != Some(TX_MAGIC.as_slice()) {
            break;
        }
        let record_len = u64::from(read_u32(&header, 4)?);
        if record_len < minimum {
            break;
        }
        let end = match offset.checked_add(record_len) {
            Some(value) if value <= physical => value,
            _ => break,
        };
        file.seek(SeekFrom::Start(offset))?;
        let mut record = vec![0u8; usize::try_from(record_len)
            .map_err(|_| format_error("hot transaction length overflow"))?];
        if file.read_exact(&mut record).is_err() {
            break;
        }
        let tx = match parse_transaction(&record, geometry, offset) {
            Ok(value) => value,
            Err(_) => break,
        };
        geometry.advance(&tx);
        transactions.push(tx);
        offset = end;
    }
    Ok(HotParse {
        transactions,
        logical_tail: offset,
    })
}

fn file_starts_with_tx_magic(path: &Path) -> Result<bool, CheckpointStoreError> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    match file.read_exact(&mut magic) {
        Ok(()) => Ok(magic == TX_MAGIC),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn validate_transaction_against_state(
    state: &StoreState,
    tx: &ParsedTransaction,
) -> Result<(), CheckpointStoreError> {
    if let Some(parent) = tx.checkpoint.parent_checkpoint_id.as_ref() {
        let key = (tx.checkpoint.thread_id.clone(), parent.clone());
        let parent_ordinal = state
            .checkpoint_ordinals
            .get(&key)
            .copied()
            .ok_or_else(|| format_error("checkpoint parent is not committed"))?;
        if parent_ordinal >= tx.checkpoint.ordinal {
            return Err(format_error("checkpoint parent is not prior"));
        }
    }
    if state
        .checkpoint_ordinals
        .contains_key(&(tx.checkpoint.thread_id.clone(), tx.checkpoint.checkpoint_id.clone()))
    {
        return Err(format_error("duplicate checkpoint key"));
    }
    if state
        .deleted_checkpoints
        .contains(&(tx.checkpoint.thread_id.clone(), tx.checkpoint.checkpoint_id.clone()))
    {
        return Err(CheckpointStoreError::CheckpointDeleted);
    }
    if let Some(request_id) = tx.request_id.as_ref() {
        if let Some(existing) = state.request_records.get(request_id) {
            if existing.operation_digest != tx.operation_digest {
                return Err(CheckpointStoreError::RequestIdConflict);
            }
            return Err(format_error("duplicate request id"));
        }
    }
    validate_new_nodes(state, tx)
}

fn reject_deleted_checkpoint(
    state: &StoreState,
    tx: &ParsedTransaction,
) -> Result<(), CheckpointStoreError> {
    if state.deleted_checkpoints.contains(&(
        tx.checkpoint.thread_id.clone(),
        tx.checkpoint.checkpoint_id.clone(),
    )) {
        return Err(CheckpointStoreError::CheckpointDeleted);
    }
    Ok(())
}

fn validate_new_nodes(state: &StoreState, tx: &ParsedTransaction) -> Result<(), CheckpointStoreError> {
    let mut arena_len = state.geometry()?.byte_len;
    arena_len = arena_len
        .checked_add(u64::try_from(tx.bytes.len()).map_err(|_| format_error("delta length overflow"))?)
        .ok_or_else(|| format_error("arena length after delta overflow"))?;
    let base_nodes = state.geometry()?.node_count;
    let base_wide_count = state.lazy_base_geometry().wide_count;
    let combined_wide_len = state
        .geometry()?
        .wide_count
        .checked_mul(u64::try_from(WIDE_RECORD_SIZE).map_err(|_| {
            format_error("wide record size overflow")
        })?)
        .and_then(|length| {
            length.checked_add(u64::try_from(tx.wide_nodes.len()).ok()?)
        })
        .ok_or_else(|| format_error("combined wide-node length overflow"))?;
    for (local, slot) in tx.compact_nodes.chunks_exact(COMPACT_NODE_SIZE).enumerate() {
        let node_id = base_nodes
            .checked_add(u64::try_from(local).map_err(|_| format_error("node index overflow"))?)
            .ok_or_else(|| format_error("global node id overflow"))?;
        let kind = read_u32(slot, 12)?;
        match kind {
            KIND_LEAF => {
                let offset = read_u64(slot, 0)?;
                let length = u64::from(read_u32(slot, 8)?);
                if offset.checked_add(length).ok_or_else(|| format_error("leaf end overflow"))? > arena_len {
                    return Err(format_error("leaf references bytes beyond committed arena"));
                }
            }
            KIND_BINARY => {
                let left_delta = u64::from(read_u32(slot, 0)?);
                let right_delta = u64::from(read_u32(slot, 4)?);
                if left_delta == 0 || right_delta == 0 || left_delta > node_id || right_delta > node_id {
                    return Err(format_error("compact binary delta is invalid"));
                }
            }
            KIND_WIDE => {
                let wide_index = read_u64(slot, 0)?;
                if wide_index < base_wide_count {
                    continue;
                }
                let local_wide_index = wide_index
                    .checked_sub(base_wide_count)
                    .ok_or_else(|| format_error("local wide index underflow"))?;
                let wide_index = usize::try_from(local_wide_index)
                    .map_err(|_| format_error("wide index overflow"))?;
                let start = wide_index
                    .checked_mul(WIDE_RECORD_SIZE)
                    .ok_or_else(|| format_error("wide-record offset overflow"))?;
                let end = start
                    .checked_add(WIDE_RECORD_SIZE)
                    .ok_or_else(|| format_error("wide-record end overflow"))?;
                let record = if end <= state.wide_nodes.len() {
                    state
                        .wide_nodes
                        .get(start..end)
                        .ok_or_else(|| format_error("wide record outside base state"))?
                } else {
                    let relative_start = start.saturating_sub(state.wide_nodes.len());
                    let relative_end = end.saturating_sub(state.wide_nodes.len());
                    tx.wide_nodes
                        .get(relative_start..relative_end)
                        .ok_or_else(|| format_error("wide record outside transaction"))?
                };
                match read_u32(record, 0)? {
                    WIDE_KIND_LEAF => {
                        let offset = read_u64(record, 8)?;
                        let length = read_u64(record, 16)?;
                        if offset.checked_add(length).ok_or_else(|| format_error("wide leaf end overflow"))? > arena_len {
                            return Err(format_error("wide leaf references bytes beyond committed arena"));
                        }
                    }
                    WIDE_KIND_BINARY => {
                        let left = read_u64(record, 8)?;
                        let right = read_u64(record, 16)?;
                        if left >= node_id || right >= node_id {
                            return Err(format_error("wide binary child is not topologically prior"));
                        }
                    }
                    _ => return Err(format_error("unknown wide node kind")),
                }
            }
            _ => return Err(format_error("unknown compact node kind")),
        }
    }
    if combined_wide_len
        % u64::try_from(WIDE_RECORD_SIZE).map_err(|_| format_error("wide record size overflow"))?
        != 0
    {
        return Err(format_error("wide-node bytes are not record aligned"));
    }
    for root in tx.roots.iter().flatten() {
        if *root >= tx.node_end {
            return Err(format_error("version root is outside committed node range"));
        }
    }
    Ok(())
}

fn apply_transaction(state: &mut StoreState, tx: &ParsedTransaction) -> Result<(), CheckpointStoreError> {
    if Geometry::from_state(state)?
        != (Geometry {
            byte_len: tx.byte_start,
            node_count: tx.node_start,
            wide_count: tx.wide_start,
            version_count: tx.version_start,
            checkpoint_count: tx.checkpoint_count - 1,
        })
    {
        return Err(format_error("transaction does not append to current state geometry"));
    }
    validate_transaction_against_state(state, tx)?;
    state.arena_bytes.extend_from_slice(&tx.bytes);
    state.compact_nodes.extend_from_slice(&tx.compact_nodes);
    state.wide_nodes.extend_from_slice(&tx.wide_nodes);
    state.versions.extend_from_slice(&tx.roots);
    state.parents.extend_from_slice(&tx.parents);
    if !state.thread_ordinals.contains_key(&tx.checkpoint.thread_id) {
        let ordinal = u32::try_from(state.threads.len())
            .map_err(|_| format_error("thread ordinal exceeds u32"))?;
        state
            .thread_ordinals
            .insert(tx.checkpoint.thread_id.clone(), ordinal);
        state.threads.push(tx.checkpoint.thread_id.clone());
    }
    state.checkpoint_ordinals.insert(
        (tx.checkpoint.thread_id.clone(), tx.checkpoint.checkpoint_id.clone()),
        tx.checkpoint.ordinal,
    );
    if let Some(request_id) = tx.request_id.as_ref() {
        state.request_records.insert(
            request_id.clone(),
            RequestRecord {
                key: request_id.clone(),
                operation_digest: tx.operation_digest,
                checkpoint_ordinal: tx.checkpoint.ordinal,
            },
        );
    }
    state.checkpoints.push(tx.checkpoint.clone());
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct DecodedNode {
    kind: u8,
    a: u64,
    b: u64,
}

fn decode_node(state: &StoreState, node_id: u64) -> Result<DecodedNode, CheckpointStoreError> {
    let local_node_id = node_id
        .checked_sub(state.lazy_base_geometry().node_count)
        .ok_or_else(|| format_error("node id belongs to lazy sealed base"))?;
    let index = usize::try_from(local_node_id)
        .map_err(|_| format_error("node id exceeds usize"))?;
    let start = index
        .checked_mul(COMPACT_NODE_SIZE)
        .ok_or_else(|| format_error("compact node offset overflow"))?;
    let end = start
        .checked_add(COMPACT_NODE_SIZE)
        .ok_or_else(|| format_error("compact node end overflow"))?;
    let slot = state
        .compact_nodes
        .get(start..end)
        .ok_or_else(|| format_error("node id outside committed compact nodes"))?;
    match read_u32(slot, 12)? {
        KIND_LEAF => Ok(DecodedNode {
            kind: 0,
            a: read_u64(slot, 0)?,
            b: u64::from(read_u32(slot, 8)?),
        }),
        KIND_BINARY => {
            let left_delta = u64::from(read_u32(slot, 0)?);
            let right_delta = u64::from(read_u32(slot, 4)?);
            if left_delta == 0 || right_delta == 0 || left_delta > node_id || right_delta > node_id {
                return Err(format_error("compact binary delta underflow"));
            }
            Ok(DecodedNode {
                kind: 1,
                a: node_id - left_delta,
                b: node_id - right_delta,
            })
        }
        KIND_WIDE => {
            let wide_index = read_u64(slot, 0)?
                .checked_sub(state.lazy_base_geometry().wide_count)
                .ok_or_else(|| format_error("wide index belongs to lazy sealed base"))?;
            let wide_index = usize::try_from(wide_index)
                .map_err(|_| format_error("wide index exceeds usize"))?;
            let wide_start = wide_index
                .checked_mul(WIDE_RECORD_SIZE)
                .ok_or_else(|| format_error("wide offset overflow"))?;
            let wide_end = wide_start
                .checked_add(WIDE_RECORD_SIZE)
                .ok_or_else(|| format_error("wide end overflow"))?;
            let record = state
                .wide_nodes
                .get(wide_start..wide_end)
                .ok_or_else(|| format_error("wide index outside committed records"))?;
            match read_u32(record, 0)? {
                WIDE_KIND_LEAF => Ok(DecodedNode {
                    kind: 0,
                    a: read_u64(record, 8)?,
                    b: read_u64(record, 16)?,
                }),
                WIDE_KIND_BINARY => {
                    let left = read_u64(record, 8)?;
                    let right = read_u64(record, 16)?;
                    if left >= node_id || right >= node_id {
                        return Err(format_error("wide binary child not prior"));
                    }
                    Ok(DecodedNode {
                        kind: 1,
                        a: left,
                        b: right,
                    })
                }
                _ => Err(format_error("unknown wide node kind")),
            }
        }
        _ => Err(format_error("unknown compact node kind")),
    }
}

fn extract_root(state: &StoreState, root: u64) -> Result<Vec<u8>, CheckpointStoreError> {
    let mut out = Vec::new();
    let mut stack = vec![root];
    while let Some(node_id) = stack.pop() {
        let node = decode_node(state, node_id)?;
        if node.kind == 0 {
            let start = usize::try_from(node.a).map_err(|_| format_error("leaf offset overflow"))?;
            let length = usize::try_from(node.b).map_err(|_| format_error("leaf length overflow"))?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| format_error("leaf byte end overflow"))?;
            out.extend_from_slice(
                state
                    .arena_bytes
                    .get(start..end)
                    .ok_or_else(|| format_error("leaf bytes outside committed arena"))?,
            );
        } else {
            stack.push(node.b);
            stack.push(node.a);
        }
    }
    Ok(out)
}

fn extract_version(state: &StoreState, version: u32) -> Result<Vec<u8>, CheckpointStoreError> {
    let index = usize::try_from(version).map_err(|_| format_error("version index overflow"))?;
    let root = state
        .versions
        .get(index)
        .copied()
        .flatten()
        .ok_or_else(|| format_error("version has no root"))?;
    extract_root(state, root)
}

fn reconstruct_checkpoint(
    state: &StoreState,
    checkpoint: &CheckpointInfo,
) -> Result<Vec<u8>, CheckpointStoreError> {
    let identity = extract_version(state, checkpoint.identity_version)?;
    let messages = checkpoint
        .messages_version
        .map(|version| extract_version(state, version))
        .transpose()?
        .unwrap_or_default();
    let result = checkpoint
        .result_version
        .map(|version| extract_version(state, version))
        .transpose()?;
    let mut out = Vec::new();
    out.extend_from_slice(b"{\"identity\":");
    out.extend_from_slice(&identity);
    out.extend_from_slice(b",\"messages\":[");
    out.extend_from_slice(&messages);
    out.extend_from_slice(b"]");
    if let Some(result) = result {
        out.extend_from_slice(b",\"result\":");
        out.extend_from_slice(&result);
    }
    out.extend_from_slice(b"}");
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
struct SegmentHeader {
    generation: u64,
    checkpoint_start_count: u64,
    checkpoint_end_count: u64,
    version_start_count: u64,
    version_end_count: u64,
    block_size: u32,
    block_count: u32,
    payload_offset: u64,
    payload_bytes: u64,
    index_offset: u64,
    stream_table_bytes: u64,
    block_table_bytes: u64,
    index_xxh3_64: u64,
    header_xxh3_64: u64,
}

#[derive(Debug, Clone, Copy)]
struct StreamEntry {
    global_start: u64,
    global_end: u64,
    raw_len: u64,
    first_block: u32,
    block_count: u32,
    raw_xxh3_64: u64,
}

#[derive(Debug, Clone, Copy)]
struct BlockEntry {
    stream_id: u32,
    raw_offset: u64,
    encoded_offset: u64,
    raw_len: u32,
    encoded_len: u32,
    raw_xxh3_64: u64,
}

#[derive(Debug, Clone)]
struct SegmentMeta {
    generation: u64,
    file: String,
    wal_start_bytes: u64,
    wal_end_bytes: u64,
    checkpoint_start_count: u64,
    checkpoint_end_count: u64,
    version_start_count: u64,
    version_end_count: u64,
    raw_stream_bytes: u64,
    stream_starts: Vec<u64>,
    stream_ends: Vec<u64>,
    segment_file_bytes: u64,
    segment_file_xxh3_64: u64,
    index_bytes: u64,
    index_xxh3_64: u64,
    block_size: u32,
    block_count: u32,
    zstd_level: i32,
}

#[derive(Debug, Clone)]
struct RouteMeta {
    generation: u64,
    file: String,
    thread_entry_count: u64,
    checkpoint_entry_count: u64,
    route_file_bytes: u64,
    route_index_xxh3_64: u64,
}

#[derive(Debug, Clone)]
struct Manifest {
    generation: u64,
    sealed_end_wal_bytes: u64,
    checkpoint_count: u64,
    version_count: u64,
    thread_count: u64,
    stream_sizes: Vec<u64>,
    segments: Vec<SegmentMeta>,
    routes: Vec<RouteMeta>,
    store_id: Option<StoreId>,
    deleted_checkpoints: Vec<CheckpointTombstone>,
    retired_requests: Vec<RetiredRequestRecord>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            generation: 0,
            sealed_end_wal_bytes: 0,
            checkpoint_count: 0,
            version_count: 0,
            thread_count: 0,
            stream_sizes: vec![0; STREAM_NAMES.len()],
            segments: Vec::new(),
            routes: Vec::new(),
            store_id: None,
            deleted_checkpoints: Vec::new(),
            retired_requests: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LocalBlock {
    raw_offset: u64,
    encoded_offset: u64,
    raw_len: u32,
    encoded_len: u32,
    raw_xxh3_64: u64,
}

struct StreamState {
    raw_buf: Vec<u8>,
    raw_len: u64,
    flushed_raw_len: u64,
    blocks: Vec<LocalBlock>,
    hasher: Xxh3,
}

impl StreamState {
    fn new(block_size: usize) -> Self {
        Self {
            raw_buf: Vec::with_capacity(block_size),
            raw_len: 0,
            flushed_raw_len: 0,
            blocks: Vec::new(),
            hasher: Xxh3::new(),
        }
    }

    fn raw_hash(&self) -> u64 {
        if self.raw_len == 0 {
            0
        } else {
            self.hasher.digest()
        }
    }
}

struct StreamingSegmentWriter {
    file: File,
    tmp_path: PathBuf,
    states: Vec<StreamState>,
    stream_starts: Vec<u64>,
    block_size: u32,
    zstd_level: i32,
    generation: u64,
    checkpoint_start: u64,
    version_start: u64,
    payload_bytes: u64,
}

struct FinalizedSegment {
    tmp_path: PathBuf,
    meta: SegmentMeta,
}

impl StreamingSegmentWriter {
    fn new(
        live_dir: &Path,
        generation: u64,
        stream_starts: Vec<u64>,
        checkpoint_start: u64,
        version_start: u64,
        block_size: u32,
        zstd_level: i32,
    ) -> Result<Self, CheckpointStoreError> {
        if stream_starts.len() != STREAM_NAMES.len() {
            return Err(format_error("stream start width mismatch"));
        }
        let block_size_usize = usize::try_from(block_size)
            .map_err(|_| format_error("sealed block size overflow"))?;
        let final_path = live_dir.join(format!("structured-g{generation:06}.t3s"));
        let tmp_path = tmp_path_for(&final_path)?;
        if tmp_path.exists() {
            fs::remove_file(&tmp_path)?;
        }
        let mut file = File::create(&tmp_path)?;
        file.write_all(&vec![0u8; SEGMENT_HEADER_SIZE])?;
        Ok(Self {
            file,
            tmp_path,
            states: (0..STREAM_NAMES.len())
                .map(|_| StreamState::new(block_size_usize))
                .collect(),
            stream_starts,
            block_size,
            zstd_level,
            generation,
            checkpoint_start,
            version_start,
            payload_bytes: 0,
        })
    }

    fn current_stream_len(&self, id: usize) -> Result<u64, CheckpointStoreError> {
        let state = self
            .states
            .get(id)
            .ok_or_else(|| format_error("stream id outside writer"))?;
        self.stream_starts
            .get(id)
            .copied()
            .ok_or_else(|| format_error("stream start id outside writer"))?
            .checked_add(state.raw_len)
            .ok_or_else(|| format_error("stream length overflow"))
    }

    fn push(&mut self, id: usize, mut data: &[u8]) -> Result<(), CheckpointStoreError> {
        if id >= self.states.len() {
            return Err(format_error("stream id outside writer"));
        }
        {
            let state = self
                .states
                .get_mut(id)
                .ok_or_else(|| format_error("stream state missing"))?;
            state.hasher.update(data);
            state.raw_len = state
                .raw_len
                .checked_add(u64::try_from(data.len()).map_err(|_| format_error("stream push length overflow"))?)
                .ok_or_else(|| format_error("stream raw length overflow"))?;
        }
        let block_size = usize::try_from(self.block_size)
            .map_err(|_| format_error("block size overflow"))?;
        while !data.is_empty() {
            let current = self
                .states
                .get(id)
                .ok_or_else(|| format_error("stream state missing"))?
                .raw_buf
                .len();
            let take = (block_size - current).min(data.len());
            self.states
                .get_mut(id)
                .ok_or_else(|| format_error("stream state missing"))?
                .raw_buf
                .extend_from_slice(
                    data.get(..take)
                        .ok_or_else(|| format_error("stream push slice outside data"))?,
                );
            data = data
                .get(take..)
                .ok_or_else(|| format_error("stream remainder outside data"))?;
            if self
                .states
                .get(id)
                .ok_or_else(|| format_error("stream state missing"))?
                .raw_buf
                .len()
                == block_size
            {
                self.flush_stream_block(id)?;
            }
        }
        Ok(())
    }

    fn push_u32(&mut self, id: usize, value: u32) -> Result<(), CheckpointStoreError> {
        self.push(id, &value.to_le_bytes())
    }

    fn push_u64(&mut self, id: usize, value: u64) -> Result<(), CheckpointStoreError> {
        self.push(id, &value.to_le_bytes())
    }

    fn flush_stream_block(&mut self, id: usize) -> Result<(), CheckpointStoreError> {
        if self
            .states
            .get(id)
            .ok_or_else(|| format_error("stream state missing"))?
            .raw_buf
            .is_empty()
        {
            return Ok(());
        }
        let raw = std::mem::take(
            &mut self
                .states
                .get_mut(id)
                .ok_or_else(|| format_error("stream state missing"))?
                .raw_buf,
        );
        let encoded = zstd::bulk::compress(&raw, self.zstd_level)?;
        let state = self
            .states
            .get_mut(id)
            .ok_or_else(|| format_error("stream state missing"))?;
        let raw_offset = state.flushed_raw_len;
        let encoded_offset = self.payload_bytes;
        self.file.write_all(&encoded)?;
        let raw_len = u32::try_from(raw.len()).map_err(|_| format_error("raw block length overflow"))?;
        let encoded_len = u32::try_from(encoded.len()).map_err(|_| format_error("encoded block length overflow"))?;
        state.blocks.push(LocalBlock {
            raw_offset,
            encoded_offset,
            raw_len,
            encoded_len,
            raw_xxh3_64: xxh3_64(&raw),
        });
        state.flushed_raw_len = state
            .flushed_raw_len
            .checked_add(u64::from(raw_len))
            .ok_or_else(|| format_error("flushed raw length overflow"))?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(u64::from(encoded_len))
            .ok_or_else(|| format_error("encoded payload length overflow"))?;
        state.raw_buf = Vec::with_capacity(
            usize::try_from(self.block_size).map_err(|_| format_error("block size overflow"))?,
        );
        Ok(())
    }

    fn finalize(
        mut self,
        wal_start: u64,
        wal_end: u64,
        checkpoint_end: u64,
        version_end: u64,
    ) -> Result<FinalizedSegment, CheckpointStoreError> {
        for id in 0..self.states.len() {
            self.flush_stream_block(id)?;
        }
        let mut streams = Vec::with_capacity(STREAM_NAMES.len());
        let mut blocks = Vec::new();
        for (sid, state) in self.states.iter().enumerate() {
            let first = u32::try_from(blocks.len()).map_err(|_| format_error("block index overflow"))?;
            for block in &state.blocks {
                blocks.push(BlockEntry {
                    stream_id: u32::try_from(sid).map_err(|_| format_error("stream id overflow"))?,
                    raw_offset: block.raw_offset,
                    encoded_offset: block.encoded_offset,
                    raw_len: block.raw_len,
                    encoded_len: block.encoded_len,
                    raw_xxh3_64: block.raw_xxh3_64,
                });
            }
            let start = *self
                .stream_starts
                .get(sid)
                .ok_or_else(|| format_error("stream start missing"))?;
            let end = start
                .checked_add(state.raw_len)
                .ok_or_else(|| format_error("stream end overflow"))?;
            streams.push(StreamEntry {
                global_start: start,
                global_end: end,
                raw_len: state.raw_len,
                first_block: first,
                block_count: u32::try_from(state.blocks.len())
                    .map_err(|_| format_error("stream block count overflow"))?,
                raw_xxh3_64: state.raw_hash(),
            });
        }
        let mut index = Vec::new();
        for entry in &streams {
            write_stream_entry(&mut index, entry);
        }
        for block in &blocks {
            write_block_entry(&mut index, block);
        }
        let index_hash = if index.is_empty() { 0 } else { xxh3_64(&index) };
        let index_offset = u64::try_from(SEGMENT_HEADER_SIZE)
            .map_err(|_| format_error("segment header size overflow"))?
            .checked_add(self.payload_bytes)
            .ok_or_else(|| format_error("segment index offset overflow"))?;
        self.file.write_all(&index)?;
        let header = SegmentHeader {
            generation: self.generation,
            checkpoint_start_count: self.checkpoint_start,
            checkpoint_end_count: checkpoint_end,
            version_start_count: self.version_start,
            version_end_count: version_end,
            block_size: self.block_size,
            block_count: u32::try_from(blocks.len()).map_err(|_| format_error("block count overflow"))?,
            payload_offset: u64::try_from(SEGMENT_HEADER_SIZE).map_err(|_| format_error("header size overflow"))?,
            payload_bytes: self.payload_bytes,
            index_offset,
            stream_table_bytes: u64::try_from(STREAM_NAMES.len() * STREAM_ENTRY_SIZE)
                .map_err(|_| format_error("stream table length overflow"))?,
            block_table_bytes: u64::try_from(
                blocks
                    .len()
                    .checked_mul(BLOCK_ENTRY_SIZE)
                    .ok_or_else(|| format_error("block table length overflow"))?,
            )
            .map_err(|_| format_error("block table length overflow"))?,
            index_xxh3_64: index_hash,
            header_xxh3_64: 0,
        };
        let header_bytes = segment_header_bytes(header)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.flush()?;
        let file_bytes = fs::metadata(&self.tmp_path)?.len();
        let raw_total = streams.iter().try_fold(0u64, |acc, stream| {
            acc.checked_add(stream.raw_len)
                .ok_or_else(|| format_error("raw segment total overflow"))
        })?;
        let ends = streams.iter().map(|stream| stream.global_end).collect::<Vec<_>>();
        let meta = SegmentMeta {
            generation: self.generation,
            file: format!("structured-g{:06}.t3s", self.generation),
            wal_start_bytes: wal_start,
            wal_end_bytes: wal_end,
            checkpoint_start_count: self.checkpoint_start,
            checkpoint_end_count: checkpoint_end,
            version_start_count: self.version_start,
            version_end_count: version_end,
            raw_stream_bytes: raw_total,
            stream_starts: self.stream_starts.clone(),
            stream_ends: ends,
            segment_file_bytes: file_bytes,
            segment_file_xxh3_64: 0,
            index_bytes: u64::try_from(index.len()).map_err(|_| format_error("segment index length overflow"))?,
            index_xxh3_64: index_hash,
            block_size: self.block_size,
            block_count: u32::try_from(blocks.len()).map_err(|_| format_error("block count overflow"))?,
            zstd_level: self.zstd_level,
        };
        Ok(FinalizedSegment {
            tmp_path: self.tmp_path,
            meta,
        })
    }
}

fn write_transaction_to_segment(
    writer: &mut StreamingSegmentWriter,
    tx: &ParsedTransaction,
    state: &StoreState,
    next_thread_ordinal: &mut u32,
    new_threads: &mut Vec<(String, u32)>,
    new_checkpoints: &mut Vec<(String, String, u32)>,
    new_requests: &mut Vec<RequestRecord>,
) -> Result<(), CheckpointStoreError> {
    writer.push(PAYLOAD, &tx.bytes)?;
    for slot in tx.compact_nodes.chunks_exact(COMPACT_NODE_SIZE) {
        let code = match read_u32(slot, 12)? {
            KIND_LEAF => 0u8,
            KIND_BINARY => 1u8,
            KIND_WIDE => 2u8,
            _ => return Err(format_error("unknown compact node kind during sealing")),
        };
        writer.push(
            NODE_FIELD0,
            slot.get(..8).ok_or_else(|| format_error("node field0 slice missing"))?,
        )?;
        writer.push(
            NODE_FIELD1,
            slot.get(8..12).ok_or_else(|| format_error("node field1 slice missing"))?,
        )?;
        writer.push(NODE_KIND, &[code])?;
    }
    for record in tx.wide_nodes.chunks_exact(WIDE_RECORD_SIZE) {
        let code = match read_u32(record, 0)? {
            WIDE_KIND_LEAF => 0u8,
            WIDE_KIND_BINARY => 1u8,
            _ => return Err(format_error("unknown wide node kind during sealing")),
        };
        writer.push(WIDE_KIND, &[code])?;
        writer.push(WIDE_A, record.get(8..16).ok_or_else(|| format_error("wide field A missing"))?)?;
        writer.push(WIDE_B, record.get(16..24).ok_or_else(|| format_error("wide field B missing"))?)?;
        writer.push(WIDE_C, record.get(24..32).ok_or_else(|| format_error("wide field C missing"))?)?;
    }
    for (root, parent) in tx.roots.iter().zip(tx.parents.iter()) {
        writer.push_u64(VERSION_ROOT, root.unwrap_or(NONE_ROOT))?;
        writer.push_u32(VERSION_PARENT, parent.unwrap_or(NONE_PARENT))?;
    }
    let thread_ordinal = state
        .thread_ordinals
        .get(&tx.checkpoint.thread_id)
        .copied()
        .ok_or_else(|| format_error("checkpoint thread missing from committed state"))?;
    if thread_ordinal == *next_thread_ordinal {
        if writer.current_stream_len(THREAD_OFFSETS)? == 0 {
            writer.push_u32(THREAD_OFFSETS, 0)?;
        }
        writer.push(THREAD_BYTES, tx.checkpoint.thread_id.as_bytes())?;
        let end = u32::try_from(writer.current_stream_len(THREAD_BYTES)?)
            .map_err(|_| format_error("thread byte offset exceeds u32"))?;
        writer.push_u32(THREAD_OFFSETS, end)?;
        new_threads.push((tx.checkpoint.thread_id.clone(), thread_ordinal));
        *next_thread_ordinal = next_thread_ordinal
            .checked_add(1)
            .ok_or_else(|| format_error("thread ordinal overflow"))?;
    } else if thread_ordinal > *next_thread_ordinal {
        return Err(format_error("thread ordinal skipped during sealing"));
    }
    writer.push_u32(CP_THREAD, thread_ordinal)?;
    writer.push_u32(CP_NO, tx.checkpoint.checkpoint_no)?;
    if writer.current_stream_len(CP_ID_OFFSETS)? == 0 {
        writer.push_u32(CP_ID_OFFSETS, 0)?;
    }
    writer.push(CP_ID_BYTES, tx.checkpoint.checkpoint_id.as_bytes())?;
    let cp_id_end = u32::try_from(writer.current_stream_len(CP_ID_BYTES)?)
        .map_err(|_| format_error("checkpoint id bytes exceed u32"))?;
    writer.push_u32(CP_ID_OFFSETS, cp_id_end)?;
    let parent_ordinal = match tx.checkpoint.parent_checkpoint_id.as_ref() {
        None => NONE_PARENT,
        Some(parent) => state
            .checkpoint_ordinals
            .get(&(tx.checkpoint.thread_id.clone(), parent.clone()))
            .copied()
            .ok_or_else(|| format_error("checkpoint parent missing during sealing"))?,
    };
    writer.push_u32(CP_PARENT_ORDINAL, parent_ordinal)?;
    writer.push_u32(CP_IDENTITY_VERSION, tx.checkpoint.identity_version)?;
    writer.push_u32(CP_MESSAGES_VERSION, tx.checkpoint.messages_version.unwrap_or(NONE_VERSION))?;
    writer.push_u32(CP_RESULT_VERSION, tx.checkpoint.result_version.unwrap_or(NONE_VERSION))?;
    writer.push_u64(CP_LOGICAL_LEN, tx.checkpoint.logical_state_len)?;
    writer.push_u64(CP_STATE_HASH, tx.checkpoint.state_hash)?;
    new_checkpoints.push((
        tx.checkpoint.thread_id.clone(),
        tx.checkpoint.checkpoint_id.clone(),
        tx.checkpoint.ordinal,
    ));
    if let Some(request_id) = tx.request_id.as_ref() {
        new_requests.push(RequestRecord {
            key: request_id.clone(),
            operation_digest: tx.operation_digest,
            checkpoint_ordinal: tx.checkpoint.ordinal,
        });
    }
    Ok(())
}

fn segment_header_bytes(mut header: SegmentHeader) -> Result<Vec<u8>, CheckpointStoreError> {
    let mut out = Vec::with_capacity(SEGMENT_HEADER_SIZE);
    out.extend_from_slice(SEGMENT_MAGIC);
    out.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(STREAM_NAMES.len())
            .map_err(|_| format_error("stream count overflow"))?
            .to_le_bytes(),
    );
    out.extend_from_slice(&header.generation.to_le_bytes());
    out.extend_from_slice(&header.checkpoint_start_count.to_le_bytes());
    out.extend_from_slice(&header.checkpoint_end_count.to_le_bytes());
    out.extend_from_slice(&header.version_start_count.to_le_bytes());
    out.extend_from_slice(&header.version_end_count.to_le_bytes());
    out.extend_from_slice(&header.block_size.to_le_bytes());
    out.extend_from_slice(&header.block_count.to_le_bytes());
    out.extend_from_slice(&header.payload_offset.to_le_bytes());
    out.extend_from_slice(&header.payload_bytes.to_le_bytes());
    out.extend_from_slice(&header.index_offset.to_le_bytes());
    out.extend_from_slice(&header.stream_table_bytes.to_le_bytes());
    out.extend_from_slice(&header.block_table_bytes.to_le_bytes());
    out.extend_from_slice(&header.index_xxh3_64.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    if out.len() != SEGMENT_HEADER_SIZE {
        return Err(format_error("segment header width mismatch"));
    }
    header.header_xxh3_64 = xxh3_64(
        out.get(..112)
            .ok_or_else(|| format_error("segment header checksum slice missing"))?,
    );
    out.get_mut(112..120)
        .ok_or_else(|| format_error("segment header hash field missing"))?
        .copy_from_slice(&header.header_xxh3_64.to_le_bytes());
    Ok(out)
}

fn write_stream_entry(out: &mut Vec<u8>, entry: &StreamEntry) {
    out.extend_from_slice(&entry.global_start.to_le_bytes());
    out.extend_from_slice(&entry.global_end.to_le_bytes());
    out.extend_from_slice(&entry.raw_len.to_le_bytes());
    out.extend_from_slice(&entry.first_block.to_le_bytes());
    out.extend_from_slice(&entry.block_count.to_le_bytes());
    out.extend_from_slice(&entry.raw_xxh3_64.to_le_bytes());
}

fn write_block_entry(out: &mut Vec<u8>, entry: &BlockEntry) {
    out.extend_from_slice(&entry.stream_id.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&entry.raw_offset.to_le_bytes());
    out.extend_from_slice(&entry.encoded_offset.to_le_bytes());
    out.extend_from_slice(&entry.raw_len.to_le_bytes());
    out.extend_from_slice(&entry.encoded_len.to_le_bytes());
    out.extend_from_slice(&entry.raw_xxh3_64.to_le_bytes());
}

fn parse_segment_header(data: &[u8]) -> Result<SegmentHeader, CheckpointStoreError> {
    if data.len() < SEGMENT_HEADER_SIZE || data.get(..8) != Some(SEGMENT_MAGIC.as_slice()) {
        return Err(format_error("segment magic/header mismatch"));
    }
    if read_u32(data, 8)? != SEGMENT_FORMAT_VERSION {
        return Err(format_error("segment format version mismatch"));
    }
    if usize::try_from(read_u32(data, 12)?)
        .map_err(|_| format_error("segment stream count overflow"))?
        != STREAM_NAMES.len()
    {
        return Err(format_error("segment stream count mismatch"));
    }
    let header = SegmentHeader {
        generation: read_u64(data, 16)?,
        checkpoint_start_count: read_u64(data, 24)?,
        checkpoint_end_count: read_u64(data, 32)?,
        version_start_count: read_u64(data, 40)?,
        version_end_count: read_u64(data, 48)?,
        block_size: read_u32(data, 56)?,
        block_count: read_u32(data, 60)?,
        payload_offset: read_u64(data, 64)?,
        payload_bytes: read_u64(data, 72)?,
        index_offset: read_u64(data, 80)?,
        stream_table_bytes: read_u64(data, 88)?,
        block_table_bytes: read_u64(data, 96)?,
        index_xxh3_64: read_u64(data, 104)?,
        header_xxh3_64: read_u64(data, 112)?,
    };
    if xxh3_64(data.get(..112).ok_or_else(|| format_error("segment header checksum slice missing"))?)
        != header.header_xxh3_64
    {
        return Err(format_error("segment header checksum mismatch"));
    }
    if header.block_size == 0
        || header.payload_offset != u64::try_from(SEGMENT_HEADER_SIZE).map_err(|_| format_error("header size overflow"))?
        || header.index_offset
            != header
                .payload_offset
                .checked_add(header.payload_bytes)
                .ok_or_else(|| format_error("segment index offset overflow"))?
        || header.stream_table_bytes
            != u64::try_from(STREAM_NAMES.len() * STREAM_ENTRY_SIZE)
                .map_err(|_| format_error("stream table size overflow"))?
        || header.block_table_bytes
            != u64::from(header.block_count)
                .checked_mul(u64::try_from(BLOCK_ENTRY_SIZE).map_err(|_| format_error("block entry size overflow"))?)
                .ok_or_else(|| format_error("block table size overflow"))?
    {
        return Err(format_error("segment header geometry mismatch"));
    }
    Ok(header)
}

struct ParsedSegmentIndex {
    header: SegmentHeader,
    streams: Vec<StreamEntry>,
    blocks: Vec<BlockEntry>,
}

fn read_lazy_segment_index_file(path: &Path) -> Result<LazySegmentIndex, CheckpointStoreError> {
    let mut file = File::open(path)?;
    let file_bytes = file.metadata()?.len();
    let mut header_bytes = vec![0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let header = parse_segment_header(&header_bytes)?;
    let index_bytes_len = header
        .stream_table_bytes
        .checked_add(header.block_table_bytes)
        .ok_or_else(|| format_error("lazy segment index length overflow"))?;
    let index_end = header
        .index_offset
        .checked_add(index_bytes_len)
        .ok_or_else(|| format_error("lazy segment index end overflow"))?;
    if index_end != file_bytes {
        return Err(format_error("lazy segment index does not end at file boundary"));
    }
    let stream_table_len = usize::try_from(header.stream_table_bytes)
        .map_err(|_| format_error("lazy segment stream table length exceeds usize"))?;
    let mut stream_bytes = vec![0u8; stream_table_len];
    file.seek(SeekFrom::Start(header.index_offset))?;
    file.read_exact(&mut stream_bytes)?;
    let mut streams = Vec::with_capacity(STREAM_NAMES.len());
    for index in 0..STREAM_NAMES.len() {
        let base = index
            .checked_mul(STREAM_ENTRY_SIZE)
            .ok_or_else(|| format_error("lazy stream entry offset overflow"))?;
        let entry = StreamEntry {
            global_start: read_u64(&stream_bytes, base)?,
            global_end: read_u64(&stream_bytes, base + 8)?,
            raw_len: read_u64(&stream_bytes, base + 16)?,
            first_block: read_u32(&stream_bytes, base + 24)?,
            block_count: read_u32(&stream_bytes, base + 28)?,
            raw_xxh3_64: read_u64(&stream_bytes, base + 32)?,
        };
        if entry.global_end < entry.global_start
            || entry.raw_len != entry.global_end - entry.global_start
            || u64::from(entry.first_block) + u64::from(entry.block_count)
                > u64::from(header.block_count)
        {
            return Err(format_error("lazy segment stream entry invalid"));
        }
        streams.push(entry);
    }
    Ok(LazySegmentIndex { header, streams })
}

fn read_lazy_block_entry(
    file: &File,
    index: &LazySegmentIndex,
    block_index: u32,
) -> Result<BlockEntry, CheckpointStoreError> {
    if block_index >= index.header.block_count {
        return Err(format_error("lazy block index outside segment"));
    }
    let block_offset = u64::from(block_index)
        .checked_mul(u64::try_from(BLOCK_ENTRY_SIZE).map_err(|_| format_error("block entry size overflow"))?)
        .and_then(|offset| index.header.index_offset.checked_add(index.header.stream_table_bytes)?.checked_add(offset))
        .ok_or_else(|| format_error("lazy block table offset overflow"))?;
    let mut bytes = [0u8; BLOCK_ENTRY_SIZE];
    read_file_exact_at(file, &mut bytes, block_offset)?;
    let entry = BlockEntry {
        stream_id: read_u32(&bytes, 0)?,
        raw_offset: read_u64(&bytes, 8)?,
        encoded_offset: read_u64(&bytes, 16)?,
        raw_len: read_u32(&bytes, 24)?,
        encoded_len: read_u32(&bytes, 28)?,
        raw_xxh3_64: read_u64(&bytes, 32)?,
    };
    if usize::try_from(entry.stream_id).map_err(|_| format_error("lazy block stream id overflow"))?
        >= STREAM_NAMES.len()
        || entry
            .encoded_offset
            .checked_add(u64::from(entry.encoded_len))
            .ok_or_else(|| format_error("lazy encoded block end overflow"))?
            > index.header.payload_bytes
    {
        return Err(format_error("lazy segment block entry invalid"));
    }
    Ok(entry)
}

fn read_segment_index_file(path: &Path) -> Result<ParsedSegmentIndex, CheckpointStoreError> {
    let mut file = File::open(path)?;
    let file_bytes = file.metadata()?.len();
    let mut header_bytes = vec![0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let header = parse_segment_header(&header_bytes)?;
    let index_bytes_len = header
        .stream_table_bytes
        .checked_add(header.block_table_bytes)
        .ok_or_else(|| format_error("segment index length overflow"))?;
    let index_end = header
        .index_offset
        .checked_add(index_bytes_len)
        .ok_or_else(|| format_error("segment index end overflow"))?;
    if index_end != file_bytes {
        return Err(format_error("segment index does not end at file boundary"));
    }
    file.seek(SeekFrom::Start(header.index_offset))?;
    let mut index_bytes = vec![0u8; usize::try_from(index_bytes_len)
        .map_err(|_| format_error("segment index length exceeds usize"))?];
    file.read_exact(&mut index_bytes)?;
    if xxh3_64(&index_bytes) != header.index_xxh3_64 {
        return Err(format_error("segment index checksum mismatch"));
    }
    let mut streams = Vec::with_capacity(STREAM_NAMES.len());
    for index in 0..STREAM_NAMES.len() {
        let base = index
            .checked_mul(STREAM_ENTRY_SIZE)
            .ok_or_else(|| format_error("stream entry offset overflow"))?;
        let entry = StreamEntry {
            global_start: read_u64(&index_bytes, base)?,
            global_end: read_u64(&index_bytes, base + 8)?,
            raw_len: read_u64(&index_bytes, base + 16)?,
            first_block: read_u32(&index_bytes, base + 24)?,
            block_count: read_u32(&index_bytes, base + 28)?,
            raw_xxh3_64: read_u64(&index_bytes, base + 32)?,
        };
        if entry.global_end < entry.global_start
            || entry.raw_len != entry.global_end - entry.global_start
            || u64::from(entry.first_block) + u64::from(entry.block_count) > u64::from(header.block_count)
        {
            return Err(format_error("segment stream entry invalid"));
        }
        streams.push(entry);
    }
    let block_base = usize::try_from(header.stream_table_bytes)
        .map_err(|_| format_error("block table base overflow"))?;
    let mut blocks = Vec::with_capacity(
        usize::try_from(header.block_count).map_err(|_| format_error("block count overflow"))?,
    );
    for index in 0..usize::try_from(header.block_count).map_err(|_| format_error("block count overflow"))? {
        let base = block_base
            .checked_add(index.checked_mul(BLOCK_ENTRY_SIZE).ok_or_else(|| format_error("block entry offset overflow"))?)
            .ok_or_else(|| format_error("block table offset overflow"))?;
        let entry = BlockEntry {
            stream_id: read_u32(&index_bytes, base)?,
            raw_offset: read_u64(&index_bytes, base + 8)?,
            encoded_offset: read_u64(&index_bytes, base + 16)?,
            raw_len: read_u32(&index_bytes, base + 24)?,
            encoded_len: read_u32(&index_bytes, base + 28)?,
            raw_xxh3_64: read_u64(&index_bytes, base + 32)?,
        };
        if usize::try_from(entry.stream_id).map_err(|_| format_error("block stream id overflow"))? >= STREAM_NAMES.len()
            || entry
                .encoded_offset
                .checked_add(u64::from(entry.encoded_len))
                .ok_or_else(|| format_error("encoded block end overflow"))?
                > header.payload_bytes
        {
            return Err(format_error("segment block entry invalid"));
        }
        blocks.push(entry);
    }
    Ok(ParsedSegmentIndex {
        header,
        streams,
        blocks,
    })
}

fn materialize_sealed_state(
    dir: &Path,
    manifest: &Manifest,
    recovery_mode: CheckpointStoreRecoveryMode,
) -> Result<StoreState, CheckpointStoreError> {
    if manifest.generation == 0 {
        let mut state = StoreState::default();
        apply_manifest_lifecycle_metadata(&mut state, manifest)?;
        return Ok(state);
    }
    let mut streams = (0..STREAM_NAMES.len()).map(|_| Vec::<u8>::new()).collect::<Vec<_>>();
    let mut zstd_decompressor = zstd::bulk::Decompressor::new()?;
    for segment in &manifest.segments {
        let path = dir.join(&segment.file);
        let parsed = read_segment_index_file(&path)?;
        if parsed.header.generation != segment.generation
            || parsed.header.checkpoint_start_count != segment.checkpoint_start_count
            || parsed.header.checkpoint_end_count != segment.checkpoint_end_count
            || parsed.header.version_start_count != segment.version_start_count
            || parsed.header.version_end_count != segment.version_end_count
        {
            return Err(format_error("segment header disagrees with manifest"));
        }
        let mut file = File::open(&path)?;
        for (stream_id, stream_entry) in parsed.streams.iter().enumerate() {
            let current = u64::try_from(
                streams
                    .get(stream_id)
                    .ok_or_else(|| format_error("materialized stream missing"))?
                    .len(),
            )
            .map_err(|_| format_error("materialized stream length overflow"))?;
            if current != stream_entry.global_start {
                return Err(format_error("sealed stream global start is not contiguous"));
            }
            let block_start = usize::try_from(stream_entry.first_block)
                .map_err(|_| format_error("stream first block overflow"))?;
            let block_end = block_start
                .checked_add(usize::try_from(stream_entry.block_count).map_err(|_| format_error("stream block count overflow"))?)
                .ok_or_else(|| format_error("stream block range overflow"))?;
            let mut segment_raw = Vec::new();
            for block in parsed
                .blocks
                .get(block_start..block_end)
                .ok_or_else(|| format_error("stream block range outside table"))?
            {
                if usize::try_from(block.stream_id).map_err(|_| format_error("block stream id overflow"))? != stream_id {
                    return Err(format_error("block belongs to wrong stream"));
                }
                file.seek(SeekFrom::Start(
                    parsed
                        .header
                        .payload_offset
                        .checked_add(block.encoded_offset)
                        .ok_or_else(|| format_error("encoded block file offset overflow"))?,
                ))?;
                let mut encoded = vec![0u8; usize::try_from(block.encoded_len)
                    .map_err(|_| format_error("encoded block length overflow"))?];
                file.read_exact(&mut encoded)?;
                let raw = zstd_decompressor.decompress(
                    &encoded,
                    usize::try_from(block.raw_len).map_err(|_| format_error("raw block length overflow"))?,
                )?;
                if raw.len() != usize::try_from(block.raw_len).map_err(|_| format_error("raw block length overflow"))?
                    || xxh3_64(&raw) != block.raw_xxh3_64
                {
                    return Err(format_error("sealed block decompression/hash mismatch"));
                }
                segment_raw.extend_from_slice(&raw);
            }
            if u64::try_from(segment_raw.len()).map_err(|_| format_error("stream raw length overflow"))? != stream_entry.raw_len
                || (stream_entry.raw_len > 0 && xxh3_64(&segment_raw) != stream_entry.raw_xxh3_64)
            {
                return Err(format_error("sealed stream raw length/hash mismatch"));
            }
            streams
                .get_mut(stream_id)
                .ok_or_else(|| format_error("materialized stream missing"))?
                .extend_from_slice(&segment_raw);
        }
    }
    for (index, expected) in manifest.stream_sizes.iter().enumerate() {
        let actual = u64::try_from(
            streams
                .get(index)
                .ok_or_else(|| format_error("materialized stream missing"))?
                .len(),
        )
        .map_err(|_| format_error("materialized stream length overflow"))?;
        if actual != *expected {
            return Err(format_error("materialized stream size disagrees with manifest"));
        }
    }
    let mut state = state_from_streams(streams, manifest, recovery_mode)?;
    apply_manifest_lifecycle_metadata(&mut state, manifest)?;
    for route in &manifest.routes {
        for record in route_request_records(&dir.join(&route.file))? {
            if u64::from(record.checkpoint_ordinal) >= manifest.checkpoint_count
                || state
                    .checkpoints
                    .get(usize::try_from(record.checkpoint_ordinal).map_err(|_| {
                        format_error("request checkpoint ordinal overflow")
                    })?)
                    .is_none()
            {
                return Err(format_error("request checkpoint ordinal outside sealed state"));
            }
            if state.retired_requests.contains_key(&record.key) {
                return Err(format_error("active request key is also retired"));
            }
            if state.request_records.insert(record.key.clone(), record).is_some() {
                return Err(format_error("duplicate sealed request id"));
            }
        }
    }
    Ok(state)
}

fn apply_manifest_lifecycle_metadata(
    state: &mut StoreState,
    manifest: &Manifest,
) -> Result<(), CheckpointStoreError> {
    for tombstone in &manifest.deleted_checkpoints {
        let key = (tombstone.thread_id.clone(), tombstone.checkpoint_id.clone());
        if state.checkpoint_ordinals.contains_key(&key) || !state.deleted_checkpoints.insert(key) {
            return Err(format_error("duplicate or live checkpoint tombstone"));
        }
    }
    for retired in &manifest.retired_requests {
        if state.request_records.contains_key(&retired.key) {
            return Err(format_error("active request key is also retired"));
        }
        if state
            .retired_requests
            .insert(retired.key.clone(), retired.operation_digest)
            .is_some()
        {
            return Err(format_error("duplicate retired request key"));
        }
    }
    Ok(())
}

fn state_from_streams(
    mut streams: Vec<Vec<u8>>,
    manifest: &Manifest,
    recovery_mode: CheckpointStoreRecoveryMode,
) -> Result<StoreState, CheckpointStoreError> {
    let mut state = StoreState::default();
    state.arena_bytes = match recovery_mode {
        CheckpointStoreRecoveryMode::ClonePayload => streams
            .get(PAYLOAD)
            .ok_or_else(|| format_error("payload stream missing"))?
            .clone(),
        CheckpointStoreRecoveryMode::ReusePayload => streams
            .get_mut(PAYLOAD)
            .ok_or_else(|| format_error("payload stream missing"))
            .map(std::mem::take)?,
        CheckpointStoreRecoveryMode::Lazy => {
            return Err(format_error(
                "lazy recovery must use the bounded sealed reader",
            ));
        }
    };
    let kinds = streams.get(NODE_KIND).ok_or_else(|| format_error("node kind stream missing"))?;
    let field0 = streams.get(NODE_FIELD0).ok_or_else(|| format_error("node field0 stream missing"))?;
    let field1 = streams.get(NODE_FIELD1).ok_or_else(|| format_error("node field1 stream missing"))?;
    if field0.len() != kinds.len().checked_mul(8).ok_or_else(|| format_error("node field0 size overflow"))?
        || field1.len() != kinds.len().checked_mul(4).ok_or_else(|| format_error("node field1 size overflow"))?
    {
        return Err(format_error("sealed node column widths disagree"));
    }
    for index in 0..kinds.len() {
        let mut slot = [0u8; COMPACT_NODE_SIZE];
        let f0_start = index * 8;
        let f1_start = index * 4;
        slot.get_mut(..8)
            .ok_or_else(|| format_error("compact slot field0 missing"))?
            .copy_from_slice(
                field0
                    .get(f0_start..f0_start + 8)
                    .ok_or_else(|| format_error("node field0 value missing"))?,
            );
        slot.get_mut(8..12)
            .ok_or_else(|| format_error("compact slot field1 missing"))?
            .copy_from_slice(
                field1
                    .get(f1_start..f1_start + 4)
                    .ok_or_else(|| format_error("node field1 value missing"))?,
            );
        let kind = match *kinds.get(index).ok_or_else(|| format_error("node kind value missing"))? {
            0 => KIND_LEAF,
            1 => KIND_BINARY,
            2 => KIND_WIDE,
            _ => return Err(format_error("sealed node kind code invalid")),
        };
        slot.get_mut(12..16)
            .ok_or_else(|| format_error("compact slot kind field missing"))?
            .copy_from_slice(&kind.to_le_bytes());
        state.compact_nodes.extend_from_slice(&slot);
    }
    let wide_kinds = streams.get(WIDE_KIND).ok_or_else(|| format_error("wide kind stream missing"))?;
    for name in [WIDE_A, WIDE_B, WIDE_C] {
        if streams.get(name).ok_or_else(|| format_error("wide field stream missing"))?.len()
            != wide_kinds.len().checked_mul(8).ok_or_else(|| format_error("wide field size overflow"))?
        {
            return Err(format_error("sealed wide column widths disagree"));
        }
    }
    for index in 0..wide_kinds.len() {
        let mut record = [0u8; WIDE_RECORD_SIZE];
        let kind = match *wide_kinds.get(index).ok_or_else(|| format_error("wide kind missing"))? {
            0 => WIDE_KIND_LEAF,
            1 => WIDE_KIND_BINARY,
            _ => return Err(format_error("sealed wide kind code invalid")),
        };
        record.get_mut(..4).ok_or_else(|| format_error("wide kind field missing"))?.copy_from_slice(&kind.to_le_bytes());
        for (stream_id, dest_start) in [(WIDE_A, 8usize), (WIDE_B, 16usize), (WIDE_C, 24usize)] {
            let source_start = index * 8;
            record
                .get_mut(dest_start..dest_start + 8)
                .ok_or_else(|| format_error("wide destination field missing"))?
                .copy_from_slice(
                    streams
                        .get(stream_id)
                        .ok_or_else(|| format_error("wide source stream missing"))?
                        .get(source_start..source_start + 8)
                        .ok_or_else(|| format_error("wide source value missing"))?,
                );
        }
        state.wide_nodes.extend_from_slice(&record);
    }
    let roots = streams.get(VERSION_ROOT).ok_or_else(|| format_error("version-root stream missing"))?;
    let parents = streams.get(VERSION_PARENT).ok_or_else(|| format_error("version-parent stream missing"))?;
    if roots.len() % 8 != 0 || parents.len() % 4 != 0 || roots.len() / 8 != parents.len() / 4 {
        return Err(format_error("sealed version column widths disagree"));
    }
    for index in 0..roots.len() / 8 {
        let raw_root = read_u64(roots, index * 8)?;
        let raw_parent = read_u32(parents, index * 4)?;
        state.versions.push(if raw_root == NONE_ROOT { None } else { Some(raw_root) });
        state.parents.push(if raw_parent == NONE_PARENT { None } else { Some(raw_parent) });
    }
    state.threads = decode_string_table(
        streams.get(THREAD_OFFSETS).ok_or_else(|| format_error("thread offsets missing"))?,
        streams.get(THREAD_BYTES).ok_or_else(|| format_error("thread bytes missing"))?,
    )?;
    for (index, thread) in state.threads.iter().enumerate() {
        state.thread_ordinals.insert(
            thread.clone(),
            u32::try_from(index).map_err(|_| format_error("thread ordinal overflow"))?,
        );
    }
    let checkpoint_ids = decode_string_table(
        streams.get(CP_ID_OFFSETS).ok_or_else(|| format_error("checkpoint id offsets missing"))?,
        streams.get(CP_ID_BYTES).ok_or_else(|| format_error("checkpoint id bytes missing"))?,
    )?;
    let checkpoint_count = usize::try_from(manifest.checkpoint_count)
        .map_err(|_| format_error("manifest checkpoint count overflow"))?;
    if checkpoint_ids.len() != checkpoint_count {
        return Err(format_error("checkpoint id table count mismatch"));
    }
    let require_width = |stream_id: usize, width: usize| -> Result<(), CheckpointStoreError> {
        let actual = streams.get(stream_id).ok_or_else(|| format_error("checkpoint stream missing"))?.len();
        let expected = checkpoint_count.checked_mul(width).ok_or_else(|| format_error("checkpoint stream expected width overflow"))?;
        if actual != expected {
            Err(format_error("checkpoint stream width mismatch"))
        } else {
            Ok(())
        }
    };
    for stream_id in [CP_THREAD, CP_NO, CP_PARENT_ORDINAL, CP_IDENTITY_VERSION, CP_MESSAGES_VERSION, CP_RESULT_VERSION] {
        require_width(stream_id, 4)?;
    }
    for stream_id in [CP_LOGICAL_LEN, CP_STATE_HASH] {
        require_width(stream_id, 8)?;
    }
    for index in 0..checkpoint_count {
        let thread_ordinal = read_u32(streams.get(CP_THREAD).ok_or_else(|| format_error("checkpoint thread stream missing"))?, index * 4)?;
        let thread = state
            .threads
            .get(usize::try_from(thread_ordinal).map_err(|_| format_error("thread ordinal overflow"))?)
            .cloned()
            .ok_or_else(|| format_error("checkpoint thread ordinal outside table"))?;
        let parent_ordinal = read_u32(streams.get(CP_PARENT_ORDINAL).ok_or_else(|| format_error("parent ordinal stream missing"))?, index * 4)?;
        let parent_checkpoint_id = if parent_ordinal == NONE_PARENT {
            None
        } else {
            let parent_index = usize::try_from(parent_ordinal).map_err(|_| format_error("parent ordinal overflow"))?;
            if parent_index >= index {
                return Err(format_error("sealed checkpoint parent is not prior"));
            }
            Some(
                checkpoint_ids
                    .get(parent_index)
                    .cloned()
                    .ok_or_else(|| format_error("parent checkpoint id missing"))?,
            )
        };
        let optional = |raw: u32| if raw == NONE_VERSION { None } else { Some(raw) };
        let info = CheckpointInfo {
            ordinal: u32::try_from(index).map_err(|_| format_error("checkpoint ordinal overflow"))?,
            thread_id: thread.clone(),
            checkpoint_no: read_u32(streams.get(CP_NO).ok_or_else(|| format_error("checkpoint number stream missing"))?, index * 4)?,
            checkpoint_id: checkpoint_ids.get(index).cloned().ok_or_else(|| format_error("checkpoint id missing"))?,
            parent_checkpoint_id,
            identity_version: read_u32(streams.get(CP_IDENTITY_VERSION).ok_or_else(|| format_error("identity version stream missing"))?, index * 4)?,
            messages_version: optional(read_u32(streams.get(CP_MESSAGES_VERSION).ok_or_else(|| format_error("messages version stream missing"))?, index * 4)?),
            result_version: optional(read_u32(streams.get(CP_RESULT_VERSION).ok_or_else(|| format_error("result version stream missing"))?, index * 4)?),
            logical_state_len: read_u64(streams.get(CP_LOGICAL_LEN).ok_or_else(|| format_error("logical length stream missing"))?, index * 8)?,
            state_hash: read_u64(streams.get(CP_STATE_HASH).ok_or_else(|| format_error("state hash stream missing"))?, index * 8)?,
        };
        state.checkpoint_ordinals.insert((thread, info.checkpoint_id.clone()), info.ordinal);
        state.checkpoints.push(info);
    }
    if state.versions.len() != usize::try_from(manifest.version_count).map_err(|_| format_error("manifest version count overflow"))?
        || state.threads.len() != usize::try_from(manifest.thread_count).map_err(|_| format_error("manifest thread count overflow"))?
    {
        return Err(format_error("materialized sealed metadata counts disagree with manifest"));
    }
    validate_materialized_state(&state)?;
    Ok(state)
}

fn state_from_lazy_reader(
    dir: &Path,
    manifest: &Manifest,
    reader: &LazyCheckpointStore,
) -> Result<StoreState, CheckpointStoreError> {
    let mut state = StoreState {
        base_geometry: Some(Geometry::from_manifest(manifest)?),
        versions: reader.metadata.versions.clone(),
        parents: reader.metadata.version_parents.clone(),
        threads: reader.metadata.threads.clone(),
        thread_ordinals: reader.metadata.thread_ordinals.clone(),
        checkpoints: reader.metadata.checkpoints.clone(),
        checkpoint_ordinals: reader.metadata.checkpoint_ordinals.clone(),
        ..StoreState::default()
    };
    for route in &manifest.routes {
        for record in route_request_records(&dir.join(&route.file))? {
            if u64::from(record.checkpoint_ordinal) >= manifest.checkpoint_count
                || state
                    .checkpoints
                    .get(usize::try_from(record.checkpoint_ordinal).map_err(|_| {
                        format_error("request checkpoint ordinal overflow")
                    })?)
                    .is_none()
            {
                return Err(format_error("request checkpoint ordinal outside lazy state"));
            }
            if state.retired_requests.contains_key(&record.key)
                || state.request_records.insert(record.key.clone(), record).is_some()
            {
                return Err(format_error("duplicate or retired lazy request id"));
            }
        }
    }
    apply_manifest_lifecycle_metadata(&mut state, manifest)?;
    Ok(state)
}

fn decode_string_table(offsets: &[u8], bytes: &[u8]) -> Result<Vec<String>, CheckpointStoreError> {
    if offsets.is_empty() {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        return Err(format_error("string bytes exist without offsets"));
    }
    if offsets.len() % 4 != 0 || offsets.len() < 4 {
        return Err(format_error("string offset table is not u32 aligned"));
    }
    let count = offsets.len() / 4 - 1;
    let mut result = Vec::with_capacity(count);
    let mut previous = read_u32(offsets, 0)?;
    if previous != 0 {
        return Err(format_error("string offset table does not start at zero"));
    }
    for index in 0..count {
        let next = read_u32(offsets, (index + 1) * 4)?;
        if next < previous {
            return Err(format_error("string offsets regress"));
        }
        let start = usize::try_from(previous).map_err(|_| format_error("string offset overflow"))?;
        let end = usize::try_from(next).map_err(|_| format_error("string offset overflow"))?;
        let value = std::str::from_utf8(bytes.get(start..end).ok_or_else(|| format_error("string bytes outside table"))?)
            .map_err(|_| format_error("string table contains non-UTF8 bytes"))?
            .to_owned();
        result.push(value);
        previous = next;
    }
    if usize::try_from(previous).map_err(|_| format_error("final string offset overflow"))? != bytes.len() {
        return Err(format_error("final string offset does not match byte table"));
    }
    Ok(result)
}

fn validate_materialized_state(state: &StoreState) -> Result<(), CheckpointStoreError> {
    if state.compact_nodes.len() % COMPACT_NODE_SIZE != 0 || state.wide_nodes.len() % WIDE_RECORD_SIZE != 0 {
        return Err(format_error("materialized node bytes are not record aligned"));
    }
    let dummy = ParsedTransaction {
        start_offset: 0,
        end_offset: 0,
        version_start: 0,
        version_count: u64::try_from(state.versions.len()).map_err(|_| format_error("version count overflow"))?,
        checkpoint_count: u64::try_from(state.checkpoints.len()).map_err(|_| format_error("checkpoint count overflow"))?,
        byte_start: 0,
        byte_end: u64::try_from(state.arena_bytes.len()).map_err(|_| format_error("arena length overflow"))?,
        bytes: state.arena_bytes.clone(),
        node_start: 0,
        node_end: u64::try_from(state.compact_nodes.len() / COMPACT_NODE_SIZE).map_err(|_| format_error("node count overflow"))?,
        compact_nodes: state.compact_nodes.clone(),
        wide_start: 0,
        wide_end: u64::try_from(state.wide_nodes.len() / WIDE_RECORD_SIZE).map_err(|_| format_error("wide count overflow"))?,
        wide_nodes: state.wide_nodes.clone(),
        roots: state.versions.clone(),
        parents: state.parents.clone(),
        checkpoint: state.checkpoints.last().cloned().unwrap_or(CheckpointInfo {
            ordinal: 0,
            thread_id: String::new(),
            checkpoint_no: 0,
            checkpoint_id: String::new(),
            parent_checkpoint_id: None,
            identity_version: 0,
            messages_version: None,
            result_version: None,
            logical_state_len: 0,
            state_hash: 0,
        }),
        request_id: None,
        operation_digest: [0u8; 32],
    };
    let empty = StoreState::default();
    validate_new_nodes(&empty, &dummy)?;
    Ok(())
}

struct CompactedState {
    state: StoreState,
    deleted_checkpoints: Vec<(String, String)>,
    retired_requests: Vec<(Vec<u8>, [u8; 32])>,
}

struct CompactedGeneration {
    finalized: FinalizedSegment,
    route_bytes: Vec<u8>,
    route_meta: RouteMeta,
}

fn append_compacted_leaf(
    source: &StoreState,
    target: &mut StoreState,
    offset: u64,
    length: u64,
) -> Result<u64, CheckpointStoreError> {
    let start = usize::try_from(offset).map_err(|_| format_error("source leaf offset exceeds usize"))?;
    let length_usize = usize::try_from(length).map_err(|_| format_error("source leaf length exceeds usize"))?;
    let end = start
        .checked_add(length_usize)
        .ok_or_else(|| format_error("source leaf end overflows usize"))?;
    let bytes = source
        .arena_bytes
        .get(start..end)
        .ok_or_else(|| format_error("source leaf lies outside committed payload"))?;
    let new_offset = u64::try_from(target.arena_bytes.len())
        .map_err(|_| format_error("compacted payload exceeds u64"))?;
    target.arena_bytes.extend_from_slice(bytes);
    let new_id = u64::try_from(target.compact_nodes.len() / COMPACT_NODE_SIZE)
        .map_err(|_| format_error("compacted node count exceeds u64"))?;
    let mut slot = [0u8; COMPACT_NODE_SIZE];
    slot[..8].copy_from_slice(&new_offset.to_le_bytes());
    slot[8..12].copy_from_slice(
        &u32::try_from(length).map_err(|_| format_error("compacted leaf length exceeds u32"))?.to_le_bytes(),
    );
    slot[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
    target.compact_nodes.extend_from_slice(&slot);
    Ok(new_id)
}

fn compact_node(
    source: &StoreState,
    target: &mut StoreState,
    node_map: &mut HashMap<u64, u64>,
    old_id: u64,
) -> Result<u64, CheckpointStoreError> {
    if let Some(mapped) = node_map.get(&old_id).copied() {
        return Ok(mapped);
    }
    let decoded = decode_node(source, old_id)?;
    let new_id = match decoded.kind {
        0 => append_compacted_leaf(source, target, decoded.a, decoded.b)?,
        1 => {
            let left = compact_node(source, target, node_map, decoded.a)?;
            let right = compact_node(source, target, node_map, decoded.b)?;
            let candidate = u64::try_from(target.compact_nodes.len() / COMPACT_NODE_SIZE)
                .map_err(|_| format_error("compacted node count exceeds u64"))?;
            let left_delta = candidate
                .checked_sub(left)
                .ok_or_else(|| format_error("compacted left child is not prior"))?;
            let right_delta = candidate
                .checked_sub(right)
                .ok_or_else(|| format_error("compacted right child is not prior"))?;
            let mut slot = [0u8; COMPACT_NODE_SIZE];
            if left_delta > 0
                && right_delta > 0
                && left_delta <= u64::from(u32::MAX)
                && right_delta <= u64::from(u32::MAX)
            {
                slot[..4].copy_from_slice(
                    &u32::try_from(left_delta).map_err(|_| format_error("left delta exceeds u32"))?.to_le_bytes(),
                );
                slot[4..8].copy_from_slice(
                    &u32::try_from(right_delta).map_err(|_| format_error("right delta exceeds u32"))?.to_le_bytes(),
                );
                slot[12..16].copy_from_slice(&KIND_BINARY.to_le_bytes());
                target.compact_nodes.extend_from_slice(&slot);
                candidate
            } else {
                let wide_index = u64::try_from(target.wide_nodes.len() / WIDE_RECORD_SIZE)
                    .map_err(|_| format_error("compacted wide-node count exceeds u64"))?;
                slot[..8].copy_from_slice(&wide_index.to_le_bytes());
                slot[12..16].copy_from_slice(&KIND_WIDE.to_le_bytes());
                target.compact_nodes.extend_from_slice(&slot);
                let mut wide = [0u8; WIDE_RECORD_SIZE];
                wide[..4].copy_from_slice(&WIDE_KIND_BINARY.to_le_bytes());
                wide[8..16].copy_from_slice(&left.to_le_bytes());
                wide[16..24].copy_from_slice(&right.to_le_bytes());
                target.wide_nodes.extend_from_slice(&wide);
                candidate
            }
        }
        _ => return Err(format_error("decoded compact node kind is invalid")),
    };
    node_map.insert(old_id, new_id);
    Ok(new_id)
}

fn compact_version(
    source: &StoreState,
    target: &mut StoreState,
    node_map: &mut HashMap<u64, u64>,
    version_map: &mut HashMap<u32, u32>,
    old_version: u32,
) -> Result<u32, CheckpointStoreError> {
    if let Some(mapped) = version_map.get(&old_version).copied() {
        return Ok(mapped);
    }
    let old_index = usize::try_from(old_version).map_err(|_| format_error("version id exceeds usize"))?;
    let old_parent = source
        .parents
        .get(old_index)
        .copied()
        .ok_or_else(|| format_error("version parent is outside source state"))?;
    let mapped_parent = old_parent.and_then(|parent| version_map.get(&parent).copied());
    let root = source
        .versions
        .get(old_index)
        .copied()
        .flatten()
        .ok_or_else(|| format_error("referenced version has no root"))?;
    let mapped_root = compact_node(source, target, node_map, root)?;
    let new_version = u32::try_from(target.versions.len())
        .map_err(|_| format_error("compacted version count exceeds u32"))?;
    target.versions.push(Some(mapped_root));
    target.parents.push(mapped_parent);
    version_map.insert(old_version, new_version);
    Ok(new_version)
}

fn compact_live_state(
    source: &StoreState,
    deleted_keys: &HashSet<(String, String)>,
) -> Result<CompactedState, CheckpointStoreError> {
    let mut target = StoreState::default();
    target.deleted_checkpoints = source.deleted_checkpoints.clone();
    target.retired_requests = source.retired_requests.clone();
    for key in deleted_keys {
        if source.deleted_checkpoints.contains(key) {
            return Err(format_error("prune set contains an already-deleted checkpoint"));
        }
        target.deleted_checkpoints.insert(key.clone());
    }

    let mut node_map = HashMap::new();
    let mut version_map = HashMap::new();
    let mut old_to_new_checkpoint = HashMap::new();
    for checkpoint in &source.checkpoints {
        let old_key = (checkpoint.thread_id.clone(), checkpoint.checkpoint_id.clone());
        if deleted_keys.contains(&old_key) {
            continue;
        }
        if let Some(parent) = checkpoint.parent_checkpoint_id.as_ref() {
            let parent_key = (checkpoint.thread_id.clone(), parent.clone());
            if deleted_keys.contains(&parent_key) {
                return Err(format_error("prune would leave a live child with a deleted parent"));
            }
        }
        let thread_ordinal = if let Some(ordinal) = target.thread_ordinals.get(&checkpoint.thread_id).copied() {
            ordinal
        } else {
            let ordinal = u32::try_from(target.threads.len())
                .map_err(|_| format_error("compacted thread count exceeds u32"))?;
            target.thread_ordinals.insert(checkpoint.thread_id.clone(), ordinal);
            target.threads.push(checkpoint.thread_id.clone());
            ordinal
        };
        let _ = thread_ordinal;
        let identity_version = compact_version(
            source,
            &mut target,
            &mut node_map,
            &mut version_map,
            checkpoint.identity_version,
        )?;
        let messages_version = checkpoint
            .messages_version
            .map(|version| compact_version(source, &mut target, &mut node_map, &mut version_map, version))
            .transpose()?;
        let result_version = checkpoint
            .result_version
            .map(|version| compact_version(source, &mut target, &mut node_map, &mut version_map, version))
            .transpose()?;
        let ordinal = u32::try_from(target.checkpoints.len())
            .map_err(|_| format_error("compacted checkpoint count exceeds u32"))?;
        let mut info = checkpoint.clone();
        info.ordinal = ordinal;
        info.identity_version = identity_version;
        info.messages_version = messages_version;
        info.result_version = result_version;
        if target
            .checkpoint_ordinals
            .insert((info.thread_id.clone(), info.checkpoint_id.clone()), ordinal)
            .is_some()
        {
            return Err(format_error("duplicate live checkpoint during prune"));
        }
        old_to_new_checkpoint.insert(checkpoint.ordinal, ordinal);
        target.checkpoints.push(info);
    }

    for record in source.request_records.values() {
        if let Some(ordinal) = old_to_new_checkpoint.get(&record.checkpoint_ordinal).copied() {
            target.request_records.insert(
                record.key.clone(),
                RequestRecord {
                    key: record.key.clone(),
                    operation_digest: record.operation_digest,
                    checkpoint_ordinal: ordinal,
                },
            );
        } else if let Some(existing) = target.retired_requests.get(&record.key) {
            if existing != &record.operation_digest {
                return Err(CheckpointStoreError::RequestIdConflict);
            }
        } else {
            target
                .retired_requests
                .insert(record.key.clone(), record.operation_digest);
        }
    }
    validate_materialized_state(&target)?;
    let mut deleted_checkpoints = target.deleted_checkpoints.iter().cloned().collect::<Vec<_>>();
    deleted_checkpoints.sort();
    let mut retired_requests = target
        .retired_requests
        .iter()
        .map(|(key, digest)| (key.clone(), *digest))
        .collect::<Vec<_>>();
    retired_requests.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(CompactedState {
        state: target,
        deleted_checkpoints,
        retired_requests,
    })
}

fn write_compacted_generation(
    dir: &Path,
    generation: u64,
    config: CheckpointStoreConfig,
    compacted: &CompactedState,
) -> Result<CompactedGeneration, CheckpointStoreError> {
    let state = &compacted.state;
    let zero_starts = vec![0u64; STREAM_NAMES.len()];
    let mut writer = StreamingSegmentWriter::new(
        dir,
        generation,
        zero_starts,
        0,
        0,
        config.sealed_block_size,
        config.zstd_level,
    )?;
    writer.push(PAYLOAD, &state.arena_bytes)?;
    for slot in state.compact_nodes.chunks_exact(COMPACT_NODE_SIZE) {
        let code = match read_u32(slot, 12)? {
            KIND_LEAF => 0u8,
            KIND_BINARY => 1u8,
            KIND_WIDE => 2u8,
            _ => return Err(format_error("invalid compact node kind during prune")),
        };
        writer.push(NODE_FIELD0, slot.get(..8).ok_or_else(|| format_error("prune node field0 missing"))?)?;
        writer.push(NODE_FIELD1, slot.get(8..12).ok_or_else(|| format_error("prune node field1 missing"))?)?;
        writer.push(NODE_KIND, &[code])?;
    }
    for record in state.wide_nodes.chunks_exact(WIDE_RECORD_SIZE) {
        let code = match read_u32(record, 0)? {
            WIDE_KIND_LEAF => 0u8,
            WIDE_KIND_BINARY => 1u8,
            _ => return Err(format_error("invalid wide node kind during prune")),
        };
        writer.push(WIDE_KIND, &[code])?;
        writer.push(WIDE_A, record.get(8..16).ok_or_else(|| format_error("prune wide field A missing"))?)?;
        writer.push(WIDE_B, record.get(16..24).ok_or_else(|| format_error("prune wide field B missing"))?)?;
        writer.push(WIDE_C, record.get(24..32).ok_or_else(|| format_error("prune wide field C missing"))?)?;
    }
    for (root, parent) in state.versions.iter().zip(&state.parents) {
        writer.push_u64(VERSION_ROOT, root.unwrap_or(NONE_ROOT))?;
        writer.push_u32(VERSION_PARENT, parent.unwrap_or(NONE_PARENT))?;
    }
    if !state.threads.is_empty() {
        writer.push_u32(THREAD_OFFSETS, 0)?;
        for thread in &state.threads {
            writer.push(THREAD_BYTES, thread.as_bytes())?;
            writer.push_u32(
                THREAD_OFFSETS,
                u32::try_from(writer.current_stream_len(THREAD_BYTES)?)
                    .map_err(|_| format_error("prune thread bytes exceed u32"))?,
            )?;
        }
    }
    if !state.checkpoints.is_empty() {
        writer.push_u32(CP_ID_OFFSETS, 0)?;
    }
    let mut route_checkpoints = Vec::with_capacity(state.checkpoints.len());
    for checkpoint in &state.checkpoints {
        let thread_ordinal = state
            .thread_ordinals
            .get(&checkpoint.thread_id)
            .copied()
            .ok_or_else(|| format_error("prune checkpoint thread is missing"))?;
        writer.push_u32(CP_THREAD, thread_ordinal)?;
        writer.push_u32(CP_NO, checkpoint.checkpoint_no)?;
        writer.push(CP_ID_BYTES, checkpoint.checkpoint_id.as_bytes())?;
        writer.push_u32(
            CP_ID_OFFSETS,
            u32::try_from(writer.current_stream_len(CP_ID_BYTES)?)
                .map_err(|_| format_error("prune checkpoint ids exceed u32"))?,
        )?;
        let parent_ordinal = checkpoint
            .parent_checkpoint_id
            .as_ref()
            .map(|parent| {
                state
                    .checkpoint_ordinals
                    .get(&(checkpoint.thread_id.clone(), parent.clone()))
                    .copied()
                    .ok_or_else(|| format_error("prune checkpoint parent is missing"))
            })
            .transpose()?
            .unwrap_or(NONE_PARENT);
        writer.push_u32(CP_PARENT_ORDINAL, parent_ordinal)?;
        writer.push_u32(CP_IDENTITY_VERSION, checkpoint.identity_version)?;
        writer.push_u32(CP_MESSAGES_VERSION, checkpoint.messages_version.unwrap_or(NONE_VERSION))?;
        writer.push_u32(CP_RESULT_VERSION, checkpoint.result_version.unwrap_or(NONE_VERSION))?;
        writer.push_u64(CP_LOGICAL_LEN, checkpoint.logical_state_len)?;
        writer.push_u64(CP_STATE_HASH, checkpoint.state_hash)?;
        route_checkpoints.push((
            checkpoint.thread_id.clone(),
            checkpoint.checkpoint_id.clone(),
            checkpoint.ordinal,
        ));
    }
    let finalized = writer.finalize(
        0,
        0,
        u64::try_from(state.checkpoints.len()).map_err(|_| format_error("prune checkpoint count overflow"))?,
        u64::try_from(state.versions.len()).map_err(|_| format_error("prune version count overflow"))?,
    )?;
    let mut route_threads = state
        .threads
        .iter()
        .enumerate()
        .map(|(ordinal, thread)| {
            Ok::<_, CheckpointStoreError>((
                thread.clone(),
                u32::try_from(ordinal).map_err(|_| format_error("prune thread ordinal overflow"))?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    route_threads.sort_by(|left, right| left.0.cmp(&right.0));
    let mut requests = state.request_records.values().cloned().collect::<Vec<_>>();
    requests.sort_by(|left, right| left.key.cmp(&right.key));
    let (route_bytes, route_hash) = build_route_file(generation, &route_threads, &route_checkpoints, &requests)?;
    let route_name = format!("route-g{generation:06}.t3r");
    let route_meta = RouteMeta {
        generation,
        file: route_name,
        thread_entry_count: u64::try_from(route_threads.len())
            .map_err(|_| format_error("prune route thread count overflow"))?,
        checkpoint_entry_count: u64::try_from(route_checkpoints.len())
            .map_err(|_| format_error("prune route checkpoint count overflow"))?,
        route_file_bytes: u64::try_from(route_bytes.len())
            .map_err(|_| format_error("prune route file length overflow"))?,
        route_index_xxh3_64: route_hash,
    };
    Ok(CompactedGeneration {
        finalized,
        route_bytes,
        route_meta,
    })
}

fn write_stream_entry_from_bytes(out: &mut Vec<u8>, entry: &StreamEntry) {
    write_stream_entry(out, entry);
}

fn build_route_file(
    generation: u64,
    new_threads: &[(String, u32)],
    new_checkpoints: &[(String, String, u32)],
    new_requests: &[RequestRecord],
) -> Result<(Vec<u8>, u64), CheckpointStoreError> {
    #[derive(Clone)]
    struct RouteEntry {
        hash: u64,
        ordinal: u32,
        key: Vec<u8>,
    }
    fn route_key(thread: &str, checkpoint: Option<&str>) -> Result<Vec<u8>, CheckpointStoreError> {
        let thread_len = u32::try_from(thread.len()).map_err(|_| format_error("route thread length overflow"))?;
        let checkpoint_len = checkpoint
            .map(|value| u32::try_from(value.len()).map_err(|_| format_error("route checkpoint length overflow")))
            .transpose()?
            .unwrap_or(0);
        let mut key = Vec::new();
        key.push(if checkpoint.is_some() { 1 } else { 0 });
        key.extend_from_slice(&thread_len.to_le_bytes());
        key.extend_from_slice(thread.as_bytes());
        key.extend_from_slice(&checkpoint_len.to_le_bytes());
        if let Some(checkpoint) = checkpoint {
            key.extend_from_slice(checkpoint.as_bytes());
        }
        Ok(key)
    }
    let mut threads = Vec::new();
    for (thread, ordinal) in new_threads {
        let key = route_key(thread, None)?;
        threads.push(RouteEntry {
            hash: xxh3_64(&key),
            ordinal: *ordinal,
            key,
        });
    }
    let mut checkpoints = Vec::new();
    for (thread, checkpoint, ordinal) in new_checkpoints {
        let key = route_key(thread, Some(checkpoint))?;
        checkpoints.push(RouteEntry {
            hash: xxh3_64(&key),
            ordinal: *ordinal,
            key,
        });
    }
    threads.sort_by(|left, right| left.hash.cmp(&right.hash).then_with(|| left.key.cmp(&right.key)));
    checkpoints.sort_by(|left, right| left.hash.cmp(&right.hash).then_with(|| left.key.cmp(&right.key)));
    let thread_table_offset = u64::try_from(ROUTE_HEADER_SIZE).map_err(|_| format_error("route header size overflow"))?;
    let thread_table_bytes = threads
        .len()
        .checked_mul(ROUTE_ENTRY_SIZE)
        .ok_or_else(|| format_error("route thread table overflow"))?;
    let checkpoint_table_bytes = checkpoints
        .len()
        .checked_mul(ROUTE_ENTRY_SIZE)
        .ok_or_else(|| format_error("route checkpoint table overflow"))?;
    let checkpoint_table_offset = thread_table_offset
        .checked_add(u64::try_from(thread_table_bytes).map_err(|_| format_error("route thread table size overflow"))?)
        .ok_or_else(|| format_error("route checkpoint offset overflow"))?;
    let key_blob_offset = checkpoint_table_offset
        .checked_add(u64::try_from(checkpoint_table_bytes).map_err(|_| format_error("route checkpoint table size overflow"))?)
        .ok_or_else(|| format_error("route key blob offset overflow"))?;
    let mut table = Vec::new();
    let mut blob = Vec::new();
    for entry in threads.iter().chain(checkpoints.iter()) {
        let key_offset = u32::try_from(blob.len()).map_err(|_| format_error("route key offset overflow"))?;
        let key_len = u32::try_from(entry.key.len()).map_err(|_| format_error("route key length overflow"))?;
        blob.extend_from_slice(&entry.key);
        table.extend_from_slice(&entry.hash.to_le_bytes());
        table.extend_from_slice(&entry.ordinal.to_le_bytes());
        table.extend_from_slice(&key_offset.to_le_bytes());
        table.extend_from_slice(&key_len.to_le_bytes());
        table.extend_from_slice(&0u32.to_le_bytes());
    }
    let mut tail = table;
    tail.extend_from_slice(&blob);
    tail.extend_from_slice(&request_section_bytes(new_requests)?);
    let index_hash = if tail.is_empty() { 0 } else { xxh3_64(&tail) };
    let mut out = Vec::new();
    out.extend_from_slice(ROUTE_MAGIC);
    out.extend_from_slice(&ROUTE_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&generation.to_le_bytes());
    out.extend_from_slice(&u32::try_from(threads.len()).map_err(|_| format_error("route thread count overflow"))?.to_le_bytes());
    out.extend_from_slice(&u32::try_from(checkpoints.len()).map_err(|_| format_error("route checkpoint count overflow"))?.to_le_bytes());
    out.extend_from_slice(&thread_table_offset.to_le_bytes());
    out.extend_from_slice(&checkpoint_table_offset.to_le_bytes());
    out.extend_from_slice(&key_blob_offset.to_le_bytes());
    out.extend_from_slice(&index_hash.to_le_bytes());
    if out.len() != ROUTE_HEADER_SIZE {
        return Err(format_error("route header width mismatch"));
    }
    out.extend_from_slice(&tail);
    Ok((out, index_hash))
}

fn request_section_bytes(records: &[RequestRecord]) -> Result<Vec<u8>, CheckpointStoreError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let mut section = Vec::new();
    section.extend_from_slice(REQUEST_SECTION_MAGIC);
    section.extend_from_slice(&REQUEST_SECTION_VERSION.to_le_bytes());
    section.extend_from_slice(&0u32.to_le_bytes());
    section.extend_from_slice(
        &u32::try_from(records.len())
            .map_err(|_| format_error("request section record count exceeds u32"))?
            .to_le_bytes(),
    );
    for record in records {
        validate_request_id(&record.key)?;
        section.extend_from_slice(
            &u32::try_from(record.key.len())
                .map_err(|_| format_error("request key length exceeds u32"))?
                .to_le_bytes(),
        );
        section.extend_from_slice(&record.checkpoint_ordinal.to_le_bytes());
        section.extend_from_slice(&record.operation_digest);
        section.extend_from_slice(&record.key);
    }
    let section_bytes = u64::try_from(section.len())
        .map_err(|_| format_error("request section length exceeds u64"))?;
    section.extend_from_slice(&section_bytes.to_le_bytes());
    section.extend_from_slice(REQUEST_FOOTER_MAGIC);
    Ok(section)
}

fn route_request_records(path: &Path) -> Result<Vec<RequestRecord>, CheckpointStoreError> {
    let bytes = fs::read(path)?;
    if bytes.len() < ROUTE_HEADER_SIZE {
        return Err(format_error("route file is shorter than its header"));
    }
    let thread_count = usize::try_from(read_u32(&bytes, 24)?)
        .map_err(|_| format_error("route thread count overflow"))?;
    let checkpoint_count = usize::try_from(read_u32(&bytes, 28)?)
        .map_err(|_| format_error("route checkpoint count overflow"))?;
    let thread_table_offset = usize::try_from(read_u64(&bytes, 32)?)
        .map_err(|_| format_error("route thread table offset overflow"))?;
    let checkpoint_table_offset = usize::try_from(read_u64(&bytes, 40)?)
        .map_err(|_| format_error("route checkpoint table offset overflow"))?;
    let key_blob_offset = usize::try_from(read_u64(&bytes, 48)?)
        .map_err(|_| format_error("route key blob offset overflow"))?;
    let entry_bytes = ROUTE_ENTRY_SIZE;
    let mut key_blob_len = 0usize;
    for (table_offset, count) in [(thread_table_offset, thread_count), (checkpoint_table_offset, checkpoint_count)] {
        for index in 0..count {
            let base = table_offset
                .checked_add(index.checked_mul(entry_bytes).ok_or_else(|| format_error("route entry offset overflow"))?)
                .ok_or_else(|| format_error("route entry offset overflow"))?;
            let key_offset = usize::try_from(read_u32(&bytes, base + 12)?)
                .map_err(|_| format_error("route key offset overflow"))?;
            let key_len = usize::try_from(read_u32(&bytes, base + 16)?)
                .map_err(|_| format_error("route key length overflow"))?;
            key_blob_len = key_blob_len.max(
                key_offset
                    .checked_add(key_len)
                    .ok_or_else(|| format_error("route key end overflow"))?,
            );
        }
    }
    let key_blob_end = key_blob_offset
        .checked_add(key_blob_len)
        .ok_or_else(|| format_error("route key blob end overflow"))?;
    if key_blob_end > bytes.len() {
        return Err(format_error("route key blob exceeds file"));
    }
    if bytes.len() < REQUEST_SECTION_FOOTER_BYTES {
        return Ok(Vec::new());
    }
    let footer_start = bytes.len() - REQUEST_SECTION_FOOTER_BYTES;
    if bytes.get(footer_start + 8..footer_start + 16) != Some(REQUEST_FOOTER_MAGIC.as_slice()) {
        return Ok(Vec::new());
    }
    let section_bytes = usize::try_from(read_u64(&bytes, footer_start)?)
        .map_err(|_| format_error("request section length overflow"))?;
    let section_start = footer_start
        .checked_sub(section_bytes)
        .ok_or_else(|| format_error("request section starts before file"))?;
    if section_start != key_blob_end {
        return Err(format_error("request section is not contiguous with route keys"));
    }
    if section_bytes < REQUEST_SECTION_HEADER_BYTES
        || read_u64(&bytes, footer_start)?
            != u64::try_from(footer_start - section_start)
                .map_err(|_| format_error("request section size overflow"))?
        || bytes.get(section_start..section_start + 8) != Some(REQUEST_SECTION_MAGIC.as_slice())
        || read_u32(&bytes, section_start + 8)? != REQUEST_SECTION_VERSION
        || read_u32(&bytes, section_start + 12)? != 0
    {
        return Err(format_error("request section header is invalid"));
    }
    let record_count = usize::try_from(read_u32(&bytes, section_start + 16)?)
        .map_err(|_| format_error("request section record count overflow"))?;
    let section_end = section_start
        .checked_add(section_bytes)
        .ok_or_else(|| format_error("request section end overflow"))?;
    let mut cursor = section_start + REQUEST_SECTION_HEADER_BYTES;
    let mut records = Vec::new();
    for _ in 0..record_count {
        let key_len = usize::try_from(read_u32(&bytes, cursor)?)
            .map_err(|_| format_error("request key length overflow"))?;
        let checkpoint_ordinal = read_u32(&bytes, cursor + 4)?;
        let digest_start = cursor
            .checked_add(8)
            .ok_or_else(|| format_error("request digest offset overflow"))?;
        let digest_end = digest_start
            .checked_add(32)
            .ok_or_else(|| format_error("request digest end overflow"))?;
        let key_start = digest_end;
        let key_end = key_start
            .checked_add(key_len)
            .ok_or_else(|| format_error("request key end overflow"))?;
        if key_end > section_end {
            return Err(format_error("request record exceeds section"));
        }
        let key = bytes
            .get(key_start..key_end)
            .ok_or_else(|| format_error("request key outside section"))?
            .to_vec();
        validate_request_id(&key)?;
        let operation_digest: [u8; 32] = bytes
            .get(digest_start..digest_end)
            .ok_or_else(|| format_error("request digest outside section"))?
            .try_into()
            .map_err(|_| format_error("request digest width mismatch"))?;
        records.push(RequestRecord {
            key,
            operation_digest,
            checkpoint_ordinal,
        });
        cursor = key_end;
    }
    if cursor != section_end {
        return Err(format_error("request section has trailing record bytes"));
    }
    Ok(records)
}

fn segment_to_value(meta: &SegmentMeta) -> Value {
    json!({
        "generation": meta.generation,
        "file": meta.file,
        "wal_start_bytes": meta.wal_start_bytes,
        "wal_end_bytes": meta.wal_end_bytes,
        "checkpoint_start_count": meta.checkpoint_start_count,
        "checkpoint_end_count": meta.checkpoint_end_count,
        "version_start_count": meta.version_start_count,
        "version_end_count": meta.version_end_count,
        "raw_stream_bytes": meta.raw_stream_bytes,
        "stream_starts": meta.stream_starts,
        "stream_ends": meta.stream_ends,
        "segment_file_bytes": meta.segment_file_bytes,
        "segment_file_xxh3_64": meta.segment_file_xxh3_64,
        "index_bytes": meta.index_bytes,
        "index_xxh3_64": meta.index_xxh3_64,
        "block_size": meta.block_size,
        "block_count": meta.block_count,
        "codec": "zstd",
        "zstd_level": meta.zstd_level
    })
}

fn route_to_value(meta: &RouteMeta) -> Value {
    json!({
        "generation": meta.generation,
        "file": meta.file,
        "thread_entry_count": meta.thread_entry_count,
        "checkpoint_entry_count": meta.checkpoint_entry_count,
        "route_file_bytes": meta.route_file_bytes,
        "route_index_xxh3_64": meta.route_index_xxh3_64
    })
}

fn tombstone_to_value(tombstone: &CheckpointTombstone) -> Value {
    json!({
        "thread_id": tombstone.thread_id,
        "checkpoint_id": tombstone.checkpoint_id,
    })
}

fn tombstone_from_value(value: &Value) -> Result<CheckpointTombstone, CheckpointStoreError> {
    let thread_id = value
        .get("thread_id")
        .and_then(Value::as_str)
        .ok_or_else(|| format_error("checkpoint tombstone thread id is missing"))?
        .to_owned();
    let checkpoint_id = value
        .get("checkpoint_id")
        .and_then(Value::as_str)
        .ok_or_else(|| format_error("checkpoint tombstone id is missing"))?
        .to_owned();
    validate_checkpoint_identifier(&thread_id, "thread id")?;
    validate_checkpoint_identifier(&checkpoint_id, "checkpoint id")?;
    Ok(CheckpointTombstone {
        thread_id,
        checkpoint_id,
    })
}

fn byte_array_from_value(value: &Value, key: &str) -> Result<Vec<u8>, CheckpointStoreError> {
    value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format_error(format!("retired request {key} is missing")))?
        .iter()
        .map(|item| {
            let value = item
                .as_u64()
                .ok_or_else(|| format_error(format!("retired request {key} contains non-byte")))?;
            u8::try_from(value).map_err(|_| format_error(format!("retired request {key} byte overflows u8")))
        })
        .collect()
}

fn retired_request_to_value(record: &RetiredRequestRecord) -> Value {
    json!({
        "key": record.key,
        "operation_digest": record.operation_digest,
    })
}

fn retired_request_from_value(value: &Value) -> Result<RetiredRequestRecord, CheckpointStoreError> {
    let key = byte_array_from_value(value, "key")?;
    validate_request_id(&key)?;
    let digest = byte_array_from_value(value, "operation_digest")?;
    let operation_digest: [u8; 32] = digest
        .try_into()
        .map_err(|_| format_error("retired request digest must contain 32 bytes"))?;
    Ok(RetiredRequestRecord {
        key,
        operation_digest,
    })
}

fn manifest_bytes(manifest: &Manifest) -> Result<Vec<u8>, CheckpointStoreError> {
    let format_version = if !manifest.deleted_checkpoints.is_empty() || !manifest.retired_requests.is_empty() {
        MANIFEST_FORMAT_VERSION_SUBTREE_PRUNE
    } else if manifest.store_id.is_some() {
        MANIFEST_FORMAT_VERSION_STORE_ID
    } else {
        MANIFEST_FORMAT_VERSION_LEGACY
    };
    let mut value = json!({
        "format": MANIFEST_FORMAT,
        "format_version": format_version,
        "generation": manifest.generation,
        "sealed_end_wal_bytes": manifest.sealed_end_wal_bytes,
        "checkpoint_count": manifest.checkpoint_count,
        "version_count": manifest.version_count,
        "stream_names": STREAM_NAMES,
        "stream_sizes": manifest.stream_sizes,
        "stream_prefix_xxh3_64": vec![0u64; STREAM_NAMES.len()],
        "stream_prefix_hashes_authoritative": false,
        "segments": manifest.segments.iter().map(segment_to_value).collect::<Vec<_>>(),
        "thread_count": manifest.thread_count,
        "routes": manifest.routes.iter().map(route_to_value).collect::<Vec<_>>(),
        "deleted_checkpoints": manifest.deleted_checkpoints.iter().map(tombstone_to_value).collect::<Vec<_>>(),
        "retired_requests": manifest.retired_requests.iter().map(retired_request_to_value).collect::<Vec<_>>(),
        "payload": "incremental suffixes of canonical 22-stream R3 materialization",
        "codec": "zstd",
        "format_variable_geometry_capable": true,
        "integrated_streaming_lifecycle_r3": true,
        "segment_file_hash_authoritative": false,
        "routing": "immutable segment-local route indexes",
        "performance_claim": false,
        "power_loss_claim": false,
        "process_crash_claim": false
    });
    if let Some(store_id) = manifest.store_id {
        value["store_id"] = Value::String(store_id.to_hex());
    }
    let mut bytes = serde_json::to_vec_pretty(&value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn ensure_new_store_manifest(
    dir: &Path,
    mut manifest: Manifest,
) -> Result<Manifest, CheckpointStoreError> {
    if dir.join(MANIFEST_FILE).exists() {
        return Ok(manifest);
    }
    manifest.store_id = Some(StoreId::generate()?);
    staged_write_new(&dir.join(MANIFEST_FILE), &manifest_bytes(&manifest)?)?;
    Ok(manifest)
}

fn required_u64(value: &Value, key: &str) -> Result<u64, CheckpointStoreError> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format_error(format!("manifest missing/invalid {key}")))
}

fn required_u64_array(value: &Value, key: &str) -> Result<Vec<u64>, CheckpointStoreError> {
    value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| format_error(format!("manifest missing/invalid {key}")))?
        .iter()
        .map(|item| item.as_u64().ok_or_else(|| format_error(format!("non-u64 in manifest {key}"))))
        .collect()
}

fn segment_from_value(value: &Value) -> Result<SegmentMeta, CheckpointStoreError> {
    Ok(SegmentMeta {
        generation: required_u64(value, "generation")?,
        file: value.get("file").and_then(Value::as_str).ok_or_else(|| format_error("segment missing file"))?.to_owned(),
        wal_start_bytes: required_u64(value, "wal_start_bytes")?,
        wal_end_bytes: required_u64(value, "wal_end_bytes")?,
        checkpoint_start_count: required_u64(value, "checkpoint_start_count")?,
        checkpoint_end_count: required_u64(value, "checkpoint_end_count")?,
        version_start_count: required_u64(value, "version_start_count")?,
        version_end_count: required_u64(value, "version_end_count")?,
        raw_stream_bytes: required_u64(value, "raw_stream_bytes")?,
        stream_starts: required_u64_array(value, "stream_starts")?,
        stream_ends: required_u64_array(value, "stream_ends")?,
        segment_file_bytes: required_u64(value, "segment_file_bytes")?,
        segment_file_xxh3_64: required_u64(value, "segment_file_xxh3_64")?,
        index_bytes: required_u64(value, "index_bytes")?,
        index_xxh3_64: required_u64(value, "index_xxh3_64")?,
        block_size: u32::try_from(required_u64(value, "block_size")?).map_err(|_| format_error("segment block size overflow"))?,
        block_count: u32::try_from(required_u64(value, "block_count")?).map_err(|_| format_error("segment block count overflow"))?,
        zstd_level: i32::try_from(value.get("zstd_level").and_then(Value::as_i64).ok_or_else(|| format_error("segment missing zstd level"))?)
            .map_err(|_| format_error("segment zstd level overflow"))?,
    })
}

fn route_from_value(value: &Value) -> Result<RouteMeta, CheckpointStoreError> {
    Ok(RouteMeta {
        generation: required_u64(value, "generation")?,
        file: value.get("file").and_then(Value::as_str).ok_or_else(|| format_error("route missing file"))?.to_owned(),
        thread_entry_count: required_u64(value, "thread_entry_count")?,
        checkpoint_entry_count: required_u64(value, "checkpoint_entry_count")?,
        route_file_bytes: required_u64(value, "route_file_bytes")?,
        route_index_xxh3_64: required_u64(value, "route_index_xxh3_64")?,
    })
}

fn load_manifest(dir: &Path) -> Result<Manifest, CheckpointStoreError> {
    let path = dir.join(MANIFEST_FILE);
    if !path.exists() {
        return Ok(Manifest::default());
    }
    let value: Value = serde_json::from_slice(&fs::read(&path)?)?;
    if value.get("format").and_then(Value::as_str) != Some(MANIFEST_FORMAT) {
        return Err(format_error("structured manifest format mismatch"));
    }
    let format_version = value
        .get("format_version")
        .and_then(Value::as_u64)
        .unwrap_or(MANIFEST_FORMAT_VERSION_LEGACY);
    let store_id = match format_version {
        MANIFEST_FORMAT_VERSION_LEGACY => {
            if value
                .get("store_id")
                .is_some_and(|value| !value.is_null())
            {
                return Err(format_error(
                    "legacy manifest contains StoreId without the StoreId revision",
                ));
            }
            None
        }
        MANIFEST_FORMAT_VERSION_STORE_ID => {
            let encoded = value
                .get("store_id")
                .and_then(Value::as_str)
                .ok_or_else(|| format_error("StoreId manifest field is missing"))?;
            Some(StoreId::from_hex(encoded)?)
        }
        MANIFEST_FORMAT_VERSION_SUBTREE_PRUNE => {
            let encoded = value
                .get("store_id")
                .and_then(Value::as_str)
                .ok_or_else(|| format_error("StoreId manifest field is missing"))?;
            Some(StoreId::from_hex(encoded)?)
        }
        _ => return Err(format_error("unsupported structured manifest revision")),
    };
    let segments = value
        .get("segments")
        .and_then(Value::as_array)
        .ok_or_else(|| format_error("manifest missing segments"))?
        .iter()
        .map(segment_from_value)
        .collect::<Result<Vec<_>, _>>()?;
    let routes = value
        .get("routes")
        .and_then(Value::as_array)
        .ok_or_else(|| format_error("manifest missing routes"))?
        .iter()
        .map(route_from_value)
        .collect::<Result<Vec<_>, _>>()?;
    let deleted_checkpoints = if format_version >= MANIFEST_FORMAT_VERSION_SUBTREE_PRUNE {
        value
            .get("deleted_checkpoints")
            .and_then(Value::as_array)
            .ok_or_else(|| format_error("manifest missing deleted checkpoints"))?
            .iter()
            .map(tombstone_from_value)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let retired_requests = if format_version >= MANIFEST_FORMAT_VERSION_SUBTREE_PRUNE {
        value
            .get("retired_requests")
            .and_then(Value::as_array)
            .ok_or_else(|| format_error("manifest missing retired requests"))?
            .iter()
            .map(retired_request_from_value)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let manifest = Manifest {
        generation: required_u64(&value, "generation")?,
        sealed_end_wal_bytes: required_u64(&value, "sealed_end_wal_bytes")?,
        checkpoint_count: required_u64(&value, "checkpoint_count")?,
        version_count: required_u64(&value, "version_count")?,
        thread_count: value.get("thread_count").and_then(Value::as_u64).unwrap_or(0),
        stream_sizes: required_u64_array(&value, "stream_sizes")?,
        segments,
        routes,
        store_id,
        deleted_checkpoints,
        retired_requests,
    };
    validate_manifest(dir, &manifest)?;
    Ok(manifest)
}

fn validate_route_file(
    dir: &Path,
    route: &RouteMeta,
) -> Result<(), CheckpointStoreError> {
    let bytes = fs::read(dir.join(&route.file))?;
    if bytes.len() < ROUTE_HEADER_SIZE {
        return Err(format_error("route header truncated"));
    }
    if bytes.get(..ROUTE_MAGIC.len()) != Some(ROUTE_MAGIC.as_slice()) {
        return Err(format_error("route magic mismatch"));
    }
    if read_u32(&bytes, 8)? != ROUTE_FORMAT_VERSION {
        return Err(format_error("route format version mismatch"));
    }
    if read_u32(&bytes, 12)? != 0 {
        return Err(format_error("route reserved header field is nonzero"));
    }
    if read_u64(&bytes, 16)? != route.generation {
        return Err(format_error("route generation mismatch"));
    }
    if u64::from(read_u32(&bytes, 24)?) != route.thread_entry_count
        || u64::from(read_u32(&bytes, 28)?) != route.checkpoint_entry_count
    {
        return Err(format_error("route entry counts mismatch manifest"));
    }

    let thread_table_offset = read_u64(&bytes, 32)?;
    let checkpoint_table_offset = read_u64(&bytes, 40)?;
    let key_blob_offset = read_u64(&bytes, 48)?;
    let stored_index_hash = read_u64(&bytes, 56)?;
    let header_bytes = u64::try_from(ROUTE_HEADER_SIZE)
        .map_err(|_| format_error("route header size overflow"))?;
    let entry_bytes = u64::try_from(ROUTE_ENTRY_SIZE)
        .map_err(|_| format_error("route entry size overflow"))?;
    let expected_checkpoint_table_offset = header_bytes
        .checked_add(
            route
                .thread_entry_count
                .checked_mul(entry_bytes)
                .ok_or_else(|| format_error("route thread table size overflow"))?,
        )
        .ok_or_else(|| format_error("route checkpoint table offset overflow"))?;
    let expected_key_blob_offset = expected_checkpoint_table_offset
        .checked_add(
            route
                .checkpoint_entry_count
                .checked_mul(entry_bytes)
                .ok_or_else(|| format_error("route checkpoint table size overflow"))?,
        )
        .ok_or_else(|| format_error("route key blob offset overflow"))?;
    let file_len = u64::try_from(bytes.len())
        .map_err(|_| format_error("route file length overflow"))?;

    if thread_table_offset != header_bytes
        || checkpoint_table_offset != expected_checkpoint_table_offset
        || key_blob_offset != expected_key_blob_offset
        || key_blob_offset > file_len
    {
        return Err(format_error("route table offsets mismatch deterministic layout"));
    }

    let tail = bytes
        .get(ROUTE_HEADER_SIZE..)
        .ok_or_else(|| format_error("route tail outside file"))?;
    let actual_index_hash = if tail.is_empty() { 0 } else { xxh3_64(tail) };
    if stored_index_hash != route.route_index_xxh3_64
        || actual_index_hash != stored_index_hash
    {
        return Err(format_error("route index checksum mismatch"));
    }
    Ok(())
}

fn validate_manifest(dir: &Path, manifest: &Manifest) -> Result<(), CheckpointStoreError> {
    if manifest.stream_sizes.len() != STREAM_NAMES.len() || manifest.segments.len() != manifest.routes.len() {
        return Err(format_error("manifest stream or segment/route width mismatch"));
    }
    let mut expected_streams = vec![0u64; STREAM_NAMES.len()];
    let mut wal = 0u64;
    let mut checkpoints = 0u64;
    let mut versions = 0u64;
    let mut generation = 0u64;
    for (segment, route) in manifest.segments.iter().zip(manifest.routes.iter()) {
        if segment.generation <= generation || segment.generation != route.generation {
            return Err(format_error("manifest generation ordering mismatch"));
        }
        if segment.wal_start_bytes != wal
            || segment.checkpoint_start_count != checkpoints
            || segment.version_start_count != versions
            || segment.stream_starts != expected_streams
            || segment.stream_ends.len() != STREAM_NAMES.len()
        {
            return Err(format_error("manifest authority ranges are not contiguous"));
        }
        if fs::metadata(dir.join(&segment.file))?.len() != segment.segment_file_bytes
            || fs::metadata(dir.join(&route.file))?.len() != route.route_file_bytes
        {
            return Err(format_error("manifest referenced file size mismatch"));
        }
        validate_route_file(dir, route)?;
        expected_streams = segment.stream_ends.clone();
        wal = segment.wal_end_bytes;
        checkpoints = segment.checkpoint_end_count;
        versions = segment.version_end_count;
        generation = segment.generation;
    }
    let route_checkpoints = manifest.routes.iter().try_fold(0u64, |acc, route| {
        acc.checked_add(route.checkpoint_entry_count).ok_or_else(|| format_error("route checkpoint total overflow"))
    })?;
    let route_threads = manifest.routes.iter().try_fold(0u64, |acc, route| {
        acc.checked_add(route.thread_entry_count).ok_or_else(|| format_error("route thread total overflow"))
    })?;
    if manifest.generation != generation
        || manifest.sealed_end_wal_bytes != wal
        || manifest.checkpoint_count != checkpoints
        || manifest.version_count != versions
        || manifest.stream_sizes != expected_streams
        || manifest.checkpoint_count != route_checkpoints
        || manifest.thread_count != route_threads
    {
        return Err(format_error("manifest top-level authority mismatch"));
    }
    Ok(())
}

fn tmp_path_for(path: &Path) -> Result<PathBuf, CheckpointStoreError> {
    let parent = path.parent().ok_or_else(|| format_error("target path has no parent"))?;
    let name = path.file_name().and_then(|value| value.to_str()).ok_or_else(|| format_error("target filename is not UTF-8"))?;
    Ok(parent.join(format!(".{name}.tmp")))
}

fn publish_existing_tmp(tmp: &Path, final_path: &Path) -> Result<(), CheckpointStoreError> {
    if !tmp.exists() {
        return Err(format_error("staged temporary file is missing"));
    }
    OpenOptions::new().read(true).write(true).open(tmp)?.sync_all()?;
    fs::rename(tmp, final_path)?;
    sync_dir(final_path.parent().ok_or_else(|| format_error("final path has no parent"))?)?;
    Ok(())
}

fn staged_write_new(path: &Path, bytes: &[u8]) -> Result<(), CheckpointStoreError> {
    let parent = path.parent().ok_or_else(|| format_error("target path has no parent"))?;
    fs::create_dir_all(parent)?;
    let tmp = tmp_path_for(path)?;
    if tmp.exists() {
        fs::remove_file(&tmp)?;
    }
    let mut file = File::create(&tmp)?;
    file.write_all(bytes)?;
    file.flush()?;
    drop(file);
    publish_existing_tmp(&tmp, path)
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
    if source_offset > logical_tail {
        return Err(format_error("WAL recycle source offset exceeds logical tail"));
    }
    let tmp = tmp_path_for(hot_path)?;
    if tmp.exists() {
        fs::remove_file(&tmp)?;
    }
    let mut source = File::open(hot_path)?;
    source.seek(SeekFrom::Start(source_offset))?;
    let suffix_len = logical_tail - source_offset;
    let mut output = OpenOptions::new().create(true).read(true).write(true).truncate(true).open(&tmp)?;
    let mut limited = source.take(suffix_len);
    let copied = io::copy(&mut limited, &mut output)?;
    if copied != suffix_len {
        return Err(format_error("WAL recycle copied an unexpected suffix length"));
    }
    let capacity = round_capacity(suffix_len, config.wal_segment_bytes)?;
    preinitialize_range(&mut output, suffix_len, capacity, config.preinit_chunk_bytes)?;
    let peak = tree_storage(dir)?;
    drop(output);
    fs::rename(&tmp, hot_path)?;
    sync_dir(hot_path.parent().ok_or_else(|| format_error("hot WAL path has no parent"))?)?;
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
            result.allocated_bytes = result.allocated_bytes.saturating_add(metadata.blocks().saturating_mul(512));
        }
        #[cfg(not(unix))]
        {
            result.allocated_bytes = result.allocated_bytes.saturating_add(metadata.len());
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn nested_process_test_guard() -> MutexGuard<'static, ()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("nested checkpoint-store process test guard was poisoned")
    }

    fn root_record(id: u32, root: u64, parent: Option<u32>) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&ROOT_MAGIC);
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&root.to_le_bytes());
        out.extend_from_slice(&parent.unwrap_or(NONE_PARENT).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        let hash = xxh3_64(&out);
        out.extend_from_slice(&hash.to_le_bytes());
        out
    }

    fn checkpoint_record(
        checkpoint_no: u32,
        identity_version: u32,
        thread: &str,
        checkpoint_id: &str,
        parent: Option<&str>,
        canonical: &[u8],
    ) -> Vec<u8> {
        let parent = parent.unwrap_or("");
        let total = CHECKPOINT_PREFIX_SIZE + thread.len() + checkpoint_id.len() + parent.len() + 8;
        let mut out = Vec::new();
        out.extend_from_slice(&CHECKPOINT_MAGIC);
        out.extend_from_slice(&u32::try_from(total).unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&checkpoint_no.to_le_bytes());
        out.extend_from_slice(&identity_version.to_le_bytes());
        out.extend_from_slice(&NONE_VERSION.to_le_bytes());
        out.extend_from_slice(&NONE_VERSION.to_le_bytes());
        out.extend_from_slice(&u32::try_from(thread.len()).unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&u32::try_from(checkpoint_id.len()).unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&u32::try_from(parent.len()).unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&u64::try_from(canonical.len()).unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&xxh3_64(canonical).to_le_bytes());
        out.extend_from_slice(thread.as_bytes());
        out.extend_from_slice(checkpoint_id.as_bytes());
        out.extend_from_slice(parent.as_bytes());
        let hash = xxh3_64(&out);
        out.extend_from_slice(&hash.to_le_bytes());
        out
    }

    fn transaction(
        version_start: u32,
        checkpoint_count: u32,
        byte_start: u64,
        node_start: u64,
        identity: &[u8],
        checkpoint_id: &str,
        parent: Option<&str>,
    ) -> Vec<u8> {
        transaction_for_thread(
            version_start,
            checkpoint_count,
            byte_start,
            node_start,
            identity,
            "thread",
            checkpoint_id,
            parent,
        )
    }

    fn transaction_for_thread(
        version_start: u32,
        checkpoint_count: u32,
        byte_start: u64,
        node_start: u64,
        identity: &[u8],
        thread: &str,
        checkpoint_id: &str,
        parent: Option<&str>,
    ) -> Vec<u8> {
        let root_id = node_start;
        let mut slot = [0u8; COMPACT_NODE_SIZE];
        slot[..8].copy_from_slice(&byte_start.to_le_bytes());
        slot[8..12].copy_from_slice(&u32::try_from(identity.len()).unwrap_or(0).to_le_bytes());
        slot[12..16].copy_from_slice(&KIND_LEAF.to_le_bytes());
        let mut canonical = Vec::new();
        canonical.extend_from_slice(b"{\"identity\":");
        canonical.extend_from_slice(identity);
        canonical.extend_from_slice(b",\"messages\":[]}");
        let cp = checkpoint_record(
            checkpoint_count - 1,
            version_start,
            thread,
            checkpoint_id,
            parent,
            &canonical,
        );
        let root = root_record(version_start, root_id, version_start.checked_sub(1));
        let total = TX_HEADER_SIZE + identity.len() + slot.len() + root.len() + cp.len() + TX_CHECKSUM_SIZE;
        let mut out = Vec::new();
        out.extend_from_slice(&TX_MAGIC);
        out.extend_from_slice(&u32::try_from(total).unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&version_start.to_le_bytes());
        out.extend_from_slice(&(version_start + 1).to_le_bytes());
        out.extend_from_slice(&checkpoint_count.to_le_bytes());
        out.extend_from_slice(&1u32.to_le_bytes());
        out.extend_from_slice(&byte_start.to_le_bytes());
        out.extend_from_slice(&u64::try_from(identity.len()).unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&node_start.to_le_bytes());
        out.extend_from_slice(&1u32.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&0u64.to_le_bytes());
        out.extend_from_slice(&u32::try_from(cp.len()).unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(identity);
        out.extend_from_slice(&slot);
        out.extend_from_slice(&root);
        out.extend_from_slice(&cp);
        let hash = xxh3_64(&out);
        out.extend_from_slice(&hash.to_le_bytes());
        out
    }

    #[test]
    fn append_seal_reopen_append_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 4096,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
        };
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        store.append_encoded_transaction(&tx1)?;
        let expected1 = b"{\"identity\":{},\"messages\":[]}";
        assert_eq!(store.read_checkpoint("thread", "cp-1")?, expected1);
        let seal = store.seal_through(1)?;
        assert_eq!(seal.hot_suffix_logical_bytes, 0);
        assert_eq!(store.verify_all()?.failures, 0);
        drop(store);

        let mut reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, expected1);
        let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
        reopened.append_encoded_transaction(&tx2)?;
        let expected2 = b"{\"identity\":{\"x\":1},\"messages\":[]}";
        assert_eq!(reopened.read_checkpoint("thread", "cp-2")?, expected2);
        assert_eq!(reopened.verify_all()?.failures, 0);
        assert_eq!(reopened.sealed_checkpoint_count(), 1);
        assert!(reopened.hot_capacity_bytes()? >= config.wal_segment_bytes);
        Ok(())
    }

    #[test]
    fn semantic_checkpoint_append_round_trips_through_seal_and_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 4096,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
        };
        let identity = br#"{"files":[{"path":"src/lib.rs","length":16}]}"#;
        let canonical = {
            let mut value = Vec::new();
            value.extend_from_slice(b"{\"identity\":");
            value.extend_from_slice(identity);
            value.extend_from_slice(b",\"messages\":[]}");
            value
        };
        let mut store = CheckpointStore::open(temp.path(), config)?;
        store.append_checkpoint("repo", "commit-0", 7, None, identity)?;
        assert_eq!(store.read_checkpoint("repo", "commit-0")?, canonical);
        assert_eq!(store.read_checkpoint_range("repo", "commit-0", 12, 16)?, identity[..16]);
        store.seal_through(1)?;
        drop(store);

        let reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.read_checkpoint("repo", "commit-0")?, canonical);
        assert_eq!(reopened.checkpoints()[0].checkpoint_no, 7);
        assert_eq!(reopened.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn message_checkpoint_append_preserves_branches_and_multiple_roots() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 4096,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
        };
        let a = json!({"role": "user", "content": "root"});
        let b1 = json!({"role": "assistant", "content": "left-1"});
        let b2 = json!({"role": "tool", "content": "left-2"});
        let c = json!({"role": "assistant", "content": "right"});
        let x = json!({"role": "user", "content": "second-thread"});

        let mut store = CheckpointStore::open(temp.path(), config)?;
        store.append_messages_checkpoint("thread-1", "A", 0, None, std::slice::from_ref(&a))?;
        store.append_messages_checkpoint(
            "thread-1",
            "B",
            1,
            Some("A"),
            &[b1.clone(), b2.clone()],
        )?;
        store.append_messages_checkpoint("thread-1", "C", 2, Some("A"), std::slice::from_ref(&c))?;
        store.append_messages_checkpoint("thread-2", "X", 3, None, std::slice::from_ref(&x))?;

        assert_eq!(
            store.read_checkpoint("thread-1", "A")?,
            serde_json::to_vec(&json!({"identity": null, "messages": [a.clone()]}))?
        );
        assert_eq!(
            store.read_checkpoint("thread-1", "B")?,
            serde_json::to_vec(&json!({"identity": null, "messages": [a.clone(), b1, b2]}))?
        );
        assert_eq!(
            store.read_checkpoint("thread-1", "C")?,
            serde_json::to_vec(&json!({"identity": null, "messages": [a, c]}))?
        );
        assert_eq!(
            store.read_checkpoint("thread-2", "X")?,
            serde_json::to_vec(&json!({"identity": null, "messages": [x]}))?
        );
        assert_eq!(store.checkpoint_count(), 4);
        assert_eq!(store.version_count(), 5);
        assert_eq!(store.verify_all()?.failures, 0);
        assert!(store
            .append_messages_checkpoint("thread-1", "empty", 4, Some("B"), &[])
            .is_err());

        store.seal_through(4)?;
        drop(store);
        let reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.checkpoint_count(), 4);
        assert_eq!(reopened.version_count(), 5);
        assert_eq!(reopened.verify_all()?.failures, 0);
        assert_eq!(
            reopened.read_checkpoint("thread-1", "B")?,
            serde_json::to_vec(&json!({
                "identity": null,
                "messages": [
                    {"role": "user", "content": "root"},
                    {"role": "assistant", "content": "left-1"},
                    {"role": "tool", "content": "left-2"}
                ]
            }))?
        );
        Ok(())
    }

    #[test]
    fn subtree_prune_reclaims_deleted_branch_and_preserves_sibling() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 4096,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
        };
        let a = b"{}".to_vec();
        let b = b"{\"branch\":\"base\"}".to_vec();
        let mut c = Vec::with_capacity(256 * 1024);
        c.extend_from_slice(b"{\"blob\":\"");
        let mut noise = 0x9e37_79b9_u32;
        for _ in 0..(256 * 1024) {
            noise = noise
                .wrapping_mul(1_664_525)
                .wrapping_add(1_013_904_223);
            c.push(b'a' + u8::try_from((noise >> 24) % 26)?);
        }
        c.extend_from_slice(b"\"}");
        let d = b"{\"branch\":\"sibling\"}".to_vec();
        let canonical = |identity: &[u8]| {
            let mut value = Vec::new();
            value.extend_from_slice(b"{\"identity\":");
            value.extend_from_slice(identity);
            value.extend_from_slice(b",\"messages\":[]}");
            value
        };
        let mut byte_start = 0u64;
        let mut node_start = 0u64;
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let tx_a = transaction_for_thread(0, 1, byte_start, node_start, &a, "thread", "A", None);
        store.append_encoded_transaction(&tx_a)?;
        byte_start += u64::try_from(a.len())?;
        node_start += 1;
        let tx_b = transaction_for_thread(1, 2, byte_start, node_start, &b, "thread", "B", Some("A"));
        store.append_encoded_transaction(&tx_b)?;
        byte_start += u64::try_from(b.len())?;
        node_start += 1;
        let tx_c = transaction_for_thread(2, 3, byte_start, node_start, &c, "thread", "C", Some("B"));
        store.append_encoded_transaction_with_request_id(b"request-c", &tx_c)?;
        byte_start += u64::try_from(c.len())?;
        node_start += 1;
        let tx_d = transaction_for_thread(3, 4, byte_start, node_start, &d, "thread", "D", Some("B"));
        store.append_encoded_transaction(&tx_d)?;
        store.seal_through(4)?;
        let before = store.storage()?.allocated_bytes;
        let store_id = store.store_id();

        let report = store.delete_checkpoint_subtree("thread", "C")?;
        assert_eq!(report.deleted_checkpoint_count, 1);
        assert_eq!(report.retained_checkpoint_count, 3);
        assert!(report.reclaimed.allocated_bytes < before);
        assert_eq!(store.store_id(), store_id);
        assert_eq!(store.read_checkpoint("thread", "A")?, canonical(&a));
        assert_eq!(store.read_checkpoint("thread", "B")?, canonical(&b));
        assert_eq!(store.read_checkpoint("thread", "D")?, canonical(&d));
        assert!(matches!(
            store.read_checkpoint("thread", "C"),
            Err(CheckpointStoreError::CheckpointDeleted)
        ));
        assert!(matches!(
            store.append_encoded_transaction_with_request_id(b"request-c", &tx_c),
            Err(CheckpointStoreError::CheckpointDeleted)
        ));
        let conflicting_c = transaction_for_thread(
            2,
            3,
            u64::try_from(a.len() + b.len())?,
            2,
            b"{\"different\":true}",
            "thread",
            "C",
            Some("B"),
        );
        assert!(matches!(
            store.append_encoded_transaction_with_request_id(b"request-c", &conflicting_c),
            Err(CheckpointStoreError::RequestIdConflict)
        ));
        assert_eq!(store.verify_all()?.failures, 0);
        drop(store);

        let mut reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.store_id(), store_id);
        assert_eq!(reopened.read_checkpoint("thread", "D")?, canonical(&d));
        assert!(matches!(
            reopened.read_checkpoint("thread", "C"),
            Err(CheckpointStoreError::CheckpointDeleted)
        ));
        let deleted_append = reopened.append_encoded_transaction(&tx_c);
        assert!(
            matches!(deleted_append, Err(CheckpointStoreError::CheckpointDeleted)),
            "unexpected append result after prune: {deleted_append:?}"
        );
        let next = transaction(
            u32::try_from(reopened.version_count())?,
            u32::try_from(reopened.checkpoint_count() + 1)?,
            u64::try_from(reopened.state.arena_bytes.len())?,
            u64::try_from(reopened.state.compact_nodes.len() / COMPACT_NODE_SIZE)?,
            b"{\"post\":true}",
            "E",
            Some("D"),
        );
        reopened.append_encoded_transaction(&next)?;
        reopened.seal_through(4)?;
        assert_eq!(reopened.verify_all()?.failures, 0);
        drop(reopened);
        let mut final_reopen = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(final_reopen.read_checkpoint("thread", "E")?, canonical(b"{\"post\":true}"));
        assert_eq!(final_reopen.verify_all()?.failures, 0);
        let second_report = final_reopen.delete_checkpoint_subtree("thread", "B")?;
        assert_eq!(second_report.deleted_checkpoint_count, 3);
        assert_eq!(second_report.retained_checkpoint_count, 1);
        assert_eq!(final_reopen.read_checkpoint("thread", "A")?, canonical(&a));
        assert!(matches!(
            final_reopen.read_checkpoint("thread", "B"),
            Err(CheckpointStoreError::CheckpointDeleted)
        ));
        assert!(matches!(
            final_reopen.read_checkpoint("thread", "E"),
            Err(CheckpointStoreError::CheckpointDeleted)
        ));
        assert!(matches!(
            final_reopen.append_encoded_transaction_with_request_id(b"request-c", &tx_c),
            Err(CheckpointStoreError::CheckpointDeleted)
        ));
        assert_eq!(final_reopen.verify_all()?.failures, 0);
        drop(final_reopen);
        let final_pruned = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(final_pruned.read_checkpoint("thread", "A")?, canonical(&a));
        assert!(matches!(
            final_pruned.read_checkpoint("thread", "D"),
            Err(CheckpointStoreError::CheckpointDeleted)
        ));
        let mut lazy = LazyCheckpointStore::open(temp.path())?;
        assert_eq!(lazy.read_checkpoint("thread", "A")?, canonical(&a));
        assert!(matches!(
            lazy.read_checkpoint("thread", "D"),
            Err(CheckpointStoreError::CheckpointDeleted)
        ));
        Ok(())
    }

    #[test]
    fn subtree_prune_requires_a_sealed_store() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let tx = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        store.append_encoded_transaction(&tx)?;
        assert!(matches!(
            store.delete_checkpoint_subtree("thread", "cp-1"),
            Err(CheckpointStoreError::PruneRequiresSealedStore)
        ));
        assert_eq!(store.read_checkpoint("thread", "cp-1")?, b"{\"identity\":{},\"messages\":[]}");
        Ok(())
    }

    #[test]
    fn subtree_prune_staging_failure_preserves_old_authority() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let tx = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        store.append_encoded_transaction(&tx)?;
        store.seal_through(1)?;
        drop(store);

        fs::create_dir(temp.path().join("structured-g000002.t3s"))?;
        let mut reopened = CheckpointStore::open(temp.path(), config)?;
        assert!(reopened.delete_checkpoint_subtree("thread", "cp-1").is_err());
        assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, b"{\"identity\":{},\"messages\":[]}");
        drop(reopened);

        let mut recovered = CheckpointStore::open(temp.path(), config)?;
        assert!(!temp.path().join(".structured-g000002.t3s.tmp").exists());
        fs::remove_dir(temp.path().join("structured-g000002.t3s"))?;
        let report = recovered.delete_checkpoint_subtree("thread", "cp-1")?;
        assert_eq!(report.deleted_checkpoint_count, 1);
        assert!(matches!(
            recovered.read_checkpoint("thread", "cp-1"),
            Err(CheckpointStoreError::CheckpointDeleted)
        ));
        Ok(())
    }

    #[test]
    fn bounded_wal_lifecycle_seals_at_transaction_boundaries() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 4096,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
        };
        let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
        let policy = BoundedWalLifecyclePolicy::new(
            u64::try_from(tx1.len() + tx2.len() - 1)?,
            u64::try_from(tx1.len() + tx2.len())?,
        )?;
        let mut store = CheckpointStore::open(temp.path(), config)?;

        let first = store.append_encoded_transaction_with_bounded_lifecycle(&tx1, policy)?;
        assert!(first.automatic_seal.is_none());
        assert_eq!(store.hot_logical_bytes(), u64::try_from(tx1.len())?);
        let second = store.append_encoded_transaction_with_bounded_lifecycle(&tx2, policy)?;
        assert_eq!(second.automatic_seal.map(|seal| seal.checkpoint_count), Some(1));
        assert_eq!(store.sealed_checkpoint_count(), 1);
        assert_eq!(store.hot_logical_bytes(), u64::try_from(tx2.len())?);
        drop(store);

        let reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, b"{\"identity\":{},\"messages\":[]}");
        assert_eq!(reopened.read_checkpoint("thread", "cp-2")?, b"{\"identity\":{\"x\":1},\"messages\":[]}");
        assert_eq!(reopened.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn bounded_wal_lifecycle_reopens_each_automatic_cycle() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
        let tx3 = transaction(2, 3, 9, 2, b"{\"x\":2}", "cp-3", Some("cp-2"));
        let policy = BoundedWalLifecyclePolicy::new(
            u64::try_from(tx1.len() + tx2.len() - 1)?,
            u64::try_from(tx1.len() + tx2.len())?,
        )?;

        let mut store = CheckpointStore::open(temp.path(), config)?;
        store.append_encoded_transaction_with_bounded_lifecycle(&tx1, policy)?;
        let second = store.append_encoded_transaction_with_bounded_lifecycle(&tx2, policy)?;
        assert!(second.automatic_seal.is_some());
        drop(store);

        let mut reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.read_checkpoint("thread", "cp-2")?, b"{\"identity\":{\"x\":1},\"messages\":[]}");
        assert_eq!(reopened.verify_all()?.failures, 0);
        let third = reopened.append_encoded_transaction_with_bounded_lifecycle(&tx3, policy)?;
        assert!(third.automatic_seal.is_some());
        drop(reopened);

        let final_reopen = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(final_reopen.sealed_checkpoint_count(), 2);
        assert_eq!(final_reopen.read_checkpoint("thread", "cp-3")?, b"{\"identity\":{\"x\":2},\"messages\":[]}");
        assert_eq!(final_reopen.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn bounded_wal_lifecycle_seal_failure_backpressures_without_append() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
        let policy = BoundedWalLifecyclePolicy::new(
            u64::try_from(tx1.len())?,
            u64::try_from(tx1.len() + tx2.len())?,
        )?;
        let mut store = CheckpointStore::open(temp.path(), config)?;
        store.append_encoded_transaction_with_bounded_lifecycle(&tx1, policy)?;
        let hot_before = store.hot_logical_bytes();
        fs::create_dir(temp.path().join(".structured-g000001.t3s.tmp"))?;

        let error = store
            .append_encoded_transaction_with_bounded_lifecycle(&tx2, policy)
            .expect_err("seal failure must backpressure the next transaction");
        assert!(matches!(error, CheckpointStoreError::Io(_)));
        assert_eq!(store.hot_logical_bytes(), hot_before);
        assert_eq!(store.checkpoint_count(), 1);
        fs::remove_dir(temp.path().join(".structured-g000001.t3s.tmp"))?;
        drop(store);

        let reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, b"{\"identity\":{},\"messages\":[]}");
        assert_eq!(reopened.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn bounded_wal_lifecycle_rejects_oversize_before_wal_change() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        let size = u64::try_from(transaction.len())?;
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let policy = BoundedWalLifecyclePolicy::new(size - 1, size - 1)?;
        let error = store
            .append_encoded_transaction_with_bounded_lifecycle(&transaction, policy)
            .expect_err("transaction above hard limit must be rejected");
        assert!(matches!(error, CheckpointStoreError::Format(_)));
        assert_eq!(store.hot_logical_bytes(), 0);
        assert_eq!(store.checkpoint_count(), 0);

        let policy = BoundedWalLifecyclePolicy::new(size - 1, size)?;
        let report = store.append_encoded_transaction_with_bounded_lifecycle(&transaction, policy)?;
        assert!(report.automatic_seal.is_none());
        assert_eq!(store.hot_logical_bytes(), size);
        Ok(())
    }

    #[test]
    fn writer_lock_rejects_second_open_and_releases_on_drop() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let store = CheckpointStore::open(temp.path(), config)?;
        let second = CheckpointStore::open(temp.path(), config);
        assert!(matches!(second, Err(CheckpointStoreError::WriterAlreadyOpen)));
        drop(store);

        let reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn writer_lock_releases_after_owner_process_exit() -> Result<(), Box<dyn std::error::Error>> {
        let _nested_process_guard = nested_process_test_guard();
        if std::env::var_os("TULYA_WRITER_LOCK_CHILD").is_some() {
            let path = std::env::var_os("TULYA_WRITER_LOCK_PATH")
                .ok_or_else(|| "writer-lock child path is missing".to_owned())?;
            let store = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
            assert_eq!(store.verify_all()?.failures, 0);
            std::process::exit(0);
        }

        let temp = tempfile::tempdir()?;
        let status = std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("checkpoint_store::tests::writer_lock_releases_after_owner_process_exit")
            .arg("--test-threads=1")
            .env("TULYA_WRITER_LOCK_CHILD", "1")
            .env("TULYA_WRITER_LOCK_PATH", temp.path())
            .status()?;
        assert!(status.success());

        let reopened = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        assert_eq!(reopened.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn checkpoint_identity_rejects_empty_and_oversized_values_before_wal_append() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let oversized = "x".repeat(MAX_CHECKPOINT_IDENTIFIER_BYTES + 1);
        let invalid_transactions = [
            transaction_for_thread(0, 1, 0, 0, b"{}", "", "cp-1", None),
            transaction_for_thread(0, 1, 0, 0, b"{}", "thread", "", None),
            transaction_for_thread(0, 1, 0, 0, b"{}", &oversized, "cp-1", None),
            transaction_for_thread(0, 1, 0, 0, b"{}", "thread", &oversized, None),
            transaction_for_thread(0, 1, 0, 0, b"{}", "thread", "cp-1", Some(&oversized)),
        ];

        for transaction in invalid_transactions {
            let before = store.hot_logical_bytes();
            let result = store.append_encoded_transaction(&transaction);
            assert!(matches!(result, Err(CheckpointStoreError::Format(_))));
            assert_eq!(store.hot_logical_bytes(), before);
            assert_eq!(store.checkpoint_count(), 0);
        }
        Ok(())
    }

    #[test]
    fn checkpoint_identity_parser_rejects_malformed_persisted_records() {
        let empty_thread = checkpoint_record(0, 0, "", "cp-1", None, b"{}");
        assert!(parse_checkpoint_record(&empty_thread, 1, 0).is_err());
        let oversized = "x".repeat(MAX_CHECKPOINT_IDENTIFIER_BYTES + 1);
        let oversized_checkpoint = checkpoint_record(0, 0, "thread", &oversized, None, b"{}");
        assert!(parse_checkpoint_record(&oversized_checkpoint, 1, 0).is_err());
    }

    #[test]
    fn transaction_parser_rejects_zero_checkpoint_count_without_underflow() {
        let mut transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        transaction[16..20].copy_from_slice(&0u32.to_le_bytes());
        let checksum = xxh3_64(&transaction[..transaction.len() - TX_CHECKSUM_SIZE]);
        let checksum_start = transaction.len() - TX_CHECKSUM_SIZE;
        transaction[checksum_start..].copy_from_slice(&checksum.to_le_bytes());

        assert!(matches!(
            parse_transaction_unchecked(&transaction, 0),
            Err(CheckpointStoreError::Format(message))
                if message == "transaction checkpoint count must be non-zero"
        ));
    }

    #[test]
    fn transaction_parser_rejects_descending_version_topology_before_body_walk() {
        let mut transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        transaction[8..12].copy_from_slice(&2u32.to_le_bytes());
        transaction[12..16].copy_from_slice(&1u32.to_le_bytes());
        transaction[20..24].copy_from_slice(&0u32.to_le_bytes());
        let checksum = xxh3_64(&transaction[..transaction.len() - TX_CHECKSUM_SIZE]);
        let checksum_start = transaction.len() - TX_CHECKSUM_SIZE;
        transaction[checksum_start..].copy_from_slice(&checksum.to_le_bytes());

        assert!(matches!(
            parse_transaction_unchecked(&transaction, 0),
            Err(CheckpointStoreError::Format(message))
                if message == "transaction version topology is inconsistent"
        ));
    }

    #[test]
    fn store_id_persists_and_legacy_migration_is_explicit() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let store = CheckpointStore::open(temp.path(), config)?;
        let initial_id = store.store_id().ok_or("new store did not receive StoreId")?;
        assert_eq!(initial_id.to_hex().len(), 32);
        assert_eq!(StoreId::from_hex(&initial_id.to_hex())?, initial_id);
        drop(store);

        let reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.store_id(), Some(initial_id));
        drop(reopened);

        let manifest_path = temp.path().join(MANIFEST_FILE);
        let mut legacy: Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        legacy["format_version"] = json!(MANIFEST_FORMAT_VERSION_LEGACY);
        legacy
            .as_object_mut()
            .ok_or("manifest is not a JSON object")?
            .remove("store_id");
        std::fs::write(&manifest_path, serde_json::to_vec_pretty(&legacy)?)?;
        let legacy_bytes = std::fs::read(&manifest_path)?;

        let mut legacy_store = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(legacy_store.store_id(), None);
        assert_eq!(std::fs::read(&manifest_path)?, legacy_bytes);
        let migrated_id = legacy_store.migrate_store_id()?;
        assert_ne!(migrated_id, initial_id);
        assert_eq!(legacy_store.store_id(), Some(migrated_id));
        assert_eq!(legacy_store.migrate_store_id()?, migrated_id);
        drop(legacy_store);

        let migrated = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(migrated.store_id(), Some(migrated_id));
        Ok(())
    }

    #[test]
    fn store_id_fork_preserves_checkpoint_and_request_identity() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let original_id = store.store_id().ok_or("new store did not receive StoreId")?;
        let transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        store.append_encoded_transaction_with_request_id(b"request-1", &transaction)?;
        let expected_state = store.read_checkpoint("thread", "cp-1")?;
        let forked_id = store.fork_as_new_store()?;
        assert_ne!(forked_id, original_id);
        assert_eq!(store.read_checkpoint("thread", "cp-1")?, expected_state);
        assert_eq!(store.verify_all()?.failures, 0);
        assert!(matches!(
            store.append_encoded_transaction_with_request_id(b"request-1", &transaction)?,
            CheckpointStoreAppendOutcome::AlreadyCommitted
        ));
        drop(store);

        let reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.store_id(), Some(forked_id));
        assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, expected_state);
        Ok(())
    }

    #[test]
    fn malformed_store_id_is_rejected_without_regeneration() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let store = CheckpointStore::open(temp.path(), config)?;
        drop(store);
        let manifest_path = temp.path().join(MANIFEST_FILE);
        let mut manifest: Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        manifest["store_id"] = json!("not-a-store-id");
        std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

        assert!(matches!(
            CheckpointStore::open(temp.path(), config),
            Err(CheckpointStoreError::Format(_))
        ));
        Ok(())
    }

    #[test]
    fn positional_reads_are_exact_without_mutating_file_cursor() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("positional-read.bin");
        std::fs::write(&path, b"abcdef")?;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(5))?;
        let mut bytes = [0u8; 3];
        read_file_exact_at(&file, &mut bytes, 1)?;
        assert_eq!(&bytes, b"bcd");
        assert_eq!(file.stream_position()?, 5);
        Ok(())
    }

    #[test]
    fn lazy_payload_fast_path_uses_the_declared_cap() {
        assert!(lazy_payload_fast_path_allowed(0));
        assert!(lazy_payload_fast_path_allowed(LAZY_EAGER_PAYLOAD_CAP_BYTES));
        assert!(!lazy_payload_fast_path_allowed(LAZY_EAGER_PAYLOAD_CAP_BYTES + 1));
    }

    #[test]
    fn lazy_payload_fast_path_falls_back_above_cap() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 16 * 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 64 * 1024,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::ClonePayload,
        };
        let identity_len = usize::try_from(LAZY_EAGER_PAYLOAD_CAP_BYTES)? + 1024;
        let mut identity = Vec::with_capacity(identity_len);
        identity.push(b'\"');
        for index in 0..identity_len.saturating_sub(2) {
            let value = (index as u64)
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            identity.push(b'a' + u8::try_from(value % 26)?);
        }
        identity.push(b'\"');

        let mut store = CheckpointStore::open(temp.path(), config)?;
        store.append_encoded_transaction(&transaction(0, 1, 0, 0, &identity, "cp-1", None))?;
        store.seal_through(1)?;
        drop(store);

        let mut lazy = LazyCheckpointStore::open(temp.path())?;
        assert!(!lazy.payload_fast_path_active());
        assert!(lazy.cached_block_count() <= LazyCheckpointStore::cache_capacity_blocks());
        assert_eq!(lazy.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn request_id_retry_survives_seal_and_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 4096,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::ReusePayload,
        };
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let first = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        assert!(matches!(
            store.append_encoded_transaction_with_request_id(b"", &first),
            Err(CheckpointStoreError::Format(_))
        ));
        let oversized_request = vec![b'x'; MAX_REQUEST_ID_BYTES + 1];
        assert!(matches!(
            store.append_encoded_transaction_with_request_id(&oversized_request, &first),
            Err(CheckpointStoreError::Format(_))
        ));
        let first_outcome = store.append_encoded_transaction_with_request_id(b"request-1", &first)?;
        assert!(matches!(first_outcome, CheckpointStoreAppendOutcome::Appended(_)));
        let logical_tail = store.hot_logical_bytes();
        let checkpoint_count = store.checkpoint_count();
        assert!(matches!(
            store.append_encoded_transaction_with_request_id(b"request-1", &first)?,
            CheckpointStoreAppendOutcome::AlreadyCommitted
        ));
        assert_eq!(store.hot_logical_bytes(), logical_tail);
        assert_eq!(store.checkpoint_count(), checkpoint_count);

        let conflicting = transaction(0, 1, 0, 0, b"{\"x\":1}", "cp-1", None);
        assert!(matches!(
            store.append_encoded_transaction_with_request_id(b"request-1", &conflicting),
            Err(CheckpointStoreError::RequestIdConflict)
        ));
        assert_eq!(store.hot_logical_bytes(), logical_tail);
        assert_eq!(store.checkpoint_count(), checkpoint_count);

        store.seal_through(1)?;
        drop(store);
        let mut reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.verify_all()?.failures, 0);
        assert!(matches!(
            reopened.append_encoded_transaction_with_request_id(b"request-1", &first)?,
            CheckpointStoreAppendOutcome::AlreadyCommitted
        ));

        let second = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
        assert!(matches!(
            reopened.append_encoded_transaction_with_request_id(b"request-2", &second)?,
            CheckpointStoreAppendOutcome::Appended(_)
        ));
        assert_eq!(reopened.checkpoint_count(), 2);
        assert_eq!(reopened.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn malformed_request_route_suffix_is_rejected_on_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig::default();
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        let _ = store.append_encoded_transaction_with_request_id(b"request-1", &transaction)?;
        store.seal_through(1)?;
        drop(store);

        let route_path = temp.path().join("route-g000001.t3r");
        let mut route = std::fs::read(&route_path)?;
        route.truncate(route.len().saturating_sub(1));
        std::fs::write(&route_path, route)?;
        assert!(CheckpointStore::open(temp.path(), config).is_err());
        Ok(())
    }

    #[cfg(feature = "bench-diagnostics")]
    #[test]
    fn durable_append_recovery_after_publication_crash() -> Result<(), Box<dyn std::error::Error>> {
        let _nested_process_guard = nested_process_test_guard();
        if std::env::var_os("TULYA_DURABLE_APPEND_CHILD").is_some() {
            let path = std::env::var_os("TULYA_DURABLE_APPEND_PATH")
                .ok_or_else(|| "durable-append child path is missing".to_owned())?;
            let mut store = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
            let transaction = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
            let _ = store.append_encoded_transaction_with_request_id(b"crash-request", &transaction)?;
            return Err("diagnostic child did not exit at the publication boundary".into());
        }

        let temp = tempfile::tempdir()?;
        let status = std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("checkpoint_store::tests::durable_append_recovery_after_publication_crash")
            .arg("--test-threads=1")
            .env("TULYA_DURABLE_APPEND_CHILD", "1")
            .env("TULYA_DURABLE_APPEND_PATH", temp.path())
            .env(
                "TULYA_CHECKPOINT_STORE_CRASH_POINT",
                "after-hot-sync-before-memory-publication",
            )
            .status()?;
        assert_eq!(status.code(), Some(86));

        let mut reopened = CheckpointStore::open(temp.path(), CheckpointStoreConfig::default())?;
        assert_eq!(reopened.checkpoint_count(), 1);
        assert_eq!(
            reopened.read_checkpoint("thread", "cp-1")?,
            b"{\"identity\":{},\"messages\":[]}"
        );
        assert_eq!(reopened.verify_all()?.failures, 0);
        assert!(matches!(
            reopened.append_encoded_transaction_with_request_id(
                b"crash-request",
                &transaction(0, 1, 0, 0, b"{}", "cp-1", None),
            )?,
            CheckpointStoreAppendOutcome::AlreadyCommitted
        ));
        Ok(())
    }

    #[cfg(feature = "bench-diagnostics")]
    #[test]
    fn store_id_manifest_replacement_crash_recovery() -> Result<(), Box<dyn std::error::Error>> {
        let _nested_process_guard = nested_process_test_guard();
        if std::env::var_os("TULYA_STORE_ID_CHILD").is_some() {
            let path = std::env::var_os("TULYA_STORE_ID_PATH")
                .ok_or_else(|| "StoreId child path is missing".to_owned())?;
            let mut store = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
            assert_eq!(store.store_id(), None);
            let _ = store.migrate_store_id()?;
            return Err("diagnostic child did not exit at the StoreId manifest boundary".into());
        }

        let boundaries = [
            ("after-manifest-write", false),
            ("after-manifest-sync", false),
            ("after-manifest-rename", true),
            ("after-manifest-dir-sync", true),
        ];
        for (boundary, new_authority) in boundaries {
            let temp = tempfile::tempdir()?;
            let config = CheckpointStoreConfig::default();
            let store = CheckpointStore::open(temp.path(), config)?;
            drop(store);
            let manifest_path = temp.path().join(MANIFEST_FILE);
            let mut legacy: Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
            legacy["format_version"] = json!(MANIFEST_FORMAT_VERSION_LEGACY);
            legacy
                .as_object_mut()
                .ok_or("manifest is not a JSON object")?
                .remove("store_id");
            std::fs::write(&manifest_path, serde_json::to_vec_pretty(&legacy)?)?;

            let status = std::process::Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("checkpoint_store::tests::store_id_manifest_replacement_crash_recovery")
                .arg("--test-threads=1")
                .env("TULYA_STORE_ID_CHILD", "1")
                .env("TULYA_STORE_ID_PATH", temp.path())
                .env("TULYA_CHECKPOINT_STORE_CRASH_POINT", boundary)
                .status()?;
            assert_eq!(status.code(), Some(86), "boundary {boundary}");

            let reopened = CheckpointStore::open(temp.path(), config)?;
            assert_eq!(reopened.store_id().is_some(), new_authority, "boundary {boundary}");
        }
        Ok(())
    }

    #[cfg(feature = "bench-diagnostics")]
    #[test]
    fn subtree_prune_crash_recovery() -> Result<(), Box<dyn std::error::Error>> {
        let _nested_process_guard = nested_process_test_guard();
        if std::env::var_os("TULYA_SUBTREE_PRUNE_CHILD").is_some() {
            let path = std::env::var_os("TULYA_SUBTREE_PRUNE_PATH")
                .ok_or_else(|| "subtree-prune child path is missing".to_owned())?;
            let mut store = CheckpointStore::open(path, CheckpointStoreConfig::default())?;
            let _ = store.delete_checkpoint_subtree("thread", "C")?;
            return Err("diagnostic child did not exit at the subtree-prune boundary".into());
        }

        let boundaries = [
            ("after-prune-segment-write", false),
            ("after-segment-sync", false),
            ("after-segment-rename", false),
            ("after-segment-dir-sync", false),
            ("after-route-write", false),
            ("after-route-sync", false),
            ("after-route-rename", false),
            ("after-route-dir-sync", false),
            ("after-manifest-write", false),
            ("after-manifest-sync", false),
            ("after-manifest-rename", true),
            ("after-manifest-dir-sync", true),
            ("after-prune-manifest-authority", true),
            ("after-prune-old-file-delete", true),
        ];
        for (boundary, new_authority) in boundaries {
            let temp = tempfile::tempdir()?;
            let config = CheckpointStoreConfig::default();
            let mut store = CheckpointStore::open(temp.path(), config)?;
            let tx_a = transaction_for_thread(0, 1, 0, 0, b"{}", "thread", "A", None);
            store.append_encoded_transaction(&tx_a)?;
            let tx_b = transaction_for_thread(1, 2, 2, 1, b"{\"base\":true}", "thread", "B", Some("A"));
            store.append_encoded_transaction(&tx_b)?;
            let tx_c = transaction_for_thread(2, 3, 15, 2, b"{\"deleted\":true}", "thread", "C", Some("B"));
            store.append_encoded_transaction_with_request_id(b"request-c", &tx_c)?;
            let tx_d = transaction_for_thread(3, 4, 31, 3, b"{\"sibling\":true}", "thread", "D", Some("B"));
            store.append_encoded_transaction(&tx_d)?;
            store.seal_through(4)?;
            drop(store);

            let status = std::process::Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("checkpoint_store::tests::subtree_prune_crash_recovery")
                .arg("--test-threads=1")
                .env("TULYA_SUBTREE_PRUNE_CHILD", "1")
                .env("TULYA_SUBTREE_PRUNE_PATH", temp.path())
                .env("TULYA_CHECKPOINT_STORE_CRASH_POINT", boundary)
                .status()?;
            assert_eq!(status.code(), Some(86), "boundary {boundary}");

            let mut reopened = CheckpointStore::open(temp.path(), config)?;
            assert_eq!(reopened.verify_all()?.failures, 0, "boundary {boundary}");
            assert_eq!(reopened.read_checkpoint("thread", "A")?, b"{\"identity\":{},\"messages\":[]}");
            assert_eq!(reopened.read_checkpoint("thread", "B")?, b"{\"identity\":{\"base\":true},\"messages\":[]}");
            assert_eq!(reopened.read_checkpoint("thread", "D")?, b"{\"identity\":{\"sibling\":true},\"messages\":[]}");
            if new_authority {
                assert!(matches!(
                    reopened.read_checkpoint("thread", "C"),
                    Err(CheckpointStoreError::CheckpointDeleted)
                ));
                assert!(matches!(
                    reopened.append_encoded_transaction_with_request_id(b"request-c", &tx_c),
                    Err(CheckpointStoreError::CheckpointDeleted)
                ));
                assert_eq!(reopened.manifest.generation, 2);
                assert_eq!(reopened.manifest.segments.len(), 1);
                assert_eq!(reopened.manifest.routes.len(), 1);
                assert!(!temp.path().join("structured-g000001.t3s").exists());
                assert!(!temp.path().join("route-g000001.t3r").exists());
            } else {
                assert_eq!(reopened.read_checkpoint("thread", "C")?, b"{\"identity\":{\"deleted\":true},\"messages\":[]}");
                assert!(matches!(
                    reopened.append_encoded_transaction_with_request_id(b"request-c", &tx_c)?,
                    CheckpointStoreAppendOutcome::AlreadyCommitted
                ));
                assert_eq!(reopened.manifest.generation, 1);
                assert!(temp.path().join("structured-g000001.t3s").exists());
                assert!(temp.path().join("route-g000001.t3r").exists());
            }
            drop(reopened);
        }
        Ok(())
    }

    #[test]
    fn lazy_sealed_reader_round_trip_and_bounded_cache() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 32,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::ClonePayload,
        };
        let mut store = CheckpointStore::open(temp.path(), config)?;
        let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        store.append_encoded_transaction(&tx1)?;
        store.seal_through(1)?;
        drop(store);

        let mut lazy = LazyCheckpointStore::open(temp.path())?;
        assert!(lazy.payload_fast_path_active());
        assert_eq!(lazy.checkpoint_count(), 1);
        assert_eq!(lazy.version_count(), 1);
        assert_eq!(
            lazy.read_checkpoint("thread", "cp-1")?,
            b"{\"identity\":{},\"messages\":[]}"
        );
        assert_eq!(lazy.verify_all()?.failures, 0);
        assert!(lazy.cached_block_count() <= LazyCheckpointStore::cache_capacity_blocks());
        assert!(lazy.read_metrics().cache_misses > 0);

        let mut ownership_config = config;
        ownership_config.recovery_mode = CheckpointStoreRecoveryMode::ReusePayload;
        let mut ownership_reopened = CheckpointStore::open(temp.path(), ownership_config)?;
        assert_eq!(
            ownership_reopened.read_checkpoint("thread", "cp-1")?,
            b"{\"identity\":{},\"messages\":[]}"
        );
        assert_eq!(ownership_reopened.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn lazy_open_does_not_create_a_writable_store_manifest() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let _reader = LazyCheckpointStore::open(temp.path())?;
        assert!(!temp.path().join(MANIFEST_FILE).exists());
        Ok(())
    }

    #[test]
    fn write_capable_lazy_tier_reads_sealed_and_hot_overlay_across_reopen(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 32,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::Lazy,
        };
        let tx1 = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        let tx2 = transaction(1, 2, 2, 1, b"{\"x\":1}", "cp-2", Some("cp-1"));
        let tx3 = transaction(2, 3, 9, 2, b"{\"x\":2}", "cp-3", Some("cp-2"));

        let mut store = CheckpointStore::open(temp.path(), config)?;
        store.append_encoded_transaction(&tx1)?;
        store.seal_through(1)?;
        store.append_encoded_transaction(&tx2)?;
        assert_eq!(store.checkpoint_count(), 2);
        assert_eq!(store.read_checkpoint("thread", "cp-1")?, b"{\"identity\":{},\"messages\":[]}");
        assert_eq!(store.read_checkpoint("thread", "cp-2")?, b"{\"identity\":{\"x\":1},\"messages\":[]}");
        assert_eq!(store.verify_all()?.failures, 0);
        drop(store);

        let mut reopened = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, b"{\"identity\":{},\"messages\":[]}");
        assert_eq!(reopened.read_checkpoint("thread", "cp-2")?, b"{\"identity\":{\"x\":1},\"messages\":[]}");
        assert_eq!(reopened.verify_all()?.failures, 0);
        reopened.seal_through(2)?;
        reopened.append_encoded_transaction(&tx3)?;
        assert_eq!(reopened.read_checkpoint("thread", "cp-3")?, b"{\"identity\":{\"x\":2},\"messages\":[]}");
        drop(reopened);

        let mut final_reopen = CheckpointStore::open(temp.path(), config)?;
        assert_eq!(final_reopen.checkpoint_count(), 3);
        assert_eq!(final_reopen.sealed_checkpoint_count(), 2);
        assert_eq!(final_reopen.read_checkpoint("thread", "cp-1")?, b"{\"identity\":{},\"messages\":[]}");
        assert_eq!(final_reopen.read_checkpoint("thread", "cp-2")?, b"{\"identity\":{\"x\":1},\"messages\":[]}");
        assert_eq!(final_reopen.read_checkpoint("thread", "cp-3")?, b"{\"identity\":{\"x\":2},\"messages\":[]}");
        assert_eq!(final_reopen.verify_all()?.failures, 0);
        final_reopen.seal_through(3)?;
        assert!(matches!(
            final_reopen.delete_checkpoint_subtree("thread", "cp-1"),
            Err(CheckpointStoreError::PruneRequiresEagerRecovery)
        ));
        Ok(())
    }

    #[test]
    fn write_capable_lazy_tier_preserves_request_retry_after_reopen(
        ) -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 32,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::Lazy,
        };
        let tx = transaction(0, 1, 0, 0, b"{}", "cp-1", None);
        let mut store = CheckpointStore::open(temp.path(), config)?;
        assert!(matches!(
            store.append_encoded_transaction_with_request_id(b"lazy-request", &tx)?,
            CheckpointStoreAppendOutcome::Appended(_)
        ));
        store.seal_through(1)?;
        drop(store);

        let mut reopened = CheckpointStore::open(temp.path(), config)?;
        assert!(matches!(
            reopened.append_encoded_transaction_with_request_id(b"lazy-request", &tx)?,
            CheckpointStoreAppendOutcome::AlreadyCommitted
        ));
        assert_eq!(reopened.read_checkpoint("thread", "cp-1")?, b"{\"identity\":{},\"messages\":[]}");
        assert_eq!(reopened.verify_all()?.failures, 0);
        Ok(())
    }

    #[test]
    fn checkpoint_range_matches_full_read_for_eager_and_lazy_modes(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for recovery_mode in [
            CheckpointStoreRecoveryMode::ReusePayload,
            CheckpointStoreRecoveryMode::Lazy,
        ] {
            let temp = tempfile::tempdir()?;
            let config = CheckpointStoreConfig {
                wal_segment_bytes: 1024 * 1024,
                preinit_chunk_bytes: 64 * 1024,
                sealed_block_size: 32,
                zstd_level: 1,
                recovery_mode,
            };
            let tx1 = transaction(0, 1, 0, 0, b"{\"x\":1}", "cp-1", None);
            let tx2 = transaction(1, 2, 7, 1, b"{\"x\":2}", "cp-2", Some("cp-1"));
            let mut store = CheckpointStore::open(temp.path(), config)?;
            store.append_encoded_transaction(&tx1)?;
            store.seal_through(1)?;
            store.append_encoded_transaction(&tx2)?;
            drop(store);

            let store = CheckpointStore::open(temp.path(), config)?;
            for checkpoint in store.checkpoints() {
                let full = store.read_checkpoint(&checkpoint.thread_id, &checkpoint.checkpoint_id)?;
                let length = u64::try_from(full.len())?;
                for (offset, range_length) in [
                    (0, 0),
                    (0, 1),
                    (1, length.saturating_sub(2)),
                    (length / 2, 2),
                    (length.saturating_sub(2), 2),
                    (length, 0),
                ] {
                    let end = offset.checked_add(range_length).ok_or("test range overflow")?;
                    let actual = store.read_checkpoint_range(
                        &checkpoint.thread_id,
                        &checkpoint.checkpoint_id,
                        offset,
                        range_length,
                    )?;
                    assert_eq!(actual, full[usize::try_from(offset)?..usize::try_from(end)?]);
                }
                assert!(matches!(
                    store.read_checkpoint_range(
                        &checkpoint.thread_id,
                        &checkpoint.checkpoint_id,
                        length + 1,
                        0,
                    ),
                    Err(CheckpointStoreError::Format(_))
                ));
                assert!(matches!(
                    store.read_checkpoint_range(
                        &checkpoint.thread_id,
                        &checkpoint.checkpoint_id,
                        length,
                        1,
                    ),
                    Err(CheckpointStoreError::Format(_))
                ));
                assert!(matches!(
                    store.read_checkpoint_range(
                        &checkpoint.thread_id,
                        &checkpoint.checkpoint_id,
                        u64::MAX,
                        1,
                    ),
                    Err(CheckpointStoreError::Format(_))
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn lazy_checkpoint_range_reads_above_payload_fast_path_cap(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let config = CheckpointStoreConfig {
            wal_segment_bytes: 16 * 1024 * 1024,
            preinit_chunk_bytes: 64 * 1024,
            sealed_block_size: 64 * 1024,
            zstd_level: 1,
            recovery_mode: CheckpointStoreRecoveryMode::Lazy,
        };
        let blob_len = 8 * 1024 * 1024 + 1;
        let blob_prefix = b"{\"blob\":\"";
        let blob_suffix = b"\"}";
        let mut identity = Vec::with_capacity(blob_prefix.len() + blob_len + blob_suffix.len());
        identity.extend_from_slice(blob_prefix);
        identity.resize(blob_prefix.len() + blob_len, b'x');
        identity.extend_from_slice(blob_suffix);
        let tx = transaction(0, 1, 0, 0, &identity, "large", None);

        let mut store = CheckpointStore::open(temp.path(), config)?;
        store.append_encoded_transaction(&tx)?;
        store.seal_through(1)?;
        drop(store);

        let store = CheckpointStore::open(temp.path(), config)?;
        let checkpoint = store
            .checkpoints()
            .first()
            .ok_or("large checkpoint metadata missing")?;
        let total = checkpoint.logical_state_len;
        let prefix = b"{\"identity\":{\"blob\":\"";
        assert_eq!(
            store.read_checkpoint_range("thread", "large", 0, u64::try_from(prefix.len())?)?,
            prefix
        );
        let blob_start = u64::try_from(prefix.len())?;
        assert_eq!(
            store.read_checkpoint_range("thread", "large", blob_start + 1024, 128)?,
            vec![b'x'; 128]
        );
        let suffix = b",\"messages\":[]}";
        let suffix_start = total
            .checked_sub(u64::try_from(suffix.len())?)
            .ok_or("large checkpoint suffix underflow")?;
        assert_eq!(
            store.read_checkpoint_range("thread", "large", suffix_start, u64::try_from(suffix.len())?)?,
            suffix
        );
        assert_eq!(store.verify_all()?.failures, 0);
        Ok(())
    }
}
