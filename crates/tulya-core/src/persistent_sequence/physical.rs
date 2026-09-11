//! Authoritative physical content store for the durable history core (E6).
//!
//! Before E6 the durable authority sealed and reopened a complete in-memory
//! arena through the `T2I2` full-image codec, so reopen materialized every
//! retained payload byte and seal rewrote them. This module makes the
//! physical content store itself authoritative for retained content:
//!
//! ```text
//! VersionId
//!    v
//! catalogue root descriptor (schema6 metadata snapshot)
//!    v
//! physical node records (fixed-width canonical T2N2, direct coordinates)
//!    v
//! bounded leaf payload (exact offsets, 16 KiB staging bound)
//! ```
//!
//! Layout per physical generation `P` (distinct from the authority snapshot
//! generation, which may seal many times against one physical generation):
//!
//! ```text
//! content-{P:020}.payload   raw leaf bytes, 16-byte header + dense extents
//! content-{P:020}.nodes     16-byte header + dense 72-byte T2N2 records
//! ```
//!
//! `NodeId` is the dense record index and `payload_offset` the dense byte
//! offset, so both resolve by arithmetic without loading any index map. Old
//! nodes and payload are immutable; fresh splices append new payload plus
//! path-copy node records. There is deliberately no LSM, SQLite, RocksDB,
//! mmap, or storage plugin framework here: two append-only files plus the
//! metadata authority.
//!
//! Crash discipline (two-phase physical-before-metadata publication):
//!
//! ```text
//! prepare delta in memory (reads only, no file mutation)
//! append fresh payload, append fresh nodes, sync physical content
//! append THL5 metadata frame, sync metadata WAL
//! infallibly adopt catalogue roots and frontiers in memory
//! ```
//!
//! Bytes written before their metadata frame commits are orphans, never
//! authority: the authoritative frontier comes from the sealed schema6
//! snapshot plus the valid bounded THL5 suffix. A longer file is an orphan
//! tail (ignored read-only, truncated before writable mutation); a shorter
//! file fails closed. Truncation failure is an explicit failure, never
//! silent reuse.
//!
//! All names, magics, and digest domains here are STAGING. No release Format
//! v1 freeze happens before E9, and there is no migration from E5 bytes:
//! pre-E6 authority fails closed at the THL magic and schema gates.

use super::avl::{ArenaNode, ArenaStore, V2AvlError};
use super::compaction_v2::{
    plan_reachable_arena, rebuild_compact_records, remapped_node_id, repack_compact_ranges,
};
use super::format_v2::{
    decode_v2_node, encode_v2_node, V2NodeRecord, V2RootRecord, V2_NODE_RECORD_SIZE,
};
use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;

/// Magic for physical payload files (staging).
const PHYSICAL_PAYLOAD_MAGIC: [u8; 4] = *b"TPC1";
/// Magic for physical node files (staging).
const PHYSICAL_NODE_MAGIC: [u8; 4] = *b"TNC1";
/// Fixed physical file header: magic[4] + generation[u64] + reserved[u64].
/// Header reads are file-identity checks and count as metadata I/O, never
/// payload I/O, so cold-reopen locality evidence stays exact.
const PHYSICAL_HEADER_SIZE: u64 = 16;
/// Canonical node record width on disk: reuse of the `T2N2` codec, never a
/// third tree serialization.
const PHYSICAL_NODE_RECORD_SIZE: u64 = V2_NODE_RECORD_SIZE as u64;
/// Staging domain binding one exact physical delta realization. The logical
/// operation digest keeps its E2 domain and stays physical-coordinate free;
/// this digest binds the durable byte realization. Exact string freezes at
/// E9 with everything else staging.
const PHYSICAL_DELTA_DIGEST_DOMAIN: &[u8] = b"tulya-history/staging/physical-delta\0";

/// Physical operation scope for I/O accounting: foreground edits/reads,
/// reopen/recovery, seal/publication, and GC/compaction maintenance stay in
/// separate buckets so a 1 GiB GC can never contaminate the next 4 KiB
/// splice measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpScope {
    #[default]
    Foreground,
    Reopen,
    Publication,
    Maintenance,
}

/// Exact byte/barrier counters for one operation scope.
///
/// Every field counts actual Tulya `read`/`write` byte requests (or sync
/// barriers), never inferred logical lengths and never storage-device
/// sectors. All arithmetic is checked: on overflow the slot pins at
/// `u64::MAX` and the sticky overflow flag records that saturation happened,
/// so no overflow is ever hidden and counter failure never affects storage
/// correctness.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoBucket {
    pub node_bytes_read: u64,
    pub node_bytes_written: u64,
    pub payload_bytes_read: u64,
    pub payload_bytes_written: u64,
    pub metadata_bytes_read: u64,
    pub metadata_bytes_written: u64,
    pub wal_bytes_read: u64,
    pub wal_bytes_written: u64,
    pub snapshot_bytes_read: u64,
    pub snapshot_bytes_written: u64,
    pub physical_sync_count: u64,
    pub wal_sync_count: u64,
    pub metadata_sync_count: u64,
    pub directory_sync_count: u64,
    pub physical_sync_nanos: u64,
    pub wal_sync_nanos: u64,
    pub metadata_sync_nanos: u64,
    pub directory_sync_nanos: u64,
}

/// Physical I/O counters across the four operation scopes, plus the sticky
/// overflow flag. Separate from the algorithmic
/// [`SequenceWorkCounters`](super::SequenceWorkCounters), which keep their
/// traversal-shape semantics unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PhysicalIoCounters {
    pub foreground: IoBucket,
    pub reopen: IoBucket,
    pub publication: IoBucket,
    pub maintenance: IoBucket,
    pub overflow: bool,
}

impl PhysicalIoCounters {
    /// Borrows the bucket for one scope.
    pub fn bucket(&self, scope: OpScope) -> &IoBucket {
        match scope {
            OpScope::Foreground => &self.foreground,
            OpScope::Reopen => &self.reopen,
            OpScope::Publication => &self.publication,
            OpScope::Maintenance => &self.maintenance,
        }
    }

    fn bucket_mut(&mut self, scope: OpScope) -> &mut IoBucket {
        match scope {
            OpScope::Foreground => &mut self.foreground,
            OpScope::Reopen => &mut self.reopen,
            OpScope::Publication => &mut self.publication,
            OpScope::Maintenance => &mut self.maintenance,
        }
    }
}

#[derive(Debug)]
struct IoLedgerShared {
    counters: Cell<PhysicalIoCounters>,
    scope: Cell<OpScope>,
}

/// Shared physical I/O ledger: interior mutability (matching the existing
/// `Cell`-based work counters) so `&self` file reads can account bytes
/// without restructuring every caller to `&mut`.
#[derive(Debug, Clone)]
pub(crate) struct IoLedger {
    shared: Rc<IoLedgerShared>,
}

impl IoLedger {
    pub(crate) fn new() -> Self {
        Self {
            shared: Rc::new(IoLedgerShared {
                counters: Cell::new(PhysicalIoCounters::default()),
                scope: Cell::new(OpScope::Foreground),
            }),
        }
    }

    /// Runs `body` under `scope`, restoring the prior scope afterwards.
    pub(crate) fn scoped(&self, scope: OpScope) -> IoScopeGuard<'_> {
        let prior = self.shared.scope.get();
        self.shared.scope.set(scope);
        IoScopeGuard {
            ledger: self,
            prior,
        }
    }

    /// Snapshots all counters.
    pub(crate) fn snapshot(&self) -> PhysicalIoCounters {
        self.shared.counters.get()
    }

    /// Resets every bucket and clears the overflow flag. Tests reset after
    /// reopen so deltas around one operation are unambiguous.
    pub(crate) fn reset(&self) {
        self.shared.counters.set(PhysicalIoCounters::default());
    }

    /// Applies a checked mutation to the current scope's bucket.
    fn update(&self, mutate: impl FnOnce(&mut IoBucket, &mut bool)) {
        let mut counters = self.shared.counters.get();
        let scope = self.shared.scope.get();
        let mut overflow = counters.overflow;
        mutate(counters.bucket_mut(scope), &mut overflow);
        counters.overflow = overflow;
        self.shared.counters.set(counters);
    }

    /// Counts a byte/barrier amount into one slot with overflow pinning.
    fn bump(slot: &mut u64, amount: u64, overflow: &mut bool) {
        match slot.checked_add(amount) {
            Some(next) => *slot = next,
            None => {
                *slot = u64::MAX;
                *overflow = true;
            }
        }
    }

    pub(crate) fn count_node_read(&self, bytes: u64) {
        self.update(|bucket, overflow| Self::bump(&mut bucket.node_bytes_read, bytes, overflow));
    }

    pub(crate) fn count_node_written(&self, bytes: u64) {
        self.update(|bucket, overflow| Self::bump(&mut bucket.node_bytes_written, bytes, overflow));
    }

    pub(crate) fn count_payload_read(&self, bytes: u64) {
        self.update(|bucket, overflow| Self::bump(&mut bucket.payload_bytes_read, bytes, overflow));
    }

    pub(crate) fn count_payload_written(&self, bytes: u64) {
        self.update(|bucket, overflow| {
            Self::bump(&mut bucket.payload_bytes_written, bytes, overflow)
        });
    }

    pub(crate) fn count_metadata_read(&self, bytes: u64) {
        self.update(|bucket, overflow| {
            Self::bump(&mut bucket.metadata_bytes_read, bytes, overflow)
        });
    }

    pub(crate) fn count_metadata_written(&self, bytes: u64) {
        self.update(|bucket, overflow| {
            Self::bump(&mut bucket.metadata_bytes_written, bytes, overflow)
        });
    }

    pub(crate) fn count_wal_read(&self, bytes: u64) {
        self.update(|bucket, overflow| Self::bump(&mut bucket.wal_bytes_read, bytes, overflow));
    }

    pub(crate) fn count_wal_written(&self, bytes: u64) {
        self.update(|bucket, overflow| Self::bump(&mut bucket.wal_bytes_written, bytes, overflow));
    }

    pub(crate) fn count_snapshot_read(&self, bytes: u64) {
        self.update(|bucket, overflow| {
            Self::bump(&mut bucket.snapshot_bytes_read, bytes, overflow)
        });
    }

    pub(crate) fn count_snapshot_written(&self, bytes: u64) {
        self.update(|bucket, overflow| {
            Self::bump(&mut bucket.snapshot_bytes_written, bytes, overflow)
        });
    }

    pub(crate) fn count_physical_sync(&self, nanos: u64) {
        self.update(|bucket, overflow| {
            Self::bump(&mut bucket.physical_sync_count, 1, overflow);
            Self::bump(&mut bucket.physical_sync_nanos, nanos, overflow);
        });
    }

    pub(crate) fn count_wal_sync(&self, nanos: u64) {
        self.update(|bucket, overflow| {
            Self::bump(&mut bucket.wal_sync_count, 1, overflow);
            Self::bump(&mut bucket.wal_sync_nanos, nanos, overflow);
        });
    }

    /// Counts a metadata-file (snapshot/manifest) durability barrier.
    pub(crate) fn count_metadata_sync(&self, nanos: u64) {
        self.update(|bucket, overflow| {
            Self::bump(&mut bucket.metadata_sync_count, 1, overflow);
            Self::bump(&mut bucket.metadata_sync_nanos, nanos, overflow);
        });
    }

    pub(crate) fn count_directory_sync(&self, nanos: u64) {
        self.update(|bucket, overflow| {
            Self::bump(&mut bucket.directory_sync_count, 1, overflow);
            Self::bump(&mut bucket.directory_sync_nanos, nanos, overflow);
        });
    }
}

/// Restores the ledger's prior scope on drop.
pub(crate) struct IoScopeGuard<'a> {
    ledger: &'a IoLedger,
    prior: OpScope,
}

impl Drop for IoScopeGuard<'_> {
    fn drop(&mut self) {
        self.ledger.shared.scope.set(self.prior);
    }
}

/// Physical payload filename for one generation (staging namespace).
pub(crate) fn content_payload_filename(generation: u64) -> String {
    format!("content-{generation:020}.payload")
}

/// Physical node filename for one generation (staging namespace).
pub(crate) fn content_nodes_filename(generation: u64) -> String {
    format!("content-{generation:020}.nodes")
}

/// Reports whether a directory entry is an obsolete Tulya physical-generation
/// file: exactly `content-{u64}.payload` or `content-{u64}.nodes` for a
/// generation other than the current physical authority. Only exact names
/// ever match — the E5 strict-grammar lesson applied to the new namespace —
/// so unrelated application files sharing a prefix are never touched.
pub(crate) fn superseded_physical_content_file(name: &str, current: u64) -> bool {
    for (prefix, suffix) in [("content-", ".payload"), ("content-", ".nodes")] {
        if let Some(rest) = name
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
        {
            if let Ok(generation) = rest.parse::<u64>() {
                // Canonical 20-digit zero-padded form only: padded and
                // unpadded spellings of one number must not disagree about
                // deletability.
                if rest.len() == 20 {
                    return generation != current;
                }
            }
        }
    }
    false
}

fn encode_header(magic: [u8; 4], generation: u64) -> [u8; 16] {
    let mut header = [0u8; 16];
    header[..4].copy_from_slice(&magic);
    header[4..12].copy_from_slice(&generation.to_le_bytes());
    header
}

fn decode_header(expected_magic: [u8; 4], header: &[u8; 16]) -> Result<u64, V2AvlError> {
    if header[..4] != expected_magic {
        return Err(V2AvlError::Invalid(
            "history physical content magic mismatch",
        ));
    }
    let generation = u64::from_le_bytes(
        header[4..12]
            .try_into()
            .map_err(|_| V2AvlError::Invalid("history physical content header width mismatch"))?,
    );
    if header[12..16] != [0u8; 4] {
        return Err(V2AvlError::Invalid(
            "history physical content header reserves nonzero bytes",
        ));
    }
    Ok(generation)
}

/// Portable exact-position read: `read_at` in a loop so short reads can never
/// silently truncate a node record or leaf extent.
fn read_exact_at(file: &File, mut offset: u64, mut output: &mut [u8]) -> std::io::Result<()> {
    while !output.is_empty() {
        match file.read_at(output, offset)? {
            0 => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "history physical content read reached end of file",
                ));
            }
            read => {
                offset = offset.checked_add(read as u64).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "history physical content offset exceeds u64",
                    )
                })?;
                output = &mut output[read..];
            }
        }
    }
    Ok(())
}

/// Authoritative file-backed content arena for one physical generation.
///
/// Reads resolve by arithmetic (node index, payload offset) against the
/// committed frontiers; anything at or beyond the frontier fails closed, so
/// orphan tails are never addressable. Writes append at the file end and
/// only the commit path advances the in-memory frontiers. Every byte request
/// is counted on the shared ledger under the ambient scope.
pub(crate) struct PhysicalContentStore {
    dir: PathBuf,
    generation: u64,
    payload: Option<File>,
    nodes: Option<File>,
    writable: bool,
    committed_payload_end: u64,
    committed_node_count: u64,
    ledger: IoLedger,
    #[cfg(test)]
    fault_next_payload_append: bool,
    #[cfg(test)]
    fault_next_node_append: bool,
    #[cfg(test)]
    fault_next_sync: bool,
}

impl std::fmt::Debug for PhysicalContentStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PhysicalContentStore")
            .field("generation", &self.generation)
            .field(
                "files_open",
                &(self.payload.is_some() && self.nodes.is_some()),
            )
            .field("committed_payload_end", &self.committed_payload_end)
            .field("committed_node_count", &self.committed_node_count)
            .finish_non_exhaustive()
    }
}

impl PhysicalContentStore {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn committed_payload_end(&self) -> u64 {
        self.committed_payload_end
    }

    pub(crate) fn committed_node_count(&self) -> u64 {
        self.committed_node_count
    }

    pub(crate) fn ledger(&self) -> IoLedger {
        self.ledger.clone()
    }

    fn content_paths(dir: &Path, generation: u64) -> (PathBuf, PathBuf) {
        (
            dir.join(content_payload_filename(generation)),
            dir.join(content_nodes_filename(generation)),
        )
    }

    /// Creates a brand-new empty physical generation: both files with valid
    /// headers, frontiers zero. Fails closed when either name already exists
    /// (a crashed earlier build is recreated explicitly, never silently
    /// resumed).
    pub(crate) fn create_new(
        dir: &Path,
        generation: u64,
        ledger: &IoLedger,
    ) -> Result<Self, V2AvlError> {
        const CREATE_FAILED: V2AvlError =
            V2AvlError::Invalid("history physical content creation failed");
        let (payload_path, nodes_path) = Self::content_paths(dir, generation);
        let mut payload = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&payload_path)
            .map_err(|_| CREATE_FAILED)?;
        let mut nodes = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&nodes_path)
            .map_err(|_| CREATE_FAILED)?;
        payload
            .write_all(&encode_header(PHYSICAL_PAYLOAD_MAGIC, generation))
            .map_err(|_| CREATE_FAILED)?;
        nodes
            .write_all(&encode_header(PHYSICAL_NODE_MAGIC, generation))
            .map_err(|_| CREATE_FAILED)?;
        let started = Instant::now();
        payload.sync_all().map_err(|_| CREATE_FAILED)?;
        nodes.sync_all().map_err(|_| CREATE_FAILED)?;
        let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        ledger.count_metadata_written(2 * PHYSICAL_HEADER_SIZE);
        ledger.count_physical_sync(nanos);
        ledger.count_physical_sync(0);
        Ok(Self {
            dir: dir.to_path_buf(),
            generation,
            payload: Some(payload),
            nodes: Some(nodes),
            writable: true,
            committed_payload_end: 0,
            committed_node_count: 0,
            ledger: ledger.clone(),
            #[cfg(test)]
            fault_next_payload_append: false,
            #[cfg(test)]
            fault_next_node_append: false,
            #[cfg(test)]
            fault_next_sync: false,
        })
    }

    /// Recreates a physical generation after a crashed build left a partial
    /// one behind: truncates both files (or creates them) and rewrites valid
    /// headers. Only the GC builder uses this, for the next generation name
    /// that was never authoritative.
    pub(crate) fn recreate(
        dir: &Path,
        generation: u64,
        ledger: &IoLedger,
    ) -> Result<Self, V2AvlError> {
        const CREATE_FAILED: V2AvlError =
            V2AvlError::Invalid("history physical content recreation failed");
        let (payload_path, nodes_path) = Self::content_paths(dir, generation);
        let mut payload = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&payload_path)
            .map_err(|_| CREATE_FAILED)?;
        let mut nodes = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&nodes_path)
            .map_err(|_| CREATE_FAILED)?;
        payload
            .write_all(&encode_header(PHYSICAL_PAYLOAD_MAGIC, generation))
            .map_err(|_| CREATE_FAILED)?;
        nodes
            .write_all(&encode_header(PHYSICAL_NODE_MAGIC, generation))
            .map_err(|_| CREATE_FAILED)?;
        ledger.count_metadata_written(2 * PHYSICAL_HEADER_SIZE);
        Ok(Self {
            dir: dir.to_path_buf(),
            generation,
            payload: Some(payload),
            nodes: Some(nodes),
            writable: true,
            committed_payload_end: 0,
            committed_node_count: 0,
            ledger: ledger.clone(),
            #[cfg(test)]
            fault_next_payload_append: false,
            #[cfg(test)]
            fault_next_node_append: false,
            #[cfg(test)]
            fault_next_sync: false,
        })
    }

    /// Opens an existing physical generation against expected authoritative
    /// frontiers. Missing files fail closed; a wrong generation identity in
    /// either header fails closed; files shorter than the frontier fail
    /// closed (referenced bytes are absent). Files longer than the frontier
    /// hold an orphan tail: read-only opens ignore it, while
    /// [`heal_for_write`](Self::heal_for_write) truncates it before writable
    /// mutation. Nothing is mutated here on any path.
    pub(crate) fn open_existing(
        dir: &Path,
        generation: u64,
        payload_end: u64,
        node_count: u64,
        ledger: &IoLedger,
        writable: bool,
    ) -> Result<Self, V2AvlError> {
        const OPEN_FAILED: V2AvlError = V2AvlError::Invalid("history physical content open failed");
        let (payload_path, nodes_path) = Self::content_paths(dir, generation);
        let payload = OpenOptions::new()
            .read(true)
            .write(writable)
            .create(false)
            .open(&payload_path)
            .map_err(|_| OPEN_FAILED)?;
        let nodes = OpenOptions::new()
            .read(true)
            .write(writable)
            .create(false)
            .open(&nodes_path)
            .map_err(|_| OPEN_FAILED)?;
        let mut payload_header = [0u8; 16];
        let mut nodes_header = [0u8; 16];
        read_exact_at(&payload, 0, &mut payload_header).map_err(|_| OPEN_FAILED)?;
        read_exact_at(&nodes, 0, &mut nodes_header).map_err(|_| OPEN_FAILED)?;
        ledger.count_metadata_read(2 * PHYSICAL_HEADER_SIZE);
        if decode_header(PHYSICAL_PAYLOAD_MAGIC, &payload_header)? != generation
            || decode_header(PHYSICAL_NODE_MAGIC, &nodes_header)? != generation
        {
            return Err(V2AvlError::Invalid(
                "history physical content generation disagrees with its headers",
            ));
        }
        let payload_len = payload.metadata().map_err(|_| OPEN_FAILED)?.len();
        let nodes_len = nodes.metadata().map_err(|_| OPEN_FAILED)?.len();
        let expected_payload =
            PHYSICAL_HEADER_SIZE
                .checked_add(payload_end)
                .ok_or(V2AvlError::Overflow(
                    "history physical payload frontier exceeds u64",
                ))?;
        let expected_nodes = PHYSICAL_HEADER_SIZE
            .checked_add(node_count.checked_mul(PHYSICAL_NODE_RECORD_SIZE).ok_or(
                V2AvlError::Overflow("history physical node frontier exceeds u64"),
            )?)
            .ok_or(V2AvlError::Overflow(
                "history physical node frontier exceeds u64",
            ))?;
        if payload_len < expected_payload || nodes_len < expected_nodes {
            return Err(V2AvlError::Invalid(
                "history physical content is shorter than its authoritative frontier",
            ));
        }
        Ok(Self {
            dir: dir.to_path_buf(),
            generation,
            payload: Some(payload),
            nodes: Some(nodes),
            writable,
            committed_payload_end: payload_end,
            committed_node_count: node_count,
            ledger: ledger.clone(),
            #[cfg(test)]
            fault_next_payload_append: false,
            #[cfg(test)]
            fault_next_node_append: false,
            #[cfg(test)]
            fault_next_sync: false,
        })
    }

    /// Binds a physical generation whose files may not exist yet (genesis
    /// before the first durable splice): no handles, zero frontiers. Reads
    /// fail closed until files exist; the first mutation creates them with
    /// valid headers.
    pub(crate) fn open_deferred(dir: &Path, generation: u64, ledger: &IoLedger) -> Self {
        Self {
            dir: dir.to_path_buf(),
            generation,
            payload: None,
            nodes: None,
            writable: false,
            committed_payload_end: 0,
            committed_node_count: 0,
            ledger: ledger.clone(),
            #[cfg(test)]
            fault_next_payload_append: false,
            #[cfg(test)]
            fault_next_node_append: false,
            #[cfg(test)]
            fault_next_sync: false,
        }
    }

    /// Reports whether both content files for one generation exist.
    pub(crate) fn content_files_present(dir: &Path, generation: u64) -> bool {
        let (payload_path, nodes_path) = Self::content_paths(dir, generation);
        payload_path.is_file() && nodes_path.is_file()
    }

    const ABSENT: V2AvlError = V2AvlError::Invalid("history physical content files are absent");

    fn payload_file(&self) -> Result<&File, V2AvlError> {
        self.payload.as_ref().ok_or(Self::ABSENT)
    }

    fn nodes_file(&self) -> Result<&File, V2AvlError> {
        self.nodes.as_ref().ok_or(Self::ABSENT)
    }

    /// Opens (creating with valid headers when absent) both files. Called
    /// before any mutation; reads never create.
    fn ensure_open(&mut self) -> Result<(), V2AvlError> {
        if self.payload.is_none() || self.nodes.is_none() {
            if self.committed_payload_end != 0 || self.committed_node_count != 0 {
                return Err(Self::ABSENT);
            }
            let fresh = Self::create_new(&self.dir, self.generation, &self.ledger)?;
            self.payload = fresh.payload;
            self.nodes = fresh.nodes;
            self.writable = true;
        }
        Ok(())
    }

    /// Upgrades read-only handles to writable ones, revalidating nothing
    /// (identity and frontiers were checked at open): writable opens must
    /// mutate, read-only opens must never have writable handles.
    fn ensure_writable(&mut self) -> Result<(), V2AvlError> {
        const UPGRADE_FAILED: V2AvlError =
            V2AvlError::Invalid("history physical content writable upgrade failed");
        self.ensure_open()?;
        if self.writable {
            return Ok(());
        }
        let (payload_path, nodes_path) = Self::content_paths(&self.dir, self.generation);
        let payload = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&payload_path)
            .map_err(|_| UPGRADE_FAILED)?;
        let nodes = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&nodes_path)
            .map_err(|_| UPGRADE_FAILED)?;
        self.payload = Some(payload);
        self.nodes = Some(nodes);
        self.writable = true;
        Ok(())
    }

    /// Truncates orphan tails to the authoritative frontier before writable
    /// mutation. Exact-length files are untouched. A truncation failure is an
    /// explicit failure: the caller must not allocate after an unhealed tail.
    pub(crate) fn heal_for_write(&mut self) -> Result<(), V2AvlError> {
        self.ensure_writable()?;
        const HEAL_FAILED: V2AvlError =
            V2AvlError::Invalid("history physical orphan-tail healing failed");
        let expected_payload = PHYSICAL_HEADER_SIZE
            .checked_add(self.committed_payload_end)
            .ok_or(V2AvlError::Overflow(
                "history physical payload frontier exceeds u64",
            ))?;
        let expected_nodes = PHYSICAL_HEADER_SIZE
            .checked_add(
                self.committed_node_count
                    .checked_mul(PHYSICAL_NODE_RECORD_SIZE)
                    .ok_or(V2AvlError::Overflow(
                        "history physical node frontier exceeds u64",
                    ))?,
            )
            .ok_or(V2AvlError::Overflow(
                "history physical node frontier exceeds u64",
            ))?;
        let payload_len = self
            .payload_file()
            .map_err(|_| HEAL_FAILED)?
            .metadata()
            .map_err(|_| HEAL_FAILED)?
            .len();
        let nodes_len = self
            .nodes_file()
            .map_err(|_| HEAL_FAILED)?
            .metadata()
            .map_err(|_| HEAL_FAILED)?
            .len();
        if payload_len < expected_payload || nodes_len < expected_nodes {
            return Err(V2AvlError::Invalid(
                "history physical content is shorter than its authoritative frontier",
            ));
        }
        if payload_len > expected_payload {
            self.payload_file()
                .map_err(|_| HEAL_FAILED)?
                .set_len(expected_payload)
                .map_err(|_| HEAL_FAILED)?;
        }
        if nodes_len > expected_nodes {
            self.nodes_file()
                .map_err(|_| HEAL_FAILED)?
                .set_len(expected_nodes)
                .map_err(|_| HEAL_FAILED)?;
        }
        Ok(())
    }

    /// Loads one canonical node record by dense identifier. File-backed
    /// decode revalidates magic, flags, widths, and bounds exactly like the
    /// image importer, so a corrupt record fails here, at first access.
    pub(super) fn load_record(&self, node_id: u64) -> Result<V2NodeRecord, V2AvlError> {
        if node_id >= self.committed_node_count {
            return Err(V2AvlError::Invalid(
                "history physical node identifier is outside the committed frontier",
            ));
        }
        let offset = PHYSICAL_HEADER_SIZE
            .checked_add(node_id.checked_mul(PHYSICAL_NODE_RECORD_SIZE).ok_or(
                V2AvlError::Overflow("history physical node offset exceeds u64"),
            )?)
            .ok_or(V2AvlError::Overflow(
                "history physical node offset exceeds u64",
            ))?;
        let mut bytes = [0u8; PHYSICAL_NODE_RECORD_SIZE as usize];
        read_exact_at(self.nodes_file()?, offset, &mut bytes)
            .map_err(|_| V2AvlError::Invalid("history physical node read failed"))?;
        self.ledger.count_node_read(PHYSICAL_NODE_RECORD_SIZE);
        decode_v2_node(&bytes).map_err(V2AvlError::from)
    }

    /// Reads one exact payload range inside the committed frontier.
    pub(super) fn read_payload_range(&self, start: u64, end: u64) -> Result<Vec<u8>, V2AvlError> {
        let len = end.checked_sub(start).ok_or(V2AvlError::Invalid(
            "history physical payload range is inverted",
        ))?;
        if end > self.committed_payload_end {
            return Err(V2AvlError::Invalid(
                "history physical payload range is outside the committed frontier",
            ));
        }
        let len_usize = usize::try_from(len)
            .map_err(|_| V2AvlError::Overflow("v2 payload end exceeds usize"))?;
        let offset = PHYSICAL_HEADER_SIZE
            .checked_add(start)
            .ok_or(V2AvlError::Overflow(
                "history physical payload offset exceeds u64",
            ))?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(len_usize)
            .map_err(|_| V2AvlError::Capacity("history physical payload read allocation failed"))?;
        output.resize(len_usize, 0);
        read_exact_at(self.payload_file()?, offset, &mut output)
            .map_err(|_| V2AvlError::Invalid("history physical payload read failed"))?;
        self.ledger.count_payload_read(len);
        Ok(output)
    }

    /// Reads the complete committed payload (quiescent GC planning only:
    /// foreground paths never call this).
    pub(super) fn read_all_payload(&self) -> Result<Vec<u8>, V2AvlError> {
        self.read_payload_range(0, self.committed_payload_end)
    }

    /// Loads the complete committed node table (quiescent GC planning only).
    pub(super) fn load_all_records(&self) -> Result<Vec<V2NodeRecord>, V2AvlError> {
        let count_usize = usize::try_from(self.committed_node_count)
            .map_err(|_| V2AvlError::Overflow("v2 node identifier exceeds usize"))?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(count_usize)
            .map_err(|_| V2AvlError::Capacity("history physical node table allocation failed"))?;
        for index in 0..count_usize {
            let node_id = u64::try_from(index)
                .map_err(|_| V2AvlError::Overflow("v2 node identifier exceeds u64"))?;
            records.push(self.load_record(node_id)?);
        }
        Ok(records)
    }

    /// Appends already-validated payload delta bytes at the file end.
    /// Contiguity against the committed frontier is asserted: any divergence
    /// fails closed before a byte moves.
    pub(crate) fn append_payload_delta(&mut self, bytes: &[u8]) -> Result<(), V2AvlError> {
        #[cfg(test)]
        if self.fault_next_payload_append {
            self.fault_next_payload_append = false;
            return Err(V2AvlError::Invalid(
                "injected fault: history physical payload append rejected",
            ));
        }
        self.ensure_writable()?;
        if bytes.is_empty() {
            return Ok(());
        }
        let expected = PHYSICAL_HEADER_SIZE
            .checked_add(self.committed_payload_end)
            .ok_or(V2AvlError::Overflow(
                "history physical payload frontier exceeds u64",
            ))?;
        let actual = self
            .payload_file()?
            .metadata()
            .map_err(|_| V2AvlError::Invalid("history physical payload append failed"))?
            .len();
        if actual != expected {
            return Err(V2AvlError::Invalid(
                "history physical payload file disagrees with its committed frontier",
            ));
        }
        self.payload_file()
            .map_err(|_| V2AvlError::Invalid("history physical payload append failed"))?
            .seek(SeekFrom::End(0))
            .map_err(|_| V2AvlError::Invalid("history physical payload append failed"))?;
        self.payload
            .as_mut()
            .ok_or(V2AvlError::Invalid(
                "history physical payload append failed",
            ))?
            .write_all(bytes)
            .map_err(|_| V2AvlError::Invalid("history physical payload append failed"))?;
        self.ledger
            .count_payload_written(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    /// Appends already-validated canonical node records at the file end,
    /// with the same contiguity assertion as payload.
    pub(crate) fn append_node_delta(&mut self, bytes: &[u8]) -> Result<(), V2AvlError> {
        #[cfg(test)]
        if self.fault_next_node_append {
            self.fault_next_node_append = false;
            return Err(V2AvlError::Invalid(
                "injected fault: history physical node append rejected",
            ));
        }
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() as u64 % PHYSICAL_NODE_RECORD_SIZE != 0 {
            return Err(V2AvlError::Invalid(
                "history physical node delta is not record-aligned",
            ));
        }
        let expected = PHYSICAL_HEADER_SIZE
            .checked_add(
                self.committed_node_count
                    .checked_mul(PHYSICAL_NODE_RECORD_SIZE)
                    .ok_or(V2AvlError::Overflow(
                        "history physical node frontier exceeds u64",
                    ))?,
            )
            .ok_or(V2AvlError::Overflow(
                "history physical node frontier exceeds u64",
            ))?;
        self.ensure_writable()?;
        let actual = self
            .nodes_file()?
            .metadata()
            .map_err(|_| V2AvlError::Invalid("history physical node append failed"))?
            .len();
        if actual != expected {
            return Err(V2AvlError::Invalid(
                "history physical node file disagrees with its committed frontier",
            ));
        }
        self.nodes_file()
            .map_err(|_| V2AvlError::Invalid("history physical node append failed"))?
            .seek(SeekFrom::End(0))
            .map_err(|_| V2AvlError::Invalid("history physical node append failed"))?;
        self.nodes
            .as_mut()
            .ok_or(V2AvlError::Invalid("history physical node append failed"))?
            .write_all(bytes)
            .map_err(|_| V2AvlError::Invalid("history physical node append failed"))?;
        self.ledger
            .count_node_written(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    /// Durability barrier for appended content: both files, whose lengths
    /// change with every commit.
    pub(crate) fn sync_content(&mut self) -> Result<(), V2AvlError> {
        #[cfg(test)]
        if self.fault_next_sync {
            self.fault_next_sync = false;
            return Err(V2AvlError::Invalid(
                "injected fault: history physical content sync rejected",
            ));
        }
        self.ensure_writable()?;
        let first = Instant::now();
        self.payload_file()?
            .sync_all()
            .map_err(|_| V2AvlError::Invalid("history physical content sync failed"))?;
        self.ledger
            .count_physical_sync(u64::try_from(first.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let second = Instant::now();
        self.nodes_file()?
            .sync_all()
            .map_err(|_| V2AvlError::Invalid("history physical content sync failed"))?;
        self.ledger
            .count_physical_sync(u64::try_from(second.elapsed().as_nanos()).unwrap_or(u64::MAX));
        Ok(())
    }

    /// Advances the in-memory frontiers after the delta bytes are known
    /// durable and the catalogue adoption is decided. Plain stores, no I/O:
    /// infallible by construction (callers validate monotonicity before).
    pub(crate) fn adopt_frontiers(&mut self, payload_end: u64, node_count: u64) {
        debug_assert!(payload_end >= self.committed_payload_end);
        debug_assert!(node_count >= self.committed_node_count);
        self.committed_payload_end = payload_end;
        self.committed_node_count = node_count;
    }

    /// Verifies the open files cover the committed frontiers: shorter
    /// files mean referenced physical bytes are absent and fail closed.
    /// Longer files hold an orphan tail, which is fine here (reads are
    /// frontier-bounded; writable opens heal).
    pub(crate) fn check_files_cover_frontiers(&self) -> Result<(), V2AvlError> {
        const CHECK_FAILED: V2AvlError =
            V2AvlError::Invalid("history physical content length check failed");
        let Some(payload) = self.payload.as_ref() else {
            if self.committed_payload_end == 0 && self.committed_node_count == 0 {
                return Ok(());
            }
            return Err(Self::ABSENT);
        };
        let Some(nodes) = self.nodes.as_ref() else {
            return Err(Self::ABSENT);
        };
        let payload_len = payload.metadata().map_err(|_| CHECK_FAILED)?.len();
        let nodes_len = nodes.metadata().map_err(|_| CHECK_FAILED)?.len();
        let expected_payload = PHYSICAL_HEADER_SIZE
            .checked_add(self.committed_payload_end)
            .ok_or(V2AvlError::Overflow(
                "history physical payload frontier exceeds u64",
            ))?;
        let expected_nodes = PHYSICAL_HEADER_SIZE
            .checked_add(
                self.committed_node_count
                    .checked_mul(PHYSICAL_NODE_RECORD_SIZE)
                    .ok_or(V2AvlError::Overflow(
                        "history physical node frontier exceeds u64",
                    ))?,
            )
            .ok_or(V2AvlError::Overflow(
                "history physical node frontier exceeds u64",
            ))?;
        if payload_len < expected_payload || nodes_len < expected_nodes {
            return Err(V2AvlError::Invalid(
                "history physical content is shorter than its authoritative frontier",
            ));
        }
        Ok(())
    }

    /// Best-effort rollback of file ends to the committed frontier after a
    /// failed pure-path commit. Reports whether the files provably match the
    /// frontier again; when false the caller must treat the store as
    /// unhealed (reads stay safe — they are frontier-bounded — but no new
    /// content may be allocated until a writable reopen heals).
    pub(crate) fn rollback_to_committed(&mut self) -> bool {
        let expected_payload = match PHYSICAL_HEADER_SIZE.checked_add(self.committed_payload_end) {
            Some(value) => value,
            None => return false,
        };
        let expected_nodes = match self
            .committed_node_count
            .checked_mul(PHYSICAL_NODE_RECORD_SIZE)
            .and_then(|count| PHYSICAL_HEADER_SIZE.checked_add(count))
        {
            Some(value) => value,
            None => return false,
        };
        let payload = match self.payload_file() {
            Ok(file) => file,
            Err(_) => return true,
        };
        if payload.set_len(expected_payload).is_err() {
            return false;
        }
        let nodes = match self.nodes_file() {
            Ok(file) => file,
            Err(_) => return false,
        };
        if nodes.set_len(expected_nodes).is_err() {
            return false;
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn arm_fail_next_payload_append(&mut self) {
        self.fault_next_payload_append = true;
    }

    #[cfg(test)]
    pub(crate) fn arm_fail_next_node_append(&mut self) {
        self.fault_next_node_append = true;
    }

    #[cfg(test)]
    pub(crate) fn arm_fail_next_sync(&mut self) {
        self.fault_next_sync = true;
    }
}

impl ArenaStore for PhysicalContentStore {
    fn arena_payload_len(&self) -> usize {
        usize::try_from(self.committed_payload_end).unwrap_or(usize::MAX)
    }

    fn arena_node_count(&self) -> usize {
        usize::try_from(self.committed_node_count).unwrap_or(usize::MAX)
    }

    fn arena_load_node(&self, node_id: u64) -> Result<ArenaNode, V2AvlError> {
        let record = self.load_record(node_id)?;
        build_arena_node(self, node_id, record)
    }

    fn arena_read_payload(&self, start: u64, end: u64) -> Result<Vec<u8>, V2AvlError> {
        self.read_payload_range(start, end)
    }

    fn arena_alias_payload(&mut self, start: u64, end: u64) -> Result<(u64, Vec<u8>), V2AvlError> {
        let bytes = self.read_payload_range(start, end)?;
        Ok((start, bytes))
    }

    fn arena_append_payload(&mut self, bytes: &[u8]) -> Result<u64, V2AvlError> {
        let offset = self.committed_payload_end;
        self.append_payload_delta(bytes)?;
        let next = offset
            .checked_add(bytes.len() as u64)
            .ok_or(V2AvlError::Overflow(
                "history physical payload frontier exceeds u64",
            ))?;
        self.committed_payload_end = next;
        Ok(offset)
    }

    fn arena_push_node(&mut self, node: ArenaNode) -> Result<u64, V2AvlError> {
        let node_id = self.committed_node_count;
        self.append_node_delta(&encode_v2_node(node.record()))?;
        self.committed_node_count = node_id
            .checked_add(1)
            .ok_or(V2AvlError::Overflow("v2 node arena length exceeds u64"))?;
        Ok(node_id)
    }

    fn arena_truncate(&mut self, _payload_len: usize, _node_count: usize) {
        // Direct file-arena mutation always goes through the explicit
        // commit/rollback path with frontier checks; the shared algorithm
        // never runs mutably against committed files (preparation uses the
        // delta view below), so truncation here is a best-effort restore.
        let _ = self.rollback_to_committed();
    }
}

/// Builds a live arena node from one canonical record, resolving branch
/// children against committed records. One level only: child roots carry
/// the commitments the branch recomputation needs.
fn build_arena_node(
    store: &PhysicalContentStore,
    node_id: u64,
    record: V2NodeRecord,
) -> Result<ArenaNode, V2AvlError> {
    use super::image_v2::{v2_node_fields, V2NodeFields};
    match v2_node_fields(record)? {
        V2NodeFields::Leaf {
            payload_offset,
            payload_len,
        } => {
            if payload_offset
                .checked_add(payload_len)
                .ok_or(V2AvlError::Overflow("v2 leaf payload range exceeds u64"))?
                > store.committed_payload_end
            {
                return Err(V2AvlError::Invalid(
                    "history physical leaf references payload outside the committed frontier",
                ));
            }
            Ok(ArenaNode::Leaf {
                payload_offset,
                payload_len,
                record,
            })
        }
        V2NodeFields::Branch {
            left_node_id,
            right_node_id,
            left_len,
        } => {
            if left_node_id >= node_id || right_node_id >= node_id {
                return Err(V2AvlError::Invalid(
                    "history physical branch child must reference an earlier node",
                ));
            }
            let left_record = store.load_record(left_node_id)?;
            let right_record = store.load_record(right_node_id)?;
            let left = V2RootRecord::from_node(left_node_id, left_record)?;
            let right = V2RootRecord::from_node(right_node_id, right_record)?;
            if left.logical_len() != left_len {
                return Err(V2AvlError::Invalid(
                    "history physical branch left length disagrees with its child",
                ));
            }
            Ok(ArenaNode::Branch {
                left,
                right,
                record,
            })
        }
    }
}

/// In-memory preparation view over committed files plus uncommitted delta
/// buffers. The shared AVL algorithm runs against this during splice
/// preparation: old nodes and payload resolve from files (counted reads),
/// fresh bytes and records accumulate in the buffers, and no file mutation
/// occurs. New branches may reference old committed nodes plus earlier fresh
/// nodes from this same delta — never future identifiers.
pub(super) struct DeltaView<'a> {
    base: &'a PhysicalContentStore,
    base_payload_end: u64,
    base_node_count: u64,
    payload_delta: Vec<u8>,
    node_delta: Vec<u8>,
}

impl<'a> DeltaView<'a> {
    pub(super) fn new(base: &'a PhysicalContentStore) -> Self {
        Self {
            base,
            base_payload_end: base.committed_payload_end,
            base_node_count: base.committed_node_count,
            payload_delta: Vec::new(),
            node_delta: Vec::new(),
        }
    }

    /// Splits the finished delta out: fresh payload bytes plus fresh
    /// canonical node records, in allocation order.
    pub(super) fn finish(self) -> (Vec<u8>, Vec<u8>) {
        (self.payload_delta, self.node_delta)
    }

    fn delta_node_count(&self) -> u64 {
        self.node_delta.len() as u64 / PHYSICAL_NODE_RECORD_SIZE
    }

    fn load_delta_record(&self, node_id: u64) -> Result<V2NodeRecord, V2AvlError> {
        let index = node_id
            .checked_sub(self.base_node_count)
            .ok_or(V2AvlError::Invalid(
                "history physical delta node is outside its range",
            ))?;
        let start = index
            .checked_mul(PHYSICAL_NODE_RECORD_SIZE)
            .ok_or(V2AvlError::Overflow(
                "history physical delta node offset exceeds u64",
            ))?;
        let start_usize = usize::try_from(start)
            .map_err(|_| V2AvlError::Overflow("v2 payload end exceeds usize"))?;
        let end = start_usize
            .checked_add(PHYSICAL_NODE_RECORD_SIZE as usize)
            .ok_or(V2AvlError::Overflow("v2 payload end exceeds usize"))?;
        let bytes = self
            .node_delta
            .get(start_usize..end)
            .ok_or(V2AvlError::Invalid(
                "history physical delta node is outside its range",
            ))?;
        decode_v2_node(bytes).map_err(V2AvlError::from)
    }

    fn load_delta_record_struct(&self, node_id: u64) -> Result<ArenaNode, V2AvlError> {
        let record = self.load_delta_record(node_id)?;
        build_delta_arena_node(self, node_id, record)
    }
}

/// Builds an arena node for a fresh delta record. Children resolve from the
/// same delta when fresh, else from committed files — enforcing the rule
/// that fresh branches reference only old committed nodes or earlier fresh
/// nodes from the same prepared delta.
fn build_delta_arena_node(
    view: &DeltaView<'_>,
    node_id: u64,
    record: V2NodeRecord,
) -> Result<ArenaNode, V2AvlError> {
    use super::image_v2::{v2_node_fields, V2NodeFields};
    match v2_node_fields(record)? {
        V2NodeFields::Leaf {
            payload_offset,
            payload_len,
        } => {
            let available_end = view
                .base_payload_end
                .checked_add(view.payload_delta.len() as u64)
                .ok_or(V2AvlError::Overflow(
                    "history physical payload frontier exceeds u64",
                ))?;
            let payload_end = payload_offset
                .checked_add(payload_len)
                .ok_or(V2AvlError::Overflow("v2 leaf payload range exceeds u64"))?;
            if payload_end > available_end {
                return Err(V2AvlError::Invalid(
                    "history physical delta leaf references payload outside its range",
                ));
            }
            Ok(ArenaNode::Leaf {
                payload_offset,
                payload_len,
                record,
            })
        }
        V2NodeFields::Branch {
            left_node_id,
            right_node_id,
            left_len,
        } => {
            if left_node_id >= node_id || right_node_id >= node_id {
                return Err(V2AvlError::Invalid(
                    "history physical delta branch child must reference an earlier node",
                ));
            }
            let child_record = |id: u64| {
                if id < view.base_node_count {
                    view.base.load_record(id)
                } else {
                    view.load_delta_record(id)
                }
            };
            let left = V2RootRecord::from_node(left_node_id, child_record(left_node_id)?)?;
            let right = V2RootRecord::from_node(right_node_id, child_record(right_node_id)?)?;
            if left.logical_len() != left_len {
                return Err(V2AvlError::Invalid(
                    "history physical delta branch left length disagrees with its child",
                ));
            }
            Ok(ArenaNode::Branch {
                left,
                right,
                record,
            })
        }
    }
}

impl ArenaStore for DeltaView<'_> {
    fn arena_payload_len(&self) -> usize {
        usize::try_from(
            self.base_payload_end
                .checked_add(self.payload_delta.len() as u64)
                .unwrap_or(u64::MAX),
        )
        .unwrap_or(usize::MAX)
    }

    fn arena_node_count(&self) -> usize {
        usize::try_from(
            self.base_node_count
                .checked_add(self.delta_node_count())
                .unwrap_or(u64::MAX),
        )
        .unwrap_or(usize::MAX)
    }

    fn arena_load_node(&self, node_id: u64) -> Result<ArenaNode, V2AvlError> {
        if node_id < self.base_node_count {
            let record = self.base.load_record(node_id)?;
            build_arena_node(self.base, node_id, record)
        } else {
            self.load_delta_record_struct(node_id)
        }
    }

    fn arena_read_payload(&self, start: u64, end: u64) -> Result<Vec<u8>, V2AvlError> {
        if end <= self.base_payload_end {
            return self.base.read_payload_range(start, end);
        }
        if start >= self.base_payload_end {
            let base = self.base_payload_end;
            let from = usize::try_from(start - base)
                .map_err(|_| V2AvlError::Overflow("v2 payload start exceeds usize"))?;
            let upto = usize::try_from(end - base)
                .map_err(|_| V2AvlError::Overflow("v2 payload end exceeds usize"))?;
            let bytes = self
                .payload_delta
                .get(from..upto)
                .ok_or(V2AvlError::Invalid(
                    "history physical delta payload range is outside its range",
                ))?;
            // Delta bytes are memory-held preparation state, not file
            // content: no physical read is counted for them.
            return Ok(bytes.to_vec());
        }
        // A range spanning the commit boundary cannot arise from valid
        // traversal (every leaf extent is allocated wholly old or wholly
        // fresh), but serve it defensively rather than failing.
        let mut first = self.base.read_payload_range(start, self.base_payload_end)?;
        let second = self.arena_read_payload(self.base_payload_end, end)?;
        first.extend_from_slice(&second);
        Ok(first)
    }

    fn arena_alias_payload(&mut self, start: u64, end: u64) -> Result<(u64, Vec<u8>), V2AvlError> {
        let bytes = self.arena_read_payload(start, end)?;
        Ok((start, bytes))
    }

    fn arena_append_payload(&mut self, bytes: &[u8]) -> Result<u64, V2AvlError> {
        let offset = self
            .base_payload_end
            .checked_add(self.payload_delta.len() as u64)
            .ok_or(V2AvlError::Overflow(
                "history physical payload frontier exceeds u64",
            ))?;
        self.payload_delta.extend_from_slice(bytes);
        Ok(offset)
    }

    fn arena_push_node(&mut self, node: ArenaNode) -> Result<u64, V2AvlError> {
        let node_id = self
            .base_node_count
            .checked_add(self.delta_node_count())
            .ok_or(V2AvlError::Overflow("v2 node arena length exceeds u64"))?;
        self.node_delta
            .extend_from_slice(&encode_v2_node(node.record()));
        Ok(node_id)
    }

    fn arena_truncate(&mut self, payload_len: usize, node_count: usize) {
        let base_payload = usize::try_from(self.base_payload_end).unwrap_or(usize::MAX);
        let base_nodes = usize::try_from(self.base_node_count).unwrap_or(usize::MAX);
        self.payload_delta
            .truncate(payload_len.saturating_sub(base_payload));
        let keep_records = node_count.saturating_sub(base_nodes);
        self.node_delta
            .truncate(keep_records.saturating_mul(PHYSICAL_NODE_RECORD_SIZE as usize));
    }
}

/// One prepared physical splice delta: fresh payload bytes plus fresh
/// canonical node records realizing the result root, with the frontiers
/// they were prepared against and the digest binding the exact byte
/// realization. Owned buffers: preparation never mutates files.
#[derive(Debug, Clone)]
pub(crate) struct PhysicalDelta {
    old_payload_end: u64,
    old_node_count: u64,
    new_payload_end: u64,
    new_node_count: u64,
    payload: Vec<u8>,
    nodes: Vec<u8>,
    root: V2RootRecord,
    delta_digest: [u8; 32],
    allocated_nodes: usize,
    inspected_nodes: usize,
    payload_bytes_allocated: usize,
}

impl PhysicalDelta {
    pub(crate) fn old_payload_end(&self) -> u64 {
        self.old_payload_end
    }

    pub(crate) fn old_node_count(&self) -> u64 {
        self.old_node_count
    }

    pub(crate) fn new_payload_end(&self) -> u64 {
        self.new_payload_end
    }

    pub(crate) fn new_node_count(&self) -> u64 {
        self.new_node_count
    }

    pub(crate) fn payload_bytes(&self) -> &[u8] {
        &self.payload
    }

    pub(crate) fn node_bytes(&self) -> &[u8] {
        &self.nodes
    }

    pub(crate) fn root_node_id(&self) -> u64 {
        self.root.node_id()
    }

    pub(crate) fn root_record_bytes(&self) -> [u8; super::format_v2::V2_ROOT_RECORD_SIZE] {
        super::format_v2::encode_v2_root(self.root)
    }

    pub(crate) fn delta_digest(&self) -> [u8; 32] {
        self.delta_digest
    }

    pub(crate) fn allocated_nodes(&self) -> usize {
        self.allocated_nodes
    }

    pub(crate) fn inspected_nodes(&self) -> usize {
        self.inspected_nodes
    }

    pub(crate) fn payload_bytes_allocated(&self) -> usize {
        self.payload_bytes_allocated
    }
}

/// Computes the physical delta digest: SHA256 over the staging domain, the
/// physical generation, old/new frontiers, the exact fresh payload bytes,
/// the exact encoded fresh node records, and the result root record. The
/// logical operation digest is computed separately over semantic coordinates
/// only and stays physical-coordinate free.
pub(crate) fn physical_delta_digest(
    generation: u64,
    old_payload_end: u64,
    new_payload_end: u64,
    old_node_count: u64,
    new_node_count: u64,
    payload: &[u8],
    nodes: &[u8],
    result_root: &[u8; super::format_v2::V2_ROOT_RECORD_SIZE],
) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(PHYSICAL_DELTA_DIGEST_DOMAIN);
    hasher.update(generation.to_le_bytes());
    hasher.update(old_payload_end.to_le_bytes());
    hasher.update(new_payload_end.to_le_bytes());
    hasher.update(old_node_count.to_le_bytes());
    hasher.update(new_node_count.to_le_bytes());
    hasher.update(payload);
    hasher.update(nodes);
    hasher.update(result_root);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

/// Runs the shared splice algorithm against a preparation view and validates
/// the resulting delta locally (E6.14): fresh node identifiers cover exactly
/// `[node_start..node_end)`, fresh payload covers exactly
/// `[payload_start..payload_end)`, every fresh leaf references valid immutable
/// payload in the combined committed-plus-fresh arena, every fresh branch
/// references an old committed node or an earlier fresh node, every record
/// decodes canonically, the result root resolves, and its length and
/// commitment match the prepared semantic result. No file mutation occurs.
pub(super) fn prepare_physical_delta(
    base: &PhysicalContentStore,
    parent: Option<V2RootRecord>,
    offset: u64,
    delete_len: u64,
    insert: &[u8],
    result_len: u64,
) -> Result<PhysicalDelta, V2AvlError> {
    let mut view = DeltaView::new(base);
    let outcome =
        super::avl::V2AvlSequence::splice_on(&mut view, parent, offset, delete_len, insert)?;
    let root = outcome.root();
    if root.logical_len() != result_len {
        return Err(V2AvlError::Invalid(
            "history physical splice result length disagrees with its root",
        ));
    }
    let (payload, nodes) = view.finish();
    let old_payload_end = base.committed_payload_end;
    let old_node_count = base.committed_node_count;
    let fresh_payload = u64::try_from(payload.len())
        .map_err(|_| V2AvlError::Overflow("history physical delta payload exceeds u64"))?;
    if nodes.len() as u64 % PHYSICAL_NODE_RECORD_SIZE != 0 {
        return Err(V2AvlError::Invalid(
            "history physical delta is not record-aligned",
        ));
    }
    let fresh_nodes = nodes.len() as u64 / PHYSICAL_NODE_RECORD_SIZE;
    let new_payload_end =
        old_payload_end
            .checked_add(fresh_payload)
            .ok_or(V2AvlError::Overflow(
                "history physical payload frontier exceeds u64",
            ))?;
    let new_node_count = old_node_count
        .checked_add(fresh_nodes)
        .ok_or(V2AvlError::Overflow(
            "history physical node frontier exceeds u64",
        ))?;
    // Local delta validation before the digest binds anything.
    validate_fresh_delta(
        base,
        old_payload_end,
        new_payload_end,
        old_node_count,
        new_node_count,
        &payload,
        &nodes,
        root,
    )?;
    let root_bytes = super::format_v2::encode_v2_root(root);
    let delta_digest = physical_delta_digest(
        base.generation,
        old_payload_end,
        new_payload_end,
        old_node_count,
        new_node_count,
        &payload,
        &nodes,
        &root_bytes,
    );
    Ok(PhysicalDelta {
        old_payload_end,
        old_node_count,
        new_payload_end,
        new_node_count,
        payload,
        nodes,
        root,
        delta_digest,
        allocated_nodes: outcome.allocated_nodes(),
        inspected_nodes: outcome.inspected_nodes(),
        payload_bytes_allocated: outcome.payload_bytes_allocated(),
    })
}

/// Validates fresh delta buffers against their claimed frontiers without
/// touching files: every record decodes canonically, identifiers tile the
/// fresh range densely, leaves land inside fresh payload, branches reference
/// only committed-or-earlier-fresh nodes, and the result root resolves
/// inside the combined range with matching length and commitment.
fn validate_fresh_delta(
    base: &PhysicalContentStore,
    old_payload_end: u64,
    new_payload_end: u64,
    old_node_count: u64,
    new_node_count: u64,
    payload: &[u8],
    nodes: &[u8],
    root: V2RootRecord,
) -> Result<(), V2AvlError> {
    use super::image_v2::{v2_node_fields, V2NodeFields};
    if new_payload_end < old_payload_end || new_node_count < old_node_count {
        return Err(V2AvlError::Invalid(
            "history physical delta frontiers move backwards",
        ));
    }
    if payload.len() as u64 != new_payload_end - old_payload_end {
        return Err(V2AvlError::Invalid(
            "history physical delta payload disagrees with its frontiers",
        ));
    }
    let fresh_nodes = new_node_count - old_node_count;
    if nodes.len() as u64 != fresh_nodes * PHYSICAL_NODE_RECORD_SIZE {
        return Err(V2AvlError::Invalid(
            "history physical delta nodes disagree with their frontiers",
        ));
    }
    for index in 0..fresh_nodes {
        let node_id = old_node_count + index;
        let start = (index * PHYSICAL_NODE_RECORD_SIZE) as usize;
        let record = decode_v2_node(
            nodes
                .get(start..start + PHYSICAL_NODE_RECORD_SIZE as usize)
                .ok_or(V2AvlError::Invalid(
                    "history physical delta node is outside its range",
                ))?,
        )
        .map_err(V2AvlError::from)?;
        match v2_node_fields(record)? {
            V2NodeFields::Leaf {
                payload_offset,
                payload_len,
            } => {
                let leaf_end = payload_offset
                    .checked_add(payload_len)
                    .ok_or(V2AvlError::Overflow("v2 leaf payload range exceeds u64"))?;
                if leaf_end > new_payload_end {
                    return Err(V2AvlError::Invalid(
                        "history physical delta leaf references payload outside its range",
                    ));
                }
                let bytes = if leaf_end <= old_payload_end {
                    base.read_payload_range(payload_offset, leaf_end)?
                } else if payload_offset >= old_payload_end {
                    let start = usize::try_from(payload_offset - old_payload_end)
                        .map_err(|_| V2AvlError::Overflow("v2 payload start exceeds usize"))?;
                    let end = usize::try_from(leaf_end - old_payload_end)
                        .map_err(|_| V2AvlError::Overflow("v2 payload end exceeds usize"))?;
                    payload
                        .get(start..end)
                        .ok_or(V2AvlError::Invalid(
                            "history physical delta leaf references payload outside its range",
                        ))?
                        .to_vec()
                } else {
                    let mut bytes = base.read_payload_range(payload_offset, old_payload_end)?;
                    let fresh_end = usize::try_from(leaf_end - old_payload_end)
                        .map_err(|_| V2AvlError::Overflow("v2 payload end exceeds usize"))?;
                    bytes.extend_from_slice(payload.get(..fresh_end).ok_or(
                        V2AvlError::Invalid(
                            "history physical delta leaf references payload outside its range",
                        ),
                    )?);
                    bytes
                };
                let expected = V2NodeRecord::leaf(payload_offset, &bytes)?;
                if expected != record {
                    return Err(V2AvlError::Invalid(
                        "history physical delta leaf verification failed",
                    ));
                }
            }
            V2NodeFields::Branch {
                left_node_id,
                right_node_id,
                ..
            } => {
                if left_node_id >= node_id || right_node_id >= node_id {
                    return Err(V2AvlError::Invalid(
                        "history physical delta branch references a future node",
                    ));
                }
                if left_node_id >= new_node_count || right_node_id >= new_node_count {
                    return Err(V2AvlError::Invalid(
                        "history physical delta branch references an unknown node",
                    ));
                }
            }
        }
    }
    // The result root resolves inside the combined committed-plus-fresh
    // range with matching length and commitment.
    if root.node_id() >= new_node_count {
        return Err(V2AvlError::Invalid(
            "history physical delta result root is outside its range",
        ));
    }
    let root_record = if root.node_id() < old_node_count {
        base.load_record(root.node_id())?
    } else {
        let start = ((root.node_id() - old_node_count) * PHYSICAL_NODE_RECORD_SIZE) as usize;
        decode_v2_node(
            nodes
                .get(start..start + PHYSICAL_NODE_RECORD_SIZE as usize)
                .ok_or(V2AvlError::Invalid(
                    "history physical delta result root is outside its range",
                ))?,
        )
        .map_err(V2AvlError::from)?
    };
    let canonical = V2RootRecord::from_node(root.node_id(), root_record)?;
    if canonical != root {
        return Err(V2AvlError::Invalid(
            "history physical delta result root disagrees with its record",
        ));
    }
    Ok(())
}

/// Validates one committed physical delta during THL5 replay directly
/// against file bytes: frontier contiguity (this frame must extend the
/// current frontier exactly — no gaps, no overlaps), canonical record
/// decodes, the E6.14 local checks over file ranges, result-root resolution
/// with matching length and commitment, and the delta digest. Only
/// `O(delta + log n)` file reads occur; the parent state is never scanned.
pub(super) fn validate_committed_delta(
    store: &PhysicalContentStore,
    generation: u64,
    payload_start: u64,
    payload_end: u64,
    node_start: u64,
    node_end: u64,
    delta_digest: [u8; 32],
    result_root: &[u8; super::format_v2::V2_ROOT_RECORD_SIZE],
    result_len: u64,
) -> Result<V2RootRecord, V2AvlError> {
    use super::format_v2::decode_v2_root;
    if generation != store.generation {
        return Err(V2AvlError::Invalid(
            "history log splice physical generation disagrees with authority",
        ));
    }
    // Contiguity first: the frame must extend the replay frontier exactly.
    if payload_start != store.committed_payload_end || node_start != store.committed_node_count {
        return Err(V2AvlError::Invalid(
            "history log splice physical frontiers disagree with replay position",
        ));
    }
    if payload_end < payload_start || node_end < node_start {
        return Err(V2AvlError::Invalid(
            "history log splice physical frontiers move backwards",
        ));
    }
    let root = decode_v2_root(result_root).map_err(V2AvlError::from)?;
    if root.logical_len() != result_len {
        return Err(V2AvlError::Invalid(
            "history log splice result length disagrees with its physical root",
        ));
    }
    // The delta bytes live past the committed frontier (orphan until this
    // frame adopts them): read them positionally without trusting any
    // in-memory view.
    let payload_len = payload_end - payload_start;
    let payload_len_usize = usize::try_from(payload_len)
        .map_err(|_| V2AvlError::Overflow("v2 payload end exceeds usize"))?;
    let node_records = node_end - node_start;
    let nodes_len =
        node_records
            .checked_mul(PHYSICAL_NODE_RECORD_SIZE)
            .ok_or(V2AvlError::Overflow(
                "history physical node range exceeds u64",
            ))?;
    let nodes_len_usize = usize::try_from(nodes_len)
        .map_err(|_| V2AvlError::Overflow("v2 payload end exceeds usize"))?;
    let payload_offset =
        PHYSICAL_HEADER_SIZE
            .checked_add(payload_start)
            .ok_or(V2AvlError::Overflow(
                "history physical payload offset exceeds u64",
            ))?;
    let nodes_offset = PHYSICAL_HEADER_SIZE
        .checked_add(node_start.checked_mul(PHYSICAL_NODE_RECORD_SIZE).ok_or(
            V2AvlError::Overflow("history physical node offset exceeds u64"),
        )?)
        .ok_or(V2AvlError::Overflow(
            "history physical node offset exceeds u64",
        ))?;
    let mut payload = Vec::new();
    payload
        .try_reserve_exact(payload_len_usize)
        .map_err(|_| V2AvlError::Capacity("history physical delta validation allocation failed"))?;
    payload.resize(payload_len_usize, 0);
    read_exact_at(store.payload_file()?, payload_offset, &mut payload)
        .map_err(|_| V2AvlError::Invalid("history log splice physical payload is absent"))?;
    store.ledger.count_payload_read(payload_len);
    let mut node_bytes = Vec::new();
    node_bytes
        .try_reserve_exact(nodes_len_usize)
        .map_err(|_| V2AvlError::Capacity("history physical delta validation allocation failed"))?;
    node_bytes.resize(nodes_len_usize, 0);
    read_exact_at(store.nodes_file()?, nodes_offset, &mut node_bytes)
        .map_err(|_| V2AvlError::Invalid("history log splice physical nodes are absent"))?;
    store.ledger.count_node_read(nodes_len);
    // Same local checks as preparation, over the file bytes.
    validate_fresh_delta(
        store,
        payload_start,
        payload_end,
        node_start,
        node_end,
        &payload,
        &node_bytes,
        root,
    )?;
    // The digest binds the exact realization independently of the semantic
    // operation digest.
    let recomputed = physical_delta_digest(
        generation,
        payload_start,
        payload_end,
        node_start,
        node_end,
        &payload,
        &node_bytes,
        result_root,
    );
    if recomputed != delta_digest {
        return Err(V2AvlError::Invalid(
            "history log splice physical delta digest mismatch",
        ));
    }
    Ok(root)
}

/// Compact retained content into a brand-new physical generation, reusing
/// the generic compaction core byte-for-byte (plan, repack, rebuild): the
/// same functions E5 uses in memory, here fed from file-loaded tables.
/// Quiescent maintenance may use `O(retained)` RAM; foreground paths never
/// call this. The new files are synced before return; publication of the
/// metadata snapshot referencing them stays the caller's ordered protocol.
pub(super) struct PhysicalGcBuild {
    pub(super) store: PhysicalContentStore,
    pub(super) roots: Vec<V2RootRecord>,
    pub(super) nodes_before: u64,
    pub(super) nodes_after: u64,
    pub(super) payload_bytes_before: u64,
    pub(super) payload_bytes_after: u64,
}

pub(super) fn compact_physical_generation(
    source: &PhysicalContentStore,
    seeds: &[V2RootRecord],
    new_generation: u64,
    dir: &Path,
    ledger: &IoLedger,
) -> Result<PhysicalGcBuild, V2AvlError> {
    // Validate every seed against committed records before any plan work.
    let mut seed_ids: Vec<u64> = Vec::new();
    seed_ids
        .try_reserve_exact(seeds.len())
        .map_err(|_| V2AvlError::Capacity("history physical GC seed allocation failed"))?;
    for root in seeds {
        let record = source.load_record(root.node_id())?;
        let canonical = V2RootRecord::from_node(root.node_id(), record)?;
        if canonical != *root {
            return Err(V2AvlError::Invalid(
                "history physical GC seed disagrees with its record",
            ));
        }
        seed_ids.push(root.node_id());
    }
    let records = source.load_all_records()?;
    let payload = source.read_all_payload()?;
    let nodes_before = source.committed_node_count;
    let payload_bytes_before = source.committed_payload_end;
    let (retained, ranges) = plan_reachable_arena(&payload, &records, &seed_ids)?;
    let (compact_payload, payload_mapping) = repack_compact_ranges(&payload, &ranges)?;
    let (compact_records, node_mapping) =
        rebuild_compact_records(&records, &retained, &compact_payload, &payload_mapping)?;
    if compact_records.len() != retained.len() {
        return Err(V2AvlError::Invalid(
            "history physical GC node table disagrees with its retained set",
        ));
    }
    // Write the new generation: header, dense payload, dense records.
    let mut fresh = PhysicalContentStore::recreate(dir, new_generation, ledger)?;
    const GC_WRITE_FAILED: V2AvlError =
        V2AvlError::Invalid("history physical GC generation write failed");
    // The recreate handles stay open for the store lifetime: reborrow them
    // mutably for the body writes.
    if !compact_payload.is_empty() {
        let payload_file = fresh.payload.as_mut().ok_or(GC_WRITE_FAILED)?;
        payload_file
            .seek(SeekFrom::Start(PHYSICAL_HEADER_SIZE))
            .map_err(|_| GC_WRITE_FAILED)?;
        payload_file
            .write_all(&compact_payload)
            .map_err(|_| GC_WRITE_FAILED)?;
        ledger.count_payload_written(compact_payload.len() as u64);
    }
    if !compact_records.is_empty() {
        let nodes_file = fresh.nodes.as_mut().ok_or(GC_WRITE_FAILED)?;
        nodes_file
            .seek(SeekFrom::Start(PHYSICAL_HEADER_SIZE))
            .map_err(|_| GC_WRITE_FAILED)?;
        for record in &compact_records {
            let encoded = encode_v2_node(*record);
            nodes_file
                .write_all(&encoded)
                .map_err(|_| GC_WRITE_FAILED)?;
        }
        ledger.count_node_written(compact_records.len() as u64 * PHYSICAL_NODE_RECORD_SIZE);
    }
    fresh.sync_content()?;
    let nodes_after = compact_records.len() as u64;
    let payload_bytes_after = compact_payload.len() as u64;
    fresh.committed_payload_end = payload_bytes_after;
    fresh.committed_node_count = nodes_after;
    // Remap the seed roots into the compact coordinate space, rechecking
    // content identity across relocation exactly like the memory compactor.
    let mut roots = Vec::new();
    roots
        .try_reserve_exact(seeds.len())
        .map_err(|_| V2AvlError::Capacity("history physical GC root table allocation failed"))?;
    for root in seeds {
        let new_id = remapped_node_id(&node_mapping, root.node_id())?;
        let index = usize::try_from(new_id).map_err(|_| {
            V2AvlError::Overflow("history physical GC node identifier exceeds usize")
        })?;
        let record = compact_records.get(index).ok_or(V2AvlError::Invalid(
            "history physical GC remapped root is absent",
        ))?;
        roots.push(V2RootRecord::from_node(new_id, *record)?);
    }
    for (old, new) in seeds.iter().zip(roots.iter()) {
        if old.commitment() != new.commitment() || old.logical_len() != new.logical_len() {
            return Err(V2AvlError::Invalid(
                "history physical GC root disagrees with its source",
            ));
        }
    }
    Ok(PhysicalGcBuild {
        store: fresh,
        roots,
        nodes_before,
        nodes_after,
        payload_bytes_before,
        payload_bytes_after,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir() -> tempfile::TempDir {
        tempfile::TempDir::new().unwrap()
    }

    fn test_ledger() -> IoLedger {
        IoLedger::new()
    }

    #[test]
    fn content_headers_round_trip_and_reject_forgery() {
        let payload = encode_header(PHYSICAL_PAYLOAD_MAGIC, 7);
        assert_eq!(decode_header(PHYSICAL_PAYLOAD_MAGIC, &payload).unwrap(), 7);
        let nodes = encode_header(PHYSICAL_NODE_MAGIC, 7);
        assert_eq!(decode_header(PHYSICAL_NODE_MAGIC, &nodes).unwrap(), 7);
        // Crossed magics fail.
        assert!(decode_header(PHYSICAL_PAYLOAD_MAGIC, &nodes).is_err());
        assert!(decode_header(PHYSICAL_NODE_MAGIC, &payload).is_err());
        // Generation is carried (the open path checks it against the
        // expected authority generation); reserved bytes must be zero.
        let mut other_gen = payload;
        other_gen[4] ^= 0xFF;
        assert_eq!(
            decode_header(PHYSICAL_PAYLOAD_MAGIC, &other_gen).unwrap(),
            7 ^ 0xFF
        );
        let mut bad_reserved = payload;
        bad_reserved[15] = 1;
        assert!(decode_header(PHYSICAL_PAYLOAD_MAGIC, &bad_reserved).is_err());
    }

    #[test]
    fn content_filename_grammar_matches_only_canonical_names() {
        assert_eq!(
            content_payload_filename(4),
            "content-00000000000000000004.payload"
        );
        assert_eq!(
            content_nodes_filename(4),
            "content-00000000000000000004.nodes"
        );
        for superseded in [
            "content-00000000000000000003.payload",
            "content-00000000000000000003.nodes",
        ] {
            assert!(
                superseded_physical_content_file(superseded, 4),
                "{superseded} must match"
            );
        }
        for current in [
            "content-00000000000000000004.payload",
            "content-00000000000000000004.nodes",
        ] {
            assert!(
                !superseded_physical_content_file(current, 4),
                "{current} is current and must survive"
            );
        }
        // Unpadded spellings, wrong extensions, and prefix lookalikes never
        // match: only the exact canonical form is ever deleted.
        for unrelated in [
            "content-4.payload",
            "content-4.nodes",
            "content-notes.payload",
            "content-backup.nodes",
            "content-abc.payload",
            "content-00000000000000000004.payload.tmp",
            "content-00000000000000000004.wal",
            "history-00000000000000000004.wal",
            "user-data.bin",
        ] {
            assert!(
                !superseded_physical_content_file(unrelated, 4),
                "{unrelated} must never match"
            );
        }
    }

    #[test]
    fn physical_delta_digest_is_deterministic_and_sensitive() {
        let root = [0xABu8; 56];
        let first = physical_delta_digest(3, 0, 10, 0, 2, b"0123456789", b"nodes", &root);
        assert_eq!(
            first,
            physical_delta_digest(3, 0, 10, 0, 2, b"0123456789", b"nodes", &root)
        );
        // Every input is load-bearing.
        assert_ne!(
            first,
            physical_delta_digest(4, 0, 10, 0, 2, b"0123456789", b"nodes", &root)
        );
        assert_ne!(
            first,
            physical_delta_digest(3, 0, 11, 0, 2, b"0123456789", b"nodes", &root)
        );
        assert_ne!(
            first,
            physical_delta_digest(3, 0, 10, 0, 2, b"012345678X", b"nodes", &root)
        );
        assert_ne!(
            first,
            physical_delta_digest(3, 0, 10, 0, 2, b"0123456789", b"nodes", &[0xACu8; 56])
        );
    }

    #[test]
    fn ledger_overflow_pins_and_flags_without_affecting_storage() {
        let ledger = test_ledger();
        ledger.update(|bucket, overflow| {
            IoLedger::bump(&mut bucket.payload_bytes_written, u64::MAX, overflow);
            assert!(!*overflow);
            IoLedger::bump(&mut bucket.payload_bytes_written, 1, overflow);
            assert!(*overflow);
            assert_eq!(bucket.payload_bytes_written, u64::MAX);
        });
        assert!(ledger.snapshot().overflow);
        // Reset clears the flag for the next measurement window.
        ledger.reset();
        assert!(!ledger.snapshot().overflow);
        assert_eq!(ledger.snapshot().foreground.payload_bytes_written, 0);
    }

    #[test]
    fn ledger_scopes_isolate_foreground_from_maintenance() {
        let ledger = test_ledger();
        ledger.count_payload_written(100);
        {
            let _guard = ledger.scoped(OpScope::Maintenance);
            ledger.count_payload_written(1_000_000);
        }
        ledger.count_payload_written(4);
        let snapshot = ledger.snapshot();
        assert_eq!(snapshot.foreground.payload_bytes_written, 104);
        assert_eq!(snapshot.maintenance.payload_bytes_written, 1_000_000);
        assert_eq!(snapshot.reopen.payload_bytes_written, 0);
        assert_eq!(snapshot.publication.payload_bytes_written, 0);
    }

    /// Prepares a delta over committed files and commits it, proving the
    /// shared algorithm runs identically against the delta view: the same
    /// bytes the memory arena would hold land in the files.
    fn prepare_and_commit(
        store: &mut PhysicalContentStore,
        parent: Option<V2RootRecord>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
        result_len: u64,
    ) -> PhysicalDelta {
        let delta =
            prepare_physical_delta(store, parent, offset, delete_len, insert, result_len).unwrap();
        store.append_payload_delta(delta.payload_bytes()).unwrap();
        store.append_node_delta(delta.node_bytes()).unwrap();
        store.sync_content().unwrap();
        let (payload_end, node_count) = (delta.new_payload_end(), delta.new_node_count());
        store.adopt_frontiers(payload_end, node_count);
        delta
    }

    #[test]
    fn delta_prepare_commit_replays_exact_content() {
        let temp = test_dir();
        let ledger = test_ledger();
        let mut store =
            PhysicalContentStore::create_new(temp.path(), 0, &ledger).expect("create must work");
        let root_bytes = b"hello world, this is a base state";
        let first = prepare_and_commit(&mut store, None, 0, 0, root_bytes, root_bytes.len() as u64);
        // Middle replacement: bounded fresh bytes plus path nodes only.
        let mut expected = root_bytes.to_vec();
        expected.splice(6..11, b"EARTH".to_vec());
        let parent = V2RootRecord::from_node(
            first.root_node_id(),
            store.load_record(first.root_node_id()).unwrap(),
        )
        .unwrap();
        let second = prepare_and_commit(
            &mut store,
            Some(parent),
            6,
            5,
            b"EARTH",
            expected.len() as u64,
        );
        assert!(second.new_payload_end() > first.new_payload_end());
        // The committed delta validates against file bytes: generation,
        // contiguity, canonical records, root resolution, and digest.
        let placement_root = second.root_record_bytes();
        let mut checker = PhysicalContentStore::open_existing(
            temp.path(),
            0,
            first.new_payload_end(),
            first.new_node_count(),
            &ledger,
            false,
        )
        .unwrap();
        let validated = validate_committed_delta(
            &checker,
            0,
            second.old_payload_end(),
            second.new_payload_end(),
            second.old_node_count(),
            second.new_node_count(),
            second.delta_digest(),
            &placement_root,
            expected.len() as u64,
        )
        .unwrap();
        assert_eq!(validated.node_id(), second.root_node_id());
        checker.adopt_frontiers(second.new_payload_end(), second.new_node_count());
        // Full content reads exact through the shared range-read core.
        let got = super::super::avl::V2AvlSequence::read_range_counted_on(
            &checker,
            validated,
            0,
            expected.len() as u64,
        )
        .unwrap()
        .0;
        assert_eq!(got, expected);
    }

    #[test]
    fn empty_delta_from_boundary_delete_commits_cleanly() {
        // Deleting a whole leaf-aligned suffix reuses the old root with zero
        // fresh bytes: the empty delta must commit, digest, and validate.
        let temp = test_dir();
        let ledger = test_ledger();
        let mut store =
            PhysicalContentStore::create_new(temp.path(), 0, &ledger).expect("create must work");
        // Two full leaves (bound is 16 KiB): deleting exactly the second
        // leaf at its boundary copies nothing and allocates nothing — the
        // surviving side is returned as-is.
        let big = vec![0x5Au8; 32768];
        let base = prepare_and_commit(&mut store, None, 0, 0, &big, 32768);
        let parent = V2RootRecord::from_node(
            base.root_node_id(),
            store.load_record(base.root_node_id()).unwrap(),
        )
        .unwrap();
        let halve = prepare_physical_delta(&store, Some(parent), 16384, 16384, b"", 16384).unwrap();
        assert_eq!(halve.new_payload_end() - halve.old_payload_end(), 0);
        assert_eq!(halve.new_node_count() - halve.old_node_count(), 0);
        // The empty delta still digests and validates: frontier equality plus
        // the old root, which resolves inside the committed range.
        let placement_root = halve.root_record_bytes();
        let validated = validate_committed_delta(
            &store,
            0,
            halve.old_payload_end(),
            halve.new_payload_end(),
            halve.old_node_count(),
            halve.new_node_count(),
            halve.delta_digest(),
            &placement_root,
            16384,
        )
        .unwrap();
        assert_eq!(validated.node_id(), halve.root_node_id());
    }

    #[test]
    fn open_rejects_short_files_and_wrong_generation() {
        let temp = test_dir();
        let ledger = test_ledger();
        let mut store =
            PhysicalContentStore::create_new(temp.path(), 0, &ledger).expect("create must work");
        prepare_and_commit(&mut store, None, 0, 0, b"data", 4);
        // Truncated payload fails closed.
        {
            let path = temp.path().join(content_payload_filename(0));
            let len = std::fs::metadata(&path).unwrap().len();
            std::fs::write(&path, vec![0u8; (len - 1) as usize]).unwrap();
            assert!(
                PhysicalContentStore::open_existing(temp.path(), 0, 4, 1, &ledger, false).is_err()
            );
        }
        // Wrong generation identity fails closed.
        {
            let path = temp.path().join(content_nodes_filename(0));
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[4..12].copy_from_slice(&9u64.to_le_bytes());
            std::fs::write(&path, &bytes).unwrap();
            assert!(
                PhysicalContentStore::open_existing(temp.path(), 0, 4, 1, &ledger, false).is_err()
            );
        }
    }

    #[test]
    fn orphan_tail_is_ignored_read_only_and_healed_writable() {
        let temp = test_dir();
        let ledger = test_ledger();
        let mut store =
            PhysicalContentStore::create_new(temp.path(), 0, &ledger).expect("create must work");
        prepare_and_commit(&mut store, None, 0, 0, b"data", 4);
        let (payload_end, node_count) =
            (store.committed_payload_end(), store.committed_node_count());
        // Orphan bytes past the frontier: no metadata authority references them.
        {
            let path = temp.path().join(content_payload_filename(0));
            let mut bytes = std::fs::read(&path).unwrap();
            bytes.extend_from_slice(&[0xFFu8; 1024]);
            std::fs::write(&path, &bytes).unwrap();
        }
        // Read-only open ignores the tail: committed reads stay exact.
        let readonly = PhysicalContentStore::open_existing(
            temp.path(),
            0,
            payload_end,
            node_count,
            &ledger,
            false,
        )
        .unwrap();
        assert_eq!(readonly.read_payload_range(0, 4).unwrap(), b"data");
        // Writable open heals before mutation.
        let mut writable = PhysicalContentStore::open_existing(
            temp.path(),
            0,
            payload_end,
            node_count,
            &ledger,
            true,
        )
        .unwrap();
        writable.heal_for_write().unwrap();
        let payload_len = std::fs::metadata(temp.path().join(content_payload_filename(0)))
            .unwrap()
            .len();
        assert_eq!(payload_len, 16 + payload_end);
    }

    #[test]
    fn fault_arms_reject_without_mutating_frontiers() {
        let temp = test_dir();
        let ledger = test_ledger();
        let mut store =
            PhysicalContentStore::create_new(temp.path(), 0, &ledger).expect("create must work");
        store.arm_fail_next_payload_append();
        assert!(store.append_payload_delta(b"xx").is_err());
        assert_eq!(store.committed_payload_end(), 0);
        store.arm_fail_next_node_append();
        assert!(store.append_node_delta(&[0u8; 72]).is_err());
        assert_eq!(store.committed_node_count(), 0);
        store.arm_fail_next_sync();
        assert!(store.sync_content().is_err());
    }
}
