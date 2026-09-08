//! Internal persistent-sequence contract for locality-sensitive checkpoint data.
//!
//! This module is the seam between checkpoint semantics and the physical node
//! representation. It is intentionally representation-neutral: Format v1 can
//! be adapted behind this contract without changing its bytes, while a future
//! balanced format can provide stored subtree lengths and stronger locality.
//!
//! Design correspondence:
//! `Vedsaga/Tulya-MDL-Lean/formal/Tulya/Incremental/PersistentAVLFinalAPI.lean`
//! exposes persistent edit, random-access, historical-preservation, and work
//! bounds that inform this contract. The Rust implementation is not currently
//! mechanically proved to refine that Lean model.
//!
//! The production target also requires append, bounded streaming, and verify
//! operations. They are intentionally not speculative trait methods here: each
//! operation is added to the executable contract when its production caller
//! and implementation land, so strict `dead_code` checks remain meaningful.

// The v2 codec, AVL core, canonical arena image, publication records, WAL
// transaction grammar, durable commit envelope, hot completion framing, pure
// apply validator, hot recovery scanner, sealed semantic snapshot, and backend
// state recovery boundary are staged behind the sequence seam. Checkpoint-store
// integration makes the balanced path a production caller and removes the
// scoped dead-code allowances.
#[allow(dead_code)]
mod apply_v2;
#[allow(dead_code)]
mod avl;
#[allow(dead_code)]
mod backend_v2;
#[allow(dead_code)]
mod commit_v2;
#[allow(dead_code)]
mod compaction_v2;
#[cfg(test)]
mod conformance_v2;
#[allow(dead_code)]
mod format_v2;
#[allow(dead_code)]
mod hot_frame_v2;
mod image_v2;
pub(crate) mod physical;
#[allow(dead_code)]
mod publication_v2;
#[allow(dead_code)]
mod recovery_v2;
#[allow(dead_code)]
mod snapshot_v2;
#[allow(dead_code)]
mod transaction_v2;

use avl::{V2AvlError, V2AvlSequence};
use format_v2::V2RootRecord;
pub(crate) use format_v2::V2_ROOT_RECORD_SIZE;
use physical::{
    IoLedger, PhysicalContentStore, PhysicalDelta, PhysicalGcBuild, PhysicalIoCounters,
};
use std::cell::Cell;
use std::fmt;
use std::path::Path;

/// Logical byte length of a persistent sequence.
///
/// Persistent lengths stay in a fixed-width integer. Conversion to `usize`
/// belongs at an allocation or slice boundary after explicit range checking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalLength(u64);

impl LogicalLength {
    /// Creates a logical length from its persisted-width representation.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the fixed-width logical length.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Checked logical-length addition.
    pub fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }
}

/// Physical representation used by one persistent root.
///
/// `LegacyV1` names the pre-release left-deep DAG representation. It does not
/// claim the balanced-tree or persisted-subtree-length guarantees required by
/// the production locality gate.
///
/// `BalancedV2` names the release-candidate balanced AVL representation. Roots
/// re-entering the backend resolve their node identifier against the arena;
/// caller-supplied lengths must agree exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SequenceRepresentation {
    LegacyV1,
    BalancedV2,
}

/// Typed root metadata consumed by checkpoint code.
///
/// `node_id` identifies the physical root while `logical_len` carries the
/// sequence length at the semantic boundary. For Format v1 that length may
/// still have been derived by legacy traversal. A future writable format must
/// persist enough metadata to construct this value without whole-parent work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PersistentRoot {
    node_id: u64,
    logical_len: LogicalLength,
    representation: SequenceRepresentation,
}

impl PersistentRoot {
    /// Adapts a pre-release Format-v1 root without changing its on-disk meaning.
    pub const fn legacy_v1(node_id: u64, logical_len: LogicalLength) -> Self {
        Self {
            node_id,
            logical_len,
            representation: SequenceRepresentation::LegacyV1,
        }
    }

    /// Names a balanced-sequence root by arena position and exact length.
    ///
    /// The backend resolves the identifier against its arena on every call,
    /// so a forged length fails closed instead of misdirecting traversal.
    pub const fn balanced_v2(node_id: u64, logical_len: LogicalLength) -> Self {
        Self {
            node_id,
            logical_len,
            representation: SequenceRepresentation::BalancedV2,
        }
    }

    /// Returns the physical root-node identifier.
    pub const fn node_id(self) -> u64 {
        self.node_id
    }

    /// Returns the exact logical byte length represented by this root.
    pub const fn logical_len(self) -> LogicalLength {
        self.logical_len
    }

    /// Returns the physical representation version for this root.
    pub const fn representation(self) -> SequenceRepresentation {
        self.representation
    }
}

/// Checked half-open logical byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceRange {
    offset: LogicalLength,
    length: LogicalLength,
    end: LogicalLength,
}

impl SequenceRange {
    /// Creates a range only when its half-open end fits in `u64`.
    pub fn new(offset: LogicalLength, length: LogicalLength) -> Option<Self> {
        let end = offset.checked_add(length)?;
        Some(Self {
            offset,
            length,
            end,
        })
    }

    /// Returns the range start.
    pub const fn offset(self) -> LogicalLength {
        self.offset
    }

    /// Returns the range length.
    pub const fn length(self) -> LogicalLength {
        self.length
    }

    /// Returns the checked half-open range end captured at construction.
    pub const fn end(self) -> LogicalLength {
        self.end
    }
}

/// Active representation-neutral persistent byte-sequence read operations.
///
/// This first production seam contains only operations already exercised by
/// checkpoint-store callers. Format v1 is allowed to retain legacy traversal
/// costs behind this interface. The later balanced writable implementation
/// must preserve these semantics while adding the remaining target operations
/// and logarithmic/bounded locality guarantees.
pub trait PersistentSequence {
    type Error;

    /// Returns the exact logical length represented by `root`.
    fn logical_len(&self, root: PersistentRoot) -> Result<LogicalLength, Self::Error>;

    /// Appends the exact requested range to `output`.
    fn read_range(
        &self,
        root: PersistentRoot,
        range: SequenceRange,
        output: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;
}

/// Fallible sequence failure for the production seam.
///
/// Staged backend failures surface transparently so diagnostics are never
/// flattened at the seam boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceError {
    Avl(V2AvlError),
    Invalid(&'static str),
    Capacity(&'static str),
}

impl fmt::Display for SequenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Avl(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Capacity(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for SequenceError {}

impl From<V2AvlError> for SequenceError {
    fn from(error: V2AvlError) -> Self {
        Self::Avl(error)
    }
}

/// Cumulative diagnostic work counters for one sequence backend.
///
/// Counters use saturating arithmetic deliberately: they must make locality
/// regressions observable without ever failing a storage operation. Tests
/// snapshot these values around single operations and assert deltas.
///
/// `nodes_inspected` counts arena-node resolutions performed by the append
/// path itself (parent validation plus spine traversal), so an append over a
/// large parent cannot report near-zero work while hiding traversal.
/// Read/verify traversals accumulate under `nodes_read` instead; appends never
/// touch that counter.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SequenceWorkCounters {
    pub nodes_allocated: u64,
    pub nodes_inspected: u64,
    pub nodes_read: u64,
    pub payload_bytes_read: u64,
    pub payload_bytes_written: u64,
}

/// Writable persistent byte-sequence operations.
///
/// This is the write capability of the production seam. The legacy adapter is
/// intentionally read-only behind [`PersistentSequence`]; only the balanced
/// backend implements this trait today, and its `CheckpointStore` callers land
/// in the next integration slice.
pub trait PersistentSequenceAppend {
    type Error;

    /// Appends `bytes` to `parent` (or creates a root for `None`) without
    /// mutating any retained history, and returns the new root.
    ///
    /// Convenience over [`splice`](PersistentSequenceSplice::splice) at the
    /// parent end: one canonical mutation, one implementation.
    fn append(
        &mut self,
        parent: Option<PersistentRoot>,
        bytes: &[u8],
    ) -> Result<PersistentRoot, Self::Error>;

    /// Recomputes every reachable node's metadata and commitment.
    fn verify(&self, root: PersistentRoot) -> Result<(), Self::Error>;
}

/// Result of one persistent splice: the new root plus exact work accounting.
///
/// `payload_bytes_allocated` covers every new payload byte including bounded
/// boundary-leaf copies, so locality regressions cannot hide copied bytes
/// outside the inserted payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpliceResult {
    /// The new logical root; all older roots remain exact.
    pub root: PersistentRoot,
    /// Arena nodes allocated (insertion leaves plus copied path nodes).
    pub nodes_allocated: u64,
    /// Arena-node resolutions performed (descent, validation, reassembly).
    pub nodes_inspected: u64,
    /// Payload bytes appended to the arena (insert plus boundary copies).
    pub payload_bytes_allocated: u64,
}

/// Persistent local-splice capability: replace one logical range with new
/// bytes through structural sharing, preserving all historical roots.
///
/// This is the canonical content-mutation seam. Reference semantics follow
/// Lean `PersistentAVLEdit.edit` (split / split / drop / build / concat);
/// the Rust implementation is byte-oriented with bounded leaves and is not
/// claimed to formally refine the Lean proof.
pub trait PersistentSequenceSplice {
    type Error;

    /// Replaces `source[offset..offset+delete_len]` with `insert`, where the
    /// source is `parent` (`None` creates a root and requires zero offset and
    /// delete length with non-empty insert). Zero-effect and zero-result
    /// splices are rejected; the result is always non-empty.
    fn splice(
        &mut self,
        parent: Option<PersistentRoot>,
        offset: LogicalLength,
        delete_len: LogicalLength,
        insert: &[u8],
    ) -> Result<SpliceResult, Self::Error>;
}

/// Compact replacement backend from [`BalancedSequence::compact_to_roots`]:
/// the rebuilt arena, input roots remapped in order, and before/after arena
/// sizes for maintenance statistics. Reclaimed deltas are the caller's
/// checked arithmetic.
#[derive(Debug)]
pub(crate) struct CompactedSequence {
    pub(crate) backend: BalancedSequence,
    pub(crate) roots: Vec<PersistentRoot>,
    pub(crate) nodes_before: u64,
    pub(crate) nodes_after: u64,
    pub(crate) payload_bytes_before: u64,
    pub(crate) payload_bytes_after: u64,
}

/// Compact replacement from [`BalancedSequence::compact_physical_to_roots`]:
/// the new-generation backend, remapped roots in input order, and
/// before/after sizes for maintenance statistics.
#[derive(Debug)]
pub(crate) struct PhysicalCompactedSequence {
    pub(crate) backend: BalancedSequence,
    pub(crate) roots: Vec<PersistentRoot>,
    pub(crate) nodes_before: u64,
    pub(crate) nodes_after: u64,
    pub(crate) payload_bytes_before: u64,
    pub(crate) payload_bytes_after: u64,
}

/// Exports one canonical `T2I2` image from a physical store by loading the
/// complete committed tables. Explicit export/fsck/conformance tooling only:
/// `O(state)` RAM and I/O, never the normal authority path.
fn export_physical_image(
    store: &PhysicalContentStore,
    roots: &[V2RootRecord],
) -> Result<Vec<u8>, SequenceError> {
    use image_v2::{encode_v2_image, V2SequenceImage};
    let records = store.load_all_records()?;
    let payload = store.read_all_payload()?;
    Ok(encode_v2_image(&V2SequenceImage {
        payload,
        nodes: records,
        roots: roots.to_vec(),
    })
    .map_err(V2AvlError::from)?)
}

/// Decodes and validates one canonical 56-byte `T2R2` root record from a
/// metadata snapshot, returning the seam root. Non-canonical bytes fail
/// closed here, before any catalogue entry commits.
pub(crate) fn decode_canonical_root_bytes(
    bytes: &[u8; format_v2::V2_ROOT_RECORD_SIZE],
) -> Result<PersistentRoot, SequenceError> {
    let root = format_v2::decode_v2_root(bytes).map_err(V2AvlError::from)?;
    Ok(PersistentRoot::balanced_v2(
        root.node_id(),
        LogicalLength::new(root.logical_len()),
    ))
}

/// In-memory balanced persistent-sequence backend behind the production seam.
///
/// Wraps the staged AVL core without reimplementing tree logic. Every root
/// re-entering through [`PersistentRoot`] resolves its arena position to
/// canonical metadata, so forged lengths or unknown identifiers fail closed
/// inside the core's existing checks.
///
/// E6 adds the second backend variant: the authoritative physical content
/// store. A backend is either ephemeral memory (pure/unit tests, legacy
/// reconstruction) or durable physical files (the one canonical durable
/// path). Both run the SAME split/concat/rebalance/splice/range-read
/// algorithms through the private arena boundary; only the storage of nodes
/// and payload differs.
#[derive(Debug)]
pub struct BalancedSequence {
    backend: SequenceBackend,
    work: Cell<SequenceWorkCounters>,
    ledger: IoLedger,
}

#[derive(Debug)]
enum SequenceBackend {
    Memory(V2AvlSequence),
    Physical(PhysicalContentStore),
}

impl BalancedSequence {
    pub fn new() -> Self {
        Self {
            backend: SequenceBackend::Memory(V2AvlSequence::default()),
            work: Cell::new(SequenceWorkCounters::default()),
            ledger: IoLedger::new(),
        }
    }

    /// Binds an already-opened physical content store: the durable-backend
    /// constructor used by snapshot import and GC adoption.
    pub(crate) fn open_physical(store: PhysicalContentStore) -> Self {
        let ledger = store.ledger();
        Self {
            backend: SequenceBackend::Physical(store),
            work: Cell::new(SequenceWorkCounters::default()),
            ledger,
        }
    }

    /// Returns the shared physical I/O ledger for WAL/metadata accounting
    /// at the history layer.
    pub(crate) fn io_ledger(&self) -> IoLedger {
        self.ledger.clone()
    }

    /// Snapshots the physical I/O counters (all scopes).
    pub fn io_counters(&self) -> PhysicalIoCounters {
        self.ledger.snapshot()
    }

    /// Resets every physical I/O bucket and clears the overflow flag.
    pub fn reset_io_counters(&self) {
        self.ledger.reset();
    }

    /// Runs `body` under one I/O accounting scope.
    /// Reports whether this backend is the durable physical store.
    pub(crate) fn backend_is_physical(&self) -> bool {
        matches!(self.backend, SequenceBackend::Physical(_))
    }

    /// Reports the physical generation for durable backends, if any.
    pub(crate) fn physical_generation(&self) -> Option<u64> {
        match &self.backend {
            SequenceBackend::Memory(_) => None,
            SequenceBackend::Physical(store) => Some(store.generation()),
        }
    }

    /// Reports committed content frontiers for durable backends, if any.
    pub(crate) fn physical_frontiers(&self) -> Option<(u64, u64)> {
        match &self.backend {
            SequenceBackend::Memory(_) => None,
            SequenceBackend::Physical(store) => {
                Some((store.committed_payload_end(), store.committed_node_count()))
            }
        }
    }

    /// Returns a snapshot of the cumulative diagnostic work counters.
    pub fn work_counters(&self) -> SequenceWorkCounters {
        self.work.get()
    }

    /// Reports whether the arena holds no payload or nodes.
    pub fn is_empty(&self) -> bool {
        match &self.backend {
            SequenceBackend::Memory(inner) => inner.is_empty(),
            SequenceBackend::Physical(store) => {
                store.committed_payload_end() == 0 && store.committed_node_count() == 0
            }
        }
    }

    /// Counts live arena nodes, reachable or not. Maintenance regimens use
    /// this alongside [`payload_len`](Self::payload_len) for reclamation
    /// statistics; foreground locality reasoning keeps using work counters.
    pub(crate) fn node_count(&self) -> u64 {
        match &self.backend {
            SequenceBackend::Memory(inner) => u64::try_from(inner.node_count()).unwrap_or(u64::MAX),
            SequenceBackend::Physical(store) => store.committed_node_count(),
        }
    }

    /// Reports live payload-arena bytes, reachable or not.
    pub(crate) fn payload_len(&self) -> u64 {
        match &self.backend {
            SequenceBackend::Memory(inner) => {
                u64::try_from(inner.payload_len()).unwrap_or(u64::MAX)
            }
            SequenceBackend::Physical(store) => store.committed_payload_end(),
        }
    }

    /// Rebuilds a compact replacement backend from explicit retained roots.
    ///
    /// Generic history-core GC entry: every seed resolves (and therefore
    /// validates) before planning; unreachable nodes and payload bytes are
    /// discarded; input roots remap in order. Cumulative foreground work
    /// counters carry over to the replacement — GC traversal must never
    /// masquerade as foreground splice/fork/read work, and replacing the
    /// backend must not silently reset locality history either.
    pub(crate) fn compact_to_roots(
        &self,
        retained_roots: &[PersistentRoot],
    ) -> Result<CompactedSequence, SequenceError> {
        let SequenceBackend::Memory(inner) = &self.backend else {
            return Err(SequenceError::Invalid(
                "durable physical backends compact through quiescent physical GC, not in-memory compaction",
            ));
        };
        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(retained_roots.len())
            .map_err(|_| {
                SequenceError::Capacity("sequence compaction root table allocation failed")
            })?;
        for root in retained_roots.iter().copied() {
            canonical.push(self.resolve(root)?);
        }
        let nodes_before = self.node_count();
        let payload_bytes_before = self.payload_len();
        let (inner, remapped) = inner.compact_to_roots(&canonical)?;
        let backend = Self {
            backend: SequenceBackend::Memory(inner),
            work: Cell::new(self.work.get()),
            ledger: self.ledger.clone(),
        };
        let nodes_after = backend.node_count();
        let payload_bytes_after = backend.payload_len();
        let mut roots = Vec::new();
        roots.try_reserve_exact(remapped.len()).map_err(|_| {
            SequenceError::Capacity("sequence compaction root table allocation failed")
        })?;
        for root in remapped {
            roots.push(PersistentRoot::balanced_v2(
                root.node_id(),
                LogicalLength::new(root.logical_len()),
            ));
        }
        Ok(CompactedSequence {
            backend,
            roots,
            nodes_before,
            nodes_after,
            payload_bytes_before,
            payload_bytes_after,
        })
    }

    /// Encodes the complete arena plus an explicit retained-root table as one
    /// canonical image. The roots must arrive in the caller's canonical order
    /// (the snapshot layer uses version order); each resolves against the
    /// arena exactly like any re-entering root.
    ///
    /// E6 keeps this for canonical export, fsck, conformance, and tests. It
    /// is deliberately NOT part of the normal durable authority path: seal
    /// and reopen never call it on a physical backend.
    pub fn export_image(&self, roots: &[PersistentRoot]) -> Result<Vec<u8>, SequenceError> {
        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(roots.len())
            .map_err(|_| SequenceError::Capacity("sequence image root table allocation failed"))?;
        for root in roots.iter().copied() {
            canonical.push(self.resolve(root)?);
        }
        match &self.backend {
            SequenceBackend::Memory(inner) => Ok(inner.export_image(&canonical)?),
            SequenceBackend::Physical(store) => Ok(export_physical_image(store, &canonical)?),
        }
    }

    /// Rebuilds a backend from one canonical image, returning the backend
    /// plus the image's retained roots converted to seam roots. Every node
    /// revalidates during import; the work counters start empty.
    ///
    /// Memory backends only: durable physical authority binds content files
    /// through the schema6 snapshot instead, so importing an image into a
    /// physical backend fails closed.
    pub fn import_image(bytes: &[u8]) -> Result<(Self, Vec<PersistentRoot>), SequenceError> {
        let (inner, roots) = avl::V2AvlSequence::import_image(bytes)?;
        let mut seam_roots = Vec::new();
        seam_roots
            .try_reserve_exact(roots.len())
            .map_err(|_| SequenceError::Capacity("sequence image root table allocation failed"))?;
        for root in roots {
            seam_roots.push(PersistentRoot::balanced_v2(
                root.node_id(),
                LogicalLength::new(root.logical_len()),
            ));
        }
        Ok((
            Self {
                backend: SequenceBackend::Memory(inner),
                work: Cell::new(SequenceWorkCounters::default()),
                ledger: IoLedger::new(),
            },
            seam_roots,
        ))
    }

    /// Resolves one materialized root to its canonical 56-byte `T2R2` root
    /// record for metadata snapshots. Reads exactly one node record on a
    /// physical backend (metadata-scale: per retained version, never per
    /// content byte).
    pub(crate) fn canonical_root_bytes(
        &self,
        root: PersistentRoot,
    ) -> Result<[u8; format_v2::V2_ROOT_RECORD_SIZE], SequenceError> {
        Ok(format_v2::encode_v2_root(self.resolve(root)?))
    }

    /// Prepares a physical splice delta against the committed frontier
    /// without mutating any file: shared algorithm over the delta view plus
    /// local delta validation. Durable backends only.
    pub(crate) fn prepare_physical_splice(
        &self,
        parent: Option<PersistentRoot>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
        result_len: u64,
    ) -> Result<PhysicalDelta, SequenceError> {
        let SequenceBackend::Physical(store) = &self.backend else {
            return Err(SequenceError::Invalid(
                "physical splice preparation requires a durable physical backend",
            ));
        };
        let resolved = parent.map(|root| self.resolve(root)).transpose()?;
        Ok(physical::prepare_physical_delta(
            store, resolved, offset, delete_len, insert, result_len,
        )?)
    }

    /// Appends a prepared delta to the content files without advancing
    /// the in-memory frontiers or touching the catalogue: the physical half
    /// of the durability barrier. On failure the files may hold an orphan
    /// tail past the committed frontier, which reads ignore and the next
    /// commit heals. Callers sync separately through
    /// [`sync_physical_content`](Self::sync_physical_content) so append
    /// failures (definite rejection) classify distinctly from barrier
    /// failures (rollback-or-reopen).
    ///
    /// Every commit heals first: orphan tails from earlier crashed commits
    /// (physical bytes without metadata authority) truncate back to the
    /// frontier before new bytes append, so a prior failure can never brick
    /// later operations. A heal failure fails closed with no bytes written.
    pub(crate) fn commit_physical_delta(
        &mut self,
        delta: &PhysicalDelta,
    ) -> Result<(), SequenceError> {
        let SequenceBackend::Physical(store) = &mut self.backend else {
            return Err(SequenceError::Invalid(
                "physical delta commit requires a durable physical backend",
            ));
        };
        store.heal_for_write()?;
        store.append_payload_delta(delta.payload_bytes())?;
        if let Err(error) = store.append_node_delta(delta.node_bytes()) {
            let _ = store.rollback_to_committed();
            return Err(error.into());
        }
        Ok(())
    }

    /// Durability barrier for appended content. See
    /// [`commit_physical_delta`](Self::commit_physical_delta) for the
    /// failure classification contract.
    pub(crate) fn sync_physical_content(&mut self) -> Result<(), SequenceError> {
        let SequenceBackend::Physical(store) = &mut self.backend else {
            return Err(SequenceError::Invalid(
                "physical content sync requires a durable physical backend",
            ));
        };
        Ok(store.sync_content()?)
    }

    /// Best-effort rollback of file ends to the committed frontier. Reports
    /// whether the files provably match the frontier again.
    pub(crate) fn rollback_physical_to_committed(&mut self) -> bool {
        match &mut self.backend {
            SequenceBackend::Memory(_) => true,
            SequenceBackend::Physical(store) => store.rollback_to_committed(),
        }
    }

    /// Adopts a committed delta's frontiers after its bytes are durable and
    /// the catalogue decision is made. Plain stores, no I/O: infallible.
    /// The algorithmic work counters move here — exactly once per splice,
    /// only after every fallible step (append, barrier, and on the durable
    /// path the metadata WAL) succeeded — so rejected operations never
    /// contaminate locality history.
    pub(crate) fn adopt_physical_delta(&mut self, delta: &PhysicalDelta) {
        if let SequenceBackend::Physical(store) = &mut self.backend {
            store.adopt_frontiers(delta.new_payload_end(), delta.new_node_count());
            self.note_written(
                delta.allocated_nodes(),
                delta.inspected_nodes(),
                delta.payload_bytes_allocated(),
            );
        }
    }

    /// Validates one THL5 committed delta during replay against file bytes
    /// and advances the frontiers past it. Returns the validated result
    /// root for catalogue adoption.
    pub(crate) fn replay_physical_delta(
        &mut self,
        generation: u64,
        payload_start: u64,
        payload_end: u64,
        node_start: u64,
        node_end: u64,
        delta_digest: [u8; 32],
        result_root: &[u8; format_v2::V2_ROOT_RECORD_SIZE],
        result_len: u64,
    ) -> Result<PersistentRoot, SequenceError> {
        let SequenceBackend::Physical(store) = &mut self.backend else {
            return Err(SequenceError::Invalid(
                "physical delta replay requires a durable physical backend",
            ));
        };
        let root = physical::validate_committed_delta(
            store,
            generation,
            payload_start,
            payload_end,
            node_start,
            node_end,
            delta_digest,
            result_root,
            result_len,
        )?;
        store.adopt_frontiers(payload_end, node_end);
        Ok(PersistentRoot::balanced_v2(
            root.node_id(),
            LogicalLength::new(root.logical_len()),
        ))
    }

    /// Compacts retained roots into a brand-new physical generation through
    /// the quiescent GC builder. Returns the replacement backend (bound to
    /// the new generation files), remapped roots in input order, and
    /// before/after sizes for maintenance statistics.
    pub(crate) fn compact_physical_to_roots(
        &self,
        retained_roots: &[PersistentRoot],
        new_generation: u64,
        dir: &Path,
    ) -> Result<PhysicalCompactedSequence, SequenceError> {
        let SequenceBackend::Physical(source) = &self.backend else {
            return Err(SequenceError::Invalid(
                "physical generation compaction requires a durable physical backend",
            ));
        };
        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(retained_roots.len())
            .map_err(|_| {
                SequenceError::Capacity("sequence compaction root table allocation failed")
            })?;
        for root in retained_roots.iter().copied() {
            canonical.push(self.resolve(root)?);
        }
        let build: PhysicalGcBuild = physical::compact_physical_generation(
            source,
            &canonical,
            new_generation,
            dir,
            &self.ledger,
        )?;
        let mut roots = Vec::new();
        roots.try_reserve_exact(build.roots.len()).map_err(|_| {
            SequenceError::Capacity("sequence compaction root table allocation failed")
        })?;
        for root in build.roots {
            roots.push(PersistentRoot::balanced_v2(
                root.node_id(),
                LogicalLength::new(root.logical_len()),
            ));
        }
        Ok(PhysicalCompactedSequence {
            backend: {
                let replacement = Self::open_physical(build.store);
                // GC traversal must neither masquerade as foreground work
                // nor reset locality history: carry the cumulative counters
                // over to the replacement, exactly like in-memory
                // compaction.
                replacement.work.set(self.work.get());
                replacement
            },
            roots,
            nodes_before: build.nodes_before,
            nodes_after: build.nodes_after,
            payload_bytes_before: build.payload_bytes_before,
            payload_bytes_after: build.payload_bytes_after,
        })
    }

    /// Verifies open content files cover the committed frontiers.
    /// Durable backends only; memory backends are trivially covered.
    pub(crate) fn check_physical_frontiers(&self) -> Result<(), SequenceError> {
        if let SequenceBackend::Physical(store) = &self.backend {
            store.check_files_cover_frontiers()?;
        }
        Ok(())
    }

    /// Heals orphan tails before writable mutation. Durable backends only;
    /// memory backends are trivially healed.
    pub(crate) fn heal_physical_for_write(&mut self) -> Result<(), SequenceError> {
        if let SequenceBackend::Physical(store) = &mut self.backend {
            store.heal_for_write()?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn arm_fail_next_physical_payload_append(&mut self) {
        if let SequenceBackend::Physical(store) = &mut self.backend {
            store.arm_fail_next_payload_append();
        }
    }

    #[cfg(test)]
    pub(crate) fn arm_fail_next_physical_node_append(&mut self) {
        if let SequenceBackend::Physical(store) = &mut self.backend {
            store.arm_fail_next_node_append();
        }
    }

    #[cfg(test)]
    pub(crate) fn arm_fail_next_physical_sync(&mut self) {
        if let SequenceBackend::Physical(store) = &mut self.backend {
            store.arm_fail_next_sync();
        }
    }

    fn resolve(&self, root: PersistentRoot) -> Result<V2RootRecord, SequenceError> {
        if root.representation() != SequenceRepresentation::BalancedV2 {
            return Err(SequenceError::Invalid(
                "balanced sequence backend received an incompatible root",
            ));
        }
        let canonical = match &self.backend {
            SequenceBackend::Memory(inner) => inner.root_for_node_id(root.node_id())?,
            SequenceBackend::Physical(store) => {
                let record = store.load_record(root.node_id())?;
                V2RootRecord::from_node(root.node_id(), record).map_err(V2AvlError::from)?
            }
        };
        if canonical.logical_len() != root.logical_len().get() {
            return Err(SequenceError::Invalid(
                "balanced sequence root length disagrees with arena",
            ));
        }
        Ok(canonical)
    }

    fn note_read(&self, nodes: u64, payload_bytes: usize) {
        let payload_bytes = u64::try_from(payload_bytes).unwrap_or(u64::MAX);
        let mut work = self.work.get();
        work.nodes_read = work.nodes_read.saturating_add(nodes);
        work.payload_bytes_read = work.payload_bytes_read.saturating_add(payload_bytes);
        self.work.set(work);
    }

    /// Staged in P1.1: first production callers land in P1.2, which removes
    /// this allowance.
    #[allow(dead_code)]
    fn note_written(&self, allocated: usize, inspected: usize, payload_bytes: usize) {
        let allocated = u64::try_from(allocated).unwrap_or(u64::MAX);
        let inspected = u64::try_from(inspected).unwrap_or(u64::MAX);
        let payload_bytes = u64::try_from(payload_bytes).unwrap_or(u64::MAX);
        let mut work = self.work.get();
        work.nodes_allocated = work.nodes_allocated.saturating_add(allocated);
        work.nodes_inspected = work.nodes_inspected.saturating_add(inspected);
        work.payload_bytes_written = work.payload_bytes_written.saturating_add(payload_bytes);
        self.work.set(work);
    }
}

impl Default for BalancedSequence {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentSequence for BalancedSequence {
    type Error = SequenceError;

    fn logical_len(&self, root: PersistentRoot) -> Result<LogicalLength, Self::Error> {
        Ok(LogicalLength::new(self.resolve(root)?.logical_len()))
    }

    fn read_range(
        &self,
        root: PersistentRoot,
        range: SequenceRange,
        output: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        let canonical = self.resolve(root)?;
        let (bytes, visited) = match &self.backend {
            SequenceBackend::Memory(inner) => {
                inner.read_range_counted(canonical, range.offset().get(), range.length().get())?
            }
            SequenceBackend::Physical(store) => avl::V2AvlSequence::read_range_counted_on(
                store,
                canonical,
                range.offset().get(),
                range.length().get(),
            )?,
        };
        output.extend_from_slice(&bytes);
        self.note_read(visited, bytes.len());
        Ok(())
    }
}

impl PersistentSequenceAppend for BalancedSequence {
    type Error = SequenceError;

    fn append(
        &mut self,
        parent: Option<PersistentRoot>,
        bytes: &[u8],
    ) -> Result<PersistentRoot, Self::Error> {
        let (offset, delete_len) = match parent {
            None => (LogicalLength::new(0), LogicalLength::new(0)),
            Some(root) => {
                let len = LogicalLength::new(self.resolve(root)?.logical_len());
                (len, LogicalLength::new(0))
            }
        };
        Ok(self.splice(parent, offset, delete_len, bytes)?.root)
    }

    fn verify(&self, root: PersistentRoot) -> Result<(), Self::Error> {
        let canonical = self.resolve(root)?;
        let visited = match &self.backend {
            SequenceBackend::Memory(inner) => inner.verify_root_counted(canonical)?,
            SequenceBackend::Physical(store) => {
                avl::V2AvlSequence::verify_root_counted_on(store, canonical)?
            }
        };
        self.note_read(visited, 0);
        Ok(())
    }
}

impl PersistentSequenceSplice for BalancedSequence {
    type Error = SequenceError;

    fn splice(
        &mut self,
        parent: Option<PersistentRoot>,
        offset: LogicalLength,
        delete_len: LogicalLength,
        insert: &[u8],
    ) -> Result<SpliceResult, Self::Error> {
        let resolved = parent.map(|root| self.resolve(root)).transpose()?;
        // The parent length for result accounting comes from the resolved
        // canonical root, never from caller metadata.
        let parent_len = resolved.map_or(0, V2RootRecord::logical_len);
        let insert_len = u64::try_from(insert.len())
            .map_err(|_| SequenceError::Invalid("balanced sequence insert length exceeds u64"))?;
        let result_len = parent_len
            .checked_sub(delete_len.get())
            .and_then(|remaining| remaining.checked_add(insert_len))
            .ok_or(SequenceError::Invalid(
                "balanced sequence splice result length is inconsistent",
            ))?;
        if self.backend_is_physical() {
            // Durable backend, pure path: prepare against the committed
            // frontier, append and sync the delta (no WAL on this path —
            // test/tooling use only), then adopt. Every phase is shared
            // logic; only durability differs from the metadata-WAL path.
            let delta = self.prepare_physical_splice(
                parent,
                offset.get(),
                delete_len.get(),
                insert,
                result_len,
            )?;
            let (allocated, inspected, payload_allocated) = (
                delta.allocated_nodes(),
                delta.inspected_nodes(),
                delta.payload_bytes_allocated(),
            );
            self.commit_physical_delta(&delta)?;
            self.sync_physical_content()?;
            self.adopt_physical_delta(&delta);
            let root = delta.root_node_id();
            return Ok(SpliceResult {
                root: PersistentRoot::balanced_v2(root, LogicalLength::new(result_len)),
                nodes_allocated: u64::try_from(allocated).unwrap_or(u64::MAX),
                nodes_inspected: u64::try_from(inspected).unwrap_or(u64::MAX),
                payload_bytes_allocated: u64::try_from(payload_allocated).unwrap_or(u64::MAX),
            });
        }
        let SequenceBackend::Memory(inner) = &mut self.backend else {
            return Err(SequenceError::Invalid(
                "balanced sequence backend disappeared during splice",
            ));
        };
        let result = inner.splice(resolved, offset.get(), delete_len.get(), insert)?;
        self.note_written(
            result.allocated_nodes(),
            result.inspected_nodes(),
            result.payload_bytes_allocated(),
        );
        let root = result.root();
        Ok(SpliceResult {
            root: PersistentRoot::balanced_v2(
                root.node_id(),
                LogicalLength::new(root.logical_len()),
            ),
            nodes_allocated: u64::try_from(result.allocated_nodes()).unwrap_or(u64::MAX),
            nodes_inspected: u64::try_from(result.inspected_nodes()).unwrap_or(u64::MAX),
            payload_bytes_allocated: u64::try_from(result.payload_bytes_allocated())
                .unwrap_or(u64::MAX),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_length_addition_fails_closed_on_overflow() {
        assert_eq!(
            LogicalLength::new(u64::MAX).checked_add(LogicalLength::new(1)),
            None
        );
    }

    #[test]
    fn range_construction_rejects_overflow() {
        assert!(SequenceRange::new(LogicalLength::new(u64::MAX), LogicalLength::new(1)).is_none());
    }

    #[test]
    fn legacy_root_keeps_identity_length_and_representation_distinct() {
        let root = PersistentRoot::legacy_v1(41, LogicalLength::new(8192));
        assert_eq!(root.node_id(), 41);
        assert_eq!(root.logical_len().get(), 8192);
        assert_eq!(root.representation(), SequenceRepresentation::LegacyV1);
    }

    fn balanced_fixture() -> BalancedSequence {
        BalancedSequence::new()
    }

    fn read_full(sequence: &BalancedSequence, root: PersistentRoot) -> Vec<u8> {
        let mut output = Vec::new();
        let range =
            SequenceRange::new(LogicalLength::new(0), sequence.logical_len(root).unwrap()).unwrap();
        sequence.read_range(root, range, &mut output).unwrap();
        output
    }

    #[test]
    fn balanced_root_creation_reports_exact_metadata() {
        let mut sequence = balanced_fixture();
        let root = sequence.append(None, b"hello").unwrap();
        assert_eq!(root.representation(), SequenceRepresentation::BalancedV2);
        assert_eq!(root.node_id(), 0);
        assert_eq!(root.logical_len(), LogicalLength::new(5));
        assert_eq!(sequence.logical_len(root).unwrap(), LogicalLength::new(5));
        assert_eq!(read_full(&sequence, root), b"hello");
    }

    #[test]
    fn balanced_append_preserves_parent_and_extends_content() {
        let mut sequence = balanced_fixture();
        let parent = sequence.append(None, b"foo").unwrap();
        let child = sequence.append(Some(parent), b"bar").unwrap();
        assert_eq!(sequence.logical_len(child).unwrap(), LogicalLength::new(6));
        assert_eq!(read_full(&sequence, parent), b"foo");
        assert_eq!(read_full(&sequence, child), b"foobar");
    }

    #[test]
    fn balanced_append_from_old_root_preserves_every_history() {
        let mut sequence = balanced_fixture();
        let root_a = sequence.append(None, b"aaa").unwrap();
        let root_b = sequence.append(Some(root_a), b"bbb").unwrap();
        let root_c = sequence.append(Some(root_b), b"ccc").unwrap();
        let root_d = sequence.append(Some(root_a), b"ddd").unwrap();
        assert_eq!(read_full(&sequence, root_a), b"aaa");
        assert_eq!(read_full(&sequence, root_b), b"aaabbb");
        assert_eq!(read_full(&sequence, root_c), b"aaabbbccc");
        assert_eq!(read_full(&sequence, root_d), b"aaaddd");
    }

    #[test]
    fn balanced_sibling_branches_share_parent_byte_exact() {
        let mut sequence = balanced_fixture();
        let parent = sequence.append(None, b"parent").unwrap();
        let left = sequence.append(Some(parent), b"-left").unwrap();
        let right = sequence.append(Some(parent), b"-right").unwrap();
        assert_ne!(left.node_id(), right.node_id());
        assert_eq!(read_full(&sequence, parent), b"parent");
        assert_eq!(read_full(&sequence, left), b"parent-left");
        assert_eq!(read_full(&sequence, right), b"parent-right");
    }

    #[test]
    fn balanced_arbitrary_range_read_is_exact_across_leaves() {
        let mut sequence = balanced_fixture();
        let mut root = sequence.append(None, b"aa").unwrap();
        for chunk in [b"bb", b"cc", b"dd", b"ee"] {
            root = sequence.append(Some(root), chunk).unwrap();
        }
        assert_eq!(read_full(&sequence, root), b"aabbccddee");
        let mut output = Vec::new();
        let range = SequenceRange::new(LogicalLength::new(1), LogicalLength::new(6)).unwrap();
        sequence.read_range(root, range, &mut output).unwrap();
        assert_eq!(output, b"abbccd");

        let mut empty = Vec::new();
        let zero = SequenceRange::new(LogicalLength::new(3), LogicalLength::new(0)).unwrap();
        sequence.read_range(root, zero, &mut empty).unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn balanced_verify_accepts_valid_and_rejects_tampered_roots() {
        let mut sequence = balanced_fixture();
        let root = sequence.append(None, b"data").unwrap();
        let child = sequence.append(Some(root), b"more").unwrap();
        sequence.verify(root).unwrap();
        sequence.verify(child).unwrap();

        let tampered_len = PersistentRoot::balanced_v2(root.node_id(), LogicalLength::new(999));
        assert_eq!(
            sequence.verify(tampered_len),
            Err(SequenceError::Invalid(
                "balanced sequence root length disagrees with arena"
            ))
        );
        let unknown = PersistentRoot::balanced_v2(999_999, LogicalLength::new(4));
        assert!(sequence.verify(unknown).is_err());
        assert!(sequence.logical_len(unknown).is_err());
    }

    #[test]
    fn balanced_read_rejects_out_of_range_and_huge_lengths() {
        let mut sequence = balanced_fixture();
        let root = sequence.append(None, b"abc").unwrap();

        let mut output = Vec::new();
        let past_end = SequenceRange::new(LogicalLength::new(2), LogicalLength::new(2)).unwrap();
        assert!(sequence.read_range(root, past_end, &mut output).is_err());
        assert!(output.is_empty());

        // A u64::MAX length must fail closed before any allocation attempt.
        let mut huge = Vec::new();
        let unbounded =
            SequenceRange::new(LogicalLength::new(0), LogicalLength::new(u64::MAX)).unwrap();
        assert!(sequence.read_range(root, unbounded, &mut huge).is_err());
        assert!(huge.is_empty());

        // A forged maximal length on a valid identifier fails on metadata,
        // never by trusting the claimed length.
        let forged = PersistentRoot::balanced_v2(root.node_id(), LogicalLength::new(u64::MAX));
        let mut forged_output = Vec::new();
        let full = SequenceRange::new(LogicalLength::new(0), LogicalLength::new(3)).unwrap();
        assert!(sequence
            .read_range(forged, full, &mut forged_output)
            .is_err());
        assert!(forged_output.is_empty());
    }

    #[test]
    fn balanced_append_rejects_empty_payload() {
        let mut sequence = balanced_fixture();
        let root = sequence.append(None, b"abc").unwrap();
        assert!(sequence.append(Some(root), b"").is_err());
        assert!(sequence.append(None, b"").is_err());
        assert_eq!(read_full(&sequence, root), b"abc");
    }

    #[test]
    fn balanced_backend_rejects_foreign_representation() {
        let sequence = balanced_fixture();
        let legacy = PersistentRoot::legacy_v1(0, LogicalLength::new(3));
        assert_eq!(
            sequence.logical_len(legacy),
            Err(SequenceError::Invalid(
                "balanced sequence backend received an incompatible root"
            ))
        );
    }

    #[test]
    fn balanced_work_counters_observe_locality() {
        let mut sequence = balanced_fixture();
        let before = sequence.work_counters();
        let root = sequence.append(None, b"x").unwrap();
        let after_create = sequence.work_counters();
        assert_eq!(
            after_create.payload_bytes_written - before.payload_bytes_written,
            1
        );
        assert!(after_create.nodes_allocated - before.nodes_allocated >= 1);
        // Creating the root reads no parent bytes: there is nothing to hash.
        assert_eq!(after_create.nodes_read, before.nodes_read);

        let parent_bytes = vec![b'p'; 4096];
        let big = sequence.append(None, &parent_bytes).unwrap();
        let before_append = sequence.work_counters();
        let child = sequence.append(Some(big), b"delta").unwrap();
        let after_append = sequence.work_counters();
        assert_eq!(
            after_append.payload_bytes_written - before_append.payload_bytes_written,
            5
        );
        // One leaf plus a logarithmic AVL spine: nowhere near a whole-parent
        // copy of 4096 payload bytes into hundreds of nodes.
        let allocated = after_append.nodes_allocated - before_append.nodes_allocated;
        let inspected = after_append.nodes_inspected - before_append.nodes_inspected;
        assert!(allocated <= 8);
        // Every inspected node sits on the copy path: inspection stays
        // proportional to allocation, so hidden linear traversal cannot hide
        // behind a small allocation count.
        assert!(inspected > 0);
        assert!(inspected <= 8 * allocated);
        // The append path performs no counted read traversal of the parent:
        // this is the regression tripwire for whole-parent reconstruction.
        assert_eq!(after_append.nodes_read, before_append.nodes_read);
        assert_eq!(
            after_append.payload_bytes_read,
            before_append.payload_bytes_read
        );

        let before_read = sequence.work_counters();
        let mut output = Vec::new();
        let range = SequenceRange::new(LogicalLength::new(0), LogicalLength::new(5)).unwrap();
        sequence.read_range(child, range, &mut output).unwrap();
        let after_read = sequence.work_counters();
        assert_eq!(output, b"ppppp");
        assert!(after_read.nodes_read > before_read.nodes_read);
        assert_eq!(
            after_read.payload_bytes_read - before_read.payload_bytes_read,
            5
        );

        let before_verify = sequence.work_counters();
        sequence.verify(child).unwrap();
        let after_verify = sequence.work_counters();
        assert!(after_verify.nodes_read > before_verify.nodes_read);

        assert_eq!(read_full(&sequence, root), b"x");
        assert_eq!(sequence.logical_len(big).unwrap(), LogicalLength::new(4096));
    }

    #[test]
    fn balanced_append_inspection_stays_logarithmic_after_deep_history() {
        let mut sequence = balanced_fixture();
        let mut root = sequence.append(None, b"seed").unwrap();
        for index in 0..200u32 {
            let payload = [(index % 251) as u8, b'z'];
            root = sequence.append(Some(root), &payload).unwrap();
        }
        let before = sequence.work_counters();
        let child = sequence.append(Some(root), b"new").unwrap();
        let after = sequence.work_counters();
        let allocated = after.nodes_allocated - before.nodes_allocated;
        let inspected = after.nodes_inspected - before.nodes_inspected;
        assert!(allocated <= 24, "allocated {allocated} nodes for one delta");
        assert!(inspected <= 96, "inspected {inspected} nodes for one delta");
        assert!(inspected <= 8 * allocated + 8);
        sequence.verify(child).unwrap();
        assert_eq!(sequence.logical_len(child).unwrap().get(), 4 + 400 + 3);
    }

    /// E2.17 locality regression: a small middle splice of a large parent
    /// costs tree height plus inserted payload, never total parent bytes.
    /// The parent is built through the bounded-leaf path, and the hard
    /// invariant bounds new payload bytes by inserted bytes plus at most
    /// two boundary-leaf fragments — not parent size.
    fn middle_splice_locality_case(parent_len: usize, edit_len: usize) {
        use super::format_v2::MAX_LEAF_PAYLOAD_BYTES;

        let mut sequence = balanced_fixture();
        let parent_bytes = vec![0x5Au8; parent_len];
        let parent = sequence
            .splice(
                None,
                LogicalLength::new(0),
                LogicalLength::new(0),
                &parent_bytes,
            )
            .expect("bounded root creation should succeed")
            .root;
        assert_eq!(parent.logical_len().get(), parent_len as u64);
        // Sanity: the large root really is chunked, not one giant leaf.
        assert!(sequence.work_counters().payload_bytes_written as usize >= parent_len);

        let offset = (parent_len / 2) as u64;
        let replacement = vec![0xA5u8; edit_len];
        let before = sequence.work_counters();
        let child = sequence
            .splice(
                Some(parent),
                LogicalLength::new(offset),
                LogicalLength::new(edit_len as u64),
                &replacement,
            )
            .expect("middle splice should succeed");
        let after = sequence.work_counters();
        let new_payload = (after.payload_bytes_written - before.payload_bytes_written) as usize;
        let bound = edit_len + 2 * MAX_LEAF_PAYLOAD_BYTES;
        let allocated = after.nodes_allocated - before.nodes_allocated;
        let inspected = after.nodes_inspected - before.nodes_inspected;
        assert!(
            new_payload <= bound,
            "4 KiB-class edit of {parent_len} bytes allocated {new_payload} payload bytes (bound {bound})"
        );
        assert!(
            allocated <= 256,
            "allocated {allocated} nodes for a local edit of {parent_len} bytes"
        );
        assert!(
            inspected <= 4096,
            "inspected {inspected} nodes for a local edit of {parent_len} bytes"
        );

        // Exactness on both sides of the edit.
        let mut expected = parent_bytes;
        expected.splice(offset as usize..offset as usize + edit_len, replacement);
        assert_eq!(read_full(&sequence, child.root), expected);
        assert_eq!(read_full(&sequence, parent), vec![0x5Au8; parent_len]);
        sequence.verify(child.root).unwrap();
        sequence.verify(parent).unwrap();
    }

    #[test]
    fn middle_splice_of_100mib_parent_stays_local() {
        middle_splice_locality_case(100 * 1024 * 1024, 4 * 1024);
    }

    #[test]
    fn middle_splice_of_1mib_parent_stays_local() {
        middle_splice_locality_case(1024 * 1024, 1024);
    }
}
