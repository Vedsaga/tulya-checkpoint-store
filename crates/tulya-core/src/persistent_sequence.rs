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
use std::cell::Cell;
use std::fmt;

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

/// In-memory balanced persistent-sequence backend behind the production seam.
///
/// Wraps the staged AVL core without reimplementing tree logic. Every root
/// re-entering through [`PersistentRoot`] resolves its arena position to
/// canonical metadata, so forged lengths or unknown identifiers fail closed
/// inside the core's existing checks.
#[derive(Debug)]
pub struct BalancedSequence {
    inner: V2AvlSequence,
    work: Cell<SequenceWorkCounters>,
}

impl BalancedSequence {
    pub fn new() -> Self {
        Self {
            inner: V2AvlSequence::default(),
            work: Cell::new(SequenceWorkCounters::default()),
        }
    }

    /// Returns a snapshot of the cumulative diagnostic work counters.
    pub fn work_counters(&self) -> SequenceWorkCounters {
        self.work.get()
    }

    /// Reports whether the arena holds no payload or nodes.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Counts live arena nodes, reachable or not. Maintenance regimens use
    /// this alongside [`payload_len`](Self::payload_len) for reclamation
    /// statistics; foreground locality reasoning keeps using work counters.
    pub(crate) fn node_count(&self) -> u64 {
        u64::try_from(self.inner.node_count()).unwrap_or(u64::MAX)
    }

    /// Reports live payload-arena bytes, reachable or not.
    pub(crate) fn payload_len(&self) -> u64 {
        u64::try_from(self.inner.payload_len()).unwrap_or(u64::MAX)
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
        let (inner, remapped) = self.inner.compact_to_roots(&canonical)?;
        let backend = Self {
            inner,
            work: Cell::new(self.work.get()),
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
    pub fn export_image(&self, roots: &[PersistentRoot]) -> Result<Vec<u8>, SequenceError> {
        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(roots.len())
            .map_err(|_| SequenceError::Capacity("sequence image root table allocation failed"))?;
        for root in roots.iter().copied() {
            canonical.push(self.resolve(root)?);
        }
        Ok(self.inner.export_image(&canonical)?)
    }

    /// Rebuilds a backend from one canonical image, returning the backend
    /// plus the image's retained roots converted to seam roots. Every node
    /// revalidates during import; the work counters start empty.
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
                inner,
                work: Cell::new(SequenceWorkCounters::default()),
            },
            seam_roots,
        ))
    }

    fn resolve(&self, root: PersistentRoot) -> Result<V2RootRecord, SequenceError> {
        if root.representation() != SequenceRepresentation::BalancedV2 {
            return Err(SequenceError::Invalid(
                "balanced sequence backend received an incompatible root",
            ));
        }
        let canonical = self.inner.root_for_node_id(root.node_id())?;
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
        let (bytes, visited) =
            self.inner
                .read_range_counted(canonical, range.offset().get(), range.length().get())?;
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
        let visited = self.inner.verify_root_counted(canonical)?;
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
        let result = self
            .inner
            .splice(resolved, offset.get(), delete_len.get(), insert)?;
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
