//! Pure persistent AVL sequence core for Format v2.
//!
//! This module owns immutable path-copy edits and range navigation. It has no
//! filesystem, WAL, manifest, or publication responsibilities. Nodes are never
//! mutated after allocation, so every returned root remains a valid historical
//! snapshot while later appends allocate only a new leaf and the changed AVL
//! path.

use super::compaction_v2::{
    plan_reachable_arena, rebuild_compact_records, remapped_node_id, repack_compact_ranges,
};
use super::format_v2::{V2FormatError, V2NodeRecord, V2RootRecord, MAX_LEAF_PAYLOAD_BYTES};
use super::image_v2::{
    decode_v2_image, encode_v2_image, v2_node_fields, V2ImageError, V2NodeFields, V2SequenceImage,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2AvlError {
    Format(V2FormatError),
    Image(V2ImageError),
    Invalid(&'static str),
    Overflow(&'static str),
    Capacity(&'static str),
}

impl fmt::Display for V2AvlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(error) => write!(formatter, "{error}"),
            Self::Image(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) | Self::Capacity(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl std::error::Error for V2AvlError {}

impl From<V2FormatError> for V2AvlError {
    fn from(error: V2FormatError) -> Self {
        Self::Format(error)
    }
}

impl From<V2ImageError> for V2AvlError {
    fn from(error: V2ImageError) -> Self {
        Self::Image(error)
    }
}

/// Maps the shared compaction-core error into the AVL seam error: the
/// string-carrying variants transfer exactly, while checkpoint-shaped
/// wrappers (unreachable from the generic mark/repack/rebuild path) map to
/// static context messages.
impl From<super::compaction_v2::V2CompactionError> for V2AvlError {
    fn from(error: super::compaction_v2::V2CompactionError) -> Self {
        use super::compaction_v2::V2CompactionError as Source;
        match error {
            Source::Image(inner) => Self::Image(inner),
            Source::Format(inner) => Self::Format(inner),
            Source::Invalid(message) => Self::Invalid(message),
            Source::Overflow(message) => Self::Overflow(message),
            Source::Capacity(message) => Self::Capacity(message),
            Source::Publication(_)
            | Source::Commit(_)
            | Source::Snapshot(_)
            | Source::Backend(_) => Self::Invalid("v2 history compaction hit checkpoint state"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct V2AppendResult {
    root: V2RootRecord,
    allocated_nodes: usize,
    inspected_nodes: usize,
}

impl V2AppendResult {
    pub(super) const fn root(self) -> V2RootRecord {
        self.root
    }

    pub(super) const fn allocated_nodes(self) -> usize {
        self.allocated_nodes
    }

    pub(super) const fn inspected_nodes(self) -> usize {
        self.inspected_nodes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct V2SpliceResult {
    root: V2RootRecord,
    allocated_nodes: usize,
    inspected_nodes: usize,
    payload_bytes_allocated: usize,
}

impl V2SpliceResult {
    pub(super) const fn root(self) -> V2RootRecord {
        self.root
    }

    pub(super) const fn allocated_nodes(self) -> usize {
        self.allocated_nodes
    }

    pub(super) const fn inspected_nodes(self) -> usize {
        self.inspected_nodes
    }

    pub(super) const fn payload_bytes_allocated(self) -> usize {
        self.payload_bytes_allocated
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ArenaNode {
    Leaf {
        payload_offset: u64,
        payload_len: u64,
        record: V2NodeRecord,
    },
    Branch {
        left: V2RootRecord,
        right: V2RootRecord,
        record: V2NodeRecord,
    },
}

/// Private content-access boundary shared by the in-memory arena and the
/// durable physical content store (E6).
///
/// The AVL split/concat/rebalance/splice/range-read/verification logic below
/// is written once, against this boundary: there is no separate disk splice
/// algorithm and memory splice algorithm. The in-memory
/// [`V2AvlSequence`] implements it over its vectors (existing pure/unit
/// tests), while the authoritative durable store implements it over
/// fixed-width node records and bounded payload ranges in content files.
///
/// Length accessors report `usize` for direct allocation/index use; every
/// conversion to persisted `u64` widths is checked at the call site. Reads
/// return owned values so file-backed implementations never hand out
/// borrowed arena memory. Builders append only: old nodes and payload are
/// immutable, and new branches may reference old nodes plus earlier fresh
/// nodes from the same delta. Rollback truncates to entry lengths.
///
/// Integrity validation of visited nodes (leaf recomputation against full
/// leaf bytes, branch recomputation against child roots) lives in the shared
/// traversal code, not in any one backend, so both paths enforce identical
/// read integrity. Validation work deliberately does not move the
/// [`SequenceWorkCounters`](super::SequenceWorkCounters) traversal counters:
/// those keep their exact historical traversal-shape semantics, while actual
/// file bytes surface in the physical I/O counters.
pub(super) trait ArenaStore {
    /// Current payload-arena length in bytes.
    fn arena_payload_len(&self) -> usize;
    /// Current node-arena length in records.
    fn arena_node_count(&self) -> usize;
    /// Loads one node by identifier, failing closed on unknown identifiers
    /// or non-canonical record bytes.
    fn arena_load_node(&self, node_id: u64) -> Result<ArenaNode, V2AvlError>;
    /// Reads one exact payload range `[start..end)`, failing closed outside
    /// the arena.
    fn arena_read_payload(&self, start: u64, end: u64) -> Result<Vec<u8>, V2AvlError>;
    /// Materializes a leaf view of an existing immutable payload range.
    ///
    /// The default arena policy copies the bytes into fresh payload so the
    /// legacy in-memory/image representation keeps its contiguous-allocation
    /// invariant. Durable physical arenas may override this and return the
    /// original `start`, allowing fresh leaf metadata to alias an immutable
    /// subrange without rewriting those payload bytes.
    fn arena_alias_payload(&mut self, start: u64, end: u64) -> Result<(u64, Vec<u8>), V2AvlError> {
        let bytes = self.arena_read_payload(start, end)?;
        let offset = self.arena_append_payload(&bytes)?;
        Ok((offset, bytes))
    }
    /// Appends payload bytes, returning the base offset they occupy.
    fn arena_append_payload(&mut self, bytes: &[u8]) -> Result<u64, V2AvlError>;
    /// Pushes one node, returning its dense identifier.
    fn arena_push_node(&mut self, node: ArenaNode) -> Result<u64, V2AvlError>;
    /// Rolls back both arenas to entry lengths after a failed edit. Only
    /// ever called with lengths at or above the delta base.
    fn arena_truncate(&mut self, payload_len: usize, node_count: usize);
}

impl ArenaNode {
    pub(super) const fn record(&self) -> V2NodeRecord {
        match self {
            Self::Leaf { record, .. } | Self::Branch { record, .. } => *record,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct V2AvlSequence {
    payload: Vec<u8>,
    nodes: Vec<ArenaNode>,
}

impl ArenaStore for V2AvlSequence {
    fn arena_payload_len(&self) -> usize {
        self.payload.len()
    }

    fn arena_node_count(&self) -> usize {
        self.nodes.len()
    }

    fn arena_load_node(&self, node_id: u64) -> Result<ArenaNode, V2AvlError> {
        let index = usize::try_from(node_id)
            .map_err(|_| V2AvlError::Overflow("v2 node identifier exceeds usize"))?;
        self.nodes.get(index).cloned().ok_or(V2AvlError::Invalid(
            "v2 root references a missing arena node",
        ))
    }

    fn arena_read_payload(&self, start: u64, end: u64) -> Result<Vec<u8>, V2AvlError> {
        Ok(self.payload_slice(start, end)?.to_vec())
    }

    fn arena_append_payload(&mut self, bytes: &[u8]) -> Result<u64, V2AvlError> {
        let offset = u64::try_from(self.payload.len())
            .map_err(|_| V2AvlError::Overflow("v2 payload arena length exceeds u64"))?;
        self.payload.extend_from_slice(bytes);
        Ok(offset)
    }

    fn arena_push_node(&mut self, node: ArenaNode) -> Result<u64, V2AvlError> {
        let node_id = u64::try_from(self.nodes.len())
            .map_err(|_| V2AvlError::Overflow("v2 node arena length exceeds u64"))?;
        self.nodes.push(node);
        Ok(node_id)
    }

    fn arena_truncate(&mut self, payload_len: usize, node_count: usize) {
        self.payload.truncate(payload_len);
        self.nodes.truncate(node_count);
    }
}

impl V2AvlSequence {
    /// Appends `bytes` to `parent`, preserving `parent` and all older roots.
    ///
    /// Single-implementation convenience over [`splice`](Self::splice):
    /// append canonicalizes immediately to a splice at the parent end, so
    /// there is exactly one durable content-mutation encoding.
    pub(super) fn append(
        &mut self,
        parent: Option<V2RootRecord>,
        bytes: &[u8],
    ) -> Result<V2AppendResult, V2AvlError> {
        // The offset reads record metadata directly; splice re-resolves the
        // parent against the arena and fails closed on any disagreement.
        Self::append_on(self, parent, bytes)
    }

    /// Shared append core: identical logic for memory and physical arenas.
    pub(super) fn append_on<A: ArenaStore>(
        arena: &mut A,
        parent: Option<V2RootRecord>,
        bytes: &[u8],
    ) -> Result<V2AppendResult, V2AvlError> {
        let offset = parent.map_or(0, V2RootRecord::logical_len);
        let result = Self::splice_on(arena, parent, offset, 0, bytes)?;
        Ok(V2AppendResult {
            root: result.root(),
            allocated_nodes: result.allocated_nodes(),
            inspected_nodes: result.inspected_nodes(),
        })
    }

    /// Persistent local splice: replaces `source[offset..offset+delete_len]`
    /// with `insert`, preserving `parent` and every older root.
    ///
    /// Reference semantics (Lean `PersistentAVLEdit.edit`): split at the
    /// offset, split the right side at the delete length, drop the deleted
    /// middle, build the inserted payload as a balanced subtree, and concat
    /// left + insertion + right. Only the descent paths are copied; a leaf
    /// split strictly inside one bounded leaf copies at most that leaf's
    /// fragments.
    ///
    /// An error rolls back every allocation made by this call: both arenas
    /// truncate to their entry lengths and every old root remains exact.
    /// `None` parent creates a root and requires `offset == 0`,
    /// `delete_len == 0`, and non-empty `insert`; a zero-effect or
    /// zero-result splice on an existing parent is rejected.
    pub(super) fn splice(
        &mut self,
        parent: Option<V2RootRecord>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
    ) -> Result<V2SpliceResult, V2AvlError> {
        Self::splice_on(self, parent, offset, delete_len, insert)
    }

    /// Shared persistent local splice over any arena: replaces
    /// `source[offset..offset+delete_len]` with `insert`, preserving
    /// `parent` and every older root. See [`splice`](Self::splice) for the
    /// reference semantics and rollback contract.
    pub(super) fn splice_on<A: ArenaStore>(
        arena: &mut A,
        parent: Option<V2RootRecord>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
    ) -> Result<V2SpliceResult, V2AvlError> {
        let source_len = match parent {
            None => 0,
            Some(root) => {
                Self::node_for_root_on(arena, root)?;
                root.logical_len()
            }
        };
        if offset > source_len {
            return Err(V2AvlError::Invalid(
                "v2 sequence splice offset exceeds parent length",
            ));
        }
        if delete_len > source_len - offset {
            return Err(V2AvlError::Invalid(
                "v2 sequence splice delete range exceeds parent length",
            ));
        }
        let insert_len = u64::try_from(insert.len())
            .map_err(|_| V2AvlError::Overflow("v2 sequence insert length exceeds u64"))?;
        let result_len =
            (source_len - delete_len)
                .checked_add(insert_len)
                .ok_or(V2AvlError::Overflow(
                    "v2 sequence splice result length exceeds u64",
                ))?;
        match parent {
            None => {
                if offset != 0 || delete_len != 0 {
                    return Err(V2AvlError::Invalid(
                        "v2 sequence root creation requires zero offset and delete length",
                    ));
                }
                if insert.is_empty() {
                    return Err(V2AvlError::Invalid(
                        "v2 sequence root creation requires non-empty insert",
                    ));
                }
            }
            Some(_) => {
                if delete_len == 0 && insert.is_empty() {
                    return Err(V2AvlError::Invalid(
                        "v2 sequence splice without effect is rejected",
                    ));
                }
            }
        }
        if result_len == 0 {
            return Err(V2AvlError::Invalid(
                "v2 sequence splice result must be non-empty",
            ));
        }

        let payload_start = arena.arena_payload_len();
        let node_start = arena.arena_node_count();
        let mut inspected = 0usize;
        // The parent node itself is an accessed path: boundary splits may
        // return it untouched, so validate it explicitly here rather than
        // relying on descent to visit it.
        if let Some(root) = parent {
            let node = Self::node_for_root_on(arena, root)?;
            Self::validate_node_on(arena, &node)?;
        }
        match Self::splice_inner_on(arena, parent, offset, delete_len, insert, &mut inspected) {
            Ok(root) => Ok(V2SpliceResult {
                root,
                allocated_nodes: arena.arena_node_count() - node_start,
                inspected_nodes: inspected,
                payload_bytes_allocated: arena.arena_payload_len() - payload_start,
            }),
            Err(error) => {
                arena.arena_truncate(payload_start, node_start);
                Err(error)
            }
        }
    }

    fn splice_inner_on<A: ArenaStore>(
        arena: &mut A,
        parent: Option<V2RootRecord>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        let middle = if insert.is_empty() {
            None
        } else {
            Some(Self::build_insert_subtree_on(arena, insert, inspected)?)
        };
        let Some(root) = parent else {
            // Root creation validated non-empty above; the option unwrap
            // stays a closed error rather than a panic.
            return middle.ok_or(V2AvlError::Invalid(
                "v2 sequence root creation produced no content",
            ));
        };
        let (left, mid_right) = Self::split_on(arena, root, offset, inspected)?;
        let right = match mid_right {
            None => None,
            Some(mid) => {
                if delete_len == 0 {
                    Some(mid)
                } else {
                    let (_, right) = Self::split_on(arena, mid, delete_len, inspected)?;
                    right
                }
            }
        };
        let combined = Self::concat_optional_on(arena, left, middle, inspected)?;
        let combined = Self::concat_optional_on(arena, combined, right, inspected)?;
        combined.ok_or(V2AvlError::Invalid(
            "v2 sequence splice produced an empty root",
        ))
    }

    /// Persistent split at `offset`: left holds `[0..offset)`, right holds
    /// `[offset..len)`. Either side is `None` exactly at the boundaries.
    /// Only the descent path is copied and rejoined through the balancing
    /// concat, so both halves stay valid AVL trees sharing every untouched
    /// node with the source.
    fn split_on<A: ArenaStore>(
        arena: &mut A,
        root: V2RootRecord,
        offset: u64,
        inspected: &mut usize,
    ) -> Result<(Option<V2RootRecord>, Option<V2RootRecord>), V2AvlError> {
        let len = root.logical_len();
        if offset > len {
            return Err(V2AvlError::Invalid(
                "v2 sequence split offset exceeds root length",
            ));
        }
        if offset == 0 {
            return Ok((None, Some(root)));
        }
        if offset == len {
            return Ok((Some(root), None));
        }
        *inspected = inspected.saturating_add(1);
        let node = Self::node_for_root_on(arena, root)?;
        // Every descended node is an accessed path: its stored record must
        // agree with its payload (leaf) or children (branch), identically on
        // memory and physical arenas.
        Self::validate_node_on(arena, &node)?;
        match node {
            ArenaNode::Leaf {
                payload_offset,
                payload_len,
                ..
            } => {
                let left_end = payload_offset
                    .checked_add(offset)
                    .ok_or(V2AvlError::Overflow("v2 leaf split range exceeds u64"))?;
                let leaf_end = payload_offset
                    .checked_add(payload_len)
                    .ok_or(V2AvlError::Overflow("v2 leaf split range exceeds u64"))?;
                // Preserve immutable boundary bytes by range whenever the
                // arena supports it. The pure in-memory arena intentionally
                // falls back to copying so its canonical image format remains
                // unchanged; the durable physical arena aliases the ranges.
                let left = Self::allocate_leaf_slice_on(arena, payload_offset, left_end)?;
                let right = Self::allocate_leaf_slice_on(arena, left_end, leaf_end)?;
                Ok((Some(left), Some(right)))
            }
            ArenaNode::Branch { left, right, .. } => {
                let left_len = left.logical_len();
                match offset.cmp(&left_len) {
                    std::cmp::Ordering::Less => {
                        let (far_left, near) = Self::split_on(arena, left, offset, inspected)?;
                        let joined_right = match near {
                            None => right,
                            Some(near) => Self::concat_on(arena, near, right, inspected)?,
                        };
                        Ok((far_left, Some(joined_right)))
                    }
                    std::cmp::Ordering::Equal => Ok((Some(left), Some(right))),
                    std::cmp::Ordering::Greater => {
                        let (near, far_right) =
                            Self::split_on(arena, right, offset - left_len, inspected)?;
                        let joined_left = match near {
                            None => left,
                            Some(near) => Self::concat_on(arena, left, near, inspected)?,
                        };
                        Ok((Some(joined_left), far_right))
                    }
                }
            }
        }
    }

    /// Concatenation over possibly empty sides, so split/splice represent
    /// temporary empty pieces without an externally visible empty root.
    fn concat_optional_on<A: ArenaStore>(
        arena: &mut A,
        left: Option<V2RootRecord>,
        right: Option<V2RootRecord>,
        inspected: &mut usize,
    ) -> Result<Option<V2RootRecord>, V2AvlError> {
        match (left, right) {
            (None, None) => Ok(None),
            (None, Some(root)) | (Some(root), None) => Ok(Some(root)),
            (Some(left), Some(right)) => Ok(Some(Self::concat_on(arena, left, right, inspected)?)),
        }
    }

    /// Builds a balanced subtree for inserted bytes in linear chunk work:
    /// bounded leaves first, then bottom-up pairing rounds that halve the
    /// level each round instead of repeatedly path-copying a growing tree.
    fn build_insert_subtree_on<A: ArenaStore>(
        arena: &mut A,
        insert: &[u8],
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        let chunks = insert.len().div_ceil(MAX_LEAF_PAYLOAD_BYTES);
        let mut level = Vec::new();
        level
            .try_reserve_exact(chunks)
            .map_err(|_| V2AvlError::Invalid("v2 sequence insert level allocation failed"))?;
        for chunk in insert.chunks(MAX_LEAF_PAYLOAD_BYTES) {
            level.push(Self::allocate_leaf_on(arena, chunk)?);
        }
        while level.len() > 1 {
            let mut next = Vec::new();
            next.try_reserve_exact(level.len().div_ceil(2))
                .map_err(|_| V2AvlError::Invalid("v2 sequence insert level allocation failed"))?;
            let mut index = 0;
            while index < level.len() {
                if index + 1 < level.len() {
                    next.push(Self::concat_on(
                        arena,
                        level[index],
                        level[index + 1],
                        inspected,
                    )?);
                    index += 2;
                } else {
                    next.push(level[index]);
                    index += 1;
                }
            }
            level = next;
        }
        level.pop().ok_or(V2AvlError::Invalid(
            "v2 sequence insert produced no content",
        ))
    }

    /// Allocates leaf metadata for an immutable source-payload subrange.
    ///
    /// `ArenaStore::arena_alias_payload` chooses the physical policy. The
    /// durable store keeps the original payload coordinates (zero payload
    /// rewrite); the legacy in-memory arena copies the bytes and returns the
    /// fresh coordinate. In both cases the commitment is recomputed from the
    /// exact bytes and the returned leaf is canonical.
    fn allocate_leaf_slice_on<A: ArenaStore>(
        arena: &mut A,
        start: u64,
        end: u64,
    ) -> Result<V2RootRecord, V2AvlError> {
        if start >= end {
            return Err(V2AvlError::Invalid(
                "v2 leaf slice must reference a non-empty payload range",
            ));
        }
        let (payload_offset, bytes) = arena.arena_alias_payload(start, end)?;
        let record = V2NodeRecord::leaf(payload_offset, &bytes)?;
        let payload_len = record.logical_len();
        Self::allocate_node_on(
            arena,
            ArenaNode::Leaf {
                payload_offset,
                payload_len,
                record,
            },
        )
    }

    /// Allocates one bounded leaf. The record constructor enforces
    /// non-emptiness and the staging payload bound.
    fn allocate_leaf_on<A: ArenaStore>(
        arena: &mut A,
        bytes: &[u8],
    ) -> Result<V2RootRecord, V2AvlError> {
        let payload_offset = u64::try_from(arena.arena_payload_len())
            .map_err(|_| V2AvlError::Overflow("v2 payload arena length exceeds u64"))?;
        let record = V2NodeRecord::leaf(payload_offset, bytes)?;
        let payload_len = record.logical_len();
        let placed = arena.arena_append_payload(bytes)?;
        if placed != payload_offset {
            return Err(V2AvlError::Invalid(
                "v2 leaf payload placement disagrees with its record",
            ));
        }
        Self::allocate_node_on(
            arena,
            ArenaNode::Leaf {
                payload_offset,
                payload_len,
                record,
            },
        )
    }

    /// Returns an exact logical byte range from one retained root.
    pub(super) fn read_range(
        &self,
        root: V2RootRecord,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, V2AvlError> {
        Ok(Self::read_range_counted_on(self, root, offset, length)?.0)
    }

    /// Returns an exact logical byte range plus the number of arena nodes
    /// traversed. The count makes future locality regressions observable; it
    /// does not change traversal semantics.
    pub(super) fn read_range_counted(
        &self,
        root: V2RootRecord,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, u64), V2AvlError> {
        Self::read_range_counted_on(self, root, offset, length)
    }

    /// Shared range-read core over any arena. Every visited node is an
    /// accessed path: leaf payload is re-read whole and recomputed against
    /// its record (a sub-range read still validates the complete bounded
    /// boundary leaf), and branch records recompute against their child
    /// roots. A corrupt node fails the read closed even when the requested
    /// slice itself would decode; corruption outside visited paths stays
    /// lazy for `verify`/`fsck` to catch.
    pub(super) fn read_range_counted_on<A: ArenaStore>(
        arena: &A,
        root: V2RootRecord,
        offset: u64,
        length: u64,
    ) -> Result<(Vec<u8>, u64), V2AvlError> {
        Self::node_for_root_on(arena, root)?;
        let end = offset
            .checked_add(length)
            .ok_or(V2AvlError::Overflow("v2 range end exceeds u64"))?;
        if end > root.logical_len() {
            return Err(V2AvlError::Invalid("v2 range exceeds root logical length"));
        }
        let capacity = usize::try_from(length)
            .map_err(|_| V2AvlError::Overflow("v2 range length exceeds usize"))?;
        let mut output = Vec::with_capacity(capacity);
        if length == 0 {
            return Ok((output, 0));
        }

        let mut visited = 0u64;
        let mut stack = vec![(root, offset, length)];
        while let Some((current, local_offset, local_length)) = stack.pop() {
            visited = visited.saturating_add(1);
            let node = Self::node_for_root_on(arena, current)?;
            Self::validate_node_on(arena, &node)?;
            match node {
                ArenaNode::Leaf {
                    payload_offset,
                    payload_len,
                    ..
                } => {
                    let local_end = local_offset
                        .checked_add(local_length)
                        .ok_or(V2AvlError::Overflow("v2 leaf range exceeds u64"))?;
                    if local_end > payload_len {
                        return Err(V2AvlError::Invalid("v2 leaf range exceeds leaf payload"));
                    }
                    let start = payload_offset
                        .checked_add(local_offset)
                        .ok_or(V2AvlError::Overflow("v2 payload start exceeds u64"))?;
                    let end = start
                        .checked_add(local_length)
                        .ok_or(V2AvlError::Overflow("v2 payload end exceeds u64"))?;
                    output.extend_from_slice(&arena.arena_read_payload(start, end)?);
                }
                ArenaNode::Branch { left, right, .. } => {
                    let left_len = left.logical_len();
                    if local_offset < left_len {
                        let left_available = left_len - local_offset;
                        let left_length = local_length.min(left_available);
                        let right_length = local_length - left_length;
                        if right_length > 0 {
                            stack.push((right, 0, right_length));
                        }
                        if left_length > 0 {
                            stack.push((left, local_offset, left_length));
                        }
                    } else {
                        stack.push((right, local_offset - left_len, local_length));
                    }
                }
            }
        }

        if output.len() != capacity {
            return Err(V2AvlError::Invalid(
                "v2 range traversal produced an unexpected byte count",
            ));
        }
        Ok((output, visited))
    }

    /// Recomputes every reachable node's metadata and commitment.
    pub(super) fn verify_root(&self, root: V2RootRecord) -> Result<(), V2AvlError> {
        let _ = Self::verify_root_counted_on(self, root)?;
        Ok(())
    }

    /// Verifies like [`V2AvlSequence::verify_root`] and reports how many arena
    /// nodes were revalidated.
    pub(super) fn verify_root_counted(&self, root: V2RootRecord) -> Result<u64, V2AvlError> {
        Self::verify_root_counted_on(self, root)
    }

    /// Shared full-version verification core over any arena.
    pub(super) fn verify_root_counted_on<A: ArenaStore>(
        arena: &A,
        root: V2RootRecord,
    ) -> Result<u64, V2AvlError> {
        let mut visited = 0u64;
        Self::verify_node_on(arena, root, &mut visited)?;
        Ok(visited)
    }

    pub(super) fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub(super) fn payload_len(&self) -> usize {
        self.payload.len()
    }

    /// Rebuilds a compact replacement arena from explicit retained roots.
    ///
    /// Generic history-core GC entry: validates every seed root against the
    /// live arena, marks reachable nodes/payload through the shared
    /// compaction core, repacks payload densely, canonically rebuilds nodes
    /// with remapped children, and remaps each input root. Unreachable nodes
    /// and payload bytes are discarded; an empty seed list yields an empty
    /// backend. Rebuilt records preserve source height, logical length, and
    /// commitment (enforced by the rebuild core); root commitments and
    /// lengths are rechecked across relocation below. No work counters move:
    /// callers account GC maintenance separately from foreground locality.
    pub(super) fn compact_to_roots(
        &self,
        roots: &[V2RootRecord],
    ) -> Result<(Self, Vec<V2RootRecord>), V2AvlError> {
        let mut seeds: Vec<u64> = Vec::new();
        seeds
            .try_reserve_exact(roots.len())
            .map_err(|_| V2AvlError::Capacity("v2 history compaction seed allocation failed"))?;
        for root in roots {
            // Validates representation, node identity, and exact length: a
            // forged or dangling root fails here, before any plan work.
            Self::node_for_root_on(self, *root)?;
            seeds.push(root.node_id());
        }
        let mut records: Vec<V2NodeRecord> = Vec::new();
        records
            .try_reserve_exact(self.nodes.len())
            .map_err(|_| V2AvlError::Capacity("v2 history compaction record allocation failed"))?;
        records.extend(self.nodes.iter().map(ArenaNode::record));
        let (retained, ranges) = plan_reachable_arena(&self.payload, &records, &seeds)?;
        let (compact_payload, payload_mapping) = repack_compact_ranges(&self.payload, &ranges)?;
        let (compact_records, node_mapping) =
            rebuild_compact_records(&records, &retained, &compact_payload, &payload_mapping)?;
        // Convert canonical records back to arena nodes; ascending order
        // guarantees branch children already exist for reconstruction.
        let mut nodes: Vec<ArenaNode> = Vec::new();
        nodes
            .try_reserve_exact(compact_records.len())
            .map_err(|_| V2AvlError::Capacity("v2 history compaction node allocation failed"))?;
        for record in &compact_records {
            nodes.push(Self::compact_arena_node(&nodes, *record)?);
        }
        // Fully-swept recheck: exactly the retained set was rebuilt, dense
        // by construction, with no extra nodes admitted.
        if nodes.len() != retained.len() {
            return Err(V2AvlError::Invalid(
                "v2 history compaction node table disagrees with its retained set",
            ));
        }
        let mut new_roots: Vec<V2RootRecord> = Vec::new();
        new_roots
            .try_reserve_exact(roots.len())
            .map_err(|_| V2AvlError::Capacity("v2 history compaction root allocation failed"))?;
        for root in roots {
            let new_id = remapped_node_id(&node_mapping, root.node_id())?;
            let index = usize::try_from(new_id).map_err(|_| {
                V2AvlError::Overflow("v2 history compaction node identifier exceeds usize")
            })?;
            let record = nodes
                .get(index)
                .ok_or(V2AvlError::Invalid(
                    "v2 history compaction remapped root is absent",
                ))?
                .record();
            new_roots.push(V2RootRecord::from_node(new_id, record)?);
        }
        // Relocation must preserve content identity byte-exact.
        for (old, new) in roots.iter().zip(new_roots.iter()) {
            if old.commitment() != new.commitment() || old.logical_len() != new.logical_len() {
                return Err(V2AvlError::Invalid(
                    "v2 history compaction root disagrees with its source",
                ));
            }
        }
        Ok((
            Self {
                payload: compact_payload,
                nodes,
            },
            new_roots,
        ))
    }

    /// Converts one canonically rebuilt record into a live arena node,
    /// resolving branch children against already-built compact nodes.
    fn compact_arena_node(
        built: &[ArenaNode],
        record: V2NodeRecord,
    ) -> Result<ArenaNode, V2AvlError> {
        match v2_node_fields(record)? {
            V2NodeFields::Leaf {
                payload_offset,
                payload_len,
            } => Ok(ArenaNode::Leaf {
                payload_offset,
                payload_len,
                record,
            }),
            V2NodeFields::Branch {
                left_node_id,
                right_node_id,
                ..
            } => {
                let left_index = usize::try_from(left_node_id).map_err(|_| {
                    V2AvlError::Overflow("v2 history compaction node identifier exceeds usize")
                })?;
                let right_index = usize::try_from(right_node_id).map_err(|_| {
                    V2AvlError::Overflow("v2 history compaction node identifier exceeds usize")
                })?;
                let left = built.get(left_index).ok_or(V2AvlError::Invalid(
                    "v2 history compaction branch child is absent",
                ))?;
                let right = built.get(right_index).ok_or(V2AvlError::Invalid(
                    "v2 history compaction branch child is absent",
                ))?;
                Ok(ArenaNode::Branch {
                    left: V2RootRecord::from_node(left_node_id, left.record())?,
                    right: V2RootRecord::from_node(right_node_id, right.record())?,
                    record,
                })
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.payload.is_empty() && self.nodes.is_empty()
    }

    /// Encodes the complete append-only arena plus an explicit retained-root table.
    ///
    /// This is an O(total arena) snapshot operation for sealing/reopen work. It
    /// is deliberately not part of the foreground append locality path.
    pub(super) fn export_image(&self, roots: &[V2RootRecord]) -> Result<Vec<u8>, V2AvlError> {
        if roots.is_empty() {
            return Err(V2AvlError::Invalid(
                "v2 image export requires at least one retained root",
            ));
        }
        for root in roots {
            Self::node_for_root_on(self, *root)?;
        }
        let image = V2SequenceImage {
            payload: self.payload.clone(),
            nodes: self.nodes.iter().map(ArenaNode::record).collect(),
            roots: roots.to_vec(),
        };
        Ok(encode_v2_image(&image)?)
    }

    /// Reconstructs an arena from one canonical v2 image and validates every node.
    pub(super) fn import_image(bytes: &[u8]) -> Result<(Self, Vec<V2RootRecord>), V2AvlError> {
        let image = decode_v2_image(bytes)?;
        let mut sequence = Self {
            payload: image.payload,
            nodes: Vec::with_capacity(image.nodes.len()),
        };
        let mut expected_payload_offset = 0u64;

        for (index, record) in image.nodes.into_iter().enumerate() {
            let node_id = u64::try_from(index)
                .map_err(|_| V2AvlError::Overflow("v2 image node index exceeds u64"))?;
            match v2_node_fields(record)? {
                V2NodeFields::Leaf {
                    payload_offset,
                    payload_len,
                } => {
                    if payload_offset != expected_payload_offset {
                        return Err(V2AvlError::Invalid(
                            "v2 image leaf payloads are not contiguous in allocation order",
                        ));
                    }
                    let payload_end =
                        payload_offset
                            .checked_add(payload_len)
                            .ok_or(V2AvlError::Overflow(
                                "v2 image leaf payload range exceeds u64",
                            ))?;
                    let expected = V2NodeRecord::leaf(
                        payload_offset,
                        sequence.payload_slice(payload_offset, payload_end)?,
                    )?;
                    if expected != record {
                        return Err(V2AvlError::Invalid(
                            "v2 image leaf metadata or commitment verification failed",
                        ));
                    }
                    sequence.nodes.push(ArenaNode::Leaf {
                        payload_offset,
                        payload_len,
                        record,
                    });
                    expected_payload_offset = payload_end;
                }
                V2NodeFields::Branch {
                    left_node_id,
                    right_node_id,
                    left_len,
                } => {
                    if left_node_id >= node_id || right_node_id >= node_id {
                        return Err(V2AvlError::Invalid(
                            "v2 image branch child must reference an earlier arena node",
                        ));
                    }
                    let left = sequence.root_for_node_id(left_node_id)?;
                    let right = sequence.root_for_node_id(right_node_id)?;
                    if left.logical_len() != left_len {
                        return Err(V2AvlError::Invalid(
                            "v2 image branch left length disagrees with its child",
                        ));
                    }
                    let expected = V2NodeRecord::branch(left, right)?;
                    if expected != record {
                        return Err(V2AvlError::Invalid(
                            "v2 image branch metadata or commitment verification failed",
                        ));
                    }
                    sequence.nodes.push(ArenaNode::Branch {
                        left,
                        right,
                        record,
                    });
                }
            }
        }

        let payload_len = u64::try_from(sequence.payload.len())
            .map_err(|_| V2AvlError::Overflow("v2 image payload length exceeds u64"))?;
        if expected_payload_offset != payload_len {
            return Err(V2AvlError::Invalid(
                "v2 image contains payload bytes not owned by canonical leaves",
            ));
        }
        for root in &image.roots {
            Self::node_for_root_on(&sequence, *root)?;
        }
        Ok((sequence, image.roots))
    }

    /// Persistent AVL concatenation. Only the changed spine is copied.
    fn concat_on<A: ArenaStore>(
        arena: &mut A,
        left: V2RootRecord,
        right: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        if left.height() > right.height().saturating_add(1) {
            let (left_left, left_right) = Self::branch_children_on(arena, left, inspected)?;
            let joined = Self::concat_on(arena, left_right, right, inspected)?;
            return Self::rebalance_on(arena, left_left, joined, inspected);
        }
        if right.height() > left.height().saturating_add(1) {
            let (right_left, right_right) = Self::branch_children_on(arena, right, inspected)?;
            let joined = Self::concat_on(arena, left, right_left, inspected)?;
            return Self::rebalance_on(arena, joined, right_right, inspected);
        }
        Self::allocate_branch_on(arena, left, right, inspected)
    }

    /// Restores the AVL height invariant with a single or double rotation.
    /// Test-visible entry kept for rotation coverage: shares the single
    /// implementation below.
    #[cfg(test)]
    fn rebalance(
        &mut self,
        left: V2RootRecord,
        right: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        Self::rebalance_on(self, left, right, inspected)
    }

    fn rebalance_on<A: ArenaStore>(
        arena: &mut A,
        left: V2RootRecord,
        right: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        if left.height().abs_diff(right.height()) <= 1 {
            return Self::allocate_branch_on(arena, left, right, inspected);
        }

        if left.height() > right.height() {
            let (left_left, left_right) = Self::branch_children_on(arena, left, inspected)?;
            if left_left.height() >= left_right.height() {
                let new_right = Self::allocate_branch_on(arena, left_right, right, inspected)?;
                return Self::allocate_branch_on(arena, left_left, new_right, inspected);
            }
            let (middle_left, middle_right) =
                Self::branch_children_on(arena, left_right, inspected)?;
            let new_left = Self::allocate_branch_on(arena, left_left, middle_left, inspected)?;
            let new_right = Self::allocate_branch_on(arena, middle_right, right, inspected)?;
            return Self::allocate_branch_on(arena, new_left, new_right, inspected);
        }

        let (right_left, right_right) = Self::branch_children_on(arena, right, inspected)?;
        if right_right.height() >= right_left.height() {
            let new_left = Self::allocate_branch_on(arena, left, right_left, inspected)?;
            return Self::allocate_branch_on(arena, new_left, right_right, inspected);
        }
        let (middle_left, middle_right) = Self::branch_children_on(arena, right_left, inspected)?;
        let new_left = Self::allocate_branch_on(arena, left, middle_left, inspected)?;
        let new_right = Self::allocate_branch_on(arena, middle_right, right_right, inspected)?;
        Self::allocate_branch_on(arena, new_left, new_right, inspected)
    }

    /// Test-visible branch allocation entry kept for rotation coverage:
    /// shares the single implementation below.
    #[cfg(test)]
    fn allocate_branch(
        &mut self,
        left: V2RootRecord,
        right: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        Self::allocate_branch_on(self, left, right, inspected)
    }

    fn allocate_branch_on<A: ArenaStore>(
        arena: &mut A,
        left: V2RootRecord,
        right: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        // Both child validations below resolve one arena node each.
        *inspected = inspected.saturating_add(2);
        Self::node_for_root_on(arena, left)?;
        Self::node_for_root_on(arena, right)?;
        let record = V2NodeRecord::branch(left, right)?;
        Self::allocate_node_on(
            arena,
            ArenaNode::Branch {
                left,
                right,
                record,
            },
        )
    }

    fn allocate_node_on<A: ArenaStore>(
        arena: &mut A,
        node: ArenaNode,
    ) -> Result<V2RootRecord, V2AvlError> {
        let node_id = u64::try_from(arena.arena_node_count())
            .map_err(|_| V2AvlError::Overflow("v2 node arena length exceeds u64"))?;
        let root = V2RootRecord::from_node(node_id, node.record())?;
        let placed = arena.arena_push_node(node)?;
        if placed != node_id {
            return Err(V2AvlError::Invalid(
                "v2 node placement disagrees with its record",
            ));
        }
        Ok(root)
    }

    fn branch_children_on<A: ArenaStore>(
        arena: &A,
        root: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<(V2RootRecord, V2RootRecord), V2AvlError> {
        // The single resolution below reads one arena node.
        *inspected = inspected.saturating_add(1);
        match Self::node_for_root_on(arena, root)? {
            ArenaNode::Branch { left, right, .. } => Ok((left, right)),
            ArenaNode::Leaf { .. } => Err(V2AvlError::Invalid(
                "v2 AVL traversal expected a branch node",
            )),
        }
    }

    /// Resolves an arena position to its canonical root metadata.
    ///
    /// Exposed to the production seam so a [`PersistentRoot`](super::PersistentRoot)
    /// re-entering the backend resolves against the actual arena node instead
    /// of trusting caller-supplied metadata.
    pub(super) fn root_for_node_id(&self, node_id: u64) -> Result<V2RootRecord, V2AvlError> {
        Self::root_for_node_id_on(self, node_id)
    }

    /// Shared arena-position resolution over any arena.
    pub(super) fn root_for_node_id_on<A: ArenaStore>(
        arena: &A,
        node_id: u64,
    ) -> Result<V2RootRecord, V2AvlError> {
        let node = arena.arena_load_node(node_id)?;
        Ok(V2RootRecord::from_node(node_id, node.record())?)
    }

    /// Resolves a re-entering root against the arena and checks exact
    /// metadata agreement, returning the stored node. Every traversal entry
    /// funnels through here so forged lengths or unknown identifiers fail
    /// closed before any content work.
    pub(super) fn node_for_root_on<A: ArenaStore>(
        arena: &A,
        root: V2RootRecord,
    ) -> Result<ArenaNode, V2AvlError> {
        let node = arena.arena_load_node(root.node_id())?;
        let canonical = V2RootRecord::from_node(root.node_id(), node.record())?;
        if canonical != root {
            return Err(V2AvlError::Invalid(
                "v2 root metadata disagrees with its arena node",
            ));
        }
        Ok(node)
    }

    /// Validates one resolved node's stored record against its content: a
    /// leaf recomputes from its complete payload bytes, a branch recomputes
    /// from its child roots. Pure metadata agreement (above) is not enough:
    /// a stored record with intact identity but corrupt commitment must fail
    /// every path that visits it.
    fn validate_node_on<A: ArenaStore>(arena: &A, node: &ArenaNode) -> Result<(), V2AvlError> {
        match node {
            ArenaNode::Leaf {
                payload_offset,
                payload_len,
                record,
            } => {
                let payload_end = payload_offset
                    .checked_add(*payload_len)
                    .ok_or(V2AvlError::Overflow("v2 leaf payload range exceeds u64"))?;
                let bytes = arena.arena_read_payload(*payload_offset, payload_end)?;
                let expected = V2NodeRecord::leaf(*payload_offset, &bytes)?;
                if expected != *record {
                    return Err(V2AvlError::Invalid(
                        "v2 leaf metadata or commitment verification failed",
                    ));
                }
                Ok(())
            }
            ArenaNode::Branch {
                left,
                right,
                record,
            } => {
                let expected = V2NodeRecord::branch(*left, *right)?;
                if expected != *record {
                    return Err(V2AvlError::Invalid(
                        "v2 branch metadata or commitment verification failed",
                    ));
                }
                Ok(())
            }
        }
    }

    fn payload_slice(&self, start: u64, end: u64) -> Result<&[u8], V2AvlError> {
        let start = usize::try_from(start)
            .map_err(|_| V2AvlError::Overflow("v2 payload start exceeds usize"))?;
        let end = usize::try_from(end)
            .map_err(|_| V2AvlError::Overflow("v2 payload end exceeds usize"))?;
        self.payload
            .get(start..end)
            .ok_or(V2AvlError::Invalid("v2 payload range is outside the arena"))
    }

    fn verify_node_on<A: ArenaStore>(
        arena: &A,
        root: V2RootRecord,
        visited: &mut u64,
    ) -> Result<(), V2AvlError> {
        *visited = visited.saturating_add(1);
        let node = Self::node_for_root_on(arena, root)?;
        Self::validate_node_on(arena, &node)?;
        if let ArenaNode::Branch { left, right, .. } = node {
            Self::verify_node_on(arena, left, visited)?;
            Self::verify_node_on(arena, right, visited)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistent_sequence::image_v2::corrupt_first_branch_child_for_test;

    fn append_leaf(sequence: &mut V2AvlSequence, byte: u8) -> V2RootRecord {
        sequence
            .append(None, &[byte])
            .expect("test leaf append should succeed")
            .root()
    }

    fn ceil_log2(value: usize) -> usize {
        if value <= 1 {
            return 0;
        }
        usize::BITS as usize - (value - 1).leading_zeros() as usize
    }

    #[test]
    fn sequential_appends_preserve_history_and_logarithmic_height() {
        let mut sequence = V2AvlSequence::default();
        let mut root = None;
        let mut expected = Vec::new();
        let mut snapshots = Vec::new();

        for index in 0..4096usize {
            let chunk_len = (index * 17 % 31) + 1;
            let byte = b'a' + u8::try_from(index % 26).expect("alphabet index fits u8");
            let chunk = vec![byte; chunk_len];
            let result = sequence
                .append(root, &chunk)
                .expect("sequential append should succeed");
            let next = result.root();
            expected.extend_from_slice(&chunk);
            assert_eq!(next.logical_len(), u64::try_from(expected.len()).unwrap());
            assert!(result.allocated_nodes() <= usize::from(next.height()) * 2 + 2);

            let checkpoint = index < 8 || index.is_power_of_two() || index % 511 == 0;
            if checkpoint {
                sequence
                    .verify_root(next)
                    .expect("checkpoint root should verify");
                snapshots.push((next, expected.clone()));
            }
            root = Some(next);
        }

        let root = root.expect("at least one append produced a root");
        sequence
            .verify_root(root)
            .expect("final root should verify after sequential appends");
        let height_bound = 2 * ceil_log2(4097) + 1;
        assert!(usize::from(root.height()) <= height_bound);
        assert!(sequence.node_count() < 4096 * height_bound);

        for (historical_root, historical_bytes) in snapshots {
            sequence
                .verify_root(historical_root)
                .expect("historical root should remain valid");
            assert_eq!(
                sequence
                    .read_range(historical_root, 0, historical_root.logical_len())
                    .expect("historical root should remain readable"),
                historical_bytes
            );
        }
    }

    #[test]
    fn append_to_historical_root_creates_sibling_without_mutating_parent() {
        let mut sequence = V2AvlSequence::default();
        let root_a = sequence
            .append(None, b"root")
            .expect("root append should succeed")
            .root();
        let root_b = sequence
            .append(Some(root_a), b"-left")
            .expect("left append should succeed")
            .root();
        let root_c = sequence
            .append(Some(root_a), b"-right")
            .expect("right append should succeed")
            .root();

        assert_eq!(sequence.read_range(root_a, 0, 4).unwrap(), b"root");
        assert_eq!(sequence.read_range(root_b, 0, 9).unwrap(), b"root-left");
        assert_eq!(sequence.read_range(root_c, 0, 10).unwrap(), b"root-right");
        sequence.verify_root(root_a).unwrap();
        sequence.verify_root(root_b).unwrap();
        sequence.verify_root(root_c).unwrap();
    }

    #[test]
    fn range_reads_cross_leaf_and_rotation_boundaries_exactly() {
        let mut sequence = V2AvlSequence::default();
        let chunks: [&[u8]; 8] = [
            b"abc",
            b"defgh",
            b"ij",
            b"klmnop",
            b"q",
            b"rstuv",
            b"wxyz",
            b"0123456789",
        ];
        let mut root = None;
        let mut expected = Vec::new();
        for chunk in chunks {
            expected.extend_from_slice(chunk);
            root = Some(sequence.append(root, chunk).unwrap().root());
        }
        let root = root.unwrap();

        for offset in 0..=expected.len() {
            let remaining = expected.len() - offset;
            for length in [0, remaining.min(1), remaining.min(4), remaining] {
                assert_eq!(
                    sequence
                        .read_range(
                            root,
                            u64::try_from(offset).unwrap(),
                            u64::try_from(length).unwrap(),
                        )
                        .unwrap(),
                    expected[offset..offset + length]
                );
            }
        }
    }

    #[test]
    fn rebalance_exercises_single_and_double_rotations() {
        let mut sequence = V2AvlSequence::default();
        let mut work = 0usize;

        let a = append_leaf(&mut sequence, b'a');
        let b = append_leaf(&mut sequence, b'b');
        let c = append_leaf(&mut sequence, b'c');
        let d = append_leaf(&mut sequence, b'd');
        let cd = sequence.allocate_branch(c, d, &mut work).unwrap();
        let bcd = sequence.allocate_branch(b, cd, &mut work).unwrap();
        let single = sequence.rebalance(a, bcd, &mut work).unwrap();
        assert_eq!(single.height(), 3);
        assert_eq!(sequence.read_range(single, 0, 4).unwrap(), b"abcd");
        sequence.verify_root(single).unwrap();

        let e = append_leaf(&mut sequence, b'e');
        let f = append_leaf(&mut sequence, b'f');
        let g = append_leaf(&mut sequence, b'g');
        let h = append_leaf(&mut sequence, b'h');
        let fg = sequence.allocate_branch(f, g, &mut work).unwrap();
        let fgh = sequence.allocate_branch(fg, h, &mut work).unwrap();
        let double = sequence.rebalance(e, fgh, &mut work).unwrap();
        assert_eq!(double.height(), 3);
        assert_eq!(sequence.read_range(double, 0, 4).unwrap(), b"efgh");
        sequence.verify_root(double).unwrap();
    }

    #[test]
    fn failed_append_rolls_back_arena_growth() {
        let mut sequence = V2AvlSequence::default();
        let root = sequence.append(None, b"stable").unwrap().root();
        let nodes_before = sequence.nodes.len();
        let payload_before = sequence.payload.len();

        // An empty append canonicalizes to a zero-effect splice, which the
        // splice engine rejects: one canonical mutation, one encoding.
        assert_eq!(
            sequence.append(Some(root), b""),
            Err(V2AvlError::Invalid(
                "v2 sequence splice without effect is rejected"
            ))
        );
        assert_eq!(sequence.nodes.len(), nodes_before);
        assert_eq!(sequence.payload.len(), payload_before);
        assert_eq!(sequence.read_range(root, 0, 6).unwrap(), b"stable");
    }

    #[test]
    fn image_round_trip_preserves_historical_roots_and_future_appends() {
        let mut sequence = V2AvlSequence::default();
        let root_a = sequence.append(None, b"root").unwrap().root();
        let root_b = sequence.append(Some(root_a), b"-left").unwrap().root();
        let root_c = sequence.append(Some(root_a), b"-right").unwrap().root();
        let mut latest = root_b;
        for index in 0..128u16 {
            latest = sequence
                .append(Some(latest), &index.to_le_bytes())
                .unwrap()
                .root();
        }
        let retained = vec![root_a, root_b, root_c, latest];
        let expected_latest = sequence
            .read_range(latest, 0, latest.logical_len())
            .expect("latest root should be readable before export");

        let encoded = sequence
            .export_image(&retained)
            .expect("v2 image export should succeed");
        let (mut reopened, reopened_roots) =
            V2AvlSequence::import_image(&encoded).expect("v2 image import should succeed");
        assert_eq!(reopened_roots, retained);
        assert_eq!(reopened.read_range(root_a, 0, 4).unwrap(), b"root");
        assert_eq!(reopened.read_range(root_b, 0, 9).unwrap(), b"root-left");
        assert_eq!(reopened.read_range(root_c, 0, 10).unwrap(), b"root-right");
        assert_eq!(
            reopened
                .read_range(latest, 0, latest.logical_len())
                .unwrap(),
            expected_latest
        );

        let extended = reopened
            .append(Some(latest), b"-after-reopen")
            .expect("append after reopen should succeed")
            .root();
        let mut expected_extended = expected_latest;
        expected_extended.extend_from_slice(b"-after-reopen");
        assert_eq!(
            reopened
                .read_range(extended, 0, extended.logical_len())
                .unwrap(),
            expected_extended
        );
        reopened.verify_root(extended).unwrap();
    }

    #[test]
    fn image_import_rejects_semantic_corruption_with_valid_outer_digest() {
        let mut sequence = V2AvlSequence::default();
        let root = sequence.append(None, b"a").unwrap().root();
        let root = sequence.append(Some(root), b"b").unwrap().root();
        let root = sequence.append(Some(root), b"c").unwrap().root();
        let mut encoded = sequence.export_image(&[root]).unwrap();
        corrupt_first_branch_child_for_test(&mut encoded, 100_000)
            .expect("test corruption should find a branch");

        assert!(matches!(
            V2AvlSequence::import_image(&encoded),
            Err(V2AvlError::Invalid(
                "v2 image branch child must reference an earlier arena node"
            ))
        ));
    }

    fn splice_bytes(
        sequence: &mut V2AvlSequence,
        parent: Option<V2RootRecord>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
    ) -> Vec<u8> {
        let root = sequence
            .splice(parent, offset, delete_len, insert)
            .expect("test splice should succeed")
            .root();
        sequence
            .verify_root(root)
            .expect("splice root should verify");
        let len = root.logical_len();
        sequence
            .read_range(root, 0, len)
            .expect("splice root should read exactly")
    }

    fn splice_root(
        sequence: &mut V2AvlSequence,
        parent: Option<V2RootRecord>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
    ) -> V2RootRecord {
        let result = sequence
            .splice(parent, offset, delete_len, insert)
            .expect("test splice should succeed");
        sequence
            .verify_root(result.root())
            .expect("splice root should verify");
        result.root()
    }

    #[test]
    fn splice_insert_start_middle_end_is_exact() {
        let mut sequence = V2AvlSequence::default();
        let base = splice_root(&mut sequence, None, 0, 0, b"abcdef");
        assert_eq!(
            splice_bytes(&mut sequence, Some(base), 0, 0, b"START-"),
            b"START-abcdef"
        );
        assert_eq!(
            splice_bytes(&mut sequence, Some(base), 3, 0, b"-MID-"),
            b"abc-MID-def"
        );
        assert_eq!(
            splice_bytes(&mut sequence, Some(base), 6, 0, b"-END"),
            b"abcdef-END"
        );
        // The source root is untouched by every edit.
        assert_eq!(sequence.read_range(base, 0, 6).unwrap(), b"abcdef");
    }

    #[test]
    fn splice_delete_start_middle_end_across_leaves_is_exact() {
        let mut sequence = V2AvlSequence::default();
        // Three appends force multiple leaves so deletions span them.
        let mut base = None;
        for chunk in [b"aa".as_slice(), b"bbbb".as_slice(), b"cc".as_slice()] {
            base = Some(sequence.append(base, chunk).unwrap().root());
        }
        let base = base.unwrap();
        assert_eq!(sequence.read_range(base, 0, 8).unwrap(), b"aabbbbcc");
        assert_eq!(
            splice_bytes(&mut sequence, Some(base), 0, 2, b""),
            b"bbbbcc"
        );
        assert_eq!(splice_bytes(&mut sequence, Some(base), 2, 4, b""), b"aacc");
        assert_eq!(
            splice_bytes(&mut sequence, Some(base), 6, 2, b""),
            b"aabbbb"
        );
        assert_eq!(splice_bytes(&mut sequence, Some(base), 1, 6, b""), b"ac");
        assert_eq!(sequence.read_range(base, 0, 8).unwrap(), b"aabbbbcc");
    }

    #[test]
    fn splice_replace_equal_shorter_longer_cross_leaf_is_exact() {
        let mut sequence = V2AvlSequence::default();
        let mut base = None;
        for chunk in [b"0123456789".as_slice(), b"ABCDEFGHIJ".as_slice()] {
            base = Some(sequence.append(base, chunk).unwrap().root());
        }
        let base = base.unwrap();
        // Equal length, interior, spanning the leaf boundary.
        assert_eq!(
            splice_bytes(&mut sequence, Some(base), 8, 4, b"xxxx"),
            b"01234567xxxxCDEFGHIJ"
        );
        // Shorter.
        assert_eq!(
            splice_bytes(&mut sequence, Some(base), 5, 10, b"Q"),
            b"01234QFGHIJ"
        );
        // Longer, reaching both ends.
        assert_eq!(
            splice_bytes(&mut sequence, Some(base), 2, 16, b"0123456789ABCDEFGHIJ"),
            b"010123456789ABCDEFGHIJIJ"
        );
        assert_eq!(
            sequence.read_range(base, 0, 20).unwrap(),
            b"0123456789ABCDEFGHIJ"
        );
    }

    #[test]
    fn splice_preserves_history_across_branches() {
        let mut sequence = V2AvlSequence::default();
        let v0 = splice_root(&mut sequence, None, 0, 0, b"base-payload");
        let v1 = splice_root(&mut sequence, Some(v0), 5, 0, b"[edit1]");
        assert_eq!(
            sequence.read_range(v1, 0, v1.logical_len()).unwrap(),
            b"base-[edit1]payload"
        );
        // A sibling edit off the same historical parent leaves both intact.
        let v2 = splice_root(&mut sequence, Some(v0), 0, 4, b"EDIT");
        assert_eq!(
            sequence.read_range(v2, 0, v2.logical_len()).unwrap(),
            b"EDIT-payload"
        );
        for (root, expected) in [
            (v0, b"base-payload".as_slice()),
            (v1, b"base-[edit1]payload".as_slice()),
            (v2, b"EDIT-payload".as_slice()),
        ] {
            sequence.verify_root(root).unwrap();
            assert_eq!(
                sequence.read_range(root, 0, root.logical_len()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn splice_rejects_invalid_coordinates_without_allocation() {
        let mut sequence = V2AvlSequence::default();
        let base = splice_root(&mut sequence, None, 0, 0, b"abcdef");
        let nodes_before = sequence.node_count();
        let cases = [
            // offset past the end
            (Some(base), 7, 0, b"x".as_slice()),
            (Some(base), u64::MAX, 0, b"x"),
            // delete past the end
            (Some(base), 5, 2, b""),
            (Some(base), 0, 7, b""),
            // zero-effect splice
            (Some(base), 3, 0, b""),
            // zero-result splice
            (Some(base), 0, 6, b""),
            // root creation violations
            (None, 1, 0, b"x"),
            (None, 0, 1, b"x"),
            (None, 0, 0, b""),
            // cross-history shape is a history-layer rule; the AVL layer
            // rejects the unknown-root coordinate here
        ];
        for (parent, offset, delete_len, insert) in cases {
            assert!(
                sequence.splice(parent, offset, delete_len, insert).is_err(),
                "offset={offset} delete={delete_len} must fail"
            );
        }
        // Rejections allocate nothing and disturb no root.
        assert_eq!(sequence.node_count(), nodes_before);
        assert_eq!(sequence.read_range(base, 0, 6).unwrap(), b"abcdef");
        // A root from another arena fails closed at resolution even when the
        // node identifier collides: commitments disagree.
        let mut foreign = V2AvlSequence::default();
        let foreign_root = splice_root(&mut foreign, None, 0, 0, b"foreign");
        assert!(sequence.splice(Some(foreign_root), 0, 0, b"x").is_err());
    }

    #[test]
    fn root_creation_chunks_large_payload_into_bounded_leaves() {
        let mut sequence = V2AvlSequence::default();
        let payload = vec![0xABu8; 100 * 1024];
        let result = sequence
            .splice(None, 0, 0, &payload)
            .expect("large root creation should succeed");
        // 100 KiB needs 7 bounded leaves plus branch structure: never one
        // giant leaf, and the arena holds exactly the payload once.
        assert!(result.allocated_nodes() >= 7);
        assert_eq!(result.payload_bytes_allocated(), 100 * 1024);
        let root = result.root();
        assert_eq!(root.logical_len(), 100 * 1024);
        assert_eq!(sequence.read_range(root, 0, 100 * 1024).unwrap(), payload);
        sequence.verify_root(root).unwrap();
        // The chunked image round-trips through the bound-enforcing import.
        let image = sequence.export_image(&[root]).unwrap();
        let (rebuilt, roots) = V2AvlSequence::import_image(&image).unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(
            rebuilt.read_range(roots[0], 0, 100 * 1024).unwrap(),
            payload
        );
    }

    #[test]
    fn node_codec_rejects_oversized_staging_leaf() {
        use crate::persistent_sequence::format_v2::{
            decode_v2_node, encode_v2_node, V2NodeRecord, MAX_LEAF_PAYLOAD_BYTES,
        };

        let record = encode_v2_node(V2NodeRecord::leaf(0, b"ok").unwrap());
        let mut oversized = record;
        let wide = (MAX_LEAF_PAYLOAD_BYTES + 1) as u64;
        oversized[8..16].copy_from_slice(&wide.to_le_bytes());
        oversized[24..32].copy_from_slice(&wide.to_le_bytes());
        assert_eq!(
            decode_v2_node(&oversized),
            Err(V2FormatError::Invalid(
                "v2 leaf payload exceeds the staging bound"
            ))
        );
        // The constructor gate agrees: no valid leaf can exceed the bound.
        assert!(matches!(
            V2NodeRecord::leaf(0, &vec![0u8; MAX_LEAF_PAYLOAD_BYTES + 1]),
            Err(V2FormatError::Invalid(_))
        ));
    }
}
