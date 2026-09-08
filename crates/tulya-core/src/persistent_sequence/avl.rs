//! Pure persistent AVL sequence core for Format v2.
//!
//! This module owns immutable path-copy edits and range navigation. It has no
//! filesystem, WAL, manifest, or publication responsibilities. Nodes are never
//! mutated after allocation, so every returned root remains a valid historical
//! snapshot while later appends allocate only a new leaf and the changed AVL
//! path.

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
}

impl fmt::Display for V2AvlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(error) => write!(formatter, "{error}"),
            Self::Image(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) => formatter.write_str(message),
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
enum ArenaNode {
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

impl ArenaNode {
    const fn record(&self) -> V2NodeRecord {
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
        let offset = parent.map_or(0, V2RootRecord::logical_len);
        let result = self.splice(parent, offset, 0, bytes)?;
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
        let source_len = match parent {
            None => 0,
            Some(root) => {
                self.node_for_root(root)?;
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

        let payload_start = self.payload.len();
        let node_start = self.nodes.len();
        let mut inspected = 0usize;
        match self.splice_inner(parent, offset, delete_len, insert, &mut inspected) {
            Ok(root) => Ok(V2SpliceResult {
                root,
                allocated_nodes: self.nodes.len() - node_start,
                inspected_nodes: inspected,
                payload_bytes_allocated: self.payload.len() - payload_start,
            }),
            Err(error) => {
                self.payload.truncate(payload_start);
                self.nodes.truncate(node_start);
                Err(error)
            }
        }
    }

    fn splice_inner(
        &mut self,
        parent: Option<V2RootRecord>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        let middle = if insert.is_empty() {
            None
        } else {
            Some(self.build_insert_subtree(insert, inspected)?)
        };
        let Some(root) = parent else {
            // Root creation validated non-empty above; the option unwrap
            // stays a closed error rather than a panic.
            return middle.ok_or(V2AvlError::Invalid(
                "v2 sequence root creation produced no content",
            ));
        };
        let (left, mid_right) = self.split(root, offset, inspected)?;
        let right = match mid_right {
            None => None,
            Some(mid) => {
                if delete_len == 0 {
                    Some(mid)
                } else {
                    let (_, right) = self.split(mid, delete_len, inspected)?;
                    right
                }
            }
        };
        let combined = self.concat_optional(left, middle, inspected)?;
        let combined = self.concat_optional(combined, right, inspected)?;
        combined.ok_or(V2AvlError::Invalid(
            "v2 sequence splice produced an empty root",
        ))
    }

    /// Persistent split at `offset`: left holds `[0..offset)`, right holds
    /// `[offset..len)`. Either side is `None` exactly at the boundaries.
    /// Only the descent path is copied and rejoined through the balancing
    /// concat, so both halves stay valid AVL trees sharing every untouched
    /// node with the source.
    fn split(
        &mut self,
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
        let node = self.node_for_root(root)?.clone();
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
                // Bounded boundary fragments: the source leaf itself respects
                // the staging bound, so both copies stay within it.
                let left_bytes = self.payload_slice(payload_offset, left_end)?.to_vec();
                let right_bytes = self.payload_slice(left_end, leaf_end)?.to_vec();
                let left = self.allocate_leaf(&left_bytes)?;
                let right = self.allocate_leaf(&right_bytes)?;
                Ok((Some(left), Some(right)))
            }
            ArenaNode::Branch { left, right, .. } => {
                let left_len = left.logical_len();
                match offset.cmp(&left_len) {
                    std::cmp::Ordering::Less => {
                        let (far_left, near) = self.split(left, offset, inspected)?;
                        let joined_right = match near {
                            None => right,
                            Some(near) => self.concat(near, right, inspected)?,
                        };
                        Ok((far_left, Some(joined_right)))
                    }
                    std::cmp::Ordering::Equal => Ok((Some(left), Some(right))),
                    std::cmp::Ordering::Greater => {
                        let (near, far_right) = self.split(right, offset - left_len, inspected)?;
                        let joined_left = match near {
                            None => left,
                            Some(near) => self.concat(left, near, inspected)?,
                        };
                        Ok((Some(joined_left), far_right))
                    }
                }
            }
        }
    }

    /// Concatenation over possibly empty sides, so split/splice represent
    /// temporary empty pieces without an externally visible empty root.
    fn concat_optional(
        &mut self,
        left: Option<V2RootRecord>,
        right: Option<V2RootRecord>,
        inspected: &mut usize,
    ) -> Result<Option<V2RootRecord>, V2AvlError> {
        match (left, right) {
            (None, None) => Ok(None),
            (None, Some(root)) | (Some(root), None) => Ok(Some(root)),
            (Some(left), Some(right)) => Ok(Some(self.concat(left, right, inspected)?)),
        }
    }

    /// Builds a balanced subtree for inserted bytes in linear chunk work:
    /// bounded leaves first, then bottom-up pairing rounds that halve the
    /// level each round instead of repeatedly path-copying a growing tree.
    fn build_insert_subtree(
        &mut self,
        insert: &[u8],
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        let chunks = insert.len().div_ceil(MAX_LEAF_PAYLOAD_BYTES);
        let mut level = Vec::new();
        level
            .try_reserve_exact(chunks)
            .map_err(|_| V2AvlError::Invalid("v2 sequence insert level allocation failed"))?;
        for chunk in insert.chunks(MAX_LEAF_PAYLOAD_BYTES) {
            level.push(self.allocate_leaf(chunk)?);
        }
        while level.len() > 1 {
            let mut next = Vec::new();
            next.try_reserve_exact(level.len().div_ceil(2))
                .map_err(|_| V2AvlError::Invalid("v2 sequence insert level allocation failed"))?;
            let mut index = 0;
            while index < level.len() {
                if index + 1 < level.len() {
                    next.push(self.concat(level[index], level[index + 1], inspected)?);
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

    /// Allocates one bounded leaf. The record constructor enforces
    /// non-emptiness and the staging payload bound.
    fn allocate_leaf(&mut self, bytes: &[u8]) -> Result<V2RootRecord, V2AvlError> {
        let payload_offset = u64::try_from(self.payload.len())
            .map_err(|_| V2AvlError::Overflow("v2 payload arena length exceeds u64"))?;
        let record = V2NodeRecord::leaf(payload_offset, bytes)?;
        let payload_len = record.logical_len();
        self.payload.extend_from_slice(bytes);
        self.allocate_node(ArenaNode::Leaf {
            payload_offset,
            payload_len,
            record,
        })
    }

    /// Returns an exact logical byte range from one retained root.
    pub(super) fn read_range(
        &self,
        root: V2RootRecord,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, V2AvlError> {
        Ok(self.read_range_counted(root, offset, length)?.0)
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
        self.node_for_root(root)?;
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
            match self.node_for_root(current)? {
                ArenaNode::Leaf {
                    payload_offset,
                    payload_len,
                    ..
                } => {
                    let local_end = local_offset
                        .checked_add(local_length)
                        .ok_or(V2AvlError::Overflow("v2 leaf range exceeds u64"))?;
                    if local_end > *payload_len {
                        return Err(V2AvlError::Invalid("v2 leaf range exceeds leaf payload"));
                    }
                    let start = payload_offset
                        .checked_add(local_offset)
                        .ok_or(V2AvlError::Overflow("v2 payload start exceeds u64"))?;
                    let end = start
                        .checked_add(local_length)
                        .ok_or(V2AvlError::Overflow("v2 payload end exceeds u64"))?;
                    output.extend_from_slice(self.payload_slice(start, end)?);
                }
                ArenaNode::Branch { left, right, .. } => {
                    let left_len = left.logical_len();
                    if local_offset < left_len {
                        let left_available = left_len - local_offset;
                        let left_length = local_length.min(left_available);
                        let right_length = local_length - left_length;
                        if right_length > 0 {
                            stack.push((*right, 0, right_length));
                        }
                        if left_length > 0 {
                            stack.push((*left, local_offset, left_length));
                        }
                    } else {
                        stack.push((*right, local_offset - left_len, local_length));
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
        let _ = self.verify_root_counted(root)?;
        Ok(())
    }

    /// Verifies like [`V2AvlSequence::verify_root`] and reports how many arena
    /// nodes were revalidated.
    pub(super) fn verify_root_counted(&self, root: V2RootRecord) -> Result<u64, V2AvlError> {
        let mut visited = 0u64;
        self.verify_node(root, &mut visited)?;
        Ok(visited)
    }

    pub(super) fn node_count(&self) -> usize {
        self.nodes.len()
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
            self.node_for_root(*root)?;
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
            sequence.node_for_root(*root)?;
        }
        Ok((sequence, image.roots))
    }

    /// Persistent AVL concatenation. Only the changed spine is copied.
    fn concat(
        &mut self,
        left: V2RootRecord,
        right: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        if left.height() > right.height().saturating_add(1) {
            let (left_left, left_right) = self.branch_children(left, inspected)?;
            let joined = self.concat(left_right, right, inspected)?;
            return self.rebalance(left_left, joined, inspected);
        }
        if right.height() > left.height().saturating_add(1) {
            let (right_left, right_right) = self.branch_children(right, inspected)?;
            let joined = self.concat(left, right_left, inspected)?;
            return self.rebalance(joined, right_right, inspected);
        }
        self.allocate_branch(left, right, inspected)
    }

    /// Restores the AVL height invariant with a single or double rotation.
    fn rebalance(
        &mut self,
        left: V2RootRecord,
        right: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        if left.height().abs_diff(right.height()) <= 1 {
            return self.allocate_branch(left, right, inspected);
        }

        if left.height() > right.height() {
            let (left_left, left_right) = self.branch_children(left, inspected)?;
            if left_left.height() >= left_right.height() {
                let new_right = self.allocate_branch(left_right, right, inspected)?;
                return self.allocate_branch(left_left, new_right, inspected);
            }
            let (middle_left, middle_right) = self.branch_children(left_right, inspected)?;
            let new_left = self.allocate_branch(left_left, middle_left, inspected)?;
            let new_right = self.allocate_branch(middle_right, right, inspected)?;
            return self.allocate_branch(new_left, new_right, inspected);
        }

        let (right_left, right_right) = self.branch_children(right, inspected)?;
        if right_right.height() >= right_left.height() {
            let new_left = self.allocate_branch(left, right_left, inspected)?;
            return self.allocate_branch(new_left, right_right, inspected);
        }
        let (middle_left, middle_right) = self.branch_children(right_left, inspected)?;
        let new_left = self.allocate_branch(left, middle_left, inspected)?;
        let new_right = self.allocate_branch(middle_right, right_right, inspected)?;
        self.allocate_branch(new_left, new_right, inspected)
    }

    fn allocate_branch(
        &mut self,
        left: V2RootRecord,
        right: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<V2RootRecord, V2AvlError> {
        // Both child validations below resolve one arena node each.
        *inspected = inspected.saturating_add(2);
        self.node_for_root(left)?;
        self.node_for_root(right)?;
        let record = V2NodeRecord::branch(left, right)?;
        self.allocate_node(ArenaNode::Branch {
            left,
            right,
            record,
        })
    }

    fn allocate_node(&mut self, node: ArenaNode) -> Result<V2RootRecord, V2AvlError> {
        let node_id = u64::try_from(self.nodes.len())
            .map_err(|_| V2AvlError::Overflow("v2 node arena length exceeds u64"))?;
        let root = V2RootRecord::from_node(node_id, node.record())?;
        self.nodes.push(node);
        Ok(root)
    }

    fn branch_children(
        &self,
        root: V2RootRecord,
        inspected: &mut usize,
    ) -> Result<(V2RootRecord, V2RootRecord), V2AvlError> {
        // The single resolution below reads one arena node.
        *inspected = inspected.saturating_add(1);
        match self.node_for_root(root)? {
            ArenaNode::Branch { left, right, .. } => Ok((*left, *right)),
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
        let index = usize::try_from(node_id)
            .map_err(|_| V2AvlError::Overflow("v2 node identifier exceeds usize"))?;
        let node = self.nodes.get(index).ok_or(V2AvlError::Invalid(
            "v2 root references a missing arena node",
        ))?;
        Ok(V2RootRecord::from_node(node_id, node.record())?)
    }

    fn node_for_root(&self, root: V2RootRecord) -> Result<&ArenaNode, V2AvlError> {
        let index = usize::try_from(root.node_id())
            .map_err(|_| V2AvlError::Overflow("v2 node identifier exceeds usize"))?;
        let node = self.nodes.get(index).ok_or(V2AvlError::Invalid(
            "v2 root references a missing arena node",
        ))?;
        let canonical = V2RootRecord::from_node(root.node_id(), node.record())?;
        if canonical != root {
            return Err(V2AvlError::Invalid(
                "v2 root metadata disagrees with its arena node",
            ));
        }
        Ok(node)
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

    fn verify_node(&self, root: V2RootRecord, visited: &mut u64) -> Result<(), V2AvlError> {
        *visited = visited.saturating_add(1);
        match self.node_for_root(root)? {
            ArenaNode::Leaf {
                payload_offset,
                payload_len,
                record,
            } => {
                let payload_end = payload_offset
                    .checked_add(*payload_len)
                    .ok_or(V2AvlError::Overflow("v2 leaf payload range exceeds u64"))?;
                let expected = V2NodeRecord::leaf(
                    *payload_offset,
                    self.payload_slice(*payload_offset, payload_end)?,
                )?;
                if expected != *record {
                    return Err(V2AvlError::Invalid(
                        "v2 leaf metadata or commitment verification failed",
                    ));
                }
            }
            ArenaNode::Branch {
                left,
                right,
                record,
            } => {
                self.verify_node(*left, visited)?;
                self.verify_node(*right, visited)?;
                let expected = V2NodeRecord::branch(*left, *right)?;
                if expected != *record {
                    return Err(V2AvlError::Invalid(
                        "v2 branch metadata or commitment verification failed",
                    ));
                }
            }
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
