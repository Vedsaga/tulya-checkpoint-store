//! Pure persistent AVL sequence core for Format v2.
//!
//! This module owns immutable path-copy edits and range navigation. It has no
//! filesystem, WAL, manifest, or publication responsibilities. Nodes are never
//! mutated after allocation, so every returned root remains a valid historical
//! snapshot while later appends allocate only a new leaf and the changed AVL
//! path.

use super::format_v2::{V2FormatError, V2NodeRecord, V2RootRecord};
use super::image_v2::{
    decode_v2_image, encode_v2_image, v2_node_fields, V2ImageError, V2NodeFields,
    V2SequenceImage,
};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2AvlError {
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
}

impl V2AppendResult {
    pub(super) const fn root(self) -> V2RootRecord {
        self.root
    }

    pub(super) const fn allocated_nodes(self) -> usize {
        self.allocated_nodes
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
    /// An error rolls back every allocation made by this call. Successful
    /// appends allocate one leaf plus only nodes on the changed AVL path.
    pub(super) fn append(
        &mut self,
        parent: Option<V2RootRecord>,
        bytes: &[u8],
    ) -> Result<V2AppendResult, V2AvlError> {
        if bytes.is_empty() {
            return Err(V2AvlError::Invalid(
                "v2 sequence append payload must be non-empty",
            ));
        }
        if let Some(root) = parent {
            self.node_for_root(root)?;
        }

        let payload_start = self.payload.len();
        let node_start = self.nodes.len();
        match self.append_inner(parent, bytes) {
            Ok(root) => Ok(V2AppendResult {
                root,
                allocated_nodes: self.nodes.len() - node_start,
            }),
            Err(error) => {
                self.payload.truncate(payload_start);
                self.nodes.truncate(node_start);
                Err(error)
            }
        }
    }

    /// Returns an exact logical byte range from one retained root.
    pub(super) fn read_range(
        &self,
        root: V2RootRecord,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, V2AvlError> {
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
            return Ok(output);
        }

        let mut stack = vec![(root, offset, length)];
        while let Some((current, local_offset, local_length)) = stack.pop() {
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
        Ok(output)
    }

    /// Recomputes every reachable node's metadata and commitment.
    pub(super) fn verify_root(&self, root: V2RootRecord) -> Result<(), V2AvlError> {
        self.verify_node(root)?;
        Ok(())
    }

    pub(super) fn node_count(&self) -> usize {
        self.nodes.len()
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
    pub(super) fn import_image(
        bytes: &[u8],
    ) -> Result<(Self, Vec<V2RootRecord>), V2AvlError> {
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
                    let payload_end = payload_offset
                        .checked_add(payload_len)
                        .ok_or(V2AvlError::Overflow("v2 image leaf payload range exceeds u64"))?;
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

    fn append_inner(
        &mut self,
        parent: Option<V2RootRecord>,
        bytes: &[u8],
    ) -> Result<V2RootRecord, V2AvlError> {
        let payload_offset = u64::try_from(self.payload.len())
            .map_err(|_| V2AvlError::Overflow("v2 payload arena length exceeds u64"))?;
        let record = V2NodeRecord::leaf(payload_offset, bytes)?;
        let payload_len = record.logical_len();
        self.payload.extend_from_slice(bytes);
        let leaf = self.allocate_node(ArenaNode::Leaf {
            payload_offset,
            payload_len,
            record,
        })?;
        match parent {
            Some(root) => self.concat(root, leaf),
            None => Ok(leaf),
        }
    }

    /// Persistent AVL concatenation. Only the changed spine is copied.
    fn concat(
        &mut self,
        left: V2RootRecord,
        right: V2RootRecord,
    ) -> Result<V2RootRecord, V2AvlError> {
        if left.height() > right.height().saturating_add(1) {
            let (left_left, left_right) = self.branch_children(left)?;
            let joined = self.concat(left_right, right)?;
            return self.rebalance(left_left, joined);
        }
        if right.height() > left.height().saturating_add(1) {
            let (right_left, right_right) = self.branch_children(right)?;
            let joined = self.concat(left, right_left)?;
            return self.rebalance(joined, right_right);
        }
        self.allocate_branch(left, right)
    }

    /// Restores the AVL height invariant with a single or double rotation.
    fn rebalance(
        &mut self,
        left: V2RootRecord,
        right: V2RootRecord,
    ) -> Result<V2RootRecord, V2AvlError> {
        if left.height().abs_diff(right.height()) <= 1 {
            return self.allocate_branch(left, right);
        }

        if left.height() > right.height() {
            let (left_left, left_right) = self.branch_children(left)?;
            if left_left.height() >= left_right.height() {
                let new_right = self.allocate_branch(left_right, right)?;
                return self.allocate_branch(left_left, new_right);
            }
            let (middle_left, middle_right) = self.branch_children(left_right)?;
            let new_left = self.allocate_branch(left_left, middle_left)?;
            let new_right = self.allocate_branch(middle_right, right)?;
            return self.allocate_branch(new_left, new_right);
        }

        let (right_left, right_right) = self.branch_children(right)?;
        if right_right.height() >= right_left.height() {
            let new_left = self.allocate_branch(left, right_left)?;
            return self.allocate_branch(new_left, right_right);
        }
        let (middle_left, middle_right) = self.branch_children(right_left)?;
        let new_left = self.allocate_branch(left, middle_left)?;
        let new_right = self.allocate_branch(middle_right, right_right)?;
        self.allocate_branch(new_left, new_right)
    }

    fn allocate_branch(
        &mut self,
        left: V2RootRecord,
        right: V2RootRecord,
    ) -> Result<V2RootRecord, V2AvlError> {
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
    ) -> Result<(V2RootRecord, V2RootRecord), V2AvlError> {
        match self.node_for_root(root)? {
            ArenaNode::Branch { left, right, .. } => Ok((*left, *right)),
            ArenaNode::Leaf { .. } => Err(V2AvlError::Invalid(
                "v2 AVL traversal expected a branch node",
            )),
        }
    }

    fn root_for_node_id(&self, node_id: u64) -> Result<V2RootRecord, V2AvlError> {
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

    fn verify_node(&self, root: V2RootRecord) -> Result<(), V2AvlError> {
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
                self.verify_node(*left)?;
                self.verify_node(*right)?;
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

        let a = append_leaf(&mut sequence, b'a');
        let b = append_leaf(&mut sequence, b'b');
        let c = append_leaf(&mut sequence, b'c');
        let d = append_leaf(&mut sequence, b'd');
        let cd = sequence.allocate_branch(c, d).unwrap();
        let bcd = sequence.allocate_branch(b, cd).unwrap();
        let single = sequence.rebalance(a, bcd).unwrap();
        assert_eq!(single.height(), 3);
        assert_eq!(sequence.read_range(single, 0, 4).unwrap(), b"abcd");
        sequence.verify_root(single).unwrap();

        let e = append_leaf(&mut sequence, b'e');
        let f = append_leaf(&mut sequence, b'f');
        let g = append_leaf(&mut sequence, b'g');
        let h = append_leaf(&mut sequence, b'h');
        let fg = sequence.allocate_branch(f, g).unwrap();
        let fgh = sequence.allocate_branch(fg, h).unwrap();
        let double = sequence.rebalance(e, fgh).unwrap();
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

        assert_eq!(
            sequence.append(Some(root), b""),
            Err(V2AvlError::Invalid(
                "v2 sequence append payload must be non-empty"
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
            reopened.read_range(latest, 0, latest.logical_len()).unwrap(),
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

        assert_eq!(
            V2AvlSequence::import_image(&encoded),
            Err(V2AvlError::Invalid(
                "v2 image branch child must reference an earlier arena node"
            ))
        );
    }
}
