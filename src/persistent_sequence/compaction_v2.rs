//! Staged Format-v2 physical-compaction reachability plan.
//!
//! After partial logical subtree deletion the committed state deliberately
//! retains the append-only payload/node/version arena, including history that
//! is no longer reachable from any live checkpoint. This module computes only
//! the deterministic reachability plan answering which source versions, AVL
//! nodes, and payload ranges remain semantically required.
//!
//! This unit builds no replacement arena, assigns no compacted identifiers,
//! mutates no committed state, and publishes nothing. Identifier remapping and
//! actual reclamation are later units. The plan carries source identifiers
//! only, so a future apply step cannot confuse old and new coordinates.

use super::apply_v2::V2CommittedState;
use super::image_v2::{v2_node_fields, V2ImageError, V2NodeFields};
use std::collections::HashSet;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2CompactionError {
    Image(V2ImageError),
    Invalid(&'static str),
    Overflow(&'static str),
    Capacity(&'static str),
}

impl fmt::Display for V2CompactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Image(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) | Self::Capacity(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl std::error::Error for V2CompactionError {}

impl From<V2ImageError> for V2CompactionError {
    fn from(error: V2ImageError) -> Self {
        Self::Image(error)
    }
}

/// One retained source payload range, recorded verbatim from a retained leaf.
///
/// Ranges are reported per retained leaf node without merging or
/// deduplication: a future remapping unit must see exactly what the reachable
/// leaves reference. Overlapping entries can only arise from a malformed arena
/// because canonical appends allocate disjoint delta ranges; they are preserved
/// here so the remapping unit can reject or handle them explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct V2RetainedPayloadRange {
    offset: u64,
    length: u64,
}

impl V2RetainedPayloadRange {
    pub(super) const fn offset(self) -> u64 {
        self.offset
    }

    pub(super) const fn length(self) -> u64 {
        self.length
    }
}

/// Deterministic physical reachability plan over source identifiers.
///
/// `retained_versions` holds source version IDs in ascending order,
/// `retained_nodes` holds source node IDs in ascending order, and
/// `retained_payload_ranges` holds source `(offset, length)` ranges ordered by
/// offset, then length. No compacted identifiers exist at this stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2CompactionPlan {
    retained_versions: Vec<u32>,
    retained_nodes: Vec<u64>,
    retained_payload_ranges: Vec<V2RetainedPayloadRange>,
}

impl V2CompactionPlan {
    pub(super) fn retained_versions(&self) -> &[u32] {
        &self.retained_versions
    }

    pub(super) fn retained_nodes(&self) -> &[u64] {
        &self.retained_nodes
    }

    pub(super) fn retained_payload_ranges(&self) -> &[V2RetainedPayloadRange] {
        &self.retained_payload_ranges
    }
}

/// Computes which source versions, nodes, and payload ranges remain reachable.
///
/// The planner receives `&V2CommittedState` only, so validation, overflow, or
/// allocation failure leaves committed state untouched by construction. There
/// is no rollback path because no semantic mutation ever starts.
pub(super) fn plan_v2_compaction(
    state: &V2CommittedState,
) -> Result<V2CompactionPlan, V2CompactionError> {
    let retained_versions = plan_retained_versions(state)?;
    let (retained_nodes, retained_payload_ranges) = plan_retained_arena(state, &retained_versions)?;
    Ok(V2CompactionPlan {
        retained_versions,
        retained_nodes,
        retained_payload_ranges,
    })
}

/// Seeds every live checkpoint version reference, then retains the transitive
/// `parent_version` ancestry with an iterative worklist. Traversal depth never
/// grows the native call stack regardless of production history length.
fn plan_retained_versions(state: &V2CommittedState) -> Result<Vec<u32>, V2CompactionError> {
    let seed_capacity =
        state
            .checkpoints
            .len()
            .checked_mul(3)
            .ok_or(V2CompactionError::Overflow(
                "v2 compaction checkpoint seed count exceeds usize",
            ))?;
    let work_capacity =
        seed_capacity
            .checked_add(state.versions.len())
            .ok_or(V2CompactionError::Overflow(
                "v2 compaction version worklist size exceeds usize",
            ))?;
    let mut retained: HashSet<u32> = HashSet::new();
    retained
        .try_reserve(work_capacity)
        .map_err(|_| V2CompactionError::Capacity("v2 compaction version set allocation failed"))?;
    let mut worklist: Vec<u32> = Vec::new();
    worklist.try_reserve(work_capacity).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction version worklist allocation failed")
    })?;
    for checkpoint in &state.checkpoints {
        claim_version(&mut retained, &mut worklist, checkpoint.identity_version);
        if let Some(version) = checkpoint.messages_version {
            claim_version(&mut retained, &mut worklist, version);
        }
        if let Some(version) = checkpoint.result_version {
            claim_version(&mut retained, &mut worklist, version);
        }
    }
    while let Some(version_id) = worklist.pop() {
        let index = usize::try_from(version_id).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction version identifier exceeds usize")
        })?;
        let version = state.versions.get(index).ok_or(V2CompactionError::Invalid(
            "v2 compaction checkpoint references a nonexistent version",
        ))?;
        if version.version_id() != version_id {
            return Err(V2CompactionError::Invalid(
                "v2 compaction version id disagrees with its vector coordinate",
            ));
        }
        if let Some(parent) = version.parent_version() {
            if parent >= version_id {
                return Err(V2CompactionError::Invalid(
                    "v2 compaction version parent is not topologically prior",
                ));
            }
            let parent_index = usize::try_from(parent).map_err(|_| {
                V2CompactionError::Overflow("v2 compaction version parent identifier exceeds usize")
            })?;
            if parent_index >= state.versions.len() {
                return Err(V2CompactionError::Invalid(
                    "v2 compaction version parent is absent",
                ));
            }
            claim_version(&mut retained, &mut worklist, parent);
        }
    }
    let mut ordered: Vec<u32> = Vec::new();
    ordered
        .try_reserve_exact(retained.len())
        .map_err(|_| V2CompactionError::Capacity("v2 compaction version plan allocation failed"))?;
    ordered.extend(retained.iter().copied());
    ordered.sort_unstable();
    Ok(ordered)
}

/// Traverses the AVL DAG from every retained version root with an iterative
/// worklist, retaining each reachable node once and recording every retained
/// leaf payload range. Shared nodes are claimed on first visit, so diamonds in
/// the DAG cannot duplicate plan entries or loop the traversal.
fn plan_retained_arena(
    state: &V2CommittedState,
    retained_versions: &[u32],
) -> Result<(Vec<u64>, Vec<V2RetainedPayloadRange>), V2CompactionError> {
    let arena_len = u64::try_from(state.payload.len())
        .map_err(|_| V2CompactionError::Overflow("v2 compaction payload length exceeds u64"))?;
    let work_capacity = state
        .nodes
        .len()
        .checked_add(retained_versions.len())
        .ok_or(V2CompactionError::Overflow(
            "v2 compaction node worklist size exceeds usize",
        ))?;
    let mut retained: HashSet<u64> = HashSet::new();
    retained
        .try_reserve(work_capacity)
        .map_err(|_| V2CompactionError::Capacity("v2 compaction node set allocation failed"))?;
    let mut worklist: Vec<u64> = Vec::new();
    worklist.try_reserve(work_capacity).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction node worklist allocation failed")
    })?;
    for version_id in retained_versions.iter().copied() {
        let index = usize::try_from(version_id).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction retained version identifier exceeds usize")
        })?;
        let version = state.versions.get(index).ok_or(V2CompactionError::Invalid(
            "v2 compaction retained version is absent",
        ))?;
        claim_node(&mut retained, &mut worklist, version.root().node_id());
    }
    let mut ranges: Vec<V2RetainedPayloadRange> = Vec::new();
    ranges.try_reserve(state.nodes.len()).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction payload range allocation failed")
    })?;
    while let Some(node_id) = worklist.pop() {
        let index = usize::try_from(node_id).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction node identifier exceeds usize")
        })?;
        let node = state
            .nodes
            .get(index)
            .copied()
            .ok_or(V2CompactionError::Invalid(
                "v2 compaction node reference is outside the node table",
            ))?;
        match v2_node_fields(node)? {
            V2NodeFields::Leaf {
                payload_offset,
                payload_len,
            } => {
                if payload_len == 0 {
                    return Err(V2CompactionError::Invalid(
                        "v2 compaction leaf payload length is zero",
                    ));
                }
                let end =
                    payload_offset
                        .checked_add(payload_len)
                        .ok_or(V2CompactionError::Overflow(
                            "v2 compaction leaf payload range exceeds u64",
                        ))?;
                if end > arena_len {
                    return Err(V2CompactionError::Invalid(
                        "v2 compaction leaf payload range is outside the payload arena",
                    ));
                }
                ranges.push(V2RetainedPayloadRange {
                    offset: payload_offset,
                    length: payload_len,
                });
            }
            V2NodeFields::Branch {
                left_node_id,
                right_node_id,
                ..
            } => {
                if left_node_id >= node_id || right_node_id >= node_id {
                    return Err(V2CompactionError::Invalid(
                        "v2 compaction branch child is not topologically prior",
                    ));
                }
                claim_node(&mut retained, &mut worklist, left_node_id);
                claim_node(&mut retained, &mut worklist, right_node_id);
            }
        }
    }
    let mut ordered_nodes: Vec<u64> = Vec::new();
    ordered_nodes
        .try_reserve_exact(retained.len())
        .map_err(|_| V2CompactionError::Capacity("v2 compaction node plan allocation failed"))?;
    ordered_nodes.extend(retained.iter().copied());
    ordered_nodes.sort_unstable();
    // Unstable ordering is allocation-free, consistent with this unit's
    // explicit `Capacity` handling. Stability has no semantic value here
    // because identical `(offset, length)` entries are indistinguishable.
    ranges.sort_unstable_by(|left, right| {
        (left.offset, left.length).cmp(&(right.offset, right.length))
    });
    Ok((ordered_nodes, ranges))
}

fn claim_version(retained: &mut HashSet<u32>, worklist: &mut Vec<u32>, version_id: u32) {
    if retained.insert(version_id) {
        worklist.push(version_id);
    }
}

fn claim_node(retained: &mut HashSet<u64>, worklist: &mut Vec<u64>, node_id: u64) {
    if retained.insert(node_id) {
        worklist.push(node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::super::apply_v2::{apply_v2_commit, V2CommittedState};
    use super::super::commit_v2::encode_v2_commit;
    use super::super::format_v2::{V2NodeRecord, V2RootRecord};
    use super::super::publication_v2::{
        checkpoint_state_metadata, V2CheckpointRecord, V2VersionRecord,
    };
    use super::super::transaction_v2::{V2WalGeometry, V2WalTransaction};
    use super::*;

    fn genesis_transaction(checkpoint_id: &str, payload: &[u8]) -> V2WalTransaction {
        let node = V2NodeRecord::leaf(0, payload).unwrap();
        let root = V2RootRecord::from_node(0, node).unwrap();
        V2WalTransaction {
            payload: payload.to_vec(),
            nodes: vec![node],
            versions: vec![V2VersionRecord::new(0, None, root).unwrap()],
            checkpoint: V2CheckpointRecord {
                checkpoint_no: 1,
                thread_id: "thread".to_owned(),
                checkpoint_id: checkpoint_id.to_owned(),
                parent_checkpoint_id: None,
                identity_version: 0,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(root, None, None).unwrap(),
            },
        }
    }

    fn branch_child_transaction(
        base: V2WalGeometry,
        checkpoint_no: u32,
        thread_id: &str,
        checkpoint_id: &str,
        parent_checkpoint_id: Option<&str>,
        parent_version: u32,
        parent_root: V2RootRecord,
        payload: &[u8],
    ) -> V2WalTransaction {
        let leaf = V2NodeRecord::leaf(base.payload_len, payload).unwrap();
        let leaf_root = V2RootRecord::from_node(base.node_count, leaf).unwrap();
        let branch = V2NodeRecord::branch(parent_root, leaf_root).unwrap();
        let branch_root = V2RootRecord::from_node(base.node_count + 1, branch).unwrap();
        let version_id = u32::try_from(base.version_count).unwrap();
        V2WalTransaction {
            payload: payload.to_vec(),
            nodes: vec![leaf, branch],
            versions: vec![
                V2VersionRecord::new(version_id, Some(parent_version), branch_root).unwrap(),
            ],
            checkpoint: V2CheckpointRecord {
                checkpoint_no,
                thread_id: thread_id.to_owned(),
                checkpoint_id: checkpoint_id.to_owned(),
                parent_checkpoint_id: parent_checkpoint_id.map(str::to_owned),
                identity_version: version_id,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(branch_root, None, None).unwrap(),
            },
        }
    }

    fn apply_transaction(
        state: &mut V2CommittedState,
        transaction: &V2WalTransaction,
        request_id: &[u8],
    ) {
        let base = state.geometry().unwrap();
        let encoded = encode_v2_commit(base, transaction, Some(request_id)).unwrap();
        apply_v2_commit(state, &encoded).unwrap();
    }

    fn payload_ranges(plan: &V2CompactionPlan) -> Vec<(u64, u64)> {
        plan.retained_payload_ranges()
            .iter()
            .map(|range| (range.offset(), range.length()))
            .collect()
    }

    #[test]
    fn deleted_subtree_history_is_unreachable_but_retained_roots_survive() {
        // Physical topology under test:
        //   A(cp-1, v0/n0)
        //   ├── B(cp-2, v1/n1-n2 derived from A)
        //   │   └── C(cp-3, v2/n3-n4 derived from B)
        //   └── D(cp-sibling, v3/n5-n6 derived from A, distinct root)
        //   other-thread E(other-root, v4/n7-n8 derived from A, distinct root)
        // Deleting B must drop only the B/C-exclusive versions, nodes, and
        // payload ranges while keeping the distinct D and E histories.
        let mut state = V2CommittedState::default();
        apply_transaction(&mut state, &genesis_transaction("cp-1", b"aaa"), b"req-1");
        let genesis_root = state.versions[0].root();
        let base = state.geometry().unwrap();
        apply_transaction(
            &mut state,
            &branch_child_transaction(
                base,
                2,
                "thread",
                "cp-2",
                Some("cp-1"),
                0,
                genesis_root,
                b"bbb",
            ),
            b"req-2",
        );
        let second_root = state.versions[1].root();
        let base = state.geometry().unwrap();
        apply_transaction(
            &mut state,
            &branch_child_transaction(
                base,
                3,
                "thread",
                "cp-3",
                Some("cp-2"),
                1,
                second_root,
                b"ccc",
            ),
            b"req-3",
        );
        let base = state.geometry().unwrap();
        apply_transaction(
            &mut state,
            &branch_child_transaction(
                base,
                4,
                "thread",
                "cp-sibling",
                Some("cp-1"),
                0,
                genesis_root,
                b"ddd",
            ),
            b"req-4",
        );
        let base = state.geometry().unwrap();
        apply_transaction(
            &mut state,
            &branch_child_transaction(
                base,
                5,
                "other-thread",
                "other-root",
                None,
                0,
                genesis_root,
                b"eee",
            ),
            b"req-5",
        );

        let before = plan_v2_compaction(&state).unwrap();
        assert_eq!(before.retained_versions(), &[0, 1, 2, 3, 4]);
        assert_eq!(before.retained_nodes(), &[0, 1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            payload_ranges(&before),
            vec![(0, 3), (3, 3), (6, 3), (9, 3), (12, 3)]
        );

        let prepared = state
            .prepare_delete_checkpoint_subtree("thread", "cp-2")
            .unwrap();
        assert_eq!(prepared.deleted_checkpoint_count(), 2);
        state.apply_prepared_delete_checkpoint_subtree(prepared);

        let after = plan_v2_compaction(&state).unwrap();
        assert_eq!(after.retained_versions(), &[0, 3, 4]);
        assert_eq!(after.retained_nodes(), &[0, 5, 6, 7, 8]);
        let after_ranges = payload_ranges(&after);
        assert_eq!(after_ranges, vec![(0, 3), (9, 3), (12, 3)]);
        for pruned_version in [1, 2] {
            assert!(!after.retained_versions().contains(&pruned_version));
        }
        for pruned_node in [1, 2, 3, 4] {
            assert!(!after.retained_nodes().contains(&pruned_node));
        }
        assert!(!after_ranges.contains(&(3, 3)));
        assert!(!after_ranges.contains(&(6, 3)));
        assert_eq!(
            after
                .retained_nodes()
                .iter()
                .filter(|node| **node == 0)
                .count(),
            1
        );
    }

    #[test]
    fn shared_nodes_appear_exactly_once_across_sibling_branches() {
        let mut state = V2CommittedState::default();
        apply_transaction(&mut state, &genesis_transaction("cp-1", b"aaa"), b"req-1");
        let genesis_root = state.versions[0].root();
        let base = state.geometry().unwrap();
        apply_transaction(
            &mut state,
            &branch_child_transaction(
                base,
                2,
                "thread",
                "cp-2",
                Some("cp-1"),
                0,
                genesis_root,
                b"bbb",
            ),
            b"req-2",
        );
        let base = state.geometry().unwrap();
        apply_transaction(
            &mut state,
            &branch_child_transaction(
                base,
                3,
                "thread",
                "cp-3",
                Some("cp-1"),
                0,
                genesis_root,
                b"ccc",
            ),
            b"req-3",
        );

        let plan = plan_v2_compaction(&state).unwrap();
        assert_eq!(plan.retained_versions(), &[0, 1, 2]);
        assert_eq!(plan.retained_nodes(), &[0, 1, 2, 3, 4]);
        assert_eq!(
            plan.retained_nodes()
                .iter()
                .filter(|node| **node == 0)
                .count(),
            1
        );
        assert_eq!(payload_ranges(&plan), vec![(0, 3), (3, 3), (6, 3)]);
    }

    #[test]
    fn live_checkpoint_retains_its_transitive_version_ancestry() {
        let mut state = V2CommittedState::default();
        apply_transaction(&mut state, &genesis_transaction("cp-1", b"aaa"), b"req-1");
        let genesis_root = state.versions[0].root();
        let base = state.geometry().unwrap();
        apply_transaction(
            &mut state,
            &branch_child_transaction(
                base,
                2,
                "thread",
                "cp-2",
                Some("cp-1"),
                0,
                genesis_root,
                b"bbb",
            ),
            b"req-2",
        );
        let second_root = state.versions[1].root();
        let base = state.geometry().unwrap();
        apply_transaction(
            &mut state,
            &branch_child_transaction(
                base,
                3,
                "thread",
                "cp-3",
                Some("cp-2"),
                1,
                second_root,
                b"ccc",
            ),
            b"req-3",
        );

        let plan = plan_v2_compaction(&state).unwrap();
        assert_eq!(plan.retained_versions(), &[0, 1, 2]);
        assert_eq!(plan.retained_nodes(), &[0, 1, 2, 3, 4]);
    }

    #[test]
    fn messages_and_result_versions_seed_reachability() {
        let mut state = V2CommittedState::default();
        apply_transaction(&mut state, &genesis_transaction("cp-1", b"aaa"), b"req-1");
        let genesis_root = state.versions[0].root();

        let base = state.geometry().unwrap();
        let mut second = branch_child_transaction(
            base,
            2,
            "thread",
            "cp-2",
            Some("cp-1"),
            0,
            genesis_root,
            b"bbb",
        );
        let second_version = u32::try_from(base.version_count).unwrap();
        second.checkpoint.identity_version = 0;
        second.checkpoint.messages_version = Some(second_version);
        second.checkpoint.state =
            checkpoint_state_metadata(genesis_root, Some(second.versions[0].root()), None).unwrap();
        apply_transaction(&mut state, &second, b"req-2");

        let second_root = state.versions[1].root();
        let base = state.geometry().unwrap();
        let mut third = branch_child_transaction(
            base,
            3,
            "thread",
            "cp-3",
            Some("cp-2"),
            1,
            second_root,
            b"ccc",
        );
        let third_version = u32::try_from(base.version_count).unwrap();
        third.checkpoint.identity_version = 0;
        third.checkpoint.result_version = Some(third_version);
        third.checkpoint.state =
            checkpoint_state_metadata(genesis_root, None, Some(third.versions[0].root())).unwrap();
        apply_transaction(&mut state, &third, b"req-3");

        let plan = plan_v2_compaction(&state).unwrap();
        assert_eq!(plan.retained_versions(), &[0, 1, 2]);
        assert_eq!(plan.retained_nodes(), &[0, 1, 2, 3, 4]);
        assert_eq!(payload_ranges(&plan), vec![(0, 3), (3, 3), (6, 3)]);
    }

    #[test]
    fn checkpoint_with_nonexistent_version_fails_closed() {
        let mut state = V2CommittedState::default();
        apply_transaction(&mut state, &genesis_transaction("cp-1", b"aaa"), b"req-1");
        let before = state.geometry().unwrap();

        state.checkpoints[0].identity_version = 999;
        assert_eq!(
            plan_v2_compaction(&state),
            Err(V2CompactionError::Invalid(
                "v2 compaction checkpoint references a nonexistent version"
            ))
        );
        assert_eq!(state.geometry().unwrap(), before);

        state.checkpoints[0].identity_version = 0;
        state.checkpoints[0].messages_version = Some(999);
        assert_eq!(
            plan_v2_compaction(&state),
            Err(V2CompactionError::Invalid(
                "v2 compaction checkpoint references a nonexistent version"
            ))
        );
        assert_eq!(state.geometry().unwrap(), before);
    }

    #[test]
    fn version_with_coordinate_mismatch_fails_closed() {
        let mut state = V2CommittedState::default();
        apply_transaction(&mut state, &genesis_transaction("cp-1", b"aaa"), b"req-1");
        let genesis_root = state.versions[0].root();
        let before = state.geometry().unwrap();

        // Version 1 claims vector position 0, so its parent 0 is dangling and
        // its coordinate disagrees. A non-prior parent cannot be built through
        // the canonical constructors or codec because both enforce
        // parent < version_id before a record exists; the planner still
        // re-checks priority defensively for future construction paths.
        state.versions = vec![V2VersionRecord::new(1, Some(0), genesis_root).unwrap()];
        assert_eq!(
            plan_v2_compaction(&state),
            Err(V2CompactionError::Invalid(
                "v2 compaction version id disagrees with its vector coordinate"
            ))
        );
        assert_eq!(state.geometry().unwrap(), before);
    }

    #[test]
    fn node_reference_outside_the_table_fails_closed() {
        let leaf = V2NodeRecord::leaf(0, b"aaa").unwrap();
        let outside_root = V2RootRecord::from_node(9_000, leaf).unwrap();
        let version = V2VersionRecord::new(0, None, outside_root).unwrap();
        let mut state = V2CommittedState::default();
        state.payload = b"aaa".to_vec();
        state.nodes = vec![leaf];
        state.versions = vec![version];
        state.checkpoints = vec![V2CheckpointRecord {
            checkpoint_no: 1,
            thread_id: "thread".to_owned(),
            checkpoint_id: "cp-1".to_owned(),
            parent_checkpoint_id: None,
            identity_version: 0,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(outside_root, None, None).unwrap(),
        }];
        assert_eq!(
            plan_v2_compaction(&state),
            Err(V2CompactionError::Invalid(
                "v2 compaction node reference is outside the node table"
            ))
        );
    }

    #[test]
    fn branch_child_that_is_not_prior_fails_closed() {
        let leaf = V2NodeRecord::leaf(0, b"aaa").unwrap();
        let left_root = V2RootRecord::from_node(0, leaf).unwrap();
        // The right child reuses this branch's own future identifier, so it is
        // not topologically prior. A child beyond the table would surface here
        // first for the same reason: any in-bounds child of a valid parent is
        // necessarily already committed.
        let self_root = V2RootRecord::from_node(1, leaf).unwrap();
        let branch = V2NodeRecord::branch(left_root, self_root).unwrap();
        let branch_root = V2RootRecord::from_node(1, branch).unwrap();
        let version = V2VersionRecord::new(0, None, branch_root).unwrap();
        let mut state = V2CommittedState::default();
        state.payload = b"aaa".to_vec();
        state.nodes = vec![leaf, branch];
        state.versions = vec![version];
        state.checkpoints = vec![V2CheckpointRecord {
            checkpoint_no: 1,
            thread_id: "thread".to_owned(),
            checkpoint_id: "cp-1".to_owned(),
            parent_checkpoint_id: None,
            identity_version: 0,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(branch_root, None, None).unwrap(),
        }];
        assert_eq!(
            plan_v2_compaction(&state),
            Err(V2CompactionError::Invalid(
                "v2 compaction branch child is not topologically prior"
            ))
        );
    }

    #[test]
    fn leaf_with_out_of_bounds_payload_range_fails_closed() {
        // The canonical leaf constructor and codec both reject an overflowing
        // offset + length and an empty payload, so the planner's overflow and
        // zero-length checks are defense-in-depth. An in-range offset beyond
        // the committed payload is constructible and must fail closed here.
        let leaf = V2NodeRecord::leaf(u64::MAX - 8, b"aaa").unwrap();
        let root = V2RootRecord::from_node(0, leaf).unwrap();
        let version = V2VersionRecord::new(0, None, root).unwrap();
        let mut state = V2CommittedState::default();
        state.payload = b"aaa".to_vec();
        state.nodes = vec![leaf];
        state.versions = vec![version];
        state.checkpoints = vec![V2CheckpointRecord {
            checkpoint_no: 1,
            thread_id: "thread".to_owned(),
            checkpoint_id: "cp-1".to_owned(),
            parent_checkpoint_id: None,
            identity_version: 0,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(root, None, None).unwrap(),
        }];
        assert_eq!(
            plan_v2_compaction(&state),
            Err(V2CompactionError::Invalid(
                "v2 compaction leaf payload range is outside the payload arena"
            ))
        );
    }

    #[test]
    fn tombstone_only_state_plans_empty_but_valid() {
        let mut state = V2CommittedState::default();
        state
            .deleted_checkpoints
            .insert(("thread".to_owned(), "cp-1".to_owned()));
        state.retired_requests.insert(b"req-1".to_vec(), [0x77; 32]);

        let plan = plan_v2_compaction(&state).unwrap();
        assert!(plan.retained_versions().is_empty());
        assert!(plan.retained_nodes().is_empty());
        assert!(plan.retained_payload_ranges().is_empty());
        assert!(state
            .deleted_checkpoints
            .contains(&("thread".to_owned(), "cp-1".to_owned())));
        assert!(state.retired_requests.contains_key(b"req-1".as_slice()));
    }
}
