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
//!
//! The second compaction slice prepares the compact replacement itself:
//! deterministic dense old-to-new remapping, canonical reconstruction of every
//! retained leaf/branch/version-record (never byte patching), and checkpoint
//! tables whose logical state and operation digests are proven identical. The
//! prepared object still mutates nothing and publishes nothing; applying it is
//! a later unit.

use super::apply_v2::V2CommittedState;
use super::backend_v2::{validate_checkpoint_index, V2BackendError};
use super::commit_v2::{checkpoint_operation_digest, V2CommitError};
use super::format_v2::{V2FormatError, V2NodeRecord, V2RootRecord};
use super::image_v2::{
    encode_v2_image, v2_node_fields, V2ImageError, V2NodeFields, V2SequenceImage,
};
use super::publication_v2::{
    checkpoint_state_metadata, V2CheckpointRecord, V2PublicationError, V2VersionRecord,
};
use super::snapshot_v2::{
    encode_v2_sealed_snapshot, V2ActiveRequestRecord, V2DeletedCheckpointRecord,
    V2RetiredRequestRecord, V2SealedSnapshot, V2SnapshotError,
};
use std::collections::HashSet;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2CompactionError {
    Image(V2ImageError),
    Format(V2FormatError),
    Publication(V2PublicationError),
    Commit(V2CommitError),
    Snapshot(V2SnapshotError),
    Backend(V2BackendError),
    Invalid(&'static str),
    Overflow(&'static str),
    Capacity(&'static str),
}

impl fmt::Display for V2CompactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Image(error) => write!(formatter, "{error}"),
            Self::Format(error) => write!(formatter, "{error}"),
            Self::Publication(error) => write!(formatter, "{error}"),
            Self::Commit(error) => write!(formatter, "{error}"),
            Self::Snapshot(error) => write!(formatter, "{error}"),
            Self::Backend(error) => write!(formatter, "{error}"),
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

impl From<V2FormatError> for V2CompactionError {
    fn from(error: V2FormatError) -> Self {
        Self::Format(error)
    }
}

impl From<V2PublicationError> for V2CompactionError {
    fn from(error: V2PublicationError) -> Self {
        Self::Publication(error)
    }
}

impl From<V2CommitError> for V2CompactionError {
    fn from(error: V2CommitError) -> Self {
        Self::Commit(error)
    }
}

impl From<V2SnapshotError> for V2CompactionError {
    fn from(error: V2SnapshotError) -> Self {
        Self::Snapshot(error)
    }
}

impl From<V2BackendError> for V2CompactionError {
    fn from(error: V2BackendError) -> Self {
        Self::Backend(error)
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
    let mut seeds: Vec<u64> = Vec::new();
    seeds
        .try_reserve(retained_versions.len())
        .map_err(|_| V2CompactionError::Capacity("v2 compaction seed allocation failed"))?;
    for version_id in retained_versions.iter().copied() {
        let index = usize::try_from(version_id).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction retained version identifier exceeds usize")
        })?;
        let version = state.versions.get(index).ok_or(V2CompactionError::Invalid(
            "v2 compaction retained version is absent",
        ))?;
        seeds.push(version.root().node_id());
    }
    plan_reachable_arena(&state.payload, &state.nodes, &seeds)
}

/// Generic reachability core shared by checkpoint compaction and history-core
/// GC: iterative worklist from explicit root node seeds over a node table,
/// with no version-table or checkpoint vocabulary. `nodes` index equals node
/// ID; every seed must name a table entry.
pub(super) fn plan_reachable_arena(
    payload: &[u8],
    nodes: &[V2NodeRecord],
    seeds: &[u64],
) -> Result<(Vec<u64>, Vec<V2RetainedPayloadRange>), V2CompactionError> {
    let arena_len = u64::try_from(payload.len())
        .map_err(|_| V2CompactionError::Overflow("v2 compaction payload length exceeds u64"))?;
    let work_capacity = nodes
        .len()
        .checked_add(seeds.len())
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
    for seed in seeds.iter().copied() {
        // Out-of-table seeds fail closed on first pop below with the same
        // node-table error as dangling branch children.
        claim_node(&mut retained, &mut worklist, seed);
    }
    let mut ranges: Vec<V2RetainedPayloadRange> = Vec::new();
    ranges.try_reserve(nodes.len()).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction payload range allocation failed")
    })?;
    while let Some(node_id) = worklist.pop() {
        let index = usize::try_from(node_id).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction node identifier exceeds usize")
        })?;
        let node = nodes.get(index).copied().ok_or(V2CompactionError::Invalid(
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

/// One deterministic old-to-new version identifier mapping entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct V2VersionIdMapping {
    old_version: u32,
    new_version: u32,
}

impl V2VersionIdMapping {
    pub(super) const fn old_version(self) -> u32 {
        self.old_version
    }

    pub(super) const fn new_version(self) -> u32 {
        self.new_version
    }
}

/// One deterministic old-to-new node identifier mapping entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct V2NodeIdMapping {
    old_node: u64,
    new_node: u64,
}

impl V2NodeIdMapping {
    pub(super) const fn old_node(self) -> u64 {
        self.old_node
    }

    pub(super) const fn new_node(self) -> u64 {
        self.new_node
    }
}

/// One deterministic source-range to compact-offset mapping entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct V2PayloadRangeMapping {
    old_offset: u64,
    length: u64,
    new_offset: u64,
}

impl V2PayloadRangeMapping {
    pub(super) const fn old_offset(self) -> u64 {
        self.old_offset
    }

    pub(super) const fn length(self) -> u64 {
        self.length
    }

    pub(super) const fn new_offset(self) -> u64 {
        self.new_offset
    }
}

/// Fully prepared compact physical replacement for the committed state.
///
/// The object owns the dense compact payload, node table, version table, and
/// checkpoint table with remapped physical version references, plus the
/// deterministic mappings that produced them. Checkpoint order, logical
/// identity, state commitments, and operation digests are identical to the
/// source; only physical coordinates change. Semantic ledgers (ordinals,
/// active/retired requests, tombstones) are intentionally absent: checkpoint
/// order is unchanged, so they need no physical replacement and stay with the
/// committed state until a later apply unit consumes this preparation.
///
/// `Clone` is deliberately absent: the prepared replacement is a single-use
/// owned transition object, and no caller may duplicate it for a later
/// independent apply. The only production transition is `compact_v2_state`,
/// which prepares and applies under one exclusive borrow.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct V2PreparedCompaction {
    payload: Vec<u8>,
    nodes: Vec<V2NodeRecord>,
    versions: Vec<V2VersionRecord>,
    checkpoints: Vec<V2CheckpointRecord>,
    version_mapping: Vec<V2VersionIdMapping>,
    node_mapping: Vec<V2NodeIdMapping>,
    payload_mapping: Vec<V2PayloadRangeMapping>,
}

impl V2PreparedCompaction {
    pub(super) fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub(super) fn nodes(&self) -> &[V2NodeRecord] {
        &self.nodes
    }

    pub(super) fn versions(&self) -> &[V2VersionRecord] {
        &self.versions
    }

    pub(super) fn checkpoints(&self) -> &[V2CheckpointRecord] {
        &self.checkpoints
    }

    pub(super) fn version_mapping(&self) -> &[V2VersionIdMapping] {
        &self.version_mapping
    }

    pub(super) fn node_mapping(&self) -> &[V2NodeIdMapping] {
        &self.node_mapping
    }

    pub(super) fn payload_mapping(&self) -> &[V2PayloadRangeMapping] {
        &self.payload_mapping
    }
}

/// Prepares the dense compact replacement for `state` without mutating it.
///
/// Preparation takes `&V2CommittedState` only: every fallible validation,
/// allocation, canonical reconstruction, and digest comparison completes
/// before any future mutation point could exist, so failure leaves committed
/// state untouched by construction.
pub(super) fn prepare_v2_compaction(
    state: &V2CommittedState,
) -> Result<V2PreparedCompaction, V2CompactionError> {
    let plan = plan_v2_compaction(state)?;
    let (payload, payload_mapping) = repack_compact_payload(state, &plan)?;
    let (nodes, node_mapping) = rebuild_compact_nodes(state, &plan, &payload, &payload_mapping)?;
    let (versions, version_mapping) =
        rebuild_compact_versions(state, &plan, &nodes, &node_mapping)?;
    let checkpoints = rebuild_compact_checkpoints(state, &versions, &version_mapping)?;
    Ok(V2PreparedCompaction {
        payload,
        nodes,
        versions,
        checkpoints,
        version_mapping,
        node_mapping,
        payload_mapping,
    })
}

/// Atomically compacts the committed physical state in place.
///
/// Preparation runs against the exclusively borrowed state, so no caller can
/// observe or mutate the state between preparation and application: a stale
/// prepared object can never overwrite newer state because no prepared object
/// ever escapes. After preparation succeeds, the private apply performs only
/// infallible field replacement; there is no `Result`-producing operation,
/// no rollback path, and no fallible allocation past that point. Semantic
/// ledgers are preserved untouched because checkpoint order is unchanged.
pub(super) fn compact_v2_state(state: &mut V2CommittedState) -> Result<(), V2CompactionError> {
    let prepared = prepare_v2_compaction(&*state)?;
    apply_prepared_compaction(state, prepared);
    Ok(())
}

/// Replaces the physical tables with the prepared compact replacement.
///
/// Module-private on purpose: only `compact_v2_state` may call this, with a
/// preparation produced from the same exclusive borrow. Assignments,
/// destructuring, and drops only.
fn apply_prepared_compaction(state: &mut V2CommittedState, prepared: V2PreparedCompaction) {
    let V2PreparedCompaction {
        payload,
        nodes,
        versions,
        checkpoints,
        ..
    } = prepared;
    state.payload = payload;
    state.nodes = nodes;
    state.versions = versions;
    state.checkpoints = checkpoints;
}

/// Prepares the fully encoded compact `T2S2` authority candidate without
/// mutating the authoritative state.
///
/// This is the pre-publication bridge, not the in-memory compactor: the
/// source state stays authoritative while every fallible step (index
/// validation, compact preparation, image encoding, ledger serialization,
/// snapshot encoding) completes. A later publisher can write/sync these bytes
/// and only then adopt the compact memory state. The prepared compact tables
/// are consumed directly; the whole state is never cloned and compacted.
///
/// Empty-state rules mirror the backend exactly: a truly unused state yields
/// no artifact, while a tombstone-only state yields an authoritative
/// tombstone-only artifact.
pub(super) fn prepare_compacted_v2_sealed_artifact(
    state: &V2CommittedState,
) -> Result<Option<Vec<u8>>, V2CompactionError> {
    validate_checkpoint_index(state)?;
    let V2PreparedCompaction {
        payload,
        nodes,
        versions,
        checkpoints,
        ..
    } = prepare_v2_compaction(state)?;
    if checkpoints.is_empty() {
        if !payload.is_empty()
            || !nodes.is_empty()
            || !versions.is_empty()
            || !state.request_records.is_empty()
        {
            return Err(V2CompactionError::Invalid(
                "tombstone-only v2 backend contains live semantic state",
            ));
        }
        if state.retired_requests.is_empty() && state.deleted_checkpoints.is_empty() {
            return Ok(None);
        }
        let snapshot = V2SealedSnapshot {
            image: Vec::new(),
            versions: Vec::new(),
            checkpoints: Vec::new(),
            active_requests: Vec::new(),
            retired_requests: artifact_retired_requests(state)?,
            deleted_checkpoints: artifact_deleted_checkpoints(state)?,
        };
        return Ok(Some(encode_v2_sealed_snapshot(&snapshot)?));
    }
    if versions.is_empty() || nodes.is_empty() || payload.is_empty() {
        return Err(V2CompactionError::Invalid(
            "non-empty v2 backend is missing persistent sequence state",
        ));
    }
    let mut roots: Vec<V2RootRecord> = Vec::new();
    roots.try_reserve_exact(versions.len()).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction artifact root table allocation failed")
    })?;
    for version in &versions {
        roots.push(version.root());
    }
    let image = encode_v2_image(&V2SequenceImage {
        payload,
        nodes,
        roots,
    })?;
    let snapshot = V2SealedSnapshot {
        image,
        versions,
        checkpoints,
        active_requests: artifact_active_requests(state)?,
        retired_requests: artifact_retired_requests(state)?,
        deleted_checkpoints: artifact_deleted_checkpoints(state)?,
    };
    Ok(Some(encode_v2_sealed_snapshot(&snapshot)?))
}

/// Serializes the authoritative source active-request ledger with exact
/// request IDs, operation digests, and (unchanged) checkpoint ordinals.
fn artifact_active_requests(
    state: &V2CommittedState,
) -> Result<Vec<V2ActiveRequestRecord>, V2CompactionError> {
    let mut records: Vec<V2ActiveRequestRecord> = Vec::new();
    records
        .try_reserve(state.request_records.len())
        .map_err(|_| {
            V2CompactionError::Capacity("v2 compaction artifact active request allocation failed")
        })?;
    for (request_id, record) in &state.request_records {
        records.push(V2ActiveRequestRecord::new(
            try_clone_request_id(request_id)?,
            record.operation_digest,
            record.checkpoint_ordinal,
        )?);
    }
    Ok(records)
}

/// Serializes the authoritative source retired-request ledger exactly.
fn artifact_retired_requests(
    state: &V2CommittedState,
) -> Result<Vec<V2RetiredRequestRecord>, V2CompactionError> {
    let mut records: Vec<V2RetiredRequestRecord> = Vec::new();
    records
        .try_reserve(state.retired_requests.len())
        .map_err(|_| {
            V2CompactionError::Capacity("v2 compaction artifact retired request allocation failed")
        })?;
    for (request_id, operation_digest) in &state.retired_requests {
        records.push(V2RetiredRequestRecord::new(
            try_clone_request_id(request_id)?,
            *operation_digest,
        )?);
    }
    Ok(records)
}

/// Serializes the authoritative source deleted-checkpoint identities exactly.
fn artifact_deleted_checkpoints(
    state: &V2CommittedState,
) -> Result<Vec<V2DeletedCheckpointRecord>, V2CompactionError> {
    let mut records: Vec<V2DeletedCheckpointRecord> = Vec::new();
    records
        .try_reserve(state.deleted_checkpoints.len())
        .map_err(|_| {
            V2CompactionError::Capacity("v2 compaction artifact tombstone allocation failed")
        })?;
    for (thread_id, checkpoint_id) in &state.deleted_checkpoints {
        records.push(V2DeletedCheckpointRecord::new(
            try_clone_compaction_string(thread_id)?,
            try_clone_compaction_string(checkpoint_id)?,
        )?);
    }
    Ok(records)
}

fn try_clone_request_id(request_id: &[u8]) -> Result<Vec<u8>, V2CompactionError> {
    let mut cloned = Vec::new();
    cloned.try_reserve_exact(request_id.len()).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction request identity allocation failed")
    })?;
    cloned.extend_from_slice(request_id);
    Ok(cloned)
}

/// Validates retained source ranges and repacks their exact bytes densely.
///
/// Ranges arrive ordered by `(offset, length)`. Gaps (deleted leaves) are
/// valid and simply disappear; any overlap, duplicate, backward move, overflow,
/// or out-of-bounds reference fails closed instead of being silently merged.
fn repack_compact_payload(
    state: &V2CommittedState,
    plan: &V2CompactionPlan,
) -> Result<(Vec<u8>, Vec<V2PayloadRangeMapping>), V2CompactionError> {
    repack_compact_ranges(&state.payload, plan.retained_payload_ranges())
}

/// Generic payload-repack core shared by checkpoint compaction and
/// history-core GC: validates sorted source ranges and dense-copies their
/// exact bytes, with no arena or checkpoint vocabulary.
pub(super) fn repack_compact_ranges(
    payload: &[u8],
    ranges: &[V2RetainedPayloadRange],
) -> Result<(Vec<u8>, Vec<V2PayloadRangeMapping>), V2CompactionError> {
    let mut total = 0u64;
    for range in ranges {
        total = total
            .checked_add(range.length())
            .ok_or(V2CompactionError::Overflow(
                "v2 compaction compact payload length exceeds u64",
            ))?;
    }
    let total_usize = usize::try_from(total).map_err(|_| {
        V2CompactionError::Overflow("v2 compaction compact payload length exceeds usize")
    })?;
    let mut compact: Vec<u8> = Vec::new();
    compact.try_reserve_exact(total_usize).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction compact payload allocation failed")
    })?;
    let mut mapping: Vec<V2PayloadRangeMapping> = Vec::new();
    mapping.try_reserve_exact(ranges.len()).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction payload mapping allocation failed")
    })?;
    let mut previous_end = 0u64;
    for range in ranges {
        if range.offset() < previous_end {
            return Err(V2CompactionError::Invalid(
                "v2 compaction retained payload ranges overlap or duplicate",
            ));
        }
        let end = range
            .offset()
            .checked_add(range.length())
            .ok_or(V2CompactionError::Overflow(
                "v2 compaction retained payload range exceeds u64",
            ))?;
        let start_usize = usize::try_from(range.offset()).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction retained payload offset exceeds usize")
        })?;
        let end_usize = usize::try_from(end).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction retained payload end exceeds usize")
        })?;
        let bytes = payload
            .get(start_usize..end_usize)
            .ok_or(V2CompactionError::Invalid(
                "v2 compaction retained payload range is outside the source payload",
            ))?;
        let new_offset = u64::try_from(compact.len()).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction compact payload offset exceeds u64")
        })?;
        compact.extend_from_slice(bytes);
        mapping.push(V2PayloadRangeMapping {
            old_offset: range.offset(),
            length: range.length(),
            new_offset,
        });
        previous_end = end;
    }
    Ok((compact, mapping))
}

/// Rebuilds every retained node canonically in ascending old-ID order.
///
/// Ascending order guarantees each retained branch child is already remapped
/// (valid children are topologically prior). Leaves and branches are
/// re-derived with the canonical constructors from compact coordinates and
/// required to preserve source height, logical length, and commitment; branch
/// `left_len` semantics are checked explicitly as well. Nothing is copied and
/// patched.
fn rebuild_compact_nodes(
    state: &V2CommittedState,
    plan: &V2CompactionPlan,
    compact_payload: &[u8],
    payload_mapping: &[V2PayloadRangeMapping],
) -> Result<(Vec<V2NodeRecord>, Vec<V2NodeIdMapping>), V2CompactionError> {
    rebuild_compact_records(
        &state.nodes,
        plan.retained_nodes(),
        compact_payload,
        payload_mapping,
    )
}

/// Generic node-rebuild core shared by checkpoint compaction and history-core
/// GC: canonical reconstruction of every retained record in ascending old-ID
/// order (children remap before parents), with height/length/commitment
/// equality against source. No version or checkpoint vocabulary.
pub(super) fn rebuild_compact_records(
    nodes: &[V2NodeRecord],
    retained: &[u64],
    compact_payload: &[u8],
    payload_mapping: &[V2PayloadRangeMapping],
) -> Result<(Vec<V2NodeRecord>, Vec<V2NodeIdMapping>), V2CompactionError> {
    let mut rebuilt_nodes: Vec<V2NodeRecord> = Vec::new();
    rebuilt_nodes
        .try_reserve_exact(retained.len())
        .map_err(|_| {
            V2CompactionError::Capacity("v2 compaction compact node table allocation failed")
        })?;
    let mut mapping: Vec<V2NodeIdMapping> = Vec::new();
    mapping
        .try_reserve_exact(retained.len())
        .map_err(|_| V2CompactionError::Capacity("v2 compaction node mapping allocation failed"))?;
    for old_id in retained.iter().copied() {
        let old_index = usize::try_from(old_id).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction source node identifier exceeds usize")
        })?;
        let old = nodes
            .get(old_index)
            .copied()
            .ok_or(V2CompactionError::Invalid(
                "v2 compaction planned node is absent from the source table",
            ))?;
        let rebuilt = match v2_node_fields(old)? {
            V2NodeFields::Leaf {
                payload_offset,
                payload_len,
            } => {
                let new_offset =
                    remapped_payload_offset(payload_mapping, payload_offset, payload_len)?;
                let start = usize::try_from(new_offset).map_err(|_| {
                    V2CompactionError::Overflow(
                        "v2 compaction compact payload offset exceeds usize",
                    )
                })?;
                let len = usize::try_from(payload_len).map_err(|_| {
                    V2CompactionError::Overflow(
                        "v2 compaction compact payload length exceeds usize",
                    )
                })?;
                let end = start.checked_add(len).ok_or(V2CompactionError::Overflow(
                    "v2 compaction compact payload range exceeds usize",
                ))?;
                // Bounds were proven while repacking; this re-slices the exact
                // compact bytes the rebuilt leaf commits to.
                let bytes = compact_payload
                    .get(start..end)
                    .ok_or(V2CompactionError::Invalid(
                        "v2 compaction compact payload range is outside the compact payload",
                    ))?;
                let rebuilt = V2NodeRecord::leaf(new_offset, bytes)?;
                if rebuilt.height() != old.height() {
                    return Err(V2CompactionError::Invalid(
                        "v2 compaction rebuilt leaf height disagrees with source",
                    ));
                }
                if rebuilt.logical_len() != old.logical_len() {
                    return Err(V2CompactionError::Invalid(
                        "v2 compaction rebuilt leaf logical length disagrees with source",
                    ));
                }
                if rebuilt.commitment() != old.commitment() {
                    return Err(V2CompactionError::Invalid(
                        "v2 compaction rebuilt leaf commitment disagrees with source",
                    ));
                }
                rebuilt
            }
            V2NodeFields::Branch {
                left_node_id,
                right_node_id,
                left_len,
            } => {
                let new_left = remapped_node_id(&mapping, left_node_id)?;
                let new_right = remapped_node_id(&mapping, right_node_id)?;
                let left_index = usize::try_from(new_left).map_err(|_| {
                    V2CompactionError::Overflow(
                        "v2 compaction compact node identifier exceeds usize",
                    )
                })?;
                let right_index = usize::try_from(new_right).map_err(|_| {
                    V2CompactionError::Overflow(
                        "v2 compaction compact node identifier exceeds usize",
                    )
                })?;
                let left_root = V2RootRecord::from_node(
                    new_left,
                    rebuilt_nodes
                        .get(left_index)
                        .copied()
                        .ok_or(V2CompactionError::Invalid(
                            "v2 compaction remapped left child is absent",
                        ))?,
                )?;
                let right_root = V2RootRecord::from_node(
                    new_right,
                    rebuilt_nodes
                        .get(right_index)
                        .copied()
                        .ok_or(V2CompactionError::Invalid(
                            "v2 compaction remapped right child is absent",
                        ))?,
                )?;
                let rebuilt = V2NodeRecord::branch(left_root, right_root)?;
                if rebuilt.height() != old.height() {
                    return Err(V2CompactionError::Invalid(
                        "v2 compaction rebuilt branch height disagrees with source",
                    ));
                }
                if rebuilt.logical_len() != old.logical_len() {
                    return Err(V2CompactionError::Invalid(
                        "v2 compaction rebuilt branch logical length disagrees with source",
                    ));
                }
                if rebuilt.commitment() != old.commitment() {
                    return Err(V2CompactionError::Invalid(
                        "v2 compaction rebuilt branch commitment disagrees with source",
                    ));
                }
                match v2_node_fields(rebuilt)? {
                    V2NodeFields::Branch {
                        left_len: rebuilt_left_len,
                        ..
                    } => {
                        if rebuilt_left_len != left_len {
                            return Err(V2CompactionError::Invalid(
                                "v2 compaction rebuilt branch left length disagrees with source",
                            ));
                        }
                    }
                    V2NodeFields::Leaf { .. } => {
                        return Err(V2CompactionError::Invalid(
                            "v2 compaction rebuilt branch decoded as a leaf",
                        ));
                    }
                }
                rebuilt
            }
        };
        let new_id = u64::try_from(rebuilt_nodes.len()).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction compact node identifier exceeds u64")
        })?;
        rebuilt_nodes.push(rebuilt);
        mapping.push(V2NodeIdMapping {
            old_node: old_id,
            new_node: new_id,
        });
    }
    Ok((rebuilt_nodes, mapping))
}

/// Rebuilds every retained version with dense sequential IDs.
///
/// Retained versions arrive ascending, and every retained parent is
/// topologically prior, so each parent mapping already exists. Roots are
/// re-derived canonically from the rebuilt compact nodes and required to match
/// the source root height, logical length, and commitment: a source root is
/// never trusted merely because its `node_id` resolves.
fn rebuild_compact_versions(
    state: &V2CommittedState,
    plan: &V2CompactionPlan,
    compact_nodes: &[V2NodeRecord],
    node_mapping: &[V2NodeIdMapping],
) -> Result<(Vec<V2VersionRecord>, Vec<V2VersionIdMapping>), V2CompactionError> {
    let retained = plan.retained_versions();
    let mut versions: Vec<V2VersionRecord> = Vec::new();
    versions.try_reserve_exact(retained.len()).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction compact version table allocation failed")
    })?;
    let mut mapping: Vec<V2VersionIdMapping> = Vec::new();
    mapping.try_reserve_exact(retained.len()).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction version mapping allocation failed")
    })?;
    for old_id in retained.iter().copied() {
        let old_index = usize::try_from(old_id).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction source version identifier exceeds usize")
        })?;
        let old = state
            .versions
            .get(old_index)
            .copied()
            .ok_or(V2CompactionError::Invalid(
                "v2 compaction planned version is absent from the source table",
            ))?;
        if old.version_id() != old_id {
            return Err(V2CompactionError::Invalid(
                "v2 compaction source version id disagrees with its vector coordinate",
            ));
        }
        let new_parent = match old.parent_version() {
            None => None,
            Some(parent) => Some(remapped_version_id(&mapping, parent)?),
        };
        let new_root_node = remapped_node_id(node_mapping, old.root().node_id())?;
        let new_root_index = usize::try_from(new_root_node).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction compact root node identifier exceeds usize")
        })?;
        let new_root = V2RootRecord::from_node(
            new_root_node,
            compact_nodes
                .get(new_root_index)
                .copied()
                .ok_or(V2CompactionError::Invalid(
                    "v2 compaction remapped version root is absent",
                ))?,
        )?;
        let source_root = old.root();
        if new_root.logical_len() != source_root.logical_len()
            || new_root.height() != source_root.height()
            || new_root.commitment() != source_root.commitment()
        {
            return Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt version root disagrees with source",
            ));
        }
        let new_id = u32::try_from(versions.len()).map_err(|_| {
            V2CompactionError::Overflow("v2 compaction compact version identifier exceeds u32")
        })?;
        versions.push(V2VersionRecord::new(new_id, new_parent, new_root)?);
        mapping.push(V2VersionIdMapping {
            old_version: old_id,
            new_version: new_id,
        });
    }
    Ok((versions, mapping))
}

/// Rebuilds the checkpoint table with remapped physical version references.
///
/// Logical identity, order, and state commitments are preserved exactly; only
/// `identity/messages/result_version` coordinates change. Each rebuilt
/// checkpoint must reproduce the source state commitment and the source
/// operation digest, proving physical compaction cannot alter logical request
/// identity. String materialization stays in this fallible preparation phase.
fn rebuild_compact_checkpoints(
    state: &V2CommittedState,
    compact_versions: &[V2VersionRecord],
    version_mapping: &[V2VersionIdMapping],
) -> Result<Vec<V2CheckpointRecord>, V2CompactionError> {
    let mut checkpoints: Vec<V2CheckpointRecord> = Vec::new();
    checkpoints
        .try_reserve_exact(state.checkpoints.len())
        .map_err(|_| {
            V2CompactionError::Capacity("v2 compaction compact checkpoint table allocation failed")
        })?;
    for old in &state.checkpoints {
        let new_identity = remapped_version_id(version_mapping, old.identity_version)?;
        let new_messages = match old.messages_version {
            None => None,
            Some(version) => Some(remapped_version_id(version_mapping, version)?),
        };
        let new_result = match old.result_version {
            None => None,
            Some(version) => Some(remapped_version_id(version_mapping, version)?),
        };
        let identity_root = compact_version_root(compact_versions, new_identity)?;
        let messages_root = match new_messages {
            None => None,
            Some(version) => Some(compact_version_root(compact_versions, version)?),
        };
        let result_root = match new_result {
            None => None,
            Some(version) => Some(compact_version_root(compact_versions, version)?),
        };
        let recomputed = checkpoint_state_metadata(identity_root, messages_root, result_root)?;
        if recomputed != old.state {
            return Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt checkpoint state disagrees with source",
            ));
        }
        let rebuilt = V2CheckpointRecord {
            checkpoint_no: old.checkpoint_no,
            thread_id: try_clone_compaction_string(&old.thread_id)?,
            checkpoint_id: try_clone_compaction_string(&old.checkpoint_id)?,
            parent_checkpoint_id: old
                .parent_checkpoint_id
                .as_deref()
                .map(try_clone_compaction_string)
                .transpose()?,
            identity_version: new_identity,
            messages_version: new_messages,
            result_version: new_result,
            state: old.state,
        };
        if checkpoint_operation_digest(&rebuilt)? != checkpoint_operation_digest(old)? {
            return Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt checkpoint operation digest disagrees with source",
            ));
        }
        checkpoints.push(rebuilt);
    }
    Ok(checkpoints)
}

fn compact_version_root(
    compact_versions: &[V2VersionRecord],
    version_id: u32,
) -> Result<V2RootRecord, V2CompactionError> {
    let index = usize::try_from(version_id).map_err(|_| {
        V2CompactionError::Overflow("v2 compaction compact version identifier exceeds usize")
    })?;
    compact_versions
        .get(index)
        .map(|version| version.root())
        .ok_or(V2CompactionError::Invalid(
            "v2 compaction compact version is absent",
        ))
}

fn remapped_version_id(
    mapping: &[V2VersionIdMapping],
    old_version: u32,
) -> Result<u32, V2CompactionError> {
    mapping
        .binary_search_by(|entry| entry.old_version.cmp(&old_version))
        .map(|index| mapping[index].new_version)
        .map_err(|_| {
            V2CompactionError::Invalid("v2 compaction version reference has no compact mapping")
        })
}

pub(super) fn remapped_node_id(
    mapping: &[V2NodeIdMapping],
    old_node: u64,
) -> Result<u64, V2CompactionError> {
    mapping
        .binary_search_by(|entry| entry.old_node.cmp(&old_node))
        .map(|index| mapping[index].new_node)
        .map_err(|_| {
            V2CompactionError::Invalid("v2 compaction node reference has no compact mapping")
        })
}

fn remapped_payload_offset(
    mapping: &[V2PayloadRangeMapping],
    old_offset: u64,
    length: u64,
) -> Result<u64, V2CompactionError> {
    mapping
        .binary_search_by(|entry| (entry.old_offset, entry.length).cmp(&(old_offset, length)))
        .map(|index| mapping[index].new_offset)
        .map_err(|_| {
            V2CompactionError::Invalid("v2 compaction payload range has no compact mapping")
        })
}

fn try_clone_compaction_string(value: &str) -> Result<String, V2CompactionError> {
    let mut cloned = String::new();
    cloned.try_reserve_exact(value.len()).map_err(|_| {
        V2CompactionError::Capacity("v2 compaction checkpoint identity allocation failed")
    })?;
    cloned.push_str(value);
    Ok(cloned)
}

#[cfg(test)]
mod tests {
    use super::super::apply_v2::{
        apply_v2_commit, V2ApplyError, V2ApplyOutcome, V2CommittedState, V2RequestStatus,
    };
    use super::super::backend_v2::{export_v2_sealed_state, recover_v2_backend, V2BackendError};
    use super::super::commit_v2::{checkpoint_operation_digest, encode_v2_commit};
    use super::super::format_v2::{decode_v2_node, encode_v2_node, V2NodeRecord, V2RootRecord};
    use super::super::image_v2::{v2_node_fields, V2NodeFields};
    use super::super::publication_v2::{
        checkpoint_state_metadata, V2CheckpointRecord, V2VersionRecord,
    };
    use super::super::snapshot_v2::V2SnapshotError;
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

    /// Builds the shared A/B/C/D/E sparse-history fixture:
    /// A(cp-1, v0/n0) -> B(cp-2, v1/n1-n2) -> C(cp-3, v2/n3-n4);
    /// A -> D(cp-sibling, v3/n5-n6, distinct root);
    /// other-thread E(other-root, v4/n7-n8, distinct root derived from A).
    fn build_abcde_state() -> V2CommittedState {
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
        state
    }

    fn delete_bc_subtree(state: &mut V2CommittedState) {
        let prepared = state
            .prepare_delete_checkpoint_subtree("thread", "cp-2")
            .unwrap();
        assert_eq!(prepared.deleted_checkpoint_count(), 2);
        state.apply_prepared_delete_checkpoint_subtree(prepared);
    }

    #[test]
    fn deleted_subtree_history_is_unreachable_but_retained_roots_survive() {
        // Physical topology under test is built by `build_abcde_state`:
        //   A(cp-1, v0/n0)
        //   ├── B(cp-2, v1/n1-n2 derived from A)
        //   │   └── C(cp-3, v2/n3-n4 derived from B)
        //   └── D(cp-sibling, v3/n5-n6 derived from A, distinct root)
        //   other-thread E(other-root, v4/n7-n8 derived from A, distinct root)
        // Deleting B must drop only the B/C-exclusive versions, nodes, and
        // payload ranges while keeping the distinct D and E histories.
        let mut state = build_abcde_state();

        let before = plan_v2_compaction(&state).unwrap();
        assert_eq!(before.retained_versions(), &[0, 1, 2, 3, 4]);
        assert_eq!(before.retained_nodes(), &[0, 1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            payload_ranges(&before),
            vec![(0, 3), (3, 3), (6, 3), (9, 3), (12, 3)]
        );

        delete_bc_subtree(&mut state);

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

    #[test]
    fn sparse_source_compaction_produces_exact_dense_remapping() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);
        let before_geometry = state.geometry().unwrap();

        let prepared = prepare_v2_compaction(&state).unwrap();
        assert_eq!(state.geometry().unwrap(), before_geometry);

        assert_eq!(
            prepared
                .version_mapping()
                .iter()
                .map(|entry| (entry.old_version(), entry.new_version()))
                .collect::<Vec<_>>(),
            vec![(0, 0), (3, 1), (4, 2)]
        );
        assert_eq!(
            prepared
                .node_mapping()
                .iter()
                .map(|entry| (entry.old_node(), entry.new_node()))
                .collect::<Vec<_>>(),
            vec![(0, 0), (5, 1), (6, 2), (7, 3), (8, 4)]
        );
        assert_eq!(
            prepared
                .payload_mapping()
                .iter()
                .map(|entry| (entry.old_offset(), entry.length(), entry.new_offset()))
                .collect::<Vec<_>>(),
            vec![(0, 3, 0), (9, 3, 3), (12, 3, 6)]
        );

        assert_eq!(prepared.payload().to_vec(), b"aaadddeee".to_vec());
        assert_eq!(prepared.nodes().len(), 5);
        assert_eq!(prepared.versions().len(), 3);
        assert_eq!(prepared.checkpoints().len(), 3);
        let checkpoint_ids: Vec<&str> = prepared
            .checkpoints()
            .iter()
            .map(|checkpoint| checkpoint.checkpoint_id.as_str())
            .collect();
        assert_eq!(checkpoint_ids, vec!["cp-1", "cp-sibling", "other-root"]);
        let checkpoint_nos: Vec<u32> = prepared
            .checkpoints()
            .iter()
            .map(|checkpoint| checkpoint.checkpoint_no)
            .collect();
        assert_eq!(checkpoint_nos, vec![1, 4, 5]);
        assert_eq!(prepared.checkpoints()[0].identity_version, 0);
        assert_eq!(prepared.checkpoints()[1].identity_version, 1);
        assert_eq!(prepared.checkpoints()[2].identity_version, 2);
        for (old, new) in state.checkpoints.iter().zip(prepared.checkpoints().iter()) {
            assert_eq!(new.thread_id, old.thread_id);
            assert_eq!(new.checkpoint_id, old.checkpoint_id);
            assert_eq!(new.parent_checkpoint_id, old.parent_checkpoint_id);
            assert_eq!(new.checkpoint_no, old.checkpoint_no);
            assert_eq!(new.state, old.state);
            assert_eq!(
                checkpoint_operation_digest(new).unwrap(),
                checkpoint_operation_digest(old).unwrap()
            );
        }
    }

    #[test]
    fn shared_source_nodes_map_to_single_compact_nodes() {
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

        let prepared = prepare_v2_compaction(&state).unwrap();
        assert_eq!(prepared.nodes().len(), 5);
        let shared: Vec<u64> = prepared
            .node_mapping()
            .iter()
            .filter(|entry| entry.old_node() == 0)
            .map(|entry| entry.new_node())
            .collect();
        assert_eq!(shared, vec![0]);
        for compact in [prepared.nodes()[2], prepared.nodes()[4]] {
            assert!(
                matches!(
                    v2_node_fields(compact).unwrap(),
                    V2NodeFields::Branch {
                        left_node_id: 0,
                        ..
                    }
                ),
                "compact branch must reference compact node 0"
            );
        }
        assert!(matches!(
            v2_node_fields(prepared.nodes()[2]).unwrap(),
            V2NodeFields::Branch {
                left_node_id: 0,
                right_node_id: 1,
                ..
            }
        ));
        assert!(matches!(
            v2_node_fields(prepared.nodes()[4]).unwrap(),
            V2NodeFields::Branch {
                left_node_id: 0,
                right_node_id: 3,
                ..
            }
        ));
    }

    #[test]
    fn retained_version_parents_remap_across_identifier_holes() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);

        let prepared = prepare_v2_compaction(&state).unwrap();
        let versions = prepared.versions();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].version_id(), 0);
        assert_eq!(versions[0].parent_version(), None);
        assert_eq!(versions[0].root().node_id(), 0);
        assert_eq!(versions[1].version_id(), 1);
        assert_eq!(versions[1].parent_version(), Some(0));
        assert_eq!(versions[1].root().node_id(), 2);
        assert_eq!(versions[2].version_id(), 2);
        assert_eq!(versions[2].parent_version(), Some(0));
        assert_eq!(versions[2].root().node_id(), 4);
    }

    #[test]
    fn version_root_metadata_mismatch_fails_closed() {
        // The forged root resolves to existing node 0 but carries another
        // leaf's commitment. Reachability accepts node existence; preparation
        // must compare root metadata and fail.
        let mut state = V2CommittedState::default();
        apply_transaction(&mut state, &genesis_transaction("cp-1", b"aaa"), b"req-1");
        let other = V2NodeRecord::leaf(0, b"xyz").unwrap();
        let forged_root = V2RootRecord::from_node(0, other).unwrap();
        state.versions = vec![V2VersionRecord::new(0, None, forged_root).unwrap()];

        let plan = plan_v2_compaction(&state).unwrap();
        assert_eq!(plan.retained_versions(), &[0]);
        assert_eq!(
            prepare_v2_compaction(&state),
            Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt version root disagrees with source"
            ))
        );
        assert_eq!(state.versions[0].root(), forged_root);
    }

    fn retained_branch_mutation_fixture() -> V2CommittedState {
        let payload = b"aaa";
        let leaf = V2NodeRecord::leaf(0, payload).unwrap();
        let leaf_root = V2RootRecord::from_node(0, leaf).unwrap();
        let branch = V2NodeRecord::branch(leaf_root, leaf_root).unwrap();
        let branch_root = V2RootRecord::from_node(1, branch).unwrap();
        let mut state = V2CommittedState::default();
        state.payload = payload.to_vec();
        state.nodes = vec![leaf, branch];
        state.versions = vec![V2VersionRecord::new(0, None, branch_root).unwrap()];
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
        state
    }

    fn with_corrupt_branch(
        mut state: V2CommittedState,
        corrupt: impl FnOnce(&mut [u8]),
    ) -> V2CommittedState {
        let mut encoded = encode_v2_node(state.nodes[1]);
        corrupt(&mut encoded);
        state.nodes[1] = decode_v2_node(&encoded).unwrap();
        state
    }

    #[test]
    fn retained_branch_semantic_mismatch_fails_closed() {
        assert!(prepare_v2_compaction(&retained_branch_mutation_fixture()).is_ok());

        let commitment = with_corrupt_branch(retained_branch_mutation_fixture(), |bytes| {
            bytes[40] ^= 1;
        });
        assert!(plan_v2_compaction(&commitment).is_ok());
        assert_eq!(
            prepare_v2_compaction(&commitment),
            Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt branch commitment disagrees with source"
            ))
        );

        let height = with_corrupt_branch(retained_branch_mutation_fixture(), |bytes| {
            bytes[6..8].copy_from_slice(&5u16.to_le_bytes());
        });
        assert!(plan_v2_compaction(&height).is_ok());
        assert_eq!(
            prepare_v2_compaction(&height),
            Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt branch height disagrees with source"
            ))
        );

        let logical_len = with_corrupt_branch(retained_branch_mutation_fixture(), |bytes| {
            bytes[8..16].copy_from_slice(&99u64.to_le_bytes());
        });
        assert!(plan_v2_compaction(&logical_len).is_ok());
        assert_eq!(
            prepare_v2_compaction(&logical_len),
            Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt branch logical length disagrees with source"
            ))
        );

        // Mutating only the left-length field keeps the stored commitment
        // identical to the canonical rebuild, so the explicit left-length
        // comparison is the load-bearing check here.
        let left_len = with_corrupt_branch(retained_branch_mutation_fixture(), |bytes| {
            bytes[32..40].copy_from_slice(&1u64.to_le_bytes());
        });
        assert!(plan_v2_compaction(&left_len).is_ok());
        assert_eq!(
            prepare_v2_compaction(&left_len),
            Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt branch left length disagrees with source"
            ))
        );
    }

    fn overlapping_range_state() -> V2CommittedState {
        let payload = b"aaabbb";
        let first = V2NodeRecord::leaf(0, payload).unwrap();
        let second = V2NodeRecord::leaf(2, b"bb").unwrap();
        let first_root = V2RootRecord::from_node(0, first).unwrap();
        let second_root = V2RootRecord::from_node(1, second).unwrap();
        let mut state = V2CommittedState::default();
        state.payload = payload.to_vec();
        state.nodes = vec![first, second];
        state.versions = vec![
            V2VersionRecord::new(0, None, first_root).unwrap(),
            V2VersionRecord::new(1, Some(0), second_root).unwrap(),
        ];
        state.checkpoints = vec![
            V2CheckpointRecord {
                checkpoint_no: 1,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-1".to_owned(),
                parent_checkpoint_id: None,
                identity_version: 0,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(first_root, None, None).unwrap(),
            },
            V2CheckpointRecord {
                checkpoint_no: 2,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-2".to_owned(),
                parent_checkpoint_id: Some("cp-1".to_owned()),
                identity_version: 1,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(second_root, None, None).unwrap(),
            },
        ];
        state
            .checkpoint_ordinals
            .insert(("thread".to_owned(), "cp-1".to_owned()), 0);
        state
            .checkpoint_ordinals
            .insert(("thread".to_owned(), "cp-2".to_owned()), 1);
        state
    }

    #[test]
    fn overlapping_retained_payload_ranges_fail_closed() {
        // Each range is individually in-bounds, so reachability retains both
        // leaves; preparation must reject the overlap instead of merging it.
        let overlapping = overlapping_range_state();
        assert!(plan_v2_compaction(&overlapping).is_ok());
        assert_eq!(
            prepare_v2_compaction(&overlapping),
            Err(V2CompactionError::Invalid(
                "v2 compaction retained payload ranges overlap or duplicate"
            ))
        );

        let payload = b"aaa";
        let leaf = V2NodeRecord::leaf(0, payload).unwrap();
        let first_root = V2RootRecord::from_node(0, leaf).unwrap();
        let second_root = V2RootRecord::from_node(1, leaf).unwrap();
        let duplicate = V2CommittedState {
            payload: payload.to_vec(),
            nodes: vec![leaf, leaf],
            versions: vec![
                V2VersionRecord::new(0, None, first_root).unwrap(),
                V2VersionRecord::new(1, Some(0), second_root).unwrap(),
            ],
            checkpoints: vec![V2CheckpointRecord {
                checkpoint_no: 1,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-1".to_owned(),
                parent_checkpoint_id: None,
                identity_version: 1,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(second_root, None, None).unwrap(),
            }],
            ..Default::default()
        };
        assert!(plan_v2_compaction(&duplicate).is_ok());
        assert_eq!(
            prepare_v2_compaction(&duplicate),
            Err(V2CompactionError::Invalid(
                "v2 compaction retained payload ranges overlap or duplicate"
            ))
        );
    }

    #[test]
    fn inconsistent_checkpoint_state_fails_closed() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);
        let other = V2NodeRecord::leaf(0, b"zzz").unwrap();
        let other_root = V2RootRecord::from_node(0, other).unwrap();
        state.checkpoints[1].state = checkpoint_state_metadata(other_root, None, None).unwrap();
        assert_eq!(
            prepare_v2_compaction(&state),
            Err(V2CompactionError::Invalid(
                "v2 compaction rebuilt checkpoint state disagrees with source"
            ))
        );
        assert_eq!(state.checkpoints.len(), 3);
    }

    #[test]
    fn operation_digests_survive_physical_remapping() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);

        let prepared = prepare_v2_compaction(&state).unwrap();
        assert_eq!(state.checkpoints.len(), prepared.checkpoints().len());
        let mut seen: Vec<[u8; 32]> = Vec::new();
        for (old, new) in state.checkpoints.iter().zip(prepared.checkpoints().iter()) {
            let old_digest = checkpoint_operation_digest(old).unwrap();
            assert_eq!(checkpoint_operation_digest(new).unwrap(), old_digest);
            assert!(!seen.contains(&old_digest));
            seen.push(old_digest);
        }
        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn tombstone_only_state_prepares_empty_replacement() {
        let mut state = V2CommittedState::default();
        state
            .deleted_checkpoints
            .insert(("thread".to_owned(), "cp-1".to_owned()));
        state.retired_requests.insert(b"req-1".to_vec(), [0x99; 32]);
        let before_deleted = state.deleted_checkpoints.clone();
        let before_retired = state.retired_requests.clone();

        let prepared = prepare_v2_compaction(&state).unwrap();
        assert!(prepared.payload().is_empty());
        assert!(prepared.nodes().is_empty());
        assert!(prepared.versions().is_empty());
        assert!(prepared.checkpoints().is_empty());
        assert!(prepared.version_mapping().is_empty());
        assert!(prepared.node_mapping().is_empty());
        assert!(prepared.payload_mapping().is_empty());
        assert_eq!(state.deleted_checkpoints, before_deleted);
        assert_eq!(state.retired_requests, before_retired);
    }

    fn operation_digest_at(state: &V2CommittedState, ordinal: usize) -> [u8; 32] {
        checkpoint_operation_digest(&state.checkpoints[ordinal]).unwrap()
    }

    #[test]
    fn compacted_state_exports_and_reopens_with_authority_preserved() {
        let mut state = build_abcde_state();
        let digest_a = operation_digest_at(&state, 0);
        let digest_b = operation_digest_at(&state, 1);
        let digest_c = operation_digest_at(&state, 2);
        let digest_d = operation_digest_at(&state, 3);
        let digest_e = operation_digest_at(&state, 4);
        delete_bc_subtree(&mut state);

        compact_v2_state(&mut state).unwrap();
        let geometry = state.geometry().unwrap();
        assert_eq!(
            geometry,
            V2WalGeometry {
                payload_len: 9,
                node_count: 5,
                version_count: 3,
                checkpoint_count: 3,
            }
        );

        let snapshot = export_v2_sealed_state(&state).unwrap().unwrap();
        let reopened = recover_v2_backend(Some(&snapshot), &[]).unwrap();
        assert_eq!(reopened.state.geometry().unwrap(), geometry);
        assert_eq!(reopened.state.checkpoints, state.checkpoints);
        assert_eq!(
            reopened.state.checkpoint_ordinals,
            state.checkpoint_ordinals
        );
        assert_eq!(reopened.state.request_records, state.request_records);
        assert_eq!(reopened.state.retired_requests, state.retired_requests);
        assert_eq!(
            reopened.state.deleted_checkpoints,
            state.deleted_checkpoints
        );
        assert!(reopened
            .state
            .deleted_checkpoints
            .contains(&("thread".to_owned(), "cp-2".to_owned())));
        assert!(reopened
            .state
            .deleted_checkpoints
            .contains(&("thread".to_owned(), "cp-3".to_owned())));

        assert_eq!(
            reopened.state.classify_request(b"req-1", digest_a),
            Ok(V2RequestStatus::Replay {
                checkpoint_ordinal: 0
            })
        );
        assert_eq!(
            reopened.state.classify_request(b"req-4", digest_d),
            Ok(V2RequestStatus::Replay {
                checkpoint_ordinal: 1
            })
        );
        assert_eq!(
            reopened.state.classify_request(b"req-5", digest_e),
            Ok(V2RequestStatus::Replay {
                checkpoint_ordinal: 2
            })
        );
        assert_eq!(
            reopened.state.classify_request(b"req-2", digest_b),
            Ok(V2RequestStatus::Retired)
        );
        assert_eq!(
            reopened.state.classify_request(b"req-3", digest_c),
            Ok(V2RequestStatus::Retired)
        );
        assert_eq!(
            reopened.state.classify_request(b"req-1", [0xFF; 32]),
            Err(V2ApplyError::RequestConflict)
        );
        assert_eq!(
            reopened.state.classify_request(b"req-2", [0xFF; 32]),
            Err(V2ApplyError::RequestConflict)
        );
    }

    #[test]
    fn compacted_state_remains_appendable_and_reopens() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);
        compact_v2_state(&mut state).unwrap();

        // Append a new checkpoint from retained compacted D (ordinal 1,
        // compact version 1) using the standard transaction machinery.
        let compact_base = state.geometry().unwrap();
        let parent_root = state.versions[1].root();
        let next = branch_child_transaction(
            compact_base,
            6,
            "thread",
            "cp-4",
            Some("cp-sibling"),
            1,
            parent_root,
            b"fff",
        );
        let encoded = encode_v2_commit(compact_base, &next, Some(b"req-6")).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Ok(V2ApplyOutcome::Applied {
                checkpoint_ordinal: 3
            })
        );
        assert_eq!(state.versions[3].version_id(), 3);
        assert_eq!(state.versions[3].parent_version(), Some(1));
        assert_eq!(state.checkpoints.len(), 4);
        assert_eq!(state.checkpoints[3].checkpoint_id, "cp-4");

        // Deleted B/C identities remain non-resurrectable after compaction.
        let zombie_base = state.geometry().unwrap();
        let zombie_root = state.versions[0].root();
        let zombie = branch_child_transaction(
            zombie_base,
            7,
            "thread",
            "cp-2",
            Some("cp-1"),
            0,
            zombie_root,
            b"zzz",
        );
        let zombie_encoded = encode_v2_commit(zombie_base, &zombie, Some(b"req-zombie")).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &zombie_encoded),
            Err(V2ApplyError::Invalid(
                "v2 checkpoint identity was logically deleted"
            ))
        );
        assert_eq!(state.geometry().unwrap(), zombie_base);

        let snapshot = export_v2_sealed_state(&state).unwrap().unwrap();
        let reopened = recover_v2_backend(Some(&snapshot), &[]).unwrap();
        assert_eq!(reopened.state.checkpoints.len(), 4);
        assert_eq!(reopened.state.checkpoints[3].checkpoint_id, "cp-4");
        assert_eq!(
            reopened.state.checkpoints[3].parent_checkpoint_id,
            Some("cp-sibling".to_owned())
        );
    }

    #[test]
    fn tombstone_only_state_compacts_as_idempotent_noop() {
        let mut state = V2CommittedState::default();
        state
            .deleted_checkpoints
            .insert(("thread".to_owned(), "cp-1".to_owned()));
        state.retired_requests.insert(b"req-1".to_vec(), [0x11; 32]);

        compact_v2_state(&mut state).unwrap();
        assert_eq!(state.geometry().unwrap(), V2WalGeometry::default());

        let snapshot = export_v2_sealed_state(&state).unwrap().unwrap();
        let reopened = recover_v2_backend(Some(&snapshot), &[]).unwrap();
        assert_eq!(reopened.state.geometry().unwrap(), V2WalGeometry::default());
        assert_eq!(
            reopened.state.deleted_checkpoints,
            state.deleted_checkpoints
        );
        assert_eq!(reopened.state.retired_requests, state.retired_requests);
    }

    #[test]
    fn compacting_twice_reaches_identical_physical_state() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);
        compact_v2_state(&mut state).unwrap();

        let payload = state.payload.clone();
        let nodes = state.nodes.clone();
        let versions = state.versions.clone();
        let checkpoints = state.checkpoints.clone();
        let ordinals = state.checkpoint_ordinals.clone();
        let active = state.request_records.clone();
        let retired = state.retired_requests.clone();
        let tombstones = state.deleted_checkpoints.clone();

        compact_v2_state(&mut state).unwrap();
        assert_eq!(state.payload, payload);
        assert_eq!(state.nodes, nodes);
        assert_eq!(state.versions, versions);
        assert_eq!(state.checkpoints, checkpoints);
        assert_eq!(state.checkpoint_ordinals, ordinals);
        assert_eq!(state.request_records, active);
        assert_eq!(state.retired_requests, retired);
        assert_eq!(state.deleted_checkpoints, tombstones);
    }

    #[test]
    fn failed_compaction_leaves_committed_state_unchanged() {
        let mut state = overlapping_range_state();
        let payload = state.payload.clone();
        let nodes = state.nodes.clone();
        let versions = state.versions.clone();
        let checkpoints = state.checkpoints.clone();
        let ordinals = state.checkpoint_ordinals.clone();
        let active = state.request_records.clone();
        let retired = state.retired_requests.clone();
        let tombstones = state.deleted_checkpoints.clone();

        assert_eq!(
            compact_v2_state(&mut state),
            Err(V2CompactionError::Invalid(
                "v2 compaction retained payload ranges overlap or duplicate"
            ))
        );
        assert_eq!(state.payload, payload);
        assert_eq!(state.nodes, nodes);
        assert_eq!(state.versions, versions);
        assert_eq!(state.checkpoints, checkpoints);
        assert_eq!(state.checkpoint_ordinals, ordinals);
        assert_eq!(state.request_records, active);
        assert_eq!(state.retired_requests, retired);
        assert_eq!(state.deleted_checkpoints, tombstones);
    }

    fn assert_unchanged_against_twin(state: &V2CommittedState, twin: &V2CommittedState) {
        assert_eq!(state.payload, twin.payload);
        assert_eq!(state.nodes, twin.nodes);
        assert_eq!(state.versions, twin.versions);
        assert_eq!(state.checkpoints, twin.checkpoints);
        assert_eq!(state.checkpoint_ordinals, twin.checkpoint_ordinals);
        assert_eq!(state.request_records, twin.request_records);
        assert_eq!(state.retired_requests, twin.retired_requests);
        assert_eq!(state.deleted_checkpoints, twin.deleted_checkpoints);
    }

    #[test]
    fn compact_artifact_prepares_from_sparse_source_without_mutation() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);
        let mut twin = build_abcde_state();
        delete_bc_subtree(&mut twin);

        let bytes = prepare_compacted_v2_sealed_artifact(&state)
            .unwrap()
            .unwrap();
        assert_unchanged_against_twin(&state, &twin);
        // The source physical geometry is still the sparse pre-compaction
        // geometry; only the reopened artifact is dense.
        assert_eq!(
            state.geometry().unwrap(),
            V2WalGeometry {
                payload_len: 15,
                node_count: 9,
                version_count: 5,
                checkpoint_count: 3,
            }
        );

        let reopened = recover_v2_backend(Some(&bytes), &[]).unwrap();
        assert_eq!(
            reopened.state.geometry().unwrap(),
            V2WalGeometry {
                payload_len: 9,
                node_count: 5,
                version_count: 3,
                checkpoint_count: 3,
            }
        );
    }

    #[test]
    fn compact_artifact_reopens_with_exact_semantic_equivalence() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);

        let bytes = prepare_compacted_v2_sealed_artifact(&state)
            .unwrap()
            .unwrap();
        let reopened = recover_v2_backend(Some(&bytes), &[]).unwrap();
        // Logical checkpoint content is exact; only physical version
        // coordinates change (source [0,3,4] becomes dense [0,1,2]).
        assert_eq!(reopened.state.checkpoints.len(), state.checkpoints.len());
        for (new, old) in reopened
            .state
            .checkpoints
            .iter()
            .zip(state.checkpoints.iter())
        {
            assert_eq!(new.checkpoint_no, old.checkpoint_no);
            assert_eq!(new.thread_id, old.thread_id);
            assert_eq!(new.checkpoint_id, old.checkpoint_id);
            assert_eq!(new.parent_checkpoint_id, old.parent_checkpoint_id);
            assert_eq!(new.state, old.state);
        }
        let reopened_identities: Vec<u32> = reopened
            .state
            .checkpoints
            .iter()
            .map(|checkpoint| checkpoint.identity_version)
            .collect();
        assert_eq!(reopened_identities, vec![0, 1, 2]);
        assert_eq!(
            reopened.state.checkpoint_ordinals,
            state.checkpoint_ordinals
        );
        assert_eq!(reopened.state.request_records, state.request_records);
        assert_eq!(reopened.state.retired_requests, state.retired_requests);
        assert_eq!(
            reopened.state.deleted_checkpoints,
            state.deleted_checkpoints
        );
        assert_ne!(reopened.state.payload.len(), state.payload.len());
        assert_ne!(reopened.state.nodes.len(), state.nodes.len());
        assert_ne!(reopened.state.versions.len(), state.versions.len());
    }

    #[test]
    fn compact_artifact_reopen_preserves_request_behavior() {
        let mut state = build_abcde_state();
        let digest_a = operation_digest_at(&state, 0);
        let digest_b = operation_digest_at(&state, 1);
        let digest_c = operation_digest_at(&state, 2);
        let digest_d = operation_digest_at(&state, 3);
        let digest_e = operation_digest_at(&state, 4);
        delete_bc_subtree(&mut state);

        let bytes = prepare_compacted_v2_sealed_artifact(&state)
            .unwrap()
            .unwrap();
        let reopened = recover_v2_backend(Some(&bytes), &[]).unwrap();
        assert_eq!(
            reopened.state.classify_request(b"req-1", digest_a),
            Ok(V2RequestStatus::Replay {
                checkpoint_ordinal: 0
            })
        );
        assert_eq!(
            reopened.state.classify_request(b"req-4", digest_d),
            Ok(V2RequestStatus::Replay {
                checkpoint_ordinal: 1
            })
        );
        assert_eq!(
            reopened.state.classify_request(b"req-5", digest_e),
            Ok(V2RequestStatus::Replay {
                checkpoint_ordinal: 2
            })
        );
        assert_eq!(
            reopened.state.classify_request(b"req-2", digest_b),
            Ok(V2RequestStatus::Retired)
        );
        assert_eq!(
            reopened.state.classify_request(b"req-3", digest_c),
            Ok(V2RequestStatus::Retired)
        );
        assert_eq!(
            reopened.state.classify_request(b"req-4", [0xFF; 32]),
            Err(V2ApplyError::RequestConflict)
        );
        assert_eq!(
            reopened.state.classify_request(b"req-2", [0xFF; 32]),
            Err(V2ApplyError::RequestConflict)
        );
    }

    #[test]
    fn compact_artifact_matches_accepted_semantic_compactor_bytes() {
        let mut source1 = build_abcde_state();
        delete_bc_subtree(&mut source1);
        let bytes = prepare_compacted_v2_sealed_artifact(&source1)
            .unwrap()
            .unwrap();

        let mut source2 = build_abcde_state();
        delete_bc_subtree(&mut source2);
        compact_v2_state(&mut source2).unwrap();
        let ordinary = export_v2_sealed_state(&source2).unwrap().unwrap();

        assert_eq!(bytes, ordinary);
        let from_artifact = recover_v2_backend(Some(&bytes), &[]).unwrap();
        let from_compacted = recover_v2_backend(Some(&ordinary), &[]).unwrap();
        assert_eq!(
            from_artifact.state.checkpoints,
            from_compacted.state.checkpoints
        );
        assert_eq!(
            from_artifact.state.checkpoint_ordinals,
            from_compacted.state.checkpoint_ordinals
        );
        assert_eq!(
            from_artifact.state.request_records,
            from_compacted.state.request_records
        );
        assert_eq!(
            from_artifact.state.retired_requests,
            from_compacted.state.retired_requests
        );
        assert_eq!(
            from_artifact.state.deleted_checkpoints,
            from_compacted.state.deleted_checkpoints
        );
        assert_eq!(
            from_artifact.state.geometry().unwrap(),
            from_compacted.state.geometry().unwrap()
        );
    }

    #[test]
    fn compact_artifact_rejects_corrupt_checkpoint_index() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);
        let mut twin = build_abcde_state();
        delete_bc_subtree(&mut twin);
        state
            .checkpoint_ordinals
            .insert(("thread".to_owned(), "bogus".to_owned()), 99);
        twin.checkpoint_ordinals
            .insert(("thread".to_owned(), "bogus".to_owned()), 99);

        assert_eq!(
            prepare_compacted_v2_sealed_artifact(&state),
            Err(V2CompactionError::Backend(V2BackendError::Invalid(
                "v2 checkpoint index cardinality disagrees with checkpoint table"
            )))
        );
        assert_unchanged_against_twin(&state, &twin);
    }

    #[test]
    fn compact_artifact_rejects_active_retired_overlap() {
        let mut state = build_abcde_state();
        delete_bc_subtree(&mut state);
        let mut twin = build_abcde_state();
        delete_bc_subtree(&mut twin);
        state.retired_requests.insert(b"req-4".to_vec(), [0x55; 32]);
        twin.retired_requests.insert(b"req-4".to_vec(), [0x55; 32]);

        assert_eq!(
            prepare_compacted_v2_sealed_artifact(&state),
            Err(V2CompactionError::Snapshot(V2SnapshotError::Invalid(
                "v2 request identity is both active and retired"
            )))
        );
        assert_unchanged_against_twin(&state, &twin);
    }

    #[test]
    fn compact_artifact_preserves_tombstone_only_authority() {
        let mut state = V2CommittedState::default();
        state
            .deleted_checkpoints
            .insert(("thread".to_owned(), "cp-1".to_owned()));
        state.retired_requests.insert(b"req-1".to_vec(), [0x77; 32]);

        let bytes = prepare_compacted_v2_sealed_artifact(&state)
            .unwrap()
            .unwrap();
        let reopened = recover_v2_backend(Some(&bytes), &[]).unwrap();
        assert_eq!(reopened.state.geometry().unwrap(), V2WalGeometry::default());
        assert_eq!(
            reopened.state.deleted_checkpoints,
            state.deleted_checkpoints
        );
        assert_eq!(reopened.state.retired_requests, state.retired_requests);
    }

    #[test]
    fn compact_artifact_of_truly_empty_state_is_absent() {
        let state = V2CommittedState::default();
        assert_eq!(prepare_compacted_v2_sealed_artifact(&state), Ok(None));
    }

    #[test]
    fn compact_artifact_rejects_uncompactionable_physical_state() {
        let state = overlapping_range_state();
        let twin = overlapping_range_state();
        assert_eq!(
            prepare_compacted_v2_sealed_artifact(&state),
            Err(V2CompactionError::Invalid(
                "v2 compaction retained payload ranges overlap or duplicate"
            ))
        );
        assert_unchanged_against_twin(&state, &twin);
    }
}
