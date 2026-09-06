//! Pure committed-state validation and idempotent apply for staged Format v2.
//!
//! This module deliberately performs no filesystem I/O. A production WAL
//! scanner/writer can use the same validation boundary before publishing or
//! replaying a `T2C2` commit into authoritative state.

use super::commit_v2::{
    checkpoint_operation_digest, decode_v2_commit, V2CommitError, V2DecodedCommit,
};
use super::format_v2::{encode_v2_node, V2FormatError, V2NodeRecord, V2RootRecord};
use super::publication_v2::{
    checkpoint_state_metadata, V2CheckpointRecord, V2PublicationError, V2VersionRecord,
};
use super::transaction_v2::{V2WalGeometry, V2WalTransaction};
use std::collections::{HashMap, HashSet};
use std::fmt;

const V2_MAX_REQUEST_ID_BYTES: usize = 4096;
const V2_MAX_IDENTIFIER_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2ApplyError {
    Commit(V2CommitError),
    Format(V2FormatError),
    Publication(V2PublicationError),
    Invalid(&'static str),
    Overflow(&'static str),
    Capacity(&'static str),
    RequestConflict,
}

impl fmt::Display for V2ApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Commit(error) => write!(formatter, "{error}"),
            Self::Format(error) => write!(formatter, "{error}"),
            Self::Publication(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) | Self::Capacity(message) => {
                formatter.write_str(message)
            }
            Self::RequestConflict => formatter.write_str(
                "v2 request identity was already used for a different logical operation",
            ),
        }
    }
}

impl std::error::Error for V2ApplyError {}

impl From<V2CommitError> for V2ApplyError {
    fn from(error: V2CommitError) -> Self {
        Self::Commit(error)
    }
}

impl From<V2FormatError> for V2ApplyError {
    fn from(error: V2FormatError) -> Self {
        Self::Format(error)
    }
}

impl From<V2PublicationError> for V2ApplyError {
    fn from(error: V2PublicationError) -> Self {
        Self::Publication(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2RequestStatus {
    New,
    Replay { checkpoint_ordinal: u64 },
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2ApplyOutcome {
    Applied { checkpoint_ordinal: u64 },
    Replayed { checkpoint_ordinal: u64 },
    RetiredRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2RequestRecord {
    pub(super) operation_digest: [u8; 32],
    pub(super) checkpoint_ordinal: u64,
}

#[derive(Debug)]
pub(super) struct V2PreparedSubtreeDelete {
    checkpoints: Vec<V2CheckpointRecord>,
    checkpoint_ordinals: HashMap<(String, String), u64>,
    request_records: HashMap<Vec<u8>, V2RequestRecord>,
    retired_requests: HashMap<Vec<u8>, [u8; 32]>,
    deleted_checkpoints: HashSet<(String, String)>,
    clear_sequence_geometry: bool,
    deleted_checkpoint_count: u64,
}

impl V2PreparedSubtreeDelete {
    pub(super) const fn deleted_checkpoint_count(&self) -> u64 {
        self.deleted_checkpoint_count
    }
}

#[derive(Debug, Default)]
pub(super) struct V2CommittedState {
    pub(super) payload: Vec<u8>,
    pub(super) nodes: Vec<V2NodeRecord>,
    pub(super) versions: Vec<V2VersionRecord>,
    pub(super) checkpoints: Vec<V2CheckpointRecord>,
    pub(super) checkpoint_ordinals: HashMap<(String, String), u64>,
    pub(super) request_records: HashMap<Vec<u8>, V2RequestRecord>,
    pub(super) retired_requests: HashMap<Vec<u8>, [u8; 32]>,
    pub(super) deleted_checkpoints: HashSet<(String, String)>,
}

impl V2CommittedState {
    pub(super) fn geometry(&self) -> Result<V2WalGeometry, V2ApplyError> {
        Ok(V2WalGeometry {
            payload_len: u64::try_from(self.payload.len())
                .map_err(|_| V2ApplyError::Overflow("v2 state payload length exceeds u64"))?,
            node_count: u64::try_from(self.nodes.len())
                .map_err(|_| V2ApplyError::Overflow("v2 state node count exceeds u64"))?,
            version_count: u64::try_from(self.versions.len())
                .map_err(|_| V2ApplyError::Overflow("v2 state version count exceeds u64"))?,
            checkpoint_count: u64::try_from(self.checkpoints.len())
                .map_err(|_| V2ApplyError::Overflow("v2 state checkpoint count exceeds u64"))?,
        })
    }

    pub(super) fn classify_request(
        &self,
        request_id: &[u8],
        operation_digest: [u8; 32],
    ) -> Result<V2RequestStatus, V2ApplyError> {
        validate_request_id(request_id)?;
        if let Some(record) = self.request_records.get(request_id) {
            return if record.operation_digest == operation_digest {
                Ok(V2RequestStatus::Replay {
                    checkpoint_ordinal: record.checkpoint_ordinal,
                })
            } else {
                Err(V2ApplyError::RequestConflict)
            };
        }
        if let Some(retired_digest) = self.retired_requests.get(request_id) {
            return if *retired_digest == operation_digest {
                Ok(V2RequestStatus::Retired)
            } else {
                Err(V2ApplyError::RequestConflict)
            };
        }
        Ok(V2RequestStatus::New)
    }

    pub(super) fn retire_request(&mut self, request_id: &[u8]) -> Result<(), V2ApplyError> {
        validate_request_id(request_id)?;
        if self.retired_requests.contains_key(request_id) {
            return Err(V2ApplyError::Invalid(
                "v2 request identity is already retired",
            ));
        }

        let operation_digest = self
            .request_records
            .get(request_id)
            .ok_or(V2ApplyError::Invalid(
                "v2 active request identity is absent",
            ))?
            .operation_digest;

        let mut retired_key = Vec::new();
        retired_key
            .try_reserve_exact(request_id.len())
            .map_err(|_| V2ApplyError::Capacity("v2 retired request key allocation failed"))?;
        retired_key.extend_from_slice(request_id);
        self.retired_requests
            .try_reserve(1)
            .map_err(|_| V2ApplyError::Capacity("v2 retired request map allocation failed"))?;

        let _ = self.request_records.remove(request_id);
        let _ = self.retired_requests.insert(retired_key, operation_digest);
        Ok(())
    }

    pub(super) fn prepare_delete_checkpoint_subtree(
        &self,
        thread_id: &str,
        checkpoint_id: &str,
    ) -> Result<V2PreparedSubtreeDelete, V2ApplyError> {
        validate_checkpoint_identifier(thread_id)?;
        validate_checkpoint_identifier(checkpoint_id)?;
        let checkpoint_positions = self.validate_delete_source_state()?;

        let target_index = match checkpoint_positions.get(&(thread_id, checkpoint_id)).copied() {
            Some(index) => index,
            None => {
                if self
                    .deleted_checkpoints
                    .iter()
                    .any(|(thread, checkpoint)| thread == thread_id && checkpoint == checkpoint_id)
                {
                    return Err(V2ApplyError::Invalid(
                        "v2 checkpoint identity is already deleted",
                    ));
                }
                return Err(V2ApplyError::Invalid(
                    "v2 checkpoint identity to delete is absent",
                ));
            }
        };

        let checkpoint_count = self.checkpoints.len();
        let mut delete_mask = Vec::new();
        delete_mask
            .try_reserve_exact(checkpoint_count)
            .map_err(|_| V2ApplyError::Capacity("v2 deletion mask allocation failed"))?;
        delete_mask.resize(checkpoint_count, 0);
        delete_mask[target_index] = 1;

        for index in target_index.saturating_add(1)..checkpoint_count {
            let checkpoint = &self.checkpoints[index];
            if checkpoint.thread_id != thread_id {
                continue;
            }
            let Some(parent) = checkpoint.parent_checkpoint_id.as_deref() else {
                continue;
            };
            let parent_index = checkpoint_positions
                .get(&(checkpoint.thread_id.as_str(), parent))
                .copied()
                .ok_or(V2ApplyError::Invalid(
                    "v2 live checkpoint parent is absent from the index",
                ))?;
            if parent_index >= index {
                return Err(V2ApplyError::Invalid(
                    "v2 live checkpoint parent is not topologically prior",
                ));
            }
            if delete_mask[parent_index] != 0 {
                delete_mask[index] = 1;
            }
        }

        self.prepare_delete_ledgers(delete_mask)
    }

    pub(super) fn apply_prepared_delete_checkpoint_subtree(
        &mut self,
        prepared: V2PreparedSubtreeDelete,
    ) {
        let V2PreparedSubtreeDelete {
            checkpoints,
            checkpoint_ordinals,
            request_records,
            retired_requests,
            deleted_checkpoints,
            clear_sequence_geometry,
            deleted_checkpoint_count: _,
        } = prepared;

        self.checkpoints = checkpoints;
        self.checkpoint_ordinals = checkpoint_ordinals;
        self.request_records = request_records;
        self.retired_requests = retired_requests;
        self.deleted_checkpoints = deleted_checkpoints;

        if clear_sequence_geometry {
            self.payload.clear();
            self.nodes.clear();
            self.versions.clear();
        }
    }

    fn validate_delete_source_state(
        &self,
    ) -> Result<HashMap<(&str, &str), usize>, V2ApplyError> {
        if self.checkpoint_ordinals.len() != self.checkpoints.len() {
            return Err(V2ApplyError::Invalid(
                "v2 checkpoint index cardinality disagrees with checkpoint table",
            ));
        }

        let mut positions = HashMap::new();
        positions
            .try_reserve(self.checkpoints.len())
            .map_err(|_| V2ApplyError::Capacity("v2 checkpoint position map allocation failed"))?;

        for (index, checkpoint) in self.checkpoints.iter().enumerate() {
            validate_checkpoint_identifier(&checkpoint.thread_id)?;
            validate_checkpoint_identifier(&checkpoint.checkpoint_id)?;
            if let Some(parent) = checkpoint.parent_checkpoint_id.as_deref() {
                validate_checkpoint_identifier(parent)?;
            }
            if self.deleted_checkpoints.iter().any(|(thread_id, checkpoint_id)| {
                thread_id == &checkpoint.thread_id && checkpoint_id == &checkpoint.checkpoint_id
            }) {
                return Err(V2ApplyError::Invalid(
                    "v2 checkpoint identity is both live and deleted",
                ));
            }
            if positions
                .insert(
                    (
                        checkpoint.thread_id.as_str(),
                        checkpoint.checkpoint_id.as_str(),
                    ),
                    index,
                )
                .is_some()
            {
                return Err(V2ApplyError::Invalid(
                    "v2 checkpoint table contains duplicate identity",
                ));
            }
            if let Some(parent) = checkpoint.parent_checkpoint_id.as_deref() {
                let parent_index = positions
                    .get(&(checkpoint.thread_id.as_str(), parent))
                    .copied()
                    .ok_or(V2ApplyError::Invalid(
                        "v2 live checkpoint parent is not topologically prior in the same thread",
                    ))?;
                if parent_index >= index {
                    return Err(V2ApplyError::Invalid(
                        "v2 live checkpoint parent is not topologically prior",
                    ));
                }
            }
        }

        for ((thread_id, checkpoint_id), ordinal) in &self.checkpoint_ordinals {
            let index = positions
                .get(&(thread_id.as_str(), checkpoint_id.as_str()))
                .copied()
                .ok_or(V2ApplyError::Invalid(
                    "v2 checkpoint index contains an identity absent from the checkpoint table",
                ))?;
            let expected_ordinal = u64::try_from(index)
                .map_err(|_| V2ApplyError::Overflow("v2 checkpoint ordinal exceeds u64"))?;
            if *ordinal != expected_ordinal {
                return Err(V2ApplyError::Invalid(
                    "v2 checkpoint index disagrees with checkpoint table",
                ));
            }
        }

        for (thread_id, checkpoint_id) in &self.deleted_checkpoints {
            validate_checkpoint_identifier(thread_id)?;
            validate_checkpoint_identifier(checkpoint_id)?;
        }

        for (request_id, record) in &self.request_records {
            validate_request_id(request_id)?;
            if self.retired_requests.contains_key(request_id) {
                return Err(V2ApplyError::Invalid(
                    "v2 request identity is both active and retired",
                ));
            }
            let checkpoint_index = usize::try_from(record.checkpoint_ordinal)
                .map_err(|_| V2ApplyError::Overflow("v2 request checkpoint ordinal exceeds usize"))?;
            let checkpoint = self
                .checkpoints
                .get(checkpoint_index)
                .ok_or(V2ApplyError::Invalid(
                    "v2 active request checkpoint ordinal is outside the checkpoint table",
                ))?;
            if checkpoint_operation_digest(checkpoint)? != record.operation_digest {
                return Err(V2ApplyError::Invalid(
                    "v2 active request digest disagrees with its checkpoint",
                ));
            }
        }
        for request_id in self.retired_requests.keys() {
            validate_request_id(request_id)?;
        }

        Ok(positions)
    }

    fn prepare_delete_ledgers(
        &self,
        delete_mask: Vec<u8>,
    ) -> Result<V2PreparedSubtreeDelete, V2ApplyError> {
        let deleted_count = delete_mask.iter().filter(|value| **value != 0).count();
        let retained_count = self
            .checkpoints
            .len()
            .checked_sub(deleted_count)
            .ok_or(V2ApplyError::Invalid(
                "v2 deletion mask exceeds checkpoint table",
            ))?;
        let deleted_checkpoint_count = u64::try_from(deleted_count)
            .map_err(|_| V2ApplyError::Overflow("v2 deleted checkpoint count exceeds u64"))?;

        let mut checkpoints = Vec::new();
        checkpoints
            .try_reserve_exact(retained_count)
            .map_err(|_| V2ApplyError::Capacity("v2 retained checkpoint table allocation failed"))?;

        let mut checkpoint_ordinals = HashMap::new();
        checkpoint_ordinals
            .try_reserve(retained_count)
            .map_err(|_| V2ApplyError::Capacity("v2 retained checkpoint index allocation failed"))?;

        let mut new_ordinals = Vec::new();
        new_ordinals
            .try_reserve_exact(self.checkpoints.len())
            .map_err(|_| V2ApplyError::Capacity("v2 checkpoint ordinal map allocation failed"))?;
        new_ordinals.resize(self.checkpoints.len(), None);

        let final_deleted_count = self
            .deleted_checkpoints
            .len()
            .checked_add(deleted_count)
            .ok_or(V2ApplyError::Overflow(
                "v2 deleted checkpoint ledger length exceeds usize",
            ))?;
        let mut deleted_checkpoints = HashSet::new();
        deleted_checkpoints
            .try_reserve(final_deleted_count)
            .map_err(|_| V2ApplyError::Capacity("v2 deleted checkpoint ledger allocation failed"))?;
        for (thread_id, checkpoint_id) in &self.deleted_checkpoints {
            let key = try_clone_checkpoint_key(thread_id, checkpoint_id)?;
            let _ = deleted_checkpoints.insert(key);
        }

        let mut next_ordinal = 0u64;
        for (index, checkpoint) in self.checkpoints.iter().enumerate() {
            let deleted = delete_mask
                .get(index)
                .copied()
                .ok_or(V2ApplyError::Invalid(
                    "v2 checkpoint is outside the deletion mask",
                ))?
                != 0;
            if deleted {
                let key = try_clone_checkpoint_key(&checkpoint.thread_id, &checkpoint.checkpoint_id)?;
                let _ = deleted_checkpoints.insert(key);
            } else {
                let retained = try_clone_checkpoint_record(checkpoint)?;
                let key = try_clone_checkpoint_key(&checkpoint.thread_id, &checkpoint.checkpoint_id)?;
                checkpoints.push(retained);
                let _ = checkpoint_ordinals.insert(key, next_ordinal);
                new_ordinals[index] = Some(next_ordinal);
                next_ordinal = next_ordinal
                    .checked_add(1)
                    .ok_or(V2ApplyError::Overflow("v2 retained checkpoint ordinal exceeds u64"))?;
            }
        }

        let mut retire_count = 0usize;
        for record in self.request_records.values() {
            let old_index = usize::try_from(record.checkpoint_ordinal)
                .map_err(|_| V2ApplyError::Overflow("v2 request checkpoint ordinal exceeds usize"))?;
            if delete_mask
                .get(old_index)
                .copied()
                .ok_or(V2ApplyError::Invalid(
                    "v2 active request checkpoint ordinal is outside the deletion mask",
                ))?
                != 0
            {
                retire_count = retire_count
                    .checked_add(1)
                    .ok_or(V2ApplyError::Overflow("v2 retired request count exceeds usize"))?;
            }
        }
        let retained_request_count = self
            .request_records
            .len()
            .checked_sub(retire_count)
            .ok_or(V2ApplyError::Invalid(
                "v2 retired request count exceeds active request ledger",
            ))?;

        let mut request_records = HashMap::new();
        request_records
            .try_reserve(retained_request_count)
            .map_err(|_| V2ApplyError::Capacity("v2 retained request ledger allocation failed"))?;

        let final_retired_count = self
            .retired_requests
            .len()
            .checked_add(retire_count)
            .ok_or(V2ApplyError::Overflow(
                "v2 retired request ledger length exceeds usize",
            ))?;
        let mut retired_requests = HashMap::new();
        retired_requests
            .try_reserve(final_retired_count)
            .map_err(|_| V2ApplyError::Capacity("v2 retired request ledger allocation failed"))?;
        for (request_id, operation_digest) in &self.retired_requests {
            let key = try_clone_bytes(request_id, "v2 retired request key allocation failed")?;
            let _ = retired_requests.insert(key, *operation_digest);
        }

        for (request_id, record) in &self.request_records {
            let old_index = usize::try_from(record.checkpoint_ordinal)
                .map_err(|_| V2ApplyError::Overflow("v2 request checkpoint ordinal exceeds usize"))?;
            let deleted = delete_mask
                .get(old_index)
                .copied()
                .ok_or(V2ApplyError::Invalid(
                    "v2 active request checkpoint ordinal is outside the deletion mask",
                ))?
                != 0;
            let key = try_clone_bytes(request_id, "v2 request key allocation failed")?;
            if deleted {
                let _ = retired_requests.insert(key, record.operation_digest);
            } else {
                let new_ordinal = new_ordinals
                    .get(old_index)
                    .copied()
                    .flatten()
                    .ok_or(V2ApplyError::Invalid(
                        "v2 retained request checkpoint is absent from the prepared ordinal map",
                    ))?;
                let _ = request_records.insert(
                    key,
                    V2RequestRecord {
                        operation_digest: record.operation_digest,
                        checkpoint_ordinal: new_ordinal,
                    },
                );
            }
        }

        Ok(V2PreparedSubtreeDelete {
            checkpoints,
            checkpoint_ordinals,
            request_records,
            retired_requests,
            deleted_checkpoints,
            clear_sequence_geometry: retained_count == 0,
            deleted_checkpoint_count,
        })
    }
}

pub(super) fn apply_v2_commit(
    state: &mut V2CommittedState,
    bytes: &[u8],
) -> Result<V2ApplyOutcome, V2ApplyError> {
    let decoded = decode_v2_commit(bytes)?;

    if let Some(request_id) = decoded.request_id.as_deref() {
        match state.classify_request(request_id, decoded.operation_digest)? {
            V2RequestStatus::Replay { checkpoint_ordinal } => {
                return Ok(V2ApplyOutcome::Replayed { checkpoint_ordinal });
            }
            V2RequestStatus::Retired => return Ok(V2ApplyOutcome::RetiredRequest),
            V2RequestStatus::New => {}
        }
    }

    let current = state.geometry()?;
    if decoded.encoded_base != current {
        return Err(V2ApplyError::Invalid(
            "v2 commit starting geometry disagrees with committed state",
        ));
    }
    validate_against_committed_state(state, &decoded)?;

    let checkpoint_ordinal = current.checkpoint_count;
    let request_id = decoded.request_id;
    let operation_digest = decoded.operation_digest;
    let transaction = decoded.wal.transaction;
    let checkpoint_key = (
        transaction.checkpoint.thread_id.clone(),
        transaction.checkpoint.checkpoint_id.clone(),
    );

    state.payload.extend_from_slice(&transaction.payload);
    state.nodes.extend(transaction.nodes);
    state.versions.extend(transaction.versions);
    state.checkpoints.push(transaction.checkpoint);
    state
        .checkpoint_ordinals
        .insert(checkpoint_key, checkpoint_ordinal);
    if let Some(request_id) = request_id {
        state.request_records.insert(
            request_id,
            V2RequestRecord {
                operation_digest,
                checkpoint_ordinal,
            },
        );
    }
    Ok(V2ApplyOutcome::Applied { checkpoint_ordinal })
}

fn validate_against_committed_state(
    state: &V2CommittedState,
    decoded: &V2DecodedCommit,
) -> Result<(), V2ApplyError> {
    let base = decoded.encoded_base;
    let transaction = &decoded.wal.transaction;
    validate_nodes(state, base, transaction)?;
    validate_versions(state, base, transaction)?;
    validate_checkpoint(state, base, transaction)?;
    Ok(())
}

fn validate_nodes(
    state: &V2CommittedState,
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
) -> Result<(), V2ApplyError> {
    for (local, node) in transaction.nodes.iter().copied().enumerate() {
        if node.height() == 1 {
            continue;
        }
        let node_id = base
            .node_count
            .checked_add(
                u64::try_from(local)
                    .map_err(|_| V2ApplyError::Overflow("v2 apply node index exceeds u64"))?,
            )
            .ok_or(V2ApplyError::Overflow(
                "v2 apply node identifier exceeds u64",
            ))?;
        let encoded = encode_v2_node(node);
        let left_id = read_u64(&encoded, 16)?;
        let right_id = read_u64(&encoded, 24)?;
        if left_id >= node_id || right_id >= node_id {
            return Err(V2ApplyError::Invalid(
                "v2 apply branch child is not topologically prior",
            ));
        }
        let left = resolve_node(state, base, transaction, left_id)?;
        let right = resolve_node(state, base, transaction, right_id)?;
        let expected = V2NodeRecord::branch(
            V2RootRecord::from_node(left_id, left)?,
            V2RootRecord::from_node(right_id, right)?,
        )?;
        if expected != node {
            return Err(V2ApplyError::Invalid(
                "v2 apply branch metadata disagrees with committed children",
            ));
        }
    }
    Ok(())
}

fn validate_versions(
    state: &V2CommittedState,
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
) -> Result<(), V2ApplyError> {
    for version in &transaction.versions {
        if let Some(parent) = version.parent_version() {
            resolve_version(state, base, transaction, parent)?;
        }
        let root = version.root();
        let node = resolve_node(state, base, transaction, root.node_id())?;
        if V2RootRecord::from_node(root.node_id(), node)? != root {
            return Err(V2ApplyError::Invalid(
                "v2 apply version root disagrees with committed node metadata",
            ));
        }
    }
    Ok(())
}

fn validate_checkpoint(
    state: &V2CommittedState,
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
) -> Result<(), V2ApplyError> {
    let checkpoint = &transaction.checkpoint;
    let key = (
        checkpoint.thread_id.clone(),
        checkpoint.checkpoint_id.clone(),
    );
    if state.checkpoint_ordinals.contains_key(&key) {
        return Err(V2ApplyError::Invalid(
            "v2 checkpoint identity is already committed",
        ));
    }
    if state.deleted_checkpoints.contains(&key) {
        return Err(V2ApplyError::Invalid(
            "v2 checkpoint identity was logically deleted",
        ));
    }
    if let Some(parent) = checkpoint.parent_checkpoint_id.as_ref() {
        if !state
            .checkpoint_ordinals
            .contains_key(&(checkpoint.thread_id.clone(), parent.clone()))
        {
            return Err(V2ApplyError::Invalid(
                "v2 checkpoint parent is not a committed checkpoint in the same thread",
            ));
        }
    }

    let identity = resolve_version(state, base, transaction, checkpoint.identity_version)?.root();
    let messages = match checkpoint.messages_version {
        Some(version) => Some(resolve_version(state, base, transaction, version)?.root()),
        None => None,
    };
    let result = match checkpoint.result_version {
        Some(version) => Some(resolve_version(state, base, transaction, version)?.root()),
        None => None,
    };
    let expected_state = checkpoint_state_metadata(identity, messages, result)?;
    if expected_state != checkpoint.state {
        return Err(V2ApplyError::Invalid(
            "v2 checkpoint state commitment disagrees with committed version roots",
        ));
    }
    Ok(())
}

fn resolve_node(
    state: &V2CommittedState,
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
    node_id: u64,
) -> Result<V2NodeRecord, V2ApplyError> {
    if node_id < base.node_count {
        return state
            .nodes
            .get(usize::try_from(node_id).map_err(|_| {
                V2ApplyError::Overflow("v2 committed node identifier exceeds usize")
            })?)
            .copied()
            .ok_or(V2ApplyError::Invalid("v2 committed node is absent"));
    }
    let local = node_id
        .checked_sub(base.node_count)
        .ok_or(V2ApplyError::Invalid("v2 local node identifier underflow"))?;
    transaction
        .nodes
        .get(
            usize::try_from(local)
                .map_err(|_| V2ApplyError::Overflow("v2 local node identifier exceeds usize"))?,
        )
        .copied()
        .ok_or(V2ApplyError::Invalid("v2 referenced new node is absent"))
}

fn resolve_version(
    state: &V2CommittedState,
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
    version_id: u32,
) -> Result<V2VersionRecord, V2ApplyError> {
    let version_id_u64 = u64::from(version_id);
    if version_id_u64 < base.version_count {
        return state
            .versions
            .get(usize::try_from(version_id_u64).map_err(|_| {
                V2ApplyError::Overflow("v2 committed version identifier exceeds usize")
            })?)
            .copied()
            .ok_or(V2ApplyError::Invalid("v2 committed version is absent"));
    }
    let local = version_id_u64
        .checked_sub(base.version_count)
        .ok_or(V2ApplyError::Invalid(
            "v2 local version identifier underflow",
        ))?;
    transaction
        .versions
        .get(
            usize::try_from(local)
                .map_err(|_| V2ApplyError::Overflow("v2 local version identifier exceeds usize"))?,
        )
        .copied()
        .ok_or(V2ApplyError::Invalid("v2 referenced new version is absent"))
}

fn validate_checkpoint_identifier(identifier: &str) -> Result<(), V2ApplyError> {
    if identifier.is_empty() || identifier.len() > V2_MAX_IDENTIFIER_BYTES {
        return Err(V2ApplyError::Invalid(
            "v2 checkpoint identifier is empty or exceeds the byte limit",
        ));
    }
    Ok(())
}

fn try_clone_bytes(bytes: &[u8], message: &'static str) -> Result<Vec<u8>, V2ApplyError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(bytes.len())
        .map_err(|_| V2ApplyError::Capacity(message))?;
    cloned.extend_from_slice(bytes);
    Ok(cloned)
}

fn try_clone_string(value: &str) -> Result<String, V2ApplyError> {
    let mut cloned = String::new();
    cloned
        .try_reserve_exact(value.len())
        .map_err(|_| V2ApplyError::Capacity("v2 checkpoint identity allocation failed"))?;
    cloned.push_str(value);
    Ok(cloned)
}

fn try_clone_checkpoint_key(
    thread_id: &str,
    checkpoint_id: &str,
) -> Result<(String, String), V2ApplyError> {
    Ok((
        try_clone_string(thread_id)?,
        try_clone_string(checkpoint_id)?,
    ))
}

fn try_clone_checkpoint_record(
    checkpoint: &V2CheckpointRecord,
) -> Result<V2CheckpointRecord, V2ApplyError> {
    Ok(V2CheckpointRecord {
        checkpoint_no: checkpoint.checkpoint_no,
        thread_id: try_clone_string(&checkpoint.thread_id)?,
        checkpoint_id: try_clone_string(&checkpoint.checkpoint_id)?,
        parent_checkpoint_id: checkpoint
            .parent_checkpoint_id
            .as_deref()
            .map(try_clone_string)
            .transpose()?,
        identity_version: checkpoint.identity_version,
        messages_version: checkpoint.messages_version,
        result_version: checkpoint.result_version,
        state: checkpoint.state,
    })
}

fn validate_request_id(request_id: &[u8]) -> Result<(), V2ApplyError> {
    if request_id.is_empty() || request_id.len() > V2_MAX_REQUEST_ID_BYTES {
        return Err(V2ApplyError::Invalid(
            "v2 request id is empty or exceeds the byte limit",
        ));
    }
    Ok(())
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, V2ApplyError> {
    let end = offset
        .checked_add(8)
        .ok_or(V2ApplyError::Overflow("v2 apply u64 range exceeds usize"))?;
    let encoded: [u8; 8] = bytes
        .get(offset..end)
        .ok_or(V2ApplyError::Invalid("v2 apply node field is truncated"))?
        .try_into()
        .map_err(|_| V2ApplyError::Invalid("v2 apply node field width mismatch"))?;
    Ok(u64::from_le_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::super::commit_v2::{checkpoint_operation_digest, encode_v2_commit};
    use super::super::format_v2::{decode_v2_node, decode_v2_root, encode_v2_root};
    use super::*;

    fn initial_transaction(checkpoint_id: &str) -> V2WalTransaction {
        let node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let root = V2RootRecord::from_node(0, node).unwrap();
        V2WalTransaction {
            payload: b"abc".to_vec(),
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

    fn child_transaction(base: V2WalGeometry, corrupt_branch: bool) -> V2WalTransaction {
        let old_node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let old_root = V2RootRecord::from_node(0, old_node).unwrap();
        let leaf = V2NodeRecord::leaf(base.payload_len, b"XYZ").unwrap();
        let leaf_root = V2RootRecord::from_node(base.node_count, leaf).unwrap();
        let branch = V2NodeRecord::branch(old_root, leaf_root).unwrap();
        let branch = if corrupt_branch {
            let mut encoded = encode_v2_node(branch);
            encoded[40] ^= 1;
            decode_v2_node(&encoded).unwrap()
        } else {
            branch
        };
        let branch_id = base.node_count + 1;
        let branch_root = V2RootRecord::from_node(branch_id, branch).unwrap();
        let version_id = u32::try_from(base.version_count).unwrap();
        V2WalTransaction {
            payload: b"XYZ".to_vec(),
            nodes: vec![leaf, branch],
            versions: vec![V2VersionRecord::new(version_id, Some(0), branch_root).unwrap()],
            checkpoint: V2CheckpointRecord {
                checkpoint_no: 2,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-2".to_owned(),
                parent_checkpoint_id: Some("cp-1".to_owned()),
                identity_version: version_id,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(branch_root, None, None).unwrap(),
            },
        }
    }
    fn checkpoint_only_transaction(
        checkpoint_no: u32,
        thread_id: &str,
        checkpoint_id: &str,
        parent_checkpoint_id: Option<&str>,
        identity_version: u32,
        state: V2StateMetadata,
    ) -> V2WalTransaction {
        V2WalTransaction {
            payload: Vec::new(),
            nodes: Vec::new(),
            versions: Vec::new(),
            checkpoint: V2CheckpointRecord {
                checkpoint_no,
                thread_id: thread_id.to_owned(),
                checkpoint_id: checkpoint_id.to_owned(),
                parent_checkpoint_id: parent_checkpoint_id.map(str::to_owned),
                identity_version,
                messages_version: None,
                result_version: None,
                state,
            },
        }
    }


    #[test]
    fn retry_same_request_is_noop_and_conflicting_reuse_fails() {
        let mut state = V2CommittedState::default();
        let transaction = initial_transaction("cp-1");
        let encoded =
            encode_v2_commit(V2WalGeometry::default(), &transaction, Some(b"req-1")).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Ok(V2ApplyOutcome::Applied {
                checkpoint_ordinal: 0
            })
        );
        let geometry = state.geometry().unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Ok(V2ApplyOutcome::Replayed {
                checkpoint_ordinal: 0
            })
        );
        assert_eq!(state.geometry().unwrap(), geometry);

        let base = geometry;
        let child = child_transaction(base, false);
        let conflict = encode_v2_commit(base, &child, Some(b"req-1")).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &conflict),
            Err(V2ApplyError::RequestConflict)
        );
    }

    #[test]
    fn retire_request_is_fail_atomic_when_ledgers_overlap() {
        let mut state = V2CommittedState::default();
        let active = V2RequestRecord {
            operation_digest: [0x11; 32],
            checkpoint_ordinal: 7,
        };
        state
            .request_records
            .insert(b"req-1".to_vec(), active.clone());
        state.retired_requests.insert(b"req-1".to_vec(), [0x22; 32]);

        assert_eq!(
            state.retire_request(b"req-1"),
            Err(V2ApplyError::Invalid(
                "v2 request identity is already retired"
            ))
        );
        assert_eq!(
            state.request_records.get(b"req-1".as_slice()),
            Some(&active)
        );
        assert_eq!(
            state.retired_requests.get(b"req-1".as_slice()),
            Some(&[0x22; 32])
        );
    }

    #[test]
    fn retired_request_cannot_resurrect_checkpoint() {
        let mut state = V2CommittedState::default();
        let transaction = initial_transaction("cp-1");
        let digest = checkpoint_operation_digest(&transaction.checkpoint).unwrap();
        let encoded =
            encode_v2_commit(V2WalGeometry::default(), &transaction, Some(b"req-1")).unwrap();
        apply_v2_commit(&mut state, &encoded).unwrap();
        state.retire_request(b"req-1").unwrap();
        assert!(!state.request_records.contains_key(b"req-1".as_slice()));
        assert_eq!(
            state.retired_requests.get(b"req-1".as_slice()),
            Some(&digest)
        );
        assert_eq!(
            state.classify_request(b"req-1", digest),
            Ok(V2RequestStatus::Retired)
        );
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Ok(V2ApplyOutcome::RetiredRequest)
        );
        assert_eq!(state.geometry().unwrap().checkpoint_count, 1);
    }

    #[test]
    fn committed_old_child_is_required_for_branch_validation() {
        let mut state = V2CommittedState::default();
        let initial = initial_transaction("cp-1");
        let first = encode_v2_commit(V2WalGeometry::default(), &initial, Some(b"req-1")).unwrap();
        apply_v2_commit(&mut state, &first).unwrap();
        let base = state.geometry().unwrap();

        let corrupt = child_transaction(base, true);
        let encoded = encode_v2_commit(base, &corrupt, Some(b"req-2")).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Err(V2ApplyError::Invalid(
                "v2 apply branch metadata disagrees with committed children"
            ))
        );
        assert_eq!(state.geometry().unwrap(), base);

        let valid = child_transaction(base, false);
        let encoded = encode_v2_commit(base, &valid, Some(b"req-2")).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Ok(V2ApplyOutcome::Applied {
                checkpoint_ordinal: 1
            })
        );
    }

    #[test]
    fn old_version_root_and_checkpoint_state_are_revalidated() {
        let mut state = V2CommittedState::default();
        let initial = initial_transaction("cp-1");
        let first = encode_v2_commit(V2WalGeometry::default(), &initial, Some(b"req-1")).unwrap();
        apply_v2_commit(&mut state, &first).unwrap();
        let base = state.geometry().unwrap();

        let old_node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let old_root = V2RootRecord::from_node(0, old_node).unwrap();
        let mut forged_root_bytes = encode_v2_root(old_root);
        forged_root_bytes[24] ^= 1;
        let forged_root = decode_v2_root(&forged_root_bytes).unwrap();
        let forged_version = V2VersionRecord::new(1, Some(0), forged_root).unwrap();
        let forged_version_tx = V2WalTransaction {
            payload: Vec::new(),
            nodes: Vec::new(),
            versions: vec![forged_version],
            checkpoint: V2CheckpointRecord {
                checkpoint_no: 2,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-2".to_owned(),
                parent_checkpoint_id: Some("cp-1".to_owned()),
                identity_version: 1,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(forged_root, None, None).unwrap(),
            },
        };
        let encoded = encode_v2_commit(base, &forged_version_tx, Some(b"req-2")).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Err(V2ApplyError::Invalid(
                "v2 apply version root disagrees with committed node metadata"
            ))
        );

        let other = V2NodeRecord::leaf(0, b"xyz").unwrap();
        let other_root = V2RootRecord::from_node(0, other).unwrap();
        let bad_state_tx = V2WalTransaction {
            payload: Vec::new(),
            nodes: Vec::new(),
            versions: Vec::new(),
            checkpoint: V2CheckpointRecord {
                checkpoint_no: 2,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-2".to_owned(),
                parent_checkpoint_id: Some("cp-1".to_owned()),
                identity_version: 0,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(other_root, None, None).unwrap(),
            },
        };
        let encoded = encode_v2_commit(base, &bad_state_tx, Some(b"req-3")).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Err(V2ApplyError::Invalid(
                "v2 checkpoint state commitment disagrees with committed version roots"
            ))
        );
        assert_eq!(state.geometry().unwrap(), base);
    }

    #[test]
    fn prepared_subtree_delete_is_atomic_and_remaps_request_ordinals() {
        let mut state = V2CommittedState::default();

        let first = initial_transaction("cp-1");
        let first_encoded =
            encode_v2_commit(V2WalGeometry::default(), &first, Some(b"req-1")).unwrap();
        apply_v2_commit(&mut state, &first_encoded).unwrap();

        let second_base = state.geometry().unwrap();
        let second = child_transaction(second_base, false);
        let second_digest = checkpoint_operation_digest(&second.checkpoint).unwrap();
        let second_encoded =
            encode_v2_commit(second_base, &second, Some(b"req-2")).unwrap();
        apply_v2_commit(&mut state, &second_encoded).unwrap();

        let third_base = state.geometry().unwrap();
        let third = checkpoint_only_transaction(
            3,
            "thread",
            "cp-3",
            Some("cp-2"),
            state.checkpoints[1].identity_version,
            state.checkpoints[1].state,
        );
        let third_digest = checkpoint_operation_digest(&third.checkpoint).unwrap();
        let third_encoded = encode_v2_commit(third_base, &third, Some(b"req-3")).unwrap();
        apply_v2_commit(&mut state, &third_encoded).unwrap();

        let other_base = state.geometry().unwrap();
        let other = checkpoint_only_transaction(
            4,
            "other-thread",
            "other-root",
            None,
            state.checkpoints[0].identity_version,
            state.checkpoints[0].state,
        );
        let other_digest = checkpoint_operation_digest(&other.checkpoint).unwrap();
        let other_encoded = encode_v2_commit(other_base, &other, Some(b"req-4")).unwrap();
        apply_v2_commit(&mut state, &other_encoded).unwrap();

        let before = state.geometry().unwrap();
        let prepared = state
            .prepare_delete_checkpoint_subtree("thread", "cp-2")
            .unwrap();
        assert_eq!(prepared.deleted_checkpoint_count(), 2);

        // Preparation owns replacement ledgers and cannot mutate committed state.
        assert_eq!(state.geometry().unwrap(), before);
        assert_eq!(state.checkpoints.len(), 4);
        assert_eq!(
            state.classify_request(b"req-2", second_digest),
            Ok(V2RequestStatus::Replay {
                checkpoint_ordinal: 1
            })
        );
        assert_eq!(
            state.classify_request(b"req-4", other_digest),
            Ok(V2RequestStatus::Replay {
                checkpoint_ordinal: 3
            })
        );
        assert!(state.deleted_checkpoints.is_empty());
        assert!(state.retired_requests.is_empty());

        state.apply_prepared_delete_checkpoint_subtree(prepared);

        assert_eq!(state.checkpoints.len(), 2);
        assert_eq!(state.checkpoints[0].checkpoint_id, "cp-1");
        assert_eq!(state.checkpoints[1].checkpoint_id, "other-root");
        assert_eq!(
            state
                .checkpoint_ordinals
                .get(&("thread".to_owned(), "cp-1".to_owned())),
            Some(&0)
        );
        assert_eq!(
            state
                .checkpoint_ordinals
                .get(&("other-thread".to_owned(), "other-root".to_owned())),
            Some(&1)
        );
        assert_eq!(
            state.classify_request(b"req-2", second_digest),
            Ok(V2RequestStatus::Retired)
        );
        assert_eq!(
            state.classify_request(b"req-3", third_digest),
            Ok(V2RequestStatus::Retired)
        );
        assert_eq!(
            state.classify_request(b"req-4", other_digest),
            Ok(V2RequestStatus::Replay {
                checkpoint_ordinal: 1
            })
        );
        assert!(state
            .deleted_checkpoints
            .contains(&("thread".to_owned(), "cp-2".to_owned())));
        assert!(state
            .deleted_checkpoints
            .contains(&("thread".to_owned(), "cp-3".to_owned())));

        let after = state.geometry().unwrap();
        assert_eq!(after.payload_len, before.payload_len);
        assert_eq!(after.node_count, before.node_count);
        assert_eq!(after.version_count, before.version_count);
        assert_eq!(after.checkpoint_count, 2);

        assert_eq!(
            apply_v2_commit(&mut state, &second_encoded),
            Ok(V2ApplyOutcome::RetiredRequest)
        );
    }

    #[test]
    fn delete_prepare_failure_leaves_all_semantic_ledgers_unchanged() {
        let mut state = V2CommittedState::default();
        let first = initial_transaction("cp-1");
        let first_encoded =
            encode_v2_commit(V2WalGeometry::default(), &first, Some(b"req-1")).unwrap();
        apply_v2_commit(&mut state, &first_encoded).unwrap();

        let base = state.geometry().unwrap();
        let second = child_transaction(base, false);
        let second_encoded = encode_v2_commit(base, &second, Some(b"req-2")).unwrap();
        apply_v2_commit(&mut state, &second_encoded).unwrap();

        let active = state.request_records.get(b"req-2".as_slice()).unwrap().clone();
        state.retired_requests.insert(b"req-2".to_vec(), [0x55; 32]);
        let before = state.geometry().unwrap();

        assert_eq!(
            state.prepare_delete_checkpoint_subtree("thread", "cp-2"),
            Err(V2ApplyError::Invalid(
                "v2 request identity is both active and retired"
            ))
        );
        assert_eq!(state.geometry().unwrap(), before);
        assert_eq!(
            state.request_records.get(b"req-2".as_slice()),
            Some(&active)
        );
        assert_eq!(
            state.retired_requests.get(b"req-2".as_slice()),
            Some(&[0x55; 32])
        );
        assert!(state
            .checkpoint_ordinals
            .contains_key(&("thread".to_owned(), "cp-2".to_owned())));
        assert!(state.deleted_checkpoints.is_empty());
    }

    #[test]
    fn checkpoint_parent_must_exist_in_same_thread() {
        let mut state = V2CommittedState::default();
        let initial = initial_transaction("cp-1");
        let first = encode_v2_commit(V2WalGeometry::default(), &initial, None).unwrap();
        apply_v2_commit(&mut state, &first).unwrap();
        let base = state.geometry().unwrap();
        let old_root = state.versions[0].root();
        let transaction = V2WalTransaction {
            payload: Vec::new(),
            nodes: Vec::new(),
            versions: Vec::new(),
            checkpoint: V2CheckpointRecord {
                checkpoint_no: 2,
                thread_id: "other-thread".to_owned(),
                checkpoint_id: "cp-2".to_owned(),
                parent_checkpoint_id: Some("cp-1".to_owned()),
                identity_version: 0,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(old_root, None, None).unwrap(),
            },
        };
        let encoded = encode_v2_commit(base, &transaction, None).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Err(V2ApplyError::Invalid(
                "v2 checkpoint parent is not a committed checkpoint in the same thread"
            ))
        );
    }

    #[test]
    fn deleted_checkpoint_identity_cannot_be_reused() {
        let mut state = V2CommittedState::default();
        let initial = initial_transaction("cp-1");
        let first = encode_v2_commit(V2WalGeometry::default(), &initial, None).unwrap();
        apply_v2_commit(&mut state, &first).unwrap();
        state
            .deleted_checkpoints
            .insert(("thread".to_owned(), "cp-2".to_owned()));
        let base = state.geometry().unwrap();
        let child = child_transaction(base, false);
        let encoded = encode_v2_commit(base, &child, None).unwrap();
        assert_eq!(
            apply_v2_commit(&mut state, &encoded),
            Err(V2ApplyError::Invalid(
                "v2 checkpoint identity was logically deleted"
            ))
        );
        assert_eq!(state.geometry().unwrap(), base);
    }
}
