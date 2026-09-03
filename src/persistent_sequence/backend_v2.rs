//! Semantic backend state for staged Format v2.
//!
//! This module is the representation boundary between one immutable `T2S2`
//! sealed base and the append-local `T2C2 + T2E2` hot suffix. It deliberately
//! contains no filesystem, manifest, or public `CheckpointStore` policy.

use super::apply_v2::{
    V2ApplyError, V2CommittedState, V2RequestRecord, V2RequestStatus,
};
use super::image_v2::{decode_v2_image, encode_v2_image, V2ImageError, V2SequenceImage};
use super::recovery_v2::{
    recover_v2_hot_wal, V2RecoveredHotWal, V2RecoveryError, V2RecoveryStop,
};
use super::snapshot_v2::{
    decode_v2_sealed_snapshot, encode_v2_sealed_snapshot, V2ActiveRequestRecord,
    V2RetiredRequestRecord, V2SealedSnapshot, V2SnapshotError,
};
use super::transaction_v2::V2WalGeometry;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2BackendError {
    Apply(V2ApplyError),
    Image(V2ImageError),
    Recovery(V2RecoveryError),
    Snapshot(V2SnapshotError),
    Invalid(&'static str),
    Overflow(&'static str),
}

impl fmt::Display for V2BackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Apply(error) => write!(formatter, "{error}"),
            Self::Image(error) => write!(formatter, "{error}"),
            Self::Recovery(error) => write!(formatter, "{error}"),
            Self::Snapshot(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for V2BackendError {}

impl From<V2ApplyError> for V2BackendError {
    fn from(error: V2ApplyError) -> Self {
        Self::Apply(error)
    }
}

impl From<V2ImageError> for V2BackendError {
    fn from(error: V2ImageError) -> Self {
        Self::Image(error)
    }
}

impl From<V2RecoveryError> for V2BackendError {
    fn from(error: V2RecoveryError) -> Self {
        Self::Recovery(error)
    }
}

impl From<V2SnapshotError> for V2BackendError {
    fn from(error: V2SnapshotError) -> Self {
        Self::Snapshot(error)
    }
}

#[derive(Debug)]
pub(super) struct V2BackendRecovery {
    pub(super) state: V2CommittedState,
    pub(super) base_geometry: V2WalGeometry,
    pub(super) logical_hot_tail: u64,
    pub(super) hot_commit_count: u64,
    pub(super) stop: V2RecoveryStop,
}

/// Reconstructs one Format-v2 semantic backend from an optional sealed base
/// plus the authoritative hot suffix.
///
/// A missing sealed snapshot means the store has no sealed checkpoint base.
/// The hot scanner then begins from empty geometry. A present snapshot is fully
/// decoded and semantically validated before any hot commit is considered.
pub(super) fn recover_v2_backend(
    sealed_snapshot: Option<&[u8]>,
    hot_bytes: &[u8],
) -> Result<V2BackendRecovery, V2BackendError> {
    let state = match sealed_snapshot {
        Some(bytes) => import_v2_sealed_state(bytes)?,
        None => V2CommittedState::default(),
    };
    let base_geometry = state.geometry()?;
    let V2RecoveredHotWal {
        state,
        logical_tail,
        commit_count,
        stop,
    } = recover_v2_hot_wal(hot_bytes, state)?;
    Ok(V2BackendRecovery {
        state,
        base_geometry,
        logical_hot_tail: logical_tail,
        hot_commit_count: commit_count,
        stop,
    })
}

/// Encodes the complete committed semantic state as one immutable `T2S2`
/// sealed base. Empty state has no snapshot artifact and returns `None`.
pub(super) fn export_v2_sealed_state(
    state: &V2CommittedState,
) -> Result<Option<Vec<u8>>, V2BackendError> {
    let geometry = state.geometry()?;
    if geometry.checkpoint_count == 0 {
        if geometry.payload_len != 0
            || geometry.node_count != 0
            || geometry.version_count != 0
            || !state.checkpoint_ordinals.is_empty()
            || !state.request_records.is_empty()
            || !state.retired_requests.is_empty()
        {
            return Err(V2BackendError::Invalid(
                "empty v2 backend has non-empty semantic state",
            ));
        }
        return Ok(None);
    }
    if geometry.version_count == 0 || geometry.node_count == 0 || geometry.payload_len == 0 {
        return Err(V2BackendError::Invalid(
            "non-empty v2 backend is missing persistent sequence state",
        ));
    }

    let roots = state
        .versions
        .iter()
        .copied()
        .map(|version| version.root())
        .collect::<Vec<_>>();
    let image = encode_v2_image(&V2SequenceImage {
        payload: state.payload.clone(),
        nodes: state.nodes.clone(),
        roots,
    })?;

    let mut active_requests = Vec::with_capacity(state.request_records.len());
    for (request_id, record) in &state.request_records {
        active_requests.push(V2ActiveRequestRecord::new(
            request_id.clone(),
            record.operation_digest,
            record.checkpoint_ordinal,
        )?);
    }
    let mut retired_requests = Vec::with_capacity(state.retired_requests.len());
    for (request_id, operation_digest) in &state.retired_requests {
        retired_requests.push(V2RetiredRequestRecord::new(
            request_id.clone(),
            *operation_digest,
        )?);
    }

    let snapshot = V2SealedSnapshot {
        image,
        versions: state.versions.clone(),
        checkpoints: state.checkpoints.clone(),
        active_requests,
        retired_requests,
    };
    Ok(Some(encode_v2_sealed_snapshot(&snapshot)?))
}

fn import_v2_sealed_state(bytes: &[u8]) -> Result<V2CommittedState, V2BackendError> {
    let snapshot = decode_v2_sealed_snapshot(bytes)?;
    // `decode_v2_sealed_snapshot` already imports the nested image through the
    // AVL semantic verifier. Decode it once more only to materialize the exact
    // payload/node arrays owned by `V2CommittedState`.
    let image = decode_v2_image(&snapshot.image)?;

    let mut checkpoint_ordinals = HashMap::with_capacity(snapshot.checkpoints.len());
    for (index, checkpoint) in snapshot.checkpoints.iter().enumerate() {
        let ordinal = u64::try_from(index)
            .map_err(|_| V2BackendError::Overflow("v2 checkpoint ordinal exceeds u64"))?;
        if checkpoint_ordinals
            .insert(
                (checkpoint.thread_id.clone(), checkpoint.checkpoint_id.clone()),
                ordinal,
            )
            .is_some()
        {
            return Err(V2BackendError::Invalid(
                "validated v2 snapshot contains duplicate checkpoint identity",
            ));
        }
    }

    let mut request_records = HashMap::with_capacity(snapshot.active_requests.len());
    for record in snapshot.active_requests {
        if request_records
            .insert(
                record.request_id().to_vec(),
                V2RequestRecord {
                    operation_digest: record.operation_digest(),
                    checkpoint_ordinal: record.checkpoint_ordinal(),
                },
            )
            .is_some()
        {
            return Err(V2BackendError::Invalid(
                "validated v2 snapshot contains duplicate active request identity",
            ));
        }
    }

    let mut retired_requests = HashMap::with_capacity(snapshot.retired_requests.len());
    for record in snapshot.retired_requests {
        if retired_requests
            .insert(record.request_id().to_vec(), record.operation_digest())
            .is_some()
        {
            return Err(V2BackendError::Invalid(
                "validated v2 snapshot contains duplicate retired request identity",
            ));
        }
    }

    let state = V2CommittedState {
        payload: image.payload,
        nodes: image.nodes,
        versions: snapshot.versions,
        checkpoints: snapshot.checkpoints,
        checkpoint_ordinals,
        request_records,
        retired_requests,
    };
    // Keep geometry conversion as the final fixed-width boundary check even
    // though all section counts were already bounded by their codecs.
    state.geometry()?;
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::super::commit_v2::{checkpoint_operation_digest, encode_v2_commit};
    use super::super::format_v2::{V2NodeRecord, V2RootRecord};
    use super::super::hot_frame_v2::encode_v2_hot_frame;
    use super::super::publication_v2::{
        checkpoint_state_metadata, V2CheckpointRecord, V2VersionRecord,
    };
    use super::super::transaction_v2::V2WalTransaction;
    use super::*;

    fn first_transaction() -> V2WalTransaction {
        let node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let root = V2RootRecord::from_node(0, node).unwrap();
        V2WalTransaction {
            payload: b"abc".to_vec(),
            nodes: vec![node],
            versions: vec![V2VersionRecord::new(0, None, root).unwrap()],
            checkpoint: V2CheckpointRecord {
                checkpoint_no: 1,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-1".to_owned(),
                parent_checkpoint_id: None,
                identity_version: 0,
                messages_version: None,
                result_version: None,
                state: checkpoint_state_metadata(root, None, None).unwrap(),
            },
        }
    }

    fn second_transaction(base: V2WalGeometry) -> V2WalTransaction {
        let old = V2NodeRecord::leaf(0, b"abc").unwrap();
        let old_root = V2RootRecord::from_node(0, old).unwrap();
        let leaf = V2NodeRecord::leaf(base.payload_len, b"XYZ").unwrap();
        let leaf_root = V2RootRecord::from_node(base.node_count, leaf).unwrap();
        let branch = V2NodeRecord::branch(old_root, leaf_root).unwrap();
        let branch_root = V2RootRecord::from_node(base.node_count + 1, branch).unwrap();
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

    fn frame(
        base: V2WalGeometry,
        transaction: &V2WalTransaction,
        request_id: &[u8],
    ) -> Vec<u8> {
        let commit = encode_v2_commit(base, transaction, Some(request_id)).unwrap();
        encode_v2_hot_frame(&commit).unwrap()
    }

    #[test]
    fn sealed_base_plus_hot_suffix_matches_full_hot_replay() {
        let first = first_transaction();
        let first_frame = frame(V2WalGeometry::default(), &first, b"req-1");
        let first_recovered = recover_v2_backend(None, &first_frame).unwrap();
        let base_snapshot = export_v2_sealed_state(&first_recovered.state)
            .unwrap()
            .unwrap();

        let base = first_recovered.state.geometry().unwrap();
        let second = second_transaction(base);
        let second_frame = frame(base, &second, b"req-2");
        let mut suffix_with_reserve = second_frame.clone();
        suffix_with_reserve.resize(suffix_with_reserve.len() + 256, 0);
        let layered = recover_v2_backend(Some(&base_snapshot), &suffix_with_reserve).unwrap();
        assert_eq!(layered.base_geometry, base);
        assert_eq!(layered.hot_commit_count, 1);
        assert_eq!(layered.stop, V2RecoveryStop::ZeroReserve);

        let mut full_hot = first_frame;
        full_hot.extend_from_slice(&second_frame);
        let direct = recover_v2_backend(None, &full_hot).unwrap();
        assert_eq!(
            export_v2_sealed_state(&layered.state).unwrap(),
            export_v2_sealed_state(&direct.state).unwrap()
        );
    }

    #[test]
    fn empty_backend_has_no_snapshot_and_accepts_first_commit() {
        assert_eq!(
            export_v2_sealed_state(&V2CommittedState::default()).unwrap(),
            None
        );
        let first = first_transaction();
        let frame = frame(V2WalGeometry::default(), &first, b"req-1");
        let recovered = recover_v2_backend(None, &frame).unwrap();
        assert_eq!(recovered.base_geometry, V2WalGeometry::default());
        assert_eq!(recovered.hot_commit_count, 1);
        assert_eq!(recovered.state.geometry().unwrap().checkpoint_count, 1);
    }

    #[test]
    fn hot_suffix_must_continue_sealed_snapshot_geometry() {
        let first = first_transaction();
        let first_frame = frame(V2WalGeometry::default(), &first, b"req-1");
        let recovered = recover_v2_backend(None, &first_frame).unwrap();
        let snapshot = export_v2_sealed_state(&recovered.state).unwrap().unwrap();

        let error = recover_v2_backend(Some(&snapshot), &first_frame).unwrap_err();
        assert!(error
            .to_string()
            .contains("starting geometry disagrees with committed state"));
    }

    #[test]
    fn retired_request_ledger_survives_seal_and_reopen() {
        let first = first_transaction();
        let operation_digest = checkpoint_operation_digest(&first.checkpoint).unwrap();
        let first_frame = frame(V2WalGeometry::default(), &first, b"req-1");
        let mut recovered = recover_v2_backend(None, &first_frame).unwrap();
        recovered.state.retire_request(b"req-1").unwrap();
        let snapshot = export_v2_sealed_state(&recovered.state).unwrap().unwrap();

        let reopened = recover_v2_backend(Some(&snapshot), &[]).unwrap();
        assert_eq!(
            reopened
                .state
                .classify_request(b"req-1", operation_digest)
                .unwrap(),
            V2RequestStatus::Retired
        );
        assert_eq!(reopened.logical_hot_tail, 0);
    }

    #[test]
    fn corrupt_sealed_snapshot_is_rejected_before_hot_replay() {
        let first = first_transaction();
        let first_frame = frame(V2WalGeometry::default(), &first, b"req-1");
        let recovered = recover_v2_backend(None, &first_frame).unwrap();
        let mut snapshot = export_v2_sealed_state(&recovered.state).unwrap().unwrap();
        let last = snapshot.len() - 1;
        snapshot[last] ^= 1;

        let error = recover_v2_backend(Some(&snapshot), &first_frame).unwrap_err();
        assert!(matches!(error, V2BackendError::Snapshot(_)));
    }
}
