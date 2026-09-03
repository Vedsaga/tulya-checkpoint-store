//! Sequential recovery for the staged Format-v2 `T2C2` hot WAL.
//!
//! This scanner is filesystem-neutral. A later checkpoint-store integration
//! will feed it the authoritative logical WAL bytes after manifest/version
//! dispatch. It never treats bare `T2W2` structural records as commits.

use super::apply_v2::{apply_v2_commit, V2ApplyError, V2ApplyOutcome, V2CommittedState};
use std::fmt;

const V2_COMMIT_MAGIC: [u8; 4] = *b"T2C2";
const V2_STRUCTURAL_MAGIC: [u8; 4] = *b"T2W2";
const V2_COMMIT_HEADER_SIZE: usize = 88;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2RecoveryError {
    Apply(V2ApplyError),
    Invalid(&'static str),
    Overflow(&'static str),
}

impl fmt::Display for V2RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Apply(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for V2RecoveryError {}

impl From<V2ApplyError> for V2RecoveryError {
    fn from(error: V2ApplyError) -> Self {
        Self::Apply(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2RecoveryStop {
    EndOfBytes,
    ZeroReserve,
    TornFinalCommit,
}

#[derive(Debug)]
pub(super) struct V2RecoveredHotWal {
    pub(super) state: V2CommittedState,
    pub(super) logical_tail: u64,
    pub(super) commit_count: u64,
    pub(super) stop: V2RecoveryStop,
}

pub(super) fn recover_v2_hot_wal(
    bytes: &[u8],
    mut state: V2CommittedState,
) -> Result<V2RecoveredHotWal, V2RecoveryError> {
    let mut cursor = 0usize;
    let mut commit_count = 0u64;
    let mut stop = V2RecoveryStop::EndOfBytes;

    while cursor < bytes.len() {
        let remaining = bytes
            .get(cursor..)
            .ok_or(V2RecoveryError::Invalid("v2 recovery cursor exceeds WAL bytes"))?;

        if starts_with_zero_reserve(remaining) {
            if remaining.iter().any(|byte| *byte != 0) {
                return Err(V2RecoveryError::Invalid(
                    "v2 WAL zero reserve contains nonzero trailing bytes",
                ));
            }
            stop = V2RecoveryStop::ZeroReserve;
            break;
        }

        if remaining.len() < V2_COMMIT_MAGIC.len() {
            if V2_COMMIT_MAGIC.starts_with(remaining) {
                stop = V2RecoveryStop::TornFinalCommit;
                break;
            }
            return Err(V2RecoveryError::Invalid(
                "v2 WAL suffix is neither a commit nor zero reserve",
            ));
        }

        let magic = remaining
            .get(..4)
            .ok_or(V2RecoveryError::Invalid("v2 WAL commit magic is truncated"))?;
        if magic == V2_STRUCTURAL_MAGIC.as_slice() {
            return Err(V2RecoveryError::Invalid(
                "bare T2W2 structural transaction cannot be recovered as a commit",
            ));
        }
        if magic != V2_COMMIT_MAGIC.as_slice() {
            return Err(V2RecoveryError::Invalid("v2 WAL commit magic mismatch"));
        }
        if remaining.len() < 8 {
            stop = V2RecoveryStop::TornFinalCommit;
            break;
        }

        let record_len = usize::try_from(read_u32(remaining, 4)?)
            .map_err(|_| V2RecoveryError::Overflow("v2 commit length exceeds usize"))?;
        if record_len < V2_COMMIT_HEADER_SIZE {
            return Err(V2RecoveryError::Invalid(
                "v2 commit length is shorter than its header",
            ));
        }
        if record_len > remaining.len() {
            stop = V2RecoveryStop::TornFinalCommit;
            break;
        }

        let record = remaining
            .get(..record_len)
            .ok_or(V2RecoveryError::Invalid("v2 commit range exceeds WAL bytes"))?;
        match apply_v2_commit(&mut state, record)? {
            V2ApplyOutcome::Applied { .. } => {}
            V2ApplyOutcome::Replayed { .. } => {
                return Err(V2RecoveryError::Invalid(
                    "v2 WAL contains a duplicate physical retry commit",
                ));
            }
            V2ApplyOutcome::RetiredRequest => {
                return Err(V2RecoveryError::Invalid(
                    "v2 WAL commit resolves to a retired request identity",
                ));
            }
        }
        cursor = cursor
            .checked_add(record_len)
            .ok_or(V2RecoveryError::Overflow("v2 recovery cursor exceeds usize"))?;
        commit_count = commit_count
            .checked_add(1)
            .ok_or(V2RecoveryError::Overflow("v2 recovery commit count exceeds u64"))?;
    }

    let logical_tail = u64::try_from(cursor)
        .map_err(|_| V2RecoveryError::Overflow("v2 logical WAL tail exceeds u64"))?;
    Ok(V2RecoveredHotWal {
        state,
        logical_tail,
        commit_count,
        stop,
    })
}

fn starts_with_zero_reserve(bytes: &[u8]) -> bool {
    let width = bytes.len().min(V2_COMMIT_MAGIC.len());
    width > 0
        && bytes
            .get(..width)
            .is_some_and(|prefix| prefix.iter().all(|byte| *byte == 0))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, V2RecoveryError> {
    let end = offset
        .checked_add(4)
        .ok_or(V2RecoveryError::Overflow("v2 recovery u32 range exceeds usize"))?;
    let encoded: [u8; 4] = bytes
        .get(offset..end)
        .ok_or(V2RecoveryError::Invalid("v2 recovery u32 field is truncated"))?
        .try_into()
        .map_err(|_| V2RecoveryError::Invalid("v2 recovery u32 width mismatch"))?;
    Ok(u32::from_le_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::super::commit_v2::encode_v2_commit;
    use super::super::format_v2::{V2NodeRecord, V2RootRecord};
    use super::super::publication_v2::{
        checkpoint_state_metadata, V2CheckpointRecord, V2VersionRecord,
    };
    use super::super::transaction_v2::{V2WalGeometry, V2WalTransaction};
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

    fn two_commits() -> (Vec<u8>, usize) {
        let first = first_transaction();
        let first_bytes = encode_v2_commit(V2WalGeometry::default(), &first, Some(b"req-1"))
            .unwrap();
        let base = V2WalGeometry {
            payload_len: 3,
            node_count: 1,
            version_count: 1,
            checkpoint_count: 1,
        };
        let second = second_transaction(base);
        let second_bytes = encode_v2_commit(base, &second, Some(b"req-2")).unwrap();
        let logical_tail = first_bytes.len() + second_bytes.len();
        let mut wal = first_bytes;
        wal.extend_from_slice(&second_bytes);
        (wal, logical_tail)
    }

    #[test]
    fn sequential_commits_recover_before_zero_reserve() {
        let (mut wal, logical_tail) = two_commits();
        wal.resize(wal.len() + 1024, 0);
        let recovered = recover_v2_hot_wal(&wal, V2CommittedState::default()).unwrap();
        assert_eq!(recovered.logical_tail, u64::try_from(logical_tail).unwrap());
        assert_eq!(recovered.commit_count, 2);
        assert_eq!(recovered.stop, V2RecoveryStop::ZeroReserve);
        assert_eq!(
            recovered.state.geometry().unwrap(),
            V2WalGeometry {
                payload_len: 6,
                node_count: 3,
                version_count: 2,
                checkpoint_count: 2,
            }
        );
    }

    #[test]
    fn torn_final_commit_is_uncommitted_suffix() {
        let first = first_transaction();
        let first_bytes = encode_v2_commit(V2WalGeometry::default(), &first, Some(b"req-1"))
            .unwrap();
        let base = V2WalGeometry {
            payload_len: 3,
            node_count: 1,
            version_count: 1,
            checkpoint_count: 1,
        };
        let second = second_transaction(base);
        let second_bytes = encode_v2_commit(base, &second, Some(b"req-2")).unwrap();
        let mut wal = first_bytes.clone();
        wal.extend_from_slice(&second_bytes[..second_bytes.len() / 2]);

        let recovered = recover_v2_hot_wal(&wal, V2CommittedState::default()).unwrap();
        assert_eq!(recovered.logical_tail, u64::try_from(first_bytes.len()).unwrap());
        assert_eq!(recovered.commit_count, 1);
        assert_eq!(recovered.stop, V2RecoveryStop::TornFinalCommit);
        assert_eq!(recovered.state.geometry().unwrap().checkpoint_count, 1);
    }

    #[test]
    fn complete_corruption_and_duplicate_physical_retry_fail_closed() {
        let first = first_transaction();
        let first_bytes = encode_v2_commit(V2WalGeometry::default(), &first, Some(b"req-1"))
            .unwrap();
        let base = V2WalGeometry {
            payload_len: 3,
            node_count: 1,
            version_count: 1,
            checkpoint_count: 1,
        };
        let second = second_transaction(base);
        let mut corrupt_second = encode_v2_commit(base, &second, Some(b"req-2")).unwrap();
        *corrupt_second.last_mut().unwrap() ^= 1;
        let mut corrupt_wal = first_bytes.clone();
        corrupt_wal.extend_from_slice(&corrupt_second);
        assert!(recover_v2_hot_wal(&corrupt_wal, V2CommittedState::default()).is_err());

        let mut duplicate = first_bytes.clone();
        duplicate.extend_from_slice(&first_bytes);
        let error = recover_v2_hot_wal(&duplicate, V2CommittedState::default()).unwrap_err();
        assert!(error
            .to_string()
            .contains("duplicate physical retry commit"));
    }

    #[test]
    fn reserve_garbage_and_bare_structural_record_are_rejected() {
        let (mut wal, _) = two_commits();
        wal.extend_from_slice(&[0, 0, 0, 0, 9]);
        let error = recover_v2_hot_wal(&wal, V2CommittedState::default()).unwrap_err();
        assert!(error
            .to_string()
            .contains("zero reserve contains nonzero trailing bytes"));

        let error = recover_v2_hot_wal(b"T2W2payload", V2CommittedState::default()).unwrap_err();
        assert!(error
            .to_string()
            .contains("bare T2W2 structural transaction"));
    }
}
