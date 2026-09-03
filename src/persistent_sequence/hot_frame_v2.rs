//! Physical hot-WAL framing for complete staged Format-v2 commits.
//!
//! `T2C2` remains the logical durable-commit envelope. The hot file appends a
//! fixed completion footer after each commit so recovery can distinguish a
//! zero-padded partial write from a complete record in a preinitialized WAL.

use super::commit_v2::{decode_v2_commit, V2CommitError};
use std::fmt;

const V2_COMMIT_MAGIC: [u8; 4] = *b"T2C2";
const V2_COMMIT_HEADER_SIZE: usize = 88;
const V2_COMMIT_DIGEST_OFFSET: usize = 56;
const V2_COMMIT_DIGEST_END: usize = 88;
const V2_HOT_FOOTER_MAGIC: [u8; 4] = *b"T2E2";
const V2_HOT_FOOTER_SIZE: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2HotFrameError {
    Commit(V2CommitError),
    Invalid(&'static str),
    Overflow(&'static str),
}

impl fmt::Display for V2HotFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Commit(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for V2HotFrameError {}

impl From<V2CommitError> for V2HotFrameError {
    fn from(error: V2CommitError) -> Self {
        Self::Commit(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2HotFrameProbe {
    Complete {
        commit_len: usize,
        frame_len: usize,
    },
    Torn,
}

pub(super) fn encode_v2_hot_frame(commit: &[u8]) -> Result<Vec<u8>, V2HotFrameError> {
    let decoded = decode_v2_commit(commit)?;
    let frame_len = commit
        .len()
        .checked_add(V2_HOT_FOOTER_SIZE)
        .ok_or(V2HotFrameError::Overflow("v2 hot frame length exceeds usize"))?;
    let frame_len_u32 = u32::try_from(frame_len)
        .map_err(|_| V2HotFrameError::Overflow("v2 hot frame length exceeds u32"))?;
    let mut output = Vec::with_capacity(frame_len);
    output.extend_from_slice(commit);
    output.extend_from_slice(&V2_HOT_FOOTER_MAGIC);
    output.extend_from_slice(&frame_len_u32.to_le_bytes());
    output.extend_from_slice(&decoded.commit_digest);
    if output.len() != frame_len {
        return Err(V2HotFrameError::Invalid(
            "v2 hot frame encoder produced an unexpected length",
        ));
    }
    Ok(output)
}

pub(super) fn probe_v2_hot_frame(bytes: &[u8]) -> Result<V2HotFrameProbe, V2HotFrameError> {
    if bytes.len() < V2_COMMIT_MAGIC.len() {
        return if V2_COMMIT_MAGIC.starts_with(bytes) {
            Ok(V2HotFrameProbe::Torn)
        } else {
            Err(V2HotFrameError::Invalid("v2 hot frame magic is truncated"))
        };
    }
    if bytes.get(..4) != Some(V2_COMMIT_MAGIC.as_slice()) {
        return Err(V2HotFrameError::Invalid("v2 hot frame commit magic mismatch"));
    }
    if bytes.len() < 8 {
        return Ok(V2HotFrameProbe::Torn);
    }

    let commit_len = usize::try_from(read_u32(bytes, 4)?)
        .map_err(|_| V2HotFrameError::Overflow("v2 hot commit length exceeds usize"))?;
    if commit_len < V2_COMMIT_HEADER_SIZE {
        let header_tail_end = bytes.len().min(V2_COMMIT_HEADER_SIZE);
        if bytes
            .get(8..header_tail_end)
            .is_some_and(|tail| tail.iter().all(|byte| *byte == 0))
        {
            return Ok(V2HotFrameProbe::Torn);
        }
        return Err(V2HotFrameError::Invalid(
            "v2 hot commit length is shorter than its header",
        ));
    }

    let frame_len = commit_len
        .checked_add(V2_HOT_FOOTER_SIZE)
        .ok_or(V2HotFrameError::Overflow("v2 hot frame length exceeds usize"))?;
    if frame_len > bytes.len() {
        return Ok(V2HotFrameProbe::Torn);
    }
    let frame_len_u32 = u32::try_from(frame_len)
        .map_err(|_| V2HotFrameError::Overflow("v2 hot frame length exceeds u32"))?;
    let commit_digest = bytes
        .get(V2_COMMIT_DIGEST_OFFSET..V2_COMMIT_DIGEST_END)
        .ok_or(V2HotFrameError::Invalid(
            "v2 hot commit digest field is truncated",
        ))?;
    let mut expected_footer = [0u8; V2_HOT_FOOTER_SIZE];
    expected_footer[0..4].copy_from_slice(&V2_HOT_FOOTER_MAGIC);
    expected_footer[4..8].copy_from_slice(&frame_len_u32.to_le_bytes());
    expected_footer[8..40].copy_from_slice(commit_digest);
    let actual_footer = bytes
        .get(commit_len..frame_len)
        .ok_or(V2HotFrameError::Invalid("v2 hot footer range is truncated"))?;
    if actual_footer != expected_footer {
        if is_zero_padded_prefix(actual_footer, &expected_footer) {
            return Ok(V2HotFrameProbe::Torn);
        }
        return Err(V2HotFrameError::Invalid("v2 hot completion footer mismatch"));
    }
    Ok(V2HotFrameProbe::Complete {
        commit_len,
        frame_len,
    })
}

fn is_zero_padded_prefix(actual: &[u8], expected: &[u8]) -> bool {
    if actual.len() != expected.len() {
        return false;
    }
    let written = actual.iter().position(|byte| *byte == 0).unwrap_or(actual.len());
    actual.get(..written) == expected.get(..written)
        && actual
            .get(written..)
            .is_some_and(|suffix| suffix.iter().all(|byte| *byte == 0))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, V2HotFrameError> {
    let end = offset
        .checked_add(4)
        .ok_or(V2HotFrameError::Overflow("v2 hot u32 range exceeds usize"))?;
    let encoded: [u8; 4] = bytes
        .get(offset..end)
        .ok_or(V2HotFrameError::Invalid("v2 hot u32 field is truncated"))?
        .try_into()
        .map_err(|_| V2HotFrameError::Invalid("v2 hot u32 width mismatch"))?;
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

    fn commit() -> Vec<u8> {
        let node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let root = V2RootRecord::from_node(0, node).unwrap();
        let transaction = V2WalTransaction {
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
        };
        encode_v2_commit(V2WalGeometry::default(), &transaction, Some(b"req-1")).unwrap()
    }

    #[test]
    fn complete_footer_binds_frame_length_and_commit_digest() {
        let commit = commit();
        let frame = encode_v2_hot_frame(&commit).unwrap();
        assert_eq!(
            probe_v2_hot_frame(&frame),
            Ok(V2HotFrameProbe::Complete {
                commit_len: commit.len(),
                frame_len: frame.len(),
            })
        );
        assert_eq!(frame.get(commit.len()..commit.len() + 4), Some(b"T2E2".as_slice()));
        assert_eq!(
            frame.get(commit.len() + 8..commit.len() + V2_HOT_FOOTER_SIZE),
            commit.get(V2_COMMIT_DIGEST_OFFSET..V2_COMMIT_DIGEST_END)
        );
    }

    #[test]
    fn zero_padded_partial_commit_and_footer_are_torn() {
        let commit = commit();
        let frame = encode_v2_hot_frame(&commit).unwrap();

        let mut partial_commit = vec![0u8; frame.len() + 128];
        let written = 37usize;
        partial_commit[..written].copy_from_slice(&frame[..written]);
        assert_eq!(
            probe_v2_hot_frame(&partial_commit),
            Ok(V2HotFrameProbe::Torn)
        );

        let mut partial_footer = vec![0u8; frame.len() + 128];
        let written = commit.len() + 11;
        partial_footer[..written].copy_from_slice(&frame[..written]);
        assert_eq!(
            probe_v2_hot_frame(&partial_footer),
            Ok(V2HotFrameProbe::Torn)
        );
    }

    #[test]
    fn complete_noncanonical_footer_fails_closed() {
        let commit = commit();
        let mut frame = encode_v2_hot_frame(&commit).unwrap();
        let footer_last = frame.len() - 1;
        frame[footer_last] ^= 1;
        assert_eq!(
            probe_v2_hot_frame(&frame),
            Err(V2HotFrameError::Invalid("v2 hot completion footer mismatch"))
        );
    }
}
