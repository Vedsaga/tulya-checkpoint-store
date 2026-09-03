//! Canonical staged Format-v2 durable commit envelope.
//!
//! `T2C2` binds one already-canonical `T2W2` transaction to an optional
//! idempotency request identity and a logical-operation digest. The accepted
//! `T2W2` bytes remain unchanged; a future production v2 WAL publishes the
//! complete `T2C2` record as the authoritative unit.

use super::publication_v2::{encode_v2_checkpoint, V2CheckpointRecord, V2PublicationError};
use super::transaction_v2::{
    decode_v2_wal_transaction, encode_v2_wal_transaction, V2DecodedWalTransaction, V2WalError,
    V2WalGeometry, V2WalTransaction,
};
use sha2::{Digest, Sha256};
use std::fmt;

const V2_COMMIT_MAGIC: [u8; 4] = *b"T2C2";
const V2_COMMIT_SCHEMA: u32 = 1;
const V2_COMMIT_PREFIX_SIZE: usize = 56;
const V2_COMMIT_HEADER_SIZE: usize = 88;
const V2_COMMIT_DIGEST_OFFSET: usize = 56;
const V2_COMMIT_DIGEST_SIZE: usize = 32;
const V2_MAX_REQUEST_ID_BYTES: usize = 4096;
const V2_COMMIT_DOMAIN: &[u8] = b"tulya-checkpoint-v2/commit\0";
const V2_OPERATION_DOMAIN: &[u8] = b"tulya-checkpoint-v2/checkpoint-operation\0";
const V2_INNER_WAL_MAGIC: [u8; 4] = *b"T2W2";
const V2_INNER_WAL_SCHEMA: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2CommitError {
    Publication(V2PublicationError),
    Wal(V2WalError),
    Invalid(&'static str),
    Overflow(&'static str),
}

impl fmt::Display for V2CommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Publication(error) => write!(formatter, "{error}"),
            Self::Wal(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for V2CommitError {}

impl From<V2PublicationError> for V2CommitError {
    fn from(error: V2PublicationError) -> Self {
        Self::Publication(error)
    }
}

impl From<V2WalError> for V2CommitError {
    fn from(error: V2WalError) -> Self {
        Self::Wal(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2DecodedCommit {
    pub(super) encoded_base: V2WalGeometry,
    pub(super) wal: V2DecodedWalTransaction,
    pub(super) request_id: Option<Vec<u8>>,
    pub(super) operation_digest: [u8; 32],
    pub(super) commit_digest: [u8; 32],
}

pub(super) fn checkpoint_operation_digest(
    checkpoint: &V2CheckpointRecord,
) -> Result<[u8; 32], V2CommitError> {
    // Reuse the canonical checkpoint encoder as the fail-closed field validator,
    // but do not hash its physical version identifiers. Retry identity is bound
    // to the logical checkpoint operation, not allocator watermarks.
    encode_v2_checkpoint(checkpoint)?;

    let parent = checkpoint.parent_checkpoint_id.as_deref().unwrap_or("");
    let mut hasher = Sha256::new();
    hasher.update(V2_OPERATION_DOMAIN);
    hasher.update(V2_COMMIT_SCHEMA.to_le_bytes());
    hasher.update(checkpoint.checkpoint_no.to_le_bytes());
    update_len_prefixed(&mut hasher, checkpoint.thread_id.as_bytes())?;
    update_len_prefixed(&mut hasher, checkpoint.checkpoint_id.as_bytes())?;
    update_len_prefixed(&mut hasher, parent.as_bytes())?;
    hasher.update(checkpoint.state.logical_len().to_le_bytes());
    hasher.update(checkpoint.state.commitment());
    Ok(finish_digest(hasher))
}

pub(super) fn encode_v2_commit(
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
    request_id: Option<&[u8]>,
) -> Result<Vec<u8>, V2CommitError> {
    validate_request_id(request_id)?;
    let wal = encode_v2_wal_transaction(base, transaction)?;
    let operation_digest = match request_id {
        Some(_) => checkpoint_operation_digest(&transaction.checkpoint)?,
        None => [0u8; 32],
    };
    let request_len = request_id.map_or(0usize, <[u8]>::len);
    let request_len_u32 = u32::try_from(request_len)
        .map_err(|_| V2CommitError::Overflow("v2 commit request id length exceeds u32"))?;
    let wal_len_u32 = u32::try_from(wal.len())
        .map_err(|_| V2CommitError::Overflow("v2 commit inner WAL length exceeds u32"))?;
    let total_len = V2_COMMIT_HEADER_SIZE
        .checked_add(wal.len())
        .and_then(|value| value.checked_add(request_len))
        .ok_or(V2CommitError::Overflow("v2 commit byte length exceeds usize"))?;
    let total_len_u32 = u32::try_from(total_len)
        .map_err(|_| V2CommitError::Overflow("v2 commit byte length exceeds u32"))?;

    let mut output = vec![0u8; V2_COMMIT_HEADER_SIZE];
    output[0..4].copy_from_slice(&V2_COMMIT_MAGIC);
    output[4..8].copy_from_slice(&total_len_u32.to_le_bytes());
    output[8..12].copy_from_slice(&V2_COMMIT_SCHEMA.to_le_bytes());
    output[12..16].copy_from_slice(&0u32.to_le_bytes());
    output[16..20].copy_from_slice(&wal_len_u32.to_le_bytes());
    output[20..24].copy_from_slice(&request_len_u32.to_le_bytes());
    output[24..56].copy_from_slice(&operation_digest);
    output.extend_from_slice(&wal);
    if let Some(request_id) = request_id {
        output.extend_from_slice(request_id);
    }
    if output.len() != total_len {
        return Err(V2CommitError::Invalid(
            "v2 commit encoder produced an unexpected byte length",
        ));
    }
    let digest = commit_digest(
        &output[..V2_COMMIT_PREFIX_SIZE],
        &output[V2_COMMIT_HEADER_SIZE..],
    );
    output[V2_COMMIT_DIGEST_OFFSET..V2_COMMIT_HEADER_SIZE].copy_from_slice(&digest);
    Ok(output)
}

pub(super) fn decode_v2_commit(bytes: &[u8]) -> Result<V2DecodedCommit, V2CommitError> {
    if bytes.len() < V2_COMMIT_HEADER_SIZE {
        return Err(V2CommitError::Invalid("v2 commit header is truncated"));
    }
    if bytes.get(0..4) != Some(V2_COMMIT_MAGIC.as_slice()) {
        return Err(V2CommitError::Invalid("v2 commit magic mismatch"));
    }
    let total_len = usize::try_from(read_u32(bytes, 4, "v2 commit length is truncated")?)
        .map_err(|_| V2CommitError::Overflow("v2 commit length exceeds usize"))?;
    if total_len != bytes.len() {
        return Err(V2CommitError::Invalid("v2 commit byte length mismatch"));
    }
    if read_u32(bytes, 8, "v2 commit schema is truncated")? != V2_COMMIT_SCHEMA {
        return Err(V2CommitError::Invalid("v2 commit schema is unsupported"));
    }
    if read_u32(bytes, 12, "v2 commit flags are truncated")? != 0 {
        return Err(V2CommitError::Invalid("v2 commit flags must be zero"));
    }
    let wal_len = usize::try_from(read_u32(bytes, 16, "v2 commit WAL length is truncated")?)
        .map_err(|_| V2CommitError::Overflow("v2 commit WAL length exceeds usize"))?;
    let request_len = usize::try_from(read_u32(
        bytes,
        20,
        "v2 commit request id length is truncated",
    )?)
    .map_err(|_| V2CommitError::Overflow("v2 commit request id length exceeds usize"))?;
    if request_len > V2_MAX_REQUEST_ID_BYTES {
        return Err(V2CommitError::Invalid(
            "v2 commit request id exceeds the byte limit",
        ));
    }
    let stored_operation_digest: [u8; 32] = bytes
        .get(24..56)
        .ok_or(V2CommitError::Invalid(
            "v2 commit operation digest is truncated",
        ))?
        .try_into()
        .map_err(|_| V2CommitError::Invalid("v2 commit operation digest width mismatch"))?;
    let stored_commit_digest: [u8; V2_COMMIT_DIGEST_SIZE] = bytes
        .get(V2_COMMIT_DIGEST_OFFSET..V2_COMMIT_HEADER_SIZE)
        .ok_or(V2CommitError::Invalid("v2 commit digest is truncated"))?
        .try_into()
        .map_err(|_| V2CommitError::Invalid("v2 commit digest width mismatch"))?;
    let expected_commit_digest = commit_digest(
        &bytes[..V2_COMMIT_PREFIX_SIZE],
        &bytes[V2_COMMIT_HEADER_SIZE..],
    );
    if stored_commit_digest != expected_commit_digest {
        return Err(V2CommitError::Invalid("v2 commit digest mismatch"));
    }

    let wal_end = V2_COMMIT_HEADER_SIZE
        .checked_add(wal_len)
        .ok_or(V2CommitError::Overflow("v2 commit WAL range exceeds usize"))?;
    let request_end = wal_end
        .checked_add(request_len)
        .ok_or(V2CommitError::Overflow(
            "v2 commit request range exceeds usize",
        ))?;
    if request_end != bytes.len() {
        return Err(V2CommitError::Invalid("v2 commit section geometry mismatch"));
    }
    let wal_bytes = bytes
        .get(V2_COMMIT_HEADER_SIZE..wal_end)
        .ok_or(V2CommitError::Invalid("v2 commit WAL bytes are truncated"))?;
    let request_bytes = bytes
        .get(wal_end..request_end)
        .ok_or(V2CommitError::Invalid(
            "v2 commit request bytes are truncated",
        ))?;
    if request_len == 0 && stored_operation_digest != [0u8; 32] {
        return Err(V2CommitError::Invalid(
            "v2 requestless commit operation digest must be zero",
        ));
    }
    if request_len > 0 && request_bytes.is_empty() {
        return Err(V2CommitError::Invalid("v2 commit request id is empty"));
    }

    let encoded_base = decode_inner_wal_base(wal_bytes)?;
    let wal = decode_v2_wal_transaction(wal_bytes, encoded_base)?;
    let operation_digest = if request_len == 0 {
        [0u8; 32]
    } else {
        let expected = checkpoint_operation_digest(&wal.transaction.checkpoint)?;
        if stored_operation_digest != expected {
            return Err(V2CommitError::Invalid(
                "v2 commit operation digest disagrees with checkpoint semantics",
            ));
        }
        expected
    };
    Ok(V2DecodedCommit {
        encoded_base,
        wal,
        request_id: if request_len == 0 {
            None
        } else {
            Some(request_bytes.to_vec())
        },
        operation_digest,
        commit_digest: stored_commit_digest,
    })
}

fn validate_request_id(request_id: Option<&[u8]>) -> Result<(), V2CommitError> {
    if request_id.is_some_and(|request| request.is_empty() || request.len() > V2_MAX_REQUEST_ID_BYTES)
    {
        return Err(V2CommitError::Invalid(
            "v2 commit request id is empty or exceeds the byte limit",
        ));
    }
    Ok(())
}

fn decode_inner_wal_base(bytes: &[u8]) -> Result<V2WalGeometry, V2CommitError> {
    if bytes.len() < 72 {
        return Err(V2CommitError::Invalid(
            "v2 commit inner WAL header is truncated",
        ));
    }
    if bytes.get(0..4) != Some(V2_INNER_WAL_MAGIC.as_slice()) {
        return Err(V2CommitError::Invalid("v2 commit inner WAL magic mismatch"));
    }
    if read_u32(bytes, 8, "v2 commit inner WAL schema is truncated")? != V2_INNER_WAL_SCHEMA {
        return Err(V2CommitError::Invalid(
            "v2 commit inner WAL schema is unsupported",
        ));
    }
    Ok(V2WalGeometry {
        payload_len: read_u64(bytes, 16, "v2 commit payload watermark is truncated")?,
        node_count: read_u64(bytes, 32, "v2 commit node watermark is truncated")?,
        version_count: read_u64(bytes, 48, "v2 commit version watermark is truncated")?,
        checkpoint_count: read_u64(bytes, 64, "v2 commit checkpoint watermark is truncated")?,
    })
}

fn update_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), V2CommitError> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| V2CommitError::Overflow("v2 operation field length exceeds u32"))?;
    hasher.update(len.to_le_bytes());
    hasher.update(bytes);
    Ok(())
}

fn commit_digest(prefix: &[u8], body: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(V2_COMMIT_DOMAIN);
    hasher.update(prefix);
    hasher.update(body);
    finish_digest(hasher)
}

fn finish_digest(hasher: Sha256) -> [u8; 32] {
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn read_u32(bytes: &[u8], offset: usize, message: &'static str) -> Result<u32, V2CommitError> {
    let end = offset
        .checked_add(4)
        .ok_or(V2CommitError::Overflow("v2 commit u32 range exceeds usize"))?;
    let encoded: [u8; 4] = bytes
        .get(offset..end)
        .ok_or(V2CommitError::Invalid(message))?
        .try_into()
        .map_err(|_| V2CommitError::Invalid(message))?;
    Ok(u32::from_le_bytes(encoded))
}

fn read_u64(bytes: &[u8], offset: usize, message: &'static str) -> Result<u64, V2CommitError> {
    let end = offset
        .checked_add(8)
        .ok_or(V2CommitError::Overflow("v2 commit u64 range exceeds usize"))?;
    let encoded: [u8; 8] = bytes
        .get(offset..end)
        .ok_or(V2CommitError::Invalid(message))?
        .try_into()
        .map_err(|_| V2CommitError::Invalid(message))?;
    Ok(u64::from_le_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::super::format_v2::{V2NodeRecord, V2RootRecord};
    use super::super::publication_v2::{checkpoint_state_metadata, V2VersionRecord};
    use super::*;

    fn initial_transaction() -> V2WalTransaction {
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

    fn rewrite_commit_digest(bytes: &mut [u8]) {
        let digest = commit_digest(
            &bytes[..V2_COMMIT_PREFIX_SIZE],
            &bytes[V2_COMMIT_HEADER_SIZE..],
        );
        bytes[V2_COMMIT_DIGEST_OFFSET..V2_COMMIT_HEADER_SIZE].copy_from_slice(&digest);
    }

    fn hex(bytes: &[u8]) -> String {
        const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(bytes.len().saturating_mul(2));
        for byte in bytes {
            output.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
            output.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
        }
        output
    }

    #[test]
    fn logical_operation_digest_is_stable_and_has_golden_vector() {
        let transaction = initial_transaction();
        let digest = checkpoint_operation_digest(&transaction.checkpoint).unwrap();
        assert_eq!(
            hex(&digest),
            "6e0363445809801219e0a146b177509d7a74ff7cbff1ee35013d94ad433e9eda"
        );

        let mut relocated = transaction.checkpoint.clone();
        relocated.identity_version = 42;
        assert_eq!(checkpoint_operation_digest(&relocated).unwrap(), digest);
    }

    #[test]
    fn requestful_and_requestless_commits_round_trip() {
        let base = V2WalGeometry::default();
        let transaction = initial_transaction();
        let requestful = encode_v2_commit(base, &transaction, Some(b"req-1")).unwrap();
        let decoded = decode_v2_commit(&requestful).unwrap();
        assert_eq!(decoded.encoded_base, base);
        assert_eq!(decoded.wal.transaction, transaction);
        assert_eq!(decoded.request_id.as_deref(), Some(b"req-1".as_slice()));
        assert_eq!(
            decoded.operation_digest,
            checkpoint_operation_digest(&decoded.wal.transaction.checkpoint).unwrap()
        );

        let requestless = encode_v2_commit(base, &transaction, None).unwrap();
        let decoded = decode_v2_commit(&requestless).unwrap();
        assert_eq!(decoded.request_id, None);
        assert_eq!(decoded.operation_digest, [0u8; 32]);
    }

    #[test]
    fn valid_outer_digest_cannot_hide_operation_digest_mismatch() {
        let base = V2WalGeometry::default();
        let transaction = initial_transaction();
        let mut encoded = encode_v2_commit(base, &transaction, Some(b"req-1")).unwrap();
        encoded[24] ^= 1;
        rewrite_commit_digest(&mut encoded);
        assert_eq!(
            decode_v2_commit(&encoded),
            Err(V2CommitError::Invalid(
                "v2 commit operation digest disagrees with checkpoint semantics"
            ))
        );
    }

    #[test]
    fn commit_decoder_rejects_torn_corrupt_and_noncanonical_request_fields() {
        let base = V2WalGeometry::default();
        let transaction = initial_transaction();
        let encoded = encode_v2_commit(base, &transaction, Some(b"req-1")).unwrap();
        assert_eq!(
            decode_v2_commit(&encoded[..encoded.len() - 1]),
            Err(V2CommitError::Invalid("v2 commit byte length mismatch"))
        );

        let mut corrupt = encoded.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode_v2_commit(&corrupt),
            Err(V2CommitError::Invalid("v2 commit digest mismatch"))
        );

        let mut requestless = encode_v2_commit(base, &transaction, None).unwrap();
        requestless[24] = 1;
        rewrite_commit_digest(&mut requestless);
        assert_eq!(
            decode_v2_commit(&requestless),
            Err(V2CommitError::Invalid(
                "v2 requestless commit operation digest must be zero"
            ))
        );
    }
}
