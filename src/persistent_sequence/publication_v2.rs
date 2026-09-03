//! Canonical staged publication metadata for Format v2.
//!
//! These records sit between the balanced persistent-sequence root and the
//! future T2W2 WAL envelope. They are not yet authoritative checkpoint-store
//! bytes; public Format v1 remains unchanged until migration/recovery lands.

use super::format_v2::{decode_v2_root, encode_v2_root, V2FormatError, V2RootRecord};
use sha2::{Digest, Sha256};
use std::fmt;

const V2_VERSION_MAGIC: [u8; 4] = *b"T2V2";
const V2_CHECKPOINT_MAGIC: [u8; 4] = *b"T2P2";
const V2_VERSION_RECORD_SIZE: usize = 72;
const V2_CHECKPOINT_PREFIX_SIZE: usize = 88;
const V2_NONE_VERSION: u32 = u32::MAX;
const V2_CHECKPOINT_SCHEMA: u32 = 1;
const V2_MAX_IDENTIFIER_BYTES: usize = 4096;
const V2_CANONICAL_BASE_BYTES: u64 = 27;
const V2_CANONICAL_RESULT_PREFIX_BYTES: u64 = 10;
const V2_MIN_CANONICAL_STATE_BYTES: u64 = V2_CANONICAL_BASE_BYTES + 1;
const V2_STATE_DOMAIN: &[u8] = b"tulya-checkpoint-v2/state\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2PublicationError {
    Format(V2FormatError),
    RecordLength {
        record: &'static str,
        expected: usize,
        actual: usize,
    },
    Invalid(&'static str),
    Overflow(&'static str),
}

impl fmt::Display for V2PublicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(error) => write!(formatter, "{error}"),
            Self::RecordLength {
                record,
                expected,
                actual,
            } => write!(
                formatter,
                "{record} record length mismatch: expected {expected}, got {actual}"
            ),
            Self::Invalid(message) | Self::Overflow(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for V2PublicationError {}

impl From<V2FormatError> for V2PublicationError {
    fn from(error: V2FormatError) -> Self {
        Self::Format(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct V2StateMetadata {
    logical_len: u64,
    commitment: [u8; 32],
}

impl V2StateMetadata {
    pub(super) const fn logical_len(self) -> u64 {
        self.logical_len
    }

    pub(super) const fn commitment(self) -> [u8; 32] {
        self.commitment
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct V2VersionRecord {
    version_id: u32,
    parent_version: Option<u32>,
    root: V2RootRecord,
}

impl V2VersionRecord {
    pub(super) fn new(
        version_id: u32,
        parent_version: Option<u32>,
        root: V2RootRecord,
    ) -> Result<Self, V2PublicationError> {
        validate_parent_version(version_id, parent_version)?;
        Ok(Self {
            version_id,
            parent_version,
            root,
        })
    }

    pub(super) const fn version_id(self) -> u32 {
        self.version_id
    }

    pub(super) const fn parent_version(self) -> Option<u32> {
        self.parent_version
    }

    pub(super) const fn root(self) -> V2RootRecord {
        self.root
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2CheckpointRecord {
    pub(super) checkpoint_no: u32,
    pub(super) thread_id: String,
    pub(super) checkpoint_id: String,
    pub(super) parent_checkpoint_id: Option<String>,
    pub(super) identity_version: u32,
    pub(super) messages_version: Option<u32>,
    pub(super) result_version: Option<u32>,
    pub(super) state: V2StateMetadata,
}

pub(super) fn checkpoint_state_metadata(
    identity: V2RootRecord,
    messages: Option<V2RootRecord>,
    result: Option<V2RootRecord>,
) -> Result<V2StateMetadata, V2PublicationError> {
    let mut logical_len = V2_CANONICAL_BASE_BYTES
        .checked_add(identity.logical_len())
        .ok_or(V2PublicationError::Overflow(
            "v2 checkpoint canonical length exceeds u64",
        ))?;
    if let Some(messages) = messages {
        logical_len =
            logical_len
                .checked_add(messages.logical_len())
                .ok_or(V2PublicationError::Overflow(
                    "v2 checkpoint canonical length exceeds u64",
                ))?;
    }
    if let Some(result) = result {
        logical_len = logical_len
            .checked_add(V2_CANONICAL_RESULT_PREFIX_BYTES)
            .and_then(|value| value.checked_add(result.logical_len()))
            .ok_or(V2PublicationError::Overflow(
                "v2 checkpoint canonical length exceeds u64",
            ))?;
    }

    let mut hasher = Sha256::new();
    hasher.update(V2_STATE_DOMAIN);
    hasher.update(V2_CHECKPOINT_SCHEMA.to_le_bytes());
    hasher.update(logical_len.to_le_bytes());
    update_root_commitment(&mut hasher, identity);
    update_optional_root_commitment(&mut hasher, messages);
    update_optional_root_commitment(&mut hasher, result);
    let digest = hasher.finalize();
    let mut commitment = [0u8; 32];
    commitment.copy_from_slice(&digest);
    Ok(V2StateMetadata {
        logical_len,
        commitment,
    })
}

pub(super) fn encode_v2_version(
    record: V2VersionRecord,
) -> Result<[u8; V2_VERSION_RECORD_SIZE], V2PublicationError> {
    validate_parent_version(record.version_id, record.parent_version)?;
    let mut output = [0u8; V2_VERSION_RECORD_SIZE];
    output[0..4].copy_from_slice(&V2_VERSION_MAGIC);
    output[4..8].copy_from_slice(&record.version_id.to_le_bytes());
    output[8..12].copy_from_slice(
        &record
            .parent_version
            .unwrap_or(V2_NONE_VERSION)
            .to_le_bytes(),
    );
    output[12..16].copy_from_slice(&0u32.to_le_bytes());
    output[16..72].copy_from_slice(&encode_v2_root(record.root));
    Ok(output)
}

pub(super) fn decode_v2_version(
    bytes: &[u8],
    expected_version_id: u32,
) -> Result<V2VersionRecord, V2PublicationError> {
    require_record_len("v2 version", bytes, V2_VERSION_RECORD_SIZE)?;
    if bytes.get(0..4) != Some(V2_VERSION_MAGIC.as_slice()) {
        return Err(V2PublicationError::Invalid("v2 version magic mismatch"));
    }
    let version_id = read_u32(bytes, 4, "v2 version id is truncated")?;
    if version_id != expected_version_id {
        return Err(V2PublicationError::Invalid(
            "v2 version id is not sequential",
        ));
    }
    let raw_parent = read_u32(bytes, 8, "v2 version parent is truncated")?;
    let parent_version = if raw_parent == V2_NONE_VERSION {
        None
    } else {
        Some(raw_parent)
    };
    validate_parent_version(version_id, parent_version)?;
    if read_u32(bytes, 12, "v2 version flags are truncated")? != 0 {
        return Err(V2PublicationError::Invalid("v2 version flags must be zero"));
    }
    let root = decode_v2_root(
        bytes
            .get(16..72)
            .ok_or(V2PublicationError::Invalid("v2 version root is truncated"))?,
    )?;
    Ok(V2VersionRecord {
        version_id,
        parent_version,
        root,
    })
}

pub(super) fn encode_v2_checkpoint(
    record: &V2CheckpointRecord,
) -> Result<Vec<u8>, V2PublicationError> {
    validate_identifier(&record.thread_id, "v2 checkpoint thread id is invalid")?;
    validate_identifier(&record.checkpoint_id, "v2 checkpoint id is invalid")?;
    if let Some(parent) = record.parent_checkpoint_id.as_deref() {
        validate_identifier(parent, "v2 checkpoint parent id is invalid")?;
    }
    validate_encodable_version_reference(record.identity_version)?;
    if let Some(version) = record.messages_version {
        validate_encodable_version_reference(version)?;
    }
    if let Some(version) = record.result_version {
        validate_encodable_version_reference(version)?;
    }
    if record.state.logical_len < V2_MIN_CANONICAL_STATE_BYTES {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint canonical length is too small",
        ));
    }

    let parent = record.parent_checkpoint_id.as_deref().unwrap_or("");
    let record_len = V2_CHECKPOINT_PREFIX_SIZE
        .checked_add(record.thread_id.len())
        .and_then(|value| value.checked_add(record.checkpoint_id.len()))
        .and_then(|value| value.checked_add(parent.len()))
        .ok_or(V2PublicationError::Overflow(
            "v2 checkpoint record length exceeds usize",
        ))?;
    let record_len_u32 = u32::try_from(record_len)
        .map_err(|_| V2PublicationError::Overflow("v2 checkpoint record length exceeds u32"))?;
    let thread_len = u32::try_from(record.thread_id.len())
        .map_err(|_| V2PublicationError::Overflow("v2 checkpoint thread length exceeds u32"))?;
    let checkpoint_len = u32::try_from(record.checkpoint_id.len())
        .map_err(|_| V2PublicationError::Overflow("v2 checkpoint id length exceeds u32"))?;
    let parent_len = u32::try_from(parent.len())
        .map_err(|_| V2PublicationError::Overflow("v2 checkpoint parent length exceeds u32"))?;

    let mut output = Vec::with_capacity(record_len);
    output.extend_from_slice(&V2_CHECKPOINT_MAGIC);
    output.extend_from_slice(&record_len_u32.to_le_bytes());
    output.extend_from_slice(&record.checkpoint_no.to_le_bytes());
    output.extend_from_slice(&record.identity_version.to_le_bytes());
    output.extend_from_slice(
        &record
            .messages_version
            .unwrap_or(V2_NONE_VERSION)
            .to_le_bytes(),
    );
    output.extend_from_slice(
        &record
            .result_version
            .unwrap_or(V2_NONE_VERSION)
            .to_le_bytes(),
    );
    output.extend_from_slice(&thread_len.to_le_bytes());
    output.extend_from_slice(&checkpoint_len.to_le_bytes());
    output.extend_from_slice(&parent_len.to_le_bytes());
    output.extend_from_slice(&V2_CHECKPOINT_SCHEMA.to_le_bytes());
    output.extend_from_slice(&record.state.logical_len.to_le_bytes());
    output.extend_from_slice(&record.state.commitment);
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(record.thread_id.as_bytes());
    output.extend_from_slice(record.checkpoint_id.as_bytes());
    output.extend_from_slice(parent.as_bytes());
    if output.len() != record_len {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint encoder produced an unexpected byte length",
        ));
    }
    Ok(output)
}

pub(super) fn decode_v2_checkpoint(
    bytes: &[u8],
    version_count: u64,
) -> Result<V2CheckpointRecord, V2PublicationError> {
    if bytes.len() < V2_CHECKPOINT_PREFIX_SIZE {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint record is shorter than its prefix",
        ));
    }
    if bytes.get(0..4) != Some(V2_CHECKPOINT_MAGIC.as_slice()) {
        return Err(V2PublicationError::Invalid("v2 checkpoint magic mismatch"));
    }
    let record_len = usize::try_from(read_u32(
        bytes,
        4,
        "v2 checkpoint record length is truncated",
    )?)
    .map_err(|_| V2PublicationError::Overflow("v2 checkpoint record length exceeds usize"))?;
    if record_len != bytes.len() {
        return Err(V2PublicationError::RecordLength {
            record: "v2 checkpoint",
            expected: record_len,
            actual: bytes.len(),
        });
    }

    let checkpoint_no = read_u32(bytes, 8, "v2 checkpoint number is truncated")?;
    let identity_version = read_u32(bytes, 12, "v2 identity version is truncated")?;
    let messages_version =
        decode_optional_version(read_u32(bytes, 16, "v2 messages version is truncated")?);
    let result_version =
        decode_optional_version(read_u32(bytes, 20, "v2 result version is truncated")?);
    validate_version_reference(identity_version, version_count)?;
    if let Some(version) = messages_version {
        validate_version_reference(version, version_count)?;
    }
    if let Some(version) = result_version {
        validate_version_reference(version, version_count)?;
    }

    let thread_len = usize::try_from(read_u32(
        bytes,
        24,
        "v2 checkpoint thread length is truncated",
    )?)
    .map_err(|_| V2PublicationError::Overflow("v2 checkpoint thread length exceeds usize"))?;
    let checkpoint_len =
        usize::try_from(read_u32(bytes, 28, "v2 checkpoint id length is truncated")?)
            .map_err(|_| V2PublicationError::Overflow("v2 checkpoint id length exceeds usize"))?;
    let parent_len = usize::try_from(read_u32(
        bytes,
        32,
        "v2 checkpoint parent length is truncated",
    )?)
    .map_err(|_| V2PublicationError::Overflow("v2 checkpoint parent length exceeds usize"))?;
    if thread_len == 0 || thread_len > V2_MAX_IDENTIFIER_BYTES {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint thread id is invalid",
        ));
    }
    if checkpoint_len == 0 || checkpoint_len > V2_MAX_IDENTIFIER_BYTES {
        return Err(V2PublicationError::Invalid("v2 checkpoint id is invalid"));
    }
    if parent_len > V2_MAX_IDENTIFIER_BYTES {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint parent id is invalid",
        ));
    }
    if read_u32(bytes, 36, "v2 checkpoint schema is truncated")? != V2_CHECKPOINT_SCHEMA {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint schema is unsupported",
        ));
    }
    let logical_len = read_u64(bytes, 40, "v2 checkpoint logical length is truncated")?;
    if logical_len < V2_MIN_CANONICAL_STATE_BYTES {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint canonical length is too small",
        ));
    }
    let commitment: [u8; 32] = bytes
        .get(48..80)
        .ok_or(V2PublicationError::Invalid(
            "v2 checkpoint commitment is truncated",
        ))?
        .try_into()
        .map_err(|_| V2PublicationError::Invalid("v2 checkpoint commitment width mismatch"))?;
    if read_u32(bytes, 80, "v2 checkpoint flags are truncated")? != 0 {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint flags must be zero",
        ));
    }
    if read_u32(bytes, 84, "v2 checkpoint reserved field is truncated")? != 0 {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint reserved field must be zero",
        ));
    }

    let payload_len = thread_len
        .checked_add(checkpoint_len)
        .and_then(|value| value.checked_add(parent_len))
        .ok_or(V2PublicationError::Overflow(
            "v2 checkpoint identifier bytes exceed usize",
        ))?;
    let payload_end =
        V2_CHECKPOINT_PREFIX_SIZE
            .checked_add(payload_len)
            .ok_or(V2PublicationError::Overflow(
                "v2 checkpoint payload end exceeds usize",
            ))?;
    if payload_end != bytes.len() {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint identifier geometry mismatch",
        ));
    }

    let thread_start = V2_CHECKPOINT_PREFIX_SIZE;
    let thread_end = thread_start + thread_len;
    let checkpoint_end = thread_end + checkpoint_len;
    let parent_end = checkpoint_end + parent_len;
    let thread_id = decode_identifier(
        bytes
            .get(thread_start..thread_end)
            .ok_or(V2PublicationError::Invalid(
                "v2 checkpoint thread bytes are truncated",
            ))?,
        "v2 checkpoint thread id is not UTF-8",
    )?;
    let checkpoint_id = decode_identifier(
        bytes
            .get(thread_end..checkpoint_end)
            .ok_or(V2PublicationError::Invalid(
                "v2 checkpoint id bytes are truncated",
            ))?,
        "v2 checkpoint id is not UTF-8",
    )?;
    let parent = decode_identifier(
        bytes
            .get(checkpoint_end..parent_end)
            .ok_or(V2PublicationError::Invalid(
                "v2 checkpoint parent bytes are truncated",
            ))?,
        "v2 checkpoint parent id is not UTF-8",
    )?;

    Ok(V2CheckpointRecord {
        checkpoint_no,
        thread_id,
        checkpoint_id,
        parent_checkpoint_id: if parent.is_empty() {
            None
        } else {
            Some(parent)
        },
        identity_version,
        messages_version,
        result_version,
        state: V2StateMetadata {
            logical_len,
            commitment,
        },
    })
}

fn update_root_commitment(hasher: &mut Sha256, root: V2RootRecord) {
    hasher.update(root.logical_len().to_le_bytes());
    hasher.update(root.commitment().as_bytes());
}

fn update_optional_root_commitment(hasher: &mut Sha256, root: Option<V2RootRecord>) {
    match root {
        Some(root) => {
            hasher.update([1u8]);
            update_root_commitment(hasher, root);
        }
        None => hasher.update([0u8]),
    }
}

fn validate_parent_version(
    version_id: u32,
    parent_version: Option<u32>,
) -> Result<(), V2PublicationError> {
    if version_id == V2_NONE_VERSION {
        return Err(V2PublicationError::Invalid(
            "v2 version id uses the reserved none sentinel",
        ));
    }
    if parent_version == Some(V2_NONE_VERSION) {
        return Err(V2PublicationError::Invalid(
            "v2 version parent uses the reserved none sentinel",
        ));
    }
    if parent_version.is_some_and(|parent| parent >= version_id) {
        return Err(V2PublicationError::Invalid(
            "v2 version parent is not topologically prior",
        ));
    }
    Ok(())
}

fn validate_identifier(value: &str, message: &'static str) -> Result<(), V2PublicationError> {
    if value.is_empty() || value.len() > V2_MAX_IDENTIFIER_BYTES {
        return Err(V2PublicationError::Invalid(message));
    }
    Ok(())
}

fn validate_encodable_version_reference(version: u32) -> Result<(), V2PublicationError> {
    if version == V2_NONE_VERSION {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint version reference uses the reserved none sentinel",
        ));
    }
    Ok(())
}

fn validate_version_reference(version: u32, version_count: u64) -> Result<(), V2PublicationError> {
    validate_encodable_version_reference(version)?;
    if u64::from(version) >= version_count {
        return Err(V2PublicationError::Invalid(
            "v2 checkpoint version reference is outside the committed watermark",
        ));
    }
    Ok(())
}

const fn decode_optional_version(raw: u32) -> Option<u32> {
    if raw == V2_NONE_VERSION {
        None
    } else {
        Some(raw)
    }
}

fn decode_identifier(bytes: &[u8], message: &'static str) -> Result<String, V2PublicationError> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| V2PublicationError::Invalid(message))
}

fn require_record_len(
    record: &'static str,
    bytes: &[u8],
    expected: usize,
) -> Result<(), V2PublicationError> {
    if bytes.len() != expected {
        return Err(V2PublicationError::RecordLength {
            record,
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn read_u32(bytes: &[u8], offset: usize, message: &'static str) -> Result<u32, V2PublicationError> {
    let end = offset.checked_add(4).ok_or(V2PublicationError::Overflow(
        "v2 publication u32 range exceeds usize",
    ))?;
    let encoded: [u8; 4] = bytes
        .get(offset..end)
        .ok_or(V2PublicationError::Invalid(message))?
        .try_into()
        .map_err(|_| V2PublicationError::Invalid(message))?;
    Ok(u32::from_le_bytes(encoded))
}

fn read_u64(bytes: &[u8], offset: usize, message: &'static str) -> Result<u64, V2PublicationError> {
    let end = offset.checked_add(8).ok_or(V2PublicationError::Overflow(
        "v2 publication u64 range exceeds usize",
    ))?;
    let encoded: [u8; 8] = bytes
        .get(offset..end)
        .ok_or(V2PublicationError::Invalid(message))?
        .try_into()
        .map_err(|_| V2PublicationError::Invalid(message))?;
    Ok(u64::from_le_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::super::format_v2::V2NodeRecord;
    use super::*;

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
    fn checkpoint_state_commitment_matches_golden_and_excludes_node_ids() {
        let identity_node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let messages_node = V2NodeRecord::leaf(3, b"XYZ").unwrap();
        let result_node = V2NodeRecord::leaf(6, b"42").unwrap();
        let identity = V2RootRecord::from_node(7, identity_node).unwrap();
        let identity_relocated = V2RootRecord::from_node(700, identity_node).unwrap();
        let messages = V2RootRecord::from_node(8, messages_node).unwrap();
        let result = V2RootRecord::from_node(9, result_node).unwrap();

        let metadata = checkpoint_state_metadata(identity, Some(messages), Some(result)).unwrap();
        let relocated =
            checkpoint_state_metadata(identity_relocated, Some(messages), Some(result)).unwrap();
        assert_eq!(metadata, relocated);
        assert_eq!(metadata.logical_len(), 45);
        assert_eq!(
            hex(&metadata.commitment()),
            "1b3379c80cc8de2af35c72e82f7b72ea84190c4508d327925a87477d21d824fb"
        );
    }

    #[test]
    fn version_codec_matches_golden_and_fails_closed() {
        let node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let root = V2RootRecord::from_node(7, node).unwrap();
        let version = V2VersionRecord::new(9, Some(4), root).unwrap();
        let encoded = encode_v2_version(version).unwrap();
        assert_eq!(
            hex(&encoded),
            "54325632090000000400000000000000543252320100010007000000000000000300000000000000631a198b7685ef4dfb1032c761262dc81d3f1c2b1e5e740f81f056e1b847d068"
        );
        assert_eq!(decode_v2_version(&encoded, 9), Ok(version));

        let mut bad_flags = encoded;
        bad_flags[12] = 1;
        assert_eq!(
            decode_v2_version(&bad_flags, 9),
            Err(V2PublicationError::Invalid("v2 version flags must be zero"))
        );

        let mut bad_parent = encoded;
        bad_parent[8..12].copy_from_slice(&9u32.to_le_bytes());
        assert_eq!(
            decode_v2_version(&bad_parent, 9),
            Err(V2PublicationError::Invalid(
                "v2 version parent is not topologically prior"
            ))
        );

        assert_eq!(
            V2VersionRecord::new(V2_NONE_VERSION, None, root),
            Err(V2PublicationError::Invalid(
                "v2 version id uses the reserved none sentinel"
            ))
        );
    }

    #[test]
    fn checkpoint_codec_matches_golden_and_validates_topology() {
        let identity_node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let identity = V2RootRecord::from_node(7, identity_node).unwrap();
        let state = checkpoint_state_metadata(identity, None, None).unwrap();
        let record = V2CheckpointRecord {
            checkpoint_no: 11,
            thread_id: "t".to_owned(),
            checkpoint_id: "c".to_owned(),
            parent_checkpoint_id: None,
            identity_version: 9,
            messages_version: None,
            result_version: None,
            state,
        };
        let encoded = encode_v2_checkpoint(&record).unwrap();
        assert_eq!(
            hex(&encoded),
            "543250325a0000000b00000009000000ffffffffffffffff010000000100000000000000010000001e0000000000000074a8ea61c65c688272bbf9662aabb1ee88301f89810198b88c7eadcac5384a6800000000000000007463"
        );
        assert_eq!(decode_v2_checkpoint(&encoded, 10), Ok(record.clone()));

        assert_eq!(
            decode_v2_checkpoint(&encoded, 9),
            Err(V2PublicationError::Invalid(
                "v2 checkpoint version reference is outside the committed watermark"
            ))
        );

        let mut bad_schema = encoded.clone();
        bad_schema[36..40].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(
            decode_v2_checkpoint(&bad_schema, 10),
            Err(V2PublicationError::Invalid(
                "v2 checkpoint schema is unsupported"
            ))
        );

        let mut bad_reserved = encoded;
        bad_reserved[84] = 1;
        assert_eq!(
            decode_v2_checkpoint(&bad_reserved, 10),
            Err(V2PublicationError::Invalid(
                "v2 checkpoint reserved field must be zero"
            ))
        );

        let reserved_version = V2CheckpointRecord {
            identity_version: V2_NONE_VERSION,
            ..record
        };
        assert_eq!(
            encode_v2_checkpoint(&reserved_version),
            Err(V2PublicationError::Invalid(
                "v2 checkpoint version reference uses the reserved none sentinel"
            ))
        );
    }
}
