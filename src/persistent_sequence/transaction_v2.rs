//! Canonical staged Format-v2 WAL transaction codec.
//!
//! `T2W2` batches only append-local payload/node/version deltas plus one
//! checkpoint record. It is not yet connected to the production hot WAL;
//! Format v1 remains the only authoritative checkpoint-store format until
//! migration and dual-version recovery are implemented.

use super::format_v2::{
    decode_v2_node, encode_v2_node, V2FormatError, V2NodeRecord, V2RootRecord,
};
use super::publication_v2::{
    checkpoint_state_metadata, decode_v2_checkpoint, decode_v2_version, encode_v2_checkpoint,
    encode_v2_version, V2CheckpointRecord, V2PublicationError, V2VersionRecord,
};
use sha2::{Digest, Sha256};
use std::fmt;

const V2_WAL_MAGIC: [u8; 4] = *b"T2W2";
const V2_WAL_SCHEMA: u32 = 1;
const V2_WAL_HEADER_PREFIX_SIZE: usize = 88;
const V2_WAL_HEADER_SIZE: usize = 120;
const V2_WAL_DIGEST_OFFSET: usize = 88;
const V2_WAL_DIGEST_SIZE: usize = 32;
const V2_WAL_NODE_RECORD_SIZE: usize = 72;
const V2_WAL_VERSION_RECORD_SIZE: usize = 72;
const V2_WAL_DOMAIN: &[u8] = b"tulya-checkpoint-v2/wal-transaction\0";
const V2_NONE_VERSION: u64 = u32::MAX as u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2WalError {
    Format(V2FormatError),
    Publication(V2PublicationError),
    Invalid(&'static str),
    Overflow(&'static str),
}

impl fmt::Display for V2WalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(error) => write!(formatter, "{error}"),
            Self::Publication(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for V2WalError {}

impl From<V2FormatError> for V2WalError {
    fn from(error: V2FormatError) -> Self {
        Self::Format(error)
    }
}

impl From<V2PublicationError> for V2WalError {
    fn from(error: V2PublicationError) -> Self {
        Self::Publication(error)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct V2WalGeometry {
    pub(super) payload_len: u64,
    pub(super) node_count: u64,
    pub(super) version_count: u64,
    pub(super) checkpoint_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2WalTransaction {
    pub(super) payload: Vec<u8>,
    pub(super) nodes: Vec<V2NodeRecord>,
    pub(super) versions: Vec<V2VersionRecord>,
    pub(super) checkpoint: V2CheckpointRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2DecodedWalTransaction {
    pub(super) transaction: V2WalTransaction,
    pub(super) next_geometry: V2WalGeometry,
    pub(super) digest: [u8; 32],
}

pub(super) fn encode_v2_wal_transaction(
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
) -> Result<Vec<u8>, V2WalError> {
    let next_geometry = validate_transaction(base, transaction)?;
    let checkpoint = encode_v2_checkpoint(&transaction.checkpoint)?;
    let payload_len = u64::try_from(transaction.payload.len())
        .map_err(|_| V2WalError::Overflow("v2 WAL payload delta length exceeds u64"))?;
    let node_count = u64::try_from(transaction.nodes.len())
        .map_err(|_| V2WalError::Overflow("v2 WAL node delta count exceeds u64"))?;
    let version_count = u64::try_from(transaction.versions.len())
        .map_err(|_| V2WalError::Overflow("v2 WAL version delta count exceeds u64"))?;
    let checkpoint_len = u32::try_from(checkpoint.len())
        .map_err(|_| V2WalError::Overflow("v2 WAL checkpoint record length exceeds u32"))?;
    let total_len = checked_total_len(
        transaction.payload.len(),
        transaction.nodes.len(),
        transaction.versions.len(),
        checkpoint.len(),
    )?;
    let total_len_u32 = u32::try_from(total_len)
        .map_err(|_| V2WalError::Overflow("v2 WAL transaction length exceeds u32"))?;

    let mut output = vec![0u8; V2_WAL_HEADER_SIZE];
    output[0..4].copy_from_slice(&V2_WAL_MAGIC);
    output[4..8].copy_from_slice(&total_len_u32.to_le_bytes());
    output[8..12].copy_from_slice(&V2_WAL_SCHEMA.to_le_bytes());
    output[12..16].copy_from_slice(&0u32.to_le_bytes());
    output[16..24].copy_from_slice(&base.payload_len.to_le_bytes());
    output[24..32].copy_from_slice(&payload_len.to_le_bytes());
    output[32..40].copy_from_slice(&base.node_count.to_le_bytes());
    output[40..48].copy_from_slice(&node_count.to_le_bytes());
    output[48..56].copy_from_slice(&base.version_count.to_le_bytes());
    output[56..64].copy_from_slice(&version_count.to_le_bytes());
    output[64..72].copy_from_slice(&base.checkpoint_count.to_le_bytes());
    output[72..76].copy_from_slice(&checkpoint_len.to_le_bytes());
    output[76..80].copy_from_slice(
        &u32::try_from(V2_WAL_NODE_RECORD_SIZE)
            .map_err(|_| V2WalError::Overflow("v2 WAL node record width exceeds u32"))?
            .to_le_bytes(),
    );
    output[80..84].copy_from_slice(
        &u32::try_from(V2_WAL_VERSION_RECORD_SIZE)
            .map_err(|_| V2WalError::Overflow("v2 WAL version record width exceeds u32"))?
            .to_le_bytes(),
    );
    output[84..88].copy_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&transaction.payload);
    for node in &transaction.nodes {
        output.extend_from_slice(&encode_v2_node(*node));
    }
    for version in &transaction.versions {
        output.extend_from_slice(&encode_v2_version(*version)?);
    }
    output.extend_from_slice(&checkpoint);
    if output.len() != total_len {
        return Err(V2WalError::Invalid(
            "v2 WAL encoder produced an unexpected byte length",
        ));
    }
    let digest = transaction_digest(
        &output[..V2_WAL_HEADER_PREFIX_SIZE],
        &output[V2_WAL_HEADER_SIZE..],
    );
    output[V2_WAL_DIGEST_OFFSET..V2_WAL_HEADER_SIZE].copy_from_slice(&digest);

    let encoded_next = V2WalGeometry {
        payload_len: base
            .payload_len
            .checked_add(payload_len)
            .ok_or(V2WalError::Overflow("v2 WAL payload watermark exceeds u64"))?,
        node_count: base
            .node_count
            .checked_add(node_count)
            .ok_or(V2WalError::Overflow("v2 WAL node watermark exceeds u64"))?,
        version_count: base
            .version_count
            .checked_add(version_count)
            .ok_or(V2WalError::Overflow("v2 WAL version watermark exceeds u64"))?,
        checkpoint_count: base
            .checkpoint_count
            .checked_add(1)
            .ok_or(V2WalError::Overflow(
                "v2 WAL checkpoint watermark exceeds u64",
            ))?,
    };
    if encoded_next != next_geometry {
        return Err(V2WalError::Invalid(
            "v2 WAL validated geometry changed during encoding",
        ));
    }
    Ok(output)
}

pub(super) fn decode_v2_wal_transaction(
    bytes: &[u8],
    base: V2WalGeometry,
) -> Result<V2DecodedWalTransaction, V2WalError> {
    if bytes.len() < V2_WAL_HEADER_SIZE {
        return Err(V2WalError::Invalid("v2 WAL header is truncated"));
    }
    if bytes.get(0..4) != Some(V2_WAL_MAGIC.as_slice()) {
        return Err(V2WalError::Invalid("v2 WAL magic mismatch"));
    }
    let total_len = usize::try_from(read_u32(bytes, 4, "v2 WAL length is truncated")?)
        .map_err(|_| V2WalError::Overflow("v2 WAL transaction length exceeds usize"))?;
    if total_len != bytes.len() {
        return Err(V2WalError::Invalid("v2 WAL transaction length mismatch"));
    }
    if read_u32(bytes, 8, "v2 WAL schema is truncated")? != V2_WAL_SCHEMA {
        return Err(V2WalError::Invalid("v2 WAL schema is unsupported"));
    }
    if read_u32(bytes, 12, "v2 WAL flags are truncated")? != 0 {
        return Err(V2WalError::Invalid("v2 WAL flags must be zero"));
    }
    let payload_start = read_u64(bytes, 16, "v2 WAL payload start is truncated")?;
    let payload_len = read_u64(bytes, 24, "v2 WAL payload length is truncated")?;
    let node_start = read_u64(bytes, 32, "v2 WAL node start is truncated")?;
    let node_count = read_u64(bytes, 40, "v2 WAL node count is truncated")?;
    let version_start = read_u64(bytes, 48, "v2 WAL version start is truncated")?;
    let version_count = read_u64(bytes, 56, "v2 WAL version count is truncated")?;
    let checkpoint_start = read_u64(bytes, 64, "v2 WAL checkpoint start is truncated")?;
    let checkpoint_len = usize::try_from(read_u32(
        bytes,
        72,
        "v2 WAL checkpoint record length is truncated",
    )?)
    .map_err(|_| V2WalError::Overflow("v2 WAL checkpoint record length exceeds usize"))?;
    if usize::try_from(read_u32(bytes, 76, "v2 WAL node record width is truncated")?)
        .map_err(|_| V2WalError::Overflow("v2 WAL node record width exceeds usize"))?
        != V2_WAL_NODE_RECORD_SIZE
    {
        return Err(V2WalError::Invalid(
            "v2 WAL node record width is unsupported",
        ));
    }
    if usize::try_from(read_u32(
        bytes,
        80,
        "v2 WAL version record width is truncated",
    )?)
    .map_err(|_| V2WalError::Overflow("v2 WAL version record width exceeds usize"))?
        != V2_WAL_VERSION_RECORD_SIZE
    {
        return Err(V2WalError::Invalid(
            "v2 WAL version record width is unsupported",
        ));
    }
    if read_u32(bytes, 84, "v2 WAL reserved field is truncated")? != 0 {
        return Err(V2WalError::Invalid(
            "v2 WAL reserved field must be zero",
        ));
    }
    if payload_start != base.payload_len
        || node_start != base.node_count
        || version_start != base.version_count
        || checkpoint_start != base.checkpoint_count
    {
        return Err(V2WalError::Invalid(
            "v2 WAL transaction starting watermark mismatch",
        ));
    }

    let stored_digest: [u8; V2_WAL_DIGEST_SIZE] = bytes
        .get(V2_WAL_DIGEST_OFFSET..V2_WAL_HEADER_SIZE)
        .ok_or(V2WalError::Invalid("v2 WAL digest is truncated"))?
        .try_into()
        .map_err(|_| V2WalError::Invalid("v2 WAL digest width mismatch"))?;
    let expected_digest = transaction_digest(
        &bytes[..V2_WAL_HEADER_PREFIX_SIZE],
        &bytes[V2_WAL_HEADER_SIZE..],
    );
    if stored_digest != expected_digest {
        return Err(V2WalError::Invalid("v2 WAL digest mismatch"));
    }

    let payload_len_usize = usize::try_from(payload_len)
        .map_err(|_| V2WalError::Overflow("v2 WAL payload length exceeds usize"))?;
    let node_count_usize = usize::try_from(node_count)
        .map_err(|_| V2WalError::Overflow("v2 WAL node count exceeds usize"))?;
    let version_count_usize = usize::try_from(version_count)
        .map_err(|_| V2WalError::Overflow("v2 WAL version count exceeds usize"))?;
    let expected_len = checked_total_len(
        payload_len_usize,
        node_count_usize,
        version_count_usize,
        checkpoint_len,
    )?;
    if expected_len != bytes.len() {
        return Err(V2WalError::Invalid("v2 WAL section geometry mismatch"));
    }

    let header_next = next_geometry_from_counts(base, payload_len, node_count, version_count)?;
    let mut cursor = V2_WAL_HEADER_SIZE;
    let payload_end = cursor
        .checked_add(payload_len_usize)
        .ok_or(V2WalError::Overflow("v2 WAL payload range exceeds usize"))?;
    let payload = bytes
        .get(cursor..payload_end)
        .ok_or(V2WalError::Invalid("v2 WAL payload is truncated"))?
        .to_vec();
    cursor = payload_end;

    let mut nodes = Vec::with_capacity(node_count_usize);
    for _ in 0..node_count_usize {
        let end = cursor
            .checked_add(V2_WAL_NODE_RECORD_SIZE)
            .ok_or(V2WalError::Overflow("v2 WAL node range exceeds usize"))?;
        let node = decode_v2_node(
            bytes
                .get(cursor..end)
                .ok_or(V2WalError::Invalid("v2 WAL node table is truncated"))?,
        )?;
        nodes.push(node);
        cursor = end;
    }

    let mut versions = Vec::with_capacity(version_count_usize);
    for local in 0..version_count_usize {
        let end = cursor
            .checked_add(V2_WAL_VERSION_RECORD_SIZE)
            .ok_or(V2WalError::Overflow("v2 WAL version range exceeds usize"))?;
        let version_id = version_id_for_local(base.version_count, local)?;
        let version = decode_v2_version(
            bytes
                .get(cursor..end)
                .ok_or(V2WalError::Invalid("v2 WAL version table is truncated"))?,
            version_id,
        )?;
        versions.push(version);
        cursor = end;
    }

    let checkpoint_end = cursor
        .checked_add(checkpoint_len)
        .ok_or(V2WalError::Overflow(
            "v2 WAL checkpoint range exceeds usize",
        ))?;
    let checkpoint = decode_v2_checkpoint(
        bytes
            .get(cursor..checkpoint_end)
            .ok_or(V2WalError::Invalid("v2 WAL checkpoint record is truncated"))?,
        header_next.version_count,
    )?;
    cursor = checkpoint_end;
    if cursor != bytes.len() {
        return Err(V2WalError::Invalid(
            "v2 WAL decoder did not consume all bytes",
        ));
    }

    let transaction = V2WalTransaction {
        payload,
        nodes,
        versions,
        checkpoint,
    };
    let next_geometry = validate_transaction(base, &transaction)?;
    if next_geometry != header_next {
        return Err(V2WalError::Invalid(
            "v2 WAL header geometry disagrees with decoded sections",
        ));
    }
    Ok(V2DecodedWalTransaction {
        transaction,
        next_geometry,
        digest: stored_digest,
    })
}

fn validate_transaction(
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
) -> Result<V2WalGeometry, V2WalError> {
    validate_base_geometry(base)?;
    let payload_len = u64::try_from(transaction.payload.len())
        .map_err(|_| V2WalError::Overflow("v2 WAL payload delta length exceeds u64"))?;
    let node_count = u64::try_from(transaction.nodes.len())
        .map_err(|_| V2WalError::Overflow("v2 WAL node delta count exceeds u64"))?;
    let version_count = u64::try_from(transaction.versions.len())
        .map_err(|_| V2WalError::Overflow("v2 WAL version delta count exceeds u64"))?;
    let next = next_geometry_from_counts(base, payload_len, node_count, version_count)?;
    validate_nodes(base, next, transaction)?;
    validate_versions(base, next, transaction)?;
    validate_checkpoint(base, next, transaction)?;
    Ok(next)
}

fn validate_base_geometry(base: V2WalGeometry) -> Result<(), V2WalError> {
    if base.version_count > V2_NONE_VERSION {
        return Err(V2WalError::Invalid(
            "v2 WAL base version watermark exceeds the encodable range",
        ));
    }
    Ok(())
}

fn next_geometry_from_counts(
    base: V2WalGeometry,
    payload_delta_len: u64,
    node_delta_count: u64,
    version_delta_count: u64,
) -> Result<V2WalGeometry, V2WalError> {
    validate_base_geometry(base)?;
    let version_count = base
        .version_count
        .checked_add(version_delta_count)
        .ok_or(V2WalError::Overflow("v2 WAL version watermark exceeds u64"))?;
    if version_count > V2_NONE_VERSION {
        return Err(V2WalError::Invalid(
            "v2 WAL version watermark crosses the reserved none sentinel",
        ));
    }
    Ok(V2WalGeometry {
        payload_len: base
            .payload_len
            .checked_add(payload_delta_len)
            .ok_or(V2WalError::Overflow("v2 WAL payload watermark exceeds u64"))?,
        node_count: base
            .node_count
            .checked_add(node_delta_count)
            .ok_or(V2WalError::Overflow("v2 WAL node watermark exceeds u64"))?,
        version_count,
        checkpoint_count: base
            .checkpoint_count
            .checked_add(1)
            .ok_or(V2WalError::Overflow(
                "v2 WAL checkpoint watermark exceeds u64",
            ))?,
    })
}

fn validate_nodes(
    base: V2WalGeometry,
    next: V2WalGeometry,
    transaction: &V2WalTransaction,
) -> Result<(), V2WalError> {
    let mut leaf_ranges = Vec::new();
    for (local, node) in transaction.nodes.iter().copied().enumerate() {
        let node_id = base
            .node_count
            .checked_add(
                u64::try_from(local)
                    .map_err(|_| V2WalError::Overflow("v2 WAL node index exceeds u64"))?,
            )
            .ok_or(V2WalError::Overflow("v2 WAL node identifier exceeds u64"))?;
        let encoded = encode_v2_node(node);
        if encoded.len() != V2_WAL_NODE_RECORD_SIZE {
            return Err(V2WalError::Invalid(
                "v2 WAL T2N2 record width disagrees with schema",
            ));
        }
        if decode_v2_node(&encoded)? != node {
            return Err(V2WalError::Invalid(
                "v2 WAL node does not round-trip canonically",
            ));
        }
        let field_a = read_u64(&encoded, 16, "v2 WAL node field A is truncated")?;
        let field_b = read_u64(&encoded, 24, "v2 WAL node field B is truncated")?;
        if node.height() == 1 {
            let end = field_a
                .checked_add(field_b)
                .ok_or(V2WalError::Overflow(
                    "v2 WAL leaf payload range exceeds u64",
                ))?;
            if field_a < base.payload_len || end > next.payload_len {
                return Err(V2WalError::Invalid(
                    "v2 WAL new leaf references bytes outside the payload delta",
                ));
            }
            let local_start = usize::try_from(field_a - base.payload_len)
                .map_err(|_| V2WalError::Overflow("v2 WAL leaf start exceeds usize"))?;
            let local_end = usize::try_from(end - base.payload_len)
                .map_err(|_| V2WalError::Overflow("v2 WAL leaf end exceeds usize"))?;
            let payload = transaction
                .payload
                .get(local_start..local_end)
                .ok_or(V2WalError::Invalid(
                    "v2 WAL leaf payload slice is outside the delta",
                ))?;
            if V2NodeRecord::leaf(field_a, payload)? != node {
                return Err(V2WalError::Invalid(
                    "v2 WAL leaf commitment disagrees with payload bytes",
                ));
            }
            leaf_ranges.push((field_a, end));
        } else {
            if field_a >= node_id || field_b >= node_id {
                return Err(V2WalError::Invalid(
                    "v2 WAL branch child is not topologically prior",
                ));
            }
            if let (Some(left), Some(right)) = (
                local_node(base, transaction, field_a)?,
                local_node(base, transaction, field_b)?,
            ) {
                let expected = V2NodeRecord::branch(
                    V2RootRecord::from_node(field_a, left)?,
                    V2RootRecord::from_node(field_b, right)?,
                )?;
                if expected != node {
                    return Err(V2WalError::Invalid(
                        "v2 WAL local branch metadata or commitment is inconsistent",
                    ));
                }
            }
        }
    }

    leaf_ranges.sort_unstable_by_key(|range| range.0);
    let mut cursor = base.payload_len;
    for (start, end) in leaf_ranges {
        if start != cursor {
            return Err(V2WalError::Invalid(
                "v2 WAL new leaves do not contiguously own the payload delta",
            ));
        }
        cursor = end;
    }
    if cursor != next.payload_len {
        return Err(V2WalError::Invalid(
            "v2 WAL payload delta contains bytes not owned by new leaves",
        ));
    }
    Ok(())
}

fn local_node(
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
    node_id: u64,
) -> Result<Option<V2NodeRecord>, V2WalError> {
    if node_id < base.node_count {
        return Ok(None);
    }
    let local = usize::try_from(node_id - base.node_count)
        .map_err(|_| V2WalError::Overflow("v2 WAL local node index exceeds usize"))?;
    Ok(transaction.nodes.get(local).copied())
}

fn validate_versions(
    base: V2WalGeometry,
    next: V2WalGeometry,
    transaction: &V2WalTransaction,
) -> Result<(), V2WalError> {
    for (local, version) in transaction.versions.iter().copied().enumerate() {
        let expected_id = version_id_for_local(base.version_count, local)?;
        if version.version_id() != expected_id {
            return Err(V2WalError::Invalid(
                "v2 WAL version identifiers are not sequential",
            ));
        }
        let encoded = encode_v2_version(version)?;
        if encoded.len() != V2_WAL_VERSION_RECORD_SIZE {
            return Err(V2WalError::Invalid(
                "v2 WAL T2V2 record width disagrees with schema",
            ));
        }
        if decode_v2_version(&encoded, expected_id)? != version {
            return Err(V2WalError::Invalid(
                "v2 WAL version does not round-trip canonically",
            ));
        }
        let root = version.root();
        if root.node_id() >= next.node_count {
            return Err(V2WalError::Invalid(
                "v2 WAL version root references an uncommitted node",
            ));
        }
        if let Some(node) = local_node(base, transaction, root.node_id())? {
            if V2RootRecord::from_node(root.node_id(), node)? != root {
                return Err(V2WalError::Invalid(
                    "v2 WAL version root disagrees with its new node metadata",
                ));
            }
        }
    }
    Ok(())
}

fn validate_checkpoint(
    base: V2WalGeometry,
    next: V2WalGeometry,
    transaction: &V2WalTransaction,
) -> Result<(), V2WalError> {
    let encoded = encode_v2_checkpoint(&transaction.checkpoint)?;
    if decode_v2_checkpoint(&encoded, next.version_count)? != transaction.checkpoint {
        return Err(V2WalError::Invalid(
            "v2 WAL checkpoint does not round-trip canonically",
        ));
    }

    let identity = local_version_root(base, transaction, transaction.checkpoint.identity_version);
    let messages = match transaction.checkpoint.messages_version {
        Some(version) => local_version_root(base, transaction, version).map(Some),
        None => Some(None),
    };
    let result = match transaction.checkpoint.result_version {
        Some(version) => local_version_root(base, transaction, version).map(Some),
        None => Some(None),
    };
    if let (Some(identity), Some(messages), Some(result)) = (identity, messages, result) {
        let expected = checkpoint_state_metadata(identity, messages, result)?;
        if expected != transaction.checkpoint.state {
            return Err(V2WalError::Invalid(
                "v2 WAL checkpoint state commitment disagrees with local version roots",
            ));
        }
    }
    Ok(())
}

fn local_version_root(
    base: V2WalGeometry,
    transaction: &V2WalTransaction,
    version_id: u32,
) -> Option<V2RootRecord> {
    let version_id = u64::from(version_id);
    if version_id < base.version_count {
        return None;
    }
    let local = usize::try_from(version_id - base.version_count).ok()?;
    transaction.versions.get(local).map(|version| version.root())
}

fn version_id_for_local(base_version_count: u64, local: usize) -> Result<u32, V2WalError> {
    let id = base_version_count
        .checked_add(
            u64::try_from(local)
                .map_err(|_| V2WalError::Overflow("v2 WAL version index exceeds u64"))?,
        )
        .ok_or(V2WalError::Overflow(
            "v2 WAL version identifier exceeds u64",
        ))?;
    if id >= V2_NONE_VERSION {
        return Err(V2WalError::Invalid(
            "v2 WAL version identifier reaches the reserved none sentinel",
        ));
    }
    u32::try_from(id).map_err(|_| V2WalError::Overflow("v2 WAL version identifier exceeds u32"))
}

fn checked_total_len(
    payload_len: usize,
    node_count: usize,
    version_count: usize,
    checkpoint_len: usize,
) -> Result<usize, V2WalError> {
    let node_bytes = node_count
        .checked_mul(V2_WAL_NODE_RECORD_SIZE)
        .ok_or(V2WalError::Overflow("v2 WAL node table length exceeds usize"))?;
    let version_bytes = version_count
        .checked_mul(V2_WAL_VERSION_RECORD_SIZE)
        .ok_or(V2WalError::Overflow(
            "v2 WAL version table length exceeds usize",
        ))?;
    V2_WAL_HEADER_SIZE
        .checked_add(payload_len)
        .and_then(|value| value.checked_add(node_bytes))
        .and_then(|value| value.checked_add(version_bytes))
        .and_then(|value| value.checked_add(checkpoint_len))
        .ok_or(V2WalError::Overflow(
            "v2 WAL transaction length exceeds usize",
        ))
}

fn transaction_digest(header_prefix: &[u8], body: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(V2_WAL_DOMAIN);
    hasher.update(header_prefix);
    hasher.update(body);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn read_u32(bytes: &[u8], offset: usize, message: &'static str) -> Result<u32, V2WalError> {
    let end = offset
        .checked_add(4)
        .ok_or(V2WalError::Overflow("v2 WAL u32 range exceeds usize"))?;
    let encoded: [u8; 4] = bytes
        .get(offset..end)
        .ok_or(V2WalError::Invalid(message))?
        .try_into()
        .map_err(|_| V2WalError::Invalid(message))?;
    Ok(u32::from_le_bytes(encoded))
}

fn read_u64(bytes: &[u8], offset: usize, message: &'static str) -> Result<u64, V2WalError> {
    let end = offset
        .checked_add(8)
        .ok_or(V2WalError::Overflow("v2 WAL u64 range exceeds usize"))?;
    let encoded: [u8; 8] = bytes
        .get(offset..end)
        .ok_or(V2WalError::Invalid(message))?
        .try_into()
        .map_err(|_| V2WalError::Invalid(message))?;
    Ok(u64::from_le_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_initial_transaction() -> V2WalTransaction {
        let node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let root = V2RootRecord::from_node(0, node).unwrap();
        let version = V2VersionRecord::new(0, None, root).unwrap();
        let state = checkpoint_state_metadata(root, None, None).unwrap();
        V2WalTransaction {
            payload: b"abc".to_vec(),
            nodes: vec![node],
            versions: vec![version],
            checkpoint: V2CheckpointRecord {
                checkpoint_no: 1,
                thread_id: "thread".to_owned(),
                checkpoint_id: "cp-1".to_owned(),
                parent_checkpoint_id: None,
                identity_version: 0,
                messages_version: None,
                result_version: None,
                state,
            },
        }
    }

    fn rewrite_digest(bytes: &mut [u8]) {
        let digest = transaction_digest(
            &bytes[..V2_WAL_HEADER_PREFIX_SIZE],
            &bytes[V2_WAL_HEADER_SIZE..],
        );
        bytes[V2_WAL_DIGEST_OFFSET..V2_WAL_HEADER_SIZE].copy_from_slice(&digest);
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
    fn append_transaction_round_trip_freezes_geometry_and_digest() {
        let base = V2WalGeometry::default();
        let transaction = simple_initial_transaction();
        let encoded = encode_v2_wal_transaction(base, &transaction).unwrap();
        let decoded = decode_v2_wal_transaction(&encoded, base).unwrap();
        assert_eq!(decoded.transaction, transaction);
        assert_eq!(
            decoded.next_geometry,
            V2WalGeometry {
                payload_len: 3,
                node_count: 1,
                version_count: 1,
                checkpoint_count: 1,
            }
        );
        assert_eq!(encoded.len(), 365);
        assert_eq!(
            hex(&decoded.digest),
            "185d3f5767c8e1fc76a457b52d3620eb4dd4357176b52270adf4a58095a36227"
        );
    }

    #[test]
    fn zero_delta_checkpoint_fork_advances_only_checkpoint_watermark() {
        let old_node = V2NodeRecord::leaf(0, b"abc").unwrap();
        let old_root = V2RootRecord::from_node(0, old_node).unwrap();
        let state = checkpoint_state_metadata(old_root, None, None).unwrap();
        let base = V2WalGeometry {
            payload_len: 3,
            node_count: 1,
            version_count: 1,
            checkpoint_count: 1,
        };
        let transaction = V2WalTransaction {
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
                state,
            },
        };
        let encoded = encode_v2_wal_transaction(base, &transaction).unwrap();
        let decoded = decode_v2_wal_transaction(&encoded, base).unwrap();
        assert!(decoded.transaction.payload.is_empty());
        assert!(decoded.transaction.nodes.is_empty());
        assert!(decoded.transaction.versions.is_empty());
        assert_eq!(
            decoded.next_geometry,
            V2WalGeometry {
                checkpoint_count: 2,
                ..base
            }
        );
    }

    #[test]
    fn decoder_rejects_torn_digest_and_wrong_starting_geometry() {
        let base = V2WalGeometry::default();
        let transaction = simple_initial_transaction();
        let encoded = encode_v2_wal_transaction(base, &transaction).unwrap();
        assert_eq!(
            decode_v2_wal_transaction(&encoded[..encoded.len() - 1], base),
            Err(V2WalError::Invalid("v2 WAL transaction length mismatch"))
        );

        let mut corrupt = encoded.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert_eq!(
            decode_v2_wal_transaction(&corrupt, base),
            Err(V2WalError::Invalid("v2 WAL digest mismatch"))
        );

        assert_eq!(
            decode_v2_wal_transaction(
                &encoded,
                V2WalGeometry {
                    checkpoint_count: 1,
                    ..base
                }
            ),
            Err(V2WalError::Invalid(
                "v2 WAL transaction starting watermark mismatch"
            ))
        );
    }

    #[test]
    fn valid_digest_cannot_hide_nonlocal_leaf_or_version_topology() {
        let base = V2WalGeometry::default();
        let transaction = simple_initial_transaction();
        let encoded = encode_v2_wal_transaction(base, &transaction).unwrap();

        let mut bad_leaf = encoded.clone();
        let node_offset = V2_WAL_HEADER_SIZE + transaction.payload.len();
        bad_leaf[node_offset + 16..node_offset + 24].copy_from_slice(&1u64.to_le_bytes());
        rewrite_digest(&mut bad_leaf);
        assert_eq!(
            decode_v2_wal_transaction(&bad_leaf, base),
            Err(V2WalError::Invalid(
                "v2 WAL new leaf references bytes outside the payload delta"
            ))
        );

        let mut bad_version = encoded;
        let version_offset = node_offset + V2_WAL_NODE_RECORD_SIZE;
        bad_version[version_offset + 4..version_offset + 8].copy_from_slice(&1u32.to_le_bytes());
        rewrite_digest(&mut bad_version);
        assert_eq!(
            decode_v2_wal_transaction(&bad_version, base),
            Err(V2WalError::Publication(V2PublicationError::Invalid(
                "v2 version id is not sequential"
            )))
        );
    }

    #[test]
    fn encoder_rejects_payload_gaps_and_forward_branch_references() {
        let first = V2NodeRecord::leaf(0, b"a").unwrap();
        let second = V2NodeRecord::leaf(2, b"c").unwrap();
        let root = V2RootRecord::from_node(1, second).unwrap();
        let state = checkpoint_state_metadata(root, None, None).unwrap();
        let gap = V2WalTransaction {
            payload: b"abc".to_vec(),
            nodes: vec![first, second],
            versions: vec![V2VersionRecord::new(0, None, root).unwrap()],
            checkpoint: V2CheckpointRecord {
                checkpoint_no: 1,
                thread_id: "t".to_owned(),
                checkpoint_id: "c".to_owned(),
                parent_checkpoint_id: None,
                identity_version: 0,
                messages_version: None,
                result_version: None,
                state,
            },
        };
        assert_eq!(
            encode_v2_wal_transaction(V2WalGeometry::default(), &gap),
            Err(V2WalError::Invalid(
                "v2 WAL new leaves do not contiguously own the payload delta"
            ))
        );

        let second_local = V2NodeRecord::leaf(1, b"c").unwrap();
        let left = V2RootRecord::from_node(9, first).unwrap();
        let right = V2RootRecord::from_node(10, second_local).unwrap();
        let branch = V2NodeRecord::branch(left, right).unwrap();
        let branch_root = V2RootRecord::from_node(2, branch).unwrap();
        let branch_state = checkpoint_state_metadata(branch_root, None, None).unwrap();
        let forward = V2WalTransaction {
            payload: b"ac".to_vec(),
            nodes: vec![first, second_local, branch],
            versions: vec![V2VersionRecord::new(0, None, branch_root).unwrap()],
            checkpoint: V2CheckpointRecord {
                checkpoint_no: 1,
                thread_id: "t".to_owned(),
                checkpoint_id: "c".to_owned(),
                parent_checkpoint_id: None,
                identity_version: 0,
                messages_version: None,
                result_version: None,
                state: branch_state,
            },
        };
        assert_eq!(
            encode_v2_wal_transaction(V2WalGeometry::default(), &forward),
            Err(V2WalError::Invalid(
                "v2 WAL branch child is not topologically prior"
            ))
        );
    }

    #[test]
    fn schema_record_widths_match_nested_codecs() {
        let transaction = simple_initial_transaction();
        assert_eq!(encode_v2_node(transaction.nodes[0]).len(), V2_WAL_NODE_RECORD_SIZE);
        assert_eq!(
            encode_v2_version(transaction.versions[0]).unwrap().len(),
            V2_WAL_VERSION_RECORD_SIZE
        );
    }
}
