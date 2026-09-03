//! Canonical internal image for the Format-v2 persistent AVL arena.
//!
//! This is a staging persistence boundary for the balanced sequence core. It
//! is not the public checkpoint-store format and is not yet wired into the WAL,
//! manifest, or sealed-segment lifecycle.

use super::format_v2::{
    decode_v2_node, decode_v2_root, encode_v2_node, encode_v2_root, V2FormatError, V2NodeRecord,
    V2RootRecord,
};
use sha2::{Digest, Sha256};
use std::fmt;

const V2_IMAGE_MAGIC: [u8; 4] = *b"T2I2";
const V2_IMAGE_VERSION: u32 = 2;
const V2_IMAGE_HEADER_SIZE: usize = 72;
const V2_IMAGE_PREFIX_SIZE: usize = 40;
const V2_IMAGE_DIGEST_OFFSET: usize = 40;
const V2_IMAGE_DIGEST_SIZE: usize = 32;
const V2_IMAGE_MAX_RECORD_SIZE: u32 = 4096;
const V2_IMAGE_DOMAIN: &[u8] = b"tulya-sequence-v2/image\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2ImageError {
    Format(V2FormatError),
    Invalid(&'static str),
    Overflow(&'static str),
}

impl fmt::Display for V2ImageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for V2ImageError {}

impl From<V2FormatError> for V2ImageError {
    fn from(error: V2FormatError) -> Self {
        Self::Format(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2SequenceImage {
    pub(super) payload: Vec<u8>,
    pub(super) nodes: Vec<V2NodeRecord>,
    pub(super) roots: Vec<V2RootRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2NodeFields {
    Leaf {
        payload_offset: u64,
        payload_len: u64,
    },
    Branch {
        left_node_id: u64,
        right_node_id: u64,
        left_len: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V2ImageHeader {
    payload_len: u64,
    node_count: u64,
    root_count: u64,
    node_record_size: u32,
    root_record_size: u32,
}

pub(super) fn encode_v2_image(image: &V2SequenceImage) -> Result<Vec<u8>, V2ImageError> {
    if image.payload.is_empty() {
        return Err(V2ImageError::Invalid("v2 image payload must be non-empty"));
    }
    if image.nodes.is_empty() {
        return Err(V2ImageError::Invalid(
            "v2 image node table must be non-empty",
        ));
    }
    if image.roots.is_empty() {
        return Err(V2ImageError::Invalid(
            "v2 image root table must be non-empty",
        ));
    }

    let node_record_size = u32::try_from(encode_v2_node(image.nodes[0]).len())
        .map_err(|_| V2ImageError::Overflow("v2 image node record size exceeds u32"))?;
    let root_record_size = u32::try_from(encode_v2_root(image.roots[0]).len())
        .map_err(|_| V2ImageError::Overflow("v2 image root record size exceeds u32"))?;
    validate_record_size(
        node_record_size,
        "v2 image node record size is outside bounds",
    )?;
    validate_record_size(
        root_record_size,
        "v2 image root record size is outside bounds",
    )?;

    let header = V2ImageHeader {
        payload_len: u64::try_from(image.payload.len())
            .map_err(|_| V2ImageError::Overflow("v2 image payload length exceeds u64"))?,
        node_count: u64::try_from(image.nodes.len())
            .map_err(|_| V2ImageError::Overflow("v2 image node count exceeds u64"))?,
        root_count: u64::try_from(image.roots.len())
            .map_err(|_| V2ImageError::Overflow("v2 image root count exceeds u64"))?,
        node_record_size,
        root_record_size,
    };
    let total_len = checked_total_len(header)?;
    let mut output = Vec::with_capacity(total_len);
    output.resize(V2_IMAGE_HEADER_SIZE, 0);
    encode_header_prefix(header, &mut output[..V2_IMAGE_PREFIX_SIZE]);
    output.extend_from_slice(&image.payload);

    for node in &image.nodes {
        let encoded = encode_v2_node(*node);
        if encoded.len()
            != usize::try_from(node_record_size)
                .map_err(|_| V2ImageError::Overflow("v2 image node record size exceeds usize"))?
        {
            return Err(V2ImageError::Invalid(
                "v2 image node records do not have one canonical width",
            ));
        }
        output.extend_from_slice(&encoded);
    }
    for root in &image.roots {
        let encoded = encode_v2_root(*root);
        if encoded.len()
            != usize::try_from(root_record_size)
                .map_err(|_| V2ImageError::Overflow("v2 image root record size exceeds usize"))?
        {
            return Err(V2ImageError::Invalid(
                "v2 image root records do not have one canonical width",
            ));
        }
        output.extend_from_slice(&encoded);
    }

    if output.len() != total_len {
        return Err(V2ImageError::Invalid(
            "v2 image encoder produced an unexpected byte length",
        ));
    }
    let digest = image_digest(
        &output[..V2_IMAGE_PREFIX_SIZE],
        &output[V2_IMAGE_HEADER_SIZE..],
    );
    output[V2_IMAGE_DIGEST_OFFSET..V2_IMAGE_HEADER_SIZE].copy_from_slice(&digest);
    Ok(output)
}

pub(super) fn decode_v2_image(bytes: &[u8]) -> Result<V2SequenceImage, V2ImageError> {
    let header = decode_header(bytes)?;
    let expected_len = checked_total_len(header)?;
    if bytes.len() != expected_len {
        return Err(V2ImageError::Invalid("v2 image byte length mismatch"));
    }

    let stored_digest: [u8; V2_IMAGE_DIGEST_SIZE] = bytes
        .get(V2_IMAGE_DIGEST_OFFSET..V2_IMAGE_HEADER_SIZE)
        .ok_or(V2ImageError::Invalid("v2 image digest is truncated"))?
        .try_into()
        .map_err(|_| V2ImageError::Invalid("v2 image digest width mismatch"))?;
    let expected_digest = image_digest(
        &bytes[..V2_IMAGE_PREFIX_SIZE],
        &bytes[V2_IMAGE_HEADER_SIZE..],
    );
    if stored_digest != expected_digest {
        return Err(V2ImageError::Invalid("v2 image digest mismatch"));
    }

    let payload_len = usize::try_from(header.payload_len)
        .map_err(|_| V2ImageError::Overflow("v2 image payload length exceeds usize"))?;
    let node_count = usize::try_from(header.node_count)
        .map_err(|_| V2ImageError::Overflow("v2 image node count exceeds usize"))?;
    let root_count = usize::try_from(header.root_count)
        .map_err(|_| V2ImageError::Overflow("v2 image root count exceeds usize"))?;
    let node_record_size = usize::try_from(header.node_record_size)
        .map_err(|_| V2ImageError::Overflow("v2 image node record size exceeds usize"))?;
    let root_record_size = usize::try_from(header.root_record_size)
        .map_err(|_| V2ImageError::Overflow("v2 image root record size exceeds usize"))?;

    let mut cursor = V2_IMAGE_HEADER_SIZE;
    let payload_end = cursor
        .checked_add(payload_len)
        .ok_or(V2ImageError::Overflow(
            "v2 image payload range exceeds usize",
        ))?;
    let payload = bytes
        .get(cursor..payload_end)
        .ok_or(V2ImageError::Invalid("v2 image payload is truncated"))?
        .to_vec();
    cursor = payload_end;

    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        let end = cursor
            .checked_add(node_record_size)
            .ok_or(V2ImageError::Overflow("v2 image node range exceeds usize"))?;
        let record = decode_v2_node(
            bytes
                .get(cursor..end)
                .ok_or(V2ImageError::Invalid("v2 image node table is truncated"))?,
        )?;
        nodes.push(record);
        cursor = end;
    }

    let mut roots = Vec::with_capacity(root_count);
    for _ in 0..root_count {
        let end = cursor
            .checked_add(root_record_size)
            .ok_or(V2ImageError::Overflow("v2 image root range exceeds usize"))?;
        let root = decode_v2_root(
            bytes
                .get(cursor..end)
                .ok_or(V2ImageError::Invalid("v2 image root table is truncated"))?,
        )?;
        roots.push(root);
        cursor = end;
    }
    if cursor != bytes.len() {
        return Err(V2ImageError::Invalid(
            "v2 image decoder did not consume all bytes",
        ));
    }

    Ok(V2SequenceImage {
        payload,
        nodes,
        roots,
    })
}

pub(super) fn v2_node_fields(record: V2NodeRecord) -> Result<V2NodeFields, V2ImageError> {
    let encoded = encode_v2_node(record);
    let field_a = read_u64(&encoded, 16, "v2 image node field A is truncated")?;
    let field_b = read_u64(&encoded, 24, "v2 image node field B is truncated")?;
    if record.height() == 1 {
        Ok(V2NodeFields::Leaf {
            payload_offset: field_a,
            payload_len: field_b,
        })
    } else {
        let left_len = read_u64(&encoded, 32, "v2 image node field C is truncated")?;
        Ok(V2NodeFields::Branch {
            left_node_id: field_a,
            right_node_id: field_b,
            left_len,
        })
    }
}

fn decode_header(bytes: &[u8]) -> Result<V2ImageHeader, V2ImageError> {
    if bytes.len() < V2_IMAGE_HEADER_SIZE {
        return Err(V2ImageError::Invalid("v2 image header is truncated"));
    }
    if bytes
        .get(0..4)
        .ok_or(V2ImageError::Invalid("v2 image magic is truncated"))?
        != V2_IMAGE_MAGIC
    {
        return Err(V2ImageError::Invalid("v2 image magic mismatch"));
    }
    let version = read_u32(bytes, 4, "v2 image version is truncated")?;
    if version != V2_IMAGE_VERSION {
        return Err(V2ImageError::Invalid("v2 image version is unsupported"));
    }
    let header = V2ImageHeader {
        payload_len: read_u64(bytes, 8, "v2 image payload length is truncated")?,
        node_count: read_u64(bytes, 16, "v2 image node count is truncated")?,
        root_count: read_u64(bytes, 24, "v2 image root count is truncated")?,
        node_record_size: read_u32(bytes, 32, "v2 image node record size is truncated")?,
        root_record_size: read_u32(bytes, 36, "v2 image root record size is truncated")?,
    };
    if header.payload_len == 0 {
        return Err(V2ImageError::Invalid("v2 image payload must be non-empty"));
    }
    if header.node_count == 0 {
        return Err(V2ImageError::Invalid(
            "v2 image node table must be non-empty",
        ));
    }
    if header.root_count == 0 {
        return Err(V2ImageError::Invalid(
            "v2 image root table must be non-empty",
        ));
    }
    validate_record_size(
        header.node_record_size,
        "v2 image node record size is outside bounds",
    )?;
    validate_record_size(
        header.root_record_size,
        "v2 image root record size is outside bounds",
    )?;
    Ok(header)
}

fn checked_total_len(header: V2ImageHeader) -> Result<usize, V2ImageError> {
    let payload_len = usize::try_from(header.payload_len)
        .map_err(|_| V2ImageError::Overflow("v2 image payload length exceeds usize"))?;
    let node_count = usize::try_from(header.node_count)
        .map_err(|_| V2ImageError::Overflow("v2 image node count exceeds usize"))?;
    let root_count = usize::try_from(header.root_count)
        .map_err(|_| V2ImageError::Overflow("v2 image root count exceeds usize"))?;
    let node_record_size = usize::try_from(header.node_record_size)
        .map_err(|_| V2ImageError::Overflow("v2 image node record size exceeds usize"))?;
    let root_record_size = usize::try_from(header.root_record_size)
        .map_err(|_| V2ImageError::Overflow("v2 image root record size exceeds usize"))?;
    let node_bytes = node_count
        .checked_mul(node_record_size)
        .ok_or(V2ImageError::Overflow(
            "v2 image node table length exceeds usize",
        ))?;
    let root_bytes = root_count
        .checked_mul(root_record_size)
        .ok_or(V2ImageError::Overflow(
            "v2 image root table length exceeds usize",
        ))?;
    V2_IMAGE_HEADER_SIZE
        .checked_add(payload_len)
        .and_then(|value| value.checked_add(node_bytes))
        .and_then(|value| value.checked_add(root_bytes))
        .ok_or(V2ImageError::Overflow(
            "v2 image total length exceeds usize",
        ))
}

fn validate_record_size(size: u32, message: &'static str) -> Result<(), V2ImageError> {
    if size == 0 || size > V2_IMAGE_MAX_RECORD_SIZE {
        return Err(V2ImageError::Invalid(message));
    }
    Ok(())
}

fn encode_header_prefix(header: V2ImageHeader, output: &mut [u8]) {
    output[0..4].copy_from_slice(&V2_IMAGE_MAGIC);
    output[4..8].copy_from_slice(&V2_IMAGE_VERSION.to_le_bytes());
    output[8..16].copy_from_slice(&header.payload_len.to_le_bytes());
    output[16..24].copy_from_slice(&header.node_count.to_le_bytes());
    output[24..32].copy_from_slice(&header.root_count.to_le_bytes());
    output[32..36].copy_from_slice(&header.node_record_size.to_le_bytes());
    output[36..40].copy_from_slice(&header.root_record_size.to_le_bytes());
}

fn image_digest(prefix: &[u8], body: &[u8]) -> [u8; V2_IMAGE_DIGEST_SIZE] {
    let mut hasher = Sha256::new();
    hasher.update(V2_IMAGE_DOMAIN);
    hasher.update(prefix);
    hasher.update(body);
    let digest = hasher.finalize();
    let mut output = [0u8; V2_IMAGE_DIGEST_SIZE];
    output.copy_from_slice(&digest);
    output
}

fn read_u32(bytes: &[u8], offset: usize, message: &'static str) -> Result<u32, V2ImageError> {
    let end = offset
        .checked_add(4)
        .ok_or(V2ImageError::Overflow("v2 image u32 range exceeds usize"))?;
    let encoded: [u8; 4] = bytes
        .get(offset..end)
        .ok_or(V2ImageError::Invalid(message))?
        .try_into()
        .map_err(|_| V2ImageError::Invalid(message))?;
    Ok(u32::from_le_bytes(encoded))
}

fn read_u64(bytes: &[u8], offset: usize, message: &'static str) -> Result<u64, V2ImageError> {
    let end = offset
        .checked_add(8)
        .ok_or(V2ImageError::Overflow("v2 image u64 range exceeds usize"))?;
    let encoded: [u8; 8] = bytes
        .get(offset..end)
        .ok_or(V2ImageError::Invalid(message))?
        .try_into()
        .map_err(|_| V2ImageError::Invalid(message))?;
    Ok(u64::from_le_bytes(encoded))
}

#[cfg(test)]
pub(super) fn corrupt_first_branch_child_for_test(
    bytes: &mut [u8],
    new_child_id: u64,
) -> Result<(), V2ImageError> {
    let header = decode_header(bytes)?;
    let payload_len = usize::try_from(header.payload_len)
        .map_err(|_| V2ImageError::Overflow("v2 image payload length exceeds usize"))?;
    let node_count = usize::try_from(header.node_count)
        .map_err(|_| V2ImageError::Overflow("v2 image node count exceeds usize"))?;
    let node_record_size = usize::try_from(header.node_record_size)
        .map_err(|_| V2ImageError::Overflow("v2 image node record size exceeds usize"))?;
    let mut cursor =
        V2_IMAGE_HEADER_SIZE
            .checked_add(payload_len)
            .ok_or(V2ImageError::Overflow(
                "v2 image node table offset exceeds usize",
            ))?;
    for _ in 0..node_count {
        let end = cursor
            .checked_add(node_record_size)
            .ok_or(V2ImageError::Overflow("v2 image node range exceeds usize"))?;
        let record = decode_v2_node(
            bytes
                .get(cursor..end)
                .ok_or(V2ImageError::Invalid("v2 image node table is truncated"))?,
        )?;
        if record.height() > 1 {
            let child_end = cursor.checked_add(24).ok_or(V2ImageError::Overflow(
                "v2 image branch field exceeds usize",
            ))?;
            let child_start = cursor.checked_add(16).ok_or(V2ImageError::Overflow(
                "v2 image branch field exceeds usize",
            ))?;
            bytes
                .get_mut(child_start..child_end)
                .ok_or(V2ImageError::Invalid("v2 image branch field is truncated"))?
                .copy_from_slice(&new_child_id.to_le_bytes());
            refresh_digest_for_test(bytes)?;
            return Ok(());
        }
        cursor = end;
    }
    Err(V2ImageError::Invalid(
        "v2 image contains no branch to corrupt",
    ))
}

#[cfg(test)]
fn refresh_digest_for_test(bytes: &mut [u8]) -> Result<(), V2ImageError> {
    decode_header(bytes)?;
    let digest = image_digest(
        &bytes[..V2_IMAGE_PREFIX_SIZE],
        &bytes[V2_IMAGE_HEADER_SIZE..],
    );
    bytes[V2_IMAGE_DIGEST_OFFSET..V2_IMAGE_HEADER_SIZE].copy_from_slice(&digest);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_image() -> V2SequenceImage {
        let left_node = V2NodeRecord::leaf(0, b"abc").expect("left leaf should be valid");
        let right_node = V2NodeRecord::leaf(3, b"XYZ").expect("right leaf should be valid");
        let left_root = V2RootRecord::from_node(0, left_node).expect("left root should be valid");
        let right_root =
            V2RootRecord::from_node(1, right_node).expect("right root should be valid");
        let branch = V2NodeRecord::branch(left_root, right_root).expect("branch should be valid");
        let root = V2RootRecord::from_node(2, branch).expect("root should be valid");
        V2SequenceImage {
            payload: b"abcXYZ".to_vec(),
            nodes: vec![left_node, right_node, branch],
            roots: vec![left_root, root],
        }
    }

    #[test]
    fn image_codec_round_trip_preserves_payload_nodes_and_roots() {
        let image = sample_image();
        let encoded = encode_v2_image(&image).expect("image encoding should succeed");
        assert_eq!(decode_v2_image(&encoded), Ok(image));
    }

    #[test]
    fn image_decoder_rejects_truncation_and_digest_corruption() {
        let image = sample_image();
        let encoded = encode_v2_image(&image).expect("image encoding should succeed");
        assert_eq!(
            decode_v2_image(&encoded[..encoded.len() - 1]),
            Err(V2ImageError::Invalid("v2 image byte length mismatch"))
        );

        let mut corrupt = encoded;
        corrupt[V2_IMAGE_HEADER_SIZE] ^= 0x01;
        assert_eq!(
            decode_v2_image(&corrupt),
            Err(V2ImageError::Invalid("v2 image digest mismatch"))
        );
    }

    #[test]
    fn node_field_projection_uses_canonical_v2_record_encoding() {
        let image = sample_image();
        assert_eq!(
            v2_node_fields(image.nodes[0]).expect("leaf projection should succeed"),
            V2NodeFields::Leaf {
                payload_offset: 0,
                payload_len: 3,
            }
        );
        assert_eq!(
            v2_node_fields(image.nodes[2]).expect("branch projection should succeed"),
            V2NodeFields::Branch {
                left_node_id: 0,
                right_node_id: 1,
                left_len: 3,
            }
        );
    }
}
