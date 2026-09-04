//! Canonical Format-v2 metadata codec for the balanced persistent sequence.
//!
//! This module defines only the new sequence/root records and their commitment
//! construction. It does not publish Format v2 or route production writes to
//! it yet; the current public store format remains v1 until migration and
//! publication semantics land.

use sha2::{Digest, Sha256};
use std::fmt;

const V2_NODE_MAGIC: [u8; 4] = *b"T2N2";
const V2_ROOT_MAGIC: [u8; 4] = *b"T2R2";
const V2_NODE_RECORD_SIZE: usize = 72;
const V2_ROOT_RECORD_SIZE: usize = 56;
const V2_ROOT_REPRESENTATION_BALANCED: u8 = 1;
const V2_NODE_KIND_LEAF: u8 = 1;
const V2_NODE_KIND_BRANCH: u8 = 2;
const V2_NONE_NODE: u64 = u64::MAX;
const V2_LEAF_DOMAIN: &[u8] = b"tulya-sequence-v2/leaf\0";
const V2_BRANCH_DOMAIN: &[u8] = b"tulya-sequence-v2/branch\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct V2Commitment([u8; 32]);

impl V2Commitment {
    pub(super) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(super) const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2FormatError {
    RecordLength {
        record: &'static str,
        expected: usize,
        actual: usize,
    },
    Invalid(&'static str),
    Overflow(&'static str),
}

impl fmt::Display for V2FormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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

impl std::error::Error for V2FormatError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V2NodeBody {
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
pub(super) struct V2NodeRecord {
    height: u16,
    logical_len: u64,
    commitment: V2Commitment,
    body: V2NodeBody,
}

impl V2NodeRecord {
    pub(super) fn leaf(payload_offset: u64, payload: &[u8]) -> Result<Self, V2FormatError> {
        if payload.is_empty() {
            return Err(V2FormatError::Invalid(
                "v2 sequence leaf payload must be non-empty",
            ));
        }
        let logical_len = u64::try_from(payload.len())
            .map_err(|_| V2FormatError::Overflow("v2 sequence leaf payload length exceeds u64"))?;
        payload_offset
            .checked_add(logical_len)
            .ok_or(V2FormatError::Overflow(
                "v2 sequence leaf payload range exceeds u64",
            ))?;
        Ok(Self {
            height: 1,
            logical_len,
            commitment: leaf_commitment(logical_len, payload),
            body: V2NodeBody::Leaf {
                payload_offset,
                payload_len: logical_len,
            },
        })
    }

    pub(super) fn branch(left: V2RootRecord, right: V2RootRecord) -> Result<Self, V2FormatError> {
        validate_child_root(left)?;
        validate_child_root(right)?;
        if left.height.abs_diff(right.height) > 1 {
            return Err(V2FormatError::Invalid(
                "v2 sequence branch children are not AVL-balanced",
            ));
        }
        let height =
            left.height
                .max(right.height)
                .checked_add(1)
                .ok_or(V2FormatError::Overflow(
                    "v2 sequence branch height exceeds u16",
                ))?;
        let logical_len =
            left.logical_len
                .checked_add(right.logical_len)
                .ok_or(V2FormatError::Overflow(
                    "v2 sequence branch logical length exceeds u64",
                ))?;
        Ok(Self {
            height,
            logical_len,
            commitment: branch_commitment(
                height,
                left.logical_len,
                left.commitment,
                right.logical_len,
                right.commitment,
            ),
            body: V2NodeBody::Branch {
                left_node_id: left.node_id,
                right_node_id: right.node_id,
                left_len: left.logical_len,
            },
        })
    }

    pub(super) const fn height(self) -> u16 {
        self.height
    }

    pub(super) const fn logical_len(self) -> u64 {
        self.logical_len
    }

    pub(super) const fn commitment(self) -> V2Commitment {
        self.commitment
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct V2RootRecord {
    node_id: u64,
    logical_len: u64,
    height: u16,
    commitment: V2Commitment,
}

impl V2RootRecord {
    pub(super) fn from_node(node_id: u64, node: V2NodeRecord) -> Result<Self, V2FormatError> {
        if node_id == V2_NONE_NODE {
            return Err(V2FormatError::Invalid(
                "v2 sequence root uses the reserved none-node identifier",
            ));
        }
        Ok(Self {
            node_id,
            logical_len: node.logical_len,
            height: node.height,
            commitment: node.commitment,
        })
    }

    pub(super) const fn node_id(self) -> u64 {
        self.node_id
    }

    pub(super) const fn logical_len(self) -> u64 {
        self.logical_len
    }

    pub(super) const fn height(self) -> u16 {
        self.height
    }

    pub(super) const fn commitment(self) -> V2Commitment {
        self.commitment
    }
}

pub(super) fn encode_v2_node(record: V2NodeRecord) -> [u8; V2_NODE_RECORD_SIZE] {
    let mut output = [0u8; V2_NODE_RECORD_SIZE];
    output[0..4].copy_from_slice(&V2_NODE_MAGIC);
    match record.body {
        V2NodeBody::Leaf {
            payload_offset,
            payload_len,
        } => {
            output[4] = V2_NODE_KIND_LEAF;
            output[6..8].copy_from_slice(&record.height.to_le_bytes());
            output[8..16].copy_from_slice(&record.logical_len.to_le_bytes());
            output[16..24].copy_from_slice(&payload_offset.to_le_bytes());
            output[24..32].copy_from_slice(&payload_len.to_le_bytes());
        }
        V2NodeBody::Branch {
            left_node_id,
            right_node_id,
            left_len,
        } => {
            output[4] = V2_NODE_KIND_BRANCH;
            output[6..8].copy_from_slice(&record.height.to_le_bytes());
            output[8..16].copy_from_slice(&record.logical_len.to_le_bytes());
            output[16..24].copy_from_slice(&left_node_id.to_le_bytes());
            output[24..32].copy_from_slice(&right_node_id.to_le_bytes());
            output[32..40].copy_from_slice(&left_len.to_le_bytes());
        }
    }
    output[40..72].copy_from_slice(record.commitment.as_bytes());
    output
}

pub(super) fn decode_v2_node(bytes: &[u8]) -> Result<V2NodeRecord, V2FormatError> {
    require_record_len("v2 node", bytes, V2_NODE_RECORD_SIZE)?;
    if read_array::<4>(bytes, 0, "v2 node magic is truncated")? != V2_NODE_MAGIC {
        return Err(V2FormatError::Invalid("v2 node magic mismatch"));
    }
    let kind = read_byte(bytes, 4, "v2 node kind is truncated")?;
    let flags = read_byte(bytes, 5, "v2 node flags are truncated")?;
    if flags != 0 {
        return Err(V2FormatError::Invalid("v2 node flags must be zero"));
    }
    let height = u16::from_le_bytes(read_array::<2>(bytes, 6, "v2 node height is truncated")?);
    let logical_len = u64::from_le_bytes(read_array::<8>(
        bytes,
        8,
        "v2 node logical length is truncated",
    )?);
    let field_a = u64::from_le_bytes(read_array::<8>(bytes, 16, "v2 node field A is truncated")?);
    let field_b = u64::from_le_bytes(read_array::<8>(bytes, 24, "v2 node field B is truncated")?);
    let field_c = u64::from_le_bytes(read_array::<8>(bytes, 32, "v2 node field C is truncated")?);
    let commitment = V2Commitment::from_bytes(read_array::<32>(
        bytes,
        40,
        "v2 node commitment is truncated",
    )?);

    match kind {
        V2_NODE_KIND_LEAF => {
            if height != 1 {
                return Err(V2FormatError::Invalid("v2 leaf height must be one"));
            }
            if logical_len == 0 {
                return Err(V2FormatError::Invalid(
                    "v2 leaf logical length must be positive",
                ));
            }
            if field_b != logical_len {
                return Err(V2FormatError::Invalid(
                    "v2 leaf payload length disagrees with logical length",
                ));
            }
            if field_c != 0 {
                return Err(V2FormatError::Invalid(
                    "v2 leaf reserved field must be zero",
                ));
            }
            field_a
                .checked_add(field_b)
                .ok_or(V2FormatError::Overflow("v2 leaf payload range exceeds u64"))?;
            Ok(V2NodeRecord {
                height,
                logical_len,
                commitment,
                body: V2NodeBody::Leaf {
                    payload_offset: field_a,
                    payload_len: field_b,
                },
            })
        }
        V2_NODE_KIND_BRANCH => {
            if height < 2 {
                return Err(V2FormatError::Invalid(
                    "v2 branch height must be at least two",
                ));
            }
            if field_a == V2_NONE_NODE || field_b == V2_NONE_NODE {
                return Err(V2FormatError::Invalid(
                    "v2 branch uses the reserved none-node identifier",
                ));
            }
            if field_c == 0 || field_c >= logical_len {
                return Err(V2FormatError::Invalid(
                    "v2 branch left length must split two non-empty children",
                ));
            }
            Ok(V2NodeRecord {
                height,
                logical_len,
                commitment,
                body: V2NodeBody::Branch {
                    left_node_id: field_a,
                    right_node_id: field_b,
                    left_len: field_c,
                },
            })
        }
        _ => Err(V2FormatError::Invalid("v2 node kind is unsupported")),
    }
}

pub(super) fn encode_v2_root(record: V2RootRecord) -> [u8; V2_ROOT_RECORD_SIZE] {
    let mut output = [0u8; V2_ROOT_RECORD_SIZE];
    output[0..4].copy_from_slice(&V2_ROOT_MAGIC);
    output[4] = V2_ROOT_REPRESENTATION_BALANCED;
    output[6..8].copy_from_slice(&record.height.to_le_bytes());
    output[8..16].copy_from_slice(&record.node_id.to_le_bytes());
    output[16..24].copy_from_slice(&record.logical_len.to_le_bytes());
    output[24..56].copy_from_slice(record.commitment.as_bytes());
    output
}

pub(super) fn decode_v2_root(bytes: &[u8]) -> Result<V2RootRecord, V2FormatError> {
    require_record_len("v2 root", bytes, V2_ROOT_RECORD_SIZE)?;
    if read_array::<4>(bytes, 0, "v2 root magic is truncated")? != V2_ROOT_MAGIC {
        return Err(V2FormatError::Invalid("v2 root magic mismatch"));
    }
    let representation = read_byte(bytes, 4, "v2 root representation is truncated")?;
    if representation != V2_ROOT_REPRESENTATION_BALANCED {
        return Err(V2FormatError::Invalid(
            "v2 root representation is unsupported",
        ));
    }
    let flags = read_byte(bytes, 5, "v2 root flags are truncated")?;
    if flags != 0 {
        return Err(V2FormatError::Invalid("v2 root flags must be zero"));
    }
    let height = u16::from_le_bytes(read_array::<2>(bytes, 6, "v2 root height is truncated")?);
    let node_id = u64::from_le_bytes(read_array::<8>(
        bytes,
        8,
        "v2 root node identifier is truncated",
    )?);
    let logical_len = u64::from_le_bytes(read_array::<8>(
        bytes,
        16,
        "v2 root logical length is truncated",
    )?);
    let commitment = V2Commitment::from_bytes(read_array::<32>(
        bytes,
        24,
        "v2 root commitment is truncated",
    )?);
    let root = V2RootRecord {
        node_id,
        logical_len,
        height,
        commitment,
    };
    validate_child_root(root)?;
    Ok(root)
}

fn validate_child_root(root: V2RootRecord) -> Result<(), V2FormatError> {
    if root.node_id == V2_NONE_NODE {
        return Err(V2FormatError::Invalid(
            "v2 root uses the reserved none-node identifier",
        ));
    }
    if root.logical_len == 0 {
        return Err(V2FormatError::Invalid(
            "v2 root logical length must be positive",
        ));
    }
    if root.height == 0 {
        return Err(V2FormatError::Invalid("v2 root height must be positive"));
    }
    Ok(())
}

fn leaf_commitment(logical_len: u64, payload: &[u8]) -> V2Commitment {
    let mut hasher = Sha256::new();
    hasher.update(V2_LEAF_DOMAIN);
    hasher.update(logical_len.to_le_bytes());
    hasher.update(payload);
    finish_commitment(hasher)
}

fn branch_commitment(
    height: u16,
    left_len: u64,
    left: V2Commitment,
    right_len: u64,
    right: V2Commitment,
) -> V2Commitment {
    let mut hasher = Sha256::new();
    hasher.update(V2_BRANCH_DOMAIN);
    hasher.update(height.to_le_bytes());
    hasher.update(left_len.to_le_bytes());
    hasher.update(left.as_bytes());
    hasher.update(right_len.to_le_bytes());
    hasher.update(right.as_bytes());
    finish_commitment(hasher)
}

fn finish_commitment(hasher: Sha256) -> V2Commitment {
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    V2Commitment(output)
}

fn require_record_len(
    record: &'static str,
    bytes: &[u8],
    expected: usize,
) -> Result<(), V2FormatError> {
    if bytes.len() != expected {
        return Err(V2FormatError::RecordLength {
            record,
            expected,
            actual: bytes.len(),
        });
    }
    Ok(())
}

fn read_byte(bytes: &[u8], offset: usize, message: &'static str) -> Result<u8, V2FormatError> {
    bytes
        .get(offset)
        .copied()
        .ok_or(V2FormatError::Invalid(message))
}

fn read_array<const N: usize>(
    bytes: &[u8],
    offset: usize,
    message: &'static str,
) -> Result<[u8; N], V2FormatError> {
    let end = offset
        .checked_add(N)
        .ok_or(V2FormatError::Overflow("v2 codec range exceeds usize"))?;
    bytes
        .get(offset..end)
        .ok_or(V2FormatError::Invalid(message))?
        .try_into()
        .map_err(|_| V2FormatError::Invalid(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(bytes.len().saturating_mul(2));
        for byte in bytes {
            output.push(char::from(DIGITS[usize::from(*byte >> 4)]));
            output.push(char::from(DIGITS[usize::from(*byte & 0x0f)]));
        }
        output
    }

    #[test]
    fn leaf_commitment_and_codec_match_golden_vector() {
        let leaf = V2NodeRecord::leaf(9, b"abc").expect("golden leaf should be valid");
        assert_eq!(leaf.height(), 1);
        assert_eq!(leaf.logical_len(), 3);
        assert_eq!(
            hex(leaf.commitment().as_bytes()),
            "631a198b7685ef4dfb1032c761262dc81d3f1c2b1e5e740f81f056e1b847d068"
        );
        let encoded = encode_v2_node(leaf);
        assert_eq!(
            hex(&encoded),
            "54324e32010001000300000000000000090000000000000003000000000000000000000000000000631a198b7685ef4dfb1032c761262dc81d3f1c2b1e5e740f81f056e1b847d068"
        );
        assert_eq!(decode_v2_node(&encoded), Ok(leaf));

        let root = V2RootRecord::from_node(7, leaf).expect("golden root should be valid");
        assert_eq!(root.node_id(), 7);
        assert_eq!(root.logical_len(), 3);
        assert_eq!(root.height(), 1);
        assert_eq!(root.commitment(), leaf.commitment());
        let root_bytes = encode_v2_root(root);
        assert_eq!(
            hex(&root_bytes),
            "543252320100010007000000000000000300000000000000631a198b7685ef4dfb1032c761262dc81d3f1c2b1e5e740f81f056e1b847d068"
        );
        assert_eq!(decode_v2_root(&root_bytes), Ok(root));
    }

    #[test]
    fn branch_commitment_and_codec_match_golden_vector() {
        let left_node = V2NodeRecord::leaf(9, b"abc").expect("left leaf should be valid");
        let right_node = V2NodeRecord::leaf(12, b"XYZ").expect("right leaf should be valid");
        let left = V2RootRecord::from_node(7, left_node).expect("left root should be valid");
        let right = V2RootRecord::from_node(8, right_node).expect("right root should be valid");
        let branch = V2NodeRecord::branch(left, right).expect("balanced branch should be valid");

        assert_eq!(branch.height(), 2);
        assert_eq!(branch.logical_len(), 6);
        assert_eq!(
            hex(branch.commitment().as_bytes()),
            "4f6f4fc9213f22b2b4eb2e67e47f4ead5e75beddd8bcaa3abb257193323ed3a7"
        );
        let encoded = encode_v2_node(branch);
        assert_eq!(
            hex(&encoded),
            "54324e320200020006000000000000000700000000000000080000000000000003000000000000004f6f4fc9213f22b2b4eb2e67e47f4ead5e75beddd8bcaa3abb257193323ed3a7"
        );
        assert_eq!(decode_v2_node(&encoded), Ok(branch));

        let root = V2RootRecord::from_node(9, branch).expect("branch root should be valid");
        let root_bytes = encode_v2_root(root);
        assert_eq!(
            hex(&root_bytes),
            "5432523201000200090000000000000006000000000000004f6f4fc9213f22b2b4eb2e67e47f4ead5e75beddd8bcaa3abb257193323ed3a7"
        );
        assert_eq!(decode_v2_root(&root_bytes), Ok(root));
    }

    #[test]
    fn decoder_fails_closed_on_noncanonical_node_metadata() {
        let leaf = V2NodeRecord::leaf(9, b"abc").expect("fixture leaf should be valid");
        let encoded = encode_v2_node(leaf);
        assert!(matches!(
            decode_v2_node(&encoded[..encoded.len() - 1]),
            Err(V2FormatError::RecordLength { .. })
        ));

        let mut corrupt = encoded;
        corrupt[5] = 1;
        assert_eq!(
            decode_v2_node(&corrupt),
            Err(V2FormatError::Invalid("v2 node flags must be zero"))
        );

        let mut corrupt = encoded;
        corrupt[4] = 99;
        assert_eq!(
            decode_v2_node(&corrupt),
            Err(V2FormatError::Invalid("v2 node kind is unsupported"))
        );

        let mut corrupt = encoded;
        corrupt[24..32].copy_from_slice(&4u64.to_le_bytes());
        assert_eq!(
            decode_v2_node(&corrupt),
            Err(V2FormatError::Invalid(
                "v2 leaf payload length disagrees with logical length"
            ))
        );

        let mut corrupt = encoded;
        corrupt[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(
            decode_v2_node(&corrupt),
            Err(V2FormatError::Overflow("v2 leaf payload range exceeds u64"))
        );
    }

    #[test]
    fn decoder_fails_closed_on_noncanonical_root_and_branch_metadata() {
        let left_node = V2NodeRecord::leaf(0, b"a").expect("left leaf should be valid");
        let right_node = V2NodeRecord::leaf(1, b"b").expect("right leaf should be valid");
        let left = V2RootRecord::from_node(1, left_node).expect("left root should be valid");
        let right = V2RootRecord::from_node(2, right_node).expect("right root should be valid");
        let branch = V2NodeRecord::branch(left, right).expect("branch should be valid");
        let encoded_branch = encode_v2_node(branch);

        let mut corrupt_branch = encoded_branch;
        corrupt_branch[32..40].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(
            decode_v2_node(&corrupt_branch),
            Err(V2FormatError::Invalid(
                "v2 branch left length must split two non-empty children"
            ))
        );

        let root = V2RootRecord::from_node(3, branch).expect("root should be valid");
        let encoded_root = encode_v2_root(root);
        let mut corrupt_root = encoded_root;
        corrupt_root[4] = 99;
        assert_eq!(
            decode_v2_root(&corrupt_root),
            Err(V2FormatError::Invalid(
                "v2 root representation is unsupported"
            ))
        );

        let mut corrupt_root = encoded_root;
        corrupt_root[16..24].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(
            decode_v2_root(&corrupt_root),
            Err(V2FormatError::Invalid(
                "v2 root logical length must be positive"
            ))
        );
    }

    #[test]
    fn branch_construction_checks_balance_and_length_overflow() {
        let leaf = V2NodeRecord::leaf(0, b"a").expect("leaf should be valid");
        let right = V2RootRecord::from_node(2, leaf).expect("right root should be valid");
        let too_tall = V2RootRecord {
            node_id: 1,
            logical_len: 1,
            height: 3,
            commitment: leaf.commitment(),
        };
        assert_eq!(
            V2NodeRecord::branch(too_tall, right),
            Err(V2FormatError::Invalid(
                "v2 sequence branch children are not AVL-balanced"
            ))
        );

        let huge = V2RootRecord {
            node_id: 3,
            logical_len: u64::MAX,
            height: 1,
            commitment: leaf.commitment(),
        };
        assert_eq!(
            V2NodeRecord::branch(huge, right),
            Err(V2FormatError::Overflow(
                "v2 sequence branch logical length exceeds u64"
            ))
        );
    }
}
