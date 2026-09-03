//! Canonical sealed semantic snapshot for staged Format v2.
//!
//! `T2S2` pairs the already-accepted `T2I2` persistent-sequence arena image
//! with version/checkpoint publication metadata and durable request ledgers.
//! It is an immutable reopen artifact, not a foreground append format.

use super::avl::{V2AvlError, V2AvlSequence};
use super::commit_v2::{checkpoint_operation_digest, V2CommitError};
use super::publication_v2::{
    checkpoint_state_metadata, decode_v2_checkpoint, decode_v2_version, encode_v2_checkpoint,
    encode_v2_version, V2CheckpointRecord, V2PublicationError, V2VersionRecord,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt;

const V2_SNAPSHOT_MAGIC: [u8; 4] = *b"T2S2";
const V2_SNAPSHOT_SCHEMA: u32 = 1;
const V2_SNAPSHOT_HEADER_SIZE: usize = 96;
const V2_SNAPSHOT_PREFIX_SIZE: usize = 64;
const V2_SNAPSHOT_DIGEST_OFFSET: usize = 64;
const V2_SNAPSHOT_DIGEST_SIZE: usize = 32;
const V2_SNAPSHOT_DOMAIN: &[u8] = b"tulya-checkpoint-v2/sealed-snapshot\0";
const V2_ACTIVE_REQUEST_MAGIC: [u8; 4] = *b"T2A2";
const V2_ACTIVE_REQUEST_PREFIX_SIZE: usize = 56;
const V2_RETIRED_REQUEST_MAGIC: [u8; 4] = *b"T2D2";
const V2_RETIRED_REQUEST_PREFIX_SIZE: usize = 48;
const V2_MAX_REQUEST_ID_BYTES: usize = 4096;
const V2_MAX_FIXED_RECORD_SIZE: u32 = 4096;
const V2_MIN_CHECKPOINT_RECORD_SIZE: usize = 88;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum V2SnapshotError {
    Avl(V2AvlError),
    Commit(V2CommitError),
    Publication(V2PublicationError),
    Invalid(&'static str),
    Overflow(&'static str),
}

impl fmt::Display for V2SnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Avl(error) => write!(formatter, "{error}"),
            Self::Commit(error) => write!(formatter, "{error}"),
            Self::Publication(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for V2SnapshotError {}

impl From<V2AvlError> for V2SnapshotError {
    fn from(error: V2AvlError) -> Self {
        Self::Avl(error)
    }
}

impl From<V2CommitError> for V2SnapshotError {
    fn from(error: V2CommitError) -> Self {
        Self::Commit(error)
    }
}

impl From<V2PublicationError> for V2SnapshotError {
    fn from(error: V2PublicationError) -> Self {
        Self::Publication(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2ActiveRequestRecord {
    request_id: Vec<u8>,
    operation_digest: [u8; 32],
    checkpoint_ordinal: u64,
}

impl V2ActiveRequestRecord {
    pub(super) fn new(
        request_id: Vec<u8>,
        operation_digest: [u8; 32],
        checkpoint_ordinal: u64,
    ) -> Result<Self, V2SnapshotError> {
        validate_request_id(&request_id)?;
        Ok(Self {
            request_id,
            operation_digest,
            checkpoint_ordinal,
        })
    }

    pub(super) fn request_id(&self) -> &[u8] {
        &self.request_id
    }

    pub(super) const fn operation_digest(&self) -> [u8; 32] {
        self.operation_digest
    }

    pub(super) const fn checkpoint_ordinal(&self) -> u64 {
        self.checkpoint_ordinal
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2RetiredRequestRecord {
    request_id: Vec<u8>,
    operation_digest: [u8; 32],
}

impl V2RetiredRequestRecord {
    pub(super) fn new(
        request_id: Vec<u8>,
        operation_digest: [u8; 32],
    ) -> Result<Self, V2SnapshotError> {
        validate_request_id(&request_id)?;
        Ok(Self {
            request_id,
            operation_digest,
        })
    }

    pub(super) fn request_id(&self) -> &[u8] {
        &self.request_id
    }

    pub(super) const fn operation_digest(&self) -> [u8; 32] {
        self.operation_digest
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct V2SealedSnapshot {
    pub(super) image: Vec<u8>,
    pub(super) versions: Vec<V2VersionRecord>,
    pub(super) checkpoints: Vec<V2CheckpointRecord>,
    pub(super) active_requests: Vec<V2ActiveRequestRecord>,
    pub(super) retired_requests: Vec<V2RetiredRequestRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V2SnapshotHeader {
    total_len: u64,
    image_len: u64,
    version_count: u64,
    checkpoint_count: u64,
    active_request_count: u64,
    retired_request_count: u64,
    version_record_size: u32,
}

pub(super) fn encode_v2_sealed_snapshot(
    snapshot: &V2SealedSnapshot,
) -> Result<Vec<u8>, V2SnapshotError> {
    validate_snapshot(snapshot)?;

    let mut version_records = Vec::with_capacity(snapshot.versions.len());
    for version in &snapshot.versions {
        version_records.push(encode_v2_version(*version)?.to_vec());
    }
    let version_record_size = u32::try_from(
        version_records
            .first()
            .ok_or(V2SnapshotError::Invalid(
                "v2 sealed snapshot requires at least one version",
            ))?
            .len(),
    )
    .map_err(|_| V2SnapshotError::Overflow("v2 snapshot version record width exceeds u32"))?;
    validate_fixed_record_size(version_record_size)?;

    let mut checkpoint_records = Vec::with_capacity(snapshot.checkpoints.len());
    for checkpoint in &snapshot.checkpoints {
        checkpoint_records.push(encode_v2_checkpoint(checkpoint)?);
    }

    let mut active_requests = snapshot.active_requests.clone();
    active_requests.sort_by(|left, right| left.request_id.cmp(&right.request_id));
    let mut active_records = Vec::with_capacity(active_requests.len());
    for record in &active_requests {
        active_records.push(encode_active_request(record)?);
    }

    let mut retired_requests = snapshot.retired_requests.clone();
    retired_requests.sort_by(|left, right| left.request_id.cmp(&right.request_id));
    let mut retired_records = Vec::with_capacity(retired_requests.len());
    for record in &retired_requests {
        retired_records.push(encode_retired_request(record)?);
    }

    let body_len = snapshot
        .image
        .len()
        .checked_add(sum_lengths(&version_records)?)
        .and_then(|value| value.checked_add(sum_lengths(&checkpoint_records).ok()?))
        .and_then(|value| value.checked_add(sum_lengths(&active_records).ok()?))
        .and_then(|value| value.checked_add(sum_lengths(&retired_records).ok()?))
        .ok_or(V2SnapshotError::Overflow(
            "v2 sealed snapshot body length exceeds usize",
        ))?;
    let total_len =
        V2_SNAPSHOT_HEADER_SIZE
            .checked_add(body_len)
            .ok_or(V2SnapshotError::Overflow(
                "v2 sealed snapshot length exceeds usize",
            ))?;
    let header = V2SnapshotHeader {
        total_len: u64::try_from(total_len)
            .map_err(|_| V2SnapshotError::Overflow("v2 snapshot length exceeds u64"))?,
        image_len: u64::try_from(snapshot.image.len())
            .map_err(|_| V2SnapshotError::Overflow("v2 snapshot image length exceeds u64"))?,
        version_count: u64::try_from(snapshot.versions.len())
            .map_err(|_| V2SnapshotError::Overflow("v2 snapshot version count exceeds u64"))?,
        checkpoint_count: u64::try_from(snapshot.checkpoints.len())
            .map_err(|_| V2SnapshotError::Overflow("v2 snapshot checkpoint count exceeds u64"))?,
        active_request_count: u64::try_from(active_requests.len()).map_err(|_| {
            V2SnapshotError::Overflow("v2 snapshot active request count exceeds u64")
        })?,
        retired_request_count: u64::try_from(retired_requests.len()).map_err(|_| {
            V2SnapshotError::Overflow("v2 snapshot retired request count exceeds u64")
        })?,
        version_record_size,
    };

    let mut output = vec![0u8; V2_SNAPSHOT_HEADER_SIZE];
    encode_header_prefix(header, &mut output[..V2_SNAPSHOT_PREFIX_SIZE]);
    output.extend_from_slice(&snapshot.image);
    append_records(&mut output, &version_records);
    append_records(&mut output, &checkpoint_records);
    append_records(&mut output, &active_records);
    append_records(&mut output, &retired_records);
    if output.len() != total_len {
        return Err(V2SnapshotError::Invalid(
            "v2 sealed snapshot encoder produced an unexpected length",
        ));
    }
    let digest = snapshot_digest(
        &output[..V2_SNAPSHOT_PREFIX_SIZE],
        &output[V2_SNAPSHOT_HEADER_SIZE..],
    );
    output[V2_SNAPSHOT_DIGEST_OFFSET..V2_SNAPSHOT_HEADER_SIZE].copy_from_slice(&digest);
    Ok(output)
}

pub(super) fn decode_v2_sealed_snapshot(bytes: &[u8]) -> Result<V2SealedSnapshot, V2SnapshotError> {
    let header = decode_header(bytes)?;
    let expected_len = usize::try_from(header.total_len)
        .map_err(|_| V2SnapshotError::Overflow("v2 snapshot length exceeds usize"))?;
    if expected_len != bytes.len() {
        return Err(V2SnapshotError::Invalid(
            "v2 sealed snapshot byte length mismatch",
        ));
    }
    let stored_digest: [u8; V2_SNAPSHOT_DIGEST_SIZE] = bytes
        .get(V2_SNAPSHOT_DIGEST_OFFSET..V2_SNAPSHOT_HEADER_SIZE)
        .ok_or(V2SnapshotError::Invalid("v2 snapshot digest is truncated"))?
        .try_into()
        .map_err(|_| V2SnapshotError::Invalid("v2 snapshot digest width mismatch"))?;
    let expected_digest = snapshot_digest(
        bytes
            .get(..V2_SNAPSHOT_PREFIX_SIZE)
            .ok_or(V2SnapshotError::Invalid("v2 snapshot prefix is truncated"))?,
        bytes
            .get(V2_SNAPSHOT_HEADER_SIZE..)
            .ok_or(V2SnapshotError::Invalid("v2 snapshot body is truncated"))?,
    );
    if stored_digest != expected_digest {
        return Err(V2SnapshotError::Invalid(
            "v2 sealed snapshot digest mismatch",
        ));
    }

    let image_len = usize::try_from(header.image_len)
        .map_err(|_| V2SnapshotError::Overflow("v2 snapshot image length exceeds usize"))?;
    if image_len == 0 {
        return Err(V2SnapshotError::Invalid(
            "v2 sealed snapshot image must be non-empty",
        ));
    }
    let image_end =
        V2_SNAPSHOT_HEADER_SIZE
            .checked_add(image_len)
            .ok_or(V2SnapshotError::Overflow(
                "v2 snapshot image end exceeds usize",
            ))?;
    let image = bytes
        .get(V2_SNAPSHOT_HEADER_SIZE..image_end)
        .ok_or(V2SnapshotError::Invalid("v2 snapshot image is truncated"))?
        .to_vec();

    let version_record_size = usize::try_from(header.version_record_size)
        .map_err(|_| V2SnapshotError::Overflow("v2 snapshot version width exceeds usize"))?;
    validate_fixed_record_size(header.version_record_size)?;
    let mut cursor = image_end;
    let version_count = bounded_count(
        header.version_count,
        bytes.len().saturating_sub(cursor),
        version_record_size,
        "v2 snapshot version count exceeds body capacity",
    )?;
    let mut versions = Vec::with_capacity(version_count);
    for index in 0..version_count {
        let end = cursor
            .checked_add(version_record_size)
            .ok_or(V2SnapshotError::Overflow(
                "v2 snapshot version end exceeds usize",
            ))?;
        let expected_version_id = u32::try_from(index)
            .map_err(|_| V2SnapshotError::Overflow("v2 snapshot version id exceeds u32"))?;
        versions.push(decode_v2_version(
            bytes
                .get(cursor..end)
                .ok_or(V2SnapshotError::Invalid("v2 snapshot version is truncated"))?,
            expected_version_id,
        )?);
        cursor = end;
    }

    let checkpoint_count = bounded_count(
        header.checkpoint_count,
        bytes.len().saturating_sub(cursor),
        V2_MIN_CHECKPOINT_RECORD_SIZE,
        "v2 snapshot checkpoint count exceeds body capacity",
    )?;
    let mut checkpoints = Vec::with_capacity(checkpoint_count);
    for _ in 0..checkpoint_count {
        let record_len =
            framed_record_len(bytes, cursor, "v2 snapshot checkpoint length is truncated")?;
        let end = cursor
            .checked_add(record_len)
            .ok_or(V2SnapshotError::Overflow(
                "v2 snapshot checkpoint end exceeds usize",
            ))?;
        checkpoints.push(decode_v2_checkpoint(
            bytes.get(cursor..end).ok_or(V2SnapshotError::Invalid(
                "v2 snapshot checkpoint record is truncated",
            ))?,
            header.version_count,
        )?);
        cursor = end;
    }

    let active_count = bounded_count(
        header.active_request_count,
        bytes.len().saturating_sub(cursor),
        V2_ACTIVE_REQUEST_PREFIX_SIZE + 1,
        "v2 snapshot active request count exceeds body capacity",
    )?;
    let mut active_requests = Vec::with_capacity(active_count);
    let mut previous_active: Option<Vec<u8>> = None;
    for _ in 0..active_count {
        let record_len = framed_record_len(bytes, cursor, "v2 active request length is truncated")?;
        let end = cursor
            .checked_add(record_len)
            .ok_or(V2SnapshotError::Overflow(
                "v2 active request end exceeds usize",
            ))?;
        let record = decode_active_request(bytes.get(cursor..end).ok_or(
            V2SnapshotError::Invalid("v2 active request record is truncated"),
        )?)?;
        require_strict_request_order(previous_active.as_deref(), record.request_id())?;
        previous_active = Some(record.request_id.clone());
        active_requests.push(record);
        cursor = end;
    }

    let retired_count = bounded_count(
        header.retired_request_count,
        bytes.len().saturating_sub(cursor),
        V2_RETIRED_REQUEST_PREFIX_SIZE + 1,
        "v2 snapshot retired request count exceeds body capacity",
    )?;
    let mut retired_requests = Vec::with_capacity(retired_count);
    let mut previous_retired: Option<Vec<u8>> = None;
    for _ in 0..retired_count {
        let record_len =
            framed_record_len(bytes, cursor, "v2 retired request length is truncated")?;
        let end = cursor
            .checked_add(record_len)
            .ok_or(V2SnapshotError::Overflow(
                "v2 retired request end exceeds usize",
            ))?;
        let record = decode_retired_request(bytes.get(cursor..end).ok_or(
            V2SnapshotError::Invalid("v2 retired request record is truncated"),
        )?)?;
        require_strict_request_order(previous_retired.as_deref(), record.request_id())?;
        previous_retired = Some(record.request_id.clone());
        retired_requests.push(record);
        cursor = end;
    }

    if cursor != bytes.len() {
        return Err(V2SnapshotError::Invalid(
            "v2 sealed snapshot has trailing bytes",
        ));
    }
    let snapshot = V2SealedSnapshot {
        image,
        versions,
        checkpoints,
        active_requests,
        retired_requests,
    };
    validate_snapshot(&snapshot)?;
    Ok(snapshot)
}

fn validate_snapshot(snapshot: &V2SealedSnapshot) -> Result<(), V2SnapshotError> {
    if snapshot.versions.is_empty() || snapshot.checkpoints.is_empty() {
        return Err(V2SnapshotError::Invalid(
            "v2 sealed snapshot requires committed versions and checkpoints",
        ));
    }
    let (_, image_roots) = V2AvlSequence::import_image(&snapshot.image)?;
    if image_roots.len() != snapshot.versions.len() {
        return Err(V2SnapshotError::Invalid(
            "v2 snapshot image roots do not match version count",
        ));
    }
    for (index, (root, version)) in image_roots
        .iter()
        .copied()
        .zip(snapshot.versions.iter().copied())
        .enumerate()
    {
        let expected_id = u32::try_from(index)
            .map_err(|_| V2SnapshotError::Overflow("v2 snapshot version id exceeds u32"))?;
        if version.version_id() != expected_id {
            return Err(V2SnapshotError::Invalid(
                "v2 snapshot version identifiers are not sequential",
            ));
        }
        encode_v2_version(version)?;
        if root != version.root() {
            return Err(V2SnapshotError::Invalid(
                "v2 snapshot image root disagrees with version root",
            ));
        }
    }

    let mut checkpoint_ordinals = HashMap::<(String, String), u64>::new();
    for (index, checkpoint) in snapshot.checkpoints.iter().enumerate() {
        encode_v2_checkpoint(checkpoint)?;
        let identity = resolve_version_root(&snapshot.versions, checkpoint.identity_version)?;
        let messages = checkpoint
            .messages_version
            .map(|version| resolve_version_root(&snapshot.versions, version))
            .transpose()?;
        let result = checkpoint
            .result_version
            .map(|version| resolve_version_root(&snapshot.versions, version))
            .transpose()?;
        let expected_state = checkpoint_state_metadata(identity, messages, result)?;
        if expected_state != checkpoint.state {
            return Err(V2SnapshotError::Invalid(
                "v2 snapshot checkpoint state disagrees with version roots",
            ));
        }
        if let Some(parent) = checkpoint.parent_checkpoint_id.as_deref() {
            if !checkpoint_ordinals.contains_key(&(checkpoint.thread_id.clone(), parent.to_owned()))
            {
                return Err(V2SnapshotError::Invalid(
                    "v2 snapshot checkpoint parent is not topologically prior in the same thread",
                ));
            }
        }
        let ordinal = u64::try_from(index)
            .map_err(|_| V2SnapshotError::Overflow("v2 snapshot checkpoint ordinal exceeds u64"))?;
        if checkpoint_ordinals
            .insert(
                (
                    checkpoint.thread_id.clone(),
                    checkpoint.checkpoint_id.clone(),
                ),
                ordinal,
            )
            .is_some()
        {
            return Err(V2SnapshotError::Invalid(
                "v2 snapshot contains a duplicate checkpoint identity",
            ));
        }
    }

    let mut active_ids = HashSet::<Vec<u8>>::new();
    for record in &snapshot.active_requests {
        validate_request_id(record.request_id())?;
        let index = usize::try_from(record.checkpoint_ordinal())
            .map_err(|_| V2SnapshotError::Overflow("v2 active request ordinal exceeds usize"))?;
        let checkpoint = snapshot
            .checkpoints
            .get(index)
            .ok_or(V2SnapshotError::Invalid(
                "v2 active request checkpoint ordinal is outside the snapshot",
            ))?;
        if checkpoint_operation_digest(checkpoint)? != record.operation_digest() {
            return Err(V2SnapshotError::Invalid(
                "v2 active request digest disagrees with its checkpoint",
            ));
        }
        if !active_ids.insert(record.request_id.clone()) {
            return Err(V2SnapshotError::Invalid(
                "v2 snapshot contains a duplicate active request identity",
            ));
        }
    }

    let mut retired_ids = HashSet::<Vec<u8>>::new();
    for record in &snapshot.retired_requests {
        validate_request_id(record.request_id())?;
        if active_ids.contains(record.request_id()) {
            return Err(V2SnapshotError::Invalid(
                "v2 request identity is both active and retired",
            ));
        }
        if !retired_ids.insert(record.request_id.clone()) {
            return Err(V2SnapshotError::Invalid(
                "v2 snapshot contains a duplicate retired request identity",
            ));
        }
    }
    Ok(())
}

fn resolve_version_root(
    versions: &[V2VersionRecord],
    version: u32,
) -> Result<super::format_v2::V2RootRecord, V2SnapshotError> {
    versions
        .get(
            usize::try_from(version)
                .map_err(|_| V2SnapshotError::Overflow("v2 version reference exceeds usize"))?,
        )
        .copied()
        .map(V2VersionRecord::root)
        .ok_or(V2SnapshotError::Invalid(
            "v2 snapshot checkpoint version is outside the version table",
        ))
}

fn encode_active_request(record: &V2ActiveRequestRecord) -> Result<Vec<u8>, V2SnapshotError> {
    validate_request_id(record.request_id())?;
    let record_len = V2_ACTIVE_REQUEST_PREFIX_SIZE
        .checked_add(record.request_id.len())
        .ok_or(V2SnapshotError::Overflow(
            "v2 active request length exceeds usize",
        ))?;
    let record_len_u32 = u32::try_from(record_len)
        .map_err(|_| V2SnapshotError::Overflow("v2 active request length exceeds u32"))?;
    let request_len = u32::try_from(record.request_id.len())
        .map_err(|_| V2SnapshotError::Overflow("v2 active request id length exceeds u32"))?;
    let mut output = Vec::with_capacity(record_len);
    output.extend_from_slice(&V2_ACTIVE_REQUEST_MAGIC);
    output.extend_from_slice(&record_len_u32.to_le_bytes());
    output.extend_from_slice(&record.checkpoint_ordinal.to_le_bytes());
    output.extend_from_slice(&request_len.to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&record.operation_digest);
    output.extend_from_slice(&record.request_id);
    Ok(output)
}

fn decode_active_request(bytes: &[u8]) -> Result<V2ActiveRequestRecord, V2SnapshotError> {
    if bytes.len() < V2_ACTIVE_REQUEST_PREFIX_SIZE {
        return Err(V2SnapshotError::Invalid(
            "v2 active request record is shorter than its prefix",
        ));
    }
    if bytes.get(..4) != Some(V2_ACTIVE_REQUEST_MAGIC.as_slice()) {
        return Err(V2SnapshotError::Invalid("v2 active request magic mismatch"));
    }
    if usize::try_from(read_u32(bytes, 4, "v2 active request length is truncated")?)
        .map_err(|_| V2SnapshotError::Overflow("v2 active request length exceeds usize"))?
        != bytes.len()
    {
        return Err(V2SnapshotError::Invalid(
            "v2 active request length mismatch",
        ));
    }
    let checkpoint_ordinal = read_u64(bytes, 8, "v2 active request ordinal is truncated")?;
    let request_len = usize::try_from(read_u32(
        bytes,
        16,
        "v2 active request id length is truncated",
    )?)
    .map_err(|_| V2SnapshotError::Overflow("v2 active request id length exceeds usize"))?;
    if read_u32(bytes, 20, "v2 active request flags are truncated")? != 0 {
        return Err(V2SnapshotError::Invalid(
            "v2 active request flags must be zero",
        ));
    }
    let operation_digest: [u8; 32] = bytes
        .get(24..56)
        .ok_or(V2SnapshotError::Invalid(
            "v2 active request digest is truncated",
        ))?
        .try_into()
        .map_err(|_| V2SnapshotError::Invalid("v2 active request digest width mismatch"))?;
    let expected_len = V2_ACTIVE_REQUEST_PREFIX_SIZE
        .checked_add(request_len)
        .ok_or(V2SnapshotError::Overflow(
            "v2 active request end exceeds usize",
        ))?;
    if expected_len != bytes.len() {
        return Err(V2SnapshotError::Invalid(
            "v2 active request id geometry mismatch",
        ));
    }
    V2ActiveRequestRecord::new(
        bytes
            .get(V2_ACTIVE_REQUEST_PREFIX_SIZE..)
            .ok_or(V2SnapshotError::Invalid(
                "v2 active request id is truncated",
            ))?
            .to_vec(),
        operation_digest,
        checkpoint_ordinal,
    )
}

fn encode_retired_request(record: &V2RetiredRequestRecord) -> Result<Vec<u8>, V2SnapshotError> {
    validate_request_id(record.request_id())?;
    let record_len = V2_RETIRED_REQUEST_PREFIX_SIZE
        .checked_add(record.request_id.len())
        .ok_or(V2SnapshotError::Overflow(
            "v2 retired request length exceeds usize",
        ))?;
    let record_len_u32 = u32::try_from(record_len)
        .map_err(|_| V2SnapshotError::Overflow("v2 retired request length exceeds u32"))?;
    let request_len = u32::try_from(record.request_id.len())
        .map_err(|_| V2SnapshotError::Overflow("v2 retired request id length exceeds u32"))?;
    let mut output = Vec::with_capacity(record_len);
    output.extend_from_slice(&V2_RETIRED_REQUEST_MAGIC);
    output.extend_from_slice(&record_len_u32.to_le_bytes());
    output.extend_from_slice(&request_len.to_le_bytes());
    output.extend_from_slice(&0u32.to_le_bytes());
    output.extend_from_slice(&record.operation_digest);
    output.extend_from_slice(&record.request_id);
    Ok(output)
}

fn decode_retired_request(bytes: &[u8]) -> Result<V2RetiredRequestRecord, V2SnapshotError> {
    if bytes.len() < V2_RETIRED_REQUEST_PREFIX_SIZE {
        return Err(V2SnapshotError::Invalid(
            "v2 retired request record is shorter than its prefix",
        ));
    }
    if bytes.get(..4) != Some(V2_RETIRED_REQUEST_MAGIC.as_slice()) {
        return Err(V2SnapshotError::Invalid(
            "v2 retired request magic mismatch",
        ));
    }
    if usize::try_from(read_u32(
        bytes,
        4,
        "v2 retired request length is truncated",
    )?)
    .map_err(|_| V2SnapshotError::Overflow("v2 retired request length exceeds usize"))?
        != bytes.len()
    {
        return Err(V2SnapshotError::Invalid(
            "v2 retired request length mismatch",
        ));
    }
    let request_len = usize::try_from(read_u32(
        bytes,
        8,
        "v2 retired request id length is truncated",
    )?)
    .map_err(|_| V2SnapshotError::Overflow("v2 retired request id length exceeds usize"))?;
    if read_u32(bytes, 12, "v2 retired request flags are truncated")? != 0 {
        return Err(V2SnapshotError::Invalid(
            "v2 retired request flags must be zero",
        ));
    }
    let operation_digest: [u8; 32] = bytes
        .get(16..48)
        .ok_or(V2SnapshotError::Invalid(
            "v2 retired request digest is truncated",
        ))?
        .try_into()
        .map_err(|_| V2SnapshotError::Invalid("v2 retired request digest width mismatch"))?;
    let expected_len = V2_RETIRED_REQUEST_PREFIX_SIZE
        .checked_add(request_len)
        .ok_or(V2SnapshotError::Overflow(
            "v2 retired request end exceeds usize",
        ))?;
    if expected_len != bytes.len() {
        return Err(V2SnapshotError::Invalid(
            "v2 retired request id geometry mismatch",
        ));
    }
    V2RetiredRequestRecord::new(
        bytes
            .get(V2_RETIRED_REQUEST_PREFIX_SIZE..)
            .ok_or(V2SnapshotError::Invalid(
                "v2 retired request id is truncated",
            ))?
            .to_vec(),
        operation_digest,
    )
}

fn encode_header_prefix(header: V2SnapshotHeader, output: &mut [u8]) {
    output[0..4].copy_from_slice(&V2_SNAPSHOT_MAGIC);
    output[4..8].copy_from_slice(&V2_SNAPSHOT_SCHEMA.to_le_bytes());
    output[8..16].copy_from_slice(&header.total_len.to_le_bytes());
    output[16..24].copy_from_slice(&header.image_len.to_le_bytes());
    output[24..32].copy_from_slice(&header.version_count.to_le_bytes());
    output[32..40].copy_from_slice(&header.checkpoint_count.to_le_bytes());
    output[40..48].copy_from_slice(&header.active_request_count.to_le_bytes());
    output[48..56].copy_from_slice(&header.retired_request_count.to_le_bytes());
    output[56..60].copy_from_slice(&header.version_record_size.to_le_bytes());
    output[60..64].copy_from_slice(&0u32.to_le_bytes());
}

fn decode_header(bytes: &[u8]) -> Result<V2SnapshotHeader, V2SnapshotError> {
    if bytes.len() < V2_SNAPSHOT_HEADER_SIZE {
        return Err(V2SnapshotError::Invalid("v2 snapshot header is truncated"));
    }
    if bytes.get(..4) != Some(V2_SNAPSHOT_MAGIC.as_slice()) {
        return Err(V2SnapshotError::Invalid("v2 snapshot magic mismatch"));
    }
    if read_u32(bytes, 4, "v2 snapshot schema is truncated")? != V2_SNAPSHOT_SCHEMA {
        return Err(V2SnapshotError::Invalid(
            "v2 snapshot schema is unsupported",
        ));
    }
    if read_u32(bytes, 60, "v2 snapshot flags are truncated")? != 0 {
        return Err(V2SnapshotError::Invalid("v2 snapshot flags must be zero"));
    }
    Ok(V2SnapshotHeader {
        total_len: read_u64(bytes, 8, "v2 snapshot length is truncated")?,
        image_len: read_u64(bytes, 16, "v2 snapshot image length is truncated")?,
        version_count: read_u64(bytes, 24, "v2 snapshot version count is truncated")?,
        checkpoint_count: read_u64(bytes, 32, "v2 snapshot checkpoint count is truncated")?,
        active_request_count: read_u64(bytes, 40, "v2 snapshot active request count is truncated")?,
        retired_request_count: read_u64(
            bytes,
            48,
            "v2 snapshot retired request count is truncated",
        )?,
        version_record_size: read_u32(bytes, 56, "v2 snapshot version width is truncated")?,
    })
}

fn snapshot_digest(prefix: &[u8], body: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(V2_SNAPSHOT_DOMAIN);
    hasher.update(prefix);
    hasher.update(body);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn validate_fixed_record_size(width: u32) -> Result<(), V2SnapshotError> {
    if width == 0 || width > V2_MAX_FIXED_RECORD_SIZE {
        return Err(V2SnapshotError::Invalid(
            "v2 snapshot version record width is outside bounds",
        ));
    }
    Ok(())
}

fn validate_request_id(request_id: &[u8]) -> Result<(), V2SnapshotError> {
    if request_id.is_empty() || request_id.len() > V2_MAX_REQUEST_ID_BYTES {
        return Err(V2SnapshotError::Invalid(
            "v2 snapshot request identity is outside bounds",
        ));
    }
    Ok(())
}

fn require_strict_request_order(
    previous: Option<&[u8]>,
    current: &[u8],
) -> Result<(), V2SnapshotError> {
    if previous.is_some_and(|value| value >= current) {
        return Err(V2SnapshotError::Invalid(
            "v2 snapshot request records are not in strict lexical order",
        ));
    }
    Ok(())
}

fn bounded_count(
    count: u64,
    remaining: usize,
    min_record_size: usize,
    message: &'static str,
) -> Result<usize, V2SnapshotError> {
    let count = usize::try_from(count)
        .map_err(|_| V2SnapshotError::Overflow("v2 snapshot record count exceeds usize"))?;
    if min_record_size == 0 || count > remaining / min_record_size {
        return Err(V2SnapshotError::Invalid(message));
    }
    Ok(count)
}

fn framed_record_len(
    bytes: &[u8],
    cursor: usize,
    message: &'static str,
) -> Result<usize, V2SnapshotError> {
    let field = cursor.checked_add(4).ok_or(V2SnapshotError::Overflow(
        "v2 snapshot record length offset exceeds usize",
    ))?;
    let length = usize::try_from(read_u32(bytes, field, message)?)
        .map_err(|_| V2SnapshotError::Overflow("v2 snapshot record length exceeds usize"))?;
    if length == 0 {
        return Err(V2SnapshotError::Invalid(
            "v2 snapshot record length is zero",
        ));
    }
    Ok(length)
}

fn sum_lengths(records: &[Vec<u8>]) -> Result<usize, V2SnapshotError> {
    records.iter().try_fold(0usize, |total, record| {
        total
            .checked_add(record.len())
            .ok_or(V2SnapshotError::Overflow(
                "v2 snapshot section length exceeds usize",
            ))
    })
}

fn append_records(output: &mut Vec<u8>, records: &[Vec<u8>]) {
    for record in records {
        output.extend_from_slice(record);
    }
}

fn read_u32(bytes: &[u8], offset: usize, message: &'static str) -> Result<u32, V2SnapshotError> {
    let end = offset.checked_add(4).ok_or(V2SnapshotError::Overflow(
        "v2 snapshot u32 range exceeds usize",
    ))?;
    let encoded: [u8; 4] = bytes
        .get(offset..end)
        .ok_or(V2SnapshotError::Invalid(message))?
        .try_into()
        .map_err(|_| V2SnapshotError::Invalid(message))?;
    Ok(u32::from_le_bytes(encoded))
}

fn read_u64(bytes: &[u8], offset: usize, message: &'static str) -> Result<u64, V2SnapshotError> {
    let end = offset.checked_add(8).ok_or(V2SnapshotError::Overflow(
        "v2 snapshot u64 range exceeds usize",
    ))?;
    let encoded: [u8; 8] = bytes
        .get(offset..end)
        .ok_or(V2SnapshotError::Invalid(message))?
        .try_into()
        .map_err(|_| V2SnapshotError::Invalid(message))?;
    Ok(u64::from_le_bytes(encoded))
}

#[cfg(test)]
mod tests {
    use super::super::avl::V2AvlSequence;
    use super::super::commit_v2::checkpoint_operation_digest;
    use super::super::publication_v2::{
        checkpoint_state_metadata, V2CheckpointRecord, V2VersionRecord,
    };
    use super::*;

    fn one_checkpoint_snapshot() -> V2SealedSnapshot {
        let mut sequence = V2AvlSequence::default();
        let root = sequence.append(None, b"abc").unwrap().root();
        let image = sequence.export_image(&[root]).unwrap();
        let version = V2VersionRecord::new(0, None, root).unwrap();
        let checkpoint = V2CheckpointRecord {
            checkpoint_no: 1,
            thread_id: "thread".to_owned(),
            checkpoint_id: "cp-1".to_owned(),
            parent_checkpoint_id: None,
            identity_version: 0,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(root, None, None).unwrap(),
        };
        let operation_digest = checkpoint_operation_digest(&checkpoint).unwrap();
        V2SealedSnapshot {
            image,
            versions: vec![version],
            checkpoints: vec![checkpoint],
            active_requests: vec![V2ActiveRequestRecord::new(
                b"req-1".to_vec(),
                operation_digest,
                0,
            )
            .unwrap()],
            retired_requests: Vec::new(),
        }
    }

    #[test]
    fn sealed_snapshot_round_trip_matches_independent_golden_digest() {
        let snapshot = one_checkpoint_snapshot();
        let encoded = encode_v2_sealed_snapshot(&snapshot).unwrap();
        assert_eq!(encoded.len(), 530);
        assert_eq!(
            encoded.get(V2_SNAPSHOT_DIGEST_OFFSET..V2_SNAPSHOT_HEADER_SIZE),
            Some(
                hex_bytes("f31c62e5f4c884424addf7dcd6716c633a0e738be495f6de476ec70401db7516")
                    .as_slice()
            )
        );
        assert_eq!(decode_v2_sealed_snapshot(&encoded), Ok(snapshot));
    }

    #[test]
    fn request_ledgers_encode_in_canonical_lexical_order() {
        let mut snapshot = one_checkpoint_snapshot();
        let digest = snapshot.active_requests[0].operation_digest();
        snapshot.active_requests = vec![
            V2ActiveRequestRecord::new(b"z-request".to_vec(), digest, 0).unwrap(),
            V2ActiveRequestRecord::new(b"a-request".to_vec(), digest, 0).unwrap(),
        ];
        let encoded = encode_v2_sealed_snapshot(&snapshot).unwrap();
        let decoded = decode_v2_sealed_snapshot(&encoded).unwrap();
        assert_eq!(decoded.active_requests[0].request_id(), b"a-request");
        assert_eq!(decoded.active_requests[1].request_id(), b"z-request");
        assert_eq!(encode_v2_sealed_snapshot(&decoded).unwrap(), encoded);
    }

    #[test]
    fn valid_outer_digest_cannot_hide_semantic_ledger_corruption() {
        let snapshot = one_checkpoint_snapshot();
        let mut encoded = encode_v2_sealed_snapshot(&snapshot).unwrap();
        let request_digest_offset = encoded.len() - b"req-1".len() - 32;
        encoded[request_digest_offset] ^= 1;
        let digest = snapshot_digest(
            &encoded[..V2_SNAPSHOT_PREFIX_SIZE],
            &encoded[V2_SNAPSHOT_HEADER_SIZE..],
        );
        encoded[V2_SNAPSHOT_DIGEST_OFFSET..V2_SNAPSHOT_HEADER_SIZE].copy_from_slice(&digest);
        let error = decode_v2_sealed_snapshot(&encoded).unwrap_err();
        assert!(error
            .to_string()
            .contains("active request digest disagrees with its checkpoint"));
    }

    #[test]
    fn snapshot_rejects_version_root_and_checkpoint_parent_inconsistency() {
        let mut sequence = V2AvlSequence::default();
        let root_a = sequence.append(None, b"abc").unwrap().root();
        let root_b = sequence.append(Some(root_a), b"XYZ").unwrap().root();
        let image = sequence.export_image(&[root_a, root_b]).unwrap();
        let version_a = V2VersionRecord::new(0, None, root_a).unwrap();
        let version_b = V2VersionRecord::new(1, Some(0), root_a).unwrap();
        let checkpoint = V2CheckpointRecord {
            checkpoint_no: 2,
            thread_id: "thread".to_owned(),
            checkpoint_id: "cp-2".to_owned(),
            parent_checkpoint_id: Some("missing".to_owned()),
            identity_version: 1,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(root_a, None, None).unwrap(),
        };
        let snapshot = V2SealedSnapshot {
            image,
            versions: vec![version_a, version_b],
            checkpoints: vec![checkpoint],
            active_requests: Vec::new(),
            retired_requests: Vec::new(),
        };
        let error = encode_v2_sealed_snapshot(&snapshot).unwrap_err();
        assert!(error
            .to_string()
            .contains("image root disagrees with version root"));

        let version_b = V2VersionRecord::new(1, Some(0), root_b).unwrap();
        let checkpoint = V2CheckpointRecord {
            checkpoint_no: 2,
            thread_id: "thread".to_owned(),
            checkpoint_id: "cp-2".to_owned(),
            parent_checkpoint_id: Some("missing".to_owned()),
            identity_version: 1,
            messages_version: None,
            result_version: None,
            state: checkpoint_state_metadata(root_b, None, None).unwrap(),
        };
        let snapshot = V2SealedSnapshot {
            image: sequence.export_image(&[root_a, root_b]).unwrap(),
            versions: vec![version_a, version_b],
            checkpoints: vec![checkpoint],
            active_requests: Vec::new(),
            retired_requests: Vec::new(),
        };
        let error = encode_v2_sealed_snapshot(&snapshot).unwrap_err();
        assert!(error
            .to_string()
            .contains("checkpoint parent is not topologically prior"));
    }

    fn hex_bytes(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = hex_nibble(pair[0]);
                let low = hex_nibble(pair[1]);
                (high << 4) | low
            })
            .collect()
    }

    fn hex_nibble(value: u8) -> u8 {
        match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            _ => panic!("invalid test hex digit"),
        }
    }
}
