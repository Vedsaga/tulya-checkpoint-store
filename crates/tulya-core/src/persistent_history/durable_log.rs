//! Splice-epoch durable log for the domain-neutral history core.
//!
//! The log is the durability authority for generic history: a sequence of
//! framed records replayed from genesis on open. It stores no adapter
//! vocabulary — only numeric history/version identities, parent links,
//! splice coordinates, inserted bytes, request identities, and operation
//! digests.
//!
//! Staging epoch note: the `THL2` magic and the splice record below replace
//! the pre-E2 append-only `THL1` grammar. Old staging logs fail closed at
//! the frame magic and are never reinterpreted; there is deliberately no
//! migration parser (zero external users).
//!
//! Frame layout (all integers little-endian):
//!
//! ```text
//! magic[4] = THL2
//! body_len[u64]
//! body[..]
//! footer_magic[4] = THLF
//! frame_len[u64]
//! sha256[32] over magic + body_len + body + footer_magic + frame_len
//! ```
//!
//! Record bodies:
//!
//! ```text
//! kind[u8]: 1 = create history, 4 = splice, 3 = retire
//! create:  history_id[u64], binding-present[u8] + len[u64] + bytes
//! splice:  history_id[u64], version_id[u64], parent[u64, MAX = none],
//!          offset[u64], delete_len[u64], insert_len[u64], insert[..],
//!          request_len[u64], request[..],
//!          binding-present[u8] + len[u64] + bytes,
//!          operation_digest[32]
//! retire:  request_len[u64], request[..], operation_digest[32]
//! ```
//!
//! Recovery discipline mirrors the staged hot-WAL publication model: a torn
//! tail (fewer bytes than a complete frame) is previously unacknowledged work
//! and stops the scan without error, while any structurally corrupt complete
//! frame fails closed. Appends always truncate to the replayed logical tail
//! first, so a torn tail can never strand garbage mid-file.

use super::{
    HistoryError, HistoryId, PersistentHistoryStore, VersionId, MAX_HISTORY_BINDING_BYTES,
    MAX_HISTORY_REQUEST_ID_BYTES,
};
use crate::operation::DurabilityOperation;
use fs4::FileExt;
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const HISTORY_LOG_MAGIC: [u8; 4] = *b"THL2";
const HISTORY_LOG_FOOTER_MAGIC: [u8; 4] = *b"THLF";
const HISTORY_LOG_HEADER_SIZE: usize = 12;
const HISTORY_LOG_FOOTER_SIZE: usize = 44;
const HISTORY_LOG_DIGEST_DOMAIN: &[u8] = b"tulya-history/v1/log-frame\0";

const RECORD_CREATE_HISTORY: u8 = 1;
const RECORD_RETIRE: u8 = 3;
/// Canonical splice/version record tag. Tag 2 named the pre-E2
/// append-oriented commit record and is deliberately never reused, so no
/// staging byte sequence can be reinterpreted across the grammar epoch.
const RECORD_SPLICE: u8 = 4;
const NO_PARENT: u64 = u64::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryLogRecord {
    CreateHistory {
        history: HistoryId,
        binding: Option<Vec<u8>>,
    },
    Splice {
        history: HistoryId,
        version: VersionId,
        parent: Option<VersionId>,
        offset: u64,
        delete_len: u64,
        insert: Vec<u8>,
        request_id: Option<Vec<u8>>,
        binding: Option<Vec<u8>>,
        digest: [u8; 32],
    },
    Retire {
        request_id: Vec<u8>,
        digest: [u8; 32],
    },
}

/// Durability outcome for a mutating history operation.
///
/// `Rejected` means nothing new became durable and the logical operation may
/// be retried or abandoned. `Indeterminate` means bytes may have become
/// durable: the writer is poisoned and the caller must reopen, then resolve
/// with the same request identity. `RecoveryRequired` means an earlier
/// indeterminate outcome already poisoned this handle.
#[derive(Debug)]
pub enum DurableError {
    Rejected(HistoryError),
    Indeterminate {
        operation: DurabilityOperation,
        source: std::io::Error,
    },
    RecoveryRequired,
    /// A second writable authority was requested while one is already open.
    /// Distinct from rejection: nothing was examined or mutated, another
    /// writer simply holds the store-wide lease.
    AlreadyOpen,
}

impl fmt::Display for DurableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(error) => write!(formatter, "{error}"),
            Self::Indeterminate { operation, source } => {
                write!(
                    formatter,
                    "history durability indeterminate after {operation}: {source}"
                )
            }
            Self::RecoveryRequired => formatter.write_str(
                "history writer requires reopen after an indeterminate durability outcome",
            ),
            Self::AlreadyOpen => formatter.write_str("history store is already open for writing"),
        }
    }
}

impl std::error::Error for DurableError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Indeterminate { source, .. } => Some(source),
            Self::Rejected(_) | Self::RecoveryRequired | Self::AlreadyOpen => None,
        }
    }
}

pub fn encode_history_log_record(record: &HistoryLogRecord) -> Result<Vec<u8>, HistoryError> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(encoded_record_len(record)?)
        .map_err(|_| HistoryError::Capacity("history log record allocation failed"))?;
    match record {
        HistoryLogRecord::CreateHistory { history, binding } => {
            output.push(RECORD_CREATE_HISTORY);
            output.extend_from_slice(&history.id().to_le_bytes());
            match binding {
                Some(bytes) => {
                    if bytes.len() > MAX_HISTORY_BINDING_BYTES {
                        return Err(HistoryError::Invalid(
                            "history log binding exceeds the byte limit",
                        ));
                    }
                    put_u64(&mut output, bytes.len() as u64 + 1);
                    output.extend_from_slice(bytes);
                }
                None => put_u64(&mut output, 0),
            }
        }
        HistoryLogRecord::Splice {
            history,
            version,
            parent,
            offset,
            delete_len,
            insert,
            request_id,
            binding,
            digest,
        } => {
            output.push(RECORD_SPLICE);
            output.extend_from_slice(&history.id().to_le_bytes());
            output.extend_from_slice(&version.id().to_le_bytes());
            output.extend_from_slice(&parent.map_or(NO_PARENT, VersionId::id).to_le_bytes());
            output.extend_from_slice(&offset.to_le_bytes());
            output.extend_from_slice(&delete_len.to_le_bytes());
            put_bytes(&mut output, insert);
            match request_id {
                Some(id) => {
                    if id.len() > MAX_HISTORY_REQUEST_ID_BYTES {
                        return Err(HistoryError::Invalid(
                            "history log request identity exceeds the byte limit",
                        ));
                    }
                    put_u64(&mut output, id.len() as u64);
                    output.extend_from_slice(id);
                }
                None => put_u64(&mut output, 0),
            }
            match binding {
                Some(bytes) => {
                    if bytes.len() > MAX_HISTORY_BINDING_BYTES {
                        return Err(HistoryError::Invalid(
                            "history log binding exceeds the byte limit",
                        ));
                    }
                    put_u64(&mut output, bytes.len() as u64 + 1);
                    output.extend_from_slice(bytes);
                }
                None => put_u64(&mut output, 0),
            }
            output.extend_from_slice(digest);
        }
        HistoryLogRecord::Retire { request_id, digest } => {
            output.push(RECORD_RETIRE);
            if request_id.len() > MAX_HISTORY_REQUEST_ID_BYTES {
                return Err(HistoryError::Invalid(
                    "history log request identity exceeds the byte limit",
                ));
            }
            put_u64(&mut output, request_id.len() as u64);
            output.extend_from_slice(request_id);
            output.extend_from_slice(digest);
        }
    }
    Ok(output)
}

fn encoded_record_len(record: &HistoryLogRecord) -> Result<usize, HistoryError> {
    let len = match record {
        HistoryLogRecord::CreateHistory { binding, .. } => 1usize
            .checked_add(8)
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(binding.as_ref().map_or(0, Vec::len)))
            .ok_or(HistoryError::Overflow(
                "history log record length exceeds usize",
            ))?,
        HistoryLogRecord::Splice {
            insert,
            request_id,
            binding,
            ..
        } => 1usize
            .checked_add(8)
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(insert.len()))
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(request_id.as_ref().map_or(0, Vec::len)))
            .and_then(|value| value.checked_add(8))
            .and_then(|value| value.checked_add(binding.as_ref().map_or(0, Vec::len)))
            .and_then(|value| value.checked_add(32))
            .ok_or(HistoryError::Overflow(
                "history log record length exceeds usize",
            ))?,
        HistoryLogRecord::Retire { request_id, .. } => 1usize
            .checked_add(8)
            .and_then(|value| value.checked_add(request_id.len()))
            .and_then(|value| value.checked_add(32))
            .ok_or(HistoryError::Overflow(
                "history log record length exceeds usize",
            ))?,
    };
    Ok(len)
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(output, bytes.len() as u64);
    output.extend_from_slice(bytes);
}

pub fn decode_history_log_record(bytes: &[u8]) -> Result<HistoryLogRecord, HistoryError> {
    let mut cursor = LogCursor { bytes, pos: 0 };
    let kind = cursor.take_byte()?;
    let record = match kind {
        RECORD_CREATE_HISTORY => {
            let history = HistoryId(cursor.take_u64()?);
            let binding = decode_optional_binding(&mut cursor)?;
            HistoryLogRecord::CreateHistory { history, binding }
        }
        RECORD_SPLICE => {
            let history = HistoryId(cursor.take_u64()?);
            let version = VersionId(cursor.take_u64()?);
            let raw_parent = cursor.take_u64()?;
            let parent = if raw_parent == NO_PARENT {
                None
            } else {
                Some(VersionId(raw_parent))
            };
            let offset = cursor.take_u64()?;
            let delete_len = cursor.take_u64()?;
            let insert_len = cursor.take_u64()?;
            let insert = cursor.take_bytes(insert_len)?;
            let request_len = cursor.take_u64()?;
            let request_id = if request_len == 0 {
                None
            } else {
                if request_len > MAX_HISTORY_REQUEST_ID_BYTES as u64 {
                    return Err(HistoryError::Invalid(
                        "history log request identity exceeds the byte limit",
                    ));
                }
                let bytes = cursor.take_bytes(request_len)?;
                Some(bytes.to_vec())
            };
            let binding = decode_optional_binding(&mut cursor)?;
            let digest = cursor.take_array::<32>()?;
            HistoryLogRecord::Splice {
                history,
                version,
                parent,
                offset,
                delete_len,
                insert: insert.to_vec(),
                request_id,
                binding,
                digest,
            }
        }
        RECORD_RETIRE => {
            let request_len = cursor.take_u64()?;
            if request_len == 0 || request_len > MAX_HISTORY_REQUEST_ID_BYTES as u64 {
                return Err(HistoryError::Invalid(
                    "history log retired request identity is outside bounds",
                ));
            }
            let request_id = cursor.take_bytes(request_len)?.to_vec();
            let digest = cursor.take_array::<32>()?;
            HistoryLogRecord::Retire { request_id, digest }
        }
        _ => {
            return Err(HistoryError::Invalid(
                "history log record kind is unsupported",
            ));
        }
    };
    if cursor.pos != bytes.len() {
        return Err(HistoryError::Invalid(
            "history log record has trailing bytes",
        ));
    }
    Ok(record)
}

struct LogCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

/// Decodes the length-prefixed optional binding: zero means absent, otherwise
/// stored length plus one. Empty bindings are rejected: present bindings are
/// always meaningful adapter identity material.
fn decode_optional_binding(cursor: &mut LogCursor<'_>) -> Result<Option<Vec<u8>>, HistoryError> {
    let stored = cursor.take_u64()?;
    if stored == 0 {
        return Ok(None);
    }
    let len = stored.checked_sub(1).ok_or(HistoryError::Invalid(
        "history log binding length underflow",
    ))?;
    if len == 0 || len > MAX_HISTORY_BINDING_BYTES as u64 {
        return Err(HistoryError::Invalid(
            "history log binding is outside bounds",
        ));
    }
    Ok(Some(cursor.take_bytes(len)?.to_vec()))
}

impl<'a> LogCursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], HistoryError> {
        let end = self.pos.checked_add(len).ok_or(HistoryError::Overflow(
            "history log field range exceeds usize",
        ))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(HistoryError::Invalid("history log record is truncated"))?;
        self.pos = end;
        Ok(slice)
    }

    fn take_byte(&mut self) -> Result<u8, HistoryError> {
        Ok(self.take(1)?[0])
    }

    fn take_u64(&mut self) -> Result<u64, HistoryError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().map_err(
            |_| HistoryError::Invalid("history log field width mismatch"),
        )?))
    }

    fn take_bytes(&mut self, len: u64) -> Result<&'a [u8], HistoryError> {
        let len = usize::try_from(len)
            .map_err(|_| HistoryError::Overflow("history log field length exceeds usize"))?;
        self.take(len)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], HistoryError> {
        self.take(N)?
            .try_into()
            .map_err(|_| HistoryError::Invalid("history log field width mismatch"))
    }
}

pub fn encode_history_log_frame(body: &[u8]) -> Result<Vec<u8>, HistoryError> {
    let frame_len = body
        .len()
        .checked_add(HISTORY_LOG_HEADER_SIZE + HISTORY_LOG_FOOTER_SIZE)
        .ok_or(HistoryError::Overflow(
            "history log frame length exceeds usize",
        ))?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(frame_len)
        .map_err(|_| HistoryError::Capacity("history log frame allocation failed"))?;
    output.extend_from_slice(&HISTORY_LOG_MAGIC);
    put_u64(
        &mut output,
        u64::try_from(body.len())
            .map_err(|_| HistoryError::Overflow("history log body length exceeds u64"))?,
    );
    output.extend_from_slice(body);
    output.extend_from_slice(&HISTORY_LOG_FOOTER_MAGIC);
    put_u64(
        &mut output,
        u64::try_from(frame_len)
            .map_err(|_| HistoryError::Overflow("history log frame length exceeds u64"))?,
    );
    output.extend_from_slice(&frame_digest(&output));
    debug_assert_eq!(output.len(), frame_len);
    Ok(output)
}

fn frame_digest(prefix: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(HISTORY_LOG_DIGEST_DOMAIN);
    hasher.update(prefix);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

enum FrameProbe<'a> {
    Complete { body: &'a [u8], consumed: usize },
    Torn,
}

fn probe_history_log_frame(bytes: &[u8]) -> Result<FrameProbe<'_>, HistoryError> {
    if bytes.len() < HISTORY_LOG_MAGIC.len() {
        return Ok(FrameProbe::Torn);
    }
    if bytes.get(..4) != Some(HISTORY_LOG_MAGIC.as_slice()) {
        return Err(HistoryError::Invalid("history log frame magic mismatch"));
    }
    if bytes.len() < HISTORY_LOG_HEADER_SIZE {
        return Ok(FrameProbe::Torn);
    }
    let body_len =
        usize::try_from(u64::from_le_bytes(bytes[4..12].try_into().map_err(
            |_| HistoryError::Invalid("history log body length is truncated"),
        )?))
        .map_err(|_| HistoryError::Overflow("history log body length exceeds usize"))?;
    let frame_len = body_len
        .checked_add(HISTORY_LOG_HEADER_SIZE + HISTORY_LOG_FOOTER_SIZE)
        .ok_or(HistoryError::Overflow(
            "history log frame length exceeds usize",
        ))?;
    if frame_len > bytes.len() {
        return Ok(FrameProbe::Torn);
    }
    let body = bytes
        .get(HISTORY_LOG_HEADER_SIZE..HISTORY_LOG_HEADER_SIZE + body_len)
        .ok_or(HistoryError::Invalid("history log body range is truncated"))?;
    let footer = bytes
        .get(HISTORY_LOG_HEADER_SIZE + body_len..frame_len)
        .ok_or(HistoryError::Invalid(
            "history log footer range is truncated",
        ))?;
    if footer.get(..4) != Some(HISTORY_LOG_FOOTER_MAGIC.as_slice()) {
        return Err(HistoryError::Invalid("history log footer magic mismatch"));
    }
    let stored_len = u64::from_le_bytes(
        footer[4..12]
            .try_into()
            .map_err(|_| HistoryError::Invalid("history log frame length is truncated"))?,
    );
    if stored_len != frame_len as u64 {
        return Err(HistoryError::Invalid(
            "history log frame length disagrees with its footer",
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(HISTORY_LOG_DIGEST_DOMAIN);
    hasher.update(bytes.get(..frame_len - 32).ok_or(HistoryError::Invalid(
        "history log digest prefix is truncated",
    ))?);
    let digest = hasher.finalize();
    if footer.get(12..44) != Some(digest.as_slice()) {
        return Err(HistoryError::Invalid("history log frame digest mismatch"));
    }
    Ok(FrameProbe::Complete {
        body,
        consumed: frame_len,
    })
}

/// Decodes a complete history log, stopping cleanly at a torn tail.
///
/// Returns the decoded records plus the exact consumed byte count, so an
/// appender can truncate to the logical tail before writing: a torn tail must
/// never strand garbage ahead of newer frames.
pub fn decode_history_log(bytes: &[u8]) -> Result<(Vec<HistoryLogRecord>, u64), HistoryError> {
    let mut records: Vec<HistoryLogRecord> = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        match probe_history_log_frame(&bytes[offset..])? {
            FrameProbe::Torn => break,
            FrameProbe::Complete { body, consumed } => {
                records.try_reserve(1).map_err(|_| {
                    HistoryError::Capacity("history log recovery allocation failed")
                })?;
                records.push(decode_history_log_record(body)?);
                offset = offset.checked_add(consumed).ok_or(HistoryError::Overflow(
                    "history log recovery offset exceeds usize",
                ))?;
            }
        }
    }
    let consumed = u64::try_from(offset)
        .map_err(|_| HistoryError::Overflow("history log recovery offset exceeds u64"))?;
    Ok((records, consumed))
}

/// Rebuilds a history store by replaying a decoded log from genesis.
///
/// Every record re-validates through the store's own insertion paths with
/// exact-identity assertions, so a reordered, truncated, or forged log fails
/// closed instead of reconstructing a divergent store.
pub fn recover_history_store(bytes: &[u8]) -> Result<PersistentHistoryStore, HistoryError> {
    let mut store = PersistentHistoryStore::new();
    replay_history_suffix(&mut store, bytes)?;
    Ok(store)
}

/// Replays decoded log bytes into a live store, ignoring a torn tail exactly
/// like full recovery. Used when a snapshot already provides the prefix and
/// only the hot suffix needs application.
///
/// After applying every record, the combined history bindings are validated
/// unique in one linear pass: a repeated nonempty binding proves a forged or
/// torn record, since live creates resolve existing bindings without
/// appending. This covers an imported snapshot plus its hot suffix together.
pub fn replay_history_suffix(
    store: &mut PersistentHistoryStore,
    bytes: &[u8],
) -> Result<(), HistoryError> {
    let (records, _) = decode_history_log(bytes)?;
    for record in &records {
        apply_recovered_record(store, record)?;
    }
    store.validate_replayed_history_bindings()?;
    Ok(())
}

fn apply_recovered_record(
    store: &mut PersistentHistoryStore,
    record: &HistoryLogRecord,
) -> Result<(), HistoryError> {
    match record {
        HistoryLogRecord::CreateHistory { history, binding } => {
            store.replay_create(*history, binding.as_deref())?;
        }
        HistoryLogRecord::Splice {
            history,
            version,
            parent,
            offset,
            delete_len,
            insert,
            request_id,
            binding,
            digest,
        } => {
            let assigned = store.replay_splice(
                *history,
                *version,
                *parent,
                *offset,
                *delete_len,
                insert,
                request_id.as_deref(),
                binding.as_deref(),
                *digest,
            )?;
            if assigned != *version {
                return Err(HistoryError::Invalid(
                    "history log version identity disagrees with replay order",
                ));
            }
        }
        HistoryLogRecord::Retire { request_id, digest } => {
            store.replay_retire(request_id, *digest)?;
        }
    }
    Ok(())
}

/// Append-only file handle for one history log, tracking the replayed logical
/// tail so appends truncate any torn tail before writing.
#[derive(Debug)]
pub(crate) struct DurableHistoryLog {
    file: File,
    tail: u64,
    fail_next_append: bool,
}

impl DurableHistoryLog {
    /// Opens (creating if absent) the log file and recovers the logical tail
    /// by scanning for the last complete frame.
    ///
    /// Test-only driver: production opens generation logs through
    /// [`open_write`](Self::open_write) under the writable authority lease,
    /// never through unlocked opens.
    #[cfg(test)]
    pub(crate) fn open(path: &Path) -> std::io::Result<Self> {
        // Never truncate on open: existing frames are the authority being
        // recovered. Appends truncate explicitly to the replayed tail first.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let tail = scan_log_tail(&bytes).map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
        })?;
        Ok(Self {
            file,
            tail,
            fail_next_append: false,
        })
    }

    /// Opens a generation hot log for writing, holding an exclusive
    /// non-blocking file lock for the handle lifetime.
    ///
    /// A second concurrent writer fails with a contended-lock I/O error
    /// (`WouldBlock` kind on unix plus the `fs4` contended marker): callers
    /// map exactly that to an explicit already-open rejection, mirroring the
    /// store writer lock. The lock covers the hot file itself so seal can
    /// hold the old generation while opening the next one without
    /// self-deadlock; cross-generation safety comes from manifest-driven
    /// recovery, which never reads a superseded hot file.
    pub(crate) fn open_write(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock_exclusive()?;
        let mut log = Self {
            file,
            tail: 0,
            fail_next_append: false,
        };
        log.tail = scan_log_tail(&log.read_all()?).map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
        })?;
        log.fail_next_append = false;
        Ok(log)
    }

    /// Arms a one-shot deterministic append failure for the next
    /// [`append_frame`](Self::append_frame): it fails before touching any
    /// byte, exactly like a filesystem write rejection. Test seam only —
    /// production paths never arm it — for proving pre-authority failures
    /// leave semantic state unchanged.
    #[cfg(test)]
    pub(crate) fn arm_fail_next_append(&mut self) {
        self.fail_next_append = true;
    }

    /// Returns true when the underlying lock failure signals contention
    /// rather than a genuine I/O error.
    pub(crate) fn is_lock_contention(error: &std::io::Error) -> bool {
        error.kind() == std::io::ErrorKind::WouldBlock
            || error.raw_os_error() == fs4::lock_contended_error().raw_os_error()
    }

    /// Appends one complete frame at the logical tail, discarding any torn
    /// tail beyond it first. Callers sync separately to distinguish write
    /// failures (definite reject) from barrier failures (indeterminate).
    pub(crate) fn append_frame(&mut self, frame: &[u8]) -> std::io::Result<()> {
        if self.fail_next_append {
            self.fail_next_append = false;
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "injected fault: history log append rejected",
            ));
        }
        self.file.seek(SeekFrom::Start(self.tail))?;
        self.file.set_len(self.tail)?;
        self.file.write_all(frame)?;
        let advanced = self.tail.checked_add(frame.len() as u64).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "history log tail exceeds u64",
            )
        })?;
        self.tail = advanced;
        Ok(())
    }

    /// Full file durability barrier. File length changes with every append,
    /// so this is `sync_all`, not `sync_data`.
    pub(crate) fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    pub(crate) fn read_all(&mut self) -> std::io::Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        self.file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }
}

fn scan_log_tail(bytes: &[u8]) -> Result<u64, HistoryError> {
    Ok(decode_history_log(bytes)?.1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn splice_record() -> HistoryLogRecord {
        HistoryLogRecord::Splice {
            history: HistoryId(2),
            version: VersionId(5),
            parent: Some(VersionId(4)),
            offset: 3,
            delete_len: 2,
            insert: b"payload-bytes".to_vec(),
            request_id: Some(b"req-1".to_vec()),
            binding: Some(b"bind-1".to_vec()),
            digest: [0x33; 32],
        }
    }

    #[test]
    fn record_codec_round_trips_all_kinds() {
        for record in [
            HistoryLogRecord::CreateHistory {
                history: HistoryId(7),
                binding: Some(b"thread-a".to_vec()),
            },
            HistoryLogRecord::CreateHistory {
                history: HistoryId(8),
                binding: None,
            },
            splice_record(),
            HistoryLogRecord::Splice {
                history: HistoryId(0),
                version: VersionId(0),
                parent: None,
                offset: 0,
                delete_len: 0,
                insert: Vec::new(),
                request_id: None,
                binding: None,
                digest: [0x00; 32],
            },
            HistoryLogRecord::Retire {
                request_id: b"req-9".to_vec(),
                digest: [0x77; 32],
            },
        ] {
            let encoded = encode_history_log_record(&record).unwrap();
            assert_eq!(decode_history_log_record(&encoded).unwrap(), record);
        }
    }

    #[test]
    fn record_decoder_fails_closed_on_truncation_and_trailing() {
        let encoded = encode_history_log_record(&splice_record()).unwrap();
        for end in [0, 1, 7, 8, 9, 20, encoded.len() - 1] {
            assert!(
                decode_history_log_record(&encoded[..end]).is_err(),
                "truncation at {end} must fail"
            );
        }
        let mut trailed = encoded.clone();
        trailed.push(0xFF);
        assert!(decode_history_log_record(&trailed).is_err());

        let mut bad_kind = encoded.clone();
        bad_kind[0] = 0x7F;
        assert!(decode_history_log_record(&bad_kind).is_err());

        let oversized = HistoryLogRecord::Retire {
            request_id: vec![b'x'; MAX_HISTORY_REQUEST_ID_BYTES + 1],
            digest: [0x00; 32],
        };
        assert_eq!(
            encode_history_log_record(&oversized),
            Err(HistoryError::Invalid(
                "history log request identity exceeds the byte limit"
            ))
        );

        let mut manual = vec![RECORD_RETIRE];
        manual.extend_from_slice(&(MAX_HISTORY_REQUEST_ID_BYTES as u64 + 1).to_le_bytes());
        manual.extend_from_slice(&vec![0xAA; MAX_HISTORY_REQUEST_ID_BYTES + 1]);
        manual.extend_from_slice(&[0xBB; 32]);
        assert!(decode_history_log_record(&manual).is_err());
    }

    #[test]
    fn frame_scan_accepts_complete_prefix_and_ignores_torn_tail() {
        let first = encode_history_log_frame(&encode_history_log_record(&splice_record()).unwrap())
            .unwrap();
        let second = encode_history_log_frame(
            &encode_history_log_record(&HistoryLogRecord::CreateHistory {
                history: HistoryId(1),
                binding: None,
            })
            .unwrap(),
        )
        .unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&first);
        bytes.extend_from_slice(&second);
        bytes.extend_from_slice(&first[..first.len() / 2]);

        let (records, consumed) = decode_history_log(&bytes).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(consumed as usize, first.len() + second.len());
    }

    #[test]
    fn frame_scan_rejects_corrupt_magic_digest_and_length() {
        let frame = encode_history_log_frame(&encode_history_log_record(&splice_record()).unwrap())
            .unwrap();
        let mut bad_magic = frame.clone();
        bad_magic[0] ^= 0xFF;
        assert!(decode_history_log(&bad_magic).is_err());

        let mut bad_digest = frame.clone();
        let last = bad_digest.len() - 1;
        bad_digest[last] ^= 0x01;
        assert!(decode_history_log(&bad_digest).is_err());

        let mut bad_len = frame.clone();
        bad_len[4..12].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode_history_log(&bad_len).is_err());
    }
}
