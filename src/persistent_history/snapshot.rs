//! Generic sealed history snapshots for bounded restart.
//!
//! A snapshot captures the complete reconstructible state — generation,
//! allocation counters, histories, versions, ledgers, bindings, and the
//! canonical balanced image — so reopen loads one verified artifact plus a
//! bounded hot suffix instead of replaying lifetime history.
//!
//! The arena image reuses the staged canonical `T2I2` codec; only the small
//! envelope around it is new. Envelope layout (integers little-endian):
//!
//! ```text
//! magic[4] = THS1
//! total_len[u64]          (exact byte length of the whole artifact)
//! schema[u32] = 1
//! generation[u64]
//! represented_wal_end[u64](exact hot-log prefix byte length represented)
//! history_count[u64]
//! version_count[u64]
//! active_count[u64]
//! retired_count[u64]
//! image_len[u64]
//! next_history_id[u64]
//! next_version_id[u64]
//! histories:  per entry: binding-present[u8] + len[u64] + bytes
//! versions:   per entry: history[u64] + parent[u64, MAX = none] +
//!                            binding-present[u8] + len[u64] + bytes
//! active:     per entry, strictly ascending request id:
//!             req_len[u64] + req + digest[32] + version[u64]
//! retired:    per entry, strictly ascending request id:
//!             req_len[u64] + req + digest[32]
//! image[..]               (canonical T2I2 bytes, empty iff no versions)
//! sha256[32] over everything before it
//! ```
//!
//! No adapter vocabulary appears anywhere: histories and versions are numeric,
//! bindings are opaque bytes the core never interprets.

use super::{
    HistoryError, HistoryId, PersistentHistoryStore, VersionId, MAX_HISTORY_BINDING_BYTES,
    MAX_HISTORY_REQUEST_ID_BYTES,
};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

const HISTORY_SNAPSHOT_MAGIC: [u8; 4] = *b"THS1";
const HISTORY_SNAPSHOT_SCHEMA: u32 = 1;
const HISTORY_SNAPSHOT_HEADER_SIZE: usize = 88;
const HISTORY_SNAPSHOT_DIGEST_DOMAIN: &[u8] = b"tulya-history/v1/snapshot\0";
const NO_PARENT: u64 = u64::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotVersion {
    pub(crate) history: HistoryId,
    pub(crate) parent: Option<VersionId>,
    pub(crate) binding: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotActive {
    pub(crate) request_id: Vec<u8>,
    pub(crate) digest: [u8; 32],
    pub(crate) version: VersionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotRetired {
    pub(crate) request_id: Vec<u8>,
    pub(crate) digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistorySnapshot {
    pub(crate) generation: u64,
    pub(crate) represented_wal_end: u64,
    pub(crate) next_history_id: u64,
    pub(crate) next_version_id: u64,
    pub(crate) history_bindings: Vec<Option<Vec<u8>>>,
    pub(crate) versions: Vec<SnapshotVersion>,
    pub(crate) active: Vec<SnapshotActive>,
    pub(crate) retired: Vec<SnapshotRetired>,
    pub(crate) image: Vec<u8>,
}

/// Encodes the complete reconstructible state at one generation.
///
/// Versions are stored densely in `VersionId` order with the image root table
/// in the same order; histories densely in `HistoryId` order. Ledgers encode
/// in strictly ascending request-id order so decoding is deterministic and
/// self-validating. Allocation counters must currently equal their table
/// cardinalities; any divergence fails closed rather than silently adopting
/// a sparse identity space this schema version does not define.
pub(crate) fn encode_history_snapshot(
    store: &PersistentHistoryStore,
    generation: u64,
    represented_wal_end: u64,
) -> Result<Vec<u8>, HistoryError> {
    if store.next_history_id as usize != store.histories.len() {
        return Err(HistoryError::Invalid(
            "history snapshot source histories are not dense",
        ));
    }
    if store.next_version_id as usize != store.versions.len() {
        return Err(HistoryError::Invalid(
            "history snapshot source versions are not dense",
        ));
    }
    for (index, record) in store.versions.iter().enumerate() {
        if record.id().id() as usize != index {
            return Err(HistoryError::Invalid(
                "history snapshot source version disagrees with its coordinate",
            ));
        }
    }
    let mut roots = Vec::new();
    roots
        .try_reserve_exact(store.versions.len())
        .map_err(|_| HistoryError::Capacity("history snapshot root table allocation failed"))?;
    for record in &store.versions {
        roots.push(record.root());
    }
    let image = if store.versions.is_empty() {
        if !store.backend.is_empty() {
            return Err(HistoryError::Invalid(
                "history snapshot source arena is not empty for zero versions",
            ));
        }
        Vec::new()
    } else {
        store.backend.export_image(&roots)?
    };

    let mut active: Vec<(&Vec<u8>, [u8; 32], VersionId)> = Vec::new();
    active
        .try_reserve_exact(store.active_requests.len())
        .map_err(|_| HistoryError::Capacity("history snapshot ledger allocation failed"))?;
    for (id, record) in &store.active_requests {
        active.push((id, record.digest(), record.version()));
    }
    active.sort_by(|left, right| left.0.cmp(right.0));
    let mut retired: Vec<(&Vec<u8>, &[u8; 32])> = Vec::new();
    retired
        .try_reserve_exact(store.retired_requests.len())
        .map_err(|_| HistoryError::Capacity("history snapshot ledger allocation failed"))?;
    for (id, digest) in &store.retired_requests {
        retired.push((id, digest));
    }
    retired.sort_by(|left, right| left.0.cmp(right.0));

    let mut body = Vec::new();
    for index in 0..store.next_history_id {
        let binding = store.history_bindings.get(&HistoryId::new(index));
        put_optional_bytes(&mut body, binding.map(Vec::as_slice))?;
    }
    for record in &store.versions {
        put_u64(&mut body, record.history().id())?;
        put_u64(&mut body, record.parent().map_or(NO_PARENT, VersionId::id))?;
        put_optional_bytes(
            &mut body,
            store.version_bindings.get(&record.id()).map(Vec::as_slice),
        )?;
    }
    for (id, digest, version) in &active {
        put_bytes(&mut body, id)?;
        body.extend_from_slice(digest);
        put_u64(&mut body, version.id())?;
    }
    for (id, digest) in &retired {
        put_bytes(&mut body, id)?;
        body.extend_from_slice(digest.as_slice());
    }
    body.extend_from_slice(&image);

    let mut output = Vec::new();
    output
        .try_reserve_exact(
            HISTORY_SNAPSHOT_HEADER_SIZE
                .checked_add(body.len())
                .and_then(|value| value.checked_add(32))
                .ok_or(HistoryError::Overflow(
                    "history snapshot length exceeds usize",
                ))?,
        )
        .map_err(|_| HistoryError::Capacity("history snapshot allocation failed"))?;
    output.extend_from_slice(&HISTORY_SNAPSHOT_MAGIC);
    put_u64(
        &mut output,
        u64::try_from(HISTORY_SNAPSHOT_HEADER_SIZE + body.len() + 32)
            .map_err(|_| HistoryError::Overflow("history snapshot length exceeds u64"))?,
    )?;
    put_u32(&mut output, HISTORY_SNAPSHOT_SCHEMA);
    put_u64(&mut output, generation)?;
    put_u64(&mut output, represented_wal_end)?;
    put_u64(&mut output, store.next_history_id)?;
    put_u64(&mut output, store.next_version_id)?;
    put_u64(
        &mut output,
        u64::try_from(active.len())
            .map_err(|_| HistoryError::Overflow("history snapshot active count exceeds u64"))?,
    )?;
    put_u64(
        &mut output,
        u64::try_from(retired.len())
            .map_err(|_| HistoryError::Overflow("history snapshot retired count exceeds u64"))?,
    )?;
    put_u64(
        &mut output,
        u64::try_from(image.len())
            .map_err(|_| HistoryError::Overflow("history snapshot image length exceeds u64"))?,
    )?;
    put_u64(&mut output, store.next_history_id)?;
    put_u64(&mut output, store.next_version_id)?;
    output.extend_from_slice(&body);
    output.extend_from_slice(&snapshot_digest(&output));
    if output.len() != HISTORY_SNAPSHOT_HEADER_SIZE + body.len() + 32 {
        return Err(HistoryError::Invalid(
            "history snapshot encoder produced an unexpected length",
        ));
    }
    Ok(output)
}

fn snapshot_digest(prefix: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(HISTORY_SNAPSHOT_DIGEST_DOMAIN);
    hasher.update(prefix);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

/// Decodes and structurally validates a snapshot artifact.
///
/// Structural rules enforced here: magic, schema, exact total length,
/// trailing digest, dense identity tables matching their allocation
/// counters, topological parents, strictly ascending ledger order with no
/// duplicates or active/retired overlap, bounded identifier lengths, and
/// exact byte consumption. Semantic payload verification happens at import,
/// where backend reads recompute every active digest.
pub(crate) fn decode_history_snapshot(bytes: &[u8]) -> Result<HistorySnapshot, HistoryError> {
    let mut cursor = SnapshotCursor { bytes, pos: 0 };
    if cursor.take(4)? != HISTORY_SNAPSHOT_MAGIC {
        return Err(HistoryError::Invalid("history snapshot magic mismatch"));
    }
    let total_len = cursor.take_u64()?;
    if total_len
        != u64::try_from(bytes.len())
            .map_err(|_| HistoryError::Overflow("history snapshot length exceeds u64"))?
    {
        return Err(HistoryError::Invalid(
            "history snapshot length disagrees with its bytes",
        ));
    }
    if cursor.take_u32()? != HISTORY_SNAPSHOT_SCHEMA {
        return Err(HistoryError::Invalid(
            "history snapshot schema is unsupported",
        ));
    }
    let generation = cursor.take_u64()?;
    let represented_wal_end = cursor.take_u64()?;
    let history_count = cursor.bounded_count("history snapshot history count is excessive")?;
    let version_count = cursor.bounded_count("history snapshot version count is excessive")?;
    let active_count = cursor.bounded_count("history snapshot active count is excessive")?;
    let retired_count = cursor.bounded_count("history snapshot retired count is excessive")?;
    let image_len = cursor.bounded_count("history snapshot image length is excessive")?;
    let next_history_id = cursor.take_u64()?;
    let next_version_id = cursor.take_u64()?;
    if next_history_id as usize != history_count {
        return Err(HistoryError::Invalid(
            "history snapshot next history identity disagrees with its table",
        ));
    }
    if next_version_id as usize != version_count {
        return Err(HistoryError::Invalid(
            "history snapshot next version identity disagrees with its table",
        ));
    }

    let mut history_bindings = Vec::new();
    history_bindings
        .try_reserve_exact(history_count)
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for _ in 0..history_count {
        history_bindings.push(cursor.take_optional_bytes()?);
    }
    let mut versions = Vec::new();
    versions
        .try_reserve_exact(version_count)
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for index in 0..version_count {
        let history = cursor.take_u64()?;
        if history as usize >= history_count {
            return Err(HistoryError::Invalid(
                "history snapshot version references a missing history",
            ));
        }
        let raw_parent = cursor.take_u64()?;
        let parent = if raw_parent == NO_PARENT {
            None
        } else {
            if raw_parent as usize >= index {
                return Err(HistoryError::Invalid(
                    "history snapshot version parent is not topologically prior",
                ));
            }
            Some(VersionId::new(raw_parent))
        };
        versions.push(SnapshotVersion {
            history: HistoryId::new(history),
            parent,
            binding: cursor.take_optional_bytes()?,
        });
    }
    let mut active: Vec<SnapshotActive> = Vec::new();
    active
        .try_reserve_exact(active_count)
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for _ in 0..active_count {
        let request_id = cursor.take_request_id()?;
        if let Some(previous) = active.last() {
            if previous.request_id.as_slice() >= request_id.as_slice() {
                return Err(HistoryError::Invalid(
                    "history snapshot active requests are not strictly ordered",
                ));
            }
        }
        let digest = cursor.take_array::<32>()?;
        let version = cursor.take_u64()?;
        if version as usize >= version_count {
            return Err(HistoryError::Invalid(
                "history snapshot active request references a missing version",
            ));
        }
        active.push(SnapshotActive {
            request_id,
            digest,
            version: VersionId::new(version),
        });
    }
    // Strictly ascending order above already excludes duplicates within each
    // ledger; the set below only detects active/retired overlap.
    let mut active_ids: HashSet<Vec<u8>> = HashSet::new();
    active_ids
        .try_reserve(active.len())
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for record in &active {
        let _ = active_ids.insert(record.request_id.clone());
    }
    let mut retired: Vec<SnapshotRetired> = Vec::new();
    retired
        .try_reserve_exact(retired_count)
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for _ in 0..retired_count {
        let request_id = cursor.take_request_id()?;
        if let Some(previous) = retired.last() {
            if previous.request_id.as_slice() >= request_id.as_slice() {
                return Err(HistoryError::Invalid(
                    "history snapshot retired requests are not strictly ordered",
                ));
            }
        }
        if active_ids.contains(request_id.as_slice()) {
            return Err(HistoryError::Invalid(
                "history snapshot request identity is both active and retired",
            ));
        }
        let digest = cursor.take_array::<32>()?;
        retired.push(SnapshotRetired { request_id, digest });
    }
    let image = cursor.take(image_len)?.to_vec();
    if versions.is_empty() && !image.is_empty() {
        return Err(HistoryError::Invalid(
            "history snapshot image without versions is malformed",
        ));
    }
    let digest_start = bytes.len().checked_sub(32).ok_or(HistoryError::Invalid(
        "history snapshot digest is truncated",
    ))?;
    if cursor.pos != digest_start {
        return Err(HistoryError::Invalid(
            "history snapshot sections disagree with its digest boundary",
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(HISTORY_SNAPSHOT_DIGEST_DOMAIN);
    hasher.update(bytes.get(..digest_start).ok_or(HistoryError::Invalid(
        "history snapshot digest prefix is truncated",
    ))?);
    if hasher.finalize().as_slice() != bytes.get(digest_start..).unwrap_or(&[]) {
        return Err(HistoryError::Invalid("history snapshot digest mismatch"));
    }
    Ok(HistorySnapshot {
        generation,
        represented_wal_end,
        next_history_id,
        next_version_id,
        history_bindings,
        versions,
        active,
        retired,
        image,
    })
}

struct SnapshotCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SnapshotCursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], HistoryError> {
        let end = self.pos.checked_add(len).ok_or(HistoryError::Overflow(
            "history snapshot field range exceeds usize",
        ))?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(HistoryError::Invalid("history snapshot is truncated"))?;
        self.pos = end;
        Ok(slice)
    }

    fn take_u64(&mut self) -> Result<u64, HistoryError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().map_err(
            |_| HistoryError::Invalid("history snapshot field width mismatch"),
        )?))
    }

    fn take_u32(&mut self) -> Result<u32, HistoryError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().map_err(
            |_| HistoryError::Invalid("history snapshot field width mismatch"),
        )?))
    }

    /// Bounds a count field by the remaining artifact bytes so a corrupt
    /// count fails closed instead of driving a huge reservation.
    fn bounded_count(&mut self, message: &'static str) -> Result<usize, HistoryError> {
        let count = self.take_u64()?;
        let count = usize::try_from(count)
            .map_err(|_| HistoryError::Overflow("history snapshot count exceeds usize"))?;
        let remaining = self
            .bytes
            .len()
            .checked_sub(self.pos)
            .ok_or(HistoryError::Overflow(
                "history snapshot position exceeds usize",
            ))?;
        if count > remaining {
            return Err(HistoryError::Invalid(message));
        }
        Ok(count)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], HistoryError> {
        self.take(N)?
            .try_into()
            .map_err(|_| HistoryError::Invalid("history snapshot field width mismatch"))
    }

    fn take_request_id(&mut self) -> Result<Vec<u8>, HistoryError> {
        let len = self.take_u64()?;
        if len == 0 || len > MAX_HISTORY_REQUEST_ID_BYTES as u64 {
            return Err(HistoryError::Invalid(
                "history snapshot request identity is outside bounds",
            ));
        }
        Ok(self.take_bytes(len)?.to_vec())
    }

    fn take_optional_bytes(&mut self) -> Result<Option<Vec<u8>>, HistoryError> {
        let stored = self.take_u64()?;
        if stored == 0 {
            return Ok(None);
        }
        let len = stored.checked_sub(1).ok_or(HistoryError::Invalid(
            "history snapshot binding length underflow",
        ))?;
        if len == 0 || len > MAX_HISTORY_BINDING_BYTES as u64 {
            return Err(HistoryError::Invalid(
                "history snapshot binding is outside bounds",
            ));
        }
        Ok(Some(self.take_bytes(len)?.to_vec()))
    }

    fn take_bytes(&mut self, len: u64) -> Result<&'a [u8], HistoryError> {
        let len = usize::try_from(len)
            .map_err(|_| HistoryError::Overflow("history snapshot field length exceeds usize"))?;
        self.take(len)
    }
}

fn put_u64(output: &mut Vec<u8>, value: u64) -> Result<(), HistoryError> {
    output
        .try_reserve_exact(8)
        .map_err(|_| HistoryError::Capacity("history snapshot allocation failed"))?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), HistoryError> {
    put_u64(
        output,
        u64::try_from(bytes.len())
            .map_err(|_| HistoryError::Overflow("history snapshot length exceeds u64"))?,
    )?;
    output
        .try_reserve_exact(bytes.len())
        .map_err(|_| HistoryError::Capacity("history snapshot allocation failed"))?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn put_optional_bytes(output: &mut Vec<u8>, bytes: Option<&[u8]>) -> Result<(), HistoryError> {
    match bytes {
        None => put_u64(output, 0),
        Some(bytes) => {
            if bytes.is_empty() || bytes.len() > MAX_HISTORY_BINDING_BYTES {
                return Err(HistoryError::Invalid(
                    "history snapshot binding is outside bounds",
                ));
            }
            put_u64(
                output,
                u64::try_from(bytes.len())
                    .map_err(|_| HistoryError::Overflow("history snapshot length exceeds u64"))?
                    .checked_add(1)
                    .ok_or(HistoryError::Overflow(
                        "history snapshot length exceeds u64",
                    ))?,
            )?;
            output
                .try_reserve_exact(bytes.len())
                .map_err(|_| HistoryError::Capacity("history snapshot allocation failed"))?;
            output.extend_from_slice(bytes);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_codec_rejects_truncation_and_digest_corruption() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        store.commit(history, None, b"data", None, None).unwrap();
        let bytes = encode_history_snapshot(&store, 3, 128).unwrap();
        for end in [0, 1, 7, 88, 89, bytes.len() - 33, bytes.len() - 1] {
            assert!(
                decode_history_snapshot(&bytes[..end]).is_err(),
                "truncation at {end} must fail"
            );
        }
        let mut trailed = bytes.clone();
        trailed.push(0x00);
        assert!(decode_history_snapshot(&trailed).is_err());

        let mut bad_digest = bytes.clone();
        let last = bad_digest.len() - 1;
        bad_digest[last] ^= 0x01;
        assert!(decode_history_snapshot(&bad_digest).is_err());

        let mut bad_magic = bytes.clone();
        bad_magic[0] ^= 0xFF;
        assert!(decode_history_snapshot(&bad_magic).is_err());
    }

    #[test]
    fn snapshot_decode_is_deterministic_over_reordered_ledgers() {
        // Ledgers encode sorted regardless of HashMap iteration order, so two
        // stores with identical logical content produce identical bytes.
        let first = encode_history_snapshot(&fixture_store(), 5, 1024).unwrap();
        let second = encode_history_snapshot(&fixture_store(), 5, 1024).unwrap();
        assert_eq!(first, second);
        let snapshot = decode_history_snapshot(&first).unwrap();
        assert_eq!(snapshot.generation, 5);
        assert_eq!(snapshot.represented_wal_end, 1024);
        assert_eq!(snapshot.next_history_id, 2);
        assert_eq!(snapshot.next_version_id, 3);
    }

    fn fixture_store() -> PersistentHistoryStore {
        let mut store = PersistentHistoryStore::new();
        let first = store.create_history_with_binding(b"history-a").unwrap();
        let second = store.create_history().unwrap();
        let v0 = match store
            .commit(first, None, b"aaa", Some(b"req-a"), Some(b"bind-a0"))
            .unwrap()
        {
            crate::persistent_history::CommitOutcome::Committed(version) => version,
            crate::persistent_history::CommitOutcome::Replayed(_)
            | crate::persistent_history::CommitOutcome::Retired => {
                panic!("fixture commit must create")
            }
        };
        store
            .commit(first, Some(v0.id()), b"bbb", Some(b"req-b"), None)
            .unwrap();
        store.commit(second, None, b"zzz", None, None).unwrap();
        store.retire_request(b"req-a").unwrap();
        store
    }

    #[test]
    fn snapshot_import_rebuilds_exact_working_store() {
        let source = fixture_store();
        let bytes = encode_history_snapshot(&source, 9, 4096).unwrap();
        let snapshot = decode_history_snapshot(&bytes).unwrap();
        let mut imported = PersistentHistoryStore::import_snapshot(snapshot).unwrap();

        assert_eq!(imported.next_history_id, 2);
        assert_eq!(imported.next_version_id, 3);
        assert_eq!(imported.histories.len(), 2);
        assert_eq!(imported.versions.len(), 3);
        assert_eq!(
            imported.history_bindings.get(&HistoryId::new(0)),
            Some(&b"history-a".to_vec())
        );
        assert_eq!(
            imported.version_bindings.get(&VersionId::new(0)),
            Some(&b"bind-a0".to_vec())
        );
        for index in 0..3u64 {
            let version = imported.lookup_version(VersionId::new(index)).unwrap();
            assert_eq!(version.id().id(), index);
            imported.verify(version).unwrap();
        }
        let mut output = Vec::new();
        imported
            .read(
                imported.lookup_version(VersionId::new(1)).unwrap(),
                0,
                6,
                &mut output,
            )
            .unwrap();
        assert_eq!(output, b"aaabbb");
        assert_eq!(imported.active_requests.len(), 1);
        assert_eq!(imported.retired_requests.len(), 1);
        // The imported store continues operating with dense identities.
        let history = HistoryId::new(0);
        let parent = imported.lookup_version(VersionId::new(1)).unwrap();
        let next = match imported
            .commit(history, Some(parent.id()), b"ccc", None, None)
            .unwrap()
        {
            crate::persistent_history::CommitOutcome::Committed(version) => version,
            crate::persistent_history::CommitOutcome::Replayed(_)
            | crate::persistent_history::CommitOutcome::Retired => {
                panic!("post-import commit must create")
            }
        };
        assert_eq!(next.id().id(), 3);
    }

    #[test]
    fn snapshot_import_preserves_ledger_digests_exactly() {
        // Import revalidates structural cross-references, not operation
        // digests: digests bind commit deltas that version content cannot
        // reproduce, so their authenticity traces to commit-time and
        // log-replay validation while the artifact digest protects these
        // bytes. Import must therefore preserve them byte-exact.
        let source = fixture_store();
        let bytes = encode_history_snapshot(&source, 1, 0).unwrap();
        let snapshot = decode_history_snapshot(&bytes).unwrap();
        let imported = PersistentHistoryStore::import_snapshot(snapshot).unwrap();
        assert_eq!(imported.active_requests, source.active_requests);
        assert_eq!(imported.retired_requests, source.retired_requests);
    }

    #[test]
    fn snapshot_tampered_image_bytes_fail_closed() {
        let source = fixture_store();
        let bytes = encode_history_snapshot(&source, 1, 0).unwrap();
        let snapshot = decode_history_snapshot(&bytes).unwrap();
        assert!(!snapshot.image.is_empty());
        let mut bad_image = snapshot.image.clone();
        let flip = bad_image.len() / 2;
        bad_image[flip] ^= 0xFF;
        let mut bad = bytes.clone();
        let image_start = bytes.len() - 32 - bad_image.len();
        bad[image_start..image_start + bad_image.len()].copy_from_slice(&bad_image);
        // The artifact digest pins the sealed bytes, so any image tampering
        // fails at decode before import ever runs.
        assert!(decode_history_snapshot(&bad).is_err());
    }

    #[test]
    fn empty_snapshot_round_trips_to_empty_store() {
        let source = PersistentHistoryStore::new();
        let bytes = encode_history_snapshot(&source, 0, 0).unwrap();
        let snapshot = decode_history_snapshot(&bytes).unwrap();
        assert_eq!(snapshot.next_history_id, 0);
        assert_eq!(snapshot.next_version_id, 0);
        assert!(snapshot.versions.is_empty());
        assert!(snapshot.image.is_empty());
        let imported = PersistentHistoryStore::import_snapshot(snapshot).unwrap();
        assert_eq!(imported.next_history_id, 0);
        assert_eq!(imported.next_version_id, 0);
        assert!(imported.versions.is_empty());
    }
}
