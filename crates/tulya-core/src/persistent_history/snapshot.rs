//! Generic sealed history snapshots for bounded restart.
//!
//! A snapshot captures the reconstructible metadata — generation, the
//! physical-content epoch and authoritative frontiers, histories, versions
//! with root descriptors, ledgers, and bindings — so reopen loads one small
//! verified artifact plus a bounded hot suffix instead of replaying lifetime
//! history. Content itself stays in the physical generation files: schema6
//! embeds NO arena image. Envelope layout (integers little-endian):
//!
//! ```text
//! magic[4] = THS1
//! total_len[u64]          (exact byte length of the whole artifact)
//! schema[u32] = 6         (staging epoch: 1 = pre-E2 append grammar, rejected;
//!                          2 = E2 splice-only grammar, rejected; 3 = E3
//!                          fork grammar without lifecycle/receipt bounds,
//!                          rejected; 4 = E4 lifecycle grammar with every
//!                          version materialized, rejected; 5 = E5
//!                          image-embedding grammar with optional expired
//!                          roots, rejected: a schema6 snapshot carries root
//!                          descriptors plus physical frontiers and no image,
//!                          which E5 binaries cannot interpret)
//! generation[u64]
//! represented_wal_end[u64](exact hot-log prefix byte length represented)
//! history_count[u64]
//! version_count[u64]
//! active_count[u64]
//! retired_count[u64]
//! receipt_order_count[u64]
//! physical_generation[u64](authoritative content epoch)
//! payload_end[u64]        (authoritative committed payload frontier)
//! node_count[u64]         (authoritative committed node frontier)
//! next_history_id[u64]
//! next_version_id[u64]
//! histories:  per entry: binding-present[u8] + len[u64] + bytes
//! versions:   per entry: history[u64] + parent[u64, MAX = none] +
//!                            binding-present[u8] + len[u64] + bytes +
//!                            lifecycle[u8] (0 = retained, 1 = expired) +
//!                            root_present[u8] (0 = reclaimed/absent,
//!                                               1 = materialized) +
//!                            len[u64] (exact logical byte length) +
//!                            root[56] if materialized (canonical T2R2
//!                              root record; its length must equal len)
//! active:     per entry, strictly ascending request id:
//!             req_len[u64] + req + digest[32] + version[u64]
//! retired:    per entry, strictly ascending request id:
//!             req_len[u64] + req + digest[32]
//! receipt_order: oldest-to-newest retained receipt ids, in insertion order:
//!             per entry: req_len[u64] + req
//! sha256[32] over everything before it
//! ```
//!
//! No adapter vocabulary appears anywhere: histories and versions are numeric,
//! bindings are opaque bytes the core never interprets.

use super::{
    HistoryError, HistoryId, PersistentHistoryStore, VersionId, VersionLifecycle,
    MAX_HISTORY_BINDING_BYTES, MAX_HISTORY_REQUEST_ID_BYTES, STAGING_REQUEST_RECEIPT_CAPACITY,
};
use crate::persistent_sequence::{decode_canonical_root_bytes, V2_ROOT_RECORD_SIZE};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub(crate) const HISTORY_SNAPSHOT_MAGIC: [u8; 4] = *b"THS1";
/// Staging snapshot schema epoch: 6 covers splice + fork plus version
/// lifecycle, the bounded receipt horizon, optional expired roots after
/// reclamation, and — replacing the E5 embedded arena image — root
/// descriptors plus the authoritative physical-content epoch and frontiers.
/// Schemas 1-5 fail closed at decode. There is deliberately no migration
/// (zero external users).
/// NOT a release format version; E9 freezes release Format v1.
pub(crate) const HISTORY_SNAPSHOT_SCHEMA: u32 = 6;
pub(crate) const HISTORY_SNAPSHOT_HEADER_SIZE: usize = 112;
const HISTORY_SNAPSHOT_DIGEST_DOMAIN: &[u8] = b"tulya-history/v1/snapshot\0";
const NO_PARENT: u64 = u64::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVersion {
    pub history: HistoryId,
    pub parent: Option<VersionId>,
    pub binding: Option<Vec<u8>>,
    /// One-way lifecycle mark. The immutable logical `Version` is unchanged;
    /// this byte is the separate lifecycle metadata E4 adds.
    pub lifecycle: VersionLifecycle,
    /// Whether this version materializes a physical root descriptor below.
    /// Retained versions must always materialize; expired versions may be
    /// rootless after reclamation.
    pub root_present: bool,
    /// Exact logical byte length, authoritative without backend access — the
    /// catalogue truth that lets validation and replay work even for
    /// rootless entries.
    pub len: u64,
    /// Canonical 56-byte `T2R2` root record when materialized: the content
    /// descriptor resolving through the snapshot's physical generation. Its
    /// carried length must equal `len` (checked at import).
    pub root: Option<[u8; V2_ROOT_RECORD_SIZE]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotActive {
    pub request_id: Vec<u8>,
    pub digest: [u8; 32],
    pub version: VersionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRetired {
    pub request_id: Vec<u8>,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySnapshot {
    pub generation: u64,
    pub represented_wal_end: u64,
    /// Authoritative physical-content epoch: content files for exactly this
    /// generation back every materialized root descriptor.
    pub physical_generation: u64,
    /// Authoritative committed payload frontier at seal time.
    pub payload_end: u64,
    /// Authoritative committed node frontier at seal time.
    pub node_count: u64,
    pub next_history_id: u64,
    pub next_version_id: u64,
    pub history_bindings: Vec<Option<Vec<u8>>>,
    pub versions: Vec<SnapshotVersion>,
    pub active: Vec<SnapshotActive>,
    pub retired: Vec<SnapshotRetired>,
    /// Oldest-to-newest retained receipt identities, in insertion order
    /// (not sorted): replay, retire, and expiration never reorder.
    pub receipt_order: Vec<Vec<u8>>,
}

/// Encodes the complete reconstructible state at one generation.
///
/// Versions are stored densely in `VersionId` order with the image root table
/// in the same order; histories densely in `HistoryId` order. Ledgers encode
/// in strictly ascending request-id order so decoding is deterministic and
/// self-validating. Allocation counters must currently equal their table
/// cardinalities; any divergence fails closed rather than silently adopting
/// a sparse identity space this schema version does not define.
pub fn encode_history_snapshot(
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
    let mut descriptors: Vec<Option<[u8; V2_ROOT_RECORD_SIZE]>> = Vec::new();
    descriptors
        .try_reserve_exact(store.versions.len())
        .map_err(|_| HistoryError::Capacity("history snapshot root table allocation failed"))?;
    for record in &store.versions {
        match record.root() {
            Some(root) => {
                // The catalogue length is authoritative without traversal:
                // the root already carries authenticated length metadata, so
                // a malformed in-memory state fails here instead of being
                // laundered into a sealed generation.
                if record.len() != root.logical_len().get() {
                    return Err(HistoryError::Invalid(
                        "history snapshot source version length disagrees with its materialized root",
                    ));
                }
                // One node-record read per materialized version on a
                // physical backend: metadata-scale (per version, never per
                // content byte), so seal stays content-size-independent.
                descriptors.push(Some(store.backend.canonical_root_bytes(root)?));
            }
            None => {
                if !store.expired_versions.contains(&record.id()) {
                    return Err(HistoryError::Invalid(
                        "history snapshot source retained version has no materialized root",
                    ));
                }
                descriptors.push(None);
            }
        }
    }
    // Authoritative frontiers travel with the snapshot: durable backends
    // report committed file frontiers; ephemeral memory backends report live
    // arena sizes (self-consistent for struct-level tooling; only physical
    // snapshots bind files at import).
    let (payload_end, node_count) = match store.backend.physical_frontiers() {
        Some(frontiers) => frontiers,
        None => (store.backend.payload_len(), store.backend.node_count()),
    };
    let physical_generation = store.backend.physical_generation().unwrap_or(0);

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

    let mut history_bindings = Vec::new();
    history_bindings
        .try_reserve_exact(store.histories.len())
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for index in 0..store.next_history_id {
        let binding = store.history_bindings.get(&HistoryId::new(index));
        history_bindings.push(binding.cloned());
    }
    let mut versions = Vec::new();
    versions
        .try_reserve_exact(store.versions.len())
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for (record, descriptor) in store.versions.iter().zip(descriptors.iter()) {
        versions.push(SnapshotVersion {
            history: record.history(),
            parent: record.parent(),
            binding: store.version_bindings.get(&record.id()).cloned(),
            lifecycle: if store.expired_versions.contains(&record.id()) {
                VersionLifecycle::Expired
            } else {
                VersionLifecycle::Retained
            },
            root_present: record.root().is_some(),
            len: record.len(),
            root: *descriptor,
        });
    }
    // The live horizon invariant the decoder rechecks: every retained
    // receipt appears exactly once in insertion order.
    if store.receipt_order.len() != store.active_requests.len() + store.retired_requests.len() {
        return Err(HistoryError::Invalid(
            "history snapshot source receipt order disagrees with its ledgers",
        ));
    }
    let snapshot = HistorySnapshot {
        generation,
        represented_wal_end,
        physical_generation,
        payload_end,
        node_count,
        next_history_id: store.next_history_id,
        next_version_id: store.next_version_id,
        history_bindings,
        versions,
        active: active
            .iter()
            .map(|(id, digest, version)| SnapshotActive {
                request_id: (*id).clone(),
                digest: *digest,
                version: *version,
            })
            .collect(),
        retired: retired
            .iter()
            .map(|(id, digest)| SnapshotRetired {
                request_id: (*id).clone(),
                digest: **digest,
            })
            .collect(),
        receipt_order: store.receipt_order.iter().cloned().collect(),
    };
    encode_history_snapshot_struct(&snapshot)
}

/// Encodes an already-built snapshot struct to artifact bytes.
///
/// Structural only: density, coordinate agreement, root-descriptor presence
/// agreement, ledger order, lengths, and the trailing digest are enforced
/// exactly like the store path, but semantic rules (same-history parents,
/// binding uniqueness, retained materialization, receipt-horizon agreement,
/// root-descriptor canonicality and length agreement) are deliberately NOT
/// re-checked here — they are decode/import's job. That split is what lets
/// tests and conformance tooling construct otherwise well-formed corruption
/// vectors with recomputed integrity.
pub fn encode_history_snapshot_struct(snapshot: &HistorySnapshot) -> Result<Vec<u8>, HistoryError> {
    if snapshot.next_history_id as usize != snapshot.history_bindings.len() {
        return Err(HistoryError::Invalid(
            "history snapshot next history identity disagrees with its table",
        ));
    }
    if snapshot.next_version_id as usize != snapshot.versions.len() {
        return Err(HistoryError::Invalid(
            "history snapshot next version identity disagrees with its table",
        ));
    }
    let mut body = Vec::new();
    for binding in &snapshot.history_bindings {
        put_optional_bytes(&mut body, binding.as_deref())?;
    }
    for record in &snapshot.versions {
        if record.history.id() as usize >= snapshot.history_bindings.len() {
            return Err(HistoryError::Invalid(
                "history snapshot version references a missing history",
            ));
        }
        if record.root_present != record.root.is_some() {
            return Err(HistoryError::Invalid(
                "history snapshot version root presence disagrees with its descriptor",
            ));
        }
        put_u64(&mut body, record.history.id())?;
        put_u64(&mut body, record.parent.map_or(NO_PARENT, VersionId::id))?;
        put_optional_bytes(&mut body, record.binding.as_deref())?;
        body.try_reserve_exact(2)
            .map_err(|_| HistoryError::Capacity("history snapshot allocation failed"))?;
        body.push(match record.lifecycle {
            VersionLifecycle::Retained => 0,
            VersionLifecycle::Expired => 1,
        });
        body.push(u8::from(record.root_present));
        put_u64(&mut body, record.len)?;
        if let Some(descriptor) = &record.root {
            body.try_reserve_exact(V2_ROOT_RECORD_SIZE)
                .map_err(|_| HistoryError::Capacity("history snapshot allocation failed"))?;
            body.extend_from_slice(descriptor);
        }
    }
    let mut previous_active: Option<&[u8]> = None;
    for record in &snapshot.active {
        if record.request_id.is_empty() || record.request_id.len() > MAX_HISTORY_REQUEST_ID_BYTES {
            return Err(HistoryError::Invalid(
                "history snapshot request identity is outside bounds",
            ));
        }
        if let Some(previous) = previous_active {
            if previous >= record.request_id.as_slice() {
                return Err(HistoryError::Invalid(
                    "history snapshot active requests are not strictly ordered",
                ));
            }
        }
        previous_active = Some(&record.request_id);
        if record.version.id() as usize >= snapshot.versions.len() {
            return Err(HistoryError::Invalid(
                "history snapshot active request references a missing version",
            ));
        }
        put_bytes(&mut body, &record.request_id)?;
        body.extend_from_slice(&record.digest);
        put_u64(&mut body, record.version.id())?;
    }
    let mut previous_retired: Option<&[u8]> = None;
    for record in &snapshot.retired {
        if record.request_id.is_empty() || record.request_id.len() > MAX_HISTORY_REQUEST_ID_BYTES {
            return Err(HistoryError::Invalid(
                "history snapshot request identity is outside bounds",
            ));
        }
        if let Some(previous) = previous_retired {
            if previous >= record.request_id.as_slice() {
                return Err(HistoryError::Invalid(
                    "history snapshot retired requests are not strictly ordered",
                ));
            }
        }
        previous_retired = Some(&record.request_id);
        put_bytes(&mut body, &record.request_id)?;
        body.extend_from_slice(&record.digest);
    }
    for request_id in &snapshot.receipt_order {
        if request_id.is_empty() || request_id.len() > MAX_HISTORY_REQUEST_ID_BYTES {
            return Err(HistoryError::Invalid(
                "history snapshot request identity is outside bounds",
            ));
        }
        put_bytes(&mut body, request_id)?;
    }

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
    put_u64(&mut output, snapshot.generation)?;
    put_u64(&mut output, snapshot.represented_wal_end)?;
    put_u64(
        &mut output,
        u64::try_from(snapshot.history_bindings.len())
            .map_err(|_| HistoryError::Overflow("history snapshot history count exceeds u64"))?,
    )?;
    put_u64(
        &mut output,
        u64::try_from(snapshot.versions.len())
            .map_err(|_| HistoryError::Overflow("history snapshot version count exceeds u64"))?,
    )?;
    put_u64(
        &mut output,
        u64::try_from(snapshot.active.len())
            .map_err(|_| HistoryError::Overflow("history snapshot active count exceeds u64"))?,
    )?;
    put_u64(
        &mut output,
        u64::try_from(snapshot.retired.len())
            .map_err(|_| HistoryError::Overflow("history snapshot retired count exceeds u64"))?,
    )?;
    put_u64(
        &mut output,
        u64::try_from(snapshot.receipt_order.len()).map_err(|_| {
            HistoryError::Overflow("history snapshot receipt order length exceeds u64")
        })?,
    )?;
    put_u64(&mut output, snapshot.physical_generation)?;
    put_u64(&mut output, snapshot.payload_end)?;
    put_u64(&mut output, snapshot.node_count)?;
    put_u64(&mut output, snapshot.next_history_id)?;
    put_u64(&mut output, snapshot.next_version_id)?;
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
/// duplicates or active/retired overlap, bounded identifier lengths, nonzero
/// version lengths, retained materialization, canonical root descriptors,
/// and exact byte consumption. Import validates physical-root
/// agreement/lifecycle/lineage/receipt consistency against content files;
/// operation digests are preserved byte-exact and trace to commit/replay
/// validation, with snapshot artifact integrity protecting their stored
/// bytes.
pub fn decode_history_snapshot(bytes: &[u8]) -> Result<HistorySnapshot, HistoryError> {
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
    let receipt_order_count =
        cursor.bounded_count("history snapshot receipt order is excessive")?;
    let physical_generation = cursor.take_u64()?;
    let payload_end = cursor.take_u64()?;
    let node_count = cursor.take_u64()?;
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
    // Bindings are the idempotent external identity: the live create resolves
    // an existing binding without writing, so a valid snapshot never repeats
    // a nonempty binding. (Empty bindings cannot appear: the codec rejects
    // zero-length bindings above.)
    {
        let mut seen: HashSet<&[u8]> = HashSet::new();
        seen.try_reserve(history_bindings.len())
            .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
        for binding in history_bindings.iter().flatten() {
            if !seen.insert(binding.as_slice()) {
                return Err(HistoryError::Invalid(
                    "history snapshot history bindings are not unique",
                ));
            }
        }
    }
    let mut versions: Vec<SnapshotVersion> = Vec::new();
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
            // Parenthood is history-local on the live path: a recomputed
            // digest cannot launder a lineage the API could never create.
            if versions[raw_parent as usize].history != HistoryId::new(history) {
                return Err(HistoryError::Invalid(
                    "history snapshot version parent belongs to a different history",
                ));
            }
            Some(VersionId::new(raw_parent))
        };
        let binding = cursor.take_optional_bytes()?;
        let lifecycle = match cursor.take(1)? {
            [0] => VersionLifecycle::Retained,
            [1] => VersionLifecycle::Expired,
            _ => {
                return Err(HistoryError::Invalid(
                    "history snapshot version lifecycle is unsupported",
                ));
            }
        };
        let root_present = match cursor.take(1)? {
            [0] => false,
            [1] => true,
            _ => {
                return Err(HistoryError::Invalid(
                    "history snapshot version root presence is unsupported",
                ));
            }
        };
        let len = cursor.take_u64()?;
        // Every logical state is non-empty: a rootless expired version
        // retains the nonzero length it had before reclamation.
        if len == 0 {
            return Err(HistoryError::Invalid(
                "history snapshot version length is zero",
            ));
        }
        if lifecycle == VersionLifecycle::Retained && !root_present {
            return Err(HistoryError::Invalid(
                "history snapshot retained version has no materialized root",
            ));
        }
        let root = if root_present {
            let descriptor = cursor.take_array::<V2_ROOT_RECORD_SIZE>()?;
            // Canonical shape now: length agreement against `len` stays
            // import's job (it owns the catalogue context).
            decode_canonical_root_bytes(&descriptor).map_err(|_| {
                HistoryError::Invalid("history snapshot root descriptor is non-canonical")
            })?;
            Some(descriptor)
        } else {
            None
        };
        versions.push(SnapshotVersion {
            history: HistoryId::new(history),
            parent,
            binding,
            lifecycle,
            root_present,
            len,
            root,
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
    let mut receipt_order: Vec<Vec<u8>> = Vec::new();
    receipt_order
        .try_reserve_exact(receipt_order_count)
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for _ in 0..receipt_order_count {
        receipt_order.push(cursor.take_request_id()?);
    }
    validate_snapshot_receipt_consistency(&versions, &active, &retired, &receipt_order)?;
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
        physical_generation,
        payload_end,
        node_count,
        next_history_id,
        next_version_id,
        history_bindings,
        versions,
        active,
        retired,
        receipt_order,
    })
}

/// Validates the bounded receipt horizon as a whole: capacity, exact
/// two-way agreement between the order and the ledgers, ledger disjointness,
/// and the live-execution invariant that every active receipt resolves to a
/// known retained version with no version claimed twice.
///
/// Shared by decode (wire bytes) and import (already-built structs, which
/// may not have come from decode), so forged structs fail closed on both
/// paths.
pub(crate) fn validate_snapshot_receipt_consistency(
    versions: &[SnapshotVersion],
    active: &[SnapshotActive],
    retired: &[SnapshotRetired],
    receipt_order: &[Vec<u8>],
) -> Result<(), HistoryError> {
    if receipt_order.len() > STAGING_REQUEST_RECEIPT_CAPACITY {
        return Err(HistoryError::Invalid(
            "history snapshot receipt horizon exceeds its staging capacity",
        ));
    }
    let mut ordered: HashSet<&[u8]> = HashSet::new();
    ordered
        .try_reserve(receipt_order.len())
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for id in receipt_order {
        if !ordered.insert(id.as_slice()) {
            return Err(HistoryError::Invalid(
                "history snapshot receipt order is duplicated",
            ));
        }
    }
    if ordered.len() != active.len() + retired.len() {
        return Err(HistoryError::Invalid(
            "history snapshot receipt order disagrees with its ledgers",
        ));
    }
    let mut active_ids: HashSet<&[u8]> = HashSet::new();
    active_ids
        .try_reserve(active.len())
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for record in active {
        if !ordered.contains(record.request_id.as_slice()) {
            return Err(HistoryError::Invalid(
                "history snapshot active request is absent from its receipt order",
            ));
        }
        if !active_ids.insert(record.request_id.as_slice()) {
            return Err(HistoryError::Invalid(
                "history snapshot active request identity is duplicated",
            ));
        }
    }
    for record in retired {
        if !ordered.contains(record.request_id.as_slice()) {
            return Err(HistoryError::Invalid(
                "history snapshot retired request is absent from its receipt order",
            ));
        }
        if active_ids.contains(record.request_id.as_slice()) {
            return Err(HistoryError::Invalid(
                "history snapshot request identity is both active and retired",
            ));
        }
    }
    // Every active receipt must resolve to a known retained version claimed
    // by no other active receipt: expiring a result retires its receipt, so
    // a valid snapshot never parks an active receipt on an expired version,
    // and live execution creates at most one receipt per version.
    let mut claimed: HashSet<u64> = HashSet::new();
    claimed
        .try_reserve(active.len())
        .map_err(|_| HistoryError::Capacity("history snapshot table allocation failed"))?;
    for record in active {
        let entry = versions
            .get(record.version.id() as usize)
            .ok_or(HistoryError::Invalid(
                "history snapshot active request references a missing version",
            ))?;
        if entry.lifecycle != VersionLifecycle::Retained {
            return Err(HistoryError::Invalid(
                "history snapshot active request references an expired version",
            ));
        }
        if !claimed.insert(record.version.id()) {
            return Err(HistoryError::Invalid(
                "history snapshot active requests claim one version twice",
            ));
        }
    }
    Ok(())
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
        store.append(history, None, b"data", None, None).unwrap();
        let bytes = encode_history_snapshot(&store, 3, 128).unwrap();
        for end in [0, 1, 7, 119, 120, 121, bytes.len() - 33, bytes.len() - 1] {
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
            .append(first, None, b"aaa", Some(b"req-a"), Some(b"bind-a0"))
            .unwrap()
        {
            crate::persistent_history::CommitOutcome::Committed(version) => version,
            crate::persistent_history::CommitOutcome::Replayed(_)
            | crate::persistent_history::CommitOutcome::Retired => {
                panic!("fixture commit must create")
            }
        };
        store
            .append(first, Some(v0.id()), b"bbb", Some(b"req-b"), None)
            .unwrap();
        store.append(second, None, b"zzz", None, None).unwrap();
        store.retire_request(b"req-a").unwrap();
        store
    }

    #[test]
    fn schema6_snapshot_carries_descriptors_and_frontiers_without_image() {
        // Memory backends report live arena sizes as frontiers; every
        // materialized version carries a canonical root descriptor whose
        // length agrees with the catalogue.
        let source = fixture_store();
        let bytes = encode_history_snapshot(&source, 9, 4096).unwrap();
        let snapshot = decode_history_snapshot(&bytes).unwrap();
        assert_eq!(snapshot.versions.len(), 3);
        assert_eq!(snapshot.payload_end, source.backend.payload_len());
        assert_eq!(snapshot.node_count, source.backend.node_count());
        for entry in &snapshot.versions {
            assert!(entry.root_present);
            let descriptor = entry.root.expect("retained version must describe its root");
            let root = decode_canonical_root_bytes(&descriptor).unwrap();
            assert_eq!(root.logical_len().get(), entry.len);
        }
        // Struct round trip is exact, including descriptors and frontiers.
        let reencoded = encode_history_snapshot_struct(&snapshot).unwrap();
        assert_eq!(decode_history_snapshot(&reencoded).unwrap(), snapshot);
    }

    #[test]
    fn schema5_bytes_fail_closed_at_the_schema_gate() {
        // The previous epoch embeds an arena image and no physical epoch:
        // patching the schema byte back to 5 with a recomputed digest must
        // still fail — the epoch gate, not integrity, rejects it.
        let bytes = encode_history_snapshot(&fixture_store(), 1, 0).unwrap();
        // Header: magic[4] + total_len[8] + schema[4].
        assert_eq!(&bytes[..4], b"THS1");
        let mut forged = bytes.clone();
        forged[12..16].copy_from_slice(&5u32.to_le_bytes());
        let digest_start = forged.len() - 32;
        let mut hasher = Sha256::new();
        hasher.update(HISTORY_SNAPSHOT_DIGEST_DOMAIN);
        hasher.update(&forged[..digest_start]);
        let digest = hasher.finalize();
        forged[digest_start..].copy_from_slice(&digest);
        assert!(matches!(
            decode_history_snapshot(&forged),
            Err(HistoryError::Invalid(
                "history snapshot schema is unsupported"
            ))
        ));
    }

    #[test]
    fn noncanonical_root_descriptor_fails_decode_with_valid_integrity() {
        // Flip a byte inside the first version's root descriptor, then
        // recompute the artifact digest: integrity passes, so only the
        // descriptor gate can reject — proving the gate is load-bearing.
        let bytes = encode_history_snapshot(&fixture_store(), 1, 0).unwrap();
        let honest = decode_history_snapshot(&bytes).unwrap();
        assert!(honest.versions[0].root.is_some());
        // Locate the first version's descriptor by its known bytes rather
        // than fragile hand arithmetic: presence u8 + len u64 sit in the 9
        // bytes immediately before it.
        let descriptor = honest.versions[0].root.expect("fixture root is described");
        let descriptor_offset = bytes
            .windows(V2_ROOT_RECORD_SIZE)
            .position(|window| window == descriptor)
            .expect("descriptor bytes must appear in the artifact");
        let mut forged = bytes.clone();
        forged[descriptor_offset] ^= 0xFF;
        let digest_start = forged.len() - 32;
        let mut hasher = Sha256::new();
        hasher.update(HISTORY_SNAPSHOT_DIGEST_DOMAIN);
        hasher.update(&forged[..digest_start]);
        let digest = hasher.finalize();
        forged[digest_start..].copy_from_slice(&digest);
        assert!(matches!(
            decode_history_snapshot(&forged),
            Err(HistoryError::Invalid(_))
        ));
    }

    #[test]
    fn root_presence_descriptor_disagreement_fails_struct_encode() {
        // Presence byte and descriptor must agree structurally, before any
        // semantic check runs.
        let honest =
            decode_history_snapshot(&encode_history_snapshot(&fixture_store(), 1, 0).unwrap())
                .unwrap();
        let mut forged = honest.clone();
        forged.versions[0].root = None;
        assert!(matches!(
            encode_history_snapshot_struct(&forged),
            Err(HistoryError::Invalid(
                "history snapshot version root presence disagrees with its descriptor"
            ))
        ));
        let mut forged = honest;
        forged.versions[0].root_present = false;
        assert!(matches!(
            encode_history_snapshot_struct(&forged),
            Err(HistoryError::Invalid(
                "history snapshot version root presence disagrees with its descriptor"
            ))
        ));
    }

    #[test]
    fn rootless_retained_version_fails_decode_with_valid_integrity() {
        // Clearing the presence byte (keeping the descriptor out and the
        // digest recomputed) fails at decode: retained versions must
        // materialize.
        let bytes = encode_history_snapshot(&fixture_store(), 1, 0).unwrap();
        let honest = decode_history_snapshot(&bytes).unwrap();
        // The presence byte sits 9 bytes before the descriptor (presence u8
        // + len u64): locate the descriptor by content, not arithmetic.
        let descriptor = honest.versions[0].root.expect("fixture root is described");
        let presence_offset = bytes
            .windows(V2_ROOT_RECORD_SIZE)
            .position(|window| window == descriptor)
            .expect("descriptor bytes must appear in the artifact")
            - 9;
        assert_eq!(bytes[presence_offset], 1);
        // Presence lives inside a length-delimited region, so clearing it
        // without removing the descriptor breaks framing: rebuild at the
        // struct level instead, with valid integrity.
        let mut forged = honest;
        forged.versions[0].root_present = false;
        forged.versions[0].root = None;
        let forged_bytes = encode_history_snapshot_struct(&forged).unwrap();
        assert!(matches!(
            decode_history_snapshot(&forged_bytes),
            Err(HistoryError::Invalid(
                "history snapshot retained version has no materialized root"
            ))
        ));
    }

    #[test]
    fn empty_snapshot_round_trips_without_content() {
        let source = PersistentHistoryStore::new();
        let bytes = encode_history_snapshot(&source, 0, 0).unwrap();
        let snapshot = decode_history_snapshot(&bytes).unwrap();
        assert_eq!(snapshot.next_history_id, 0);
        assert_eq!(snapshot.next_version_id, 0);
        assert!(snapshot.versions.is_empty());
        assert_eq!(snapshot.payload_end, 0);
        assert_eq!(snapshot.node_count, 0);
        assert_eq!(
            decode_history_snapshot(&encode_history_snapshot_struct(&snapshot).unwrap()).unwrap(),
            snapshot
        );
    }

    #[test]
    fn cross_history_parent_fails_closed_with_valid_integrity() {
        // History 0 owns V0, history 1 owns V1 as a root. Grafting V1 under
        // V0 by struct mutation keeps every structural rule and the digest
        // valid, so only the lineage rule can reject — the exact lineage the
        // live API could never create.
        let mut store = PersistentHistoryStore::new();
        let first = store.create_history().unwrap();
        let second = store.create_history().unwrap();
        store.append(first, None, b"aaa", None, None).unwrap();
        store.append(second, None, b"bbb", None, None).unwrap();
        let bytes = encode_history_snapshot(&store, 1, 64).unwrap();
        let honest = decode_history_snapshot(&bytes).unwrap();
        assert_eq!(honest.versions.len(), 2);
        let mut forged = honest;
        forged.versions[1].parent = Some(VersionId::new(0));
        let forged_bytes = encode_history_snapshot_struct(&forged).unwrap();
        assert!(matches!(
            decode_history_snapshot(&forged_bytes),
            Err(HistoryError::Invalid(_))
        ));
    }

    #[test]
    fn invalid_lifecycle_byte_fails_closed_with_valid_integrity() {
        // Patch a version lifecycle byte to a reserved value and recompute
        // the artifact digest: integrity passes, so only the lifecycle gate
        // can reject — proving the gate is load-bearing, not the digest.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        store.append(history, None, b"data", None, None).unwrap();
        let bytes = encode_history_snapshot(&store, 1, 0).unwrap();
        let honest = decode_history_snapshot(&bytes).unwrap();
        assert_eq!(honest.versions.len(), 1);
        // Versions section starts after the 120-byte header plus the history
        // table (one entry, no binding: present-flag u64 = 8 bytes): the
        // entry is history u64 + parent u64 + binding u64 + lifecycle u8.
        let lifecycle_offset = HISTORY_SNAPSHOT_HEADER_SIZE + 8 + 8 + 8 + 8;
        assert_eq!(bytes[lifecycle_offset], 0);
        let mut forged = bytes.clone();
        forged[lifecycle_offset] = 2;
        let digest_start = forged.len() - 32;
        let mut hasher = Sha256::new();
        hasher.update(HISTORY_SNAPSHOT_DIGEST_DOMAIN);
        hasher.update(&forged[..digest_start]);
        let digest = hasher.finalize();
        forged[digest_start..].copy_from_slice(&digest);
        assert!(matches!(
            decode_history_snapshot(&forged),
            Err(HistoryError::Invalid(_))
        ));
    }

    #[test]
    fn malformed_version_binding_fails_closed_with_valid_integrity() {
        // Patch a version binding length to empty (present flag with zero
        // length) and recompute the artifact digest: only the binding gate
        // can reject.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        store
            .append(history, None, b"data", None, Some(b"bind-1"))
            .unwrap();
        let bytes = encode_history_snapshot(&store, 1, 0).unwrap();
        // Versions section starts after the 120-byte header plus the history
        // table (one entry, no binding: 8 bytes): entry opens with history
        // u64 + parent u64, then the binding length u64.
        let binding_len_offset = HISTORY_SNAPSHOT_HEADER_SIZE + 8 + 8 + 8;
        let stored = u64::from_le_bytes(
            bytes[binding_len_offset..binding_len_offset + 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(stored, b"bind-1".len() as u64 + 1);
        let mut forged = bytes.clone();
        forged[binding_len_offset..binding_len_offset + 8].copy_from_slice(&1u64.to_le_bytes());
        let digest_start = forged.len() - 32;
        let mut hasher = Sha256::new();
        hasher.update(HISTORY_SNAPSHOT_DIGEST_DOMAIN);
        hasher.update(&forged[..digest_start]);
        let digest = hasher.finalize();
        forged[digest_start..].copy_from_slice(&digest);
        assert!(matches!(
            decode_history_snapshot(&forged),
            Err(HistoryError::Invalid(_))
        ));
    }

    #[test]
    fn duplicate_history_binding_fails_closed_with_valid_integrity() {
        let mut store = PersistentHistoryStore::new();
        store.create_history_with_binding(b"thread-a").unwrap();
        store.create_history_with_binding(b"thread-b").unwrap();
        let bytes = encode_history_snapshot(&store, 1, 0).unwrap();
        let honest = decode_history_snapshot(&bytes).unwrap();
        let mut forged = honest;
        forged.history_bindings[1] = forged.history_bindings[0].clone();
        let forged_bytes = encode_history_snapshot_struct(&forged).unwrap();
        assert!(matches!(
            decode_history_snapshot(&forged_bytes),
            Err(HistoryError::Invalid(_))
        ));
    }
}
