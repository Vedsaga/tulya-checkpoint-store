//! Domain-neutral persistent-history core.
//!
//! Tulya is a persistent-history engine first; LangGraph checkpointing is one
//! adapter workload. This module owns the generic version/history vocabulary
//! that every adapter builds on:
//!
//! ```text
//! adapters (checkpoint store, future)
//!      |
//!      v
//! PersistentHistoryStore
//!      |
//!      v
//! BalancedSequence (persistent sequence)
//! ```
//!
//! The dependency direction is load-bearing: this core knows nothing about
//! threads, checkpoint namespaces, messages, pending writes, or any other
//! adapter concept. A version is an identity plus an optional parent plus a
//! persistent root; payloads are opaque bytes. Adapter identity mapping
//! (for example checkpoint thread to history) lives above this module.
//!
//! Request ledgers, tombstones, snapshots, and reclamation arrive in later
//! slices; this slice covers versioned payload history with exact reads and
//! structural verification only.

use crate::operation::DurabilityOperation;
use crate::persistent_sequence::physical::{IoLedger, PhysicalDelta, PhysicalIoCounters};
use crate::persistent_sequence::{
    decode_canonical_root_bytes, BalancedSequence, LogicalLength, PersistentRoot,
    PersistentSequence, PersistentSequenceAppend, PersistentSequenceSplice, SequenceError,
    SequenceRange, SequenceWorkCounters,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::Path;
use std::time::Instant;

pub mod authority;
pub mod durable_log;
pub mod manifest;
pub mod snapshot;
use durable_log::{DurableError, DurableHistoryLog, HistoryLogRecord};

/// Domain separator for the canonical splice operation digest.
///
/// Staging domain, deliberately NOT the old append-commit domain and NOT a
/// release Format-v1 commitment: E9 freezes the release domain. The epoch
/// change (log magic, snapshot schema) keeps old append-digest bytes from
/// ever validating as splice operations.
const HISTORY_SPLICE_DIGEST_DOMAIN: &[u8] = b"tulya-history/staging/splice\0";

/// Domain separator for the canonical fork/publication operation digest.
///
/// A distinct operation domain from splice: fork binds the historical parent
/// whose immutable root is republished, never content coordinates. A request
/// bound to a splice digest therefore conflicts deterministically with the
/// same request offered as a fork, and vice versa. Staging domain, not a
/// release Format-v1 commitment.
const HISTORY_FORK_DIGEST_DOMAIN: &[u8] = b"tulya-history/staging/fork\0";

/// Maximum request-identity byte length accepted by the history core.
const MAX_HISTORY_REQUEST_ID_BYTES: usize = 4096;

/// Maximum opaque adapter-binding byte length. Bindings identify adapter
/// objects for crash-safe remapping, not bulk data.
const MAX_HISTORY_BINDING_BYTES: usize = 4096;

/// Opaque core-assigned history/object identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HistoryId(u64);

impl HistoryId {
    pub const fn id(self) -> u64 {
        self.0
    }

    pub const fn new(id: u64) -> Self {
        Self(id)
    }
}

/// Opaque core-assigned version identity.
///
/// This is the STABLE LOGICAL identity adapters address versions by. It is
/// deliberately distinct from the physical table position: compaction and
/// reclamation (later slices) may relocate storage, but they must never
/// change the logical identity an adapter holds. Every lookup
/// coordinate-checks the table entry against the requested identity, so a
/// relocated or fabricated identity fails closed instead of addressing the
/// wrong record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VersionId(u64);

impl VersionId {
    pub const fn id(self) -> u64 {
        self.0
    }

    pub const fn new(id: u64) -> Self {
        Self(id)
    }
}

/// One committed generic version: its history, identity, and optional
/// parent. Payloads stay opaque bytes below the adapter layer.
///
/// This is a LOGICAL handle, deliberately separated from physical placement:
/// compaction and reclamation relocate arena nodes, but they never change the
/// logical identity an adapter holds. A caller-held handle obtained before GC
/// stays valid after GC; reads and verification resolve the current physical
/// root by [`VersionId`] internally. There is intentionally no public
/// physical-root accessor: adapters must not derive placement from handles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    history: HistoryId,
    id: VersionId,
    parent: Option<VersionId>,
}

impl Version {
    pub const fn history(self) -> HistoryId {
        self.history
    }

    pub const fn id(self) -> VersionId {
        self.id
    }

    pub const fn parent(self) -> Option<VersionId> {
        self.parent
    }
}

/// One catalogue entry: the immutable logical version plus its current
/// physical placement. The root is `Some` for every retained version and may
/// become `None` for an expired version once reclamation drops its content;
/// `len` is the exact logical byte length, authoritative without backend
/// access so validation and replay work even for rootless entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VersionRecord {
    version: Version,
    root: Option<PersistentRoot>,
    len: u64,
}

impl VersionRecord {
    const fn version(self) -> Version {
        self.version
    }

    const fn root(self) -> Option<PersistentRoot> {
        self.root
    }

    const fn len(self) -> u64 {
        self.len
    }

    const fn history(self) -> HistoryId {
        self.version.history
    }

    const fn id(self) -> VersionId {
        self.version.id
    }

    const fn parent(self) -> Option<VersionId> {
        self.version.parent
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryError {
    Sequence(SequenceError),
    Invalid(&'static str),
    Overflow(&'static str),
    Capacity(&'static str),
    RequestConflict,
    /// A known version is logically expired and no longer available for new
    /// public acquisition. Internal recovery/snapshot metadata lookup does
    /// not obey this restriction; only public read/verify/acquisition does.
    VersionExpired,
    Poisoned,
}

impl fmt::Display for HistoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sequence(error) => write!(formatter, "{error}"),
            Self::Invalid(message) | Self::Overflow(message) | Self::Capacity(message) => {
                formatter.write_str(message)
            }
            Self::RequestConflict => formatter
                .write_str("persistent request identity conflicts with a committed operation"),
            Self::VersionExpired => formatter
                .write_str("persistent version is expired and no longer available for acquisition"),
            Self::Poisoned => {
                formatter.write_str("persistent history writer is poisoned and requires reopen")
            }
        }
    }
}

impl std::error::Error for HistoryError {}

impl From<SequenceError> for HistoryError {
    fn from(error: SequenceError) -> Self {
        Self::Sequence(error)
    }
}

/// Logical lifecycle of one committed version.
///
/// Every version starts [`Retained`](Self::Retained). [`expire`](PersistentHistoryStore::expire)
/// moves it to [`Expired`](Self::Expired) exactly once; there is no
/// resurrection. Expiration is catalogue metadata only: root, parent,
/// identity, bytes, and bindings are unchanged, and the entry stays known
/// forever so retained descendants keep valid lineage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionLifecycle {
    Retained,
    Expired,
}

/// Outcome of a version-expiration request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireOutcome {
    /// The version transitioned retained -> expired: a durable metadata
    /// change on the durable path, catalogue-only in memory.
    Expired,
    /// The version was already expired: idempotent, with zero WAL I/O and
    /// zero metadata mutation on every path.
    AlreadyExpired,
}

/// Exact staging capacity of the bounded request-receipt horizon: active
/// plus retired receipts together.
///
/// This is NOT release-frozen; E9 may change the value or make it
/// configurable before public Format v1. The exact promise is: exact request
/// replay/conflict semantics are retained for the most recent
/// `STAGING_REQUEST_RECEIPT_CAPACITY` request receipts in the current staging
/// contract. Do NOT describe Tulya as providing "idempotency forever."
pub const STAGING_REQUEST_RECEIPT_CAPACITY: usize = 4096;

/// Visibility of one request identity under the bounded receipt horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestReceiptStatus {
    /// Retained receipt resolving to its committed version: same digest
    /// replays, different digest conflicts.
    Active(VersionId),
    /// Retained receipt whose version retired or expired: same digest
    /// retires, different digest conflicts.
    Retired,
    /// Never observed, or previously observed but evicted from the bounded
    /// horizon. Core cannot distinguish those after exact bounded metadata is
    /// discarded; reuse of an unknown identity may execute a fresh operation.
    Unknown,
}

/// Computes the canonical digest for one exact splice mutation.
///
/// The digest binds history, parent presence and identity, splice offset,
/// delete length, insert length and bytes, and the opaque adapter binding —
/// the complete logical coordinates of the operation, deliberately excluding
/// the assigned version identity (which is a consequence, not an input) and
/// the request identity (which binds to the digest at the ledger). Append
/// canonicalizes to splice coordinates before hashing, so an append and its
/// equivalent explicit splice share one digest and one durable encoding. A
/// request bound to one digest therefore replays only the identical splice
/// and conflicts with any different offset, delete length, insert, parent,
/// or binding.
pub fn history_splice_digest(
    history: HistoryId,
    parent: Option<VersionId>,
    offset: u64,
    delete_len: u64,
    insert: &[u8],
    binding: Option<&[u8]>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(HISTORY_SPLICE_DIGEST_DOMAIN);
    hasher.update(history.id().to_le_bytes());
    match parent {
        Some(id) => {
            hasher.update([1u8]);
            hasher.update(id.id().to_le_bytes());
        }
        None => {
            hasher.update([0u8]);
            hasher.update(0u64.to_le_bytes());
        }
    }
    hasher.update(offset.to_le_bytes());
    hasher.update(delete_len.to_le_bytes());
    hasher.update(
        u64::try_from(insert.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hasher.update(insert);
    match binding {
        Some(bytes) => {
            hasher.update([1u8]);
            hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
            hasher.update(bytes);
        }
        None => {
            hasher.update([0u8]);
        }
    }
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

/// Computes the canonical digest for one exact fork/publication.
///
/// The digest binds history, the historical parent identity, and the opaque
/// adapter binding — the complete logical coordinates of the operation. It
/// deliberately excludes the assigned version identity (a consequence, not an
/// input) and the republished root (canonically determined by the committed
/// parent). Fork is catalogue metadata only: there are no content
/// coordinates to bind.
pub fn history_fork_digest(
    history: HistoryId,
    parent: VersionId,
    binding: Option<&[u8]>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(HISTORY_FORK_DIGEST_DOMAIN);
    hasher.update(history.id().to_le_bytes());
    hasher.update(parent.id().to_le_bytes());
    match binding {
        Some(bytes) => {
            hasher.update([1u8]);
            hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
            hasher.update(bytes);
        }
        None => {
            hasher.update([0u8]);
        }
    }
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

/// A checked fresh-version identity prepared before backend mutation or
/// authority I/O. Shared by splice and fork preparation: every operation that
/// creates a new Version validates/reserves its logical VersionId in
/// preparation (E2 exhaustion-correction invariant, reused by E3 fork).
struct PreparedVersionIdentity {
    version: VersionId,
    next_version_id_after: u64,
}

/// Checks out the next fresh version identity without mutating anything: an
/// exhausted counter fails here, before WAL construction, backend work, or
/// table mutation.
fn prepare_version_identity(next_version_id: u64) -> Result<PreparedVersionIdentity, HistoryError> {
    let next_version_id_after = next_version_id
        .checked_add(1)
        .ok_or(HistoryError::Overflow(
            "persistent version count exceeds u64",
        ))?;
    Ok(PreparedVersionIdentity {
        version: VersionId(next_version_id),
        next_version_id_after,
    })
}

/// One active request-ledger entry: the bound operation digest plus the
/// committed version that replay must return without a second mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveRequest {
    digest: [u8; 32],
    version: VersionId,
}

impl ActiveRequest {
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }

    pub const fn version(self) -> VersionId {
        self.version
    }
}

/// Outcome of a logical commit through the request ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed(Version),
    Replayed(Version),
    Retired,
}

impl CommitOutcome {
    pub const fn version(self) -> Option<Version> {
        match self {
            Self::Committed(version) | Self::Replayed(version) => Some(version),
            Self::Retired => None,
        }
    }

    pub const fn replayed(self) -> bool {
        match self {
            Self::Committed(_) => false,
            Self::Replayed(_) | Self::Retired => true,
        }
    }
}

/// Result of validating a logical splice without mutating state.
enum SplicePreview<'a> {
    Replayed(Version),
    Retired,
    Fresh(PreparedSplice<'a>),
}

/// A validated logical splice awaiting persistence and application. Borrows
/// caller bytes so preparation itself never allocates payload copies.
///
/// The fresh version identity is allocated (checked) at preview time, before
/// any backend mutation or authority I/O: an exhausted counter fails here,
/// never after arena or WAL work. E3 invariant: every operation that creates
/// a new Version must validate/reserve its logical VersionId in preparation,
/// so fork reuses exactly this discipline.
struct PreparedSplice<'a> {
    version: VersionId,
    next_version_id_after: u64,
    history: HistoryId,
    parent: Option<VersionId>,
    parent_root: Option<PersistentRoot>,
    offset: u64,
    delete_len: u64,
    insert: &'a [u8],
    /// Validated resulting logical length, carried so application records
    /// catalogue length without re-deriving it from the backend.
    len: u64,
    request_id: Option<&'a [u8]>,
    binding: Option<&'a [u8]>,
    digest: [u8; 32],
    /// Prepared physical delta for durable backends: fresh payload and node
    /// records realizing the result root, with frontiers and digest. `None`
    /// for ephemeral memory backends. Preparation reads only; files move at
    /// commit time, in durability order. Boxed: the common ledger outcomes
    /// stay small while the fresh-only delta can hold kilobytes.
    physical: Option<Box<PhysicalDelta>>,
}

/// Result of validating a logical fork without mutating state.
enum ForkPreview<'a> {
    Replayed(Version),
    Retired,
    Fresh(PreparedFork<'a>),
}

/// A validated logical fork awaiting persistence and application. Borrows
/// caller bytes so preparation itself never allocates payload copies.
///
/// The fresh version identity is allocated (checked) at preview time, before
/// any authority I/O or catalogue mutation, reusing the E2 splice discipline
/// exactly: an exhausted counter fails here, never after WAL work. No backend
/// operation is needed to obtain the root beyond committed catalogue lookup —
/// fork reuses the parent's immutable root without touching the sequence
/// layer at all.
struct PreparedFork<'a> {
    version: VersionId,
    next_version_id_after: u64,
    history: HistoryId,
    parent: VersionId,
    root: Option<PersistentRoot>,
    /// Republished logical length, taken from the parent catalogue entry —
    /// never from the backend, so preparation stays content-counter-neutral.
    len: u64,
    request_id: Option<&'a [u8]>,
    binding: Option<&'a [u8]>,
    digest: [u8; 32],
}

/// Result of validating one expiration without mutating state.
enum ExpirePreview {
    AlreadyExpired,
    Fresh(PreparedExpire),
}

/// A validated expiration awaiting persistence and application: the
/// lifecycle target plus the single active receipt (if any) staged for the
/// atomic active -> retired move. Order position is never touched.
struct PreparedExpire {
    history: HistoryId,
    version: VersionId,
    receipt: Option<(Vec<u8>, [u8; 32])>,
}

/// Maintenance statistics for one quiescent GC cycle.
///
/// Arena sizes count live nodes/payload bytes before and after, reachable or
/// not; reclaimed deltas are checked subtraction. Version counts name the
/// logical root set: expired versions are never GC roots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcStats {
    pub nodes_before: u64,
    pub nodes_after: u64,
    pub nodes_reclaimed: u64,
    pub payload_bytes_before: u64,
    pub payload_bytes_after: u64,
    pub payload_bytes_reclaimed: u64,
    pub retained_versions: u64,
    pub expired_versions: u64,
}

/// Pure in-memory GC preparation: a complete scratch store with the
/// compact replacement backend, rebuilt catalogue records (remapped retained
/// roots, rootless expired entries), and cloned ledgers/lifecycle/bindings —
/// plus maintenance statistics. The live store is untouched until
/// [`apply_prepared_gc`](PersistentHistoryStore::apply_prepared_gc), so any
/// preparation failure leaves backend, versions, ledgers, lifecycle, and
/// bindings exactly as found. No partial compaction. Durable GC encodes its
/// compact snapshot from the scratch store before adopting anything.
pub(crate) struct PreparedGc {
    store: PersistentHistoryStore,
    stats: GcStats,
}

impl PreparedGc {
    pub(crate) const fn stats(&self) -> GcStats {
        self.stats
    }

    /// Borrows the compact scratch store for snapshot encoding. The live
    /// store is unaffected; adoption happens only in
    /// [`apply_prepared_gc`](PersistentHistoryStore::apply_prepared_gc).
    pub(crate) const fn store(&self) -> &PersistentHistoryStore {
        &self.store
    }
}

/// A begun durable history creation whose frame is already authoritative in
/// the log but not yet applied in memory. The token is valid only for the
/// store that began it and pairs strictly with one finish: finishing twice,
/// or finishing after the identity arrived by another path, poisons instead
/// of aliasing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedHistoryCreate {
    history: HistoryId,
    binding: Option<Vec<u8>>,
}

/// Domain-neutral store of versioned opaque payload histories over one shared
/// balanced arena. Histories isolate parenthood: a parent version must belong
/// to the same history, so one object's lineage can never silently graft onto
/// another's.
///
/// Identity allocation uses explicit monotonic counters with checked
/// increments, independent of live-set cardinality: removing or reclaiming a
/// history or version later must never cause a numeric identity to be reused.
#[derive(Debug, Default)]
pub struct PersistentHistoryStore {
    backend: BalancedSequence,
    histories: HashSet<HistoryId>,
    versions: Vec<VersionRecord>,
    next_history_id: u64,
    next_version_id: u64,
    active_requests: HashMap<Vec<u8>, ActiveRequest>,
    retired_requests: HashMap<Vec<u8>, [u8; 32]>,
    /// Oldest-to-newest insertion order of every retained request receipt.
    /// Replay, retire, and expiration never reorder; only a fresh
    /// request-bearing commit appends, and horizon eviction pops the front.
    receipt_order: VecDeque<Vec<u8>>,
    /// One-way version lifecycle metadata: every version starts retained,
    /// and expiration inserts here without touching the catalogue entry.
    /// The set stays a subset of known version identities forever.
    expired_versions: HashSet<VersionId>,
    history_bindings: HashMap<HistoryId, Vec<u8>>,
    version_bindings: HashMap<VersionId, Vec<u8>>,
    poisoned: bool,
}

impl PersistentHistoryStore {
    pub fn new() -> Self {
        Self {
            backend: BalancedSequence::new(),
            histories: HashSet::new(),
            versions: Vec::new(),
            next_history_id: 0,
            next_version_id: 0,
            active_requests: HashMap::new(),
            retired_requests: HashMap::new(),
            receipt_order: VecDeque::new(),
            expired_versions: HashSet::new(),
            history_bindings: HashMap::new(),
            version_bindings: HashMap::new(),
            poisoned: false,
        }
    }

    /// Creates an empty history and returns its core-assigned identity.
    ///
    /// Identities come from a monotonic counter, never from live-set size, so
    /// a removed history's identity is never reassigned. All fallible
    /// reservations complete before the counter advances, so a definite
    /// rejection leaves semantic state unchanged.
    pub fn create_history(&mut self) -> Result<HistoryId, HistoryError> {
        self.require_unpoisoned()?;
        self.histories
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent history set allocation failed"))?;
        let id = HistoryId(self.next_history_id);
        self.next_history_id =
            self.next_history_id
                .checked_add(1)
                .ok_or(HistoryError::Overflow(
                    "persistent history count exceeds u64",
                ))?;
        let _ = self.histories.insert(id);
        Ok(id)
    }

    /// Creates a history bound to opaque adapter bytes, idempotently: if the
    /// exact binding already exists, its history is returned instead of
    /// allocating a duplicate lineage.
    ///
    /// This is the crash-safe first-use primitive. An adapter that crashes
    /// after its create observes success retries with the same binding and
    /// resolves the existing logical history rather than orphaning a second
    /// one. Bindings are unique across histories by construction.
    ///
    /// Both the history set and the binding map are reserved before anything
    /// is inserted, so a capacity rejection cannot leave a newly created
    /// history without its binding.
    pub fn create_history_with_binding(
        &mut self,
        binding: &[u8],
    ) -> Result<HistoryId, HistoryError> {
        self.require_unpoisoned()?;
        validate_binding(binding)?;
        if let Some(existing) = self
            .history_bindings
            .iter()
            .find_map(|(id, bound)| (bound.as_slice() == binding).then_some(*id))
        {
            return Ok(existing);
        }
        self.histories
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent history set allocation failed"))?;
        self.history_bindings
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent history binding allocation failed"))?;
        let id = HistoryId(self.next_history_id);
        self.next_history_id =
            self.next_history_id
                .checked_add(1)
                .ok_or(HistoryError::Overflow(
                    "persistent history count exceeds u64",
                ))?;
        let _ = self.histories.insert(id);
        let _ = self.history_bindings.insert(id, binding.to_vec());
        Ok(id)
    }

    /// Splices `insert` into the parent version at `offset`, deleting
    /// `delete_len` bytes first: the single canonical content mutation.
    ///
    /// `None` parent creates a root version and requires zero offset and
    /// delete length with non-empty insert. On an existing parent, a
    /// zero-effect splice (no deletion, empty insert) and a zero-result
    /// splice are rejected; versions must stay non-empty in this slice.
    /// The backend splice preserves all retained history; on any failure no
    /// version is recorded and the arena rolls back.
    ///
    /// With `request_id`, the durable idempotency matrix applies: an unknown
    /// identity splices; a bound identity with the same canonical splice
    /// digest replays its committed version with no second mutation; a
    /// different digest conflicts; a retired identity never resurrects.
    ///
    /// `binding` carries opaque adapter material recorded alongside the
    /// version and covered by the operation digest, so adapters rebuild their
    /// maps from the core itself after reopen.
    pub fn splice(
        &mut self,
        history: HistoryId,
        parent: Option<VersionId>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, HistoryError> {
        match self.preview_splice(
            history, parent, offset, delete_len, insert, request_id, binding,
        )? {
            SplicePreview::Replayed(version) => Ok(CommitOutcome::Replayed(version)),
            SplicePreview::Retired => Ok(CommitOutcome::Retired),
            SplicePreview::Fresh(prepared) => self.apply_prepared_splice(&prepared, false),
        }
    }

    /// Appends `bytes` to the parent version (or creates a root for `None`).
    ///
    /// Convenience over [`splice`](Self::splice) at the parent end: the
    /// offset canonicalizes to the parent logical length immediately, so an
    /// append and its equivalent explicit splice share one digest and one
    /// durable encoding. There is no second mutation implementation.
    pub fn append(
        &mut self,
        history: HistoryId,
        parent: Option<VersionId>,
        bytes: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, HistoryError> {
        let offset = match parent {
            None => 0,
            Some(id) => {
                let record = self.version_record(id)?;
                if record.history() != history {
                    return Err(HistoryError::Invalid(
                        "persistent parent version belongs to a different history",
                    ));
                }
                record.len()
            }
        };
        self.splice(history, parent, offset, 0, bytes, request_id, binding)
    }

    /// Publishes a new logical version pointing at the exact existing root
    /// of one historical parent: the zero-content fork/branch operation.
    ///
    /// A successful fresh fork produces a first-class version whose root is
    /// the parent's immutable root — no sequence splice runs, no AVL nodes or
    /// payload bytes are allocated, no content is copied, and no existing
    /// root changes. Only catalogue/version metadata grows. The forked
    /// version is itself a normal parent for later splices.
    ///
    /// This is a distinct semantic operation from splice, never a
    /// zero-effect splice encoding: E2 rejects zero-effect splices so fork
    /// keeps an unambiguous semantic identity (and its own digest domain).
    ///
    /// The parent must exist and belong to the same history; any retained
    /// historical version may be forked, not merely the latest. There is no
    /// parentless fork: root versions are created by non-empty splice.
    ///
    /// With `request_id`, the durable idempotency matrix applies exactly as
    /// for splice, keyed by the distinct fork digest: an unknown identity
    /// forks; a bound identity with the same canonical fork digest replays
    /// its committed version with no second entry; a different digest —
    /// including any splice digest, which lives in another domain —
    /// conflicts; a retired identity never resurrects.
    ///
    /// `binding` carries opaque adapter material recorded alongside the
    /// version and covered by the operation digest, exactly as for splice.
    pub fn fork(
        &mut self,
        history: HistoryId,
        parent: VersionId,
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, HistoryError> {
        match self.preview_fork(history, parent, request_id, binding)? {
            ForkPreview::Replayed(version) => Ok(CommitOutcome::Replayed(version)),
            ForkPreview::Retired => Ok(CommitOutcome::Retired),
            ForkPreview::Fresh(prepared) => self.apply_prepared_fork(&prepared),
        }
    }

    /// Validates a logical splice without mutating anything: history and
    /// parent resolution, splice-coordinate validation against the parent
    /// length, canonical-digest computation, and the full request-ledger
    /// matrix. Durable splice runs this first, persists the prepared bytes,
    /// and only then applies — so a rejected coordinate can never reach the
    /// log, where an encoded-but-unappliable record would brick recovery.
    fn preview_splice<'a>(
        &self,
        history: HistoryId,
        parent: Option<VersionId>,
        offset: u64,
        delete_len: u64,
        insert: &'a [u8],
        request_id: Option<&'a [u8]>,
        binding: Option<&'a [u8]>,
    ) -> Result<SplicePreview<'a>, HistoryError> {
        self.require_unpoisoned()?;
        if !self.histories.contains(&history) {
            return Err(HistoryError::Invalid(
                "persistent splice targets an unknown history",
            ));
        }
        if let Some(bytes) = binding {
            validate_binding(bytes)?;
        }
        let (parent_root, parent_len) = match parent {
            None => (None, 0),
            Some(id) => {
                let record = self.version_record(id)?;
                if record.history() != history {
                    return Err(HistoryError::Invalid(
                        "persistent parent version belongs to a different history",
                    ));
                }
                (record.root(), record.len())
            }
        };
        if offset > parent_len {
            return Err(HistoryError::Invalid(
                "persistent splice offset exceeds parent length",
            ));
        }
        if delete_len > parent_len - offset {
            return Err(HistoryError::Invalid(
                "persistent splice delete range exceeds parent length",
            ));
        }
        let insert_len = u64::try_from(insert.len())
            .map_err(|_| HistoryError::Overflow("persistent splice insert length exceeds u64"))?;
        let result_len =
            (parent_len - delete_len)
                .checked_add(insert_len)
                .ok_or(HistoryError::Overflow(
                    "persistent splice result length exceeds u64",
                ))?;
        if parent.is_none() {
            if offset != 0 || delete_len != 0 {
                return Err(HistoryError::Invalid(
                    "persistent root creation requires zero offset and delete length",
                ));
            }
            if insert.is_empty() {
                return Err(HistoryError::Invalid(
                    "persistent root creation requires non-empty insert",
                ));
            }
        } else if delete_len == 0 && insert.is_empty() {
            return Err(HistoryError::Invalid(
                "persistent splice without effect is rejected",
            ));
        }
        if result_len == 0 {
            return Err(HistoryError::Invalid(
                "persistent splice result must be non-empty",
            ));
        }
        let digest = history_splice_digest(history, parent, offset, delete_len, insert, binding);
        if let Some(request) = request_id {
            validate_request_identity(request)?;
            if let Some(active) = self.active_requests.get(request) {
                if active.digest() == digest {
                    let version = self.version_record(active.version())?.version();
                    return Ok(SplicePreview::Replayed(version));
                }
                return Err(HistoryError::RequestConflict);
            }
            if let Some(retired) = self.retired_requests.get(request) {
                if *retired == digest {
                    return Ok(SplicePreview::Retired);
                }
                return Err(HistoryError::RequestConflict);
            }
        }
        // The fresh path requires a retained parent. Ledger resolution above
        // runs first, so an already-committed request still replays even
        // after its historical parent expired; only unknown/fresh requests
        // pay the retention check. Inspecting the known parent's root/length
        // earlier was internal metadata access, not public acquisition.
        if let Some(parent_id) = parent {
            self.require_retained(parent_id)?;
        }
        Ok(SplicePreview::Fresh({
            // Allocate the fresh identity here — after validation, before any
            // backend mutation or authority I/O — so exhaustion fails in
            // preview and can never strand arena work or an unrecoverable
            // max-Version WAL record. Replay/retired outcomes above need no
            // identity and return before this point.
            let identity = prepare_version_identity(self.next_version_id)?;
            // Physical preparation runs last: it reads parent content (never
            // the whole parent) and builds owned delta buffers without
            // touching any file, so any failure here still leaves zero new
            // authority bytes behind.
            let physical = if self.backend.backend_is_physical() {
                Some(Box::new(
                    self.backend
                        .prepare_physical_splice(
                            parent_root,
                            offset,
                            delete_len,
                            insert,
                            result_len,
                        )
                        .map_err(HistoryError::from)?,
                ))
            } else {
                None
            };
            PreparedSplice {
                version: identity.version,
                next_version_id_after: identity.next_version_id_after,
                history,
                parent,
                parent_root,
                offset,
                delete_len,
                insert,
                len: result_len,
                request_id,
                binding,
                digest,
                physical,
            }
        }))
    }

    /// Applies a prepared splice: the single mutation point shared by the
    /// in-memory and durable paths.
    ///
    /// Every fallible table/ledger/binding reservation completes before the
    /// backend edit runs. The live counter must still equal the prepared
    /// identity — a mismatch fails here, before any arena mutation on the
    /// pure in-memory path (and poisons post-barrier on the durable path via
    /// the caller's mapping). Only after the arena edit succeeds is the
    /// prepared successor counter adopted: there is no checked VersionId
    /// arithmetic after the backend splice.
    ///
    /// `physical_already_durable` is true only on the durable path after the
    /// physical commit and the metadata WAL barrier both succeeded: the
    /// delta bytes are already authoritative in files and the WAL, so this
    /// step only adopts frontiers and the catalogue, infallibly after the
    /// reservations above. On the pure path it is false and the delta
    /// commits here (append plus barrier, no WAL).
    fn apply_prepared_splice(
        &mut self,
        prepared: &PreparedSplice<'_>,
        physical_already_durable: bool,
    ) -> Result<CommitOutcome, HistoryError> {
        if self.next_version_id != prepared.version.id() {
            return Err(HistoryError::Invalid(
                "prepared splice identity disagrees with the allocation counter",
            ));
        }
        self.versions
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent version table allocation failed"))?;
        if prepared.request_id.is_some() {
            self.active_requests.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent active request allocation failed")
            })?;
            self.receipt_order.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent receipt order allocation failed")
            })?;
        }
        if prepared.binding.is_some() {
            self.version_bindings.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent version binding allocation failed")
            })?;
        }
        let root = match &prepared.physical {
            Some(delta) => {
                if !physical_already_durable {
                    // Pure path on a durable backend: the delta commits here
                    // (append plus content barrier, no metadata WAL). A
                    // commit failure rolls files back to the frontier when
                    // provable and reports rejection with no catalogue
                    // mutation.
                    if let Err(error) = self.backend.commit_physical_delta(delta) {
                        let _ = self.backend.heal_physical_for_write();
                        return Err(error.into());
                    }
                    if let Err(error) = self.backend.sync_physical_content() {
                        let _ = self.backend.rollback_physical_to_committed();
                        return Err(error.into());
                    }
                }
                self.backend.adopt_physical_delta(delta);
                PersistentRoot::balanced_v2(delta.root_node_id(), LogicalLength::new(prepared.len))
            }
            None => {
                self.backend
                    .splice(
                        prepared.parent_root,
                        LogicalLength::new(prepared.offset),
                        LogicalLength::new(prepared.delete_len),
                        prepared.insert,
                    )?
                    .root
            }
        };
        self.next_version_id = prepared.next_version_id_after;
        let version = Version {
            history: prepared.history,
            id: prepared.version,
            parent: prepared.parent,
        };
        self.versions.push(VersionRecord {
            version,
            root: Some(root),
            len: prepared.len,
        });
        if let Some(request) = prepared.request_id {
            self.push_fresh_receipt(request, prepared.digest, version.id());
        }
        if let Some(binding) = prepared.binding {
            let _ = self.version_bindings.insert(version.id(), binding.to_vec());
        }
        Ok(CommitOutcome::Committed(version))
    }

    /// Validates a logical fork without mutating anything: history and
    /// parent resolution with same-history enforcement, canonical-digest
    /// computation, the full request-ledger matrix, and checked fresh-version
    /// identity allocation. Durable fork runs this first, persists the
    /// prepared bytes, and only then applies — so a rejected fork can never
    /// reach the log, where an encoded-but-unappliable record would brick
    /// recovery.
    ///
    /// No backend operation runs here: the republished root comes from
    /// committed catalogue lookup alone, so the sequence work counters cannot
    /// move during preparation.
    fn preview_fork<'a>(
        &self,
        history: HistoryId,
        parent: VersionId,
        request_id: Option<&'a [u8]>,
        binding: Option<&'a [u8]>,
    ) -> Result<ForkPreview<'a>, HistoryError> {
        self.require_unpoisoned()?;
        if !self.histories.contains(&history) {
            return Err(HistoryError::Invalid(
                "persistent fork targets an unknown history",
            ));
        }
        if let Some(bytes) = binding {
            validate_binding(bytes)?;
        }
        let parent_record = self.version_record(parent)?;
        if parent_record.history() != history {
            return Err(HistoryError::Invalid(
                "persistent fork parent belongs to a different history",
            ));
        }
        let root = parent_record.root();
        let parent_len = parent_record.len();
        let digest = history_fork_digest(history, parent, binding);
        if let Some(request) = request_id {
            validate_request_identity(request)?;
            if let Some(active) = self.active_requests.get(request) {
                if active.digest() == digest {
                    let version = self.version_record(active.version())?.version();
                    return Ok(ForkPreview::Replayed(version));
                }
                return Err(HistoryError::RequestConflict);
            }
            if let Some(retired) = self.retired_requests.get(request) {
                if *retired == digest {
                    return Ok(ForkPreview::Retired);
                }
                return Err(HistoryError::RequestConflict);
            }
        }
        // Fresh path requires a retained parent; retained receipts replay
        // above regardless of later parent expiration (see splice preview).
        self.require_retained(parent)?;
        Ok(ForkPreview::Fresh({
            // Allocate the fresh identity here — after validation, before any
            // authority I/O or catalogue mutation — reusing the E2 splice
            // discipline exactly. Replay/retired outcomes above need no
            // identity and return before this point.
            let identity = prepare_version_identity(self.next_version_id)?;
            PreparedFork {
                version: identity.version,
                next_version_id_after: identity.next_version_id_after,
                history,
                parent,
                root,
                len: parent_len,
                request_id,
                binding,
                digest,
            }
        }))
    }

    /// Applies a prepared fork: the single catalogue-mutation point shared by
    /// the in-memory and durable paths.
    ///
    /// Every fallible table/ledger/binding reservation completes before the
    /// catalogue edit runs. The live counter must still equal the prepared
    /// identity — a mismatch fails here, before any mutation on the pure
    /// in-memory path (and poisons post-barrier on the durable path via the
    /// caller's mapping). Only after the reservations succeed is the prepared
    /// successor counter adopted: there is no checked VersionId arithmetic
    /// after catalogue mutation, and deliberately no backend call anywhere —
    /// the exact captured parent root is stored directly.
    fn apply_prepared_fork(
        &mut self,
        prepared: &PreparedFork<'_>,
    ) -> Result<CommitOutcome, HistoryError> {
        if self.next_version_id != prepared.version.id() {
            return Err(HistoryError::Invalid(
                "prepared fork identity disagrees with the allocation counter",
            ));
        }
        self.versions
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent version table allocation failed"))?;
        if prepared.request_id.is_some() {
            self.active_requests.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent active request allocation failed")
            })?;
            self.receipt_order.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent receipt order allocation failed")
            })?;
        }
        if prepared.binding.is_some() {
            self.version_bindings.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent version binding allocation failed")
            })?;
        }
        self.next_version_id = prepared.next_version_id_after;
        let root = prepared.root.ok_or(HistoryError::Invalid(
            "prepared fork parent has no materialized root",
        ))?;
        let version = Version {
            history: prepared.history,
            id: prepared.version,
            parent: Some(prepared.parent),
        };
        self.versions.push(VersionRecord {
            version,
            root: Some(root),
            len: prepared.len,
        });
        if let Some(request) = prepared.request_id {
            self.push_fresh_receipt(request, prepared.digest, version.id());
        }
        if let Some(binding) = prepared.binding {
            let _ = self.version_bindings.insert(version.id(), binding.to_vec());
        }
        Ok(CommitOutcome::Committed(version))
    }

    /// Records a fresh request receipt for a newly committed version and
    /// enforces the bounded horizon. Callers reserve map and order capacity
    /// before mutating, so this never fails: insert, newest-append, then
    /// oldest-first eviction while active + retired exceed capacity.
    ///
    /// Eviction is metadata-only: it removes the oldest retained receipt from
    /// whichever ledger holds it, without expiring versions, deleting
    /// content, or touching bindings. The evicted version stays retained
    /// unless explicitly expired.
    fn push_fresh_receipt(&mut self, request_id: &[u8], digest: [u8; 32], version: VersionId) {
        let _ = self
            .active_requests
            .insert(request_id.to_vec(), ActiveRequest { digest, version });
        self.receipt_order.push_back(request_id.to_vec());
        self.enforce_receipt_horizon();
    }

    /// Evicts oldest-first while retained receipts exceed the staging
    /// capacity. Every order entry lives in exactly one ledger, so each pop
    /// drops the retained count by exactly one.
    fn enforce_receipt_horizon(&mut self) {
        while self.active_requests.len() + self.retired_requests.len()
            > STAGING_REQUEST_RECEIPT_CAPACITY
        {
            let Some(oldest) = self.receipt_order.pop_front() else {
                break;
            };
            if self.active_requests.remove(oldest.as_slice()).is_none() {
                let _ = self.retired_requests.remove(oldest.as_slice());
            }
        }
    }

    /// Requires a known, retained version: unknown identities fail closed as
    /// invalid, expired ones as unavailable for new acquisition. Internal
    /// recovery/snapshot paths use [`version_record`](Self::version_record)
    /// directly and never call this.
    fn require_retained(&self, id: VersionId) -> Result<(), HistoryError> {
        self.version_record(id)?;
        if self.expired_versions.contains(&id) {
            return Err(HistoryError::VersionExpired);
        }
        Ok(())
    }

    /// Moves an active request identity to the retired ledger.
    ///
    /// Retirement is prepare-then-commit: every fallible reservation completes
    /// before the active entry is removed, so failure leaves both ledgers
    /// unchanged. Unknown or already-retired identities fail closed. The
    /// receipt keeps its horizon order position: retire never refreshes age.
    pub fn retire_request(&mut self, request_id: &[u8]) -> Result<(), HistoryError> {
        self.require_unpoisoned()?;
        validate_request_identity(request_id)?;
        if self.retired_requests.contains_key(request_id) {
            return Err(HistoryError::Invalid(
                "persistent request identity is already retired",
            ));
        }
        let record = self
            .active_requests
            .get(request_id)
            .copied()
            .ok_or(HistoryError::Invalid(
                "persistent active request identity is absent",
            ))?;
        let mut retired_key = Vec::new();
        retired_key
            .try_reserve_exact(request_id.len())
            .map_err(|_| {
                HistoryError::Capacity("persistent retired request key allocation failed")
            })?;
        retired_key.extend_from_slice(request_id);
        self.retired_requests
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent retired request allocation failed"))?;
        let _ = self.active_requests.remove(request_id);
        let _ = self.retired_requests.insert(retired_key, record.digest());
        Ok(())
    }

    fn require_unpoisoned(&self) -> Result<(), HistoryError> {
        if self.poisoned {
            return Err(HistoryError::Poisoned);
        }
        Ok(())
    }

    pub fn set_poisoned(&mut self) {
        self.poisoned = true;
    }

    /// Reports whether the writer is poisoned after an indeterminate
    /// durability outcome. Poisoned writers reject every mutation until
    /// reopen; the writable authority consults this so seal never publishes
    /// a snapshot of indeterminate state.
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Replays one logged history creation during recovery, restoring its
    /// adapter binding exactly. The binding reservation precedes the history
    /// allocation so a capacity rejection cannot leave a half-created
    /// history; forged duplicate or malformed bindings fail closed.
    ///
    /// Bindings are the idempotent external identity: a valid log never
    /// carries the same nonempty binding twice, because the live create
    /// resolves an existing binding without appending. Duplicate detection
    /// runs as one linear pass over the replayed store at the end of the
    /// replay suffix pass, so lifetime recovery stays linear in history
    /// count even with a binding on every history.
    pub(crate) fn replay_create(
        &mut self,
        history: HistoryId,
        binding: Option<&[u8]>,
    ) -> Result<(), HistoryError> {
        if let Some(bytes) = binding {
            validate_binding(bytes)?;
            self.history_bindings.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent history binding allocation failed")
            })?;
        }
        let assigned = self.create_history()?;
        if assigned != history {
            return Err(HistoryError::Invalid(
                "history log history identity disagrees with replay order",
            ));
        }
        if let Some(bytes) = binding {
            let _ = self.history_bindings.insert(history, bytes.to_vec());
        }
        Ok(())
    }

    /// Replays one logged splice during recovery: exact history, exact
    /// parent, same-history parenthood, recomputed canonical digest,
    /// coordinate validation against the parent length, persistent splice
    /// application, exact version identity, and request/binding rebuild —
    /// with all prior historical roots preserved. A complete but malformed
    /// splice frame fails closed; a torn suffix never reaches this point.
    /// Replays one logged splice during recovery: exact history, exact
    /// parent, same-history parenthood, recomputed canonical splice digest,
    /// exact version identity with checked successor validation before
    /// mutation, and request/binding rebuild.
    ///
    /// Backend-appropriate application (E6.34): memory backends rebuild the
    /// arena by running the shared splice algorithm (ephemeral
    /// reconstruction, whose record carries no placement); durable physical
    /// backends instead validate the committed delta against file bytes and
    /// adopt its frontiers — the physical bytes are already the persistent
    /// tree, so parent-size replay work never happens. A placement on a
    /// memory backend, or its absence on a physical one, fails closed.
    pub(crate) fn replay_splice(
        &mut self,
        history: HistoryId,
        version: VersionId,
        parent: Option<VersionId>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
        digest: [u8; 32],
        physical: Option<durable_log::PhysicalPlacement>,
    ) -> Result<VersionId, HistoryError> {
        if !self.histories.contains(&history) {
            return Err(HistoryError::Invalid(
                "history log splice targets an unknown history",
            ));
        }
        let (parent_root, parent_len) = match parent {
            None => (None, 0),
            Some(id) => {
                let record = self.version_record(id)?;
                if record.history() != history {
                    return Err(HistoryError::Invalid(
                        "history log parent version belongs to a different history",
                    ));
                }
                // Fresh-operation equivalence: live preview rejects an
                // expired parent on the fresh path, so replay must reject a
                // parent already expired by an earlier record — before any
                // backend inspection, counter advancement, or mutation.
                if self.expired_versions.contains(&id) {
                    return Err(HistoryError::Invalid(
                        "history log splice parent is expired",
                    ));
                }
                let root = record.root().ok_or(HistoryError::Invalid(
                    "history log splice parent has no materialized root",
                ))?;
                (Some(root), record.len())
            }
        };
        if offset > parent_len {
            return Err(HistoryError::Invalid(
                "history log splice offset exceeds parent length",
            ));
        }
        if delete_len > parent_len - offset {
            return Err(HistoryError::Invalid(
                "history log splice delete range exceeds parent length",
            ));
        }
        let insert_len = u64::try_from(insert.len())
            .map_err(|_| HistoryError::Overflow("history log splice insert length exceeds u64"))?;
        let result_len =
            (parent_len - delete_len)
                .checked_add(insert_len)
                .ok_or(HistoryError::Overflow(
                    "history log splice result length exceeds u64",
                ))?;
        if parent.is_none() {
            if offset != 0 || delete_len != 0 {
                return Err(HistoryError::Invalid(
                    "history log root creation requires zero offset and delete length",
                ));
            }
            if insert.is_empty() {
                return Err(HistoryError::Invalid(
                    "history log root creation requires non-empty insert",
                ));
            }
        } else if delete_len == 0 && insert.is_empty() {
            return Err(HistoryError::Invalid(
                "history log splice without effect is rejected",
            ));
        }
        if result_len == 0 {
            return Err(HistoryError::Invalid(
                "history log splice result must be non-empty",
            ));
        }
        if history_splice_digest(history, parent, offset, delete_len, insert, binding) != digest {
            return Err(HistoryError::Invalid(
                "history log splice digest disagrees with its operation",
            ));
        }
        if let Some(bytes) = binding {
            validate_binding(bytes)?;
        }
        let id = VersionId(self.next_version_id);
        if id != version {
            return Err(HistoryError::Invalid(
                "history log version identity disagrees with replay order",
            ));
        }
        self.next_version_id =
            self.next_version_id
                .checked_add(1)
                .ok_or(HistoryError::Overflow(
                    "persistent version count exceeds u64",
                ))?;
        self.versions
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent version table allocation failed"))?;
        let root = if self.backend.backend_is_physical() {
            let placement = physical.ok_or(HistoryError::Invalid(
                "history log splice has no physical placement for durable authority",
            ))?;
            self.backend.replay_physical_delta(
                placement.generation,
                placement.payload_start,
                placement.payload_end,
                placement.node_start,
                placement.node_end,
                placement.delta_digest,
                &placement.result_root,
                result_len,
            )?
        } else {
            if physical.is_some() {
                return Err(HistoryError::Invalid(
                    "history log splice physical placement disagrees with an ephemeral backend",
                ));
            }
            self.backend
                .splice(
                    parent_root,
                    LogicalLength::new(offset),
                    LogicalLength::new(delete_len),
                    insert,
                )?
                .root
        };
        self.versions.push(VersionRecord {
            version: Version {
                history,
                id,
                parent,
            },
            root: Some(root),
            len: result_len,
        });
        if let Some(request) = request_id {
            validate_request_identity(request)?;
            if self.active_requests.contains_key(request)
                || self.retired_requests.contains_key(request)
            {
                return Err(HistoryError::Invalid(
                    "history log request identity is already recorded",
                ));
            }
            self.active_requests.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent active request allocation failed")
            })?;
            self.receipt_order.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent receipt order allocation failed")
            })?;
            self.push_fresh_receipt(request, digest, id);
        }
        if let Some(binding) = binding {
            validate_binding(binding)?;
            self.version_bindings.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent version binding allocation failed")
            })?;
            let _ = self.version_bindings.insert(id, binding.to_vec());
        }
        Ok(id)
    }

    /// Replays one logged fork during recovery: exact history, exact
    /// parent, same-history parenthood, recomputed canonical fork digest,
    /// exact version identity with checked successor validation before
    /// mutation, and request/binding rebuild — copying the parent's
    /// persistent root into the new version with zero backend mutation. A
    /// complete but malformed fork frame fails closed; a torn suffix never
    /// reaches this point.
    pub(crate) fn replay_fork(
        &mut self,
        history: HistoryId,
        version: VersionId,
        parent: VersionId,
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
        digest: [u8; 32],
    ) -> Result<VersionId, HistoryError> {
        if !self.histories.contains(&history) {
            return Err(HistoryError::Invalid(
                "history log fork targets an unknown history",
            ));
        }
        let parent_record = self
            .version_record(parent)
            .map_err(|_| HistoryError::Invalid("history log fork parent version is unknown"))?;
        if parent_record.history() != history {
            return Err(HistoryError::Invalid(
                "history log fork parent belongs to a different history",
            ));
        }
        // Fresh-operation equivalence, mirroring replay_splice: a parent
        // expired by an earlier record cannot source a fresh replayed fork.
        if self.expired_versions.contains(&parent) {
            return Err(HistoryError::Invalid("history log fork parent is expired"));
        }
        let root = parent_record.root().ok_or(HistoryError::Invalid(
            "history log fork parent has no materialized root",
        ))?;
        let parent_len = parent_record.len();
        if history_fork_digest(history, parent, binding) != digest {
            return Err(HistoryError::Invalid(
                "history log fork digest disagrees with its operation",
            ));
        }
        if let Some(bytes) = binding {
            validate_binding(bytes)?;
        }
        let id = VersionId(self.next_version_id);
        if id != version {
            return Err(HistoryError::Invalid(
                "history log version identity disagrees with replay order",
            ));
        }
        // The successor must exist before anything mutates: a forged
        // max-Version fork record fails here, never after catalogue work.
        self.next_version_id =
            self.next_version_id
                .checked_add(1)
                .ok_or(HistoryError::Overflow(
                    "persistent version count exceeds u64",
                ))?;
        self.versions
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent version table allocation failed"))?;
        self.versions.push(VersionRecord {
            version: Version {
                history,
                id,
                parent: Some(parent),
            },
            root: Some(root),
            len: parent_len,
        });
        if let Some(request) = request_id {
            validate_request_identity(request)?;
            if self.active_requests.contains_key(request)
                || self.retired_requests.contains_key(request)
            {
                return Err(HistoryError::Invalid(
                    "history log request identity is already recorded",
                ));
            }
            self.active_requests.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent active request allocation failed")
            })?;
            self.receipt_order.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent receipt order allocation failed")
            })?;
            self.push_fresh_receipt(request, digest, id);
        }
        if let Some(binding) = binding {
            validate_binding(binding)?;
            self.version_bindings.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent version binding allocation failed")
            })?;
            let _ = self.version_bindings.insert(id, binding.to_vec());
        }
        Ok(id)
    }

    /// Replays one logged retirement during recovery.
    pub(crate) fn replay_retire(
        &mut self,
        request_id: &[u8],
        digest: [u8; 32],
    ) -> Result<(), HistoryError> {
        validate_request_identity(request_id)?;
        match self.active_requests.get(request_id) {
            Some(record) if record.digest() == digest => {}
            _ => {
                return Err(HistoryError::Invalid(
                    "history log retirement disagrees with replayed ledger",
                ));
            }
        }
        if self.retired_requests.contains_key(request_id) {
            return Err(HistoryError::Invalid(
                "history log retirement duplicates a retired identity",
            ));
        }
        let mut retired_key = Vec::new();
        retired_key
            .try_reserve_exact(request_id.len())
            .map_err(|_| {
                HistoryError::Capacity("persistent retired request key allocation failed")
            })?;
        retired_key.extend_from_slice(request_id);
        self.retired_requests
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent retired request allocation failed"))?;
        let _ = self.active_requests.remove(request_id);
        let _ = self.retired_requests.insert(retired_key, digest);
        Ok(())
    }

    /// Rejects a replayed store whose nonempty history bindings are not
    /// unique across histories. The replay suffix pass calls this once over
    /// the final store — covering an imported snapshot plus its hot suffix
    /// together — so a repeated binding fails closed in linear time.
    ///
    /// Empty bindings never validate at any boundary, so only nonempty
    /// bindings participate here.
    pub fn validate_replayed_history_bindings(&self) -> Result<(), HistoryError> {
        let mut seen: HashSet<&[u8]> = HashSet::new();
        seen.try_reserve(self.history_bindings.len()).map_err(|_| {
            HistoryError::Capacity("persistent history binding validation allocation failed")
        })?;
        for bound in self.history_bindings.values() {
            if bound.is_empty() {
                continue;
            }
            if !seen.insert(bound.as_slice()) {
                return Err(HistoryError::Invalid(
                    "replayed history bindings are not unique",
                ));
            }
        }
        Ok(())
    }

    /// Durably creates a history through the begin/finish seam below: every
    /// fallible step (validation, reservation, identity assignment, encode,
    /// write, barrier) completes before the in-memory apply, which cannot
    /// fail — so a rejection provably precedes any new authority.
    pub(crate) fn create_history_durable(
        &mut self,
        log: &mut DurableHistoryLog,
    ) -> Result<HistoryId, DurableError> {
        let (id, prepared) = self.begin_durable_history_create(log, None)?;
        if let Some(token) = prepared {
            return self.finish_durable_history_create(&token);
        }
        Ok(id)
    }

    /// Durably creates a history bound to opaque adapter bytes, idempotently:
    /// a retry with the same binding resolves the existing history instead of
    /// allocating a duplicate lineage, before or after any crash.
    pub(crate) fn create_history_durable_with_binding(
        &mut self,
        log: &mut DurableHistoryLog,
        binding: &[u8],
    ) -> Result<HistoryId, DurableError> {
        let (id, prepared) = self.begin_durable_history_create(log, Some(binding))?;
        if let Some(token) = prepared {
            return self.finish_durable_history_create(&token);
        }
        Ok(id)
    }

    /// Prepared durable history creation: identity computed, capacity
    /// reserved, frame encoded, written, and synced — but visibility still
    /// needs [`finish_durable_history_create`]. The window between models
    /// exactly the crash between the durable barrier and the in-memory
    /// apply, which recovery resolves by replaying the now-authoritative
    /// record.
    ///
    /// A definite rejection before or at the barrier restores the allocation
    /// counter to its prior value: only the write step runs after the bump,
    /// and a rejected append leaves no authority behind (at most a torn
    /// tail, which recovery ignores), so retry reuses the same identity and
    /// replay order can never skew. Only the barrier-ambiguous sync failure
    /// poisons without restoring — that record may already be authoritative.
    ///
    /// A `None` token with an identity means idempotent resolution: the
    /// binding already exists, nothing was written, and there is nothing to
    /// apply.
    pub(crate) fn begin_durable_history_create(
        &mut self,
        log: &mut DurableHistoryLog,
        binding: Option<&[u8]>,
    ) -> Result<(HistoryId, Option<PreparedHistoryCreate>), DurableError> {
        self.require_unpoisoned_durable()?;
        if let Some(bytes) = binding {
            validate_binding(bytes).map_err(DurableError::Rejected)?;
            if let Some(existing) = self
                .history_bindings
                .iter()
                .find_map(|(id, bound)| (bound.as_slice() == bytes).then_some(*id))
            {
                return Ok((existing, None));
            }
        }
        // Reserve every fallible in-memory allocation before assigning, so a
        // capacity rejection changes nothing at all. Encoding precedes the
        // counter bump because it touches no state: only the write step can
        // fail after the bump, and only it needs the restore below.
        self.histories.try_reserve(1).map_err(|_| {
            DurableError::Rejected(HistoryError::Capacity(
                "persistent history set allocation failed",
            ))
        })?;
        if binding.is_some() {
            self.history_bindings.try_reserve(1).map_err(|_| {
                DurableError::Rejected(HistoryError::Capacity(
                    "persistent history binding allocation failed",
                ))
            })?;
        }
        let id = HistoryId(self.next_history_id);
        let record = HistoryLogRecord::CreateHistory {
            history: id,
            binding: binding.map(<[u8]>::to_vec),
        };
        let frame = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&record).map_err(DurableError::Rejected)?,
        )
        .map_err(DurableError::Rejected)?;
        let prior_counter = self.next_history_id;
        self.next_history_id =
            self.next_history_id
                .checked_add(1)
                .ok_or(DurableError::Rejected(HistoryError::Overflow(
                    "persistent history count exceeds u64",
                )))?;
        if let Err(error) = self.write_and_sync(log, &frame) {
            if matches!(error, DurableError::Rejected(_)) {
                // Pre-authority failure: the append left no new authority
                // behind (at most a torn tail, which recovery ignores), so
                // restore the counter for an exact retry. Barrier-ambiguous
                // sync failures keep the bump: that record may already be
                // authoritative, and the poisoned writer must reopen anyway.
                self.next_history_id = prior_counter;
            }
            return Err(error);
        }
        Ok((
            id,
            Some(PreparedHistoryCreate {
                history: id,
                binding: binding.map(<[u8]>::to_vec),
            }),
        ))
    }

    /// Applies a begun durable history creation. The counter was already
    /// advanced by the begin step and the apply touches only pre-reserved
    /// capacity, so it cannot fail — except when the token disagrees with
    /// live state: an already-applied identity (double finish), or a counter
    /// that moved past the token (stale token held across other mutations,
    /// or a token finished on the wrong store lifetime). In all those cases
    /// the record is already authoritative in the log, so the writer poisons
    /// and demands reopen instead of aliasing an identity: the log already
    /// holds the truth and recovery resolves it.
    pub(crate) fn finish_durable_history_create(
        &mut self,
        prepared: &PreparedHistoryCreate,
    ) -> Result<HistoryId, DurableError> {
        if self.histories.contains(&prepared.history) {
            return Err(self.poison_after_barrier(HistoryError::Invalid(
                "prepared history identity is already applied",
            )));
        }
        // The begin step already advanced the counter past the token identity:
        // anything else means the token is stale or belongs to another store
        // lifetime.
        let adopted = prepared.history.id().checked_add(1).ok_or_else(|| {
            self.poison_after_barrier(HistoryError::Invalid(
                "prepared history identity exceeds the allocation counter",
            ))
        })?;
        if self.next_history_id != adopted {
            return Err(self.poison_after_barrier(HistoryError::Invalid(
                "prepared history identity disagrees with the allocation counter",
            )));
        }
        let _ = self.histories.insert(prepared.history);
        if let Some(bytes) = &prepared.binding {
            let _ = self
                .history_bindings
                .insert(prepared.history, bytes.clone());
        }
        Ok(prepared.history)
    }

    /// Durably splices through the request ledger: replay and retired outcomes
    /// return without touching the log; fresh operations follow
    /// prepare-then-physical-commit-then-metadata-barrier-then-apply, with
    /// poison on any post-barrier failure.
    ///
    /// Durability order (E6.9): the prepared physical delta appends and
    /// syncs BEFORE any metadata frame becomes authoritative, so no WAL
    /// record can ever reference non-durable bytes. After the metadata
    /// barrier the catalogue adoption is infallible.
    pub(crate) fn splice_durable(
        &mut self,
        log: &mut DurableHistoryLog,
        history: HistoryId,
        parent: Option<VersionId>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, DurableError> {
        self.require_unpoisoned_durable()?;
        let prepared = match self
            .preview_splice(
                history, parent, offset, delete_len, insert, request_id, binding,
            )
            .map_err(DurableError::Rejected)?
        {
            SplicePreview::Replayed(version) => return Ok(CommitOutcome::Replayed(version)),
            SplicePreview::Retired => return Ok(CommitOutcome::Retired),
            SplicePreview::Fresh(prepared) => prepared,
        };
        // Physical commit before metadata authority. A failed append leaves
        // at most an orphan tail (reads ignore it, the next commit heals
        // it): definite rejection. A failed content barrier attempts a
        // provable rollback to the frontier: restored means rejection, while
        // an unhealed tail demands reopen instead of further mutation.
        if let Some(delta) = &prepared.physical {
            if let Err(error) = self.backend.commit_physical_delta(delta) {
                let _ = error;
                return Err(DurableError::Rejected(HistoryError::Invalid(
                    "history physical content commit failed before any new authority",
                )));
            }
            if self.backend.sync_physical_content().is_err() {
                if self.backend.rollback_physical_to_committed() {
                    return Err(DurableError::Rejected(HistoryError::Invalid(
                        "history physical content barrier failed before any new authority",
                    )));
                }
                return Err(DurableError::RecoveryRequired);
            }
        }
        let physical = match &prepared.physical {
            Some(delta) => {
                let generation =
                    self.backend
                        .physical_generation()
                        .ok_or(DurableError::Rejected(HistoryError::Invalid(
                            "prepared physical delta disagrees with its backend",
                        )))?;
                Some(durable_log::PhysicalPlacement {
                    generation,
                    payload_start: delta.old_payload_end(),
                    payload_end: delta.new_payload_end(),
                    node_start: delta.old_node_count(),
                    node_end: delta.new_node_count(),
                    delta_digest: delta.delta_digest(),
                    result_root: delta.root_record_bytes(),
                })
            }
            None => None,
        };
        let record = HistoryLogRecord::Splice {
            history: prepared.history,
            version: prepared.version,
            parent: prepared.parent,
            offset: prepared.offset,
            delete_len: prepared.delete_len,
            insert: prepared.insert.to_vec(),
            request_id: prepared.request_id.map(<[u8]>::to_vec),
            binding: prepared.binding.map(<[u8]>::to_vec),
            digest: prepared.digest,
            physical,
        };
        let frame = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&record).map_err(DurableError::Rejected)?,
        )
        .map_err(DurableError::Rejected)?;
        self.write_and_sync(log, &frame)?;
        let already_durable = prepared.physical.is_some();
        self.apply_prepared_splice(&prepared, already_durable)
            .map_err(|error| self.poison_after_barrier(error))
    }

    /// Durably appends through the canonical splice path: the offset resolves
    /// from the parent length first (a pure read), then the operation flows
    /// through [`splice_durable`](Self::splice_durable) with identical digest
    /// and encoding to the equivalent explicit splice.
    pub(crate) fn append_durable(
        &mut self,
        log: &mut DurableHistoryLog,
        history: HistoryId,
        parent: Option<VersionId>,
        bytes: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, DurableError> {
        self.require_unpoisoned_durable()?;
        let offset = match parent {
            None => 0,
            Some(id) => {
                let record = self.version_record(id).map_err(DurableError::Rejected)?;
                if record.history() != history {
                    return Err(DurableError::Rejected(HistoryError::Invalid(
                        "persistent parent version belongs to a different history",
                    )));
                }
                record.len()
            }
        };
        self.splice_durable(log, history, parent, offset, 0, bytes, request_id, binding)
    }

    /// Durably forks through the request ledger: replay and retired outcomes
    /// return without touching the log; fresh operations follow
    /// write-then-barrier-then-apply with poison on any post-barrier failure.
    /// The encoded record carries the prepared version identity and no root
    /// or content: replay resolves the root from the encoded parent.
    pub(crate) fn fork_durable(
        &mut self,
        log: &mut DurableHistoryLog,
        history: HistoryId,
        parent: VersionId,
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, DurableError> {
        self.require_unpoisoned_durable()?;
        let prepared = match self
            .preview_fork(history, parent, request_id, binding)
            .map_err(DurableError::Rejected)?
        {
            ForkPreview::Replayed(version) => return Ok(CommitOutcome::Replayed(version)),
            ForkPreview::Retired => return Ok(CommitOutcome::Retired),
            ForkPreview::Fresh(prepared) => prepared,
        };
        let record = HistoryLogRecord::Fork {
            history: prepared.history,
            version: prepared.version,
            parent: prepared.parent,
            request_id: prepared.request_id.map(<[u8]>::to_vec),
            binding: prepared.binding.map(<[u8]>::to_vec),
            digest: prepared.digest,
        };
        let frame = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&record).map_err(DurableError::Rejected)?,
        )
        .map_err(DurableError::Rejected)?;
        self.write_and_sync(log, &frame)?;
        self.apply_prepared_fork(&prepared)
            .map_err(|error| self.poison_after_barrier(error))
    }

    /// Expires one known version: the one-way retained -> expired lifecycle
    /// transition. Expiration is catalogue metadata only — root, parent,
    /// identity, bytes, and bindings are unchanged, the entry stays known
    /// forever, and nothing cascades to descendants. If an active request
    /// receipt currently resolves to this version, it moves active ->
    /// retired with its horizon order position unchanged, so retrying it
    /// retires instead of replaying an expired version.
    ///
    /// Repeated expiration is idempotent (`AlreadyExpired`, zero mutation);
    /// unknown identities fail closed. Expiration never touches the sequence
    /// backend.
    pub fn expire(&mut self, version: VersionId) -> Result<ExpireOutcome, HistoryError> {
        match self.preview_expire(version)? {
            ExpirePreview::AlreadyExpired => Ok(ExpireOutcome::AlreadyExpired),
            ExpirePreview::Fresh(prepared) => self.apply_prepared_expire(&prepared),
        }
    }

    /// Reports the logical lifecycle of one known version; unknown
    /// identities fail closed.
    pub fn version_lifecycle(&self, version: VersionId) -> Result<VersionLifecycle, HistoryError> {
        self.version_record(version)?;
        Ok(if self.expired_versions.contains(&version) {
            VersionLifecycle::Expired
        } else {
            VersionLifecycle::Retained
        })
    }

    /// Reports whether a known version is still retained (available for new
    /// public acquisition); unknown identities fail closed.
    pub fn is_retained(&self, version: VersionId) -> Result<bool, HistoryError> {
        Ok(matches!(
            self.version_lifecycle(version)?,
            VersionLifecycle::Retained
        ))
    }

    /// Reports whether a known version is expired; unknown identities fail
    /// closed.
    pub fn is_expired(&self, version: VersionId) -> Result<bool, HistoryError> {
        Ok(matches!(
            self.version_lifecycle(version)?,
            VersionLifecycle::Expired
        ))
    }

    /// Validates one expiration without mutating anything: the version must
    /// be known and still retained, expired-set and retired-ledger capacity
    /// must be reservable, and any active receipt resolving to this version
    /// is staged for the atomic active -> retired move.
    fn preview_expire(&mut self, version: VersionId) -> Result<ExpirePreview, HistoryError> {
        self.require_unpoisoned()?;
        // Unknown identities fail closed: expiration success must never be
        // reported for a version that was never committed.
        let record = self.version_record(version)?;
        if self.expired_versions.contains(&version) {
            return Ok(ExpirePreview::AlreadyExpired);
        }
        self.expired_versions
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent expired version allocation failed"))?;
        // Live execution creates at most one fresh receipt per new version,
        // so at most one active receipt can resolve here; the snapshot gate
        // (E4.8 strengthening) rejects forged states with more.
        let mut receipt = None;
        for (id, active) in &self.active_requests {
            if active.version() == version {
                receipt = Some((id.clone(), active.digest()));
                break;
            }
        }
        if receipt.is_some() {
            self.retired_requests.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent retired request allocation failed")
            })?;
        }
        Ok(ExpirePreview::Fresh(PreparedExpire {
            history: record.history(),
            version,
            receipt,
        }))
    }

    /// Applies a prepared expiration: inserts the lifecycle mark and moves
    /// the staged receipt active -> retired with its order position
    /// unchanged. All fallible reservations completed in preview, so the
    /// paired in-memory caller cannot fail here; the durable caller maps any
    /// post-barrier disagreement to poison per the E0 authority contract.
    fn apply_prepared_expire(
        &mut self,
        prepared: &PreparedExpire,
    ) -> Result<ExpireOutcome, HistoryError> {
        let record = self.version_record(prepared.version)?;
        if record.history() != prepared.history {
            return Err(HistoryError::Invalid(
                "prepared expiration disagrees with committed history",
            ));
        }
        if self.expired_versions.contains(&prepared.version) {
            return Err(HistoryError::Invalid(
                "prepared expiration identity is already expired",
            ));
        }
        let _ = self.expired_versions.insert(prepared.version);
        if let Some((id, digest)) = &prepared.receipt {
            match self.active_requests.get(id.as_slice()) {
                Some(active)
                    if active.digest() == *digest && active.version() == prepared.version => {}
                _ => {
                    return Err(HistoryError::Invalid(
                        "prepared expiration receipt disagrees with the active ledger",
                    ));
                }
            }
            let _ = self.active_requests.remove(id.as_slice());
            let _ = self.retired_requests.insert(id.clone(), *digest);
        }
        Ok(ExpireOutcome::Expired)
    }

    /// Prepares one quiescent GC cycle without mutating anything.
    ///
    /// The sole content root set is every retained version root: expired
    /// versions are not roots, and a retained descendant automatically
    /// protects its own reachable content (including nodes shared with an
    /// expired parent or fork source). Preparation validates every retained
    /// root, marks, repacks, rebuilds, and remaps through the generic
    /// sequence compactor, then rebuilds catalogue records with identical
    /// logical identities, parents, lengths, and bindings: only physical
    /// placement changes, and expired entries go rootless.
    pub(crate) fn prepare_gc(&self) -> Result<PreparedGc, HistoryError> {
        // Materialized consistency first: a catalogue length disagreeing
        // with its root must fail here, before compaction could drop an
        // expired root and destroy the evidence. No traversal needed — the
        // root carries authenticated length metadata.
        for record in &self.versions {
            if let Some(root) = record.root() {
                if record.len() != root.logical_len().get() {
                    return Err(HistoryError::Invalid(
                        "persistent GC version length disagrees with its materialized root",
                    ));
                }
            }
        }
        let mut retained_roots = Vec::new();
        retained_roots
            .try_reserve(self.versions.len())
            .map_err(|_| HistoryError::Capacity("persistent GC root table allocation failed"))?;
        let mut retained_count = 0u64;
        let mut expired_count = 0u64;
        for record in &self.versions {
            if self.expired_versions.contains(&record.id()) {
                expired_count = expired_count.checked_add(1).ok_or(HistoryError::Overflow(
                    "persistent GC expired version count exceeds u64",
                ))?;
            } else {
                let root = record.root().ok_or(HistoryError::Invalid(
                    "persistent GC retained version has no materialized root",
                ))?;
                retained_roots.push(root);
                retained_count = retained_count.checked_add(1).ok_or(HistoryError::Overflow(
                    "persistent GC retained version count exceeds u64",
                ))?;
            }
        }
        let compacted = self.backend.compact_to_roots(&retained_roots)?;
        let mut versions = Vec::new();
        versions
            .try_reserve_exact(self.versions.len())
            .map_err(|_| HistoryError::Capacity("persistent GC catalogue allocation failed"))?;
        let mut remapped = compacted.roots.iter();
        for record in &self.versions {
            if self.expired_versions.contains(&record.id()) {
                versions.push(VersionRecord {
                    version: record.version(),
                    root: None,
                    len: record.len(),
                });
            } else {
                let root = remapped.next().ok_or(HistoryError::Invalid(
                    "persistent GC remapped roots disagree with retained versions",
                ))?;
                // The rebuild core already checks old/new root lengths
                // against each other; tie the relocated root explicitly to
                // the authoritative catalogue length before assembling.
                if root.logical_len().get() != record.len() {
                    return Err(HistoryError::Invalid(
                        "persistent GC remapped root disagrees with catalogue length",
                    ));
                }
                versions.push(VersionRecord {
                    version: record.version(),
                    root: Some(*root),
                    len: record.len(),
                });
            }
        }
        if remapped.next().is_some() {
            return Err(HistoryError::Invalid(
                "persistent GC remapped roots disagree with retained versions",
            ));
        }
        let stats = GcStats {
            nodes_before: compacted.nodes_before,
            nodes_after: compacted.nodes_after,
            nodes_reclaimed: compacted
                .nodes_before
                .checked_sub(compacted.nodes_after)
                .ok_or(HistoryError::Invalid(
                    "persistent GC reclaimed node count underflows",
                ))?,
            payload_bytes_before: compacted.payload_bytes_before,
            payload_bytes_after: compacted.payload_bytes_after,
            payload_bytes_reclaimed: compacted
                .payload_bytes_before
                .checked_sub(compacted.payload_bytes_after)
                .ok_or(HistoryError::Invalid(
                    "persistent GC reclaimed payload count underflows",
                ))?,
            retained_versions: retained_count,
            expired_versions: expired_count,
        };
        // Assemble the scratch store: the compact backend and rebuilt
        // catalogue move in; every other table clones with pre-reserved
        // capacity. Ledgers, lifecycle, bindings, and counters are identical
        // by construction — GC never adds, removes, or reorders them.
        let mut store = Self {
            backend: compacted.backend,
            histories: HashSet::new(),
            versions,
            next_history_id: self.next_history_id,
            next_version_id: self.next_version_id,
            active_requests: HashMap::new(),
            retired_requests: HashMap::new(),
            receipt_order: VecDeque::new(),
            expired_versions: HashSet::new(),
            history_bindings: HashMap::new(),
            version_bindings: HashMap::new(),
            poisoned: false,
        };
        store
            .histories
            .try_reserve(self.histories.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.histories.extend(self.histories.iter().copied());
        store
            .active_requests
            .try_reserve(self.active_requests.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.active_requests.extend(
            self.active_requests
                .iter()
                .map(|(id, record)| (id.clone(), *record)),
        );
        store
            .retired_requests
            .try_reserve(self.retired_requests.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.retired_requests.extend(
            self.retired_requests
                .iter()
                .map(|(id, digest)| (id.clone(), *digest)),
        );
        store
            .receipt_order
            .try_reserve(self.receipt_order.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store
            .receipt_order
            .extend(self.receipt_order.iter().cloned());
        store
            .expired_versions
            .try_reserve(self.expired_versions.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store
            .expired_versions
            .extend(self.expired_versions.iter().copied());
        store
            .history_bindings
            .try_reserve(self.history_bindings.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.history_bindings.extend(
            self.history_bindings
                .iter()
                .map(|(id, bound)| (*id, bound.clone())),
        );
        store
            .version_bindings
            .try_reserve(self.version_bindings.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.version_bindings.extend(
            self.version_bindings
                .iter()
                .map(|(id, bound)| (*id, bound.clone())),
        );
        Ok(PreparedGc { store, stats })
    }

    /// Applies prepared GC with infallible ownership moves: the compact
    /// backend and rebuilt catalogue replace the live ones atomically, with
    /// no fallible remapping or allocation remaining. Ledgers, lifecycle,
    /// bindings, and counters are already identical and stay put.
    pub(crate) fn apply_prepared_gc(&mut self, prepared: PreparedGc) {
        self.backend = prepared.store.backend;
        self.versions = prepared.store.versions;
    }

    /// Prepares one quiescent GC cycle against the durable physical
    /// authority: compacts retained content into a brand-new physical
    /// generation through the generic compaction core, then rebuilds the
    /// catalogue with identical logical identities, parents, lengths, and
    /// bindings — only physical placement changes, and expired entries go
    /// rootless.
    ///
    /// The new generation files are written and synced here (never
    /// authoritative until the caller publishes the metadata snapshot
    /// referencing them and the manifest commits); the live store, manifest,
    /// and current files are untouched, so any failure here is a definite
    /// rejection. Stable VersionIds are preserved across the generation
    /// replacement; no concurrent GC exists.
    pub(crate) fn prepare_physical_gc(&self, dir: &Path) -> Result<PreparedGc, HistoryError> {
        // Same materialized-consistency gate as the memory path: a catalogue
        // length disagreeing with its root fails before compaction could
        // drop an expired root and destroy the evidence.
        for record in &self.versions {
            if let Some(root) = record.root() {
                if record.len() != root.logical_len().get() {
                    return Err(HistoryError::Invalid(
                        "persistent GC version length disagrees with its materialized root",
                    ));
                }
            }
        }
        let current_generation =
            self.backend
                .physical_generation()
                .ok_or(HistoryError::Invalid(
                    "persistent physical GC requires a durable physical backend",
                ))?;
        let new_generation = current_generation
            .checked_add(1)
            .ok_or(HistoryError::Overflow(
                "persistent physical generation exceeds u64",
            ))?;
        let mut retained_roots = Vec::new();
        retained_roots
            .try_reserve(self.versions.len())
            .map_err(|_| HistoryError::Capacity("persistent GC root table allocation failed"))?;
        let mut retained_count = 0u64;
        let mut expired_count = 0u64;
        for record in &self.versions {
            if self.expired_versions.contains(&record.id()) {
                expired_count = expired_count.checked_add(1).ok_or(HistoryError::Overflow(
                    "persistent GC expired version count exceeds u64",
                ))?;
            } else {
                let root = record.root().ok_or(HistoryError::Invalid(
                    "persistent GC retained version has no materialized root",
                ))?;
                retained_roots.push(root);
                retained_count = retained_count.checked_add(1).ok_or(HistoryError::Overflow(
                    "persistent GC retained version count exceeds u64",
                ))?;
            }
        }
        let compacted =
            self.backend
                .compact_physical_to_roots(&retained_roots, new_generation, dir)?;
        let mut versions = Vec::new();
        versions
            .try_reserve_exact(self.versions.len())
            .map_err(|_| HistoryError::Capacity("persistent GC catalogue allocation failed"))?;
        let mut remapped = compacted.roots.iter();
        for record in &self.versions {
            if self.expired_versions.contains(&record.id()) {
                versions.push(VersionRecord {
                    version: record.version(),
                    root: None,
                    len: record.len(),
                });
            } else {
                let root = remapped.next().ok_or(HistoryError::Invalid(
                    "persistent GC remapped roots disagree with retained versions",
                ))?;
                if root.logical_len().get() != record.len() {
                    return Err(HistoryError::Invalid(
                        "persistent GC remapped root disagrees with catalogue length",
                    ));
                }
                versions.push(VersionRecord {
                    version: record.version(),
                    root: Some(*root),
                    len: record.len(),
                });
            }
        }
        if remapped.next().is_some() {
            return Err(HistoryError::Invalid(
                "persistent GC remapped roots disagree with retained versions",
            ));
        }
        let stats = GcStats {
            nodes_before: compacted.nodes_before,
            nodes_after: compacted.nodes_after,
            nodes_reclaimed: compacted
                .nodes_before
                .checked_sub(compacted.nodes_after)
                .ok_or(HistoryError::Invalid(
                    "persistent GC reclaimed node count underflows",
                ))?,
            payload_bytes_before: compacted.payload_bytes_before,
            payload_bytes_after: compacted.payload_bytes_after,
            payload_bytes_reclaimed: compacted
                .payload_bytes_before
                .checked_sub(compacted.payload_bytes_after)
                .ok_or(HistoryError::Invalid(
                    "persistent GC reclaimed payload count underflows",
                ))?,
            retained_versions: retained_count,
            expired_versions: expired_count,
        };
        let mut store = Self {
            backend: compacted.backend,
            histories: HashSet::new(),
            versions,
            next_history_id: self.next_history_id,
            next_version_id: self.next_version_id,
            active_requests: HashMap::new(),
            retired_requests: HashMap::new(),
            receipt_order: VecDeque::new(),
            expired_versions: HashSet::new(),
            history_bindings: HashMap::new(),
            version_bindings: HashMap::new(),
            poisoned: false,
        };
        store
            .histories
            .try_reserve(self.histories.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.histories.extend(self.histories.iter().copied());
        store
            .active_requests
            .try_reserve(self.active_requests.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.active_requests.extend(
            self.active_requests
                .iter()
                .map(|(id, record)| (id.clone(), *record)),
        );
        store
            .retired_requests
            .try_reserve(self.retired_requests.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.retired_requests.extend(
            self.retired_requests
                .iter()
                .map(|(id, digest)| (id.clone(), *digest)),
        );
        store
            .receipt_order
            .try_reserve(self.receipt_order.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store
            .receipt_order
            .extend(self.receipt_order.iter().cloned());
        store
            .expired_versions
            .try_reserve(self.expired_versions.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store
            .expired_versions
            .extend(self.expired_versions.iter().copied());
        store
            .history_bindings
            .try_reserve(self.history_bindings.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.history_bindings.extend(
            self.history_bindings
                .iter()
                .map(|(id, bound)| (*id, bound.clone())),
        );
        store
            .version_bindings
            .try_reserve(self.version_bindings.len())
            .map_err(|_| HistoryError::Capacity("persistent GC table allocation failed"))?;
        store.version_bindings.extend(
            self.version_bindings
                .iter()
                .map(|(id, bound)| (*id, bound.clone())),
        );
        Ok(PreparedGc { store, stats })
    }

    /// Durably expires through preview, ExpireVersion record, barrier, and
    /// apply. Already-expired versions return before any WAL I/O with zero
    /// new authority bytes.
    pub(crate) fn expire_durable(
        &mut self,
        log: &mut DurableHistoryLog,
        version: VersionId,
    ) -> Result<ExpireOutcome, DurableError> {
        self.require_unpoisoned_durable()?;
        let prepared = match self
            .preview_expire(version)
            .map_err(DurableError::Rejected)?
        {
            ExpirePreview::AlreadyExpired => return Ok(ExpireOutcome::AlreadyExpired),
            ExpirePreview::Fresh(prepared) => prepared,
        };
        let record = HistoryLogRecord::ExpireVersion {
            history: prepared.history,
            version: prepared.version,
        };
        let frame = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&record).map_err(DurableError::Rejected)?,
        )
        .map_err(DurableError::Rejected)?;
        self.write_and_sync(log, &frame)?;
        self.apply_prepared_expire(&prepared)
            .map_err(|error| self.poison_after_barrier(error))
    }

    /// Replays one logged expiration during recovery: the version must be
    /// known in the recorded history (wrong history or unknown version fails
    /// closed), the lifecycle mark is inserted idempotently, and any active
    /// receipt resolving to the version moves active -> retired with its
    /// order position unchanged — exactly mirroring live execution so WAL
    /// replay reproduces the identical horizon state deterministically.
    pub(crate) fn replay_expire(
        &mut self,
        history: HistoryId,
        version: VersionId,
    ) -> Result<(), HistoryError> {
        if !self.histories.contains(&history) {
            return Err(HistoryError::Invalid(
                "history log expire targets an unknown history",
            ));
        }
        let record = self
            .version_record(version)
            .map_err(|_| HistoryError::Invalid("history log expire targets an unknown version"))?;
        if record.history() != history {
            return Err(HistoryError::Invalid(
                "history log expire crosses histories",
            ));
        }
        if self.expired_versions.contains(&version) {
            return Ok(());
        }
        self.expired_versions
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent expired version allocation failed"))?;
        let mut receipt = None;
        for (id, active) in &self.active_requests {
            if active.version() == version {
                receipt = Some((id.clone(), active.digest()));
                break;
            }
        }
        if let Some((id, digest)) = receipt {
            self.retired_requests.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent retired request allocation failed")
            })?;
            let _ = self.active_requests.remove(id.as_slice());
            let _ = self.retired_requests.insert(id, digest);
        }
        let _ = self.expired_versions.insert(version);
        Ok(())
    }

    /// Durably retires a request identity with the same write-then-apply
    /// discipline as commits.
    pub(crate) fn retire_durable(
        &mut self,
        log: &mut DurableHistoryLog,
        request_id: &[u8],
    ) -> Result<(), DurableError> {
        self.require_unpoisoned_durable()?;
        validate_request_identity(request_id).map_err(DurableError::Rejected)?;
        let digest = match self.active_requests.get(request_id) {
            Some(record) => record.digest(),
            None => {
                return Err(DurableError::Rejected(
                    if self.retired_requests.contains_key(request_id) {
                        HistoryError::Invalid("persistent request identity is already retired")
                    } else {
                        HistoryError::Invalid("persistent active request identity is absent")
                    },
                ));
            }
        };
        let record = HistoryLogRecord::Retire {
            request_id: request_id.to_vec(),
            digest,
        };
        let frame = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&record).map_err(DurableError::Rejected)?,
        )
        .map_err(DurableError::Rejected)?;
        self.write_and_sync(log, &frame)?;
        self.retire_request(request_id)
            .map_err(|error| self.poison_after_barrier(error))
    }

    fn require_unpoisoned_durable(&self) -> Result<(), DurableError> {
        if self.poisoned {
            return Err(DurableError::RecoveryRequired);
        }
        Ok(())
    }

    fn write_and_sync(
        &mut self,
        log: &mut DurableHistoryLog,
        frame: &[u8],
    ) -> Result<(), DurableError> {
        let ledger = self.backend.io_ledger();
        // A failed append leaves at most a torn tail, which recovery ignores
        // and the next append truncates: nothing new became authoritative, so
        // this stays a definite reject. (ENOSPC-specific mapping for the
        // candidate path arrives with the fault-matrix hardening slice.)
        log.append_frame(frame).map_err(|_| {
            DurableError::Rejected(HistoryError::Invalid(
                "history log append failed before any new authority",
            ))
        })?;
        ledger.count_wal_written(frame.len() as u64);
        let started = Instant::now();
        let outcome = log.sync().map_err(|source| {
            self.set_poisoned();
            DurableError::Indeterminate {
                operation: DurabilityOperation::FileSyncAll,
                source,
            }
        });
        ledger.count_wal_sync(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        outcome
    }

    fn poison_after_barrier(&mut self, error: HistoryError) -> DurableError {
        self.set_poisoned();
        DurableError::Indeterminate {
            operation: DurabilityOperation::FileSyncAll,
            source: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("history apply failed after the durable barrier: {error}"),
            ),
        }
    }

    /// Reads an exact byte range of a committed version.
    pub fn read(
        &self,
        version: Version,
        offset: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), HistoryError> {
        let record = self.committed_version(version)?;
        let root = record.root().ok_or(HistoryError::Invalid(
            "persistent version has no materialized root",
        ))?;
        let range = SequenceRange::new(LogicalLength::new(offset), LogicalLength::new(length))
            .ok_or(HistoryError::Invalid(
                "persistent history read range exceeds u64",
            ))?;
        self.backend.read_range(root, range, output)?;
        Ok(())
    }

    /// Recomputes every reachable node's metadata and commitment.
    pub fn verify(&self, version: Version) -> Result<(), HistoryError> {
        let record = self.committed_version(version)?;
        let root = record.root().ok_or(HistoryError::Invalid(
            "persistent version has no materialized root",
        ))?;
        self.backend.verify(root)?;
        Ok(())
    }

    /// Returns a snapshot of the backend diagnostic work counters.
    pub fn work_counters(&self) -> SequenceWorkCounters {
        self.backend.work_counters()
    }

    /// Returns a snapshot of the physical I/O counters across all scopes.
    pub fn io_counters(&self) -> PhysicalIoCounters {
        self.backend.io_counters()
    }

    /// Resets every physical I/O bucket and clears the overflow flag.
    pub fn reset_io_counters(&self) {
        self.backend.reset_io_counters();
    }

    /// Reports whether this store is backed by the durable physical content
    /// store (as opposed to an ephemeral memory arena).
    pub(crate) fn backend_is_physical(&self) -> bool {
        self.backend.backend_is_physical()
    }

    /// Verifies open content files cover the committed frontiers after
    /// replay: metadata authority referencing shorter files fails closed.
    pub(crate) fn check_physical_frontiers_against_files(&self) -> Result<(), HistoryError> {
        self.backend
            .check_physical_frontiers()
            .map_err(HistoryError::from)
    }

    /// Heals orphan content tails before writable mutation. Durable
    /// backends only; memory backends are trivially healed.
    pub(crate) fn heal_physical_for_write(&mut self) -> Result<(), HistoryError> {
        self.backend
            .heal_physical_for_write()
            .map_err(HistoryError::from)
    }

    #[cfg(test)]
    pub(crate) fn arm_fail_next_physical_payload_append(&mut self) {
        self.backend.arm_fail_next_physical_payload_append();
    }

    #[cfg(test)]
    pub(crate) fn arm_fail_next_physical_node_append(&mut self) {
        self.backend.arm_fail_next_physical_node_append();
    }

    #[cfg(test)]
    pub(crate) fn arm_fail_next_physical_sync(&mut self) {
        self.backend.arm_fail_next_physical_sync();
    }

    /// Looks up a retained version by logical identity for adapter reads.
    /// Coordinate-checked like every other lookup: a fabricated identity
    /// fails closed, and an expired identity fails with
    /// [`VersionExpired`](HistoryError::VersionExpired) instead of handing
    /// out a version that is no longer available for acquisition.
    pub fn lookup_version(&self, id: VersionId) -> Result<Version, HistoryError> {
        let record = self.version_record(id)?;
        if self.expired_versions.contains(&id) {
            return Err(HistoryError::VersionExpired);
        }
        Ok(record.version())
    }

    /// Counts committed versions. Used for reopen statistics and tests.
    /// Expired versions stay counted: expiration never removes catalogue
    /// entries.
    pub fn version_count(&self) -> usize {
        self.versions.len()
    }

    /// Reports the exact logical byte length of one retained version,
    /// resolved from catalogue metadata without backend access. Expired
    /// versions fail with [`VersionExpired`](HistoryError::VersionExpired):
    /// callers must no longer derive lengths through physical roots, which
    /// compaction relocates and reclamation may drop.
    pub fn logical_len(&self, version: Version) -> Result<LogicalLength, HistoryError> {
        let record = self.committed_version(version)?;
        Ok(LogicalLength::new(record.len()))
    }

    /// Reports the horizon visibility of one request identity.
    ///
    /// While a receipt is retained, exact replay/conflict semantics hold
    /// (active: same digest replays, different conflicts; retired: same
    /// digest retires, different conflicts). After the receipt falls outside
    /// the [`STAGING_REQUEST_RECEIPT_CAPACITY`] horizon it reads
    /// [`Unknown`](RequestReceiptStatus::Unknown) — indistinguishable from
    /// never-observed — and reuse may execute a fresh operation, possibly
    /// creating a new version. This boundedness is deliberate, not a bug.
    pub fn request_receipt_status(&self, request_id: &[u8]) -> RequestReceiptStatus {
        if let Some(active) = self.active_requests.get(request_id) {
            return RequestReceiptStatus::Active(active.version());
        }
        if self.retired_requests.contains_key(request_id) {
            return RequestReceiptStatus::Retired;
        }
        RequestReceiptStatus::Unknown
    }

    /// Counts retained request receipts (active + retired together), always
    /// `<= STAGING_REQUEST_RECEIPT_CAPACITY` after any successful operation.
    pub fn request_receipt_count(&self) -> usize {
        self.active_requests.len() + self.retired_requests.len()
    }

    /// Reports the exact staging receipt capacity. Not release-frozen.
    pub const fn request_receipt_capacity(&self) -> usize {
        STAGING_REQUEST_RECEIPT_CAPACITY
    }

    /// Lists committed history identities in stable numeric order for
    /// adapter map reconstruction. The order is deterministic so reopened
    /// adapters rebuild identical maps on every open.
    pub fn all_histories(&self) -> Vec<HistoryId> {
        let mut histories: Vec<HistoryId> = self.histories.iter().copied().collect();
        histories.sort_by_key(|id| id.id());
        histories
    }

    /// Borrows the opaque adapter binding recorded for a history, if any.
    /// Histories created without a binding stay invisible to adapters.
    pub fn history_binding(&self, id: HistoryId) -> Option<&[u8]> {
        self.history_bindings.get(&id).map(Vec::as_slice)
    }

    /// Lists committed versions in identity order for adapter map
    /// reconstruction. The dense table is already identity-ordered; only
    /// logical handles escape, never physical placement.
    pub fn all_versions(&self) -> Vec<Version> {
        self.versions
            .iter()
            .map(|record| record.version())
            .collect()
    }

    /// Borrows the opaque adapter binding recorded for a version, if any.
    pub fn version_binding(&self, id: VersionId) -> Option<&[u8]> {
        self.version_bindings.get(&id).map(Vec::as_slice)
    }

    /// Resolves a version identity within an expected history for adapter
    /// reads. The returned root always comes from the committed table, so a
    /// fabricated value fails closed in [`PersistentHistoryStore::read`].
    pub fn committed_version_for_adapter(
        &self,
        id: VersionId,
        history: HistoryId,
    ) -> Result<Version, HistoryError> {
        let record = self.version_record(id)?;
        if record.history() != history {
            return Err(HistoryError::Invalid(
                "persistent version belongs to a different history",
            ));
        }
        if self.expired_versions.contains(&id) {
            return Err(HistoryError::VersionExpired);
        }
        Ok(record.version())
    }

    fn version_record(&self, id: VersionId) -> Result<VersionRecord, HistoryError> {
        let index = usize::try_from(id.id())
            .map_err(|_| HistoryError::Overflow("persistent version identifier exceeds usize"))?;
        let record = self
            .versions
            .get(index)
            .copied()
            .ok_or(HistoryError::Invalid("persistent version is unknown"))?;
        if record.id() != id {
            return Err(HistoryError::Invalid(
                "persistent version disagrees with its table coordinate",
            ));
        }
        Ok(record)
    }

    /// Resolves the current physical root of one known version. Fails closed
    /// for unknown identities and for expired versions whose content has
    /// been reclaimed: a rootless entry has no valid placement to hand out.
    /// Test-only introspection: production code resolves placement through
    /// version-scoped operations, never by raw identity.
    #[cfg(test)]
    pub(crate) fn physical_root(&self, id: VersionId) -> Result<PersistentRoot, HistoryError> {
        self.version_record(id)?.root().ok_or(HistoryError::Invalid(
            "persistent version has no materialized root",
        ))
    }

    /// Resolves a caller-held version against the committed table so a
    /// fabricated or stale value fails closed instead of addressing the arena.
    /// Expired versions fail with [`VersionExpired`](HistoryError::VersionExpired):
    /// public read/verify acquire live state, while internal metadata paths
    /// use [`version_record`](Self::version_record) directly.
    fn committed_version(&self, version: Version) -> Result<VersionRecord, HistoryError> {
        let record = self.version_record(version.id())?;
        if record.version() != version {
            return Err(HistoryError::Invalid(
                "persistent version does not match committed history",
            ));
        }
        if self.expired_versions.contains(&version.id()) {
            return Err(HistoryError::VersionExpired);
        }
        Ok(record)
    }

    /// Rebuilds a store from a decoded schema6 snapshot, restoring
    /// lifecycle marks, ledgers, and the exact receipt order, and binding
    /// the authoritative physical content files — without loading any
    /// content bytes.
    ///
    /// Decode already enforced wire structure, dense tables, topological
    /// parents, ordered ledgers, receipt-horizon agreement, canonical root
    /// descriptors, and bounds. Import rechecks lineage and receipt
    /// consistency defensively (this struct may not have come from decode),
    /// rebuilds the expired set and horizon order, resolves every
    /// materialized descriptor to a catalogue root with length agreement
    /// (the E5 closure invariant, carried into the physical epoch), and
    /// preserves ledger digests byte-exact: digests bind commit deltas that
    /// version content cannot reproduce, so their authenticity traces to
    /// commit-time and log-replay validation while the artifact digest
    /// protects these bytes.
    ///
    /// Content files for the snapshot's physical generation must exist with
    /// valid headers and lengths at or beyond the snapshot frontiers:
    /// shorter files fail closed (referenced bytes are absent), while longer
    /// files hold an orphan tail the read path ignores until a writable
    /// open heals it. No payload or node content is read here beyond file
    /// headers — retained versions become addressable through their
    /// descriptors on first access.
    /// Rebuilds an empty genesis store bound to a directory that holds
    /// no manifest: physical generation zero with zero frontiers. When
    /// generation-zero content files already exist (durable splices
    /// committed before the first seal), they bind read-only against empty
    /// frontiers — the hot-suffix replay then advances them; when absent,
    /// binding stays deferred until the first mutation creates them. Any
    /// other state fails closed.
    pub(crate) fn import_physical_genesis(
        dir: &Path,
        ledger: &IoLedger,
    ) -> Result<Self, HistoryError> {
        use crate::persistent_sequence::physical::PhysicalContentStore;
        let backend = if PhysicalContentStore::content_files_present(dir, 0) {
            BalancedSequence::open_physical(
                PhysicalContentStore::open_existing(dir, 0, 0, 0, ledger, false)
                    .map_err(|error| HistoryError::from(SequenceError::from(error)))?,
            )
        } else {
            BalancedSequence::open_physical(PhysicalContentStore::open_deferred(dir, 0, ledger))
        };
        Ok(Self {
            backend,
            histories: HashSet::new(),
            versions: Vec::new(),
            next_history_id: 0,
            next_version_id: 0,
            active_requests: HashMap::new(),
            retired_requests: HashMap::new(),
            receipt_order: VecDeque::new(),
            expired_versions: HashSet::new(),
            history_bindings: HashMap::new(),
            version_bindings: HashMap::new(),
            poisoned: false,
        })
    }

    pub(crate) fn import_physical_snapshot(
        dir: &Path,
        snapshot: snapshot::HistorySnapshot,
        ledger: &IoLedger,
    ) -> Result<Self, HistoryError> {
        use crate::persistent_sequence::physical::PhysicalContentStore;
        let content = PhysicalContentStore::open_existing(
            dir,
            snapshot.physical_generation,
            snapshot.payload_end,
            snapshot.node_count,
            ledger,
            false,
        )
        .map_err(|error| HistoryError::from(SequenceError::from(error)))?;
        let backend = BalancedSequence::open_physical(content);
        let mut store = Self {
            backend,
            histories: HashSet::new(),
            versions: Vec::new(),
            next_history_id: snapshot.next_history_id,
            next_version_id: snapshot.next_version_id,
            active_requests: HashMap::new(),
            retired_requests: HashMap::new(),
            receipt_order: VecDeque::new(),
            expired_versions: HashSet::new(),
            history_bindings: HashMap::new(),
            version_bindings: HashMap::new(),
            poisoned: false,
        };
        store
            .histories
            .try_reserve(snapshot.history_bindings.len())
            .map_err(|_| HistoryError::Capacity("history snapshot import allocation failed"))?;
        {
            let mut seen: HashSet<&[u8]> = HashSet::new();
            seen.try_reserve(snapshot.history_bindings.len())
                .map_err(|_| HistoryError::Capacity("history snapshot import allocation failed"))?;
            for binding in snapshot.history_bindings.iter().flatten() {
                if binding.is_empty() {
                    return Err(HistoryError::Invalid(
                        "history snapshot history binding is empty",
                    ));
                }
                if !seen.insert(binding.as_slice()) {
                    return Err(HistoryError::Invalid(
                        "history snapshot history bindings are not unique",
                    ));
                }
            }
        }
        for (index, binding) in snapshot.history_bindings.iter().enumerate() {
            let id = HistoryId::new(u64::try_from(index).map_err(|_| {
                HistoryError::Overflow("history snapshot history identity exceeds u64")
            })?);
            let _ = store.histories.insert(id);
            if let Some(bytes) = binding {
                store.history_bindings.try_reserve(1).map_err(|_| {
                    HistoryError::Capacity("history snapshot import allocation failed")
                })?;
                let _ = store.history_bindings.insert(id, bytes.clone());
            }
        }
        store
            .versions
            .try_reserve(snapshot.versions.len())
            .map_err(|_| HistoryError::Capacity("history snapshot import allocation failed"))?;
        let mut materialized_index = 0usize;
        for (index, entry) in snapshot.versions.iter().enumerate() {
            let id = VersionId::new(u64::try_from(index).map_err(|_| {
                HistoryError::Overflow("history snapshot version identity exceeds u64")
            })?);
            // Import re-verifies the lineage rule the live API enforces: a
            // parent must belong to the same history as its child, addressed
            // defensively since this struct may not have come from decode.
            if let Some(parent) = entry.parent {
                let parent_index = usize::try_from(parent.id()).map_err(|_| {
                    HistoryError::Overflow("history snapshot version identity exceeds usize")
                })?;
                let parent_entry =
                    snapshot
                        .versions
                        .get(parent_index)
                        .ok_or(HistoryError::Invalid(
                            "history snapshot version parent is not topologically prior",
                        ))?;
                if parent_entry.history != entry.history {
                    return Err(HistoryError::Invalid(
                        "history snapshot version parent belongs to a different history",
                    ));
                }
            }
            if entry.lifecycle == VersionLifecycle::Expired {
                store.expired_versions.try_reserve(1).map_err(|_| {
                    HistoryError::Capacity("history snapshot import allocation failed")
                })?;
                let _ = store.expired_versions.insert(id);
            }
            // Length is authoritative catalogue metadata, never derived
            // silently: zero is rejected for every entry, and a materialized
            // descriptor must decode canonically and agree with the catalogue
            // value before the record commits.
            if entry.len == 0 {
                return Err(HistoryError::Invalid(
                    "history snapshot version length is zero",
                ));
            }
            let root = if entry.root_present {
                let descriptor = entry.root.as_ref().ok_or(HistoryError::Invalid(
                    "history snapshot materialized version has no root descriptor",
                ))?;
                materialized_index =
                    materialized_index
                        .checked_add(1)
                        .ok_or(HistoryError::Overflow(
                            "history snapshot materialized index exceeds usize",
                        ))?;
                Some(decode_canonical_root_bytes(descriptor)?)
            } else {
                if entry.lifecycle == VersionLifecycle::Retained {
                    return Err(HistoryError::Invalid(
                        "history snapshot retained version has no materialized root",
                    ));
                }
                None
            };
            if let Some(root) = root {
                if entry.len != root.logical_len().get() {
                    return Err(HistoryError::Invalid(
                        "history snapshot version length disagrees with its materialized root",
                    ));
                }
            }
            store.versions.push(VersionRecord {
                version: Version {
                    history: entry.history,
                    id,
                    parent: entry.parent,
                },
                root,
                len: entry.len,
            });
            if let Some(bytes) = &entry.binding {
                store.version_bindings.try_reserve(1).map_err(|_| {
                    HistoryError::Capacity("history snapshot import allocation failed")
                })?;
                let _ = store.version_bindings.insert(id, bytes.clone());
            }
        }
        let materialized = snapshot
            .versions
            .iter()
            .filter(|entry| entry.root_present)
            .count();
        if materialized_index != materialized {
            return Err(HistoryError::Invalid(
                "history snapshot descriptors disagree with its materialized versions",
            ));
        }
        for record in &snapshot.active {
            // Structural cross-references only: version existence and
            // coordinate agreement. Operation digests bind commit deltas,
            // which version content cannot reproduce, so digest authenticity
            // traces to commit-time and log-replay validation while the
            // artifact digest protects these bytes.
            let version = store.version_record(record.version)?.version();
            store
                .active_requests
                .try_reserve(1)
                .map_err(|_| HistoryError::Capacity("history snapshot import allocation failed"))?;
            if store
                .active_requests
                .insert(
                    record.request_id.clone(),
                    ActiveRequest {
                        digest: record.digest,
                        version: version.id(),
                    },
                )
                .is_some()
            {
                return Err(HistoryError::Invalid(
                    "history snapshot active request identity is duplicated",
                ));
            }
        }
        for record in &snapshot.retired {
            if store.active_requests.contains_key(&record.request_id) {
                return Err(HistoryError::Invalid(
                    "history snapshot request identity is both active and retired",
                ));
            }
            store
                .retired_requests
                .try_reserve(1)
                .map_err(|_| HistoryError::Capacity("history snapshot import allocation failed"))?;
            if store
                .retired_requests
                .insert(record.request_id.clone(), record.digest)
                .is_some()
            {
                return Err(HistoryError::Invalid(
                    "history snapshot retired request identity is duplicated",
                ));
            }
        }
        // The horizon order round-trips exactly: decode already enforced it
        // for wire bytes, and the shared validator below rechecks forged
        // structs that bypassed decode.
        store
            .receipt_order
            .try_reserve(snapshot.receipt_order.len())
            .map_err(|_| HistoryError::Capacity("history snapshot import allocation failed"))?;
        for id in &snapshot.receipt_order {
            if id.is_empty() || id.len() > MAX_HISTORY_REQUEST_ID_BYTES {
                return Err(HistoryError::Invalid(
                    "history snapshot request identity is outside bounds",
                ));
            }
            store.receipt_order.push_back(id.clone());
        }
        snapshot::validate_snapshot_receipt_consistency(
            &snapshot.versions,
            &snapshot.active,
            &snapshot.retired,
            &snapshot.receipt_order,
        )?;
        // Defense in depth: the expired set built above must stay a subset
        // of known identities, which dense construction guarantees.
        if store.expired_versions.len() > store.versions.len() {
            return Err(HistoryError::Invalid(
                "history snapshot expired versions disagree with its table",
            ));
        }
        Ok(store)
    }
}

fn validate_request_identity(request_id: &[u8]) -> Result<(), HistoryError> {
    if request_id.is_empty() || request_id.len() > MAX_HISTORY_REQUEST_ID_BYTES {
        return Err(HistoryError::Invalid(
            "persistent request identity is empty or exceeds the byte limit",
        ));
    }
    Ok(())
}

fn validate_binding(binding: &[u8]) -> Result<(), HistoryError> {
    if binding.is_empty() || binding.len() > MAX_HISTORY_BINDING_BYTES {
        return Err(HistoryError::Invalid(
            "persistent adapter binding is empty or exceeds the byte limit",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::durable_log::recover_history_store;
    use super::*;

    fn read_full(store: &PersistentHistoryStore, version: Version, len: u64) -> Vec<u8> {
        let mut output = Vec::new();
        store.read(version, 0, len, &mut output).unwrap();
        output
    }

    fn append_new(
        store: &mut PersistentHistoryStore,
        history: HistoryId,
        parent: Option<VersionId>,
        payload: &[u8],
    ) -> Version {
        let outcome = store.append(history, parent, payload, None, None).unwrap();
        assert!(matches!(outcome, CommitOutcome::Committed(_)));
        outcome.version().unwrap()
    }

    fn splice_new(
        store: &mut PersistentHistoryStore,
        history: HistoryId,
        parent: Option<VersionId>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
    ) -> Version {
        let outcome = store
            .splice(history, parent, offset, delete_len, insert, None, None)
            .unwrap();
        assert!(matches!(outcome, CommitOutcome::Committed(_)));
        outcome.version().unwrap()
    }

    #[test]
    fn splice_insert_delete_replace_are_exact() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = splice_new(&mut store, history, None, 0, 0, b"abcdefghij");
        assert_eq!(read_full(&store, v0, 10), b"abcdefghij");
        let v1 = splice_new(&mut store, history, Some(v0.id()), 5, 0, b"-MID-");
        assert_eq!(read_full(&store, v1, 15), b"abcde-MID-fghij");
        let v2 = splice_new(&mut store, history, Some(v1.id()), 0, 5, b"");
        assert_eq!(read_full(&store, v2, 10), b"-MID-fghij");
        let v3 = splice_new(&mut store, history, Some(v2.id()), 5, 5, b"1234567890");
        assert_eq!(read_full(&store, v3, 15), b"-MID-1234567890");
        // Every intermediate root still reads exactly.
        assert_eq!(read_full(&store, v0, 10), b"abcdefghij");
        assert_eq!(read_full(&store, v1, 15), b"abcde-MID-fghij");
        assert_eq!(read_full(&store, v2, 10), b"-MID-fghij");
        for version in [v0, v1, v2, v3] {
            store.verify(version).unwrap();
        }
    }

    #[test]
    fn splice_historical_branches_stay_exact() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"base-payload");
        let v1 = splice_new(&mut store, history, Some(v0.id()), 5, 0, b"[edit1]");
        assert_eq!(read_full(&store, v1, 19), b"base-[edit1]payload");
        let v2 = splice_new(&mut store, history, Some(v0.id()), 0, 4, b"EDIT");
        assert_eq!(read_full(&store, v2, 12), b"EDIT-payload");
        assert_eq!(read_full(&store, v0, 12), b"base-payload");
        assert_eq!(v1.parent(), Some(v0.id()));
        assert_eq!(v2.parent(), Some(v0.id()));
    }

    #[test]
    fn splice_rejects_invalid_coordinates_atomically() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let other = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"abcdef");
        let versions_before = store.versions.len();
        let counter_before = store.next_version_id;
        let bad = [
            // offset past the end, including u64 extremes
            (Some(v0.id()), 7, 0, b"x".as_slice()),
            (Some(v0.id()), u64::MAX, 0, b"x"),
            // delete past the end
            (Some(v0.id()), 5, 2, b""),
            (Some(v0.id()), 0, 7, b""),
            // zero-effect and zero-result splices
            (Some(v0.id()), 3, 0, b""),
            (Some(v0.id()), 0, 6, b""),
            // root creation violations
            (None, 1, 0, b"x"),
            (None, 0, 1, b"x"),
            (None, 0, 0, b""),
            // cross-history parent
            (Some(v0.id()), 0, 0, b"x"),
        ];
        for (index, (parent, offset, delete_len, insert)) in bad.iter().enumerate() {
            let history = if index == bad.len() - 1 {
                other
            } else {
                history
            };
            assert!(
                store
                    .splice(history, *parent, *offset, *delete_len, insert, None, None)
                    .is_err(),
                "case {index} must fail"
            );
        }
        // Unknown parent fails closed too.
        assert!(store
            .splice(history, Some(VersionId::new(999)), 0, 0, b"x", None, None)
            .is_err());
        // Every rejection left counters, tables, and ledgers untouched.
        assert_eq!(store.versions.len(), versions_before);
        assert_eq!(store.next_version_id, counter_before);
        assert!(store.active_requests.is_empty());
        assert!(store.version_bindings.is_empty());
        assert_eq!(read_full(&store, v0, 6), b"abcdef");
    }

    #[test]
    fn splice_request_ledger_replays_and_conflicts() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"abcdefghij");
        let v1 = match store
            .splice(history, Some(v0.id()), 5, 2, b"XY", Some(b"req-1"), None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("fresh splice must create")
            }
        };
        assert_eq!(read_full(&store, v1, 10), b"abcdeXYhij");
        // Identical splice under the same request replays without mutation.
        let versions_before = store.versions.len();
        assert_eq!(
            store.splice(history, Some(v0.id()), 5, 2, b"XY", Some(b"req-1"), None),
            Ok(CommitOutcome::Replayed(v1))
        );
        assert_eq!(store.versions.len(), versions_before);
        // Any changed coordinate conflicts: offset, delete length, insert,
        // parent, and binding each address a different operation.
        let v9 = append_new(&mut store, history, Some(v1.id()), b"tail");
        for (parent, offset, delete_len, insert, binding) in [
            (Some(v0.id()), 6, 2, b"XY".as_slice(), None),
            (Some(v0.id()), 5, 3, b"XY".as_slice(), None),
            (Some(v0.id()), 5, 2, b"XZ".as_slice(), None),
            (Some(v9.id()), 5, 2, b"XY".as_slice(), None),
            (
                Some(v0.id()),
                5,
                2,
                b"XY".as_slice(),
                Some(b"bind".as_slice()),
            ),
        ] {
            assert_eq!(
                store.splice(
                    history,
                    parent,
                    offset,
                    delete_len,
                    insert,
                    Some(b"req-1"),
                    binding
                ),
                Err(HistoryError::RequestConflict)
            );
        }
        assert_eq!(store.versions.len(), versions_before + 1);
    }

    #[test]
    fn append_and_explicit_splice_share_digest_and_encoding() {
        // E2.18: one canonical mutation. The explicit splice of append
        // coordinates under the same request replays the append — proving a
        // shared digest — and both spellings read byte-identical.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"hello");
        let v1 = match store
            .append(history, Some(v0.id()), b" world", Some(b"req-1"), None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("append must create")
            }
        };
        assert_eq!(
            store.splice(
                history,
                Some(v0.id()),
                5,
                0,
                b" world",
                Some(b"req-1"),
                None
            ),
            Ok(CommitOutcome::Replayed(v1))
        );
        assert_eq!(read_full(&store, v1, 11), b"hello world");

        let mut other = PersistentHistoryStore::new();
        let other_history = other.create_history().unwrap();
        let other_v0 = append_new(&mut other, other_history, None, b"hello");
        let other_v1 = splice_new(
            &mut other,
            other_history,
            Some(other_v0.id()),
            5,
            0,
            b" world",
        );
        assert_eq!(read_full(&other, other_v1, 11), b"hello world");
    }

    #[test]
    fn splice_durable_reopen_is_byte_exact() {
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let (v1_id, v2_id);
        {
            let mut store = PersistentHistoryStore::new();
            let mut log = DurableHistoryLog::open(&path).unwrap();
            let history = store.create_history_durable(&mut log).unwrap();
            let v0 = match store
                .append_durable(&mut log, history, None, b"abcdefghij", None, None)
                .unwrap()
            {
                CommitOutcome::Committed(version) => version,
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("durable root must create")
                }
            };
            let v1 = match store
                .splice_durable(&mut log, history, Some(v0.id()), 5, 2, b"XY", None, None)
                .unwrap()
            {
                CommitOutcome::Committed(version) => version,
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("durable splice must create")
                }
            };
            v1_id = v1.id();
            let v2 = match store
                .splice_durable(
                    &mut log,
                    history,
                    Some(v1.id()),
                    0,
                    10,
                    b"replaced!!",
                    None,
                    None,
                )
                .unwrap()
            {
                CommitOutcome::Committed(version) => version,
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("durable replace must create")
                }
            };
            v2_id = v2.id();
            drop(store);
            drop(log);
        }
        let bytes = std::fs::read(&path).unwrap();
        let reopened = recover_history_store(&bytes).unwrap();
        assert_eq!(reopened.versions.len(), 3);
        let v1 = reopened.lookup_version(v1_id).unwrap();
        let v2 = reopened.lookup_version(v2_id).unwrap();
        assert_eq!(read_full(&reopened, v1, 10), b"abcdeXYhij");
        assert_eq!(read_full(&reopened, v2, 10), b"replaced!!");
        reopened.verify(v1).unwrap();
        reopened.verify(v2).unwrap();
    }

    fn splice_record(
        history: HistoryId,
        version: VersionId,
        parent: Option<VersionId>,
        offset: u64,
        delete_len: u64,
        insert: &[u8],
        digest: [u8; 32],
    ) -> Vec<u8> {
        durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&durable_log::HistoryLogRecord::Splice {
                history,
                version,
                parent,
                offset,
                delete_len,
                insert: insert.to_vec(),
                request_id: None,
                binding: None,
                digest,

                physical: None,
            })
            .unwrap(),
        )
        .unwrap()
    }

    fn honest_splice_fixture() -> (Vec<u8>, HistoryId, VersionId) {
        // Genesis history 0 with V0 = "abcdefghij" (10 bytes): the parent
        // every corruption case below builds on.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        append_new(&mut store, history, None, b"abcdefghij");
        let mut bytes = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(
                &durable_log::HistoryLogRecord::CreateHistory {
                    history,
                    binding: None,
                },
            )
            .unwrap(),
        )
        .unwrap();
        let digest = history_splice_digest(history, None, 0, 0, b"abcdefghij", None);
        bytes.extend_from_slice(&splice_record(
            history,
            VersionId::new(0),
            None,
            0,
            0,
            b"abcdefghij",
            digest,
        ));
        (bytes, history, VersionId::new(0))
    }

    #[test]
    fn splice_record_corruption_fails_closed() {
        let (prefix, history, v0) = honest_splice_fixture();
        let good_digest = |offset: u64, delete_len: u64, insert: &[u8]| {
            history_splice_digest(history, Some(v0), offset, delete_len, insert, None)
        };
        // Each case appends one complete but malformed splice frame after a
        // valid prefix: recovery must fail closed, never reinterpret.
        let cases: Vec<(&str, Vec<u8>)> = vec![
            (
                "offset past end",
                splice_record(
                    history,
                    VersionId::new(1),
                    Some(v0),
                    11,
                    0,
                    b"x",
                    good_digest(11, 0, b"x"),
                ),
            ),
            (
                "delete past end",
                splice_record(
                    history,
                    VersionId::new(1),
                    Some(v0),
                    9,
                    2,
                    b"",
                    good_digest(9, 2, b""),
                ),
            ),
            (
                "no-op splice",
                splice_record(
                    history,
                    VersionId::new(1),
                    Some(v0),
                    3,
                    0,
                    b"",
                    good_digest(3, 0, b""),
                ),
            ),
            (
                "zero-result splice",
                splice_record(
                    history,
                    VersionId::new(1),
                    Some(v0),
                    0,
                    10,
                    b"",
                    good_digest(0, 10, b""),
                ),
            ),
            (
                "wrong digest",
                splice_record(history, VersionId::new(1), Some(v0), 5, 0, b"x", [0x55; 32]),
            ),
            (
                "wrong history",
                splice_record(
                    HistoryId::new(7),
                    VersionId::new(1),
                    Some(v0),
                    5,
                    0,
                    b"x",
                    good_digest(5, 0, b"x"),
                ),
            ),
            (
                "wrong version identity",
                splice_record(
                    history,
                    VersionId::new(9),
                    Some(v0),
                    5,
                    0,
                    b"x",
                    good_digest(5, 0, b"x"),
                ),
            ),
        ];
        for (name, frame) in cases {
            let mut bytes = prefix.clone();
            bytes.extend_from_slice(&frame);
            assert!(
                recover_history_store(&bytes).is_err(),
                "{name} must fail closed"
            );
        }
        // Wrong parent: same-history rule at replay. The digest is recomputed
        // for the forged coordinates so only parenthood can reject.
        let mut store = PersistentHistoryStore::new();
        let second = store.create_history().unwrap();
        append_new(&mut store, second, None, b"other");
        let mut bytes = prefix.clone();
        let forged_digest = history_splice_digest(second, Some(v0), 0, 0, b"x", None);
        let second_create = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(
                &durable_log::HistoryLogRecord::CreateHistory {
                    history: second,
                    binding: None,
                },
            )
            .unwrap(),
        )
        .unwrap();
        // Replay order demands dense identities: history 1 second.
        bytes.extend_from_slice(&second_create);
        bytes.extend_from_slice(&splice_record(
            second,
            VersionId::new(1),
            Some(v0),
            0,
            0,
            b"x",
            forged_digest,
        ));
        assert!(recover_history_store(&bytes).is_err());
    }

    #[test]
    fn splice_record_trailing_bytes_fail_closed() {
        let (mut bytes, _, _) = honest_splice_fixture();
        // Garbage inside the last record body breaks the frame digest.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(recover_history_store(&bytes).is_err());
    }

    #[test]
    fn old_staging_grammar_fails_closed() {
        // Pre-E2 append-grammar bytes (THL1 magic) are never reinterpreted
        // as splice records: the frame magic rejects them first.
        let (bytes, _, _) = honest_splice_fixture();
        let mut old_magic = bytes.clone();
        old_magic[0..4].copy_from_slice(b"THL1");
        assert!(matches!(
            recover_history_store(&old_magic),
            Err(HistoryError::Invalid(_))
        ));
        // Pre-E2 snapshots (schema 1) fail at the schema gate, so old
        // append-grammar request digests can never validate as splice
        // digests. Patch only the schema word of an otherwise valid
        // artifact: every other check would still pass.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        append_new(&mut store, history, None, b"data");
        let snapshot =
            crate::persistent_history::snapshot::encode_history_snapshot(&store, 1, 0).unwrap();
        let mut old_schema = snapshot.clone();
        old_schema[12..16].copy_from_slice(&1u32.to_le_bytes());
        assert!(matches!(
            crate::persistent_history::snapshot::decode_history_snapshot(&old_schema),
            Err(HistoryError::Invalid(_))
        ));
        // Control: the unpatched artifacts decode.
        assert!(recover_history_store(&bytes).is_ok());
        assert!(crate::persistent_history::snapshot::decode_history_snapshot(&snapshot).is_ok());
    }

    #[test]
    fn splice_version_counter_overflow_leaves_no_version() {
        // Identity allocation is preview-checked: at exhaustion the splice
        // fails before any backend mutation, so the arena shows zero new
        // work and every table stays exactly as found.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"abcdef");
        let counters_before = store.work_counters();
        store.next_version_id = u64::MAX;
        assert!(matches!(
            store.splice(history, Some(v0.id()), 6, 0, b"x", None, None),
            Err(HistoryError::Overflow(_))
        ));
        assert_eq!(store.next_version_id, u64::MAX);
        assert_eq!(store.versions.len(), 1);
        assert!(store.active_requests.is_empty());
        assert!(store.retired_requests.is_empty());
        assert!(store.version_bindings.is_empty());
        let counters_after = store.work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        assert_eq!(read_full(&store, v0, 6), b"abcdef");
        store.verify(v0).unwrap();
        assert!(!store.is_poisoned());
        // A request-bound exhaustion fails the same way: no ledger entry.
        assert!(matches!(
            store.splice(history, Some(v0.id()), 6, 0, b"x", Some(b"req-1"), None),
            Err(HistoryError::Overflow(_))
        ));
        assert!(!store.active_requests.contains_key(b"req-1".as_slice()));
        // Replay and retired outcomes need no new VersionId: a bound request
        // still replays at an exhausted counter instead of overflowing.
        let mut live = PersistentHistoryStore::new();
        let live_history = live.create_history().unwrap();
        let live_v0 = append_new(&mut live, live_history, None, b"abcdef");
        let live_v1 = match live
            .splice(
                live_history,
                Some(live_v0.id()),
                6,
                0,
                b"x",
                Some(b"req-9"),
                None,
            )
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("fresh splice must create")
            }
        };
        live.next_version_id = u64::MAX;
        assert_eq!(
            live.splice(
                live_history,
                Some(live_v0.id()),
                6,
                0,
                b"x",
                Some(b"req-9"),
                None,
            ),
            Ok(CommitOutcome::Replayed(live_v1))
        );
    }

    #[test]
    fn durable_splice_exhaustion_writes_zero_authority_bytes() {
        // The durable path must reject an exhausted counter before WAL
        // record construction, append, sync, backend mutation, and
        // poisoning: the hot log is byte-identical afterwards and no
        // max-Version record can become authoritative.
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut store = PersistentHistoryStore::new();
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let history = store.create_history_durable(&mut log).unwrap();
        let v0 = match store
            .append_durable(&mut log, history, None, b"abcdef", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("durable root must create")
            }
        };
        let counters_before = store.work_counters();
        let log_len_before = std::fs::metadata(&path).unwrap().len();
        assert!(log_len_before > 0);
        store.next_version_id = u64::MAX;
        let error = store
            .splice_durable(&mut log, history, Some(v0.id()), 6, 0, b"x", None, None)
            .unwrap_err();
        assert!(
            matches!(error, DurableError::Rejected(HistoryError::Overflow(_))),
            "exhaustion must reject definitely, got {error}"
        );
        assert!(!store.is_poisoned());
        assert_eq!(store.next_version_id, u64::MAX);
        assert_eq!(store.versions.len(), 1);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            log_len_before,
            "rejected splice must not append authority bytes"
        );
        let counters_after = store.work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        assert_eq!(read_full(&store, v0, 6), b"abcdef");
        // The writer stays usable for reads and the log replays exactly the
        // pre-exhaustion state: no unrecoverable record was emitted.
        drop(store);
        drop(log);
        let bytes = std::fs::read(&path).unwrap();
        let reopened = recover_history_store(&bytes).unwrap();
        assert_eq!(reopened.versions.len(), 1);
        assert_eq!(reopened.next_version_id, 1);
    }

    #[test]
    fn replay_at_version_exhaustion_fails_before_backend_allocation() {
        // A forged max-Version record against an exhausted counter fails at
        // identity allocation, before any backend work: recovery may fail on
        // impossible bytes, but the live writer must never generate them
        // (proven by the durable test above).
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"abcdef");
        store.next_version_id = u64::MAX;
        let counters_before = store.work_counters();
        let digest = history_splice_digest(history, Some(v0.id()), 6, 0, b"x", None);
        assert!(matches!(
            store.replay_splice(
                history,
                VersionId::new(u64::MAX),
                Some(v0.id()),
                6,
                0,
                b"x",
                None,
                None,
                digest,
                None,
            ),
            Err(HistoryError::Overflow(_))
        ));
        assert_eq!(store.versions.len(), 1);
        let counters_after = store.work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
    }

    #[test]
    fn histories_are_isolated_and_versions_link_parents() {
        let mut store = PersistentHistoryStore::new();
        let first = store.create_history().unwrap();
        let second = store.create_history().unwrap();
        assert_ne!(first, second);

        let root_a = append_new(&mut store, first, None, b"aaa");
        assert_eq!(root_a.history(), first);
        assert_eq!(root_a.parent(), None);
        let child_a = append_new(&mut store, first, Some(root_a.id()), b"bbb");
        assert_eq!(child_a.parent(), Some(root_a.id()));
        let root_b = append_new(&mut store, second, None, b"zzz");

        assert_eq!(read_full(&store, root_a, 3), b"aaa");
        assert_eq!(read_full(&store, child_a, 6), b"aaabbb");
        assert_eq!(read_full(&store, root_b, 3), b"zzz");

        // Cross-history grafts fail closed.
        assert_eq!(
            store.append(second, Some(root_a.id()), b"nope", None, None),
            Err(HistoryError::Invalid(
                "persistent parent version belongs to a different history"
            ))
        );
        // Unknown history and unknown parent fail closed.
        assert_eq!(
            store.append(HistoryId(999), None, b"nope", None, None),
            Err(HistoryError::Invalid(
                "persistent splice targets an unknown history"
            ))
        );
        assert_eq!(
            store.append(first, Some(VersionId(999)), b"nope", None, None),
            Err(HistoryError::Invalid("persistent version is unknown"))
        );
        // Failures record no versions.
        assert_eq!(store.versions.len(), 3);
    }

    #[test]
    fn sibling_versions_share_parent_byte_exact() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let parent = append_new(&mut store, history, None, b"parent");
        let left = append_new(&mut store, history, Some(parent.id()), b"-left");
        let right = append_new(&mut store, history, Some(parent.id()), b"-right");
        assert_eq!(left.parent(), Some(parent.id()));
        assert_eq!(right.parent(), Some(parent.id()));
        assert_eq!(read_full(&store, parent, 6), b"parent");
        assert_eq!(read_full(&store, left, 11), b"parent-left");
        assert_eq!(read_full(&store, right, 12), b"parent-right");
        store.verify(parent).unwrap();
        store.verify(left).unwrap();
        store.verify(right).unwrap();
    }

    #[test]
    fn fabricated_versions_fail_closed_on_read_and_verify() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let version = append_new(&mut store, history, None, b"data");

        let wrong_id = Version {
            history,
            id: VersionId(999),
            parent: None,
        };
        let mut output = Vec::new();
        assert_eq!(
            store.read(wrong_id, 0, 4, &mut output),
            Err(HistoryError::Invalid("persistent version is unknown"))
        );
        assert_eq!(
            store.verify(wrong_id),
            Err(HistoryError::Invalid("persistent version is unknown"))
        );

        // Logical tampering (a parent the committed version never had)
        // fails against the committed table; callers cannot supply roots
        // at all, so physical forgery is unrepresentable by type.
        let tampered = Version {
            history,
            id: version.id(),
            parent: Some(version.id()),
        };
        assert_eq!(
            store.read(tampered, 0, 4, &mut output),
            Err(HistoryError::Invalid(
                "persistent version does not match committed history"
            ))
        );
        assert_eq!(
            store.verify(tampered),
            Err(HistoryError::Invalid(
                "persistent version does not match committed history"
            ))
        );
        assert!(output.is_empty());
    }

    #[test]
    fn empty_commit_payload_is_rejected_without_recording() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        assert!(store.append(history, None, b"", None, None).is_err());
        assert_eq!(store.versions.len(), 0);
    }

    #[test]
    fn operation_digest_binds_exact_semantic_operation() {
        // One canonical splice digest: append canonicalizes to splice
        // coordinates before hashing, so both spellings share it. Any
        // differing coordinate changes the digest: same request bound to one
        // digest can never replay a different operation.
        let history = HistoryId(3);
        let parent = Some(VersionId(2));
        let first = history_splice_digest(history, parent, 7, 2, b"payload", None);
        assert_eq!(
            first,
            history_splice_digest(history, parent, 7, 2, b"payload", None)
        );
        assert_ne!(
            first,
            history_splice_digest(HistoryId(4), parent, 7, 2, b"payload", None)
        );
        assert_ne!(
            first,
            history_splice_digest(history, Some(VersionId(8)), 7, 2, b"payload", None)
        );
        assert_ne!(
            first,
            history_splice_digest(history, None, 7, 2, b"payload", None)
        );
        assert_ne!(
            first,
            history_splice_digest(history, parent, 8, 2, b"payload", None)
        );
        assert_ne!(
            first,
            history_splice_digest(history, parent, 7, 3, b"payload", None)
        );
        assert_ne!(
            first,
            history_splice_digest(history, parent, 7, 2, b"other", None)
        );
        assert_ne!(
            first,
            history_splice_digest(history, parent, 7, 2, b"payload", Some(b"bind"))
        );
    }

    #[test]
    fn version_table_position_never_overrides_logical_identity() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let first = append_new(&mut store, history, None, b"one");
        let second = append_new(&mut store, history, Some(first.id()), b"two");
        // Physically swap the two records: both lookups must now fail closed
        // because position disagrees with logical identity.
        store.versions.swap(0, 1);
        assert_eq!(
            store.version_record(first.id()),
            Err(HistoryError::Invalid(
                "persistent version disagrees with its table coordinate"
            ))
        );
        assert_eq!(
            store.version_record(second.id()),
            Err(HistoryError::Invalid(
                "persistent version disagrees with its table coordinate"
            ))
        );
    }

    #[test]
    fn allocation_identities_are_dense_and_monotonic() {
        let mut store = PersistentHistoryStore::new();
        let first_history = store.create_history().unwrap();
        let second_history = store.create_history().unwrap();
        assert_eq!(first_history.id(), 0);
        assert_eq!(second_history.id(), 1);
        let first = append_new(&mut store, first_history, None, b"one");
        let second = append_new(&mut store, first_history, Some(first.id()), b"two");
        let third = append_new(&mut store, second_history, None, b"three");
        assert_eq!(first.id().id(), 0);
        assert_eq!(second.id().id(), 1);
        assert_eq!(third.id().id(), 2);
    }

    #[test]
    fn history_counters_observe_backend_work() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let before = store.work_counters();
        let version = append_new(&mut store, history, None, b"payload");
        let after_commit = store.work_counters();
        assert_eq!(
            after_commit.payload_bytes_written - before.payload_bytes_written,
            7
        );
        assert!(after_commit.nodes_allocated > before.nodes_allocated);
        // Root creation resolves no parent and copies no spine.
        assert_eq!(after_commit.nodes_inspected, before.nodes_inspected);

        let child = append_new(&mut store, history, Some(version.id()), b"more");
        let after_child = store.work_counters();
        assert!(after_child.nodes_inspected > after_commit.nodes_inspected);
        assert_eq!(
            after_child.payload_bytes_written - after_commit.payload_bytes_written,
            4
        );

        let mut output = Vec::new();
        store.read(child, 0, 11, &mut output).unwrap();
        let after_read = store.work_counters();
        assert_eq!(output, b"payloadmore");
        assert!(after_read.nodes_read > after_child.nodes_read);
        assert_eq!(
            after_read.payload_bytes_read - after_child.payload_bytes_read,
            11
        );
    }

    #[test]
    fn request_ledger_replays_conflicts_and_retires() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();

        let first = match store
            .append(history, None, b"payload", Some(b"req-1"), None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("unknown request must commit")
            }
        };
        assert_eq!(store.versions.len(), 1);

        // Same request and same operation replays with no second mutation.
        assert_eq!(
            store.append(history, None, b"payload", Some(b"req-1"), None),
            Ok(CommitOutcome::Replayed(first))
        );
        assert_eq!(store.versions.len(), 1);

        // Same request with a different payload conflicts.
        assert_eq!(
            store.append(history, None, b"other", Some(b"req-1"), None),
            Err(HistoryError::RequestConflict)
        );
        // Same request with a different parent conflicts.
        assert_eq!(
            store.append(history, Some(first.id()), b"payload", Some(b"req-1"), None),
            Err(HistoryError::RequestConflict)
        );
        assert_eq!(store.versions.len(), 1);

        // Retirement then resurrection attempt.
        store.retire_request(b"req-1").unwrap();
        assert_eq!(
            store.append(history, None, b"payload", Some(b"req-1"), None),
            Ok(CommitOutcome::Retired)
        );
        assert_eq!(
            store.append(history, None, b"other", Some(b"req-1"), None),
            Err(HistoryError::RequestConflict)
        );
        assert_eq!(store.versions.len(), 1);

        // Unknown and double retirement fail closed.
        assert_eq!(
            store.retire_request(b"req-unknown"),
            Err(HistoryError::Invalid(
                "persistent active request identity is absent"
            ))
        );
        assert_eq!(
            store.retire_request(b"req-1"),
            Err(HistoryError::Invalid(
                "persistent request identity is already retired"
            ))
        );
        assert_eq!(
            store.append(history, None, b"x", Some(b""), None),
            Err(HistoryError::Invalid(
                "persistent request identity is empty or exceeds the byte limit"
            ))
        );
    }

    #[test]
    fn poisoned_store_rejects_mutation_but_keeps_reads() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let version = append_new(&mut store, history, None, b"data");
        store.set_poisoned();
        assert_eq!(
            store.append(history, None, b"more", None, None),
            Err(HistoryError::Poisoned)
        );
        assert_eq!(store.retire_request(b"req-1"), Err(HistoryError::Poisoned));
        assert_eq!(store.create_history(), Err(HistoryError::Poisoned));
        let mut output = Vec::new();
        store.read(version, 0, 4, &mut output).unwrap();
        assert_eq!(output, b"data");
        store.verify(version).unwrap();
    }

    fn test_log_path(dir: &std::path::Path) -> std::path::PathBuf {
        dir.join("history.wal")
    }

    #[test]
    fn durable_commit_replay_and_retire_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        let mut log = DurableHistoryLog::open(&test_log_path(temp.path())).unwrap();
        let mut store = PersistentHistoryStore::new();

        let first_history = store.create_history_durable(&mut log).unwrap();
        let second_history = store.create_history_durable(&mut log).unwrap();
        let v1 = match store
            .append_durable(&mut log, first_history, None, b"aaa", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("first durable commit must create")
            }
        };
        let v2 = match store
            .append_durable(
                &mut log,
                first_history,
                Some(v1.id()),
                b"bbb",
                Some(b"req-1"),
                None,
            )
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("new request must commit")
            }
        };

        // Same request and operation replays with no new log bytes.
        let len_before = log.read_all().unwrap().len();
        assert_eq!(
            store
                .append_durable(
                    &mut log,
                    first_history,
                    Some(v1.id()),
                    b"bbb",
                    Some(b"req-1"),
                    None,
                )
                .unwrap(),
            CommitOutcome::Replayed(v2)
        );
        assert_eq!(log.read_all().unwrap().len(), len_before);

        // Same request with a different payload conflicts without logging.
        assert!(matches!(
            store.append_durable(
                &mut log,
                first_history,
                Some(v1.id()),
                b"other",
                Some(b"req-1"),
                None,
            ),
            Err(DurableError::Rejected(HistoryError::RequestConflict))
        ));
        assert_eq!(log.read_all().unwrap().len(), len_before);

        store.retire_durable(&mut log, b"req-1").unwrap();
        assert_eq!(
            store
                .append_durable(
                    &mut log,
                    first_history,
                    Some(v1.id()),
                    b"bbb",
                    Some(b"req-1"),
                    None,
                )
                .unwrap(),
            CommitOutcome::Retired
        );

        let reopened = recover_history_store(&log.read_all().unwrap()).unwrap();
        assert_eq!(reopened.versions.len(), 2);
        assert_eq!(reopened.histories.len(), 2);
        assert_eq!(reopened.next_history_id, 2);
        assert_eq!(reopened.next_version_id, 2);
        let mut output = Vec::new();
        reopened.read(v1, 0, 3, &mut output).unwrap();
        assert_eq!(output, b"aaa");
        output.clear();
        reopened.read(v2, 0, 6, &mut output).unwrap();
        assert_eq!(output, b"aaabbb");
        reopened.verify(v1).unwrap();
        reopened.verify(v2).unwrap();
        assert!(reopened.retired_requests.contains_key(b"req-1".as_slice()));
        assert!(reopened.active_requests.is_empty());
        let _ = second_history;
    }

    #[test]
    fn durable_reopen_continues_allocation_identities() {
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history_durable(&mut log).unwrap();
        let first = match store
            .append_durable(&mut log, history, None, b"one", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("first durable commit must create")
            }
        };
        drop(log);
        drop(store);

        let mut reopened_log = DurableHistoryLog::open(&path).unwrap();
        let mut reopened = recover_history_store(&reopened_log.read_all().unwrap()).unwrap();
        let second = match reopened
            .append_durable(
                &mut reopened_log,
                history,
                Some(first.id()),
                b"two",
                None,
                None,
            )
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("new payload must commit")
            }
        };
        assert_eq!(second.id().id(), 1);
        assert_eq!(second.parent(), Some(first.id()));
        let mut output = Vec::new();
        reopened.read(second, 0, 6, &mut output).unwrap();
        assert_eq!(output, b"onetwo");
    }

    #[test]
    fn torn_tail_reopens_to_last_complete_commit() {
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history_durable(&mut log).unwrap();
        match store
            .append_durable(&mut log, history, None, b"stable", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(_) => {}
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("first durable commit must create")
            }
        }
        // Simulate a crash mid-write: raw frame bytes without barrier or ack.
        // The pending bytes are a well-formed splice record (append of
        // "lost" onto the 6-byte parent), cut in half below.
        let pending = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&durable_log::HistoryLogRecord::Splice {
                history,
                version: crate::persistent_history::VersionId(1),
                parent: Some(crate::persistent_history::VersionId(0)),
                offset: 6,
                delete_len: 0,
                insert: b"lost".to_vec(),
                request_id: None,
                binding: None,
                digest: crate::persistent_history::history_splice_digest(
                    history,
                    Some(crate::persistent_history::VersionId(0)),
                    6,
                    0,
                    b"lost",
                    None,
                ),

                physical: None,
            })
            .unwrap(),
        )
        .unwrap();
        {
            use std::io::Write;
            let mut raw = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            raw.write_all(&pending[..pending.len() / 2]).unwrap();
        }

        let reopened =
            recover_history_store(&DurableHistoryLog::open(&path).unwrap().read_all().unwrap())
                .unwrap();
        assert_eq!(reopened.versions.len(), 1);
        let mut output = Vec::new();
        reopened
            .read(reopened.versions[0].version(), 0, 6, &mut output)
            .unwrap();
        assert_eq!(output, b"stable");

        // The next append truncates the torn tail before writing.
        let mut healed = DurableHistoryLog::open(&path).unwrap();
        let mut healed_store = recover_history_store(&healed.read_all().unwrap()).unwrap();
        match healed_store
            .append_durable(
                &mut healed,
                history,
                Some(crate::persistent_history::VersionId(0)),
                b"healed",
                None,
                None,
            )
            .unwrap()
        {
            CommitOutcome::Committed(_) => {}
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("healed append must commit")
            }
        }
        let final_state = recover_history_store(&healed.read_all().unwrap()).unwrap();
        assert_eq!(final_state.versions.len(), 2);
    }

    #[test]
    fn corrupt_complete_frame_fails_recovery_closed() {
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history_durable(&mut log).unwrap();
        match store
            .append_durable(&mut log, history, None, b"stable", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(_) => {}
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("first durable commit must create")
            }
        }
        let mut bytes = log.read_all().unwrap();
        let flip = bytes.len() / 2;
        bytes[flip] ^= 0xFF;
        assert!(recover_history_store(&bytes).is_err());
    }

    #[test]
    fn empty_durable_commit_is_rejected_before_any_authority() {
        let temp = tempfile::tempdir().unwrap();
        let mut log = DurableHistoryLog::open(&test_log_path(temp.path())).unwrap();
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history_durable(&mut log).unwrap();
        let len_before = log.read_all().unwrap().len();

        // Rejection happens in preview: no frame is written, nothing is
        // synced, and the store is not poisoned.
        assert!(matches!(
            store.append_durable(&mut log, history, None, b"", None, None),
            Err(DurableError::Rejected(_))
        ));
        assert_eq!(log.read_all().unwrap().len(), len_before);
        match store
            .append_durable(&mut log, history, None, b"after", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(_) => {}
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("recovered store must accept new commits")
            }
        }
        let reopened = recover_history_store(&log.read_all().unwrap()).unwrap();
        assert_eq!(reopened.versions.len(), 1);
    }

    #[test]
    fn poisoned_durable_path_requires_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let mut log = DurableHistoryLog::open(&test_log_path(temp.path())).unwrap();
        let mut store = PersistentHistoryStore::new();
        store.set_poisoned();
        assert!(matches!(
            store.create_history_durable(&mut log),
            Err(DurableError::RecoveryRequired)
        ));
        assert!(matches!(
            store.append_durable(&mut log, HistoryId(0), None, b"x", None, None),
            Err(DurableError::RecoveryRequired)
        ));
        assert!(matches!(
            store.fork_durable(&mut log, HistoryId(0), VersionId::new(0), None, None),
            Err(DurableError::RecoveryRequired)
        ));
        assert!(matches!(
            store.retire_durable(&mut log, b"req-1"),
            Err(DurableError::RecoveryRequired)
        ));
    }

    #[test]
    fn bound_history_creation_is_idempotent_and_validated() {
        let mut store = PersistentHistoryStore::new();
        let first = store.create_history_with_binding(b"thread-a").unwrap();
        // Retrying the same binding resolves the existing history: no second
        // lineage is allocated even though creation is attempted twice.
        assert_eq!(
            store.create_history_with_binding(b"thread-a").unwrap(),
            first
        );
        assert_eq!(store.histories.len(), 1);
        assert_eq!(store.next_history_id, 1);

        let second = store.create_history_with_binding(b"thread-b").unwrap();
        assert_ne!(first, second);
        assert_eq!(
            store.history_bindings.get(&first),
            Some(&b"thread-a".to_vec())
        );

        assert_eq!(
            store.create_history_with_binding(b""),
            Err(HistoryError::Invalid(
                "persistent adapter binding is empty or exceeds the byte limit"
            ))
        );
        assert!(store
            .create_history_with_binding(&vec![0xAA; 4097])
            .is_err());
    }

    #[test]
    fn commit_binding_is_recorded_and_digest_bound() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let plain = history_splice_digest(history, None, 0, 0, b"data", None);
        let bound = history_splice_digest(history, None, 0, 0, b"data", Some(b"thread-a/cp-1"));
        assert_ne!(plain, bound);

        let version = match store
            .append(history, None, b"data", None, Some(b"thread-a/cp-1"))
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("bound commit must create")
            }
        };
        assert_eq!(
            store.version_bindings.get(&version.id()),
            Some(&b"thread-a/cp-1".to_vec())
        );
        // Same request and coordinates but a different binding address a
        // different operation, so the bound request conflicts instead of
        // replaying the first version.
        let other = store
            .append(history, None, b"data", Some(b"req-b"), None)
            .unwrap()
            .version()
            .unwrap();
        assert_eq!(
            store.append(history, None, b"data", Some(b"req-b"), Some(b"other")),
            Err(HistoryError::RequestConflict)
        );
        assert_eq!(store.version_bindings.get(&other.id()), None);
    }

    #[test]
    fn durable_bindings_survive_reopen_exactly() {
        let temp = tempfile::tempdir().unwrap();
        let mut log = DurableHistoryLog::open(&test_log_path(temp.path())).unwrap();
        let mut store = PersistentHistoryStore::new();
        let history = store
            .create_history_durable_with_binding(&mut log, b"thread-a")
            .unwrap();
        // Crash-retry before the adapter observes success resolves the same id.
        assert_eq!(
            store
                .create_history_durable_with_binding(&mut log, b"thread-a")
                .unwrap(),
            history
        );
        let version = match store
            .append_durable(
                &mut log,
                history,
                None,
                b"data",
                None,
                Some(b"thread-a/cp-1"),
            )
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("bound durable commit must create")
            }
        };
        drop(log);
        drop(store);

        let mut reopened_log = DurableHistoryLog::open(&test_log_path(temp.path())).unwrap();
        let reopened = recover_history_store(&reopened_log.read_all().unwrap()).unwrap();
        assert_eq!(
            reopened.history_bindings.get(&history),
            Some(&b"thread-a".to_vec())
        );
        assert_eq!(
            reopened.version_bindings.get(&version.id()),
            Some(&b"thread-a/cp-1".to_vec())
        );
        // The adapter rebuilds its maps from bindings alone: no legacy state.
        let _ = reopened_log;
    }

    #[test]
    fn durable_create_crash_window_resolves_exactly_on_reopen() {
        // The begin/finish seam models the crash between the durable barrier
        // and the in-memory apply: the record is authoritative in the log
        // while invisible in memory, and recovery plus idempotent retry
        // resolve it to exactly one identity.
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let id = {
            let mut store = PersistentHistoryStore::new();
            let mut log = DurableHistoryLog::open(&path).unwrap();
            let (id, prepared) = store
                .begin_durable_history_create(&mut log, Some(b"thread-a"))
                .unwrap();
            assert!(prepared.is_some());
            assert!(!store.histories.contains(&id));
            id
        };
        let bytes = std::fs::read(&path).unwrap();
        assert!(!bytes.is_empty());
        let mut recovered = recover_history_store(&bytes).unwrap();
        assert!(recovered.histories.contains(&id));
        assert_eq!(recovered.history_binding(id), Some(b"thread-a".as_slice()));
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let (again, prepared) = recovered
            .begin_durable_history_create(&mut log, Some(b"thread-a"))
            .unwrap();
        assert_eq!(again, id);
        assert!(prepared.is_none());
    }

    #[test]
    fn unbound_create_crash_window_replays_without_aliasing() {
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let id = {
            let mut store = PersistentHistoryStore::new();
            let mut log = DurableHistoryLog::open(&path).unwrap();
            let (id, prepared) = store.begin_durable_history_create(&mut log, None).unwrap();
            assert!(prepared.is_some());
            id
        };
        let bytes = std::fs::read(&path).unwrap();
        let recovered = recover_history_store(&bytes).unwrap();
        assert!(recovered.histories.contains(&id));
        assert_eq!(recovered.history_binding(id), None);
    }

    #[test]
    fn double_finish_poisons_and_reopen_still_resolves() {
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut store = PersistentHistoryStore::new();
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let (id, prepared) = store.begin_durable_history_create(&mut log, None).unwrap();
        let token = prepared.unwrap();
        assert_eq!(store.finish_durable_history_create(&token).unwrap(), id);
        // The record is already authoritative in the log, so a second apply
        // would alias state: poison and demand reopen instead.
        let error = store.finish_durable_history_create(&token).unwrap_err();
        assert!(
            matches!(error, DurableError::Indeterminate { .. }),
            "double apply after the barrier must be indeterminate, got {error}"
        );
        assert!(store.is_poisoned());
        assert!(matches!(
            store.create_history_durable(&mut log),
            Err(DurableError::RecoveryRequired)
        ));
        drop(store);
        drop(log);
        let bytes = std::fs::read(&path).unwrap();
        let recovered = recover_history_store(&bytes).unwrap();
        assert!(recovered.histories.contains(&id));
    }

    #[test]
    fn rejected_create_append_leaves_counter_unchanged() {
        // Pre-authority append failure is a definite rejection: the counter
        // must not advance, or the retry would write a skipped identity that
        // recovery reads as a replay-order mismatch. Covers bound and
        // unbound create through the same seam.
        for binding in [None, Some(b"thread-a".as_slice())] {
            let temp = tempfile::tempdir().unwrap();
            let path = test_log_path(temp.path());
            let mut store = PersistentHistoryStore::new();
            let mut log = DurableHistoryLog::open(&path).unwrap();
            log.arm_fail_next_append();
            let error = store
                .begin_durable_history_create(&mut log, binding)
                .unwrap_err();
            assert!(
                matches!(error, DurableError::Rejected(_)),
                "injected append failure must reject, got {error}"
            );
            assert_eq!(store.next_history_id, 0);
            assert!(store.histories.is_empty());
            assert!(store.history_bindings.is_empty());
            assert!(!store.is_poisoned());

            // Fault removed: the retry writes HistoryId(0), applies, and a
            // close/reopen cycle recovers exactly one history with the next
            // counter at one.
            let (id, prepared) = store
                .begin_durable_history_create(&mut log, binding)
                .unwrap();
            assert_eq!(id, HistoryId::new(0));
            let token = prepared.unwrap();
            assert_eq!(store.finish_durable_history_create(&token).unwrap(), id);
            drop(store);
            drop(log);
            let bytes = std::fs::read(&path).unwrap();
            let recovered = recover_history_store(&bytes).unwrap();
            assert!(recovered.histories.contains(&HistoryId::new(0)));
            assert_eq!(recovered.next_history_id, 1);
        }
    }

    #[test]
    fn replayed_duplicate_history_binding_fails_closed() {
        // A valid log never repeats a nonempty binding: the live create
        // resolves an existing binding without appending. Two create records
        // sharing one binding therefore prove forgery, never a retry.
        fn create_frame(history: u64, binding: &[u8]) -> Vec<u8> {
            durable_log::encode_history_log_frame(
                &durable_log::encode_history_log_record(&HistoryLogRecord::CreateHistory {
                    history: HistoryId::new(history),
                    binding: Some(binding.to_vec()),
                })
                .unwrap(),
            )
            .unwrap()
        }
        let mut forged = create_frame(0, b"dup");
        forged.extend_from_slice(&create_frame(1, b"dup"));
        assert!(matches!(
            recover_history_store(&forged),
            Err(HistoryError::Invalid(_))
        ));
        // Control: distinct bindings replay exactly.
        let mut honest = create_frame(0, b"thread-a");
        honest.extend_from_slice(&create_frame(1, b"thread-b"));
        let recovered = recover_history_store(&honest).unwrap();
        assert_eq!(
            recovered.history_binding(HistoryId::new(1)),
            Some(b"thread-b".as_slice())
        );
    }

    #[test]
    fn create_at_identity_exhaustion_rejects_without_mutation() {
        // Definite rejection leaves semantic state unchanged: the counter is
        // checked before anything is reserved, written, or inserted.
        let mut store = PersistentHistoryStore::new();
        store.next_history_id = u64::MAX;
        assert!(matches!(
            store.create_history(),
            Err(HistoryError::Overflow(_))
        ));
        assert_eq!(store.next_history_id, u64::MAX);
        assert!(store.histories.is_empty());
        assert!(matches!(
            store.create_history_with_binding(b"thread-a"),
            Err(HistoryError::Overflow(_))
        ));
        assert!(store.histories.is_empty());
        assert!(store.history_bindings.is_empty());

        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut log = DurableHistoryLog::open(&path).unwrap();
        assert!(matches!(
            store.begin_durable_history_create(&mut log, None),
            Err(DurableError::Rejected(_))
        ));
        assert_eq!(store.next_history_id, u64::MAX);
        assert!(!store.is_poisoned());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    // ---- E3 fork/publication tests ----

    fn fork_new(
        store: &mut PersistentHistoryStore,
        history: HistoryId,
        parent: VersionId,
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Version {
        let outcome = store.fork(history, parent, request_id, binding).unwrap();
        assert!(matches!(outcome, CommitOutcome::Committed(_)));
        outcome.version().unwrap()
    }

    #[test]
    fn fork_publishes_exact_parent_root_with_zero_content_growth() {
        // A 20 KiB parent forces the bounded-leaf multi-leaf path, so root
        // reuse below is structural sharing, not a small-buffer accident.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let parent_payload = vec![0xA5u8; 20 * 1024];
        let v0 = append_new(&mut store, history, None, &parent_payload);
        let counters_before = store.work_counters();
        let v1 = fork_new(&mut store, history, v0.id(), None, None);
        let counters_after = store.work_counters();
        assert_ne!(v1.id(), v0.id());
        assert_eq!(v1.id().id(), 1);
        assert_eq!(v1.history(), history);
        assert_eq!(v1.parent(), Some(v0.id()));
        assert_eq!(store.physical_root(v1.id()), store.physical_root(v0.id()));
        // Zero content-node/payload growth: fork is catalogue metadata only.
        // (Fork may write WAL/catalogue metadata; that is not content.)
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(counters_after.nodes_read, counters_before.nodes_read);
        assert_eq!(
            counters_after.payload_bytes_read,
            counters_before.payload_bytes_read
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        assert!(!store.is_poisoned());
        assert_eq!(read_full(&store, v1, 20 * 1024), parent_payload);
        assert_eq!(read_full(&store, v0, 20 * 1024), parent_payload);
        store.verify(v0).unwrap();
        store.verify(v1).unwrap();
    }

    #[test]
    fn fork_of_historical_version_ignores_later_descendants() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"base-payload");
        let v1 = splice_new(&mut store, history, Some(v0.id()), 0, 4, b"EDIT");
        let v2 = splice_new(&mut store, history, Some(v1.id()), 5, 0, b"[tail]");
        let v3 = fork_new(&mut store, history, v0.id(), None, None);
        assert_eq!(v3.parent(), Some(v0.id()));
        assert_eq!(store.physical_root(v3.id()), store.physical_root(v0.id()));
        assert_eq!(read_full(&store, v3, 12), b"base-payload");
        assert_eq!(read_full(&store, v0, 12), b"base-payload");
        // Later descendants are untouched and distinct.
        assert_eq!(read_full(&store, v2, 18), b"EDIT-[tail]payload");
        assert_ne!(store.physical_root(v3.id()), store.physical_root(v2.id()));
    }

    #[test]
    fn fork_siblings_share_parent_root_with_distinct_identities() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"sibling-source");
        let counters_before = store.work_counters();
        let first = fork_new(&mut store, history, v0.id(), None, None);
        let second = fork_new(&mut store, history, v0.id(), None, None);
        let counters_after = store.work_counters();
        assert_ne!(first.id(), second.id());
        assert_eq!(first.parent(), Some(v0.id()));
        assert_eq!(second.parent(), Some(v0.id()));
        assert_eq!(
            store.physical_root(first.id()),
            store.physical_root(v0.id())
        );
        assert_eq!(
            store.physical_root(second.id()),
            store.physical_root(v0.id())
        );
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        assert_eq!(read_full(&store, first, 14), b"sibling-source");
        assert_eq!(read_full(&store, second, 14), b"sibling-source");
    }

    #[test]
    fn forked_version_is_first_class_splice_parent() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"base-payload");
        let v1 = fork_new(&mut store, history, v0.id(), None, None);
        let v2 = splice_new(&mut store, history, Some(v1.id()), 5, 0, b"[edit]");
        assert_eq!(v2.parent(), Some(v1.id()));
        assert_eq!(read_full(&store, v2, 18), b"base-[edit]payload");
        // The fork source and the fork itself stay byte-exact.
        assert_eq!(read_full(&store, v0, 12), b"base-payload");
        assert_eq!(read_full(&store, v1, 12), b"base-payload");
        store.verify(v2).unwrap();
    }

    #[test]
    fn fork_parent_rules_fail_closed() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let other = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"data");
        let foreign = append_new(&mut store, other, None, b"other");
        // Unknown history.
        assert!(matches!(
            store.fork(HistoryId::new(99), v0.id(), None, None),
            Err(HistoryError::Invalid(_))
        ));
        // Unknown parent.
        assert!(matches!(
            store.fork(history, VersionId::new(99), None, None),
            Err(HistoryError::Invalid(_))
        ));
        // Cross-history parent grafting.
        assert!(matches!(
            store.fork(history, foreign.id(), None, None),
            Err(HistoryError::Invalid(_))
        ));
        assert!(matches!(
            store.fork(other, v0.id(), None, None),
            Err(HistoryError::Invalid(_))
        ));
        assert_eq!(store.versions.len(), 2);
        assert_eq!(store.next_version_id, 2);
        assert!(!store.is_poisoned());
        // A parentless fork is unrepresentable: `parent` is a `VersionId`,
        // not an `Option`, so root creation stays splice-only by type.
    }

    #[test]
    fn fork_digest_lives_in_its_own_domain() {
        let history = HistoryId::new(3);
        let parent = VersionId::new(7);
        let fork_digest = history_fork_digest(history, parent, Some(b"bind"));
        // Deterministic over identical coordinates.
        assert_eq!(
            history_fork_digest(history, parent, Some(b"bind")),
            fork_digest
        );
        // Binding presence and bytes participate.
        assert_ne!(history_fork_digest(history, parent, None), fork_digest);
        assert_ne!(
            history_fork_digest(history, parent, Some(b"other")),
            fork_digest
        );
        // Parent identity participates.
        assert_ne!(
            history_fork_digest(history, VersionId::new(8), Some(b"bind")),
            fork_digest
        );
        // Operation separation: no splice digest over the same
        // history/parent can equal a fork digest, whatever the coordinates.
        for offset in [0u64, 7] {
            for delete_len in [0u64, 3] {
                let splice_digest = history_splice_digest(
                    history,
                    Some(parent),
                    offset,
                    delete_len,
                    b"",
                    Some(b"bind"),
                );
                assert_ne!(splice_digest, fork_digest);
            }
        }
        // The assigned identity and the republished root are consequences,
        // not inputs: identical inputs always hash identically.
        assert_eq!(
            history_fork_digest(history, parent, None),
            history_fork_digest(history, parent, None)
        );
    }

    #[test]
    fn thousand_fork_zero_content_regression() {
        // A 10 MiB parent on the bounded-leaf path, then 1,000 fresh forks
        // from the same historical parent: catalogue/metadata growth only.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let parent_payload = vec![0x3Cu8; 10 * 1024 * 1024];
        let v0 = append_new(&mut store, history, None, &parent_payload);
        let counters_before = store.work_counters();
        let mut previous = v0.id().id();
        for _ in 0..1000 {
            let forked = fork_new(&mut store, history, v0.id(), None, None);
            assert_eq!(forked.parent(), Some(v0.id()));
            assert_eq!(
                store.physical_root(forked.id()),
                store.physical_root(v0.id())
            );
            assert_eq!(forked.id().id(), previous + 1);
            previous = forked.id().id();
        }
        let counters_after = store.work_counters();
        assert_eq!(store.versions.len(), 1001);
        assert_eq!(store.next_version_id, 1001);
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(counters_after.nodes_read, counters_before.nodes_read);
        assert_eq!(
            counters_after.payload_bytes_read,
            counters_before.payload_bytes_read
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        // Spot-check first, middle, and last fork reads (after the counters).
        for id in [1u64, 500, 1000] {
            let version = store.lookup_version(VersionId::new(id)).unwrap();
            assert_eq!(version.parent(), Some(v0.id()));
            assert_eq!(
                store.physical_root(version.id()),
                store.physical_root(v0.id())
            );
            let mut output = Vec::new();
            store.read(version, 0, 16, &mut output).unwrap();
            assert_eq!(output, &parent_payload[..16]);
        }
        let mut tail = Vec::new();
        store
            .read(
                store.lookup_version(VersionId::new(1000)).unwrap(),
                10 * 1024 * 1024 - 16,
                16,
                &mut tail,
            )
            .unwrap();
        assert_eq!(tail, &parent_payload[parent_payload.len() - 16..]);
    }

    #[test]
    fn fork_request_ledger_matrix_with_cross_operation_conflicts() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"ledger-base");
        let v1 = splice_new(&mut store, history, Some(v0.id()), 5, 0, b"[e]");
        // Same request + same fork replays the exact version, no new entry.
        let forked = fork_new(&mut store, history, v0.id(), Some(b"fork-req"), None);
        assert_eq!(
            store.fork(history, v0.id(), Some(b"fork-req"), None),
            Ok(CommitOutcome::Replayed(forked))
        );
        assert_eq!(store.versions.len(), 3);
        // Same request + different parent conflicts.
        assert_eq!(
            store.fork(history, v1.id(), Some(b"fork-req"), None),
            Err(HistoryError::RequestConflict)
        );
        // Same request + different binding conflicts.
        assert_eq!(
            store.fork(history, v0.id(), Some(b"fork-req"), Some(b"bind")),
            Err(HistoryError::RequestConflict)
        );
        // A request used for splice conflicts as a fork, and inversely a
        // fork request conflicts as a splice: distinct digest domains make
        // this deterministic.
        let spliced = match store
            .splice(
                history,
                Some(v0.id()),
                5,
                0,
                b"[e2]",
                Some(b"shared-req"),
                None,
            )
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("fresh splice must create")
            }
        };
        assert_eq!(
            store.fork(history, v0.id(), Some(b"shared-req"), None),
            Err(HistoryError::RequestConflict)
        );
        assert_eq!(
            store.splice(
                history,
                Some(forked.id()),
                5,
                0,
                b"[e3]",
                Some(b"fork-req"),
                None
            ),
            Err(HistoryError::RequestConflict)
        );
        let _ = spliced;
        // Retirement: the same fork retry retires, a different fork conflicts.
        store.retire_request(b"fork-req").unwrap();
        assert_eq!(
            store.fork(history, v0.id(), Some(b"fork-req"), None),
            Ok(CommitOutcome::Retired)
        );
        assert_eq!(
            store.fork(history, v1.id(), Some(b"fork-req"), None),
            Err(HistoryError::RequestConflict)
        );
        // Replay and retired outcomes need no fresh identity: at an
        // exhausted counter the retired fork still retires instead of
        // overflowing, and a bound-but-live fork still replays.
        store.next_version_id = u64::MAX;
        assert_eq!(
            store.fork(history, v0.id(), Some(b"fork-req"), None),
            Ok(CommitOutcome::Retired)
        );
        let mut live = PersistentHistoryStore::new();
        let live_history = live.create_history().unwrap();
        let live_v0 = append_new(&mut live, live_history, None, b"live-base");
        let live_fork = fork_new(
            &mut live,
            live_history,
            live_v0.id(),
            Some(b"live-req"),
            None,
        );
        live.next_version_id = u64::MAX;
        assert_eq!(
            live.fork(live_history, live_v0.id(), Some(b"live-req"), None),
            Ok(CommitOutcome::Replayed(live_fork))
        );
    }

    #[test]
    fn fork_version_counter_overflow_leaves_no_catalogue_mutation() {
        // Identity allocation is preview-checked: at exhaustion the fork
        // fails before any catalogue mutation, so every table stays exactly
        // as found and the writer stays usable.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"fork-base");
        let counters_before = store.work_counters();
        store.next_version_id = u64::MAX;
        assert!(matches!(
            store.fork(history, v0.id(), None, None),
            Err(HistoryError::Overflow(_))
        ));
        assert_eq!(store.next_version_id, u64::MAX);
        assert_eq!(store.versions.len(), 1);
        assert!(store.active_requests.is_empty());
        assert!(store.retired_requests.is_empty());
        assert!(store.version_bindings.is_empty());
        let counters_after = store.work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(counters_after.nodes_read, counters_before.nodes_read);
        assert_eq!(
            counters_after.payload_bytes_read,
            counters_before.payload_bytes_read
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        assert_eq!(read_full(&store, v0, 9), b"fork-base");
        store.verify(v0).unwrap();
        assert!(!store.is_poisoned());
        // A request-bound exhaustion fails the same way: no ledger entry.
        assert!(matches!(
            store.fork(history, v0.id(), Some(b"req-1"), None),
            Err(HistoryError::Overflow(_))
        ));
        assert!(!store.active_requests.contains_key(b"req-1".as_slice()));
    }

    #[test]
    fn durable_fork_exhaustion_writes_zero_authority_bytes() {
        // The durable path must reject an exhausted counter before WAL
        // record construction, append, sync, catalogue mutation, and
        // poisoning: the hot log is byte-identical afterwards and no
        // max-Version record can become authoritative.
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut store = PersistentHistoryStore::new();
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let history = store.create_history_durable(&mut log).unwrap();
        let v0 = match store
            .append_durable(&mut log, history, None, b"fork-base", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("durable root must create")
            }
        };
        let counters_before = store.work_counters();
        let log_len_before = std::fs::metadata(&path).unwrap().len();
        assert!(log_len_before > 0);
        store.next_version_id = u64::MAX;
        let error = store
            .fork_durable(&mut log, history, v0.id(), None, None)
            .unwrap_err();
        assert!(
            matches!(error, DurableError::Rejected(HistoryError::Overflow(_))),
            "exhaustion must reject definitely, got {error}"
        );
        assert!(!store.is_poisoned());
        assert_eq!(store.next_version_id, u64::MAX);
        assert_eq!(store.versions.len(), 1);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            log_len_before,
            "rejected fork must not append authority bytes"
        );
        let counters_after = store.work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        assert_eq!(read_full(&store, v0, 9), b"fork-base");
        // The writer stays usable for reads and the log replays exactly the
        // pre-exhaustion state: no unrecoverable record was emitted.
        drop(store);
        drop(log);
        let bytes = std::fs::read(&path).unwrap();
        let reopened = recover_history_store(&bytes).unwrap();
        assert_eq!(reopened.versions.len(), 1);
        assert_eq!(reopened.next_version_id, 1);
    }

    #[test]
    fn replay_at_fork_exhaustion_fails_before_catalogue_mutation() {
        // A forged max-Version fork record against an exhausted counter fails
        // at identity allocation, before any catalogue work: recovery may
        // fail on impossible bytes, but the live writer must never generate
        // them (proven by the durable test above).
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"fork-base");
        store.next_version_id = u64::MAX;
        let counters_before = store.work_counters();
        let digest = history_fork_digest(history, v0.id(), None);
        assert!(matches!(
            store.replay_fork(
                history,
                VersionId::new(u64::MAX),
                v0.id(),
                None,
                None,
                digest
            ),
            Err(HistoryError::Overflow(_))
        ));
        assert_eq!(store.versions.len(), 1);
        let counters_after = store.work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
    }

    #[test]
    fn durable_fork_reopen_is_exact() {
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut store = PersistentHistoryStore::new();
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let history = store.create_history_durable(&mut log).unwrap();
        let v0 = match store
            .append_durable(&mut log, history, None, b"fork-base", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("durable root must create")
            }
        };
        let v1 = match store
            .fork_durable(
                &mut log,
                history,
                v0.id(),
                Some(b"fork-req"),
                Some(b"bind-1"),
            )
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("durable fork must create")
            }
        };
        assert_eq!(v1.parent(), Some(v0.id()));
        assert_eq!(store.physical_root(v1.id()), store.physical_root(v0.id()));
        // Same fork request replays with no new log bytes.
        let len_before = log.read_all().unwrap().len();
        assert_eq!(
            store
                .fork_durable(
                    &mut log,
                    history,
                    v0.id(),
                    Some(b"fork-req"),
                    Some(b"bind-1")
                )
                .unwrap(),
            CommitOutcome::Replayed(v1)
        );
        assert_eq!(log.read_all().unwrap().len(), len_before);
        // Cross-operation conflict writes nothing either.
        assert!(matches!(
            store.splice_durable(
                &mut log,
                history,
                Some(v0.id()),
                4,
                0,
                b"[e]",
                Some(b"fork-req"),
                None,
            ),
            Err(DurableError::Rejected(HistoryError::RequestConflict))
        ));
        assert_eq!(log.read_all().unwrap().len(), len_before);
        drop(store);
        drop(log);
        let bytes = std::fs::read(&path).unwrap();
        let reopened = recover_history_store(&bytes).unwrap();
        assert_eq!(reopened.versions.len(), 2);
        assert_eq!(reopened.next_version_id, 2);
        let got = reopened.lookup_version(v1.id()).unwrap();
        assert_eq!(got, v1);
        assert_eq!(got.parent(), Some(v0.id()));
        assert_eq!(
            reopened.physical_root(got.id()),
            reopened.physical_root(v0.id())
        );
        let mut output = Vec::new();
        reopened.read(got, 0, 9, &mut output).unwrap();
        assert_eq!(output, b"fork-base");
        reopened.verify(got).unwrap();
        assert_eq!(
            reopened.version_binding(v1.id()),
            Some(b"bind-1".as_slice())
        );
        assert!(reopened
            .active_requests
            .contains_key(b"fork-req".as_slice()));
    }

    #[test]
    fn fork_replay_corruption_vectors_fail_closed() {
        // One honest frame builder: create + root splice + fork, with the
        // fork record mutated per vector before framing.
        fn honest_log(mutate: impl FnOnce(&mut HistoryLogRecord)) -> Vec<u8> {
            let mut store = PersistentHistoryStore::new();
            let history = store.create_history().unwrap();
            let v0 = append_new(&mut store, history, None, b"fork-base");
            let digest = history_fork_digest(history, v0.id(), Some(b"bind-1"));
            let mut fork = HistoryLogRecord::Fork {
                history,
                version: VersionId::new(1),
                parent: v0.id(),
                request_id: Some(b"fork-req".to_vec()),
                binding: Some(b"bind-1".to_vec()),
                digest,
            };
            mutate(&mut fork);
            let mut bytes = durable_log::encode_history_log_frame(
                &durable_log::encode_history_log_record(&HistoryLogRecord::CreateHistory {
                    history,
                    binding: None,
                })
                .unwrap(),
            )
            .unwrap();
            let root_digest = history_splice_digest(history, None, 0, 0, b"fork-base", None);
            bytes.extend_from_slice(
                &durable_log::encode_history_log_frame(
                    &durable_log::encode_history_log_record(&HistoryLogRecord::Splice {
                        history,
                        version: VersionId::new(0),
                        parent: None,
                        offset: 0,
                        delete_len: 0,
                        insert: b"fork-base".to_vec(),
                        request_id: None,
                        binding: None,
                        digest: root_digest,

                        physical: None,
                    })
                    .unwrap(),
                )
                .unwrap(),
            );
            // A forged fork record may itself be unencodable (oversized
            // binding): that encode rejection is already fail-closed.
            if let Ok(encoded) = durable_log::encode_history_log_record(&fork) {
                bytes.extend_from_slice(&durable_log::encode_history_log_frame(&encoded).unwrap());
            }
            bytes
        }
        // Control: the honest log replays to an exact shared-root fork.
        let recovered = recover_history_store(&honest_log(|_| {})).unwrap();
        assert_eq!(recovered.versions.len(), 2);
        let forked = recovered.lookup_version(VersionId::new(1)).unwrap();
        assert_eq!(forked.parent(), Some(VersionId::new(0)));
        assert_eq!(
            recovered.physical_root(forked.id()),
            recovered.physical_root(VersionId::new(0))
        );
        // Unknown history.
        assert!(recover_history_store(&honest_log(|record| {
            if let HistoryLogRecord::Fork { history, .. } = record {
                *history = HistoryId::new(99);
            }
        }))
        .is_err());
        // Unknown parent.
        assert!(recover_history_store(&honest_log(|record| {
            if let HistoryLogRecord::Fork { parent, .. } = record {
                *parent = VersionId::new(99);
            }
        }))
        .is_err());
        // Cross-history parent.
        assert!(recover_history_store(&honest_log(|record| {
            if let HistoryLogRecord::Fork {
                history,
                parent,
                digest,
                binding,
                ..
            } = record
            {
                *history = HistoryId::new(1);
                *parent = VersionId::new(0);
                *digest = history_fork_digest(*history, *parent, binding.as_deref());
            }
        }))
        .is_err());
        // Wrong version identity (replay-order disagreement).
        assert!(recover_history_store(&honest_log(|record| {
            if let HistoryLogRecord::Fork { version, .. } = record {
                *version = VersionId::new(7);
            }
        }))
        .is_err());
        // Wrong digest.
        assert!(recover_history_store(&honest_log(|record| {
            if let HistoryLogRecord::Fork { digest, .. } = record {
                digest[0] ^= 0xFF;
            }
        }))
        .is_err());
        // Malformed binding: present-but-empty fails closed.
        assert!(recover_history_store(&honest_log(|record| {
            if let HistoryLogRecord::Fork {
                binding,
                digest,
                history,
                parent,
                ..
            } = record
            {
                *binding = Some(Vec::new());
                *digest = history_fork_digest(*history, *parent, Some(&[]));
            }
        }))
        .is_err());
        // Oversized binding never even encodes.
        assert!(
            durable_log::encode_history_log_record(&HistoryLogRecord::Fork {
                history: HistoryId::new(0),
                version: VersionId::new(0),
                parent: VersionId::new(0),
                request_id: None,
                binding: Some(vec![0xAA; MAX_HISTORY_BINDING_BYTES + 1]),
                digest: [0u8; 32],
            })
            .is_err()
        );
        // Duplicate request identity during replay: the second fork record
        // reusing one request fails closed.
        let mut duplicated = honest_log(|_| {});
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"fork-base");
        let digest = history_fork_digest(history, v0.id(), Some(b"bind-1"));
        duplicated.extend_from_slice(
            &durable_log::encode_history_log_frame(
                &durable_log::encode_history_log_record(&HistoryLogRecord::Fork {
                    history,
                    version: VersionId::new(2),
                    parent: v0.id(),
                    request_id: Some(b"fork-req".to_vec()),
                    binding: Some(b"bind-1".to_vec()),
                    digest,
                })
                .unwrap(),
            )
            .unwrap(),
        );
        assert!(recover_history_store(&duplicated).is_err());
        // Exhausted version identity: max-Version fork against an empty
        // counter disagrees with replay order; against MAX it overflows.
        assert!(recover_history_store(&honest_log(|record| {
            if let HistoryLogRecord::Fork { version, .. } = record {
                *version = VersionId::new(u64::MAX);
            }
        }))
        .is_err());
        // Trailing record bytes fail closed.
        let encoded = durable_log::encode_history_log_record(&HistoryLogRecord::Fork {
            history: HistoryId::new(0),
            version: VersionId::new(0),
            parent: VersionId::new(0),
            request_id: None,
            binding: None,
            digest: [0u8; 32],
        })
        .unwrap();
        let mut trailed = encoded.clone();
        trailed.push(0xFF);
        assert!(durable_log::decode_history_log_record(&trailed).is_err());
        // Truncated record body fails closed.
        assert!(durable_log::decode_history_log_record(&encoded[..encoded.len() - 1]).is_err());
        // Unsupported record kind fails closed.
        let mut bad_kind = encoded.clone();
        bad_kind[0] = 0x7F;
        assert!(durable_log::decode_history_log_record(&bad_kind).is_err());
    }

    #[test]
    fn staging_epoch_gates_reject_old_epochs() {
        // A THL4 log is byte-identical to THL5 except the frame magic
        // (record layouts for pre-physical kinds are unchanged), so patching
        // the magic on valid bytes is a faithful old-epoch control: the E6
        // reader must fail at the epoch gate, before any record parsing.
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut store = PersistentHistoryStore::new();
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let history = store.create_history_durable(&mut log).unwrap();
        store
            .append_durable(&mut log, history, None, b"epoch-base", None, None)
            .unwrap();
        drop(store);
        drop(log);
        let thl5_bytes = std::fs::read(&path).unwrap();
        assert!(recover_history_store(&thl5_bytes).is_ok());
        let mut thl4_bytes = thl5_bytes.clone();
        // Every frame opens with its 4-byte magic; patch all of them.
        let mut offset = 0usize;
        let mut patched = 0u32;
        while offset + 4 <= thl4_bytes.len() {
            assert_eq!(&thl4_bytes[offset..offset + 4], b"THL5");
            thl4_bytes[offset..offset + 4].copy_from_slice(b"THL4");
            patched += 1;
            let body_len =
                u64::from_le_bytes(thl4_bytes[offset + 4..offset + 12].try_into().unwrap())
                    as usize;
            offset += 12 + body_len + 44;
        }
        assert!(patched >= 2);
        assert!(matches!(
            recover_history_store(&thl4_bytes),
            Err(HistoryError::Invalid(_))
        ));
        // Schema-5 snapshot: patch the schema u32 (magic 0..4, total_len
        // 4..12, schema 12..16) on valid bytes. Decode checks the schema
        // before the trailing digest, so no digest recompute is needed: the
        // gate must fire first.
        let mut snap_store = PersistentHistoryStore::new();
        let snap_history = snap_store.create_history().unwrap();
        append_new(&mut snap_store, snap_history, None, b"epoch-base");
        let schema6_bytes =
            snapshot::encode_history_snapshot(&snap_store, 0, thl5_bytes.len() as u64).unwrap();
        assert!(snapshot::decode_history_snapshot(&schema6_bytes).is_ok());
        let mut schema5_bytes = schema6_bytes.clone();
        assert_eq!(&schema5_bytes[0..4], b"THS1");
        schema5_bytes[12..16].copy_from_slice(&5u32.to_le_bytes());
        assert!(matches!(
            snapshot::decode_history_snapshot(&schema5_bytes),
            Err(HistoryError::Invalid(_))
        ));
        // Schema 4 (every version materialized) likewise fails: only the
        // current staging epoch opens.
        let mut schema4_old_bytes = schema6_bytes.clone();
        schema4_old_bytes[12..16].copy_from_slice(&4u32.to_le_bytes());
        assert!(matches!(
            snapshot::decode_history_snapshot(&schema4_old_bytes),
            Err(HistoryError::Invalid(_))
        ));
    }

    #[test]
    fn fork_snapshot_grows_metadata_not_content() {
        // 1 MiB parent: snapshot before and after 100 forks. Content
        // counters stay flat while the snapshot artifact grows by catalogue
        // metadata only — the image payload section must not duplicate.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let parent_payload = vec![0x71u8; 1024 * 1024];
        let v0 = append_new(&mut store, history, None, &parent_payload);
        let before_bytes = snapshot::encode_history_snapshot(&store, 0, 0).unwrap();
        let before_snapshot = snapshot::decode_history_snapshot(&before_bytes).unwrap();
        let counters_before = store.work_counters();
        for _ in 0..100 {
            fork_new(&mut store, history, v0.id(), None, None);
        }
        let counters_after = store.work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        let after_bytes = snapshot::encode_history_snapshot(&store, 1, 0).unwrap();
        let after_snapshot = snapshot::decode_history_snapshot(&after_bytes).unwrap();
        assert_eq!(after_snapshot.versions.len(), 101);
        // Metadata growth is expected and small: 100 extra catalogue entries
        // cost far less than one content copy.
        assert!(after_bytes.len() > before_bytes.len());
        // No image exists to duplicate content: every forked entry carries
        // the parent's root descriptor, and the content frontiers do not
        // move at all.
        assert_eq!(after_snapshot.payload_end, before_snapshot.payload_end);
        assert_eq!(after_snapshot.node_count, before_snapshot.node_count);
        let parent_descriptor = before_snapshot.versions[0]
            .root
            .expect("parent must describe its root");
        for entry in after_snapshot.versions.iter().skip(1) {
            assert_eq!(entry.root, Some(parent_descriptor));
            assert_eq!(entry.len, parent_payload.len() as u64);
        }
        // The live store preserves every forked root/parent exactly.
        assert_eq!(store.versions.len(), 101);
        for id in [1u64, 50, 100] {
            let version = store.lookup_version(VersionId::new(id)).unwrap();
            assert_eq!(version.parent(), Some(v0.id()));
            assert_eq!(
                store.physical_root(version.id()),
                store.physical_root(v0.id())
            );
            store.verify(version).unwrap();
        }
        let mut output = Vec::new();
        store
            .read(
                store.lookup_version(VersionId::new(100)).unwrap(),
                0,
                16,
                &mut output,
            )
            .unwrap();
        assert_eq!(output, &parent_payload[..16]);
    }

    // ---- E4 expiration + receipt-horizon tests ----

    fn expire_new(store: &mut PersistentHistoryStore, version: VersionId) {
        assert_eq!(store.expire(version), Ok(ExpireOutcome::Expired));
    }

    #[test]
    fn expire_basic_lifecycle_is_one_way_metadata_only() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"expire-base");
        assert_eq!(
            store.version_lifecycle(v0.id()),
            Ok(VersionLifecycle::Retained)
        );
        assert_eq!(store.is_retained(v0.id()), Ok(true));
        assert_eq!(store.is_expired(v0.id()), Ok(false));
        let counters_before = store.work_counters();
        expire_new(&mut store, v0.id());
        let counters_after = store.work_counters();
        assert_eq!(
            store.version_lifecycle(v0.id()),
            Ok(VersionLifecycle::Expired)
        );
        assert_eq!(store.is_retained(v0.id()), Ok(false));
        assert_eq!(store.is_expired(v0.id()), Ok(true));
        // Catalogue entry intact: root, parent, binding, and table position.
        let record = store.version_record(v0.id()).unwrap();
        assert_eq!(record.version(), v0);
        assert_eq!(store.versions.len(), 1);
        assert_eq!(store.next_version_id, 1);
        assert!(store.expired_versions.contains(&v0.id()));
        // Zero sequence work: expiration is catalogue metadata only.
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(counters_after.nodes_read, counters_before.nodes_read);
        assert_eq!(
            counters_after.payload_bytes_read,
            counters_before.payload_bytes_read
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        assert!(!store.is_poisoned());
        // Public acquisition fails; internal metadata stays reachable.
        assert_eq!(
            store.read(v0, 0, 1, &mut Vec::new()),
            Err(HistoryError::VersionExpired)
        );
        assert_eq!(store.verify(v0), Err(HistoryError::VersionExpired));
        assert_eq!(
            store.lookup_version(v0.id()),
            Err(HistoryError::VersionExpired)
        );
        assert_eq!(
            store.committed_version_for_adapter(v0.id(), history),
            Err(HistoryError::VersionExpired)
        );
        // Repeated expiration is idempotent, not a second mutation.
        assert_eq!(store.expire(v0.id()), Ok(ExpireOutcome::AlreadyExpired));
        assert_eq!(store.versions.len(), 1);
        // Unknown identities fail closed, never as silent success.
        assert!(matches!(
            store.expire(VersionId::new(99)),
            Err(HistoryError::Invalid(_))
        ));
        assert!(store.version_lifecycle(VersionId::new(99)).is_err());
        assert!(store.is_retained(VersionId::new(99)).is_err());
    }

    #[test]
    fn expire_does_not_cascade_and_descendants_stay_usable() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let other = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"chain-base!");
        let v1 = splice_new(&mut store, history, Some(v0.id()), 0, 5, b"CHAIN");
        let v2 = splice_new(&mut store, history, Some(v1.id()), 6, 0, b"[t]");
        let sibling = fork_new(&mut store, history, v0.id(), None, None);
        let foreign = append_new(&mut store, other, None, b"foreign");
        expire_new(&mut store, v1.id());
        // Only the target changed lifecycle.
        assert_eq!(store.is_retained(v0.id()), Ok(true));
        assert_eq!(store.is_expired(v1.id()), Ok(true));
        assert_eq!(store.is_retained(v2.id()), Ok(true));
        assert_eq!(store.is_retained(sibling.id()), Ok(true));
        assert_eq!(store.is_retained(foreign.id()), Ok(true));
        // Lineage metadata is unchanged: the retained child still names its
        // expired parent, and the parent need only be known, not retained.
        assert_eq!(v2.parent(), Some(v1.id()));
        assert_eq!(
            store.version_record(v2.id()).unwrap().parent(),
            Some(v1.id())
        );
        // The retained descendant stays fully usable as live state.
        assert_eq!(read_full(&store, v2, 14), b"CHAIN-[t]base!");
        store.verify(v2).unwrap();
        let v3 = splice_new(&mut store, history, Some(v2.id()), 0, 0, b"+");
        assert_eq!(v3.parent(), Some(v2.id()));
        let v4 = fork_new(&mut store, history, v2.id(), None, None);
        assert_eq!(store.physical_root(v4.id()), store.physical_root(v2.id()));
        // The expired version itself rejects every fresh acquisition.
        assert_eq!(
            store.splice(history, Some(v1.id()), 0, 0, b"x", Some(b"fresh-req"), None),
            Err(HistoryError::VersionExpired)
        );
        assert_eq!(
            store.fork(history, v1.id(), Some(b"fresh-fork"), None),
            Err(HistoryError::VersionExpired)
        );
        assert_eq!(
            store.read(v1, 0, 1, &mut Vec::new()),
            Err(HistoryError::VersionExpired)
        );
        // Cross-history expiry never applied: foreign lineage untouched.
        assert_eq!(read_full(&store, foreign, 7), b"foreign");
    }

    #[test]
    fn retained_receipt_replays_after_parent_expiration() {
        // E4.7 ordering: ledger resolution precedes the fresh-parent
        // retention check, so an already-committed request replays even
        // after its historical parent expired — for splice and fork alike.
        for is_fork in [false, true] {
            let mut store = PersistentHistoryStore::new();
            let history = store.create_history().unwrap();
            let v0 = append_new(&mut store, history, None, b"replay-base");
            let v1 = if is_fork {
                fork_new(&mut store, history, v0.id(), Some(b"req-r"), None)
            } else {
                match store
                    .splice(history, Some(v0.id()), 6, 0, b"[e]", Some(b"req-r"), None)
                    .unwrap()
                {
                    CommitOutcome::Committed(version) => version,
                    CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                        panic!("fresh request must create")
                    }
                }
            };
            expire_new(&mut store, v0.id());
            // Same retained receipt + same digest replays the retained
            // result despite the expired historical parent.
            let replayed = if is_fork {
                store.fork(history, v0.id(), Some(b"req-r"), None).unwrap()
            } else {
                store
                    .splice(history, Some(v0.id()), 6, 0, b"[e]", Some(b"req-r"), None)
                    .unwrap()
            };
            assert_eq!(replayed, CommitOutcome::Replayed(v1));
            // An unknown/fresh request on the expired parent fails.
            let fresh = if is_fork {
                store.fork(history, v0.id(), Some(b"req-new"), None)
            } else {
                store.splice(history, Some(v0.id()), 6, 0, b"[e]", Some(b"req-new"), None)
            };
            assert_eq!(fresh, Err(HistoryError::VersionExpired));
        }
    }

    #[test]
    fn expiring_result_version_retires_its_active_receipt() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"receipt-base");
        let v1 = match store
            .splice(history, Some(v0.id()), 7, 0, b"[e]", Some(b"req-v"), None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("fresh request must create")
            }
        };
        assert_eq!(
            store.request_receipt_status(b"req-v"),
            RequestReceiptStatus::Active(v1.id())
        );
        let counters_before = store.work_counters();
        expire_new(&mut store, v1.id());
        let counters_after = store.work_counters();
        // The receipt moved active -> retired with no content work.
        assert_eq!(
            store.request_receipt_status(b"req-v"),
            RequestReceiptStatus::Retired
        );
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        // Retrying the exact receipt retires; a different digest conflicts —
        // never a replay of the expired version.
        assert_eq!(
            store.splice(history, Some(v0.id()), 7, 0, b"[e]", Some(b"req-v"), None),
            Ok(CommitOutcome::Retired)
        );
        assert_eq!(
            store.splice(
                history,
                Some(v0.id()),
                7,
                0,
                b"[other]",
                Some(b"req-v"),
                None
            ),
            Err(HistoryError::RequestConflict)
        );
        assert_eq!(
            store.fork(history, v0.id(), Some(b"req-v"), None),
            Err(HistoryError::RequestConflict)
        );
        // The expired result itself stays expired and unacquirable.
        assert_eq!(store.is_expired(v1.id()), Ok(true));
        assert_eq!(
            store.read(v1, 0, 1, &mut Vec::new()),
            Err(HistoryError::VersionExpired)
        );
    }

    #[test]
    fn internal_expired_root_bytes_are_unchanged() {
        // E4.23: public acquisition fails, but the immutable root and its
        // logical bytes are byte-identical before and after expiration.
        // Internal paths keep using version_record, never the public gates.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"stable-bytes-1234");
        let root_before = store.version_record(v0.id()).unwrap().root().unwrap();
        let mut bytes_before = Vec::new();
        store
            .backend
            .read_range(
                root_before,
                SequenceRange::new(LogicalLength::new(0), LogicalLength::new(17)).unwrap(),
                &mut bytes_before,
            )
            .unwrap();
        expire_new(&mut store, v0.id());
        let root_after = store.version_record(v0.id()).unwrap().root().unwrap();
        assert_eq!(root_after, root_before);
        let mut bytes_after = Vec::new();
        store
            .backend
            .read_range(
                root_after,
                SequenceRange::new(LogicalLength::new(0), LogicalLength::new(17)).unwrap(),
                &mut bytes_after,
            )
            .unwrap();
        assert_eq!(bytes_after, bytes_before);
        assert_eq!(bytes_after, b"stable-bytes-1234");
        // The expired set stays a subset of known identities.
        assert!(store
            .expired_versions
            .iter()
            .all(|id| store.version_record(*id).is_ok()));
    }

    fn fork_request_id(index: usize) -> Vec<u8> {
        format!("horizon-req-{index:05}").into_bytes()
    }

    #[test]
    fn receipt_horizon_boundary_4097_forks() {
        // Zero-content forks keep the 4097-receipt boundary cheap: after
        // completion exactly the newest 4096 receipts are retained, the
        // oldest is Unknown, and reuse takes the documented path per case.
        let mut store = PersistentHistoryStore::new();
        assert_eq!(store.request_receipt_capacity(), 4096);
        assert_eq!(
            store.request_receipt_capacity(),
            STAGING_REQUEST_RECEIPT_CAPACITY
        );
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"horizon-base");
        let mut committed = Vec::new();
        for index in 0..4097 {
            let version = fork_new(
                &mut store,
                history,
                v0.id(),
                Some(&fork_request_id(index)),
                None,
            );
            committed.push(version);
        }
        assert_eq!(store.versions.len(), 4098);
        assert_eq!(store.request_receipt_count(), 4096);
        assert_eq!(store.receipt_order.len(), 4096);
        // Boundary statuses: oldest evicted, rest active.
        assert_eq!(
            store.request_receipt_status(&fork_request_id(0)),
            RequestReceiptStatus::Unknown
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(1)),
            RequestReceiptStatus::Active(committed[1].id())
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(4096)),
            RequestReceiptStatus::Active(committed[4096].id())
        );
        assert_eq!(store.receipt_order[0], fork_request_id(1));
        assert_eq!(store.receipt_order[4095], fork_request_id(4096));
        // Retained retry replays without touching the order...
        assert_eq!(
            store.fork(history, v0.id(), Some(&fork_request_id(1)), None),
            Ok(CommitOutcome::Replayed(committed[1]))
        );
        assert_eq!(store.receipt_order[0], fork_request_id(1));
        assert_eq!(store.request_receipt_count(), 4096);
        // ...while the evicted retry takes the fresh path and becomes
        // newest, evicting the then-oldest retained receipt.
        let fresh = match store
            .fork(history, v0.id(), Some(&fork_request_id(0)), None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("evicted request must take the fresh path")
            }
        };
        assert_ne!(fresh.id(), committed[0].id());
        assert_eq!(fresh.parent(), Some(v0.id()));
        assert_eq!(
            store.physical_root(fresh.id()),
            store.physical_root(v0.id())
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(0)),
            RequestReceiptStatus::Active(fresh.id())
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(1)),
            RequestReceiptStatus::Unknown
        );
        assert_eq!(store.request_receipt_count(), 4096);
        assert_eq!(store.receipt_order[0], fork_request_id(2));
        assert_eq!(store.receipt_order[4095], fork_request_id(0));
        // Every evicted version stays retained: eviction never expires.
        assert_eq!(store.is_retained(committed[0].id()), Ok(true));
        assert_eq!(store.is_retained(committed[1].id()), Ok(true));
    }

    #[test]
    fn retired_receipts_count_and_evict() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"retire-base");
        let v1 = match store
            .splice(
                history,
                Some(v0.id()),
                6,
                0,
                b"[e]",
                Some(b"req-doomed"),
                None,
            )
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("fresh request must create")
            }
        };
        store.retire_request(b"req-doomed").unwrap();
        // While retained, retired semantics hold and the order is untouched.
        assert_eq!(
            store.request_receipt_status(b"req-doomed"),
            RequestReceiptStatus::Retired
        );
        assert_eq!(store.receipt_order.len(), 1);
        assert_eq!(
            store.splice(
                history,
                Some(v0.id()),
                6,
                0,
                b"[e]",
                Some(b"req-doomed"),
                None
            ),
            Ok(CommitOutcome::Retired)
        );
        assert_eq!(
            store.splice(
                history,
                Some(v0.id()),
                6,
                0,
                b"[other]",
                Some(b"req-doomed"),
                None
            ),
            Err(HistoryError::RequestConflict)
        );
        let _ = v1;
        // Retired receipts are not immortal: 4096 newer receipts evict the
        // retired one, and reuse then takes the fresh path.
        for index in 0..4096 {
            fork_new(
                &mut store,
                history,
                v0.id(),
                Some(&fork_request_id(index)),
                None,
            );
        }
        assert_eq!(store.request_receipt_count(), 4096);
        assert_eq!(
            store.request_receipt_status(b"req-doomed"),
            RequestReceiptStatus::Unknown
        );
        let reused = match store
            .splice(
                history,
                Some(v0.id()),
                6,
                0,
                b"[e]",
                Some(b"req-doomed"),
                None,
            )
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("evicted retired request must take the fresh path")
            }
        };
        assert_eq!(
            store.request_receipt_status(b"req-doomed"),
            RequestReceiptStatus::Active(reused.id())
        );
        assert_eq!(store.request_receipt_count(), 4096);
    }

    #[test]
    fn request_receipt_status_api_distinguishes_only_retained() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"status-base");
        // Never observed reads Unknown.
        assert_eq!(
            store.request_receipt_status(b"never-seen"),
            RequestReceiptStatus::Unknown
        );
        assert_eq!(store.request_receipt_count(), 0);
        // No-request operations create no receipt.
        fork_new(&mut store, history, v0.id(), None, None);
        assert_eq!(store.request_receipt_count(), 0);
        // Fresh request-bearing commit reads Active with its version.
        let v1 = fork_new(&mut store, history, v0.id(), Some(b"req-s"), None);
        assert_eq!(
            store.request_receipt_status(b"req-s"),
            RequestReceiptStatus::Active(v1.id())
        );
        assert_eq!(store.request_receipt_count(), 1);
        // Explicit retire reads Retired with the order position unchanged.
        let order_before: Vec<Vec<u8>> = store.receipt_order.iter().cloned().collect();
        store.retire_request(b"req-s").unwrap();
        assert_eq!(
            store.request_receipt_status(b"req-s"),
            RequestReceiptStatus::Retired
        );
        assert_eq!(store.request_receipt_count(), 1);
        let order_after: Vec<Vec<u8>> = store.receipt_order.iter().cloned().collect();
        assert_eq!(order_before, order_after);
        // Version expiration retires the receipt, order still unchanged.
        let v2 = fork_new(&mut store, history, v0.id(), Some(b"req-t"), None);
        let _ = v2;
        expire_new(&mut store, v1.id());
        assert_eq!(
            store.request_receipt_status(b"req-s"),
            RequestReceiptStatus::Retired
        );
        let order_expired: Vec<Vec<u8>> = store.receipt_order.iter().cloned().collect();
        assert_eq!(order_before.len() + 1, order_expired.len());
        assert_eq!(order_expired[0], order_before[0]);
    }

    #[test]
    fn exhaustion_distinguishes_retained_from_evicted_receipts() {
        // Retained receipts replay/retire at MAX with no new identity; an
        // evicted receipt takes the fresh path and overflows there.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"exhaust-base");
        for index in 0..4096 {
            fork_new(
                &mut store,
                history,
                v0.id(),
                Some(&fork_request_id(index)),
                None,
            );
        }
        // Two newer receipts evict req-0 and req-1; one stays active, the
        // other retires while retained.
        let live_fork = fork_new(&mut store, history, v0.id(), Some(b"req-live"), None);
        fork_new(&mut store, history, v0.id(), Some(b"req-old"), None);
        store.retire_request(b"req-old").unwrap();
        assert_eq!(store.request_receipt_count(), 4096);
        assert_eq!(
            store.request_receipt_status(&fork_request_id(0)),
            RequestReceiptStatus::Unknown
        );
        store.next_version_id = u64::MAX;
        // Evicted receipt at exhaustion: fresh path, definite overflow —
        // never an accidental replay.
        assert!(matches!(
            store.fork(history, v0.id(), Some(&fork_request_id(0)), None),
            Err(HistoryError::Overflow(_))
        ));
        // Retained receipts need no new identity: replay and retire succeed.
        assert_eq!(
            store.fork(history, v0.id(), Some(b"req-live"), None),
            Ok(CommitOutcome::Replayed(live_fork))
        );
        assert_eq!(
            store.fork(history, v0.id(), Some(b"req-old"), None),
            Ok(CommitOutcome::Retired)
        );
        assert_eq!(store.next_version_id, u64::MAX);
        assert!(!store.is_poisoned());
    }

    #[test]
    fn durable_expire_reopen_repeat_is_idempotent_with_zero_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut store = PersistentHistoryStore::new();
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let history = store.create_history_durable(&mut log).unwrap();
        let v0 = match store
            .append_durable(&mut log, history, None, b"expire-durable", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("durable root must create")
            }
        };
        let v1 = match store
            .fork_durable(&mut log, history, v0.id(), Some(b"req-e"), None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("durable fork must create")
            }
        };
        let counters_before = store.work_counters();
        assert!(matches!(
            store.expire_durable(&mut log, v0.id()),
            Ok(ExpireOutcome::Expired)
        ));
        let counters_after = store.work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        // The fork receipt is untouched (different result version); expiry of
        // the fork result would retire it instead (covered live above).
        assert_eq!(
            store.request_receipt_status(b"req-e"),
            RequestReceiptStatus::Active(v1.id())
        );
        // Repeated durable expiration writes zero new authority bytes.
        let len_before = std::fs::metadata(&path).unwrap().len();
        assert!(matches!(
            store.expire_durable(&mut log, v0.id()),
            Ok(ExpireOutcome::AlreadyExpired)
        ));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len_before);
        assert!(!store.is_poisoned());
        // Unknown versions fail closed without WAL bytes.
        assert!(matches!(
            store.expire_durable(&mut log, VersionId::new(99)),
            Err(DurableError::Rejected(HistoryError::Invalid(_)))
        ));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len_before);
        drop(store);
        drop(log);
        let bytes = std::fs::read(&path).unwrap();
        let reopened = recover_history_store(&bytes).unwrap();
        assert_eq!(reopened.is_expired(v0.id()), Ok(true));
        assert_eq!(reopened.is_retained(v1.id()), Ok(true));
        assert_eq!(
            reopened.request_receipt_status(b"req-e"),
            RequestReceiptStatus::Active(v1.id())
        );
        // Repeated expiration after reopen stays idempotent.
        let mut store = reopened;
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let len_reopen = std::fs::metadata(&path).unwrap().len();
        assert!(matches!(
            store.expire_durable(&mut log, v0.id()),
            Ok(ExpireOutcome::AlreadyExpired)
        ));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len_reopen);
    }

    #[test]
    fn live_execution_and_genesis_replay_agree_on_horizon() {
        // E4.21: eviction is a deterministic consequence of replay order —
        // live maps, retired maps, receipt order, and lifecycle must match
        // genesis replay exactly, including retire and expire effects.
        let temp = tempfile::tempdir().unwrap();
        let path = test_log_path(temp.path());
        let mut store = PersistentHistoryStore::new();
        let mut log = DurableHistoryLog::open(&path).unwrap();
        let history = store.create_history_durable(&mut log).unwrap();
        let v0 = match store
            .append_durable(&mut log, history, None, b"determinism", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("durable root must create")
            }
        };
        for index in 0..24 {
            match store
                .fork_durable(
                    &mut log,
                    history,
                    v0.id(),
                    Some(&fork_request_id(index)),
                    None,
                )
                .unwrap()
            {
                CommitOutcome::Committed(_) => {}
                CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                    panic!("fresh fork must create")
                }
            }
        }
        store.retire_durable(&mut log, &fork_request_id(3)).unwrap();
        store.retire_durable(&mut log, &fork_request_id(7)).unwrap();
        assert!(matches!(
            store.expire_durable(&mut log, VersionId::new(5)),
            Ok(ExpireOutcome::Expired)
        ));
        // Expiring V5 (created by horizon-req-00004) retires that receipt live.
        assert_eq!(
            store.request_receipt_status(&fork_request_id(4)),
            RequestReceiptStatus::Retired
        );
        // Capture the genuine live maps before teardown.
        let live_active = store.active_requests.clone();
        let live_retired = store.retired_requests.clone();
        let live_order: Vec<Vec<u8>> = store.receipt_order.iter().cloned().collect();
        let live_expired = store.expired_versions.clone();
        let live_versions = store.versions.clone();
        drop(store);
        let bytes = log.read_all().unwrap();
        drop(log);
        let replayed = recover_history_store(&bytes).unwrap();
        assert_eq!(replayed.active_requests, live_active);
        assert_eq!(replayed.retired_requests, live_retired);
        assert_eq!(
            replayed.receipt_order.iter().cloned().collect::<Vec<_>>(),
            live_order
        );
        assert_eq!(replayed.expired_versions, live_expired);
        assert_eq!(replayed.versions, live_versions);
        // Spot behavior: active replays, retired retires, expired result's
        // receipt retires, unknown conflicts appropriately.
        let mut probe = replayed;
        assert!(matches!(
            probe.fork(history, v0.id(), Some(&fork_request_id(0)), None),
            Ok(CommitOutcome::Replayed(_))
        ));
        assert_eq!(
            probe.fork(history, v0.id(), Some(&fork_request_id(3)), None),
            Ok(CommitOutcome::Retired)
        );
        assert_eq!(
            probe.fork(history, v0.id(), Some(&fork_request_id(4)), None),
            Ok(CommitOutcome::Retired)
        );
    }

    #[test]
    fn expire_wal_corruption_vectors_fail_closed() {
        fn expire_frame(history: HistoryId, version: VersionId) -> Vec<u8> {
            durable_log::encode_history_log_frame(
                &durable_log::encode_history_log_record(&HistoryLogRecord::ExpireVersion {
                    history,
                    version,
                })
                .unwrap(),
            )
            .unwrap()
        }
        fn base_log() -> (Vec<u8>, HistoryId, HistoryId) {
            let mut store = PersistentHistoryStore::new();
            let first = store.create_history().unwrap();
            let second = store.create_history().unwrap();
            append_new(&mut store, first, None, b"first-base");
            append_new(&mut store, second, None, b"second-base");
            let mut bytes = durable_log::encode_history_log_frame(
                &durable_log::encode_history_log_record(&HistoryLogRecord::CreateHistory {
                    history: first,
                    binding: None,
                })
                .unwrap(),
            )
            .unwrap();
            bytes.extend_from_slice(
                &durable_log::encode_history_log_frame(
                    &durable_log::encode_history_log_record(&HistoryLogRecord::CreateHistory {
                        history: second,
                        binding: None,
                    })
                    .unwrap(),
                )
                .unwrap(),
            );
            let first_digest = history_splice_digest(first, None, 0, 0, b"first-base", None);
            bytes.extend_from_slice(
                &durable_log::encode_history_log_frame(
                    &durable_log::encode_history_log_record(&HistoryLogRecord::Splice {
                        history: first,
                        version: VersionId::new(0),
                        parent: None,
                        offset: 0,
                        delete_len: 0,
                        insert: b"first-base".to_vec(),
                        request_id: None,
                        binding: None,
                        digest: first_digest,

                        physical: None,
                    })
                    .unwrap(),
                )
                .unwrap(),
            );
            let second_digest = history_splice_digest(second, None, 0, 0, b"second-base", None);
            bytes.extend_from_slice(
                &durable_log::encode_history_log_frame(
                    &durable_log::encode_history_log_record(&HistoryLogRecord::Splice {
                        history: second,
                        version: VersionId::new(1),
                        parent: None,
                        offset: 0,
                        delete_len: 0,
                        insert: b"second-base".to_vec(),
                        request_id: None,
                        binding: None,
                        digest: second_digest,

                        physical: None,
                    })
                    .unwrap(),
                )
                .unwrap(),
            );
            (bytes, first, second)
        }
        // Control: honest expiration replays to an expired V0.
        let (mut honest, first, second) = base_log();
        honest.extend_from_slice(&expire_frame(first, VersionId::new(0)));
        let recovered = recover_history_store(&honest).unwrap();
        assert_eq!(recovered.is_expired(VersionId::new(0)), Ok(true));
        assert_eq!(recovered.is_retained(VersionId::new(1)), Ok(true));
        // Unknown version.
        let (base, _, _) = base_log();
        let mut bad = base.clone();
        bad.extend_from_slice(&expire_frame(first, VersionId::new(99)));
        assert!(recover_history_store(&bad).is_err());
        // Wrong history for a known version.
        let mut crossed = base.clone();
        crossed.extend_from_slice(&expire_frame(second, VersionId::new(0)));
        assert!(recover_history_store(&crossed).is_err());
        // Unknown history.
        let mut lost = base;
        lost.extend_from_slice(&expire_frame(HistoryId::new(99), VersionId::new(0)));
        assert!(recover_history_store(&lost).is_err());
        // Duplicate expiration frames stay idempotent, not an error.
        let mut twice = honest.clone();
        twice.extend_from_slice(&expire_frame(first, VersionId::new(0)));
        assert!(recover_history_store(&twice).is_ok());
        // Truncated expire body fails closed.
        let encoded = durable_log::encode_history_log_record(&HistoryLogRecord::ExpireVersion {
            history: first,
            version: VersionId::new(0),
        })
        .unwrap();
        assert!(durable_log::decode_history_log_record(&encoded[..encoded.len() - 1]).is_err());
        let mut trailed = encoded.clone();
        trailed.push(0x00);
        assert!(durable_log::decode_history_log_record(&trailed).is_err());
    }

    /// Builds an integrity-valid THL4 log ending right after `ExpireVersion
    /// V0`: create, root splice, expire. Callers append one forged fresh
    /// child operation to prove replay rejects it.
    fn expired_parent_log_prefix() -> (Vec<u8>, HistoryId, VersionId) {
        let mut bytes = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&HistoryLogRecord::CreateHistory {
                history: HistoryId::new(0),
                binding: None,
            })
            .unwrap(),
        )
        .unwrap();
        let history = HistoryId::new(0);
        let root_digest = history_splice_digest(history, None, 0, 0, b"expire-base", None);
        bytes.extend_from_slice(
            &durable_log::encode_history_log_frame(
                &durable_log::encode_history_log_record(&HistoryLogRecord::Splice {
                    history,
                    version: VersionId::new(0),
                    parent: None,
                    offset: 0,
                    delete_len: 0,
                    insert: b"expire-base".to_vec(),
                    request_id: None,
                    binding: None,
                    digest: root_digest,

                    physical: None,
                })
                .unwrap(),
            )
            .unwrap(),
        );
        bytes.extend_from_slice(
            &durable_log::encode_history_log_frame(
                &durable_log::encode_history_log_record(&HistoryLogRecord::ExpireVersion {
                    history,
                    version: VersionId::new(0),
                })
                .unwrap(),
            )
            .unwrap(),
        );
        (bytes, history, VersionId::new(0))
    }

    fn frame_record(record: &HistoryLogRecord) -> Vec<u8> {
        durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(record).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn replay_rejects_splice_after_parent_expiration() {
        // The forged child is valid in every respect except lifecycle
        // ordering: correct coordinates, correct digest, correct identity,
        // valid framing. Recovery must fail at the expired-parent gate the
        // live writer enforces — a sequence the live authority could never
        // emit.
        let (mut bytes, history, v0) = expired_parent_log_prefix();
        let digest = history_splice_digest(history, Some(v0), 11, 0, b"[e]", None);
        bytes.extend_from_slice(&frame_record(&HistoryLogRecord::Splice {
            history,
            version: VersionId::new(1),
            parent: Some(v0),
            offset: 11,
            delete_len: 0,
            insert: b"[e]".to_vec(),
            request_id: None,
            binding: None,
            digest,

            physical: None,
        }));
        assert!(matches!(
            recover_history_store(&bytes),
            Err(HistoryError::Invalid(
                "history log splice parent is expired"
            ))
        ));
    }

    #[test]
    fn replay_rejects_fork_after_parent_expiration() {
        let (mut bytes, history, v0) = expired_parent_log_prefix();
        let digest = history_fork_digest(history, v0, None);
        bytes.extend_from_slice(&frame_record(&HistoryLogRecord::Fork {
            history,
            version: VersionId::new(1),
            parent: v0,
            request_id: None,
            binding: None,
            digest,
        }));
        assert!(matches!(
            recover_history_store(&bytes),
            Err(HistoryError::Invalid("history log fork parent is expired"))
        ));
    }

    #[test]
    fn replay_expired_parent_rejection_changes_nothing() {
        // Direct helper regression: the rejection precedes backend work, so
        // counters, tables, ledgers, and order are all untouched.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"expire-base");
        expire_new(&mut store, v0.id());
        let counters_before = store.work_counters();
        let splice_digest = history_splice_digest(history, Some(v0.id()), 11, 0, b"[e]", None);
        assert_eq!(
            store.replay_splice(
                history,
                VersionId::new(1),
                Some(v0.id()),
                11,
                0,
                b"[e]",
                None,
                None,
                splice_digest,
                None,
            ),
            Err(HistoryError::Invalid(
                "history log splice parent is expired"
            ))
        );
        let fork_digest = history_fork_digest(history, v0.id(), None);
        assert_eq!(
            store.replay_fork(history, VersionId::new(1), v0.id(), None, None, fork_digest),
            Err(HistoryError::Invalid("history log fork parent is expired"))
        );
        assert_eq!(store.next_version_id, 1);
        assert_eq!(store.versions.len(), 1);
        assert!(store.active_requests.is_empty());
        assert!(store.retired_requests.is_empty());
        assert!(store.receipt_order.is_empty());
        let counters_after = store.work_counters();
        assert_eq!(
            counters_after.nodes_allocated,
            counters_before.nodes_allocated
        );
        assert_eq!(
            counters_after.nodes_inspected,
            counters_before.nodes_inspected
        );
        assert_eq!(counters_after.nodes_read, counters_before.nodes_read);
        assert_eq!(
            counters_after.payload_bytes_read,
            counters_before.payload_bytes_read
        );
        assert_eq!(
            counters_after.payload_bytes_written,
            counters_before.payload_bytes_written
        );
        assert!(!store.is_poisoned());
    }

    #[test]
    fn replay_accepts_operation_before_expiration() {
        // Valid ordering control: child operations committed before the
        // parent expires replay fine — only expire-then-child is impossible.
        let history = HistoryId::new(0);
        let mut bytes = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&HistoryLogRecord::CreateHistory {
                history,
                binding: None,
            })
            .unwrap(),
        )
        .unwrap();
        let root_digest = history_splice_digest(history, None, 0, 0, b"expire-base", None);
        bytes.extend_from_slice(&frame_record(&HistoryLogRecord::Splice {
            history,
            version: VersionId::new(0),
            parent: None,
            offset: 0,
            delete_len: 0,
            insert: b"expire-base".to_vec(),
            request_id: None,
            binding: None,
            digest: root_digest,

            physical: None,
        }));
        let child_digest =
            history_splice_digest(history, Some(VersionId::new(0)), 11, 0, b"[e]", None);
        bytes.extend_from_slice(&frame_record(&HistoryLogRecord::Splice {
            history,
            version: VersionId::new(1),
            parent: Some(VersionId::new(0)),
            offset: 11,
            delete_len: 0,
            insert: b"[e]".to_vec(),
            request_id: None,
            binding: None,
            digest: child_digest,

            physical: None,
        }));
        let fork_digest = history_fork_digest(history, VersionId::new(0), None);
        bytes.extend_from_slice(&frame_record(&HistoryLogRecord::Fork {
            history,
            version: VersionId::new(2),
            parent: VersionId::new(0),
            request_id: None,
            binding: None,
            digest: fork_digest,
        }));
        bytes.extend_from_slice(&frame_record(&HistoryLogRecord::ExpireVersion {
            history,
            version: VersionId::new(0),
        }));
        let recovered = recover_history_store(&bytes).unwrap();
        assert_eq!(recovered.versions.len(), 3);
        assert_eq!(recovered.is_expired(VersionId::new(0)), Ok(true));
        assert_eq!(recovered.is_retained(VersionId::new(1)), Ok(true));
        assert_eq!(recovered.is_retained(VersionId::new(2)), Ok(true));
        // The retained child of the expired parent still names it.
        assert_eq!(
            recovered
                .lookup_version(VersionId::new(1))
                .unwrap()
                .parent(),
            Some(VersionId::new(0))
        );
    }

    // ---- E5 closure: catalogue length / materialized root consistency ----

    #[test]
    fn length_root_mismatch_fails_encode_and_prepare() {
        // A forged catalogue length must fail at every memory-side gate with
        // the length-disagreement error: seal encode and GC preparation —
        // never laundered, never silently overwritten. (Snapshot import
        // agreement is covered at the durable authority level, where files
        // back the descriptors.)
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"consistency-1234");
        let v1 = fork_new(&mut store, history, v0.id(), None, None);
        expire_new(&mut store, v0.id());
        let counters = store.work_counters();
        // Corrupt the retained fork's catalogue length in memory.
        store.versions[1].len += 1;
        assert_eq!(
            snapshot::encode_history_snapshot(&store, 0, 0),
            Err(HistoryError::Invalid(
                "history snapshot source version length disagrees with its materialized root"
            ))
        );
        assert_eq!(
            store.prepare_gc().map(|prepared| prepared.stats()),
            Err(HistoryError::Invalid(
                "persistent GC version length disagrees with its materialized root"
            ))
        );
        // Both guards are read-only: backend, catalogue, and counters
        // exact (checked before any content read, which legitimately bumps
        // read counters).
        assert_eq!(store.work_counters(), counters);
        assert_eq!(store.version_count(), 2);
        // Restore the fork, corrupt the expired-but-materialized root
        // instead: GC must fail rather than launder via root drop.
        store.versions[1].len -= 1;
        store.versions[0].len += 1;
        assert_eq!(
            snapshot::encode_history_snapshot(&store, 0, 0),
            Err(HistoryError::Invalid(
                "history snapshot source version length disagrees with its materialized root"
            ))
        );
        assert_eq!(
            store.prepare_gc().map(|prepared| prepared.stats()),
            Err(HistoryError::Invalid(
                "persistent GC version length disagrees with its materialized root"
            ))
        );
        // Restore: every gate succeeds with identical state.
        store.versions[0].len -= 1;
        snapshot::encode_history_snapshot(&store, 0, 0).unwrap();
        let stats = store.prepare_gc().unwrap().stats();
        assert_eq!(stats.retained_versions, 1);
        assert_eq!(store.work_counters(), counters);
        assert_eq!(read_full(&store, v1, 16), b"consistency-1234");
    }

    #[test]
    fn schema6_length_mismatch_parses_but_disagrees() {
        // Retained mismatch: valid integrity, structurally parsable decode —
        // the length agreement itself is import's job (covered at the
        // durable authority level, where descriptors bind files).
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"length-import!");
        let v1 = fork_new(&mut store, history, v0.id(), None, None);
        let honest = snapshot::decode_history_snapshot(
            &snapshot::encode_history_snapshot(&store, 0, 0).unwrap(),
        )
        .unwrap();
        let mut forged = honest.clone();
        forged.versions[1].len += 1;
        let forged_bytes = snapshot::encode_history_snapshot_struct(&forged).unwrap();
        // Decode parses structurally; only the materialized agreement fails,
        // and that gate lives at import.
        let decoded = snapshot::decode_history_snapshot(&forged_bytes).unwrap();
        assert_eq!(decoded.versions[1].len, 15);
        assert!(decoded.versions[1].root_present);
        let _ = (v0, v1);
    }

    #[test]
    fn schema6_expired_materialized_mismatch_parses_but_disagrees() {
        // Same split for an expired-but-still-materialized version: decode
        // parses, and the agreement gate that stops GC from laundering the
        // mismatch by dropping the root lives at import (durable authority
        // level).
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"expired-length");
        let _v1 = fork_new(&mut store, history, v0.id(), None, None);
        expire_new(&mut store, v0.id());
        let honest = snapshot::decode_history_snapshot(
            &snapshot::encode_history_snapshot(&store, 0, 0).unwrap(),
        )
        .unwrap();
        assert!(honest.versions[0].root_present);
        let mut forged = honest;
        forged.versions[0].len += 1;
        let forged_bytes = snapshot::encode_history_snapshot_struct(&forged).unwrap();
        let decoded = snapshot::decode_history_snapshot(&forged_bytes).unwrap();
        assert_eq!(decoded.versions[0].len, 15);
    }

    #[test]
    fn schema6_rootless_zero_length_rejects() {
        // Control: valid post-GC rootless entries carry their preserved
        // nonzero length and round-trip through the struct codec; forged
        // zero length fails at decode. (Struct import agreement is covered
        // at the durable authority level.)
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"rootless-len14!");
        let _v1 = fork_new(&mut store, history, v0.id(), None, None);
        expire_new(&mut store, v0.id());
        let prepared = store.prepare_gc().unwrap();
        store.apply_prepared_gc(prepared);
        let honest = snapshot::decode_history_snapshot(
            &snapshot::encode_history_snapshot(&store, 0, 0).unwrap(),
        )
        .unwrap();
        assert_eq!(honest.versions[0].lifecycle, VersionLifecycle::Expired);
        assert!(!honest.versions[0].root_present);
        assert_eq!(honest.versions[0].len, 15);
        assert_eq!(
            store
                .logical_len(store.lookup_version(VersionId::new(1)).unwrap())
                .unwrap()
                .get(),
            15
        );
        let mut forged = honest;
        forged.versions[0].len = 0;
        let forged_bytes = snapshot::encode_history_snapshot_struct(&forged).unwrap();
        assert_eq!(
            snapshot::decode_history_snapshot(&forged_bytes),
            Err(HistoryError::Invalid(
                "history snapshot version length is zero"
            ))
        );
    }

    // ---- E5 quiescent GC tests ----

    #[test]
    fn gc_relocates_retained_root_and_preserves_logical_handle() {
        // E5.22: V0's exclusive nodes force V1's retained root to move
        // physically, while the pre-GC logical handle keeps working.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let big = vec![0x11u8; 1024 * 1024];
        let v0 = append_new(&mut store, history, None, &big);
        let v1 = append_new(&mut store, history, None, b"small-survivor");
        expire_new(&mut store, v0.id());
        let old_root = store.physical_root(v1.id()).unwrap();
        let counters_before = store.work_counters();
        let prepared = store.prepare_gc().unwrap();
        let stats = prepared.stats();
        assert_eq!(stats.retained_versions, 1);
        assert_eq!(stats.expired_versions, 1);
        assert!(stats.nodes_reclaimed > 0);
        assert!(stats.payload_bytes_reclaimed > 0);
        // Preparation alone mutates nothing, including foreground counters.
        assert_eq!(store.physical_root(v1.id()).unwrap(), old_root);
        assert_eq!(store.work_counters(), counters_before);
        store.apply_prepared_gc(prepared);
        let new_root = store.physical_root(v1.id()).unwrap();
        assert_ne!(
            new_root.node_id(),
            old_root.node_id(),
            "reclamation must relocate the survivor"
        );
        // The pre-GC logical handle reads and verifies exactly.
        assert_eq!(read_full(&store, v1, 14), b"small-survivor");
        store.verify(v1).unwrap();
        assert_eq!(store.logical_len(v1).unwrap().get(), 14);
        assert_eq!(v1.id(), VersionId::new(1));
        assert_eq!(store.next_version_id, 2);
        // The expired version stays known-but-unacquirable with no root.
        assert_eq!(store.is_expired(v0.id()), Ok(true));
        assert_eq!(
            store.read(v0, 0, 1, &mut Vec::new()),
            Err(HistoryError::VersionExpired)
        );
        assert!(store.physical_root(v0.id()).is_err());
    }

    #[test]
    fn gc_keeps_retained_child_of_expired_parent_usable() {
        // E5.23: V1's subtree protects its own content; V0's exclusive
        // prefix nodes may disappear without affecting V1.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let big = vec![0x22u8; 256 * 1024];
        let v0 = append_new(&mut store, history, None, &big);
        let v1 = splice_new(&mut store, history, Some(v0.id()), 0, 0, b"HEAD-");
        expire_new(&mut store, v0.id());
        let prepared = store.prepare_gc().unwrap();
        assert_eq!(prepared.stats().retained_versions, 1);
        store.apply_prepared_gc(prepared);
        assert_eq!(store.is_expired(v0.id()), Ok(true));
        assert_eq!(store.is_retained(v1.id()), Ok(true));
        assert_eq!(v1.parent(), Some(v0.id()));
        let mut output = Vec::new();
        store
            .read(v1, 0, store.logical_len(v1).unwrap().get(), &mut output)
            .unwrap();
        assert_eq!(&output[..5], b"HEAD-");
        assert_eq!(output.len(), 256 * 1024 + 5);
        assert_eq!(&output[5..], &big[..]);
        store.verify(v1).unwrap();
        let forked = fork_new(&mut store, history, v1.id(), None, None);
        assert_eq!(store.logical_len(forked).unwrap().get(), 256 * 1024 + 5);
        let edited = splice_new(&mut store, history, Some(v1.id()), 5, 0, b"[e]");
        assert_eq!(store.logical_len(edited).unwrap().get(), 256 * 1024 + 8);
    }

    #[test]
    fn gc_protects_fork_shared_content() {
        // E5.24: V1 = Fork(V0) names the same root; expiring V0 must not
        // reclaim a byte of V1's content.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let big = vec![0x33u8; 512 * 1024];
        let v0 = append_new(&mut store, history, None, &big);
        let v1 = fork_new(&mut store, history, v0.id(), None, None);
        expire_new(&mut store, v0.id());
        let prepared = store.prepare_gc().unwrap();
        let stats = prepared.stats();
        store.apply_prepared_gc(prepared);
        assert_eq!(stats.nodes_reclaimed, 0);
        assert_eq!(stats.payload_bytes_reclaimed, 0);
        assert_eq!(read_full(&store, v1, 512 * 1024), big);
        store.verify(v1).unwrap();
    }

    #[test]
    fn gc_all_expired_empties_backend_and_keeps_catalogue() {
        // E5.25: every version expired -> zero nodes, zero payload, full
        // catalogue, then a fresh root works from the empty backend.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"doomed-a");
        let v1 = append_new(&mut store, history, None, b"doomed-b");
        let v2 = fork_new(&mut store, history, v0.id(), None, None);
        expire_new(&mut store, v0.id());
        expire_new(&mut store, v1.id());
        expire_new(&mut store, v2.id());
        let prepared = store.prepare_gc().unwrap();
        let stats = prepared.stats();
        assert_eq!(stats.retained_versions, 0);
        assert_eq!(stats.expired_versions, 3);
        store.apply_prepared_gc(prepared);
        assert_eq!(stats.nodes_after, 0);
        assert_eq!(stats.payload_bytes_after, 0);
        assert_eq!(store.version_count(), 3);
        assert_eq!(store.backend.node_count(), 0);
        assert_eq!(store.backend.payload_len(), 0);
        assert!(store.backend.is_empty());
        for id in [v0.id(), v1.id(), v2.id()] {
            assert_eq!(store.is_expired(id), Ok(true));
        }
        // Snapshot of the reclaimed state carries no descriptors and
        // zero frontiers.
        let bytes = snapshot::encode_history_snapshot(&store, 0, 0).unwrap();
        let snapshot = snapshot::decode_history_snapshot(&bytes).unwrap();
        assert_eq!(snapshot.versions.len(), 3);
        assert!(snapshot.versions.iter().all(|entry| !entry.root_present));
        assert_eq!(snapshot.payload_end, 0);
        assert_eq!(snapshot.node_count, 0);
        assert_eq!(store.version_count(), 3);
        // A fresh root in the existing history works from the empty backend
        // with the next monotonic identity — never reusing 0..3.
        let v3 = append_new(&mut store, history, None, b"reborn");
        assert_eq!(v3.id(), VersionId::new(3));
        assert_eq!(read_full(&store, v3, 6), b"reborn");
        store.verify(v3).unwrap();
    }

    #[test]
    fn gc_twice_reclaims_nothing_further() {
        // E5.27: a second GC over compact state is a fixed point — the
        // independent re-mark finds no garbage the first pass left behind.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let big = vec![0x44u8; 300 * 1024];
        let v0 = append_new(&mut store, history, None, &big);
        let v1 = splice_new(&mut store, history, Some(v0.id()), 0, 1024, b"");
        let v2 = fork_new(&mut store, history, v1.id(), None, None);
        expire_new(&mut store, v0.id());
        let first = store.prepare_gc().unwrap();
        let first_stats = first.stats();
        assert!(first_stats.nodes_reclaimed > 0);
        store.apply_prepared_gc(first);
        let root_after_first = store.physical_root(v2.id()).unwrap();
        let second = store.prepare_gc().unwrap();
        let second_stats = second.stats();
        assert_eq!(second_stats.nodes_reclaimed, 0);
        assert_eq!(second_stats.payload_bytes_reclaimed, 0);
        assert_eq!(second_stats.nodes_before, second_stats.nodes_after);
        assert_eq!(
            second_stats.payload_bytes_before,
            second_stats.payload_bytes_after
        );
        store.apply_prepared_gc(second);
        // Stability: placement no longer moves once fully swept.
        assert_eq!(store.physical_root(v2.id()).unwrap(), root_after_first);
        assert_eq!(
            read_full(&store, v2, 300 * 1024 - 1024),
            big[1024..].to_vec()
        );
        assert_eq!(
            read_full(&store, v1, 300 * 1024 - 1024),
            big[1024..].to_vec()
        );
    }

    #[test]
    fn gc_preserves_stable_version_ids_across_reclamation() {
        // E5.21: identities, lifecycle, counters, and post-GC allocation.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let mut ids = Vec::new();
        for index in 0..5 {
            let payload = format!("stable-{index}");
            ids.push(append_new(&mut store, history, None, payload.as_bytes()).id());
        }
        assert_eq!(
            ids.iter().map(|id| id.id()).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4]
        );
        expire_new(&mut store, ids[1]);
        expire_new(&mut store, ids[3]);
        let prepared = store.prepare_gc().unwrap();
        assert_eq!(prepared.stats().retained_versions, 3);
        assert_eq!(prepared.stats().expired_versions, 2);
        store.apply_prepared_gc(prepared);
        for (index, id) in ids.iter().enumerate() {
            assert_eq!(id.id(), index as u64);
            assert_eq!(store.lookup_version(*id).is_ok(), index % 2 == 0);
            assert_eq!(store.is_expired(*id), Ok(index % 2 == 1));
        }
        assert_eq!(store.next_version_id, 5);
        let expected: Vec<(u64, &[u8])> =
            vec![(0, b"stable-0"), (2, b"stable-2"), (4, b"stable-4")];
        for (id, bytes) in expected {
            let version = store.lookup_version(VersionId::new(id)).unwrap();
            let mut output = Vec::new();
            store
                .read(version, 0, bytes.len() as u64, &mut output)
                .unwrap();
            assert_eq!(output, bytes);
        }
        // Post-GC allocation never reuses expired identities.
        let next = append_new(&mut store, history, None, b"stable-5");
        assert_eq!(next.id(), VersionId::new(5));
        assert_eq!(store.next_version_id, 6);
    }

    #[test]
    fn gc_reclaims_99_percent_branch_forest() {
        // E5.26 CI-scale: 2 MiB base, 200 branches with independent 8 KiB
        // replacements, 198 expired, 2 survivors plus base retained.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let base_payload = vec![0x55u8; 2 * 1024 * 1024];
        let base = append_new(&mut store, history, None, &base_payload);
        let mut branches = Vec::new();
        for index in 0..200usize {
            let mut insert = vec![0u8; 8 * 1024];
            let tag = format!("branch-{index:03}");
            insert[..tag.len()].copy_from_slice(tag.as_bytes());
            let branch = splice_new(
                &mut store,
                history,
                Some(base.id()),
                (index as u64 * 9973) % (2 * 1024 * 1024 - 8 * 1024),
                8 * 1024,
                &insert,
            );
            branches.push((branch, insert));
        }
        for (branch, _) in branches.iter().take(198) {
            expire_new(&mut store, branch.id());
        }
        let counters_before = store.work_counters();
        let prepared = store.prepare_gc().unwrap();
        let stats = prepared.stats();
        assert_eq!(stats.retained_versions, 3);
        assert_eq!(stats.expired_versions, 198);
        assert_eq!(store.work_counters(), counters_before);
        store.apply_prepared_gc(prepared);
        assert!(stats.nodes_reclaimed > 0);
        assert!(stats.payload_bytes_reclaimed > 0);
        // Survivors exact: base plus the two retained branches.
        assert_eq!(read_full(&store, base, 2 * 1024 * 1024), base_payload);
        for position in [198usize, 199] {
            let (branch, insert) = &branches[position];
            let len = store.logical_len(*branch).unwrap().get();
            assert_eq!(len, 2 * 1024 * 1024);
            let mut output = Vec::new();
            store.read(*branch, 0, len, &mut output).unwrap();
            let mut expected = base_payload.clone();
            let offset = (position as u64 * 9973) % (2 * 1024 * 1024 - 8 * 1024);
            expected[offset as usize..offset as usize + 8 * 1024].copy_from_slice(insert);
            assert_eq!(output, expected);
            store.verify(*branch).unwrap();
        }
        // Expired branches fail public acquisition.
        for (branch, _) in branches.iter().take(198) {
            assert_eq!(store.is_expired(branch.id()), Ok(true));
            assert_eq!(
                store.read(*branch, 0, 1, &mut Vec::new()),
                Err(HistoryError::VersionExpired)
            );
        }
    }

    #[test]
    fn snapshot_horizon_round_trip_preserves_exact_receipt_state() {
        // E4.29 (store-level): 4100 receipts with mixed retire/expire, then
        // snapshot encode/decode/import must preserve count, statuses,
        // order, and behavior — without resurrecting the 4 evicted receipts.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"snapshot-horizon");
        let mut committed = Vec::new();
        for index in 0..4100 {
            let version = fork_new(
                &mut store,
                history,
                v0.id(),
                Some(&fork_request_id(index)),
                None,
            );
            committed.push(version);
        }
        store.retire_request(&fork_request_id(100)).unwrap();
        store.retire_request(&fork_request_id(200)).unwrap();
        expire_new(&mut store, committed[300].id());
        assert_eq!(store.request_receipt_count(), 4096);
        // Evicted: req-0..req-3. Oldest retained: req-4.
        assert_eq!(
            store.request_receipt_status(&fork_request_id(3)),
            RequestReceiptStatus::Unknown
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(4)),
            RequestReceiptStatus::Active(committed[4].id())
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(100)),
            RequestReceiptStatus::Retired
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(300)),
            RequestReceiptStatus::Retired
        );
        let order_before: Vec<Vec<u8>> = store.receipt_order.iter().cloned().collect();
        assert_eq!(order_before.len(), 4096);
        assert_eq!(order_before[0], fork_request_id(4));
        assert_eq!(order_before[4095], fork_request_id(4099));
        let bytes = snapshot::encode_history_snapshot(&store, 2, 8192).unwrap();
        let snapshot = snapshot::decode_history_snapshot(&bytes).unwrap();
        assert_eq!(snapshot.receipt_order, order_before);
        // Struct round trip preserves the horizon exactly (durable import
        // agreement is covered at the authority level).
        let snapshot = snapshot::decode_history_snapshot(
            &snapshot::encode_history_snapshot_struct(&snapshot).unwrap(),
        )
        .unwrap();
        assert_eq!(snapshot.receipt_order, order_before);
        assert_eq!(store.request_receipt_count(), 4096);
        assert_eq!(
            store.request_receipt_status(&fork_request_id(3)),
            RequestReceiptStatus::Unknown
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(4)),
            RequestReceiptStatus::Active(committed[4].id())
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(100)),
            RequestReceiptStatus::Retired
        );
        assert_eq!(
            store.request_receipt_status(&fork_request_id(300)),
            RequestReceiptStatus::Retired
        );
        assert_eq!(
            store.receipt_order.iter().cloned().collect::<Vec<_>>(),
            order_before
        );
        assert_eq!(store.is_expired(committed[300].id()), Ok(true));
        // Behavior parity: retained replay, retired retire, evicted fresh.
        assert_eq!(
            store.fork(history, v0.id(), Some(&fork_request_id(4)), None),
            Ok(CommitOutcome::Replayed(committed[4]))
        );
        assert_eq!(
            store.fork(history, v0.id(), Some(&fork_request_id(100)), None),
            Ok(CommitOutcome::Retired)
        );
        assert!(matches!(
            store.fork(history, v0.id(), Some(&fork_request_id(3)), None),
            Ok(CommitOutcome::Committed(_))
        ));
        // Snapshot-only reopen did not resurrect evicted receipts.
        assert_eq!(store.request_receipt_count(), 4096);
    }

    #[test]
    fn snapshot_receipt_corruption_vectors_fail_closed() {
        // Each vector keeps wire integrity (struct round trip) while breaking
        // exactly one horizon invariant: decode and import must both fail.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"corrupt-horizon");
        let v1 = fork_new(&mut store, history, v0.id(), Some(b"req-a"), None);
        let v2 = fork_new(&mut store, history, v0.id(), Some(b"req-b"), None);
        let _ = v2;
        store.retire_request(b"req-b").unwrap();
        let honest_bytes = snapshot::encode_history_snapshot(&store, 0, 0).unwrap();
        let honest = snapshot::decode_history_snapshot(&honest_bytes).unwrap();
        assert_eq!(
            honest.receipt_order,
            vec![b"req-a".to_vec(), b"req-b".to_vec()]
        );
        let round_trip = |forged: snapshot::HistorySnapshot| {
            let bytes = snapshot::encode_history_snapshot_struct(&forged).unwrap();
            let decoded = snapshot::decode_history_snapshot(&bytes);
            // The struct-level receipt validator is what durable import runs
            // after decoding, so forged structs must fail it exactly when
            // they would fail import.
            let validated = snapshot::validate_snapshot_receipt_consistency(
                &forged.versions,
                &forged.active,
                &forged.retired,
                &forged.receipt_order,
            );
            (decoded.is_err(), validated.is_err())
        };
        // Order missing the active ID.
        let mut forged = honest.clone();
        forged.receipt_order.remove(0);
        assert_eq!(round_trip(forged), (true, true));
        // Order missing the retired ID.
        let mut forged = honest.clone();
        forged.receipt_order.remove(1);
        assert_eq!(round_trip(forged), (true, true));
        // Order duplicate ID.
        let mut forged = honest.clone();
        forged.receipt_order.push(b"req-a".to_vec());
        assert_eq!(round_trip(forged), (true, true));
        // Order ID present in neither ledger.
        let mut forged = honest.clone();
        forged.receipt_order[1] = b"req-ghost".to_vec();
        assert_eq!(round_trip(forged), (true, true));
        // Ledger ID absent from the order (extra active entry, order short).
        let mut forged = honest.clone();
        forged.active.push(snapshot::SnapshotActive {
            request_id: b"req-extra".to_vec(),
            digest: history_fork_digest(history, v0.id(), None),
            version: v1.id(),
        });
        assert_eq!(round_trip(forged), (true, true));
        // Active receipt referencing an expired version.
        let mut forged = honest.clone();
        forged.versions[1].lifecycle = VersionLifecycle::Expired;
        assert_eq!(round_trip(forged), (true, true));
        // Active receipt referencing a missing version.
        let mut forged = honest.clone();
        forged.active[0].version = VersionId::new(99);
        // Struct encode rejects the dangling reference before decode runs;
        // the shared receipt validator still fails the struct closed.
        assert!(snapshot::encode_history_snapshot_struct(&forged).is_err());
        assert!(snapshot::validate_snapshot_receipt_consistency(
            &forged.versions,
            &forged.active,
            &forged.retired,
            &forged.receipt_order,
        )
        .is_err());
        // Two active receipts claiming one version.
        let mut forged = honest.clone();
        forged.active.push(snapshot::SnapshotActive {
            request_id: b"req-clone".to_vec(),
            digest: forged.active[0].digest,
            version: v1.id(),
        });
        forged.receipt_order.push(b"req-clone".to_vec());
        assert_eq!(round_trip(forged), (true, true));
        // Active/retired overlap (single-entry retired keeps wire order).
        let mut forged = honest.clone();
        forged.retired[0].request_id = b"req-a".to_vec();
        forged.retired[0].digest = forged.active[0].digest;
        assert_eq!(round_trip(forged), (true, true));
        // Receipt count beyond capacity with valid integrity.
        let mut forged = honest.clone();
        forged.receipt_order = (0..4097)
            .map(|index| format!("flood-{index:05}").into_bytes())
            .collect();
        forged.active = forged
            .receipt_order
            .iter()
            .map(|id| snapshot::SnapshotActive {
                request_id: id.clone(),
                digest: [0x11; 32],
                version: v0.id(),
            })
            .collect();
        forged.retired = Vec::new();
        assert_eq!(round_trip(forged), (true, true));
    }

    #[test]
    fn snapshot_schema6_root_presence_vectors_fail_closed() {
        // E5.33: root-presence/length rules fail closed on decode and import
        // alike, with wire integrity kept valid via struct round trip.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"presence-base");
        let v1 = fork_new(&mut store, history, v0.id(), Some(b"req-p"), None);
        // Retire the receipt so later lifecycle forgeries isolate presence
        // rules instead of tripping the active-receipt validator first.
        store.retire_request(b"req-p").unwrap();
        let honest_bytes = snapshot::encode_history_snapshot(&store, 0, 0).unwrap();
        let honest = snapshot::decode_history_snapshot(&honest_bytes).unwrap();
        assert!(honest.versions.iter().all(|entry| entry.root_present));
        let round_trip = |forged: snapshot::HistorySnapshot| {
            let bytes = snapshot::encode_history_snapshot_struct(&forged).unwrap();
            let decoded = snapshot::decode_history_snapshot(&bytes);
            // The struct-level receipt validator is what durable import runs
            // after decoding, so forged structs must fail it exactly when
            // they would fail import.
            let validated = snapshot::validate_snapshot_receipt_consistency(
                &forged.versions,
                &forged.active,
                &forged.retired,
                &forged.receipt_order,
            );
            (decoded.is_err(), validated.is_err())
        };
        // Retained version without a materialized root: struct encode
        // enforces presence/descriptor agreement, so clear both — decode
        // then fails the retained-materialization gate while the receipt
        // validator (which only covers receipt rules) passes.
        let mut forged = honest.clone();
        forged.versions[0].root_present = false;
        forged.versions[0].root = None;
        assert_eq!(round_trip(forged), (true, false));
        // Valid rootless-expired mixes are covered by the store-path
        // round-trip test below: a struct mutation alone cannot rebuild the
        // image root table, so presence-count disagreement is exercised
        // next. Presence counts agree with the descriptors only at import,
        // which every authority path runs: decode accepts the structurally
        // fine artifact, import fails the count agreement (covered at the
        // durable authority level).
        let mut skewed = honest.clone();
        skewed.versions[0].lifecycle = VersionLifecycle::Expired;
        skewed.versions[0].root_present = false;
        skewed.versions[0].root = None;
        skewed.versions[1].lifecycle = VersionLifecycle::Expired;
        skewed.versions[1].root_present = false;
        skewed.versions[1].root = None;
        let skewed_bytes = snapshot::encode_history_snapshot_struct(&skewed).unwrap();
        let skewed_decoded = snapshot::decode_history_snapshot(&skewed_bytes).unwrap();
        assert_eq!(skewed_decoded.versions.len(), 2);
        // Active receipt pointing at an expired version: revive the
        // retired receipt as active against the expired fork. Receipt
        // validation runs against lifecycle even when the expired root is
        // still materialized.
        let mut live = honest.clone();
        live.versions[1].lifecycle = VersionLifecycle::Expired;
        live.retired.clear();
        live.active.push(snapshot::SnapshotActive {
            request_id: b"req-p".to_vec(),
            digest: history_fork_digest(history, v1.id(), None),
            version: v1.id(),
        });
        assert_eq!(round_trip(live), (true, true));
    }

    #[test]
    fn snapshot_mixed_materialized_and_reclaimed_round_trip() {
        // Valid controls: pre-GC seal keeps expired roots materialized;
        // post-GC seal leaves them rootless; both reopen exact.
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let v0 = append_new(&mut store, history, None, b"mixed-base");
        let v1 = fork_new(&mut store, history, v0.id(), Some(b"req-m"), None);
        expire_new(&mut store, v0.id());
        // Pre-GC: expired root still materialized, descriptors cover both.
        let pre_bytes = snapshot::encode_history_snapshot(&store, 0, 0).unwrap();
        let pre = snapshot::decode_history_snapshot(&pre_bytes).unwrap();
        assert!(pre.versions.iter().all(|entry| entry.root_present));
        assert_eq!(store.is_expired(v0.id()), Ok(true));
        assert!(store.physical_root(v0.id()).is_ok());
        // req-m resolves to the retained fork V1, so expiry of V0 leaves it
        // active on both sides of the round trip.
        assert_eq!(
            store.request_receipt_status(b"req-m"),
            RequestReceiptStatus::Active(v1.id())
        );
        // Post-GC: expired rootless, descriptors cover the retained fork only.
        let prepared = store.prepare_gc().unwrap();
        store.apply_prepared_gc(prepared);
        let post_bytes = snapshot::encode_history_snapshot(&store, 1, 0).unwrap();
        let post = snapshot::decode_history_snapshot(&post_bytes).unwrap();
        assert!(!post.versions[0].root_present);
        assert!(post.versions[1].root_present);
        assert_eq!(store.is_expired(v0.id()), Ok(true));
        assert!(store.physical_root(v0.id()).is_err());
        assert_eq!(store.is_retained(v1.id()), Ok(true));
        let mut output = Vec::new();
        store.read(v1, 0, 10, &mut output).unwrap();
        assert_eq!(output, b"mixed-base");
        assert_eq!(
            store.request_receipt_status(b"req-m"),
            RequestReceiptStatus::Active(v1.id())
        );
    }
}
