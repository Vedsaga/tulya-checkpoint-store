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

use crate::error_classification::DurabilityOperation;
use crate::persistent_sequence::{
    BalancedSequence, LogicalLength, PersistentRoot, PersistentSequence, PersistentSequenceAppend,
    SequenceError, SequenceRange, SequenceWorkCounters,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt;

pub(crate) mod authority;
pub(crate) mod durable_log;
pub(crate) mod manifest;
pub(crate) mod snapshot;
use durable_log::{DurableError, DurableHistoryLog, HistoryLogRecord};

/// Domain separator for the generic history operation digest.
const HISTORY_OPERATION_DOMAIN: &[u8] = b"tulya-history/v1/commit\0";

/// Maximum request-identity byte length accepted by the history core.
const MAX_HISTORY_REQUEST_ID_BYTES: usize = 4096;

/// Maximum opaque adapter-binding byte length. Bindings identify adapter
/// objects for crash-safe remapping, not bulk data.
const MAX_HISTORY_BINDING_BYTES: usize = 4096;

/// Opaque core-assigned history/object identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct HistoryId(u64);

impl HistoryId {
    pub(crate) const fn id(self) -> u64 {
        self.0
    }

    pub(crate) const fn new(id: u64) -> Self {
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
pub(crate) struct VersionId(u64);

impl VersionId {
    pub(crate) const fn id(self) -> u64 {
        self.0
    }

    pub(crate) const fn new(id: u64) -> Self {
        Self(id)
    }
}

/// One committed generic version: its history, identity, optional parent,
/// and persistent root. Payloads stay opaque bytes below the adapter layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Version {
    history: HistoryId,
    id: VersionId,
    parent: Option<VersionId>,
    root: PersistentRoot,
}

impl Version {
    pub(crate) const fn history(self) -> HistoryId {
        self.history
    }

    pub(crate) const fn id(self) -> VersionId {
        self.id
    }

    pub(crate) const fn parent(self) -> Option<VersionId> {
        self.parent
    }

    pub(crate) const fn root(self) -> PersistentRoot {
        self.root
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryError {
    Sequence(SequenceError),
    Invalid(&'static str),
    Overflow(&'static str),
    Capacity(&'static str),
    RequestConflict,
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

/// Computes the generic operation digest for one exact semantic operation.
///
/// The digest binds history, parent, payload, and the opaque adapter binding
/// — the complete logical coordinates of the operation, deliberately
/// excluding the assigned version identity (which is a consequence, not an
/// input) and the request identity (which binds to the digest at the ledger).
/// A request bound to one digest therefore replays only the identical
/// operation and conflicts with any different history/parent/payload/binding.
pub(crate) fn history_operation_digest(
    history: HistoryId,
    parent: Option<VersionId>,
    payload: &[u8],
    binding: Option<&[u8]>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(HISTORY_OPERATION_DOMAIN);
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
    hasher.update(
        u64::try_from(payload.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hasher.update(payload);
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

/// One active request-ledger entry: the bound operation digest plus the
/// committed version that replay must return without a second mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveRequest {
    digest: [u8; 32],
    version: VersionId,
}

impl ActiveRequest {
    pub(crate) const fn digest(self) -> [u8; 32] {
        self.digest
    }

    pub(crate) const fn version(self) -> VersionId {
        self.version
    }
}

/// Outcome of a logical commit through the request ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitOutcome {
    Committed(Version),
    Replayed(Version),
    Retired,
}

impl CommitOutcome {
    pub(crate) const fn version(self) -> Option<Version> {
        match self {
            Self::Committed(version) | Self::Replayed(version) => Some(version),
            Self::Retired => None,
        }
    }

    pub(crate) const fn replayed(self) -> bool {
        match self {
            Self::Committed(_) => false,
            Self::Replayed(_) | Self::Retired => true,
        }
    }
}

/// Result of validating a logical commit without mutating state.
enum CommitPreview<'a> {
    Replayed(Version),
    Retired,
    Fresh(PreparedCommit<'a>),
}

/// A validated logical commit awaiting persistence and application. Borrows
/// caller bytes so preparation itself never allocates payload copies.
struct PreparedCommit<'a> {
    history: HistoryId,
    parent: Option<VersionId>,
    parent_root: Option<PersistentRoot>,
    payload: &'a [u8],
    request_id: Option<&'a [u8]>,
    binding: Option<&'a [u8]>,
    digest: [u8; 32],
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
pub(crate) struct PersistentHistoryStore {
    backend: BalancedSequence,
    histories: HashSet<HistoryId>,
    versions: Vec<Version>,
    next_history_id: u64,
    next_version_id: u64,
    active_requests: HashMap<Vec<u8>, ActiveRequest>,
    retired_requests: HashMap<Vec<u8>, [u8; 32]>,
    history_bindings: HashMap<HistoryId, Vec<u8>>,
    version_bindings: HashMap<VersionId, Vec<u8>>,
    poisoned: bool,
}

impl PersistentHistoryStore {
    pub(crate) fn new() -> Self {
        Self {
            backend: BalancedSequence::new(),
            histories: HashSet::new(),
            versions: Vec::new(),
            next_history_id: 0,
            next_version_id: 0,
            active_requests: HashMap::new(),
            retired_requests: HashMap::new(),
            history_bindings: HashMap::new(),
            version_bindings: HashMap::new(),
            poisoned: false,
        }
    }

    /// Creates an empty history and returns its core-assigned identity.
    ///
    /// Identities come from a monotonic counter, never from live-set size, so
    /// a removed history's identity is never reassigned.
    pub(crate) fn create_history(&mut self) -> Result<HistoryId, HistoryError> {
        self.require_unpoisoned()?;
        let id = HistoryId(self.next_history_id);
        self.next_history_id =
            self.next_history_id
                .checked_add(1)
                .ok_or(HistoryError::Overflow(
                    "persistent history count exceeds u64",
                ))?;
        self.histories
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent history set allocation failed"))?;
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
    pub(crate) fn create_history_with_binding(
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
        let id = self.create_history()?;
        self.history_bindings
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent history binding allocation failed"))?;
        let _ = self.history_bindings.insert(id, binding.to_vec());
        Ok(id)
    }

    /// Commits `payload` as a new version of `history` under `parent`.
    ///
    /// `None` parent creates a root version. The backend append preserves all
    /// retained history; on any failure no version is recorded.
    ///
    /// With `request_id`, the durable idempotency matrix applies: an unknown
    /// identity commits; a bound identity with the same operation digest
    /// replays its committed version with no second mutation; a different
    /// digest conflicts; a retired identity never resurrects.
    ///
    /// `binding` carries opaque adapter material recorded alongside the
    /// version and covered by the operation digest, so adapters rebuild their
    /// maps from the core itself after reopen.
    pub(crate) fn commit(
        &mut self,
        history: HistoryId,
        parent: Option<VersionId>,
        payload: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, HistoryError> {
        match self.preview_commit(history, parent, payload, request_id, binding)? {
            CommitPreview::Replayed(version) => Ok(CommitOutcome::Replayed(version)),
            CommitPreview::Retired => Ok(CommitOutcome::Retired),
            CommitPreview::Fresh(prepared) => self.apply_prepared_commit(&prepared),
        }
    }

    /// Validates a logical commit without mutating anything: history and
    /// parent resolution, operation-digest computation, and the full
    /// request-ledger matrix. Durable commit runs this first, persists the
    /// prepared bytes, and only then applies.
    fn preview_commit<'a>(
        &self,
        history: HistoryId,
        parent: Option<VersionId>,
        payload: &'a [u8],
        request_id: Option<&'a [u8]>,
        binding: Option<&'a [u8]>,
    ) -> Result<CommitPreview<'a>, HistoryError> {
        self.require_unpoisoned()?;
        if !self.histories.contains(&history) {
            return Err(HistoryError::Invalid(
                "persistent commit targets an unknown history",
            ));
        }
        // Payload validity is preview-checked so a rejected empty commit can
        // never reach the log: an encoded-but-unappliable record would brick
        // later recovery.
        if payload.is_empty() {
            return Err(HistoryError::Invalid(
                "persistent commit payload must be non-empty",
            ));
        }
        if let Some(bytes) = binding {
            validate_binding(bytes)?;
        }
        let parent_root = match parent {
            None => None,
            Some(id) => {
                let record = self.version_record(id)?;
                if record.history() != history {
                    return Err(HistoryError::Invalid(
                        "persistent parent version belongs to a different history",
                    ));
                }
                Some(record.root())
            }
        };
        let digest = history_operation_digest(history, parent, payload, binding);
        if let Some(request) = request_id {
            validate_request_identity(request)?;
            if let Some(active) = self.active_requests.get(request) {
                if active.digest() == digest {
                    let version = self.version_record(active.version())?;
                    return Ok(CommitPreview::Replayed(version));
                }
                return Err(HistoryError::RequestConflict);
            }
            if let Some(retired) = self.retired_requests.get(request) {
                if *retired == digest {
                    return Ok(CommitPreview::Retired);
                }
                return Err(HistoryError::RequestConflict);
            }
        }
        Ok(CommitPreview::Fresh(PreparedCommit {
            history,
            parent,
            parent_root,
            payload,
            request_id,
            binding,
            digest,
        }))
    }

    /// Applies a prepared commit: the single mutation point shared by the
    /// in-memory and durable paths.
    fn apply_prepared_commit(
        &mut self,
        prepared: &PreparedCommit<'_>,
    ) -> Result<CommitOutcome, HistoryError> {
        let id = VersionId(self.next_version_id);
        self.next_version_id =
            self.next_version_id
                .checked_add(1)
                .ok_or(HistoryError::Overflow(
                    "persistent version count exceeds u64",
                ))?;
        self.versions
            .try_reserve(1)
            .map_err(|_| HistoryError::Capacity("persistent version table allocation failed"))?;
        let root = self
            .backend
            .append(prepared.parent_root, prepared.payload)?;
        let version = Version {
            history: prepared.history,
            id,
            parent: prepared.parent,
            root,
        };
        self.versions.push(version);
        if let Some(request) = prepared.request_id {
            self.active_requests.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent active request allocation failed")
            })?;
            let _ = self.active_requests.insert(
                request.to_vec(),
                ActiveRequest {
                    digest: prepared.digest,
                    version: id,
                },
            );
        }
        if let Some(binding) = prepared.binding {
            self.version_bindings.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent version binding allocation failed")
            })?;
            let _ = self.version_bindings.insert(id, binding.to_vec());
        }
        Ok(CommitOutcome::Committed(version))
    }

    /// Moves an active request identity to the retired ledger.
    ///
    /// Retirement is prepare-then-commit: every fallible reservation completes
    /// before the active entry is removed, so failure leaves both ledgers
    /// unchanged. Unknown or already-retired identities fail closed.
    pub(crate) fn retire_request(&mut self, request_id: &[u8]) -> Result<(), HistoryError> {
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

    pub(crate) fn set_poisoned(&mut self) {
        self.poisoned = true;
    }

    /// Replays one logged history creation during recovery, restoring its
    /// adapter binding exactly.
    pub(crate) fn replay_create(
        &mut self,
        history: HistoryId,
        binding: Option<&[u8]>,
    ) -> Result<(), HistoryError> {
        let assigned = self.create_history()?;
        if assigned != history {
            return Err(HistoryError::Invalid(
                "history log history identity disagrees with replay order",
            ));
        }
        if let Some(bytes) = binding {
            validate_binding(bytes)?;
            self.history_bindings.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent history binding allocation failed")
            })?;
            let _ = self.history_bindings.insert(history, bytes.to_vec());
        }
        Ok(())
    }

    /// Replays one logged commit during recovery with exact-identity and
    /// digest assertions. Backend reconstruction revalidates parents and
    /// arena coordinates exactly as the live path does.
    pub(crate) fn replay_commit(
        &mut self,
        history: HistoryId,
        version: VersionId,
        parent: Option<VersionId>,
        payload: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
        digest: [u8; 32],
    ) -> Result<VersionId, HistoryError> {
        if !self.histories.contains(&history) {
            return Err(HistoryError::Invalid(
                "history log commit targets an unknown history",
            ));
        }
        let parent_root = match parent {
            None => None,
            Some(id) => {
                let record = self.version_record(id)?;
                if record.history() != history {
                    return Err(HistoryError::Invalid(
                        "history log parent version belongs to a different history",
                    ));
                }
                Some(record.root())
            }
        };
        if history_operation_digest(history, parent, payload, binding) != digest {
            return Err(HistoryError::Invalid(
                "history log commit digest disagrees with its operation",
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
        let root = self.backend.append(parent_root, payload)?;
        self.versions.push(Version {
            history,
            id,
            parent,
            root,
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
            let _ = self.active_requests.insert(
                request.to_vec(),
                ActiveRequest {
                    digest,
                    version: id,
                },
            );
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

    /// Durably creates a history: encode, write, barrier, then apply.
    pub(crate) fn create_history_durable(
        &mut self,
        log: &mut DurableHistoryLog,
    ) -> Result<HistoryId, DurableError> {
        self.require_unpoisoned_durable()?;
        let id = HistoryId(self.next_history_id);
        self.next_history_id
            .checked_add(1)
            .ok_or(DurableError::Rejected(HistoryError::Overflow(
                "persistent history count exceeds u64",
            )))?;
        let record = HistoryLogRecord::CreateHistory {
            history: id,
            binding: None,
        };
        let frame = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&record).map_err(DurableError::Rejected)?,
        )
        .map_err(DurableError::Rejected)?;
        self.write_and_sync(log, &frame)?;
        let assigned = self.create_history().map_err(DurableError::Rejected)?;
        if assigned != id {
            self.set_poisoned();
            return Err(DurableError::Indeterminate {
                operation: DurabilityOperation::FileSyncAll,
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "history identity diverged after durable write",
                ),
            });
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
        self.require_unpoisoned_durable()?;
        validate_binding(binding).map_err(DurableError::Rejected)?;
        if let Some(existing) = self
            .history_bindings
            .iter()
            .find_map(|(id, bound)| (bound.as_slice() == binding).then_some(*id))
        {
            return Ok(existing);
        }
        let id = HistoryId(self.next_history_id);
        self.next_history_id
            .checked_add(1)
            .ok_or(DurableError::Rejected(HistoryError::Overflow(
                "persistent history count exceeds u64",
            )))?;
        let record = HistoryLogRecord::CreateHistory {
            history: id,
            binding: Some(binding.to_vec()),
        };
        let frame = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&record).map_err(DurableError::Rejected)?,
        )
        .map_err(DurableError::Rejected)?;
        self.write_and_sync(log, &frame)?;
        let assigned = self
            .create_history_with_binding(binding)
            .map_err(DurableError::Rejected)?;
        if assigned != id {
            self.set_poisoned();
            return Err(DurableError::Indeterminate {
                operation: DurabilityOperation::FileSyncAll,
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "history identity diverged after durable write",
                ),
            });
        }
        Ok(id)
    }

    /// Durably commits through the request ledger: replay and retired outcomes
    /// return without touching the log; fresh operations follow
    /// write-then-barrier-then-apply with poison on any post-barrier failure.
    pub(crate) fn commit_durable(
        &mut self,
        log: &mut DurableHistoryLog,
        history: HistoryId,
        parent: Option<VersionId>,
        payload: &[u8],
        request_id: Option<&[u8]>,
        binding: Option<&[u8]>,
    ) -> Result<CommitOutcome, DurableError> {
        self.require_unpoisoned_durable()?;
        let prepared = match self
            .preview_commit(history, parent, payload, request_id, binding)
            .map_err(DurableError::Rejected)?
        {
            CommitPreview::Replayed(version) => return Ok(CommitOutcome::Replayed(version)),
            CommitPreview::Retired => return Ok(CommitOutcome::Retired),
            CommitPreview::Fresh(prepared) => prepared,
        };
        let record = HistoryLogRecord::Commit {
            history: prepared.history,
            version: VersionId(self.next_version_id),
            parent: prepared.parent,
            payload: prepared.payload.to_vec(),
            request_id: prepared.request_id.map(<[u8]>::to_vec),
            binding: prepared.binding.map(<[u8]>::to_vec),
            digest: prepared.digest,
        };
        let frame = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&record).map_err(DurableError::Rejected)?,
        )
        .map_err(DurableError::Rejected)?;
        self.write_and_sync(log, &frame)?;
        self.apply_prepared_commit(&prepared)
            .map_err(|error| self.poison_after_barrier(error))
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
        // A failed append leaves at most a torn tail, which recovery ignores
        // and the next append truncates: nothing new became authoritative, so
        // this stays a definite reject. (ENOSPC-specific mapping for the
        // candidate path arrives with the fault-matrix hardening slice.)
        log.append_frame(frame).map_err(|_| {
            DurableError::Rejected(HistoryError::Invalid(
                "history log append failed before any new authority",
            ))
        })?;
        log.sync().map_err(|source| {
            self.set_poisoned();
            DurableError::Indeterminate {
                operation: DurabilityOperation::FileSyncAll,
                source,
            }
        })
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
    pub(crate) fn read(
        &self,
        version: Version,
        offset: u64,
        length: u64,
        output: &mut Vec<u8>,
    ) -> Result<(), HistoryError> {
        let record = self.committed_version(version)?;
        let range = SequenceRange::new(LogicalLength::new(offset), LogicalLength::new(length))
            .ok_or(HistoryError::Invalid(
                "persistent history read range exceeds u64",
            ))?;
        self.backend.read_range(record.root(), range, output)?;
        Ok(())
    }

    /// Recomputes every reachable node's metadata and commitment.
    pub(crate) fn verify(&self, version: Version) -> Result<(), HistoryError> {
        let record = self.committed_version(version)?;
        self.backend.verify(record.root())?;
        Ok(())
    }

    /// Returns a snapshot of the backend diagnostic work counters.
    pub(crate) fn work_counters(&self) -> SequenceWorkCounters {
        self.backend.work_counters()
    }

    /// Looks up a committed version by logical identity for adapter reads.
    /// Coordinate-checked like every other lookup: a fabricated identity
    /// fails closed.
    pub(crate) fn lookup_version(&self, id: VersionId) -> Result<Version, HistoryError> {
        self.version_record(id)
    }

    /// Counts committed versions. Used for reopen statistics and tests.
    pub(crate) fn version_count(&self) -> usize {
        self.versions.len()
    }

    /// Resolves a version identity within an expected history for adapter
    /// reads. The returned root always comes from the committed table, so a
    /// fabricated value fails closed in [`PersistentHistoryStore::read`].
    pub(crate) fn committed_version_for_adapter(
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
        Ok(record)
    }

    fn version_record(&self, id: VersionId) -> Result<Version, HistoryError> {
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

    /// Resolves a caller-held version against the committed table so a
    /// fabricated or stale value fails closed instead of addressing the arena.
    fn committed_version(&self, version: Version) -> Result<Version, HistoryError> {
        let record = self.version_record(version.id())?;
        if record != version {
            return Err(HistoryError::Invalid(
                "persistent version does not match committed history",
            ));
        }
        Ok(record)
    }

    /// Rebuilds a store from a decoded snapshot, revalidating every active
    /// digest against freshly read payload bytes.
    ///
    /// Decode already enforced wire structure, dense tables, topological
    /// parents, ordered ledgers, and bounds. Import additionally proves each
    /// active request digest reproduces from the imported arena, so a
    /// structurally valid snapshot with tampered payloads still fails closed.
    pub(crate) fn import_snapshot(
        snapshot: snapshot::HistorySnapshot,
    ) -> Result<Self, HistoryError> {
        let (backend, roots) = if snapshot.versions.is_empty() {
            if !snapshot.image.is_empty() {
                return Err(HistoryError::Invalid(
                    "history snapshot image without versions is malformed",
                ));
            }
            (BalancedSequence::new(), Vec::new())
        } else {
            BalancedSequence::import_image(&snapshot.image)?
        };
        if roots.len() != snapshot.versions.len() {
            return Err(HistoryError::Invalid(
                "history snapshot image roots disagree with its version table",
            ));
        }
        let mut store = Self {
            backend,
            histories: HashSet::new(),
            versions: Vec::new(),
            next_history_id: snapshot.next_history_id,
            next_version_id: snapshot.next_version_id,
            active_requests: HashMap::new(),
            retired_requests: HashMap::new(),
            history_bindings: HashMap::new(),
            version_bindings: HashMap::new(),
            poisoned: false,
        };
        store
            .histories
            .try_reserve(snapshot.history_bindings.len())
            .map_err(|_| HistoryError::Capacity("history snapshot import allocation failed"))?;
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
        for (index, entry) in snapshot.versions.iter().enumerate() {
            let id = VersionId::new(u64::try_from(index).map_err(|_| {
                HistoryError::Overflow("history snapshot version identity exceeds u64")
            })?);
            store.versions.push(Version {
                history: entry.history,
                id,
                parent: entry.parent,
                root: roots[index],
            });
            if let Some(bytes) = &entry.binding {
                store.version_bindings.try_reserve(1).map_err(|_| {
                    HistoryError::Capacity("history snapshot import allocation failed")
                })?;
                let _ = store.version_bindings.insert(id, bytes.clone());
            }
        }
        for record in &snapshot.active {
            // Structural cross-references only: version existence and
            // coordinate agreement. Operation digests bind commit deltas,
            // which version content cannot reproduce, so digest authenticity
            // traces to commit-time and log-replay validation while the
            // artifact digest protects these bytes.
            let version = store.version_record(record.version)?;
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

    fn commit_new(
        store: &mut PersistentHistoryStore,
        history: HistoryId,
        parent: Option<VersionId>,
        payload: &[u8],
    ) -> Version {
        let outcome = store.commit(history, parent, payload, None, None).unwrap();
        assert!(matches!(outcome, CommitOutcome::Committed(_)));
        outcome.version().unwrap()
    }

    #[test]
    fn histories_are_isolated_and_versions_link_parents() {
        let mut store = PersistentHistoryStore::new();
        let first = store.create_history().unwrap();
        let second = store.create_history().unwrap();
        assert_ne!(first, second);

        let root_a = commit_new(&mut store, first, None, b"aaa");
        assert_eq!(root_a.history(), first);
        assert_eq!(root_a.parent(), None);
        let child_a = commit_new(&mut store, first, Some(root_a.id()), b"bbb");
        assert_eq!(child_a.parent(), Some(root_a.id()));
        let root_b = commit_new(&mut store, second, None, b"zzz");

        assert_eq!(read_full(&store, root_a, 3), b"aaa");
        assert_eq!(read_full(&store, child_a, 6), b"aaabbb");
        assert_eq!(read_full(&store, root_b, 3), b"zzz");

        // Cross-history grafts fail closed.
        assert_eq!(
            store.commit(second, Some(root_a.id()), b"nope", None, None),
            Err(HistoryError::Invalid(
                "persistent parent version belongs to a different history"
            ))
        );
        // Unknown history and unknown parent fail closed.
        assert_eq!(
            store.commit(HistoryId(999), None, b"nope", None, None),
            Err(HistoryError::Invalid(
                "persistent commit targets an unknown history"
            ))
        );
        assert_eq!(
            store.commit(first, Some(VersionId(999)), b"nope", None, None),
            Err(HistoryError::Invalid("persistent version is unknown"))
        );
        // Failures record no versions.
        assert_eq!(store.versions.len(), 3);
    }

    #[test]
    fn sibling_versions_share_parent_byte_exact() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let parent = commit_new(&mut store, history, None, b"parent");
        let left = commit_new(&mut store, history, Some(parent.id()), b"-left");
        let right = commit_new(&mut store, history, Some(parent.id()), b"-right");
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
        let version = commit_new(&mut store, history, None, b"data");

        let wrong_id = Version {
            history,
            id: VersionId(999),
            parent: None,
            root: version.root(),
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

        let tampered = Version {
            history,
            id: version.id(),
            parent: version.parent(),
            root: PersistentRoot::balanced_v2(0, LogicalLength::new(999)),
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
        assert!(store.commit(history, None, b"", None, None).is_err());
        assert_eq!(store.versions.len(), 0);
    }

    #[test]
    fn operation_digest_binds_exact_semantic_operation() {
        let history = HistoryId(3);
        let parent = Some(VersionId(2));
        let first = history_operation_digest(history, parent, b"payload", None);
        assert_eq!(
            first,
            history_operation_digest(history, parent, b"payload", None)
        );
        // Any differing coordinate changes the digest: same request bound to
        // one digest can never replay a different operation.
        assert_ne!(
            first,
            history_operation_digest(HistoryId(4), parent, b"payload", None)
        );
        assert_ne!(
            first,
            history_operation_digest(history, Some(VersionId(8)), b"payload", None)
        );
        assert_ne!(
            first,
            history_operation_digest(history, None, b"payload", None)
        );
        assert_ne!(
            first,
            history_operation_digest(history, parent, b"other", None)
        );
    }

    #[test]
    fn version_table_position_never_overrides_logical_identity() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let first = commit_new(&mut store, history, None, b"one");
        let second = commit_new(&mut store, history, Some(first.id()), b"two");
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
        let first = commit_new(&mut store, first_history, None, b"one");
        let second = commit_new(&mut store, first_history, Some(first.id()), b"two");
        let third = commit_new(&mut store, second_history, None, b"three");
        assert_eq!(first.id().id(), 0);
        assert_eq!(second.id().id(), 1);
        assert_eq!(third.id().id(), 2);
    }

    #[test]
    fn history_counters_observe_backend_work() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let before = store.work_counters();
        let version = commit_new(&mut store, history, None, b"payload");
        let after_commit = store.work_counters();
        assert_eq!(
            after_commit.payload_bytes_written - before.payload_bytes_written,
            7
        );
        assert!(after_commit.nodes_allocated > before.nodes_allocated);
        // Root creation resolves no parent and copies no spine.
        assert_eq!(after_commit.nodes_inspected, before.nodes_inspected);

        let child = commit_new(&mut store, history, Some(version.id()), b"more");
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
            .commit(history, None, b"payload", Some(b"req-1"), None)
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
            store.commit(history, None, b"payload", Some(b"req-1"), None),
            Ok(CommitOutcome::Replayed(first))
        );
        assert_eq!(store.versions.len(), 1);

        // Same request with a different payload conflicts.
        assert_eq!(
            store.commit(history, None, b"other", Some(b"req-1"), None),
            Err(HistoryError::RequestConflict)
        );
        // Same request with a different parent conflicts.
        assert_eq!(
            store.commit(history, Some(first.id()), b"payload", Some(b"req-1"), None),
            Err(HistoryError::RequestConflict)
        );
        assert_eq!(store.versions.len(), 1);

        // Retirement then resurrection attempt.
        store.retire_request(b"req-1").unwrap();
        assert_eq!(
            store.commit(history, None, b"payload", Some(b"req-1"), None),
            Ok(CommitOutcome::Retired)
        );
        assert_eq!(
            store.commit(history, None, b"other", Some(b"req-1"), None),
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
            store.commit(history, None, b"x", Some(b""), None),
            Err(HistoryError::Invalid(
                "persistent request identity is empty or exceeds the byte limit"
            ))
        );
    }

    #[test]
    fn poisoned_store_rejects_mutation_but_keeps_reads() {
        let mut store = PersistentHistoryStore::new();
        let history = store.create_history().unwrap();
        let version = commit_new(&mut store, history, None, b"data");
        store.set_poisoned();
        assert_eq!(
            store.commit(history, None, b"more", None, None),
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
            .commit_durable(&mut log, first_history, None, b"aaa", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(version) => version,
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("first durable commit must create")
            }
        };
        let v2 = match store
            .commit_durable(
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
                .commit_durable(
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
            store.commit_durable(
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
                .commit_durable(
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
            .commit_durable(&mut log, history, None, b"one", None, None)
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
            .commit_durable(
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
            .commit_durable(&mut log, history, None, b"stable", None, None)
            .unwrap()
        {
            CommitOutcome::Committed(_) => {}
            CommitOutcome::Replayed(_) | CommitOutcome::Retired => {
                panic!("first durable commit must create")
            }
        }
        // Simulate a crash mid-write: raw frame bytes without barrier or ack.
        let pending = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&durable_log::HistoryLogRecord::Commit {
                history,
                version: crate::persistent_history::VersionId(1),
                parent: Some(crate::persistent_history::VersionId(0)),
                payload: b"lost".to_vec(),
                request_id: None,
                binding: None,
                digest: crate::persistent_history::history_operation_digest(
                    history,
                    Some(crate::persistent_history::VersionId(0)),
                    b"lost",
                    None,
                ),
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
            .read(reopened.versions[0], 0, 6, &mut output)
            .unwrap();
        assert_eq!(output, b"stable");

        // The next append truncates the torn tail before writing.
        let mut healed = DurableHistoryLog::open(&path).unwrap();
        let mut healed_store = recover_history_store(&healed.read_all().unwrap()).unwrap();
        match healed_store
            .commit_durable(
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
            .commit_durable(&mut log, history, None, b"stable", None, None)
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
            store.commit_durable(&mut log, history, None, b"", None, None),
            Err(DurableError::Rejected(_))
        ));
        assert_eq!(log.read_all().unwrap().len(), len_before);
        match store
            .commit_durable(&mut log, history, None, b"after", None, None)
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
            store.commit_durable(&mut log, HistoryId(0), None, b"x", None, None),
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
        let plain = history_operation_digest(history, None, b"data", None);
        let bound = history_operation_digest(history, None, b"data", Some(b"thread-a/cp-1"));
        assert_ne!(plain, bound);

        let version = match store
            .commit(history, None, b"data", None, Some(b"thread-a/cp-1"))
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
            .commit(history, None, b"data", Some(b"req-b"), None)
            .unwrap()
            .version()
            .unwrap();
        assert_eq!(
            store.commit(history, None, b"data", Some(b"req-b"), Some(b"other")),
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
            .commit_durable(
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
}
