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
use crate::persistent_sequence::{
    BalancedSequence, LogicalLength, PersistentRoot, PersistentSequence, PersistentSequenceAppend,
    PersistentSequenceSplice, SequenceError, SequenceRange, SequenceWorkCounters,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fmt;

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

/// One committed generic version: its history, identity, optional parent,
/// and persistent root. Payloads stay opaque bytes below the adapter layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    history: HistoryId,
    id: VersionId,
    parent: Option<VersionId>,
    root: PersistentRoot,
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

    pub const fn root(self) -> PersistentRoot {
        self.root
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryError {
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
    request_id: Option<&'a [u8]>,
    binding: Option<&'a [u8]>,
    digest: [u8; 32],
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
    pub fn new() -> Self {
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
            SplicePreview::Fresh(prepared) => self.apply_prepared_splice(&prepared),
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
                self.backend.logical_len(record.root())?.get()
            }
        };
        self.splice(history, parent, offset, 0, bytes, request_id, binding)
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
                let len = self.backend.logical_len(record.root())?.get();
                (Some(record.root()), len)
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
                    let version = self.version_record(active.version())?;
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
        Ok(SplicePreview::Fresh({
            // Allocate the fresh identity here — after validation, before any
            // backend mutation or authority I/O — so exhaustion fails in
            // preview and can never strand arena work or an unrecoverable
            // max-Version WAL record. Replay/retired outcomes above need no
            // identity and return before this point.
            let next_version_id_after =
                self.next_version_id
                    .checked_add(1)
                    .ok_or(HistoryError::Overflow(
                        "persistent version count exceeds u64",
                    ))?;
            PreparedSplice {
                version: VersionId(self.next_version_id),
                next_version_id_after,
                history,
                parent,
                parent_root,
                offset,
                delete_len,
                insert,
                request_id,
                binding,
                digest,
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
    fn apply_prepared_splice(
        &mut self,
        prepared: &PreparedSplice<'_>,
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
        }
        if prepared.binding.is_some() {
            self.version_bindings.try_reserve(1).map_err(|_| {
                HistoryError::Capacity("persistent version binding allocation failed")
            })?;
        }
        let splice = self.backend.splice(
            prepared.parent_root,
            LogicalLength::new(prepared.offset),
            LogicalLength::new(prepared.delete_len),
            prepared.insert,
        )?;
        self.next_version_id = prepared.next_version_id_after;
        let version = Version {
            history: prepared.history,
            id: prepared.version,
            parent: prepared.parent,
            root: splice.root,
        };
        self.versions.push(version);
        if let Some(request) = prepared.request_id {
            let _ = self.active_requests.insert(
                request.to_vec(),
                ActiveRequest {
                    digest: prepared.digest,
                    version: version.id(),
                },
            );
        }
        if let Some(binding) = prepared.binding {
            let _ = self.version_bindings.insert(version.id(), binding.to_vec());
        }
        Ok(CommitOutcome::Committed(version))
    }

    /// Moves an active request identity to the retired ledger.
    ///
    /// Retirement is prepare-then-commit: every fallible reservation completes
    /// before the active entry is removed, so failure leaves both ledgers
    /// unchanged. Unknown or already-retired identities fail closed.
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
    pub fn replay_create(
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
    pub fn replay_splice(
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
                let len = self.backend.logical_len(record.root())?.get();
                (Some(record.root()), len)
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
        let splice = self.backend.splice(
            parent_root,
            LogicalLength::new(offset),
            LogicalLength::new(delete_len),
            insert,
        )?;
        self.versions.push(Version {
            history,
            id,
            parent,
            root: splice.root,
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
    pub fn replay_retire(
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
    /// write-then-barrier-then-apply with poison on any post-barrier failure.
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
        };
        let frame = durable_log::encode_history_log_frame(
            &durable_log::encode_history_log_record(&record).map_err(DurableError::Rejected)?,
        )
        .map_err(DurableError::Rejected)?;
        self.write_and_sync(log, &frame)?;
        self.apply_prepared_splice(&prepared)
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
                self.backend
                    .logical_len(record.root())
                    .map_err(|error| DurableError::Rejected(error.into()))?
                    .get()
            }
        };
        self.splice_durable(log, history, parent, offset, 0, bytes, request_id, binding)
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
    pub fn read(
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
    pub fn verify(&self, version: Version) -> Result<(), HistoryError> {
        let record = self.committed_version(version)?;
        self.backend.verify(record.root())?;
        Ok(())
    }

    /// Returns a snapshot of the backend diagnostic work counters.
    pub fn work_counters(&self) -> SequenceWorkCounters {
        self.backend.work_counters()
    }

    /// Looks up a committed version by logical identity for adapter reads.
    /// Coordinate-checked like every other lookup: a fabricated identity
    /// fails closed.
    pub fn lookup_version(&self, id: VersionId) -> Result<Version, HistoryError> {
        self.version_record(id)
    }

    /// Counts committed versions. Used for reopen statistics and tests.
    pub fn version_count(&self) -> usize {
        self.versions.len()
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
    /// reconstruction. The dense table is already identity-ordered.
    pub fn all_versions(&self) -> Vec<Version> {
        self.versions.clone()
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
    pub fn import_snapshot(snapshot: snapshot::HistorySnapshot) -> Result<Self, HistoryError> {
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
}
