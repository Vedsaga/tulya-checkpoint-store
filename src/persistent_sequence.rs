//! Internal persistent-sequence contract for locality-sensitive checkpoint data.
//!
//! This module is the seam between checkpoint semantics and the physical node
//! representation. It is intentionally representation-neutral: Format v1 can
//! be adapted behind this contract without changing its bytes, while a future
//! balanced format can provide stored subtree lengths and stronger locality.
//!
//! Design correspondence:
//! `Vedsaga/Tulya-MDL-Lean/formal/Tulya/Incremental/PersistentAVLFinalAPI.lean`
//! exposes persistent edit, random-access, historical-preservation, and work
//! bounds that inform this contract. The Rust implementation is not currently
//! mechanically proved to refine that Lean model.
//!
//! The production target also requires append, bounded streaming, and verify
//! operations. They are intentionally not speculative trait methods here: each
//! operation is added to the executable contract when its production caller
//! and implementation land, so strict `dead_code` checks remain meaningful.

// The v2 codec, AVL core, canonical arena image, and publication records are
// staged behind the sequence boundary. Checkpoint-store integration makes the
// balanced path a production caller and removes the scoped dead-code allowances.
#[allow(dead_code)]
mod avl;
#[allow(dead_code)]
mod format_v2;
mod image_v2;
#[allow(dead_code)]
mod publication_v2;

/// Logical byte length of a persistent sequence.
///
/// Persistent lengths stay in a fixed-width integer. Conversion to `usize`
/// belongs at an allocation or slice boundary after explicit range checking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct LogicalLength(u64);

impl LogicalLength {
    /// Creates a logical length from its persisted-width representation.
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the fixed-width logical length.
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    /// Checked logical-length addition.
    pub(crate) fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }
}

/// Physical representation used by one persistent root.
///
/// `LegacyV1` names the released left-deep DAG representation. It does not
/// claim the balanced-tree or persisted-subtree-length guarantees required by
/// the production locality gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SequenceRepresentation {
    LegacyV1,
}

/// Typed root metadata consumed by checkpoint code.
///
/// `node_id` identifies the physical root while `logical_len` carries the
/// sequence length at the semantic boundary. For Format v1 that length may
/// still have been derived by legacy traversal. A future writable format must
/// persist enough metadata to construct this value without whole-parent work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PersistentRoot {
    node_id: u64,
    logical_len: LogicalLength,
    representation: SequenceRepresentation,
}

impl PersistentRoot {
    /// Adapts a released Format-v1 root without changing its on-disk meaning.
    pub(crate) const fn legacy_v1(node_id: u64, logical_len: LogicalLength) -> Self {
        Self {
            node_id,
            logical_len,
            representation: SequenceRepresentation::LegacyV1,
        }
    }

    /// Returns the physical root-node identifier.
    pub(crate) const fn node_id(self) -> u64 {
        self.node_id
    }

    /// Returns the exact logical byte length represented by this root.
    pub(crate) const fn logical_len(self) -> LogicalLength {
        self.logical_len
    }

    /// Returns the physical representation version for this root.
    pub(crate) const fn representation(self) -> SequenceRepresentation {
        self.representation
    }
}

/// Checked half-open logical byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SequenceRange {
    offset: LogicalLength,
    length: LogicalLength,
}

impl SequenceRange {
    /// Creates a range only when its half-open end fits in `u64`.
    pub(crate) fn new(offset: LogicalLength, length: LogicalLength) -> Option<Self> {
        offset.checked_add(length)?;
        Some(Self { offset, length })
    }

    /// Returns the range start.
    pub(crate) const fn offset(self) -> LogicalLength {
        self.offset
    }

    /// Returns the range length.
    pub(crate) const fn length(self) -> LogicalLength {
        self.length
    }

    /// Returns the checked half-open range end.
    pub(crate) fn end(self) -> LogicalLength {
        self.offset
            .checked_add(self.length)
            .expect("SequenceRange construction validates its end")
    }
}

/// Active representation-neutral persistent byte-sequence read operations.
///
/// This first production seam contains only operations already exercised by
/// checkpoint-store callers. Format v1 is allowed to retain legacy traversal
/// costs behind this interface. The later balanced writable implementation
/// must preserve these semantics while adding the remaining target operations
/// and logarithmic/bounded locality guarantees.
pub(crate) trait PersistentSequence {
    type Error;

    /// Returns the exact logical length represented by `root`.
    fn logical_len(&self, root: PersistentRoot) -> Result<LogicalLength, Self::Error>;

    /// Appends the exact requested range to `output`.
    fn read_range(
        &self,
        root: PersistentRoot,
        range: SequenceRange,
        output: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_length_addition_fails_closed_on_overflow() {
        assert_eq!(
            LogicalLength::new(u64::MAX).checked_add(LogicalLength::new(1)),
            None
        );
    }

    #[test]
    fn range_construction_rejects_overflow() {
        assert!(SequenceRange::new(LogicalLength::new(u64::MAX), LogicalLength::new(1)).is_none());
    }

    #[test]
    fn legacy_root_keeps_identity_length_and_representation_distinct() {
        let root = PersistentRoot::legacy_v1(41, LogicalLength::new(8192));
        assert_eq!(root.node_id(), 41);
        assert_eq!(root.logical_len().get(), 8192);
        assert_eq!(root.representation(), SequenceRepresentation::LegacyV1);
    }
}
