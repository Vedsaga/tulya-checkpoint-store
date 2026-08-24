//! Public on-disk compatibility contract.
//!
//! Tulya's pre-release implementation used several internal component
//! revision numbers. Those numbers are not public formats. The first release
//! has one user-visible Tulya checkpoint format. The compatibility integer is
//! not a user-selectable product variant.

/// Stable name of the public on-disk format.
pub const NAME: &str = "tulya-checkpoint-store";

/// Current and only public on-disk format version.
pub const VERSION: u32 = 1;
