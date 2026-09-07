#![forbid(unsafe_code)]
#![allow(clippy::too_many_arguments)]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::unwrap_used
    )
)]

//! Domain-neutral persistent history engine.
//!
//! `tulya-core` owns version identity, persistent balanced sequences,
//! durable generation authority (WAL, sealed snapshots, manifest), and the
//! durability operation taxonomy. It depends on no adapter crate: checkpoint,
//! RL, and workspace clients all build on these primitives. In particular
//! this crate contains zero checkpoint vocabulary.

pub mod operation;
pub mod persistent_history;
pub mod persistent_sequence;

pub use operation::DurabilityOperation;
