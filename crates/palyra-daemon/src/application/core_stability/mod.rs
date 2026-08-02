//! Release-blocking qualification contracts for the production core runtime.
//!
//! Each submodule owns one independently reviewable gate. The canonical
//! evidence stays repository-owned and contains only bounded synthetic data.

pub(crate) mod performance;
pub(crate) mod retirement;
pub(crate) mod security;
