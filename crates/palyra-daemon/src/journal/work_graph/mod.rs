//! Journal-backed work graph storage and atomic host transitions.
//! Graph headers, items, and append-only events share the daemon journal database.

use super::*;

mod migration;
mod storage;

#[cfg(test)]
mod tests;

pub(super) const MIGRATION_93_SQL: &str = migration::SQL;

pub(crate) use storage::WorkGraphSnapshotV1;
