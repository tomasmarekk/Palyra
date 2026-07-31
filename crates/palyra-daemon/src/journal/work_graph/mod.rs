//! Journal-backed work graph storage and atomic host transitions.
//! Graph headers, items, and append-only events share the daemon journal database.

use super::*;

mod claim_migration;
mod claims;
mod concurrency;
mod concurrency_migration;
mod migration;
mod storage;

#[cfg(test)]
mod tests;

pub(super) const MIGRATION_94_SQL: &str = claim_migration::SQL;
pub(super) const MIGRATION_95_SQL: &str = concurrency_migration::SQL;
pub(super) const MIGRATION_93_SQL: &str = migration::SQL;

pub(crate) use storage::WorkGraphSnapshotV1;
