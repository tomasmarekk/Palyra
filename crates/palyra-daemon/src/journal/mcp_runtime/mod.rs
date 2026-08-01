//! Journal-backed MCP runtime records and immutable lifecycle evidence.

mod lifecycle_migration;
mod migration;
mod policy_migration;
mod policy_storage;
mod security_migration;
mod security_storage;
mod storage;

#[cfg(test)]
mod tests;

pub(super) const MIGRATION_97_SQL: &str = migration::SQL;
pub(super) const MIGRATION_98_SQL: &str = policy_migration::SQL;
pub(super) const MIGRATION_99_SQL: &str = security_migration::SQL;
pub(super) const MIGRATION_101_SQL: &str = lifecycle_migration::SQL;
