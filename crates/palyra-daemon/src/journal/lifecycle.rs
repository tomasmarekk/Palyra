//! Append-only process lifecycle transitions.

use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use sha2::{Digest, Sha256};

use crate::application::daemon_lifecycle::{
    DaemonLifecyclePhase, DaemonLifecycleSnapshot, LifecycleSubsystem,
};

use super::{current_unix_ms, JournalError, JournalStore};

/// Migration 79: process-wide startup, drain, checkpoint, and shutdown evidence.
pub(super) const MIGRATION_79_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS daemon_lifecycle_transitions (
        revision INTEGER PRIMARY KEY CHECK (revision > 0),
        epoch INTEGER NOT NULL CHECK (epoch > 0),
        phase TEXT NOT NULL CHECK (
            phase IN (
                'recovery_barrier',
                'running',
                'draining_admission',
                'draining_subsystems',
                'checkpointing',
                'shutdown_requested'
            )
        ),
        trigger TEXT,
        reason_code TEXT NOT NULL,
        requested_by TEXT NOT NULL,
        requested_at_unix_ms INTEGER NOT NULL,
        deadline_unix_ms INTEGER,
        admission_policy TEXT NOT NULL CHECK (
            admission_policy IN ('reject_new', 'durable_queue')
        ),
        snapshot_json TEXT NOT NULL,
        snapshot_sha256 TEXT NOT NULL,
        recorded_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1)
    );
    CREATE INDEX IF NOT EXISTS idx_daemon_lifecycle_epoch
        ON daemon_lifecycle_transitions(epoch, revision ASC);

    CREATE TABLE IF NOT EXISTS daemon_subsystem_drain_observations (
        observation_index INTEGER PRIMARY KEY AUTOINCREMENT,
        lifecycle_revision INTEGER NOT NULL,
        lifecycle_epoch INTEGER NOT NULL,
        subsystem TEXT NOT NULL CHECK (
            subsystem IN (
                'scheduler',
                'hooks',
                'background_queue',
                'channels',
                'self_healing',
                'runtime_health',
                'process_leases',
                'networked_workers',
                'transports'
            )
        ),
        state TEXT NOT NULL CHECK (state IN ('running', 'draining', 'drained', 'aborted')),
        observed_at_unix_ms INTEGER NOT NULL,
        UNIQUE(lifecycle_revision, subsystem),
        FOREIGN KEY(lifecycle_revision) REFERENCES daemon_lifecycle_transitions(revision)
    );

    CREATE TRIGGER IF NOT EXISTS trg_daemon_lifecycle_prevent_update
    BEFORE UPDATE ON daemon_lifecycle_transitions BEGIN
        SELECT RAISE(ABORT, 'daemon_lifecycle_transitions is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_daemon_lifecycle_prevent_delete
    BEFORE DELETE ON daemon_lifecycle_transitions BEGIN
        SELECT RAISE(ABORT, 'daemon_lifecycle_transitions is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_daemon_subsystem_drain_prevent_update
    BEFORE UPDATE ON daemon_subsystem_drain_observations BEGIN
        SELECT RAISE(ABORT, 'daemon_subsystem_drain_observations is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_daemon_subsystem_drain_prevent_delete
    BEFORE DELETE ON daemon_subsystem_drain_observations BEGIN
        SELECT RAISE(ABORT, 'daemon_subsystem_drain_observations is append-only');
    END;
"#;

/// Migration 92: admit the managed coding service to lifecycle drain evidence.
///
/// SQLite cannot widen a `CHECK` constraint in place, so the observation table
/// is rebuilt while preserving its append-only rows and stable observation IDs.
pub(super) const MIGRATION_92_SQL: &str = r#"
    DROP TRIGGER IF EXISTS trg_daemon_subsystem_drain_prevent_update;
    DROP TRIGGER IF EXISTS trg_daemon_subsystem_drain_prevent_delete;

    ALTER TABLE daemon_subsystem_drain_observations
        RENAME TO daemon_subsystem_drain_observations_legacy;

    CREATE TABLE daemon_subsystem_drain_observations (
        observation_index INTEGER PRIMARY KEY AUTOINCREMENT,
        lifecycle_revision INTEGER NOT NULL,
        lifecycle_epoch INTEGER NOT NULL,
        subsystem TEXT NOT NULL CHECK (
            subsystem IN (
                'scheduler',
                'hooks',
                'background_queue',
                'channels',
                'self_healing',
                'runtime_health',
                'managed_coding',
                'process_leases',
                'networked_workers',
                'transports'
            )
        ),
        state TEXT NOT NULL CHECK (state IN ('running', 'draining', 'drained', 'aborted')),
        observed_at_unix_ms INTEGER NOT NULL,
        UNIQUE(lifecycle_revision, subsystem),
        FOREIGN KEY(lifecycle_revision) REFERENCES daemon_lifecycle_transitions(revision)
    );

    INSERT INTO daemon_subsystem_drain_observations (
        observation_index,
        lifecycle_revision,
        lifecycle_epoch,
        subsystem,
        state,
        observed_at_unix_ms
    )
    SELECT
        observation_index,
        lifecycle_revision,
        lifecycle_epoch,
        subsystem,
        state,
        observed_at_unix_ms
    FROM daemon_subsystem_drain_observations_legacy;

    DROP TABLE daemon_subsystem_drain_observations_legacy;

    CREATE TRIGGER trg_daemon_subsystem_drain_prevent_update
    BEFORE UPDATE ON daemon_subsystem_drain_observations BEGIN
        SELECT RAISE(ABORT, 'daemon_subsystem_drain_observations is append-only');
    END;
    CREATE TRIGGER trg_daemon_subsystem_drain_prevent_delete
    BEFORE DELETE ON daemon_subsystem_drain_observations BEGIN
        SELECT RAISE(ABORT, 'daemon_subsystem_drain_observations is append-only');
    END;
"#;

impl JournalStore {
    /// Starts a new process epoch at the recovery barrier.
    ///
    /// # Errors
    /// Returns a journal error when the durable head is malformed or the
    /// startup transition cannot be committed.
    pub(crate) fn begin_daemon_lifecycle_startup(
        &self,
    ) -> Result<DaemonLifecycleSnapshot, JournalError> {
        let mut connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous = load_latest_snapshot(&transaction)?;
        let epoch = previous.as_ref().map_or(1, |snapshot| snapshot.epoch.saturating_add(1));
        let revision = previous.as_ref().map_or(1, |snapshot| snapshot.revision.saturating_add(1));
        let snapshot =
            DaemonLifecycleSnapshot::recovery_barrier(epoch, revision, current_unix_ms()?);
        insert_snapshot(&transaction, &snapshot)?;
        transaction.commit()?;
        Ok(snapshot)
    }

    /// Commits one compare-and-set lifecycle transition.
    ///
    /// # Errors
    /// Returns a journal error for a stale revision, malformed snapshot, or
    /// SQLite failure.
    pub(crate) fn append_daemon_lifecycle_snapshot(
        &self,
        snapshot: &DaemonLifecycleSnapshot,
    ) -> Result<(), JournalError> {
        validate_snapshot(snapshot)?;
        let mut connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous = load_latest_snapshot(&transaction)?.ok_or_else(|| {
            JournalError::InvalidArgument(
                "daemon lifecycle startup transition is missing".to_owned(),
            )
        })?;
        if snapshot.revision != previous.revision.saturating_add(1) {
            return Err(JournalError::InvalidArgument(format!(
                "daemon lifecycle revision conflict: expected {}, found {}",
                previous.revision.saturating_add(1),
                snapshot.revision
            )));
        }
        if snapshot.epoch != previous.epoch && snapshot.epoch != previous.epoch.saturating_add(1) {
            return Err(JournalError::InvalidArgument(format!(
                "daemon lifecycle epoch conflict: previous {}, found {}",
                previous.epoch, snapshot.epoch
            )));
        }
        insert_snapshot(&transaction, snapshot)?;
        transaction.commit()?;
        Ok(())
    }
}

fn insert_snapshot(
    transaction: &Transaction<'_>,
    snapshot: &DaemonLifecycleSnapshot,
) -> Result<(), JournalError> {
    validate_snapshot(snapshot)?;
    let snapshot_json = serde_json::to_string(snapshot)?;
    let snapshot_sha256 = hex::encode(Sha256::digest(snapshot_json.as_bytes()));
    let revision = i64::try_from(snapshot.revision).map_err(|_| {
        JournalError::InvalidArgument("daemon lifecycle revision exceeds SQLite range".to_owned())
    })?;
    let epoch = i64::try_from(snapshot.epoch).map_err(|_| {
        JournalError::InvalidArgument("daemon lifecycle epoch exceeds SQLite range".to_owned())
    })?;
    transaction.execute(
        r#"
            INSERT INTO daemon_lifecycle_transitions (
                revision, epoch, phase, trigger, reason_code, requested_by,
                requested_at_unix_ms, deadline_unix_ms, admission_policy,
                snapshot_json, snapshot_sha256, recorded_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1)
        "#,
        params![
            revision,
            epoch,
            snapshot.phase.as_str(),
            snapshot.trigger.map(|trigger| trigger.as_str()),
            snapshot.reason_code,
            snapshot.requested_by,
            snapshot.requested_at_unix_ms,
            snapshot.deadline_unix_ms,
            snapshot.admission_policy.as_str(),
            snapshot_json,
            snapshot_sha256,
            current_unix_ms()?,
        ],
    )?;
    for subsystem in &snapshot.subsystems {
        transaction.execute(
            r#"
                INSERT INTO daemon_subsystem_drain_observations (
                    lifecycle_revision, lifecycle_epoch, subsystem, state,
                    observed_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                revision,
                epoch,
                subsystem.subsystem.as_str(),
                match subsystem.state {
                    crate::application::daemon_lifecycle::LifecycleSubsystemState::Running =>
                        "running",
                    crate::application::daemon_lifecycle::LifecycleSubsystemState::Draining =>
                        "draining",
                    crate::application::daemon_lifecycle::LifecycleSubsystemState::Drained =>
                        "drained",
                    crate::application::daemon_lifecycle::LifecycleSubsystemState::Aborted =>
                        "aborted",
                },
                current_unix_ms()?,
            ],
        )?;
    }
    Ok(())
}

fn load_latest_snapshot(
    connection: &rusqlite::Connection,
) -> Result<Option<DaemonLifecycleSnapshot>, JournalError> {
    let row = connection
        .query_row(
            r#"
                SELECT snapshot_json, snapshot_sha256
                FROM daemon_lifecycle_transitions
                ORDER BY revision DESC
                LIMIT 1
            "#,
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((snapshot_json, expected_sha256)) = row else {
        return Ok(None);
    };
    let actual_sha256 = hex::encode(Sha256::digest(snapshot_json.as_bytes()));
    if actual_sha256 != expected_sha256 {
        return Err(JournalError::InvalidArgument(
            "daemon lifecycle snapshot digest mismatch".to_owned(),
        ));
    }
    let snapshot: DaemonLifecycleSnapshot = serde_json::from_str(snapshot_json.as_str())?;
    validate_snapshot(&snapshot)?;
    Ok(Some(snapshot))
}

fn validate_snapshot(snapshot: &DaemonLifecycleSnapshot) -> Result<(), JournalError> {
    if snapshot.epoch == 0
        || snapshot.revision == 0
        || snapshot.reason_code.trim().is_empty()
        || snapshot.reason_code.len() > 192
        || snapshot.requested_by.trim().is_empty()
        || snapshot.requested_by.len() > 192
        || snapshot.subsystems.len() != LifecycleSubsystem::DRAIN_ORDER.len()
    {
        return Err(JournalError::InvalidArgument(
            "daemon lifecycle snapshot is invalid".to_owned(),
        ));
    }
    for (expected, observed) in LifecycleSubsystem::DRAIN_ORDER.iter().zip(&snapshot.subsystems) {
        if expected != &observed.subsystem {
            return Err(JournalError::InvalidArgument(
                "daemon lifecycle subsystem order is invalid".to_owned(),
            ));
        }
    }
    if snapshot.phase == DaemonLifecyclePhase::Running && snapshot.deadline_unix_ms.is_some() {
        return Err(JournalError::InvalidArgument(
            "running daemon lifecycle cannot retain a drain deadline".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{MIGRATION_79_SQL, MIGRATION_92_SQL};

    #[test]
    fn managed_coding_migration_preserves_lifecycle_observations() {
        let connection = Connection::open_in_memory().expect("in-memory database should open");
        connection.execute_batch(MIGRATION_79_SQL).expect("lifecycle schema should initialize");
        connection
            .execute(
                r#"
                    INSERT INTO daemon_lifecycle_transitions (
                        revision, epoch, phase, reason_code, requested_by,
                        requested_at_unix_ms, admission_policy, snapshot_json,
                        snapshot_sha256, recorded_at_unix_ms
                    ) VALUES (1, 1, 'running', 'daemon.lifecycle.ready', 'test',
                        1, 'reject_new', '{}', 'digest', 1)
                "#,
                [],
            )
            .expect("legacy lifecycle transition should insert");
        connection
            .execute(
                r#"
                    INSERT INTO daemon_subsystem_drain_observations (
                        observation_index, lifecycle_revision, lifecycle_epoch,
                        subsystem, state, observed_at_unix_ms
                    ) VALUES (7, 1, 1, 'scheduler', 'running', 1)
                "#,
                [],
            )
            .expect("legacy observation should insert");

        connection
            .execute_batch(MIGRATION_92_SQL)
            .expect("managed coding lifecycle migration should apply");

        let preserved: (i64, String) = connection
            .query_row(
                r#"
                    SELECT observation_index, subsystem
                    FROM daemon_subsystem_drain_observations
                    WHERE lifecycle_revision = 1
                "#,
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("legacy observation should remain readable");
        assert_eq!(preserved, (7, "scheduler".to_owned()));
        connection
            .execute(
                r#"
                    INSERT INTO daemon_subsystem_drain_observations (
                        lifecycle_revision, lifecycle_epoch, subsystem, state,
                        observed_at_unix_ms
                    ) VALUES (1, 1, 'managed_coding', 'running', 2)
                "#,
                [],
            )
            .expect("managed coding observation should be admitted");
        assert!(
            connection
                .execute("UPDATE daemon_subsystem_drain_observations SET state = 'drained'", [],)
                .is_err(),
            "migrated observations must remain append-only"
        );
    }
}
