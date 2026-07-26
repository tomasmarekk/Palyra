//! Append-only config-watch, last-known-good, and restart-decision evidence.

use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};

use crate::application::restart_coordinator::{
    ConfigWatchEventV1, RestartDecision, RestartDecisionKind,
};

use super::{current_unix_ms, JournalError, JournalStore};

/// Migration 80: config watcher observations and idempotent restart decisions.
pub(super) const MIGRATION_80_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS config_watch_events_v1 (
        event_ulid TEXT PRIMARY KEY,
        kind TEXT NOT NULL CHECK (
            kind IN (
                'native_event',
                'polling_change',
                'missing',
                'invalid',
                'validated',
                'polling_fallback',
                'watcher_restarted'
            )
        ),
        source_identity_sha256 TEXT NOT NULL,
        config_sha256 TEXT,
        reason_code TEXT NOT NULL,
        watcher_generation INTEGER NOT NULL CHECK (watcher_generation > 0),
        observed_at_unix_ms INTEGER NOT NULL,
        event_json TEXT NOT NULL,
        event_sha256 TEXT NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1)
    );
    CREATE INDEX IF NOT EXISTS idx_config_watch_events_observed
        ON config_watch_events_v1(observed_at_unix_ms DESC);

    CREATE TABLE IF NOT EXISTS config_last_known_good_refs (
        reference_index INTEGER PRIMARY KEY AUTOINCREMENT,
        config_sha256 TEXT NOT NULL,
        source_identity_sha256 TEXT NOT NULL,
        accepted_by_request_ulid TEXT,
        accepted_at_unix_ms INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        UNIQUE(config_sha256, source_identity_sha256)
    );

    CREATE TABLE IF NOT EXISTS daemon_restart_decisions (
        request_ulid TEXT PRIMARY KEY,
        coalescing_key TEXT NOT NULL,
        config_sha256 TEXT NOT NULL,
        source_identity_sha256 TEXT NOT NULL,
        last_known_good_sha256 TEXT NOT NULL,
        kind TEXT NOT NULL CHECK (
            kind IN (
                'ready_now',
                'scheduled_after_drain',
                'deferred_by_active_mutation',
                'blocked_by_manual_review',
                'coalesced',
                'cancelled'
            )
        ),
        reason_code TEXT NOT NULL,
        coalesced_into_request_ulid TEXT,
        decision_json TEXT NOT NULL,
        decision_sha256 TEXT NOT NULL,
        decided_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1)
    );
    CREATE INDEX IF NOT EXISTS idx_daemon_restart_coalescing
        ON daemon_restart_decisions(coalescing_key, decided_at_unix_ms DESC);

    CREATE TRIGGER IF NOT EXISTS trg_config_watch_events_prevent_update
    BEFORE UPDATE ON config_watch_events_v1 BEGIN
        SELECT RAISE(ABORT, 'config_watch_events_v1 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_config_watch_events_prevent_delete
    BEFORE DELETE ON config_watch_events_v1 BEGIN
        SELECT RAISE(ABORT, 'config_watch_events_v1 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_config_lkg_prevent_update
    BEFORE UPDATE ON config_last_known_good_refs BEGIN
        SELECT RAISE(ABORT, 'config_last_known_good_refs is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_config_lkg_prevent_delete
    BEFORE DELETE ON config_last_known_good_refs BEGIN
        SELECT RAISE(ABORT, 'config_last_known_good_refs is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_daemon_restart_prevent_update
    BEFORE UPDATE ON daemon_restart_decisions BEGIN
        SELECT RAISE(ABORT, 'daemon_restart_decisions is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_daemon_restart_prevent_delete
    BEFORE DELETE ON daemon_restart_decisions BEGIN
        SELECT RAISE(ABORT, 'daemon_restart_decisions is append-only');
    END;
"#;

impl JournalStore {
    /// Appends one redacted watcher observation.
    ///
    /// # Errors
    /// Returns a journal error for invalid evidence or SQLite failure.
    pub(crate) fn record_config_watch_event(
        &self,
        event: &ConfigWatchEventV1,
    ) -> Result<(), JournalError> {
        validate_watch_event(event)?;
        let event_json = serde_json::to_string(event)?;
        let event_sha256 = hex::encode(Sha256::digest(event_json.as_bytes()));
        let watcher_generation = i64::try_from(event.watcher_generation).map_err(|_| {
            JournalError::InvalidArgument(
                "config watcher generation exceeds SQLite range".to_owned(),
            )
        })?;
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        connection.execute(
            r#"
                INSERT INTO config_watch_events_v1 (
                    event_ulid, kind, source_identity_sha256, config_sha256,
                    reason_code, watcher_generation, observed_at_unix_ms,
                    event_json, event_sha256, schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1)
            "#,
            params![
                event.event_id,
                event.kind.as_str(),
                event.source_identity_sha256,
                event.config_sha256,
                event.reason_code,
                watcher_generation,
                event.observed_at_unix_ms,
                event_json,
                event_sha256,
            ],
        )?;
        Ok(())
    }

    /// Returns the latest request that owns an equivalent coalescing key.
    ///
    /// # Errors
    /// Returns a journal error when the lookup fails.
    pub(crate) fn restart_request_for_coalescing_key(
        &self,
        coalescing_key: &str,
    ) -> Result<Option<String>, JournalError> {
        if !valid_sha256(coalescing_key) {
            return Err(JournalError::InvalidArgument(
                "restart coalescing key must be SHA-256".to_owned(),
            ));
        }
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        connection
            .query_row(
                r#"
                    SELECT request_ulid
                    FROM daemon_restart_decisions
                    WHERE coalescing_key = ?1 AND kind != 'coalesced'
                    ORDER BY decided_at_unix_ms DESC
                    LIMIT 1
                "#,
                params![coalescing_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(JournalError::from)
    }

    /// Appends one immutable restart decision.
    ///
    /// # Errors
    /// Returns a journal error for invalid evidence or SQLite failure.
    pub(crate) fn record_restart_decision(
        &self,
        decision: &RestartDecision,
    ) -> Result<(), JournalError> {
        validate_restart_decision(decision)?;
        let decision_json = serde_json::to_string(decision)?;
        let decision_sha256 = hex::encode(Sha256::digest(decision_json.as_bytes()));
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        connection.execute(
            r#"
                INSERT INTO daemon_restart_decisions (
                    request_ulid, coalescing_key, config_sha256,
                    source_identity_sha256, last_known_good_sha256, kind,
                    reason_code, coalesced_into_request_ulid, decision_json,
                    decision_sha256, decided_at_unix_ms, schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1)
            "#,
            params![
                decision.request.request_id,
                decision.request.coalescing_key,
                decision.request.config_sha256,
                decision.request.source_identity_sha256,
                decision.request.last_known_good_sha256,
                decision.kind.as_str(),
                decision.reason_code,
                decision.coalesced_into_request_id,
                decision_json,
                decision_sha256,
                decision.decided_at_unix_ms,
            ],
        )?;
        Ok(())
    }

    /// Persists an accepted hash-only last-known-good reference.
    ///
    /// # Errors
    /// Returns a journal error for invalid hashes or SQLite failure.
    pub(crate) fn record_config_last_known_good(
        &self,
        config_sha256: &str,
        source_identity_sha256: &str,
        accepted_by_request_id: Option<&str>,
        reason_code: &str,
    ) -> Result<(), JournalError> {
        if !valid_sha256(config_sha256)
            || !valid_sha256(source_identity_sha256)
            || reason_code.trim().is_empty()
            || reason_code.len() > 192
        {
            return Err(JournalError::InvalidArgument(
                "last-known-good config reference is invalid".to_owned(),
            ));
        }
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        connection.execute(
            r#"
                INSERT OR IGNORE INTO config_last_known_good_refs (
                    config_sha256, source_identity_sha256,
                    accepted_by_request_ulid, accepted_at_unix_ms, reason_code
                ) VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                config_sha256,
                source_identity_sha256,
                accepted_by_request_id,
                current_unix_ms()?,
                reason_code,
            ],
        )?;
        Ok(())
    }

    /// Loads the latest accepted config digest for one source.
    ///
    /// # Errors
    /// Returns a journal error when the lookup fails.
    pub(crate) fn latest_config_last_known_good(
        &self,
        source_identity_sha256: &str,
    ) -> Result<Option<String>, JournalError> {
        if !valid_sha256(source_identity_sha256) {
            return Err(JournalError::InvalidArgument(
                "config source identity must be SHA-256".to_owned(),
            ));
        }
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        connection
            .query_row(
                r#"
                    SELECT config_sha256
                    FROM config_last_known_good_refs
                    WHERE source_identity_sha256 = ?1
                    ORDER BY accepted_at_unix_ms DESC, reference_index DESC
                    LIMIT 1
                "#,
                params![source_identity_sha256],
                |row| row.get(0),
            )
            .optional()
            .map_err(JournalError::from)
    }

    /// Returns recent restart decisions for diagnostics.
    ///
    /// # Errors
    /// Returns a journal error when evidence is malformed or the query fails.
    pub(crate) fn recent_restart_decisions(
        &self,
        limit: usize,
    ) -> Result<Vec<RestartDecision>, JournalError> {
        let limit = i64::try_from(limit.clamp(1, 64)).unwrap_or(64);
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = connection.prepare(
            r#"
                SELECT decision_json, decision_sha256
                FROM daemon_restart_decisions
                ORDER BY decided_at_unix_ms DESC
                LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map(params![limit], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut decisions = Vec::new();
        for row in rows {
            let (decision_json, expected_sha256) = row?;
            let actual_sha256 = hex::encode(Sha256::digest(decision_json.as_bytes()));
            if actual_sha256 != expected_sha256 {
                return Err(JournalError::InvalidArgument(
                    "restart decision digest mismatch".to_owned(),
                ));
            }
            let decision: RestartDecision = serde_json::from_str(decision_json.as_str())?;
            validate_restart_decision(&decision)?;
            decisions.push(decision);
        }
        Ok(decisions)
    }
}

fn validate_watch_event(event: &ConfigWatchEventV1) -> Result<(), JournalError> {
    if event.event_id.trim().is_empty()
        || !valid_sha256(&event.source_identity_sha256)
        || event.config_sha256.as_deref().is_some_and(|value| !valid_sha256(value))
        || event.reason_code.trim().is_empty()
        || event.reason_code.len() > 192
        || event.watcher_generation == 0
        || event.schema_version != 1
    {
        return Err(JournalError::InvalidArgument("config watch event is invalid".to_owned()));
    }
    Ok(())
}

fn validate_restart_decision(decision: &RestartDecision) -> Result<(), JournalError> {
    let request = &decision.request;
    if request.request_id.trim().is_empty()
        || !valid_sha256(&request.coalescing_key)
        || !valid_sha256(&request.config_sha256)
        || !valid_sha256(&request.source_identity_sha256)
        || !valid_sha256(&request.last_known_good_sha256)
        || decision.reason_code.trim().is_empty()
        || decision.reason_code.len() > 192
        || (decision.kind == RestartDecisionKind::Coalesced
            && decision.coalesced_into_request_id.is_none())
    {
        return Err(JournalError::InvalidArgument("restart decision is invalid".to_owned()));
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        application::restart_coordinator::{
            decide_restart, ConfigWatchEventKind, RestartBlockerSnapshot, RestartRequest,
        },
        journal::JournalConfig,
    };

    #[test]
    fn restart_evidence_survives_journal_reopen_and_coalesces() {
        let directory = tempfile::tempdir().expect("temp directory should exist");
        let database = directory.path().join("journal.sqlite3");
        let config = JournalConfig {
            db_path: database,
            hash_chain_enabled: true,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        };
        let store = JournalStore::open(config.clone()).expect("journal should open");
        let source_sha256 = "a".repeat(64);
        let config_sha256 = "b".repeat(64);
        store
            .record_config_last_known_good(
                config_sha256.as_str(),
                source_sha256.as_str(),
                None,
                "daemon.config.startup_validated",
            )
            .expect("last known good should persist");
        store
            .record_config_watch_event(&ConfigWatchEventV1 {
                event_id: "01WATCHEREVENT".to_owned(),
                kind: ConfigWatchEventKind::Validated,
                source_identity_sha256: source_sha256.clone(),
                config_sha256: Some(config_sha256.clone()),
                reason_code: "daemon.config_watch.candidate_valid".to_owned(),
                watcher_generation: 1,
                observed_at_unix_ms: 10,
                schema_version: 1,
            })
            .expect("watch event should persist");
        let decision = decide_restart(
            RestartRequest {
                request_id: "01RESTARTREQUEST".to_owned(),
                coalescing_key: "c".repeat(64),
                config_sha256: config_sha256.clone(),
                source_identity_sha256: source_sha256.clone(),
                last_known_good_sha256: config_sha256.clone(),
                restart_required_steps: 1,
                hot_safe_steps: 0,
                requested_at_unix_ms: 10,
            },
            RestartBlockerSnapshot {
                active_runs: 0,
                outcome_unknown_mutations: 0,
                blocked_active_steps: 0,
                manual_review_steps: 0,
                lifecycle_phase: "running".to_owned(),
            },
            None,
            11,
        );
        store.record_restart_decision(&decision).expect("restart decision should persist");
        drop(store);

        let reopened = JournalStore::open(config).expect("journal should reopen");
        assert_eq!(
            reopened
                .latest_config_last_known_good(source_sha256.as_str())
                .expect("last known good should load")
                .as_deref(),
            Some(config_sha256.as_str())
        );
        assert_eq!(
            reopened
                .restart_request_for_coalescing_key(&"c".repeat(64))
                .expect("coalescing owner should load")
                .as_deref(),
            Some("01RESTARTREQUEST")
        );
        assert_eq!(
            reopened.recent_restart_decisions(4).expect("restart decision should load"),
            vec![decision]
        );
    }
}
