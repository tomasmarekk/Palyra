//! Connector instance rows: registration upserts, runtime-state updates, and
//! the inbound dedupe window.
//!
//! Upserts preserve runtime bookkeeping (readiness, liveness, restart count,
//! last-seen timestamps) so re-registering a connector never resets its
//! operational history.

use rusqlite::params;

use super::super::protocol::{ConnectorInstanceSpec, ConnectorLiveness, ConnectorReadiness};
use super::records::parse_instance_row;
use super::{ConnectorInstanceRecord, ConnectorStore, ConnectorStoreError};

impl ConnectorStore {
    /// Inserts the instance or updates its spec columns if it already exists.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::Sqlite`] when the spec fails protocol
    /// validation or the statement fails, and [`ConnectorStoreError::Serde`]
    /// when the allowlist cannot be encoded.
    pub fn upsert_instance(
        &self,
        spec: &ConnectorInstanceSpec,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        // Boxed into rusqlite's catch-all variant so validation failures flow
        // through the single Sqlite error path.
        spec.validate()
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let allowlist_json = serde_json::to_string(&spec.egress_allowlist)?;
        self.with_transaction(|transaction| {
            transaction.execute(
                r#"
                INSERT INTO connector_instances (
                    connector_id, kind, principal, auth_profile_ref, token_vault_ref,
                    egress_allowlist_json, enabled, readiness, liveness, restart_count,
                    last_error, last_inbound_unix_ms, last_outbound_unix_ms,
                    created_at_unix_ms, updated_at_unix_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, NULL, NULL, NULL, ?10, ?10)
                ON CONFLICT(connector_id) DO UPDATE SET
                    kind = excluded.kind,
                    principal = excluded.principal,
                    auth_profile_ref = excluded.auth_profile_ref,
                    token_vault_ref = excluded.token_vault_ref,
                    egress_allowlist_json = excluded.egress_allowlist_json,
                    enabled = excluded.enabled,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
                "#,
                params![
                    spec.connector_id,
                    spec.kind.as_str(),
                    spec.principal,
                    spec.auth_profile_ref,
                    spec.token_vault_ref,
                    allowlist_json,
                    if spec.enabled { 1_i64 } else { 0_i64 },
                    ConnectorReadiness::Ready.as_str(),
                    ConnectorLiveness::Stopped.as_str(),
                    now_unix_ms,
                ],
            )?;
            Ok(())
        })
    }

    /// Lists all instances ordered by connector id.
    ///
    /// # Errors
    /// Returns a storage error when the query fails or a row holds unknown
    /// kind/readiness/liveness labels.
    pub fn list_instances(&self) -> Result<Vec<ConnectorInstanceRecord>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let mut statement = connection.prepare(
            r#"
            SELECT connector_id, kind, principal, auth_profile_ref, token_vault_ref,
                   egress_allowlist_json, enabled, readiness, liveness, restart_count,
                   last_error, last_inbound_unix_ms, last_outbound_unix_ms,
                   created_at_unix_ms, updated_at_unix_ms
            FROM connector_instances
            ORDER BY connector_id ASC
            "#,
        )?;
        let mut rows = statement.query([])?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(parse_instance_row(row)?);
        }
        Ok(records)
    }

    /// Fetches one instance; `Ok(None)` when the connector id is unknown.
    ///
    /// # Errors
    /// Returns a storage error when the query fails or the row holds unknown
    /// kind/readiness/liveness labels.
    pub fn get_instance(
        &self,
        connector_id: &str,
    ) -> Result<Option<ConnectorInstanceRecord>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let mut statement = connection.prepare(
            r#"
            SELECT connector_id, kind, principal, auth_profile_ref, token_vault_ref,
                   egress_allowlist_json, enabled, readiness, liveness, restart_count,
                   last_error, last_inbound_unix_ms, last_outbound_unix_ms,
                   created_at_unix_ms, updated_at_unix_ms
            FROM connector_instances
            WHERE connector_id = ?1
            "#,
        )?;
        let mut rows = statement.query(params![connector_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(parse_instance_row(row)?))
        } else {
            Ok(None)
        }
    }

    /// Sets the enabled flag, switching liveness to running/stopped to match.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::NotFound`] for unknown connector ids.
    pub fn set_instance_enabled(
        &self,
        connector_id: &str,
        enabled: bool,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE connector_instances
                SET enabled = ?2,
                    liveness = ?3,
                    updated_at_unix_ms = ?4
                WHERE connector_id = ?1
                "#,
                params![
                    connector_id,
                    if enabled { 1_i64 } else { 0_i64 },
                    if enabled {
                        ConnectorLiveness::Running.as_str()
                    } else {
                        ConnectorLiveness::Stopped.as_str()
                    },
                    now_unix_ms,
                ],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    /// Deletes the instance plus its dedupe and outbox rows in one transaction.
    ///
    /// Dead letters and events are intentionally retained for post-removal
    /// auditability.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::NotFound`] for unknown connector ids.
    pub fn delete_instance(&self, connector_id: &str) -> Result<(), ConnectorStoreError> {
        let deleted = self.with_transaction(|transaction| {
            transaction.execute(
                "DELETE FROM inbound_dedupe WHERE connector_id = ?1",
                params![connector_id],
            )?;
            transaction
                .execute("DELETE FROM outbox WHERE connector_id = ?1", params![connector_id])?;
            let changed = transaction.execute(
                "DELETE FROM connector_instances WHERE connector_id = ?1",
                params![connector_id],
            )?;
            Ok(changed)
        })?;
        if deleted == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    /// Overwrites readiness, liveness, and last error in one update.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::NotFound`] for unknown connector ids.
    pub fn set_instance_runtime_state(
        &self,
        connector_id: &str,
        readiness: ConnectorReadiness,
        liveness: ConnectorLiveness,
        last_error: Option<&str>,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE connector_instances
                SET readiness = ?2,
                    liveness = ?3,
                    last_error = ?4,
                    updated_at_unix_ms = ?5
                WHERE connector_id = ?1
                "#,
                params![
                    connector_id,
                    readiness.as_str(),
                    liveness.as_str(),
                    last_error,
                    now_unix_ms
                ],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    /// Stamps the last accepted inbound time.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::NotFound`] for unknown connector ids.
    pub fn record_last_inbound(
        &self,
        connector_id: &str,
        at_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE connector_instances
                SET last_inbound_unix_ms = ?2,
                    updated_at_unix_ms = ?2
                WHERE connector_id = ?1
                "#,
                params![connector_id, at_unix_ms],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    /// Stamps the last successful delivery and resets the instance to a
    /// healthy ready/running state, clearing any stored error.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::NotFound`] for unknown connector ids.
    pub fn record_last_outbound(
        &self,
        connector_id: &str,
        at_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE connector_instances
                SET last_outbound_unix_ms = ?2,
                    last_error = NULL,
                    readiness = ?3,
                    liveness = ?4,
                    updated_at_unix_ms = ?2
                WHERE connector_id = ?1
                "#,
                params![
                    connector_id,
                    at_unix_ms,
                    ConnectorReadiness::Ready.as_str(),
                    ConnectorLiveness::Running.as_str(),
                ],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    /// Bumps the restart counter and marks the instance as restarting.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::NotFound`] for unknown connector ids.
    pub fn increment_restart_count(
        &self,
        connector_id: &str,
        now_unix_ms: i64,
        last_error: &str,
    ) -> Result<(), ConnectorStoreError> {
        let updated = self.with_transaction(|transaction| {
            let changed = transaction.execute(
                r#"
                UPDATE connector_instances
                SET restart_count = restart_count + 1,
                    liveness = ?2,
                    last_error = ?3,
                    updated_at_unix_ms = ?4
                WHERE connector_id = ?1
                "#,
                params![
                    connector_id,
                    ConnectorLiveness::Restarting.as_str(),
                    last_error,
                    now_unix_ms,
                ],
            )?;
            Ok(changed)
        })?;
        if updated == 0 {
            return Err(ConnectorStoreError::NotFound(connector_id.to_owned()));
        }
        Ok(())
    }

    /// Atomically records an inbound envelope id, returning `true` when it was
    /// new and `false` when it is a duplicate inside the dedupe window.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::Sqlite`] when the transaction fails.
    pub fn record_inbound_dedupe_if_new(
        &self,
        connector_id: &str,
        envelope_id: &str,
        now_unix_ms: i64,
        dedupe_window_ms: i64,
    ) -> Result<bool, ConnectorStoreError> {
        self.with_transaction(|transaction| {
            // Expired entries are purged opportunistically on every check so
            // the table stays bounded without a separate sweeper task.
            transaction.execute(
                "DELETE FROM inbound_dedupe WHERE expires_at_unix_ms <= ?1",
                params![now_unix_ms],
            )?;
            let inserted = transaction.execute(
                r#"
                INSERT OR IGNORE INTO inbound_dedupe (
                    connector_id, envelope_id, created_at_unix_ms, expires_at_unix_ms
                )
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![
                    connector_id,
                    envelope_id,
                    now_unix_ms,
                    now_unix_ms.saturating_add(dedupe_window_ms.max(1)),
                ],
            )?;
            Ok(inserted > 0)
        })
    }
}
