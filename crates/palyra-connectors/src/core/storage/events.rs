//! Append-only connector event log used for operator logs and derived
//! runtime metrics.
//!
//! Event type strings double as metric keys (see `supervisor::metrics`), so
//! existing values must stay stable.

use rusqlite::params;
use serde_json::Value;

use super::records::parse_event_row;
use super::{ConnectorEventRecord, ConnectorStore, ConnectorStoreError};

impl ConnectorStore {
    /// Appends one event with optional structured details.
    ///
    /// # Errors
    /// Returns [`ConnectorStoreError::Serde`] when `details` cannot be encoded
    /// and [`ConnectorStoreError::Sqlite`] when the insert fails.
    pub fn record_event(
        &self,
        connector_id: &str,
        event_type: &str,
        level: &str,
        message: &str,
        details: Option<&Value>,
        now_unix_ms: i64,
    ) -> Result<(), ConnectorStoreError> {
        let encoded_details = details.map(serde_json::to_string).transpose()?;
        self.with_transaction(|transaction| {
            transaction.execute(
                r#"
                INSERT INTO connector_events (
                    connector_id, event_type, level, message, details_json, created_at_unix_ms
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                params![connector_id, event_type, level, message, encoded_details, now_unix_ms,],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    /// Lists up to `limit` events for a connector, newest first.
    ///
    /// # Errors
    /// Returns a storage error when the query fails or stored details no
    /// longer decode as JSON.
    pub fn list_events(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<Vec<ConnectorEventRecord>, ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        let limit_i64 = i64::try_from(limit)
            .map_err(|_| ConnectorStoreError::ValueOverflow { field: "limit" })?;
        let mut statement = connection.prepare(
            r#"
            SELECT event_id, connector_id, event_type, level, message, details_json, created_at_unix_ms
            FROM connector_events
            WHERE connector_id = ?1
            ORDER BY event_id DESC
            LIMIT ?2
            "#,
        )?;
        let mut rows = statement.query(params![connector_id, limit_i64])?;
        let mut events = Vec::new();
        while let Some(row) = rows.next()? {
            events.push(parse_event_row(row)?);
        }
        Ok(events)
    }
}
