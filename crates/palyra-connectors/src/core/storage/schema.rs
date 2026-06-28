//! Sqlite schema creation and in-place column migrations for the connector
//! store.
//!
//! `initialize_schema` is idempotent and runs on every open; statements here
//! are the storage contract, so column or constraint changes need a matching
//! migration for databases created by older builds.

use rusqlite::Connection;

use super::{ConnectorStore, ConnectorStoreError};

impl ConnectorStore {
    /// Creates all tables/indexes if missing and applies column migrations.
    pub(super) fn initialize_schema(&self) -> Result<(), ConnectorStoreError> {
        let connection = self.connection.lock().map_err(|_| ConnectorStoreError::PoisonedLock)?;
        connection.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS connector_instances (
                connector_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                principal TEXT NOT NULL,
                auth_profile_ref TEXT,
                token_vault_ref TEXT,
                egress_allowlist_json TEXT NOT NULL,
                enabled INTEGER NOT NULL CHECK(enabled IN (0, 1)),
                readiness TEXT NOT NULL,
                liveness TEXT NOT NULL,
                restart_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                last_inbound_unix_ms INTEGER,
                last_outbound_unix_ms INTEGER,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS inbound_dedupe (
                connector_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                expires_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY(connector_id, envelope_id)
            );
            CREATE INDEX IF NOT EXISTS idx_inbound_dedupe_expiry
                ON inbound_dedupe(expires_at_unix_ms);

            CREATE TABLE IF NOT EXISTS channel_ingress_events (
                ingress_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_id TEXT NOT NULL,
                principal TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN (
                    'pending', 'claimed', 'retrying', 'completed', 'failed', 'quarantined'
                )),
                lane_key TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                next_attempt_unix_ms INTEGER NOT NULL,
                claim_token TEXT,
                claim_expires_unix_ms INTEGER NOT NULL DEFAULT 0,
                last_error_reason_code TEXT,
                last_error_message TEXT,
                route_key TEXT,
                session_id TEXT,
                run_id TEXT,
                completed_at_unix_ms INTEGER,
                tombstone_expires_at_unix_ms INTEGER,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                UNIQUE(connector_id, envelope_id)
            );
            CREATE INDEX IF NOT EXISTS idx_channel_ingress_due
                ON channel_ingress_events(status, next_attempt_unix_ms, ingress_event_id);
            CREATE INDEX IF NOT EXISTS idx_channel_ingress_claim_reclaim
                ON channel_ingress_events(status, claim_expires_unix_ms, next_attempt_unix_ms, ingress_event_id);
            CREATE INDEX IF NOT EXISTS idx_channel_ingress_lane
                ON channel_ingress_events(connector_id, lane_key, ingress_event_id);
            CREATE INDEX IF NOT EXISTS idx_channel_ingress_tombstone
                ON channel_ingress_events(status, tombstone_expires_at_unix_ms);

            CREATE TABLE IF NOT EXISTS outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                next_attempt_unix_ms INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('pending', 'delivered', 'dead')),
                native_message_id TEXT,
                last_error TEXT,
                claim_token TEXT,
                claim_expires_unix_ms INTEGER NOT NULL DEFAULT 0,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                UNIQUE(connector_id, envelope_id)
            );
            CREATE INDEX IF NOT EXISTS idx_outbox_pending
                ON outbox(status, next_attempt_unix_ms, outbox_id);

            CREATE TABLE IF NOT EXISTS delivery_intents (
                intent_id TEXT PRIMARY KEY,
                connector_id TEXT NOT NULL,
                ingress_event_id INTEGER NOT NULL,
                ingress_envelope_id TEXT NOT NULL,
                session_id TEXT,
                run_id TEXT,
                principal TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                output_index INTEGER NOT NULL,
                outbox_envelope_id TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                visible_text_preview TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN (
                    'created', 'planned', 'queued', 'adapter_send_started',
                    'platform_outcome_unknown', 'delivered', 'suppressed',
                    'failed', 'dead_lettered'
                )),
                send_attempts INTEGER NOT NULL DEFAULT 0,
                native_message_id TEXT,
                last_reason_code TEXT,
                redaction_summary_json TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                UNIQUE(connector_id, ingress_event_id, output_index, outbox_envelope_id)
            );
            CREATE INDEX IF NOT EXISTS idx_delivery_intents_connector
                ON delivery_intents(connector_id, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_delivery_intents_status
                ON delivery_intents(connector_id, status, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_delivery_intents_outbox
                ON delivery_intents(connector_id, outbox_envelope_id);

            CREATE TABLE IF NOT EXISTS delivery_transcript_mirror (
                intent_id TEXT PRIMARY KEY,
                connector_id TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                native_message_id TEXT NOT NULL,
                visible_text_hash TEXT NOT NULL,
                visible_text_preview TEXT NOT NULL,
                delivered_at_unix_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dead_letters (
                dead_letter_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_id TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_dead_letters_connector
                ON dead_letters(connector_id, dead_letter_id DESC);

            CREATE TABLE IF NOT EXISTS connector_queue_state (
                connector_id TEXT PRIMARY KEY,
                paused INTEGER NOT NULL CHECK(paused IN (0, 1)),
                pause_reason TEXT,
                updated_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(connector_id) REFERENCES connector_instances(connector_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS connector_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                details_json TEXT,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_connector_events_connector
                ON connector_events(connector_id, event_id DESC);
            "#,
        )?;
        // The reclaim index can only be created after the claim columns exist
        // on databases that predate the claim-lease migration.
        ensure_outbox_claim_columns(&connection)?;
        connection.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_outbox_claim_reclaim
                ON outbox(status, claim_expires_unix_ms, next_attempt_unix_ms, outbox_id);
            "#,
        )?;
        Ok(())
    }
}

/// Adds the claim-lease columns to `outbox` tables created before they existed.
fn ensure_outbox_claim_columns(connection: &Connection) -> Result<(), ConnectorStoreError> {
    if !outbox_column_exists(connection, "claim_token")? {
        connection.execute("ALTER TABLE outbox ADD COLUMN claim_token TEXT", [])?;
    }
    if !outbox_column_exists(connection, "claim_expires_unix_ms")? {
        connection.execute(
            "ALTER TABLE outbox ADD COLUMN claim_expires_unix_ms INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    Ok(())
}

fn outbox_column_exists(
    connection: &Connection,
    column_name: &str,
) -> Result<bool, ConnectorStoreError> {
    let mut statement = connection.prepare("PRAGMA table_info(outbox)")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name.eq_ignore_ascii_case(column_name) {
            return Ok(true);
        }
    }
    Ok(false)
}
