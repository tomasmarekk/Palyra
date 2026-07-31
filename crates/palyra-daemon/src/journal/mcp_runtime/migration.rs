//! Durable MCP runtime projections and append-only lifecycle evidence.

pub(super) const SQL: &str = r#"
    CREATE TABLE mcp_server_records_v2 (
        server_id TEXT PRIMARY KEY,
        schema_version INTEGER NOT NULL,
        transport TEXT NOT NULL,
        lifecycle TEXT NOT NULL,
        runtime_generation INTEGER NOT NULL,
        catalog_epoch INTEGER NOT NULL,
        catalog_digest TEXT,
        credential_scope_id TEXT,
        trust_profile_id TEXT NOT NULL,
        consecutive_failures INTEGER NOT NULL,
        next_retry_at_unix_ms INTEGER,
        quarantine_reason_code TEXT,
        revision INTEGER NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        CHECK (schema_version = 2),
        CHECK (transport IN ('stdio', 'streamable_http', 'server_sent_events')),
        CHECK (
            lifecycle IN (
                'configured',
                'handshaking',
                'ready',
                'reconnecting',
                'stopping',
                'stopped',
                'quarantined',
                'disabled'
            )
        ),
        CHECK (runtime_generation >= 0),
        CHECK (catalog_epoch >= 0),
        CHECK (consecutive_failures >= 0),
        CHECK (revision >= 0)
    );
    CREATE INDEX idx_mcp_server_records_lifecycle
        ON mcp_server_records_v2(lifecycle, server_id);

    CREATE TABLE mcp_connection_lifecycle_events_v2 (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        server_id TEXT NOT NULL,
        previous_revision INTEGER NOT NULL,
        revision INTEGER NOT NULL,
        previous_lifecycle TEXT NOT NULL,
        lifecycle TEXT NOT NULL,
        runtime_generation INTEGER NOT NULL,
        catalog_epoch INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        occurred_at_unix_ms INTEGER NOT NULL,
        UNIQUE(server_id, revision),
        FOREIGN KEY(server_id)
            REFERENCES mcp_server_records_v2(server_id)
            ON DELETE RESTRICT,
        CHECK (previous_revision >= 0),
        CHECK (revision = previous_revision + 1),
        CHECK (runtime_generation >= 0),
        CHECK (catalog_epoch >= 0)
    );
    CREATE INDEX idx_mcp_connection_events_server_seq
        ON mcp_connection_lifecycle_events_v2(server_id, seq);

    CREATE TRIGGER trg_mcp_connection_events_prevent_update
    BEFORE UPDATE ON mcp_connection_lifecycle_events_v2
    BEGIN
        SELECT RAISE(ABORT, 'mcp_connection_lifecycle_events_v2 is append-only');
    END;
    CREATE TRIGGER trg_mcp_connection_events_prevent_delete
    BEFORE DELETE ON mcp_connection_lifecycle_events_v2
    BEGIN
        SELECT RAISE(ABORT, 'mcp_connection_lifecycle_events_v2 is append-only');
    END;
"#;
