//! Exact durable MCP lifecycle projection for startup and degraded states.

pub(super) const SQL: &str = r#"
    ALTER TABLE mcp_server_records_v2
        ADD COLUMN runtime_lifecycle TEXT NOT NULL DEFAULT 'configured'
        CHECK (
            runtime_lifecycle IN (
                'configured',
                'starting',
                'handshaking',
                'ready',
                'degraded',
                'reconnecting',
                'stopping',
                'stopped',
                'quarantined',
                'disabled'
            )
        );

    UPDATE mcp_server_records_v2
    SET runtime_lifecycle = lifecycle;

    DROP INDEX idx_mcp_server_records_lifecycle;
    CREATE INDEX idx_mcp_server_records_lifecycle
        ON mcp_server_records_v2(runtime_lifecycle, server_id);
"#;
