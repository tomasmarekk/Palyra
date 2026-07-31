//! Restart-safe MCP catalog epoch and host-policy evidence.

pub(super) const SQL: &str = r#"
    CREATE TABLE mcp_catalog_epoch_evidence_v1 (
        server_id TEXT NOT NULL,
        runtime_generation INTEGER NOT NULL,
        catalog_epoch INTEGER NOT NULL,
        catalog_digest TEXT NOT NULL,
        record_revision INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        PRIMARY KEY(server_id, catalog_epoch),
        FOREIGN KEY(server_id)
            REFERENCES mcp_server_records_v2(server_id)
            ON DELETE RESTRICT,
        CHECK (runtime_generation > 0),
        CHECK (catalog_epoch > 0),
        CHECK (record_revision > 0)
    );
    CREATE INDEX idx_mcp_catalog_epoch_generation
        ON mcp_catalog_epoch_evidence_v1(
            server_id,
            runtime_generation,
            catalog_epoch
        );
    CREATE TRIGGER trg_mcp_catalog_epoch_evidence_prevent_update
    BEFORE UPDATE ON mcp_catalog_epoch_evidence_v1
    BEGIN
        SELECT RAISE(ABORT, 'mcp_catalog_epoch_evidence_v1 is append-only');
    END;
    CREATE TRIGGER trg_mcp_catalog_epoch_evidence_prevent_delete
    BEFORE DELETE ON mcp_catalog_epoch_evidence_v1
    BEGIN
        SELECT RAISE(ABORT, 'mcp_catalog_epoch_evidence_v1 is append-only');
    END;

    CREATE TABLE mcp_host_policy_events_v1 (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE,
        server_id TEXT NOT NULL,
        runtime_generation INTEGER NOT NULL,
        catalog_epoch INTEGER NOT NULL,
        binding_sha256 TEXT NOT NULL,
        kind TEXT NOT NULL,
        outcome TEXT NOT NULL,
        reserved_output_tokens INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        evidence_sha256 TEXT,
        occurred_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(server_id)
            REFERENCES mcp_server_records_v2(server_id)
            ON DELETE RESTRICT,
        CHECK (runtime_generation > 0),
        CHECK (catalog_epoch >= 0),
        CHECK (reserved_output_tokens >= 0),
        CHECK (kind IN ('oauth_refresh', 'elicitation', 'sampling', 'roots')),
        CHECK (outcome IN ('allowed', 'denied', 'refreshed', 'failed'))
    );
    CREATE INDEX idx_mcp_policy_sampling_window
        ON mcp_host_policy_events_v1(
            server_id,
            binding_sha256,
            kind,
            outcome,
            occurred_at_unix_ms
        );
    CREATE TRIGGER trg_mcp_host_policy_events_prevent_update
    BEFORE UPDATE ON mcp_host_policy_events_v1
    BEGIN
        SELECT RAISE(ABORT, 'mcp_host_policy_events_v1 is append-only');
    END;
    CREATE TRIGGER trg_mcp_host_policy_events_prevent_delete
    BEFORE DELETE ON mcp_host_policy_events_v1
    BEGIN
        SELECT RAISE(ABORT, 'mcp_host_policy_events_v1 is append-only');
    END;
"#;
