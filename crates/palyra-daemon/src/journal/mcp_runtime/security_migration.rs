//! Durable trusted MCP descriptor heads, events, and conformance reports.

pub(super) const SQL: &str = r#"
    CREATE TABLE mcp_trusted_tool_heads_v1 (
        server_id TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        schema_version INTEGER NOT NULL,
        runtime_generation INTEGER NOT NULL,
        catalog_epoch INTEGER NOT NULL,
        descriptor_json TEXT NOT NULL,
        descriptor_sha256 TEXT NOT NULL,
        verified_issuer_id TEXT NOT NULL,
        activation TEXT NOT NULL,
        approved_descriptor_sha256 TEXT,
        revision INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        PRIMARY KEY(server_id, tool_name),
        FOREIGN KEY(server_id)
            REFERENCES mcp_server_records_v2(server_id)
            ON DELETE RESTRICT,
        CHECK (schema_version = 1),
        CHECK (runtime_generation > 0),
        CHECK (catalog_epoch > 0),
        CHECK (revision >= 0),
        CHECK (activation IN ('pending_approval', 'active', 'disabled'))
    );
    CREATE INDEX idx_mcp_trusted_tool_activation
        ON mcp_trusted_tool_heads_v1(server_id, activation, tool_name);

    CREATE TABLE mcp_trusted_tool_events_v1 (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        server_id TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        previous_revision INTEGER,
        revision INTEGER NOT NULL,
        runtime_generation INTEGER NOT NULL,
        catalog_epoch INTEGER NOT NULL,
        descriptor_sha256 TEXT NOT NULL,
        activation TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        occurred_at_unix_ms INTEGER NOT NULL,
        UNIQUE(server_id, tool_name, revision),
        FOREIGN KEY(server_id, tool_name)
            REFERENCES mcp_trusted_tool_heads_v1(server_id, tool_name)
            ON DELETE RESTRICT,
        CHECK (revision >= 0),
        CHECK (
            (previous_revision IS NULL AND revision = 0)
            OR revision = previous_revision + 1
        )
    );
    CREATE INDEX idx_mcp_trusted_tool_events_server_seq
        ON mcp_trusted_tool_events_v1(server_id, seq);
    CREATE TRIGGER trg_mcp_trusted_tool_events_prevent_update
    BEFORE UPDATE ON mcp_trusted_tool_events_v1
    BEGIN
        SELECT RAISE(ABORT, 'mcp_trusted_tool_events_v1 is append-only');
    END;
    CREATE TRIGGER trg_mcp_trusted_tool_events_prevent_delete
    BEFORE DELETE ON mcp_trusted_tool_events_v1
    BEGIN
        SELECT RAISE(ABORT, 'mcp_trusted_tool_events_v1 is append-only');
    END;

    CREATE TABLE mcp_conformance_reports_v1 (
        report_sha256 TEXT PRIMARY KEY,
        server_id TEXT NOT NULL,
        transport TEXT NOT NULL,
        runtime_generation INTEGER NOT NULL,
        catalog_epoch INTEGER NOT NULL,
        qualifies_for_production INTEGER NOT NULL,
        report_json TEXT NOT NULL,
        started_at_unix_ms INTEGER NOT NULL,
        completed_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(server_id)
            REFERENCES mcp_server_records_v2(server_id)
            ON DELETE RESTRICT
    );
    CREATE INDEX idx_mcp_conformance_server_completed
        ON mcp_conformance_reports_v1(server_id, completed_at_unix_ms DESC);
    CREATE TRIGGER trg_mcp_conformance_reports_prevent_update
    BEFORE UPDATE ON mcp_conformance_reports_v1
    BEGIN
        SELECT RAISE(ABORT, 'mcp_conformance_reports_v1 is append-only');
    END;
    CREATE TRIGGER trg_mcp_conformance_reports_prevent_delete
    BEFORE DELETE ON mcp_conformance_reports_v1
    BEGIN
        SELECT RAISE(ABORT, 'mcp_conformance_reports_v1 is append-only');
    END;
"#;
