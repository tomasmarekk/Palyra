//! Durable schema for host-authoritative work graphs.

pub(super) const SQL: &str = r#"
    CREATE TABLE work_graphs (
        graph_ulid TEXT PRIMARY KEY,
        schema_version INTEGER NOT NULL,
        owner_principal TEXT NOT NULL,
        device_id TEXT NOT NULL,
        channel TEXT,
        session_ulid TEXT,
        origin_run_ulid TEXT,
        objective_id TEXT,
        routine_id TEXT,
        flow_ulid TEXT,
        flow_step_id TEXT,
        state TEXT NOT NULL,
        budget_json TEXT NOT NULL,
        revision INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        completed_at_unix_ms INTEGER
    );
    CREATE INDEX idx_work_graphs_owner_updated
        ON work_graphs(owner_principal, updated_at_unix_ms DESC);
    CREATE INDEX idx_work_graphs_flow_step
        ON work_graphs(flow_ulid, flow_step_id);

    CREATE TABLE work_graph_items (
        graph_ulid TEXT NOT NULL,
        work_item_ulid TEXT NOT NULL,
        schema_version INTEGER NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        state TEXT NOT NULL,
        priority INTEGER NOT NULL,
        capability_profile TEXT NOT NULL,
        dependencies_json TEXT NOT NULL,
        compensates_work_item_ulid TEXT,
        serialization_key TEXT,
        resource_class TEXT NOT NULL,
        provider_profile TEXT,
        workspace_scope TEXT,
        budget_json TEXT NOT NULL,
        max_runtime_ms INTEGER NOT NULL,
        requires_review INTEGER NOT NULL,
        verification_state TEXT NOT NULL,
        revision INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        artifact_refs_json TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        completed_at_unix_ms INTEGER,
        PRIMARY KEY(graph_ulid, work_item_ulid),
        FOREIGN KEY(graph_ulid) REFERENCES work_graphs(graph_ulid) ON DELETE RESTRICT
    );
    CREATE INDEX idx_work_graph_items_eligible
        ON work_graph_items(graph_ulid, state, priority DESC, created_at_unix_ms);

    CREATE TABLE work_graph_events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        event_ulid TEXT NOT NULL UNIQUE,
        graph_ulid TEXT NOT NULL,
        work_item_ulid TEXT,
        graph_revision INTEGER NOT NULL,
        item_revision INTEGER,
        event_type TEXT NOT NULL,
        actor_principal TEXT NOT NULL,
        from_state TEXT,
        to_state TEXT,
        reason_code TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(graph_ulid) REFERENCES work_graphs(graph_ulid) ON DELETE RESTRICT
    );
    CREATE INDEX idx_work_graph_events_graph_seq
        ON work_graph_events(graph_ulid, seq);
    CREATE TRIGGER trg_work_graph_events_prevent_update
    BEFORE UPDATE ON work_graph_events
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_events is append-only');
    END;
    CREATE TRIGGER trg_work_graph_events_prevent_delete
    BEFORE DELETE ON work_graph_events
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_events is append-only');
    END;
"#;
