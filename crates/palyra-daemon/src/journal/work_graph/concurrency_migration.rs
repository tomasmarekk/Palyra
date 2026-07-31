//! WorkGraph concurrency policy, failure circuits, and resource-lease references.

pub(super) const SQL: &str = r#"
    ALTER TABLE work_graphs
        ADD COLUMN concurrency_policy_json TEXT NOT NULL DEFAULT
        '{"max_active_items":16,"max_active_per_profile":{},"max_active_per_provider":{},"max_workspace_readers_per_scope":8,"failure_limit":1,"retry_backoff_base_ms":1000,"retry_backoff_max_ms":60000,"cancel_settle_timeout_ms":5000}';

    ALTER TABLE work_graph_items
        ADD COLUMN consecutive_failure_count INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE work_graph_items
        ADD COLUMN failure_limit INTEGER NOT NULL DEFAULT 1;
    ALTER TABLE work_graph_items ADD COLUMN retry_not_before_unix_ms INTEGER;
    ALTER TABLE work_graph_items ADD COLUMN circuit_opened_at_unix_ms INTEGER;
    ALTER TABLE work_graph_items ADD COLUMN failure_reason_code TEXT;
    ALTER TABLE work_graph_items ADD COLUMN resource_lease_id TEXT;

    CREATE INDEX idx_work_graph_items_concurrency
        ON work_graph_items(graph_ulid, state, capability_profile, provider_profile);
    CREATE INDEX idx_work_graph_items_workspace
        ON work_graph_items(graph_ulid, state, workspace_scope, resource_class);
"#;
