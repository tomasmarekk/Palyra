//! Claim generations, liveness identity, side-effect fences, and orphan evidence.

pub(super) const SQL: &str = r#"
    ALTER TABLE work_graph_items ADD COLUMN claim_token_sha256 TEXT;
    ALTER TABLE work_graph_items ADD COLUMN claim_worker_id TEXT;
    ALTER TABLE work_graph_items ADD COLUMN claim_worker_principal TEXT;
    ALTER TABLE work_graph_items ADD COLUMN claim_generation INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE work_graph_items ADD COLUMN claim_attempt_ulid TEXT;
    ALTER TABLE work_graph_items ADD COLUMN claim_runtime_instance_id TEXT;
    ALTER TABLE work_graph_items ADD COLUMN claim_process_start_token TEXT;
    ALTER TABLE work_graph_items ADD COLUMN claim_issued_at_unix_ms INTEGER;
    ALTER TABLE work_graph_items ADD COLUMN claim_expires_at_unix_ms INTEGER;
    ALTER TABLE work_graph_items ADD COLUMN claim_heartbeat_at_unix_ms INTEGER;
    ALTER TABLE work_graph_items
        ADD COLUMN side_effect_fence_state TEXT NOT NULL DEFAULT 'clear';
    ALTER TABLE work_graph_items ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;

    CREATE INDEX idx_work_graph_items_claim_expiry
        ON work_graph_items(state, claim_expires_at_unix_ms);

    CREATE TABLE work_graph_orphan_results (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        orphan_ulid TEXT NOT NULL UNIQUE,
        graph_ulid TEXT NOT NULL,
        work_item_ulid TEXT NOT NULL,
        observed_generation INTEGER NOT NULL,
        active_generation INTEGER,
        worker_id TEXT NOT NULL,
        result_sha256 TEXT NOT NULL,
        target_state TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(graph_ulid, work_item_ulid)
            REFERENCES work_graph_items(graph_ulid, work_item_ulid) ON DELETE RESTRICT
    );
    CREATE INDEX idx_work_graph_orphan_results_item
        ON work_graph_orphan_results(graph_ulid, work_item_ulid, seq DESC);
    CREATE TRIGGER trg_work_graph_orphan_results_prevent_update
    BEFORE UPDATE ON work_graph_orphan_results
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_orphan_results is append-only');
    END;
    CREATE TRIGGER trg_work_graph_orphan_results_prevent_delete
    BEFORE DELETE ON work_graph_orphan_results
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_orphan_results is append-only');
    END;
"#;
