//! Bounded handoffs, comments, review decisions, and artifact references.

pub(super) const SQL: &str = r#"
    CREATE TABLE work_graph_handoffs (
        handoff_ulid TEXT PRIMARY KEY,
        graph_ulid TEXT NOT NULL,
        work_item_ulid TEXT NOT NULL,
        claim_generation INTEGER NOT NULL,
        worker_principal TEXT NOT NULL,
        summary TEXT NOT NULL,
        structured_result_json TEXT NOT NULL,
        context_cost_tokens INTEGER NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        artifact_refs_json TEXT NOT NULL,
        verification_state TEXT NOT NULL,
        provenance_sha256 TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(graph_ulid, work_item_ulid)
            REFERENCES work_graph_items(graph_ulid, work_item_ulid) ON DELETE RESTRICT
    );
    CREATE INDEX idx_work_graph_handoffs_item
        ON work_graph_handoffs(graph_ulid, work_item_ulid, created_at_unix_ms DESC);

    CREATE TABLE work_graph_comments (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        comment_ulid TEXT NOT NULL UNIQUE,
        graph_ulid TEXT NOT NULL,
        work_item_ulid TEXT NOT NULL,
        author_principal TEXT NOT NULL,
        body TEXT NOT NULL,
        provenance_sha256 TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(graph_ulid, work_item_ulid)
            REFERENCES work_graph_items(graph_ulid, work_item_ulid) ON DELETE RESTRICT
    );
    CREATE INDEX idx_work_graph_comments_item
        ON work_graph_comments(graph_ulid, work_item_ulid, seq ASC);

    CREATE TABLE work_graph_reviews (
        review_ulid TEXT PRIMARY KEY,
        graph_ulid TEXT NOT NULL,
        work_item_ulid TEXT NOT NULL,
        handoff_ulid TEXT NOT NULL,
        reviewer_principal TEXT NOT NULL,
        decision TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        provenance_sha256 TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(handoff_ulid) REFERENCES work_graph_handoffs(handoff_ulid) ON DELETE RESTRICT,
        FOREIGN KEY(graph_ulid, work_item_ulid)
            REFERENCES work_graph_items(graph_ulid, work_item_ulid) ON DELETE RESTRICT
    );
    CREATE INDEX idx_work_graph_reviews_item
        ON work_graph_reviews(graph_ulid, work_item_ulid, created_at_unix_ms DESC);

    CREATE TRIGGER trg_work_graph_handoffs_prevent_update
    BEFORE UPDATE ON work_graph_handoffs
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_handoffs is append-only');
    END;
    CREATE TRIGGER trg_work_graph_handoffs_prevent_delete
    BEFORE DELETE ON work_graph_handoffs
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_handoffs is append-only');
    END;
    CREATE TRIGGER trg_work_graph_comments_prevent_update
    BEFORE UPDATE ON work_graph_comments
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_comments is append-only');
    END;
    CREATE TRIGGER trg_work_graph_comments_prevent_delete
    BEFORE DELETE ON work_graph_comments
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_comments is append-only');
    END;
    CREATE TRIGGER trg_work_graph_reviews_prevent_update
    BEFORE UPDATE ON work_graph_reviews
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_reviews is append-only');
    END;
    CREATE TRIGGER trg_work_graph_reviews_prevent_delete
    BEFORE DELETE ON work_graph_reviews
    BEGIN
        SELECT RAISE(ABORT, 'work_graph_reviews is append-only');
    END;
"#;
