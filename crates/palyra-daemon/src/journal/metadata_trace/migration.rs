//! SQLite schema for the append-only metadata trace ledger.
//!
//! The migration is kept separate from reader/writer logic so its immutable
//! historical SQL remains easy to audit.

/// Migration adding append-only metadata trace segments, events, and status history.
pub(super) const SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS metadata_trace_segments (
        segment_ulid TEXT PRIMARY KEY,
        run_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        segment_index INTEGER NOT NULL
            CHECK (segment_index >= 0 AND segment_index < 16),
        generation INTEGER NOT NULL
            CHECK (generation > 0),
        predecessor_segment_ulid TEXT,
        schema_version INTEGER NOT NULL
            CHECK (schema_version = 1),
        opened_at_unix_ms INTEGER NOT NULL,
        UNIQUE(run_ulid, segment_index),
        FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(predecessor_segment_ulid) REFERENCES metadata_trace_segments(segment_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_metadata_trace_segments_run
        ON metadata_trace_segments(run_ulid, segment_index ASC);

    CREATE TABLE IF NOT EXISTS metadata_trace_events (
        event_id_sha256 TEXT PRIMARY KEY,
        run_ulid TEXT NOT NULL,
        segment_ulid TEXT NOT NULL,
        sequence INTEGER NOT NULL
            CHECK (sequence >= 0 AND sequence < 512),
        generation INTEGER NOT NULL
            CHECK (generation > 0),
        causal_parent_event_id_sha256 TEXT,
        event_kind TEXT NOT NULL,
        event_json TEXT NOT NULL,
        recorded_at_unix_ms INTEGER NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        UNIQUE(run_ulid, sequence),
        FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(segment_ulid) REFERENCES metadata_trace_segments(segment_ulid),
        FOREIGN KEY(causal_parent_event_id_sha256)
            REFERENCES metadata_trace_events(event_id_sha256)
    );
    CREATE INDEX IF NOT EXISTS idx_metadata_trace_events_run
        ON metadata_trace_events(run_ulid, sequence ASC);
    CREATE INDEX IF NOT EXISTS idx_metadata_trace_events_segment
        ON metadata_trace_events(segment_ulid, sequence ASC);

    CREATE TABLE IF NOT EXISTS metadata_trace_segment_status_events (
        status_event_ulid TEXT PRIMARY KEY,
        run_ulid TEXT NOT NULL,
        segment_ulid TEXT NOT NULL,
        status_ordinal INTEGER NOT NULL
            CHECK (status_ordinal >= 0),
        status TEXT NOT NULL
            CHECK (status IN ('complete', 'interrupted', 'corrupt_suffix_isolated')),
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        UNIQUE(segment_ulid, status_ordinal),
        FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(segment_ulid) REFERENCES metadata_trace_segments(segment_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_metadata_trace_status_segment
        ON metadata_trace_segment_status_events(segment_ulid, status_ordinal ASC);
    CREATE INDEX IF NOT EXISTS idx_metadata_trace_status_run
        ON metadata_trace_segment_status_events(run_ulid, created_at_unix_ms ASC);

    CREATE TRIGGER IF NOT EXISTS trg_metadata_trace_segments_prevent_update
    BEFORE UPDATE ON metadata_trace_segments
    BEGIN
        SELECT RAISE(ABORT, 'metadata_trace_segments is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_metadata_trace_segments_prevent_delete
    BEFORE DELETE ON metadata_trace_segments
    BEGIN
        SELECT RAISE(ABORT, 'metadata_trace_segments is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_metadata_trace_events_prevent_update
    BEFORE UPDATE ON metadata_trace_events
    BEGIN
        SELECT RAISE(ABORT, 'metadata_trace_events is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_metadata_trace_events_prevent_delete
    BEFORE DELETE ON metadata_trace_events
    BEGIN
        SELECT RAISE(ABORT, 'metadata_trace_events is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_metadata_trace_status_prevent_update
    BEFORE UPDATE ON metadata_trace_segment_status_events
    BEGIN
        SELECT RAISE(ABORT, 'metadata_trace_segment_status_events is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_metadata_trace_status_prevent_delete
    BEFORE DELETE ON metadata_trace_segment_status_events
    BEGIN
        SELECT RAISE(ABORT, 'metadata_trace_segment_status_events is append-only');
    END;
"#;
