//! SQLite schema for typed wait barriers, source events, and wake intents.
//!
//! The behavior lives in the parent module; this file only keeps the durable
//! schema independently reviewable and below the repository module budget.

pub(super) const MIGRATION_88_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS wait_barriers_v1 (
        barrier_ulid TEXT PRIMARY KEY,
        owner_kind TEXT NOT NULL,
        owner_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        root_run_ulid TEXT,
        barrier_kind TEXT NOT NULL CHECK (
            barrier_kind IN (
                'process_session', 'terminal_pid', 'time_deadline', 'approval',
                'webhook', 'flow_step', 'delegation_child', 'background_task',
                'external_artifact', 'user_input'
            )
        ),
        source_kind TEXT NOT NULL,
        source_id TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('active', 'satisfied', 'expired', 'cancelled')),
        wake_decision TEXT NOT NULL CHECK (
            wake_decision IN ('run', 'defer', 'coalesce', 'cancel', 'delivery_only')
        ),
        continuation_prompt TEXT,
        budget_tokens INTEGER NOT NULL CHECK (budget_tokens >= 0),
        attempt_generation INTEGER NOT NULL CHECK (attempt_generation >= 1),
        wake_at_unix_ms INTEGER,
        expires_at_unix_ms INTEGER,
        liveness_probe_json TEXT NOT NULL,
        active_hours_json TEXT,
        stale_policy TEXT NOT NULL CHECK (stale_policy IN ('cancel', 'wake', 'defer')),
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        UNIQUE(owner_kind, owner_ulid, barrier_kind, source_kind, source_id, attempt_generation),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(root_run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_wait_barriers_source
        ON wait_barriers_v1(state, source_kind, source_id);
    CREATE INDEX IF NOT EXISTS idx_wait_barriers_deadline
        ON wait_barriers_v1(state, wake_at_unix_ms, expires_at_unix_ms);

    CREATE TABLE IF NOT EXISTS wake_source_events_v1 (
        source_event_ulid TEXT PRIMARY KEY,
        source_kind TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_generation INTEGER NOT NULL CHECK (source_generation >= 1),
        reason_code TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        occurred_at_unix_ms INTEGER NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        UNIQUE(source_kind, source_id, source_generation, source_event_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_wake_source_events_source
        ON wake_source_events_v1(source_kind, source_id, source_generation);

    CREATE TABLE IF NOT EXISTS wake_intents_v1 (
        intent_ulid TEXT PRIMARY KEY,
        barrier_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        source_kind TEXT NOT NULL,
        source_id TEXT NOT NULL,
        source_generation INTEGER NOT NULL CHECK (source_generation >= 1),
        wake_reason TEXT NOT NULL,
        decision TEXT NOT NULL CHECK (
            decision IN ('run', 'defer', 'coalesce', 'cancel', 'delivery_only')
        ),
        state TEXT NOT NULL CHECK (
            state IN ('pending', 'deferred', 'task_reserved', 'delivered', 'cancelled', 'expired')
        ),
        attempt_generation INTEGER NOT NULL CHECK (attempt_generation >= 1),
        source_event_count INTEGER NOT NULL DEFAULT 1 CHECK (source_event_count >= 1),
        continuation_task_ulid TEXT UNIQUE,
        delivery_outcome TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        next_eligible_at_unix_ms INTEGER,
        first_event_at_unix_ms INTEGER NOT NULL,
        last_event_at_unix_ms INTEGER NOT NULL,
        delivered_at_unix_ms INTEGER,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        UNIQUE(barrier_ulid, attempt_generation),
        FOREIGN KEY(barrier_ulid) REFERENCES wait_barriers_v1(barrier_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_wake_intents_ready
        ON wake_intents_v1(state, next_eligible_at_unix_ms, updated_at_unix_ms);

    CREATE TABLE IF NOT EXISTS wait_barrier_events_v1 (
        event_ulid TEXT PRIMARY KEY,
        barrier_ulid TEXT NOT NULL,
        intent_ulid TEXT,
        event_type TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        source_kind TEXT NOT NULL,
        source_id TEXT NOT NULL,
        attempt_generation INTEGER NOT NULL CHECK (attempt_generation >= 1),
        evidence_json TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(barrier_ulid) REFERENCES wait_barriers_v1(barrier_ulid),
        FOREIGN KEY(intent_ulid) REFERENCES wake_intents_v1(intent_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_wait_barrier_events_barrier
        ON wait_barrier_events_v1(barrier_ulid, created_at_unix_ms);
"#;
