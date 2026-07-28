//! Schema for cross-run objective budgets, progress fingerprints, and plan
//! links. Append-only observation and reset tables preserve the evidence used
//! by replay-safe guard decisions.

pub(super) const SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS objective_budget_ledgers_v1 (
        objective_ulid TEXT PRIMARY KEY,
        session_ulid TEXT NOT NULL,
        root_run_ulid TEXT NOT NULL,
        max_runs INTEGER CHECK (max_runs IS NULL OR max_runs >= 0),
        max_turns INTEGER CHECK (max_turns IS NULL OR max_turns >= 0),
        max_provider_calls INTEGER CHECK (max_provider_calls IS NULL OR max_provider_calls >= 0),
        max_tokens INTEGER CHECK (max_tokens IS NULL OR max_tokens >= 0),
        max_cost_micros INTEGER CHECK (max_cost_micros IS NULL OR max_cost_micros >= 0),
        max_wall_time_ms INTEGER CHECK (max_wall_time_ms IS NULL OR max_wall_time_ms >= 0),
        runs_consumed INTEGER NOT NULL DEFAULT 0 CHECK (runs_consumed >= 0),
        turns_consumed INTEGER NOT NULL DEFAULT 0 CHECK (turns_consumed >= 0),
        provider_calls_consumed INTEGER NOT NULL DEFAULT 0 CHECK (provider_calls_consumed >= 0),
        tokens_consumed INTEGER NOT NULL DEFAULT 0 CHECK (tokens_consumed >= 0),
        cost_micros_consumed INTEGER NOT NULL DEFAULT 0 CHECK (cost_micros_consumed >= 0),
        wall_time_ms_consumed INTEGER NOT NULL DEFAULT 0 CHECK (wall_time_ms_consumed >= 0),
        parse_failures_total INTEGER NOT NULL DEFAULT 0 CHECK (parse_failures_total >= 0),
        consecutive_parse_failures INTEGER NOT NULL DEFAULT 0
            CHECK (consecutive_parse_failures >= 0),
        consecutive_no_progress INTEGER NOT NULL DEFAULT 0
            CHECK (consecutive_no_progress >= 0),
        consecutive_identical_plan INTEGER NOT NULL DEFAULT 0
            CHECK (consecutive_identical_plan >= 0),
        consecutive_tool_error INTEGER NOT NULL DEFAULT 0
            CHECK (consecutive_tool_error >= 0),
        verdict_oscillations INTEGER NOT NULL DEFAULT 0 CHECK (verdict_oscillations >= 0),
        progress_epoch INTEGER NOT NULL DEFAULT 0 CHECK (progress_epoch >= 0),
        progress_reset_count INTEGER NOT NULL DEFAULT 0 CHECK (progress_reset_count >= 0),
        last_progress_sha256 TEXT,
        last_plan_sha256 TEXT,
        last_tool_error_sha256 TEXT,
        previous_verdict TEXT CHECK (
            previous_verdict IS NULL OR previous_verdict IN (
                'pending', 'done', 'continue', 'wait', 'blocked', 'needs_user'
            )
        ),
        last_verdict TEXT CHECK (
            last_verdict IS NULL OR last_verdict IN (
                'pending', 'done', 'continue', 'wait', 'blocked', 'needs_user'
            )
        ),
        paused_reason_code TEXT,
        revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1),
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(objective_ulid)
            REFERENCES objective_runtime_bindings_v1(objective_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(root_run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_objective_budget_ledgers_session
        ON objective_budget_ledgers_v1(session_ulid, updated_at_unix_ms);
    CREATE INDEX IF NOT EXISTS idx_objective_budget_ledgers_pause
        ON objective_budget_ledgers_v1(paused_reason_code, updated_at_unix_ms);

    CREATE TABLE IF NOT EXISTS objective_progress_fingerprints_v1 (
        attempt_ulid TEXT PRIMARY KEY,
        objective_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        root_run_ulid TEXT NOT NULL,
        source_run_ulid TEXT NOT NULL,
        source_run_generation INTEGER NOT NULL CHECK (source_run_generation >= 1),
        request_sha256 TEXT NOT NULL,
        verdict TEXT NOT NULL CHECK (
            verdict IN ('pending', 'done', 'continue', 'wait', 'blocked', 'needs_user')
        ),
        progress_sha256 TEXT,
        plan_sha256 TEXT,
        tool_error_sha256 TEXT,
        progress_detected INTEGER NOT NULL CHECK (progress_detected IN (0, 1)),
        parse_failure INTEGER NOT NULL CHECK (parse_failure IN (0, 1)),
        verification_status TEXT NOT NULL CHECK (
            verification_status IN (
                'unknown',
                'not_required',
                'verified',
                'missing_evidence',
                'missing_artifacts',
                'failed'
            )
        ),
        verification_reason_code TEXT,
        verification_evidence_json TEXT NOT NULL,
        missing_artifacts_json TEXT NOT NULL,
        disposition TEXT NOT NULL CHECK (disposition IN ('proceed', 'pause')),
        reason_code TEXT NOT NULL,
        cumulative_runs INTEGER NOT NULL CHECK (cumulative_runs >= 0),
        cumulative_turns INTEGER NOT NULL CHECK (cumulative_turns >= 0),
        cumulative_provider_calls INTEGER NOT NULL CHECK (cumulative_provider_calls >= 0),
        cumulative_tokens INTEGER NOT NULL CHECK (cumulative_tokens >= 0),
        cumulative_cost_micros INTEGER NOT NULL CHECK (cumulative_cost_micros >= 0),
        cumulative_wall_time_ms INTEGER NOT NULL CHECK (cumulative_wall_time_ms >= 0),
        consecutive_parse_failures INTEGER NOT NULL CHECK (consecutive_parse_failures >= 0),
        consecutive_no_progress INTEGER NOT NULL CHECK (consecutive_no_progress >= 0),
        consecutive_identical_plan INTEGER NOT NULL CHECK (consecutive_identical_plan >= 0),
        consecutive_tool_error INTEGER NOT NULL CHECK (consecutive_tool_error >= 0),
        verdict_oscillations INTEGER NOT NULL CHECK (verdict_oscillations >= 0),
        progress_epoch INTEGER NOT NULL CHECK (progress_epoch >= 0),
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(attempt_ulid)
            REFERENCES objective_continuation_attempts_v1(attempt_ulid),
        FOREIGN KEY(objective_ulid)
            REFERENCES objective_runtime_bindings_v1(objective_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(root_run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(source_run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_objective_progress_fingerprints_objective
        ON objective_progress_fingerprints_v1(
            objective_ulid,
            created_at_unix_ms,
            attempt_ulid
        );
    CREATE INDEX IF NOT EXISTS idx_objective_progress_fingerprints_pause
        ON objective_progress_fingerprints_v1(
            disposition,
            reason_code,
            created_at_unix_ms
        );
    CREATE TRIGGER IF NOT EXISTS trg_objective_progress_fingerprints_prevent_update
    BEFORE UPDATE ON objective_progress_fingerprints_v1
    BEGIN
        SELECT RAISE(ABORT, 'objective_progress_fingerprints_v1 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_objective_progress_fingerprints_prevent_delete
    BEFORE DELETE ON objective_progress_fingerprints_v1
    BEGIN
        SELECT RAISE(ABORT, 'objective_progress_fingerprints_v1 is append-only');
    END;

    CREATE TABLE IF NOT EXISTS objective_progress_resets_v1 (
        reset_ulid TEXT PRIMARY KEY,
        objective_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        progress_epoch INTEGER NOT NULL CHECK (progress_epoch >= 1),
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(objective_ulid)
            REFERENCES objective_runtime_bindings_v1(objective_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_objective_progress_resets_session
        ON objective_progress_resets_v1(session_ulid, created_at_unix_ms);
    CREATE TRIGGER IF NOT EXISTS trg_objective_progress_resets_prevent_update
    BEFORE UPDATE ON objective_progress_resets_v1
    BEGIN
        SELECT RAISE(ABORT, 'objective_progress_resets_v1 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_objective_progress_resets_prevent_delete
    BEFORE DELETE ON objective_progress_resets_v1
    BEGIN
        SELECT RAISE(ABORT, 'objective_progress_resets_v1 is append-only');
    END;

    CREATE TABLE IF NOT EXISTS plan_objective_links_v1 (
        objective_ulid TEXT NOT NULL,
        plan_item_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        root_run_ulid TEXT NOT NULL,
        focus TEXT NOT NULL,
        is_root INTEGER NOT NULL CHECK (is_root IN (0, 1)),
        active INTEGER NOT NULL CHECK (active IN (0, 1)),
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        PRIMARY KEY(objective_ulid, plan_item_ulid),
        UNIQUE(plan_item_ulid),
        FOREIGN KEY(plan_item_ulid) REFERENCES agent_plan_items(plan_item_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(root_run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_plan_objective_links_active_root
        ON plan_objective_links_v1(objective_ulid)
        WHERE is_root = 1 AND active = 1;
    CREATE INDEX IF NOT EXISTS idx_plan_objective_links_session
        ON plan_objective_links_v1(session_ulid, active, updated_at_unix_ms);
    CREATE TRIGGER IF NOT EXISTS trg_plan_objective_links_prevent_delete
    BEFORE DELETE ON plan_objective_links_v1
    BEGIN
        SELECT RAISE(ABORT, 'plan_objective_links_v1 cannot be deleted');
    END;

    CREATE TABLE IF NOT EXISTS plan_objective_link_events_v1 (
        event_ulid TEXT PRIMARY KEY,
        objective_ulid TEXT NOT NULL,
        plan_item_ulid TEXT NOT NULL,
        event_type TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(objective_ulid, plan_item_ulid)
            REFERENCES plan_objective_links_v1(objective_ulid, plan_item_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_plan_objective_link_events_link
        ON plan_objective_link_events_v1(
            objective_ulid,
            plan_item_ulid,
            created_at_unix_ms
        );
    CREATE TRIGGER IF NOT EXISTS trg_plan_objective_link_events_prevent_update
    BEFORE UPDATE ON plan_objective_link_events_v1
    BEGIN
        SELECT RAISE(ABORT, 'plan_objective_link_events_v1 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_plan_objective_link_events_prevent_delete
    BEFORE DELETE ON plan_objective_link_events_v1
    BEGIN
        SELECT RAISE(ABORT, 'plan_objective_link_events_v1 is append-only');
    END;
"#;
