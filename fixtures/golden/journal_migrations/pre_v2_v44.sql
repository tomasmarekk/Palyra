-- Immutable Palyra journal baseline immediately before shared runtime V2 migrations.
-- Source schema version: 44. Update only with an intentional fixture/hash review.
PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at_unix_ms INTEGER NOT NULL
);

-- Migration 1: create_event_journal
CREATE TABLE IF NOT EXISTS journal_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_ulid TEXT NOT NULL UNIQUE,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                kind INTEGER NOT NULL,
                actor INTEGER NOT NULL,
                timestamp_unix_ms INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                redacted INTEGER NOT NULL,
                hash TEXT,
                prev_hash TEXT,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_journal_events_run_ts
                ON journal_events(run_ulid, timestamp_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_journal_events_created_at
                ON journal_events(created_at_unix_ms);
            CREATE TRIGGER IF NOT EXISTS trg_journal_events_prevent_update
            BEFORE UPDATE ON journal_events
            BEGIN
                SELECT RAISE(ABORT, 'journal_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_journal_events_prevent_delete
            BEFORE DELETE ON journal_events
            BEGIN
                SELECT RAISE(ABORT, 'journal_events is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (1, 'create_event_journal', 1000);

-- Migration 2: create_orchestrator_tables
CREATE TABLE IF NOT EXISTS orchestrator_sessions (
                session_ulid TEXT PRIMARY KEY,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orchestrator_runs (
                run_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                state TEXT NOT NULL,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                cancel_reason TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                started_at_unix_ms INTEGER NOT NULL,
                completed_at_unix_ms INTEGER,
                updated_at_unix_ms INTEGER NOT NULL,
                prompt_tokens INTEGER NOT NULL DEFAULT 0,
                completion_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_session
                ON orchestrator_runs(session_ulid);

            CREATE TABLE IF NOT EXISTS orchestrator_tape (
                run_ulid TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY(run_ulid, seq),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_tape_run_seq
                ON orchestrator_tape(run_ulid, seq);
            CREATE TRIGGER IF NOT EXISTS trg_orchestrator_tape_prevent_update
            BEFORE UPDATE ON orchestrator_tape
            BEGIN
                SELECT RAISE(ABORT, 'orchestrator_tape is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_orchestrator_tape_prevent_delete
            BEFORE DELETE ON orchestrator_tape
            BEGIN
                SELECT RAISE(ABORT, 'orchestrator_tape is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (2, 'create_orchestrator_tables', 2000);

-- Migration 3: orchestrator_session_keys_and_labels
ALTER TABLE orchestrator_sessions
                ADD COLUMN session_key TEXT;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN session_label TEXT;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN last_run_ulid TEXT;

            UPDATE orchestrator_sessions
            SET session_key = session_ulid
            WHERE session_key IS NULL OR TRIM(session_key) = '';

            CREATE UNIQUE INDEX IF NOT EXISTS idx_orchestrator_sessions_session_key
                ON orchestrator_sessions(session_key);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_sessions_session_label
                ON orchestrator_sessions(session_label);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (3, 'orchestrator_session_keys_and_labels', 3000);

-- Migration 4: create_cron_jobs_and_runs
CREATE TABLE IF NOT EXISTS cron_jobs (
                job_ulid TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                prompt TEXT NOT NULL,
                owner_principal TEXT NOT NULL,
                channel TEXT NOT NULL,
                session_key TEXT,
                session_label TEXT,
                schedule_type TEXT NOT NULL,
                schedule_payload_json TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                concurrency_policy TEXT NOT NULL,
                retry_policy_json TEXT NOT NULL,
                misfire_policy TEXT NOT NULL,
                jitter_ms INTEGER NOT NULL DEFAULT 0,
                next_run_at_unix_ms INTEGER,
                last_run_at_unix_ms INTEGER,
                queued_run INTEGER NOT NULL DEFAULT 0,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_cron_jobs_enabled_next_run
                ON cron_jobs(enabled, next_run_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_cron_jobs_owner
                ON cron_jobs(owner_principal);
            CREATE INDEX IF NOT EXISTS idx_cron_jobs_channel
                ON cron_jobs(channel);

            CREATE TABLE IF NOT EXISTS cron_runs (
                run_ulid TEXT PRIMARY KEY,
                job_ulid TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                session_ulid TEXT,
                orchestrator_run_ulid TEXT,
                started_at_unix_ms INTEGER NOT NULL,
                finished_at_unix_ms INTEGER,
                status TEXT NOT NULL,
                error_kind TEXT,
                error_message_redacted TEXT,
                model_tokens_in INTEGER NOT NULL DEFAULT 0,
                model_tokens_out INTEGER NOT NULL DEFAULT 0,
                tool_calls INTEGER NOT NULL DEFAULT 0,
                tool_denies INTEGER NOT NULL DEFAULT 0,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(job_ulid) REFERENCES cron_jobs(job_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_cron_runs_job_started
                ON cron_runs(job_ulid, started_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_cron_runs_job_status
                ON cron_runs(job_ulid, status);
            CREATE INDEX IF NOT EXISTS idx_cron_runs_started
                ON cron_runs(started_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (4, 'create_cron_jobs_and_runs', 4000);

-- Migration 5: create_approvals_table
CREATE TABLE IF NOT EXISTS approvals (
                approval_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                requested_at_unix_ms INTEGER NOT NULL,
                resolved_at_unix_ms INTEGER,
                subject_type TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                request_summary TEXT NOT NULL,
                decision TEXT,
                decision_scope TEXT,
                decision_reason TEXT,
                decision_scope_ttl_ms INTEGER,
                policy_snapshot_json TEXT NOT NULL,
                prompt_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_approvals_run
                ON approvals(run_ulid, requested_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_approvals_session
                ON approvals(session_ulid, requested_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_approvals_principal
                ON approvals(principal);
            CREATE INDEX IF NOT EXISTS idx_approvals_subject_id
                ON approvals(subject_id);
            CREATE INDEX IF NOT EXISTS idx_approvals_resolved
                ON approvals(resolved_at_unix_ms DESC, approval_ulid ASC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (5, 'create_approvals_table', 5000);

-- Migration 6: create_memory_tables
CREATE TABLE IF NOT EXISTS memory_items (
                memory_ulid TEXT PRIMARY KEY,
                principal TEXT NOT NULL,
                channel TEXT,
                session_ulid TEXT,
                source TEXT NOT NULL,
                content_text TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                confidence REAL,
                ttl_unix_ms INTEGER,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_memory_items_scope
                ON memory_items(principal, channel, session_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_memory_items_ttl
                ON memory_items(ttl_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_memory_items_source
                ON memory_items(source);

            CREATE VIRTUAL TABLE IF NOT EXISTS memory_items_fts
                USING fts5(memory_ulid UNINDEXED, content_text, tokenize='unicode61');
            CREATE TRIGGER IF NOT EXISTS trg_memory_items_ai
            AFTER INSERT ON memory_items
            BEGIN
                INSERT INTO memory_items_fts(memory_ulid, content_text)
                VALUES (new.memory_ulid, new.content_text);
            END;
            CREATE TRIGGER IF NOT EXISTS trg_memory_items_ad
            AFTER DELETE ON memory_items
            BEGIN
                DELETE FROM memory_items_fts WHERE memory_ulid = old.memory_ulid;
            END;

            CREATE TABLE IF NOT EXISTS memory_vectors (
                memory_ulid TEXT PRIMARY KEY,
                embedding_model TEXT NOT NULL,
                dims INTEGER NOT NULL,
                vector_blob BLOB NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(memory_ulid) REFERENCES memory_items(memory_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_memory_vectors_model
                ON memory_vectors(embedding_model);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (6, 'create_memory_tables', 6000);

-- Migration 7: create_skill_status_table
CREATE TABLE IF NOT EXISTS skill_status (
                skill_id TEXT NOT NULL,
                version TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT,
                detected_at_ms INTEGER NOT NULL,
                operator_principal TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                PRIMARY KEY(skill_id, version)
            );
            CREATE INDEX IF NOT EXISTS idx_skill_status_skill_detected
                ON skill_status(skill_id, detected_at_ms DESC, version DESC);
            CREATE INDEX IF NOT EXISTS idx_skill_status_state
                ON skill_status(status, detected_at_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (7, 'create_skill_status_table', 7000);

-- Migration 8: create_canvas_state_tables
CREATE TABLE IF NOT EXISTS canvas_state_snapshots (
                canvas_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                principal TEXT NOT NULL,
                state_version INTEGER NOT NULL,
                state_schema_version INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                bundle_json TEXT NOT NULL,
                allowed_parent_origins_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                expires_at_unix_ms INTEGER NOT NULL,
                closed INTEGER NOT NULL,
                close_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_canvas_state_snapshots_scope
                ON canvas_state_snapshots(principal, session_ulid, updated_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS canvas_state_patches (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                canvas_ulid TEXT NOT NULL,
                state_version INTEGER NOT NULL,
                base_state_version INTEGER NOT NULL,
                state_schema_version INTEGER NOT NULL,
                patch_json TEXT NOT NULL,
                resulting_state_json TEXT NOT NULL,
                closed INTEGER NOT NULL,
                close_reason TEXT,
                actor_principal TEXT NOT NULL,
                actor_device_id TEXT NOT NULL,
                applied_at_unix_ms INTEGER NOT NULL,
                UNIQUE(canvas_ulid, state_version)
            );
            CREATE INDEX IF NOT EXISTS idx_canvas_state_patches_canvas_version
                ON canvas_state_patches(canvas_ulid, state_version ASC);
            CREATE TRIGGER IF NOT EXISTS trg_canvas_state_patches_prevent_update
            BEFORE UPDATE ON canvas_state_patches
            BEGIN
                SELECT RAISE(ABORT, 'canvas_state_patches is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_canvas_state_patches_prevent_delete
            BEFORE DELETE ON canvas_state_patches
            BEGIN
                SELECT RAISE(ABORT, 'canvas_state_patches is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (8, 'create_canvas_state_tables', 8000);

-- Migration 9: create_memory_maintenance_state
CREATE TABLE IF NOT EXISTS memory_maintenance_state (
                singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
                last_run_at_unix_ms INTEGER,
                last_vacuum_at_unix_ms INTEGER,
                next_vacuum_due_at_unix_ms INTEGER,
                next_maintenance_run_at_unix_ms INTEGER,
                last_deleted_expired_count INTEGER NOT NULL DEFAULT 0,
                last_deleted_capacity_count INTEGER NOT NULL DEFAULT 0,
                last_deleted_total_count INTEGER NOT NULL DEFAULT 0,
                last_entries_before INTEGER NOT NULL DEFAULT 0,
                last_entries_after INTEGER NOT NULL DEFAULT 0,
                last_bytes_before INTEGER NOT NULL DEFAULT 0,
                last_bytes_after INTEGER NOT NULL DEFAULT 0,
                last_vacuum_performed INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO memory_maintenance_state(singleton_key)
            VALUES (1);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (9, 'create_memory_maintenance_state', 9000);

-- Migration 10: memory_vectors_add_provenance_columns
ALTER TABLE memory_vectors ADD COLUMN embedding_model_id TEXT;
            ALTER TABLE memory_vectors ADD COLUMN embedding_dims INTEGER;
            ALTER TABLE memory_vectors ADD COLUMN embedding_version INTEGER;
            ALTER TABLE memory_vectors ADD COLUMN embedding_vector BLOB;
            ALTER TABLE memory_vectors ADD COLUMN embedded_at_unix_ms INTEGER;
            UPDATE memory_vectors
            SET
                embedding_model_id = COALESCE(embedding_model_id, embedding_model),
                embedding_dims = COALESCE(embedding_dims, dims),
                embedding_version = COALESCE(embedding_version, 1),
                embedding_vector = COALESCE(embedding_vector, vector_blob),
                embedded_at_unix_ms = COALESCE(embedded_at_unix_ms, created_at_unix_ms)
            WHERE
                embedding_model_id IS NULL OR
                embedding_dims IS NULL OR
                embedding_version IS NULL OR
                embedding_vector IS NULL OR
                embedded_at_unix_ms IS NULL;
            CREATE INDEX IF NOT EXISTS idx_memory_vectors_model_version
                ON memory_vectors(embedding_model_id, embedding_version);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (10, 'memory_vectors_add_provenance_columns', 10000);

-- Migration 11: orchestrator_sessions_add_archived_at
ALTER TABLE orchestrator_sessions
                ADD COLUMN archived_at_unix_ms INTEGER;
            CREATE INDEX IF NOT EXISTS idx_orchestrator_sessions_archived_at
                ON orchestrator_sessions(archived_at_unix_ms);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (11, 'orchestrator_sessions_add_archived_at', 11000);

-- Migration 12: orchestrator_usage_indexes_v1
CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_started_at
                ON orchestrator_runs(started_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_session_started_at
                ON orchestrator_runs(session_ulid, started_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_sessions_scope_lookup
                ON orchestrator_sessions(principal, device_id, channel, archived_at_unix_ms);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (12, 'orchestrator_usage_indexes_v1', 12000);

-- Migration 13: orchestrator_sessions_add_title_metadata
ALTER TABLE orchestrator_sessions
                ADD COLUMN auto_title TEXT;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN auto_title_source TEXT;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN auto_title_generator_version TEXT;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (13, 'orchestrator_sessions_add_title_metadata', 13000);

-- Migration 14: orchestrator_session_lineage_and_run_metadata
ALTER TABLE orchestrator_sessions
                ADD COLUMN branch_state TEXT NOT NULL DEFAULT 'root';
            ALTER TABLE orchestrator_sessions
                ADD COLUMN parent_session_ulid TEXT;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN branch_origin_run_ulid TEXT;

            ALTER TABLE orchestrator_runs
                ADD COLUMN origin_kind TEXT NOT NULL DEFAULT 'manual';
            ALTER TABLE orchestrator_runs
                ADD COLUMN origin_run_ulid TEXT;
            ALTER TABLE orchestrator_runs
                ADD COLUMN triggered_by_principal TEXT;
            ALTER TABLE orchestrator_runs
                ADD COLUMN parameter_delta_json TEXT;

            CREATE INDEX IF NOT EXISTS idx_orchestrator_sessions_parent
                ON orchestrator_sessions(parent_session_ulid);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_origin
                ON orchestrator_runs(origin_run_ulid);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (14, 'orchestrator_session_lineage_and_run_metadata', 14000);

-- Migration 15: orchestrator_queue_and_pins
CREATE TABLE IF NOT EXISTS orchestrator_queued_inputs (
                queued_input_ulid TEXT PRIMARY KEY,
                run_ulid TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                state TEXT NOT NULL,
                text TEXT NOT NULL,
                origin_run_ulid TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_queued_inputs_run
                ON orchestrator_queued_inputs(run_ulid, created_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_queued_inputs_session
                ON orchestrator_queued_inputs(session_ulid, created_at_unix_ms);

            CREATE TABLE IF NOT EXISTS orchestrator_session_pins (
                pin_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                tape_seq INTEGER NOT NULL,
                title TEXT NOT NULL,
                note TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_session_pins_session
                ON orchestrator_session_pins(session_ulid, created_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (15, 'orchestrator_queue_and_pins', 15000);

-- Migration 16: workspace_documents_and_index
CREATE TABLE IF NOT EXISTS workspace_documents (
                document_ulid TEXT PRIMARY KEY,
                principal TEXT NOT NULL,
                channel TEXT,
                agent_id TEXT,
                latest_session_ulid TEXT,
                path TEXT NOT NULL,
                parent_path TEXT,
                title TEXT NOT NULL,
                kind TEXT NOT NULL,
                document_class TEXT NOT NULL,
                state TEXT NOT NULL,
                prompt_binding TEXT NOT NULL,
                risk_state TEXT NOT NULL,
                risk_reasons_json TEXT NOT NULL,
                pinned INTEGER NOT NULL DEFAULT 0,
                manual_override INTEGER NOT NULL DEFAULT 0,
                bootstrap_template_id TEXT,
                bootstrap_template_version INTEGER,
                bootstrap_template_hash TEXT,
                source_memory_ulid TEXT,
                latest_version INTEGER NOT NULL,
                content_text TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                last_recalled_at_unix_ms INTEGER,
                deleted_at_unix_ms INTEGER
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_documents_scope_path_active
                ON workspace_documents(
                    principal,
                    IFNULL(channel, ''),
                    IFNULL(agent_id, ''),
                    path
                )
                WHERE state = 'active';
            CREATE INDEX IF NOT EXISTS idx_workspace_documents_scope_updated
                ON workspace_documents(principal, channel, agent_id, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_workspace_documents_parent
                ON workspace_documents(principal, channel, agent_id, parent_path, updated_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS workspace_document_versions (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                document_ulid TEXT NOT NULL,
                version INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                path TEXT NOT NULL,
                previous_path TEXT,
                session_ulid TEXT,
                agent_id TEXT,
                source_memory_ulid TEXT,
                risk_state TEXT NOT NULL,
                risk_reasons_json TEXT NOT NULL,
                content_text TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                UNIQUE(document_ulid, version),
                FOREIGN KEY(document_ulid) REFERENCES workspace_documents(document_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_workspace_document_versions_document
                ON workspace_document_versions(document_ulid, version DESC);
            CREATE TRIGGER IF NOT EXISTS trg_workspace_document_versions_prevent_update
            BEFORE UPDATE ON workspace_document_versions
            BEGIN
                SELECT RAISE(ABORT, 'workspace_document_versions is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_workspace_document_versions_prevent_delete
            BEFORE DELETE ON workspace_document_versions
            BEGIN
                SELECT RAISE(ABORT, 'workspace_document_versions is append-only');
            END;

            CREATE TABLE IF NOT EXISTS workspace_document_chunks (
                chunk_ulid TEXT PRIMARY KEY,
                document_ulid TEXT NOT NULL,
                version INTEGER NOT NULL,
                principal TEXT NOT NULL,
                channel TEXT,
                agent_id TEXT,
                path TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL,
                content_text TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                risk_state TEXT NOT NULL,
                prompt_binding TEXT NOT NULL,
                is_latest INTEGER NOT NULL DEFAULT 1,
                created_at_unix_ms INTEGER NOT NULL,
                embedded_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(document_ulid) REFERENCES workspace_documents(document_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_workspace_document_chunks_scope
                ON workspace_document_chunks(principal, channel, agent_id, path, is_latest, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_workspace_document_chunks_document
                ON workspace_document_chunks(document_ulid, version DESC, chunk_index ASC);

            CREATE VIRTUAL TABLE IF NOT EXISTS workspace_document_chunks_fts
                USING fts5(chunk_ulid UNINDEXED, content_text, tokenize='unicode61');
            CREATE TRIGGER IF NOT EXISTS trg_workspace_document_chunks_ai
            AFTER INSERT ON workspace_document_chunks
            BEGIN
                INSERT INTO workspace_document_chunks_fts(chunk_ulid, content_text)
                VALUES (new.chunk_ulid, new.content_text);
            END;
            CREATE TRIGGER IF NOT EXISTS trg_workspace_document_chunks_ad
            AFTER DELETE ON workspace_document_chunks
            BEGIN
                DELETE FROM workspace_document_chunks_fts WHERE chunk_ulid = old.chunk_ulid;
            END;

            CREATE TABLE IF NOT EXISTS workspace_document_chunk_vectors (
                chunk_ulid TEXT PRIMARY KEY,
                embedding_model_id TEXT NOT NULL,
                embedding_dims INTEGER NOT NULL,
                embedding_version INTEGER NOT NULL,
                embedding_vector BLOB NOT NULL,
                embedded_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(chunk_ulid) REFERENCES workspace_document_chunks(chunk_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_workspace_document_chunk_vectors_model
                ON workspace_document_chunk_vectors(embedding_model_id, embedding_version);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (16, 'workspace_documents_and_index', 16000);

-- Migration 17: orchestrator_compaction_artifacts_and_background_tasks
CREATE TABLE IF NOT EXISTS orchestrator_compaction_artifacts (
                artifact_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT,
                mode TEXT NOT NULL,
                strategy TEXT NOT NULL,
                compressor_version TEXT NOT NULL,
                trigger_reason TEXT NOT NULL,
                trigger_policy TEXT,
                trigger_inputs_json TEXT,
                summary_text TEXT NOT NULL,
                summary_preview TEXT NOT NULL,
                source_event_count INTEGER NOT NULL,
                protected_event_count INTEGER NOT NULL,
                condensed_event_count INTEGER NOT NULL,
                omitted_event_count INTEGER NOT NULL,
                estimated_input_tokens INTEGER NOT NULL,
                estimated_output_tokens INTEGER NOT NULL,
                source_records_json TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                created_by_principal TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_compaction_artifacts_session
                ON orchestrator_compaction_artifacts(session_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_compaction_artifacts_run
                ON orchestrator_compaction_artifacts(run_ulid, created_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS orchestrator_checkpoints (
                checkpoint_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT,
                name TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                note TEXT,
                branch_state TEXT NOT NULL,
                parent_session_ulid TEXT,
                referenced_compaction_ids_json TEXT NOT NULL,
                workspace_paths_json TEXT NOT NULL,
                created_by_principal TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                restore_count INTEGER NOT NULL DEFAULT 0,
                last_restored_at_unix_ms INTEGER,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_checkpoints_session
                ON orchestrator_checkpoints(session_ulid, created_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS orchestrator_background_tasks (
                task_ulid TEXT PRIMARY KEY,
                task_kind TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                parent_run_ulid TEXT,
                target_run_ulid TEXT,
                queued_input_ulid TEXT,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                state TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 0,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                budget_tokens INTEGER NOT NULL DEFAULT 0,
                not_before_unix_ms INTEGER,
                expires_at_unix_ms INTEGER,
                notification_target_json TEXT,
                input_text TEXT,
                payload_json TEXT,
                last_error TEXT,
                result_json TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                started_at_unix_ms INTEGER,
                completed_at_unix_ms INTEGER,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(parent_run_ulid) REFERENCES orchestrator_runs(run_ulid),
                FOREIGN KEY(target_run_ulid) REFERENCES orchestrator_runs(run_ulid),
                FOREIGN KEY(queued_input_ulid) REFERENCES orchestrator_queued_inputs(queued_input_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_background_tasks_owner
                ON orchestrator_background_tasks(owner_principal, device_id, channel, state, priority DESC, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_background_tasks_session
                ON orchestrator_background_tasks(session_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_background_tasks_parent_run
                ON orchestrator_background_tasks(parent_run_ulid, state, created_at_unix_ms ASC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (17, 'orchestrator_compaction_artifacts_and_background_tasks', 17000);

-- Migration 18: usage_governance_and_pricing_catalog
CREATE TABLE IF NOT EXISTS usage_pricing_catalog (
                pricing_ulid TEXT PRIMARY KEY,
                provider_id TEXT NOT NULL,
                provider_kind TEXT NOT NULL,
                model_id TEXT NOT NULL,
                effective_from_unix_ms INTEGER NOT NULL,
                effective_to_unix_ms INTEGER,
                input_cost_per_million_usd REAL,
                output_cost_per_million_usd REAL,
                fixed_request_cost_usd REAL,
                source TEXT NOT NULL,
                precision TEXT NOT NULL,
                currency TEXT NOT NULL DEFAULT 'USD',
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_usage_pricing_lookup
                ON usage_pricing_catalog(provider_kind, provider_id, model_id, effective_from_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS usage_routing_decisions (
                decision_ulid TEXT PRIMARY KEY,
                run_ulid TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                scope_kind TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                mode TEXT NOT NULL,
                default_model_id TEXT NOT NULL,
                recommended_model_id TEXT NOT NULL,
                actual_model_id TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                provider_kind TEXT NOT NULL,
                complexity_score REAL NOT NULL,
                health_state TEXT NOT NULL,
                explanation_json TEXT NOT NULL,
                estimated_cost_lower_usd REAL,
                estimated_cost_upper_usd REAL,
                budget_outcome TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_usage_routing_decisions_run
                ON usage_routing_decisions(run_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_usage_routing_decisions_session
                ON usage_routing_decisions(session_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_usage_routing_decisions_scope
                ON usage_routing_decisions(scope_kind, scope_id, created_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS usage_budget_policies (
                policy_ulid TEXT PRIMARY KEY,
                scope_kind TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                metric_kind TEXT NOT NULL,
                interval_kind TEXT NOT NULL,
                soft_limit_value REAL,
                hard_limit_value REAL,
                action TEXT NOT NULL,
                routing_mode_override TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_by_principal TEXT NOT NULL,
                updated_by_principal TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_usage_budget_policies_scope
                ON usage_budget_policies(scope_kind, scope_id, enabled, updated_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS usage_alerts (
                alert_ulid TEXT PRIMARY KEY,
                alert_kind TEXT NOT NULL,
                severity TEXT NOT NULL,
                scope_kind TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                summary TEXT NOT NULL,
                reason TEXT NOT NULL,
                recommended_action TEXT NOT NULL,
                source TEXT NOT NULL,
                dedupe_key TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                first_observed_at_unix_ms INTEGER NOT NULL,
                last_observed_at_unix_ms INTEGER NOT NULL,
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                acknowledged_at_unix_ms INTEGER,
                resolved_at_unix_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_usage_alerts_active
                ON usage_alerts(resolved_at_unix_ms, severity, last_observed_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_usage_alerts_scope
                ON usage_alerts(scope_kind, scope_id, last_observed_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (18, 'usage_governance_and_pricing_catalog', 18000);

-- Migration 19: delegation_child_runs
ALTER TABLE orchestrator_runs
                ADD COLUMN parent_run_ulid TEXT;
            ALTER TABLE orchestrator_runs
                ADD COLUMN delegation_json TEXT;
            ALTER TABLE orchestrator_runs
                ADD COLUMN merge_result_json TEXT;
            CREATE INDEX IF NOT EXISTS idx_orchestrator_runs_parent
                ON orchestrator_runs(parent_run_ulid);

            ALTER TABLE orchestrator_background_tasks
                ADD COLUMN delegation_json TEXT;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (19, 'delegation_child_runs', 19000);

-- Migration 20: learning_loop
CREATE TABLE IF NOT EXISTS learning_candidates (
                candidate_ulid TEXT PRIMARY KEY,
                candidate_kind TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                scope_kind TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                status TEXT NOT NULL,
                auto_applied INTEGER NOT NULL DEFAULT 0,
                confidence REAL NOT NULL,
                risk_level TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                target_path TEXT,
                dedupe_key TEXT NOT NULL,
                content_json TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                source_task_ulid TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                reviewed_at_unix_ms INTEGER,
                reviewed_by_principal TEXT,
                last_action_summary TEXT,
                last_action_payload_json TEXT,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
                FOREIGN KEY(source_task_ulid) REFERENCES orchestrator_background_tasks(task_ulid)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_learning_candidates_dedupe
                ON learning_candidates(owner_principal, scope_kind, scope_id, candidate_kind, dedupe_key);
            CREATE INDEX IF NOT EXISTS idx_learning_candidates_queue
                ON learning_candidates(status, candidate_kind, confidence DESC, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_learning_candidates_session
                ON learning_candidates(session_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_learning_candidates_source_task
                ON learning_candidates(source_task_ulid, created_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS learning_candidate_history (
                history_ulid TEXT PRIMARY KEY,
                candidate_ulid TEXT NOT NULL,
                status TEXT NOT NULL,
                reviewed_by_principal TEXT NOT NULL,
                action_summary TEXT,
                action_payload_json TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(candidate_ulid) REFERENCES learning_candidates(candidate_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_learning_candidate_history_candidate
                ON learning_candidate_history(candidate_ulid, created_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS learning_preferences (
                preference_ulid TEXT PRIMARY KEY,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                scope_kind TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                preference_key TEXT NOT NULL,
                value_text TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                status TEXT NOT NULL,
                confidence REAL NOT NULL,
                candidate_ulid TEXT,
                provenance_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(candidate_ulid) REFERENCES learning_candidates(candidate_ulid)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_learning_preferences_scope_key
                ON learning_preferences(owner_principal, scope_kind, scope_id, preference_key);
            CREATE INDEX IF NOT EXISTS idx_learning_preferences_status
                ON learning_preferences(status, scope_kind, scope_id, updated_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (20, 'learning_loop', 20000);

-- Migration 21: orchestrator_session_title_lifecycle
ALTER TABLE orchestrator_sessions
                ADD COLUMN auto_title_updated_at_unix_ms INTEGER;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN title_generation_state TEXT NOT NULL DEFAULT 'idle';
            ALTER TABLE orchestrator_sessions
                ADD COLUMN manual_title_locked INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN manual_title_updated_at_unix_ms INTEGER;
            CREATE INDEX IF NOT EXISTS idx_orchestrator_sessions_manual_title_locked
                ON orchestrator_sessions(manual_title_locked, updated_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (21, 'orchestrator_session_title_lifecycle', 21000);

-- Migration 22: orchestrator_session_quick_controls
ALTER TABLE orchestrator_sessions
                ADD COLUMN model_profile_override TEXT;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN thinking_override INTEGER;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN trace_override INTEGER;
            ALTER TABLE orchestrator_sessions
                ADD COLUMN verbose_override INTEGER;
            CREATE INDEX IF NOT EXISTS idx_orchestrator_sessions_model_profile_override
                ON orchestrator_sessions(model_profile_override, updated_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (22, 'orchestrator_session_quick_controls', 22000);

-- Migration 23: session_project_context_state
CREATE TABLE IF NOT EXISTS session_project_context_state (
                session_ulid TEXT PRIMARY KEY,
                focus_paths_json TEXT NOT NULL DEFAULT '[]',
                disabled_entry_ids_json TEXT NOT NULL DEFAULT '[]',
                approved_entry_ids_json TEXT NOT NULL DEFAULT '[]',
                last_refreshed_at_unix_ms INTEGER,
                updated_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid) ON DELETE CASCADE
            );
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (23, 'session_project_context_state', 23000);

-- Migration 24: workspace_checkpoints
CREATE TABLE IF NOT EXISTS workspace_checkpoints (
                checkpoint_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                source_label TEXT NOT NULL,
                tool_name TEXT,
                proposal_id TEXT,
                actor_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                summary_text TEXT NOT NULL,
                diff_summary_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                restore_count INTEGER NOT NULL DEFAULT 0,
                last_restored_at_unix_ms INTEGER,
                latest_restore_report_ulid TEXT,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid) ON DELETE CASCADE,
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_workspace_checkpoints_run
                ON workspace_checkpoints(run_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_workspace_checkpoints_session
                ON workspace_checkpoints(session_ulid, created_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS workspace_checkpoint_blobs (
                blob_sha256 TEXT PRIMARY KEY,
                content_bytes BLOB NOT NULL,
                content_size_bytes INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                is_text INTEGER NOT NULL DEFAULT 0,
                text_preview TEXT,
                search_text TEXT,
                created_at_unix_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS workspace_checkpoint_files (
                artifact_ulid TEXT PRIMARY KEY,
                checkpoint_ulid TEXT NOT NULL,
                path TEXT NOT NULL,
                workspace_root_index INTEGER NOT NULL,
                moved_from_path TEXT,
                change_kind TEXT NOT NULL,
                before_content_sha256 TEXT,
                before_size_bytes INTEGER,
                after_content_sha256 TEXT,
                after_size_bytes INTEGER,
                blob_sha256 TEXT,
                content_type TEXT NOT NULL,
                is_text INTEGER NOT NULL DEFAULT 0,
                preview_text TEXT,
                search_text TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(checkpoint_ulid) REFERENCES workspace_checkpoints(checkpoint_ulid) ON DELETE CASCADE,
                FOREIGN KEY(blob_sha256) REFERENCES workspace_checkpoint_blobs(blob_sha256)
            );
            CREATE INDEX IF NOT EXISTS idx_workspace_checkpoint_files_checkpoint
                ON workspace_checkpoint_files(checkpoint_ulid, path ASC);
            CREATE INDEX IF NOT EXISTS idx_workspace_checkpoint_files_path
                ON workspace_checkpoint_files(path, created_at_unix_ms DESC);

            CREATE VIRTUAL TABLE IF NOT EXISTS workspace_checkpoint_files_fts
                USING fts5(artifact_ulid UNINDEXED, path, search_text, tokenize='unicode61');
            CREATE TRIGGER IF NOT EXISTS trg_workspace_checkpoint_files_ai
            AFTER INSERT ON workspace_checkpoint_files
            BEGIN
                INSERT INTO workspace_checkpoint_files_fts(artifact_ulid, path, search_text)
                VALUES (new.artifact_ulid, new.path, COALESCE(new.search_text, ''));
            END;
            CREATE TRIGGER IF NOT EXISTS trg_workspace_checkpoint_files_ad
            AFTER DELETE ON workspace_checkpoint_files
            BEGIN
                DELETE FROM workspace_checkpoint_files_fts WHERE artifact_ulid = old.artifact_ulid;
            END;

            CREATE TABLE IF NOT EXISTS workspace_restore_reports (
                report_ulid TEXT PRIMARY KEY,
                checkpoint_ulid TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                scope_kind TEXT NOT NULL,
                target_path TEXT,
                restored_paths_json TEXT NOT NULL,
                failed_paths_json TEXT NOT NULL,
                reconciliation_summary TEXT NOT NULL,
                reconciliation_prompt TEXT NOT NULL,
                branched_session_ulid TEXT,
                result_state TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(checkpoint_ulid) REFERENCES workspace_checkpoints(checkpoint_ulid) ON DELETE CASCADE,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid) ON DELETE CASCADE,
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_workspace_restore_reports_checkpoint
                ON workspace_restore_reports(checkpoint_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_workspace_restore_reports_session
                ON workspace_restore_reports(session_ulid, created_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (24, 'workspace_checkpoints', 24000);

-- Migration 25: session_queue_policy_metadata
ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN queue_mode TEXT NOT NULL DEFAULT 'followup';
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN priority_lane TEXT NOT NULL DEFAULT 'normal';
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN coalescing_group TEXT;
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN overflow_summary_ref TEXT;
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN safe_boundary_flags_json TEXT NOT NULL DEFAULT '{}';
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN decision_reason TEXT NOT NULL DEFAULT 'legacy_followup';
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN accepted_at_unix_ms INTEGER;
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN coalesced_at_unix_ms INTEGER;
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN forwarded_at_unix_ms INTEGER;
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN terminal_at_unix_ms INTEGER;
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN policy_snapshot_json TEXT NOT NULL DEFAULT '{}';
            ALTER TABLE orchestrator_queued_inputs
                ADD COLUMN explain_json TEXT NOT NULL DEFAULT '{}';

            CREATE INDEX IF NOT EXISTS idx_orchestrator_queued_inputs_state
                ON orchestrator_queued_inputs(session_ulid, state, created_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_orchestrator_queued_inputs_mode
                ON orchestrator_queued_inputs(session_ulid, queue_mode, created_at_unix_ms);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (25, 'session_queue_policy_metadata', 25000);

-- Migration 26: session_queue_operator_controls
CREATE TABLE IF NOT EXISTS orchestrator_session_queue_controls (
                session_ulid TEXT PRIMARY KEY,
                paused INTEGER NOT NULL DEFAULT 0,
                pause_reason TEXT,
                updated_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_session_queue_controls_paused
                ON orchestrator_session_queue_controls(paused, updated_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (26, 'session_queue_operator_controls', 26000);

-- Migration 27: create_recall_artifacts
CREATE TABLE IF NOT EXISTS recall_artifacts (
                artifact_ulid TEXT PRIMARY KEY,
                artifact_kind TEXT NOT NULL,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                session_ulid TEXT,
                query TEXT NOT NULL,
                summary TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                diagnostics_json TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                created_by_principal TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_recall_artifacts_scope
                ON recall_artifacts(principal, device_id, channel, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_recall_artifacts_session
                ON recall_artifacts(session_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_recall_artifacts_kind
                ON recall_artifacts(artifact_kind, created_at_unix_ms DESC);
            CREATE TRIGGER IF NOT EXISTS trg_recall_artifacts_prevent_update
            BEFORE UPDATE ON recall_artifacts
            BEGIN
                SELECT RAISE(ABORT, 'recall_artifacts is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_recall_artifacts_prevent_delete
            BEFORE DELETE ON recall_artifacts
            BEGIN
                SELECT RAISE(ABORT, 'recall_artifacts is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (27, 'create_recall_artifacts', 27000);

-- Migration 28: workspace_checkpoint_pair_metadata
ALTER TABLE workspace_checkpoints
                ADD COLUMN checkpoint_stage TEXT NOT NULL DEFAULT 'post_change';
            ALTER TABLE workspace_checkpoints
                ADD COLUMN mutation_ulid TEXT;
            ALTER TABLE workspace_checkpoints
                ADD COLUMN paired_checkpoint_ulid TEXT;
            ALTER TABLE workspace_checkpoints
                ADD COLUMN compare_summary_json TEXT NOT NULL DEFAULT '{}';
            ALTER TABLE workspace_checkpoints
                ADD COLUMN risk_level TEXT NOT NULL DEFAULT 'low';
            ALTER TABLE workspace_checkpoints
                ADD COLUMN review_posture TEXT NOT NULL DEFAULT 'standard';
            CREATE INDEX IF NOT EXISTS idx_workspace_checkpoints_mutation
                ON workspace_checkpoints(mutation_ulid, checkpoint_stage, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_workspace_checkpoints_pair_missing
                ON workspace_checkpoints(checkpoint_stage, paired_checkpoint_ulid, created_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (28, 'workspace_checkpoint_pair_metadata', 28000);

-- Migration 29: durable_flow_orchestration
CREATE TABLE IF NOT EXISTS flows (
                flow_ulid TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                state TEXT NOT NULL,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                session_ulid TEXT,
                origin_run_ulid TEXT,
                objective_id TEXT,
                routine_id TEXT,
                webhook_id TEXT,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                current_step_ulid TEXT,
                revision INTEGER NOT NULL DEFAULT 1,
                lock_owner TEXT,
                lock_expires_at_unix_ms INTEGER,
                retry_policy_json TEXT NOT NULL DEFAULT '{}',
                timeout_ms INTEGER,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                completed_at_unix_ms INTEGER,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(origin_run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_flows_owner_state
                ON flows(owner_principal, device_id, channel, state, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_flows_session
                ON flows(session_ulid, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_flows_origin_run
                ON flows(origin_run_ulid, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_flows_routine
                ON flows(routine_id, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_flows_objective
                ON flows(objective_id, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_flows_webhook
                ON flows(webhook_id, updated_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS flow_steps (
                step_ulid TEXT PRIMARY KEY,
                flow_ulid TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                step_kind TEXT NOT NULL,
                adapter TEXT NOT NULL,
                state TEXT NOT NULL,
                title TEXT NOT NULL,
                input_json TEXT NOT NULL DEFAULT '{}',
                output_json TEXT,
                lineage_json TEXT NOT NULL DEFAULT '{}',
                depends_on_step_ids_json TEXT NOT NULL DEFAULT '[]',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 1,
                backoff_ms INTEGER NOT NULL DEFAULT 0,
                timeout_ms INTEGER,
                not_before_unix_ms INTEGER,
                waiting_reason TEXT,
                last_error TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                started_at_unix_ms INTEGER,
                completed_at_unix_ms INTEGER,
                UNIQUE(flow_ulid, step_index),
                FOREIGN KEY(flow_ulid) REFERENCES flows(flow_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_flow_steps_flow_index
                ON flow_steps(flow_ulid, step_index ASC);
            CREATE INDEX IF NOT EXISTS idx_flow_steps_state_due
                ON flow_steps(state, not_before_unix_ms, updated_at_unix_ms ASC);
            CREATE INDEX IF NOT EXISTS idx_flow_steps_adapter
                ON flow_steps(adapter, state, updated_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS flow_events (
                event_ulid TEXT PRIMARY KEY,
                flow_ulid TEXT NOT NULL,
                step_ulid TEXT,
                event_type TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                from_state TEXT,
                to_state TEXT,
                summary TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(flow_ulid) REFERENCES flows(flow_ulid) ON DELETE CASCADE,
                FOREIGN KEY(step_ulid) REFERENCES flow_steps(step_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_flow_events_flow_time
                ON flow_events(flow_ulid, created_at_unix_ms ASC);
            CREATE INDEX IF NOT EXISTS idx_flow_events_step_time
                ON flow_events(step_ulid, created_at_unix_ms ASC);
            CREATE TRIGGER IF NOT EXISTS trg_flow_events_prevent_update
            BEFORE UPDATE ON flow_events
            BEGIN
                SELECT RAISE(ABORT, 'flow_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_flow_events_prevent_delete
            BEFORE DELETE ON flow_events
            BEGIN
                SELECT RAISE(ABORT, 'flow_events is append-only');
            END;

            CREATE TABLE IF NOT EXISTS flow_revisions (
                revision_ulid TEXT PRIMARY KEY,
                flow_ulid TEXT NOT NULL,
                revision INTEGER NOT NULL,
                parent_revision INTEGER,
                change_kind TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at_unix_ms INTEGER NOT NULL,
                UNIQUE(flow_ulid, revision),
                FOREIGN KEY(flow_ulid) REFERENCES flows(flow_ulid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_flow_revisions_flow_revision
                ON flow_revisions(flow_ulid, revision ASC);
            CREATE TRIGGER IF NOT EXISTS trg_flow_revisions_prevent_update
            BEFORE UPDATE ON flow_revisions
            BEGIN
                SELECT RAISE(ABORT, 'flow_revisions is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_flow_revisions_prevent_delete
            BEFORE DELETE ON flow_revisions
            BEGIN
                SELECT RAISE(ABORT, 'flow_revisions is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (29, 'durable_flow_orchestration', 29000);

-- Migration 30: phase_one_runtime_invariants
CREATE TABLE IF NOT EXISTS run_lifecycle_events (
                event_ulid TEXT PRIMARY KEY,
                run_ulid TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                from_state TEXT,
                to_state TEXT NOT NULL,
                actor_kind TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                parent_run_ulid TEXT,
                idempotency_key TEXT,
                reason TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_run_lifecycle_events_run_time
                ON run_lifecycle_events(run_ulid, created_at_unix_ms ASC);
            CREATE INDEX IF NOT EXISTS idx_run_lifecycle_events_correlation
                ON run_lifecycle_events(correlation_id, created_at_unix_ms ASC);
            CREATE TRIGGER IF NOT EXISTS trg_run_lifecycle_events_prevent_update
            BEFORE UPDATE ON run_lifecycle_events
            BEGIN
                SELECT RAISE(ABORT, 'run_lifecycle_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_run_lifecycle_events_prevent_delete
            BEFORE DELETE ON run_lifecycle_events
            BEGIN
                SELECT RAISE(ABORT, 'run_lifecycle_events is append-only');
            END;

            CREATE TABLE IF NOT EXISTS idempotency_records (
                idempotency_key TEXT PRIMARY KEY,
                scope TEXT NOT NULL,
                operation_kind TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                state TEXT NOT NULL,
                result_json TEXT,
                error_json TEXT,
                first_seen_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                expires_at_unix_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_idempotency_records_scope
                ON idempotency_records(scope, operation_kind, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_idempotency_records_state
                ON idempotency_records(state, updated_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS tool_result_artifacts (
                artifact_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                proposal_ulid TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                digest_sha256 TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                sensitivity TEXT NOT NULL,
                retention_json TEXT NOT NULL,
                storage_backend TEXT NOT NULL,
                content_bytes BLOB NOT NULL,
                size_bytes INTEGER NOT NULL,
                redacted_preview TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                expires_at_unix_ms INTEGER,
                legal_hold INTEGER NOT NULL DEFAULT 0,
                purge_requested INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_tool_result_artifacts_run
                ON tool_result_artifacts(run_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_tool_result_artifacts_session
                ON tool_result_artifacts(session_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_tool_result_artifacts_expiry
                ON tool_result_artifacts(expires_at_unix_ms, legal_hold, purge_requested);

            CREATE TABLE IF NOT EXISTS tool_result_artifact_reads (
                read_ulid TEXT PRIMARY KEY,
                artifact_ulid TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                offset_bytes INTEGER NOT NULL,
                requested_bytes INTEGER NOT NULL,
                returned_bytes INTEGER NOT NULL,
                denied INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(artifact_ulid) REFERENCES tool_result_artifacts(artifact_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_tool_result_artifact_reads_artifact
                ON tool_result_artifact_reads(artifact_ulid, created_at_unix_ms DESC);
            CREATE TRIGGER IF NOT EXISTS trg_tool_result_artifact_reads_prevent_update
            BEFORE UPDATE ON tool_result_artifact_reads
            BEGIN
                SELECT RAISE(ABORT, 'tool_result_artifact_reads is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_tool_result_artifact_reads_prevent_delete
            BEFORE DELETE ON tool_result_artifact_reads
            BEGIN
                SELECT RAISE(ABORT, 'tool_result_artifact_reads is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (30, 'phase_one_runtime_invariants', 30000);

-- Migration 31: durable_tool_jobs
CREATE TABLE IF NOT EXISTS tool_jobs (
                job_ulid TEXT PRIMARY KEY,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                tool_call_ulid TEXT NOT NULL,
                tool_name TEXT NOT NULL,
                backend TEXT NOT NULL,
                backend_reason_code TEXT,
                command_sha256 TEXT NOT NULL,
                program_sha256 TEXT,
                state TEXT NOT NULL,
                attempt_count INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL,
                retry_allowed INTEGER NOT NULL,
                idempotency_key TEXT,
                cancellation_handle TEXT,
                artifact_refs_json TEXT,
                tail_preview TEXT NOT NULL,
                stdout_artifact_ulid TEXT,
                stderr_artifact_ulid TEXT,
                last_error TEXT,
                state_reason TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                started_at_unix_ms INTEGER,
                heartbeat_at_unix_ms INTEGER,
                completed_at_unix_ms INTEGER,
                expires_at_unix_ms INTEGER,
                legal_hold INTEGER NOT NULL DEFAULT 0,
                active_ref_count INTEGER NOT NULL DEFAULT 0,
                lease_expires_at_unix_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_tool_jobs_owner_updated
                ON tool_jobs(owner_principal, updated_at_unix_ms DESC, job_ulid DESC);
            CREATE INDEX IF NOT EXISTS idx_tool_jobs_session_run
                ON tool_jobs(session_ulid, run_ulid, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_tool_jobs_state_updated
                ON tool_jobs(state, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_tool_jobs_expiry
                ON tool_jobs(expires_at_unix_ms, legal_hold, active_ref_count);
            CREATE INDEX IF NOT EXISTS idx_tool_jobs_heartbeat
                ON tool_jobs(state, heartbeat_at_unix_ms, lease_expires_at_unix_ms);

            CREATE TABLE IF NOT EXISTS tool_job_tail_entries (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                job_ulid TEXT NOT NULL,
                stream TEXT NOT NULL,
                chunk_redacted TEXT NOT NULL,
                chunk_sha256 TEXT NOT NULL,
                byte_len INTEGER NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(job_ulid) REFERENCES tool_jobs(job_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_tool_job_tail_entries_job
                ON tool_job_tail_entries(job_ulid, seq ASC);
            CREATE TRIGGER IF NOT EXISTS trg_tool_job_tail_entries_prevent_update
            BEFORE UPDATE ON tool_job_tail_entries
            BEGIN
                SELECT RAISE(ABORT, 'tool_job_tail_entries is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_tool_job_tail_entries_prevent_delete
            BEFORE DELETE ON tool_job_tail_entries
            BEGIN
                SELECT RAISE(ABORT, 'tool_job_tail_entries is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (31, 'durable_tool_jobs', 31000);

-- Migration 32: cron_job_workdir
ALTER TABLE cron_jobs
                ADD COLUMN workdir TEXT;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (32, 'cron_job_workdir', 32000);

-- Migration 33: allow_recall_artifact_retention_delete
DROP TRIGGER IF EXISTS trg_recall_artifacts_prevent_delete;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (33, 'allow_recall_artifact_retention_delete', 33000);

-- Migration 34: create_approval_consumptions_table
CREATE TABLE IF NOT EXISTS approval_consumptions (
                approval_ulid TEXT PRIMARY KEY,
                consumed_at_unix_ms INTEGER NOT NULL,
                consume_reason TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_approval_consumptions_consumed_at
                ON approval_consumptions(consumed_at_unix_ms DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (34, 'create_approval_consumptions_table', 34000);

-- Migration 35: task_runtime_workboard_commitments
CREATE TABLE IF NOT EXISTS work_items (
                work_item_ulid TEXT PRIMARY KEY,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                session_ulid TEXT,
                run_ulid TEXT,
                objective_id TEXT,
                routine_id TEXT,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                state TEXT NOT NULL,
                priority INTEGER NOT NULL,
                assigned_worker TEXT,
                claim_owner TEXT,
                claim_expires_at_unix_ms INTEGER,
                heartbeat_at_unix_ms INTEGER,
                dependencies_json TEXT NOT NULL,
                artifact_refs_json TEXT NOT NULL,
                blocker_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                completed_at_unix_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_work_items_owner_state_updated
                ON work_items(owner_principal, state, updated_at_unix_ms DESC, work_item_ulid DESC);
            CREATE INDEX IF NOT EXISTS idx_work_items_claim
                ON work_items(state, claim_owner, claim_expires_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_work_items_heartbeat
                ON work_items(state, heartbeat_at_unix_ms);

            CREATE TABLE IF NOT EXISTS work_item_events (
                event_ulid TEXT PRIMARY KEY,
                work_item_ulid TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                from_state TEXT,
                to_state TEXT,
                summary TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(work_item_ulid) REFERENCES work_items(work_item_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_work_item_events_item
                ON work_item_events(work_item_ulid, created_at_unix_ms ASC, event_ulid ASC);
            CREATE TRIGGER IF NOT EXISTS trg_work_item_events_prevent_update
            BEFORE UPDATE ON work_item_events
            BEGIN
                SELECT RAISE(ABORT, 'work_item_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_work_item_events_prevent_delete
            BEFORE DELETE ON work_item_events
            BEGIN
                SELECT RAISE(ABORT, 'work_item_events is append-only');
            END;

            CREATE TABLE IF NOT EXISTS commitments (
                commitment_ulid TEXT PRIMARY KEY,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                session_ulid TEXT,
                run_ulid TEXT,
                user_wording TEXT NOT NULL,
                normalized_action TEXT NOT NULL,
                due_condition_json TEXT NOT NULL,
                recurrence_json TEXT NOT NULL,
                channel_binding_json TEXT NOT NULL,
                approval_requirement TEXT NOT NULL,
                privacy_label TEXT NOT NULL,
                status TEXT NOT NULL,
                confidence_bps INTEGER NOT NULL,
                extraction_model TEXT NOT NULL,
                review_reason TEXT NOT NULL,
                scheduler_binding_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                due_at_unix_ms INTEGER,
                scheduled_at_unix_ms INTEGER,
                completed_at_unix_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_commitments_owner_status_due
                ON commitments(owner_principal, status, due_at_unix_ms, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_commitments_due
                ON commitments(status, due_at_unix_ms);

            CREATE TABLE IF NOT EXISTS commitment_sources (
                source_ulid TEXT PRIMARY KEY,
                commitment_ulid TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                session_ulid TEXT,
                run_ulid TEXT,
                tape_start_seq INTEGER,
                tape_end_seq INTEGER,
                evidence_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(commitment_ulid) REFERENCES commitments(commitment_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_commitment_sources_commitment
                ON commitment_sources(commitment_ulid, created_at_unix_ms ASC);
            CREATE TRIGGER IF NOT EXISTS trg_commitment_sources_prevent_update
            BEFORE UPDATE ON commitment_sources
            BEGIN
                SELECT RAISE(ABORT, 'commitment_sources is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_commitment_sources_prevent_delete
            BEFORE DELETE ON commitment_sources
            BEGIN
                SELECT RAISE(ABORT, 'commitment_sources is append-only');
            END;

            CREATE TABLE IF NOT EXISTS commitment_events (
                event_ulid TEXT PRIMARY KEY,
                commitment_ulid TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                from_status TEXT,
                to_status TEXT,
                summary TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(commitment_ulid) REFERENCES commitments(commitment_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_commitment_events_commitment
                ON commitment_events(commitment_ulid, created_at_unix_ms ASC, event_ulid ASC);
            CREATE TRIGGER IF NOT EXISTS trg_commitment_events_prevent_update
            BEFORE UPDATE ON commitment_events
            BEGIN
                SELECT RAISE(ABORT, 'commitment_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_commitment_events_prevent_delete
            BEFORE DELETE ON commitment_events
            BEGIN
                SELECT RAISE(ABORT, 'commitment_events is append-only');
            END;

            CREATE TABLE IF NOT EXISTS commitment_delivery_attempts (
                attempt_ulid TEXT PRIMARY KEY,
                commitment_ulid TEXT NOT NULL,
                delivery_intent_id TEXT,
                channel_binding_json TEXT NOT NULL,
                status TEXT NOT NULL,
                reason TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(commitment_ulid) REFERENCES commitments(commitment_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_commitment_delivery_attempts_commitment
                ON commitment_delivery_attempts(commitment_ulid, created_at_unix_ms DESC);
            CREATE TRIGGER IF NOT EXISTS trg_commitment_delivery_attempts_prevent_delete
            BEFORE DELETE ON commitment_delivery_attempts
            BEGIN
                SELECT RAISE(ABORT, 'commitment_delivery_attempts is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (35, 'task_runtime_workboard_commitments', 35000);

-- Migration 36: learning_eval_rollout_and_memory_embedding_jobs
CREATE TABLE IF NOT EXISTS learning_candidate_evals (
                eval_ulid TEXT PRIMARY KEY,
                candidate_ulid TEXT NOT NULL,
                eval_suite TEXT NOT NULL,
                result TEXT NOT NULL,
                threshold REAL NOT NULL,
                score REAL NOT NULL,
                decision TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                policy_decision TEXT NOT NULL,
                evidence_refs_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(candidate_ulid) REFERENCES learning_candidates(candidate_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_learning_candidate_evals_candidate
                ON learning_candidate_evals(candidate_ulid, created_at_unix_ms DESC, eval_ulid DESC);
            CREATE TRIGGER IF NOT EXISTS trg_learning_candidate_evals_prevent_update
            BEFORE UPDATE ON learning_candidate_evals
            BEGIN
                SELECT RAISE(ABORT, 'learning_candidate_evals is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_learning_candidate_evals_prevent_delete
            BEFORE DELETE ON learning_candidate_evals
            BEGIN
                SELECT RAISE(ABORT, 'learning_candidate_evals is append-only');
            END;

            CREATE TABLE IF NOT EXISTS learning_candidate_rollouts (
                rollout_ulid TEXT PRIMARY KEY,
                candidate_ulid TEXT NOT NULL,
                rollout_kind TEXT NOT NULL,
                state TEXT NOT NULL,
                target_ref TEXT NOT NULL,
                previous_version_json TEXT NOT NULL,
                activated_version_json TEXT NOT NULL,
                telemetry_json TEXT NOT NULL,
                reason TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                policy_decision TEXT NOT NULL,
                evidence_refs_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                rolled_back_at_unix_ms INTEGER,
                FOREIGN KEY(candidate_ulid) REFERENCES learning_candidates(candidate_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_learning_candidate_rollouts_candidate
                ON learning_candidate_rollouts(candidate_ulid, created_at_unix_ms DESC, rollout_ulid DESC);
            CREATE INDEX IF NOT EXISTS idx_learning_candidate_rollouts_state
                ON learning_candidate_rollouts(state, updated_at_unix_ms DESC);
            CREATE TRIGGER IF NOT EXISTS trg_learning_candidate_rollouts_prevent_delete
            BEFORE DELETE ON learning_candidate_rollouts
            BEGIN
                SELECT RAISE(ABORT, 'learning_candidate_rollouts is append-only');
            END;

            CREATE TABLE IF NOT EXISTS memory_embedding_jobs (
                job_ulid TEXT PRIMARY KEY,
                source_kind TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                embedding_model_id TEXT NOT NULL,
                embedding_dims INTEGER NOT NULL,
                embedding_version INTEGER NOT NULL,
                index_target TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                claimed_by TEXT,
                claimed_at_unix_ms INTEGER,
                last_error TEXT,
                next_retry_unix_ms INTEGER,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                completed_at_unix_ms INTEGER,
                UNIQUE (
                    source_kind,
                    source_ref,
                    content_hash,
                    embedding_model_id,
                    embedding_dims,
                    embedding_version,
                    index_target
                )
            );
            CREATE INDEX IF NOT EXISTS idx_memory_embedding_jobs_status_retry
                ON memory_embedding_jobs(status, next_retry_unix_ms, updated_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_memory_embedding_jobs_source
                ON memory_embedding_jobs(source_kind, source_ref, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_memory_embedding_jobs_target
                ON memory_embedding_jobs(index_target, embedding_model_id, embedding_dims, embedding_version);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (36, 'learning_eval_rollout_and_memory_embedding_jobs', 36000);

-- Migration 37: agent_plan_state
CREATE TABLE IF NOT EXISTS agent_plan_items (
                plan_item_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT,
                parent_run_ulid TEXT,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                title TEXT NOT NULL,
                details_json TEXT NOT NULL,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL,
                blocked_reason TEXT,
                evidence_refs_json TEXT NOT NULL,
                redaction_level TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                completed_at_unix_ms INTEGER,
                cancelled_at_unix_ms INTEGER,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid),
                FOREIGN KEY(parent_run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_agent_plan_items_session_status
                ON agent_plan_items(session_ulid, status, priority DESC, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_agent_plan_items_run
                ON agent_plan_items(run_ulid, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_agent_plan_items_parent_run
                ON agent_plan_items(parent_run_ulid, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_agent_plan_items_owner_status
                ON agent_plan_items(owner_principal, device_id, channel, status, updated_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS agent_plan_events (
                event_ulid TEXT PRIMARY KEY,
                plan_item_ulid TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT,
                event_type TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                from_status TEXT,
                to_status TEXT,
                reason_code TEXT NOT NULL,
                summary TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                evidence_refs_json TEXT NOT NULL,
                redaction_level TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(plan_item_ulid) REFERENCES agent_plan_items(plan_item_ulid),
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_agent_plan_events_item
                ON agent_plan_events(plan_item_ulid, created_at_unix_ms ASC, event_ulid ASC);
            CREATE INDEX IF NOT EXISTS idx_agent_plan_events_session_run
                ON agent_plan_events(session_ulid, run_ulid, created_at_unix_ms ASC);
            CREATE TRIGGER IF NOT EXISTS trg_agent_plan_events_prevent_update
            BEFORE UPDATE ON agent_plan_events
            BEGIN
                SELECT RAISE(ABORT, 'agent_plan_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_agent_plan_events_prevent_delete
            BEFORE DELETE ON agent_plan_events
            BEGIN
                SELECT RAISE(ABORT, 'agent_plan_events is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (37, 'agent_plan_state', 37000);

-- Migration 38: progress_draft_storage
CREATE TABLE IF NOT EXISTS progress_drafts (
                draft_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL UNIQUE,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                channel_instance_id TEXT,
                external_message_id TEXT,
                state TEXT NOT NULL,
                summary TEXT NOT NULL,
                last_visible_step TEXT NOT NULL,
                hidden_internal_state_hash TEXT NOT NULL,
                render_policy TEXT NOT NULL,
                version INTEGER NOT NULL,
                reason_code TEXT NOT NULL,
                evidence_refs_json TEXT NOT NULL,
                redaction_level TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                completed_at_unix_ms INTEGER,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_progress_drafts_owner_state
                ON progress_drafts(owner_principal, device_id, channel, state, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_progress_drafts_session_state
                ON progress_drafts(session_ulid, state, updated_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_progress_drafts_channel_message
                ON progress_drafts(channel, channel_instance_id, external_message_id);

            CREATE TABLE IF NOT EXISTS progress_draft_events (
                event_ulid TEXT PRIMARY KEY,
                draft_ulid TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                from_state TEXT,
                to_state TEXT,
                reason_code TEXT NOT NULL,
                summary TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                evidence_refs_json TEXT NOT NULL,
                redaction_level TEXT NOT NULL,
                source_tape_seq INTEGER NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(draft_ulid) REFERENCES progress_drafts(draft_ulid),
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_progress_draft_events_draft
                ON progress_draft_events(draft_ulid, created_at_unix_ms ASC, event_ulid ASC);
            CREATE INDEX IF NOT EXISTS idx_progress_draft_events_run
                ON progress_draft_events(run_ulid, source_tape_seq ASC, event_ulid ASC);
            CREATE TRIGGER IF NOT EXISTS trg_progress_draft_events_prevent_update
            BEFORE UPDATE ON progress_draft_events
            BEGIN
                SELECT RAISE(ABORT, 'progress_draft_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_progress_draft_events_prevent_delete
            BEFORE DELETE ON progress_draft_events
            BEGIN
                SELECT RAISE(ABORT, 'progress_draft_events is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (38, 'progress_draft_storage', 38000);

-- Migration 39: turn_control_audit_events
CREATE TABLE IF NOT EXISTS turn_control_events (
                event_ulid TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                operation TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                target_kind TEXT NOT NULL,
                target_id TEXT,
                session_ulid TEXT,
                run_ulid TEXT,
                outcome TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                evidence_refs_json TEXT NOT NULL,
                redaction_level TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_turn_control_events_operation_actor
                ON turn_control_events(operation, actor_principal, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_turn_control_events_session_run
                ON turn_control_events(session_ulid, run_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_turn_control_events_target
                ON turn_control_events(target_kind, target_id, created_at_unix_ms DESC);
            CREATE TRIGGER IF NOT EXISTS trg_turn_control_events_prevent_update
            BEFORE UPDATE ON turn_control_events
            BEGIN
                SELECT RAISE(ABORT, 'turn_control_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_turn_control_events_prevent_delete
            BEFORE DELETE ON turn_control_events
            BEGIN
                SELECT RAISE(ABORT, 'turn_control_events is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (39, 'turn_control_audit_events', 39000);

-- Migration 40: compat_response_store
CREATE TABLE IF NOT EXISTS compat_response_records (
                response_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL UNIQUE,
                owner_principal TEXT NOT NULL,
                device_id TEXT NOT NULL,
                channel TEXT,
                status TEXT NOT NULL,
                response_json TEXT NOT NULL,
                error_json TEXT,
                redaction_state_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                completed_at_unix_ms INTEGER,
                updated_at_unix_ms INTEGER NOT NULL,
                retention_expires_at_unix_ms INTEGER,
                deleted_at_unix_ms INTEGER,
                delete_reason TEXT,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_compat_response_records_owner_created
                ON compat_response_records(owner_principal, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_compat_response_records_session
                ON compat_response_records(session_ulid, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_compat_response_records_run
                ON compat_response_records(run_ulid);
            CREATE INDEX IF NOT EXISTS idx_compat_response_records_retention
                ON compat_response_records(retention_expires_at_unix_ms, deleted_at_unix_ms);

            CREATE TABLE IF NOT EXISTS compat_response_events (
                event_ulid TEXT PRIMARY KEY,
                response_ulid TEXT NOT NULL,
                session_ulid TEXT NOT NULL,
                run_ulid TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor_principal TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(response_ulid) REFERENCES compat_response_records(response_ulid),
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
                FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_compat_response_events_response
                ON compat_response_events(response_ulid, created_at_unix_ms ASC);
            CREATE INDEX IF NOT EXISTS idx_compat_response_events_run
                ON compat_response_events(run_ulid, created_at_unix_ms ASC);
            CREATE TRIGGER IF NOT EXISTS trg_compat_response_events_prevent_update
            BEFORE UPDATE ON compat_response_events
            BEGIN
                SELECT RAISE(ABORT, 'compat_response_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_compat_response_events_prevent_delete
            BEFORE DELETE ON compat_response_events
            BEGIN
                SELECT RAISE(ABORT, 'compat_response_events is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (40, 'compat_response_store', 40000);

-- Migration 41: session_write_leases
CREATE TABLE IF NOT EXISTS orchestrator_session_write_leases (
                session_ulid TEXT PRIMARY KEY,
                lease_ulid TEXT NOT NULL UNIQUE,
                owner_process_id INTEGER NOT NULL,
                owner_label TEXT NOT NULL,
                reason TEXT NOT NULL,
                acquired_at_unix_ms INTEGER NOT NULL,
                expires_at_unix_ms INTEGER NOT NULL,
                reentrant_depth INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_session_write_leases_expires
                ON orchestrator_session_write_leases(expires_at_unix_ms);

            CREATE TABLE IF NOT EXISTS orchestrator_session_write_lease_events (
                event_ulid TEXT PRIMARY KEY,
                session_ulid TEXT NOT NULL,
                lease_ulid TEXT,
                event_type TEXT NOT NULL,
                owner_process_id INTEGER NOT NULL,
                owner_label TEXT NOT NULL,
                reason TEXT NOT NULL,
                reentrant_depth INTEGER NOT NULL,
                observed_holder_json TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
            );
            CREATE INDEX IF NOT EXISTS idx_orchestrator_session_write_lease_events_session
                ON orchestrator_session_write_lease_events(session_ulid, created_at_unix_ms DESC);
            CREATE TRIGGER IF NOT EXISTS trg_orchestrator_session_write_lease_events_prevent_update
            BEFORE UPDATE ON orchestrator_session_write_lease_events
            BEGIN
                SELECT RAISE(ABORT, 'orchestrator_session_write_lease_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS trg_orchestrator_session_write_lease_events_prevent_delete
            BEFORE DELETE ON orchestrator_session_write_lease_events
            BEGIN
                SELECT RAISE(ABORT, 'orchestrator_session_write_lease_events is append-only');
            END;
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (41, 'session_write_leases', 41000);

-- Migration 42: work_item_graph_metadata
ALTER TABLE work_items ADD COLUMN parent_work_item_ulid TEXT;
            ALTER TABLE work_items ADD COLUMN evidence_refs_json TEXT NOT NULL DEFAULT '[]';
            ALTER TABLE work_items ADD COLUMN verification_state TEXT NOT NULL DEFAULT 'unverified';
            CREATE INDEX IF NOT EXISTS idx_work_items_parent
                ON work_items(parent_work_item_ulid, updated_at_unix_ms DESC, work_item_ulid DESC);
            CREATE INDEX IF NOT EXISTS idx_work_items_objective_routine
                ON work_items(objective_id, routine_id, updated_at_unix_ms DESC, work_item_ulid DESC);
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (42, 'work_item_graph_metadata', 42000);

-- Migration 43: flow_dependency_reconciliation_projection
ALTER TABLE flows
                ADD COLUMN dependency_health TEXT NOT NULL DEFAULT 'valid'
                CHECK (dependency_health IN ('valid', 'invalid'));

            UPDATE flows
            SET dependency_health = CASE
                WHEN COALESCE((
                    SELECT revisions.change_kind
                    FROM flow_revisions revisions
                    WHERE revisions.flow_ulid = flows.flow_ulid
                      AND revisions.change_kind IN (
                          'flow.dependencies_invalid',
                          'flow.dependencies_repaired'
                      )
                    ORDER BY revisions.revision DESC
                    LIMIT 1
                ), '') = 'flow.dependencies_invalid'
                THEN 'invalid'
                ELSE 'valid'
            END;

            CREATE INDEX IF NOT EXISTS idx_flows_reconciliation_eligible
                ON flows(updated_at_unix_ms ASC, created_at_unix_ms ASC, flow_ulid ASC)
                WHERE state = 'cancel_requested'
                   OR (
                       dependency_health = 'valid'
                       AND state NOT IN (
                           'paused',
                           'succeeded',
                           'failed',
                           'timed_out',
                           'cancelled'
                       )
                   );
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (43, 'flow_dependency_reconciliation_projection', 43000);

-- Migration 44: metadata_trace_segments
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
INSERT INTO schema_migrations(version, name, applied_at_unix_ms) VALUES (44, 'metadata_trace_segments', 44000);

COMMIT;
