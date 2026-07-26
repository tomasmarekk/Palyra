//! Durable shared runtime state for generations, event ordering, side-effect fences,
//! health, handles, cleanup evidence, and startup compatibility.

use std::collections::BTreeMap;

use palyra_common::runtime_contracts::{
    AuxiliaryTaskKind, AuxiliaryTaskState, CancellationContextV1, CancellationScopeKind,
    CleanupOutcome, CleanupReportV1, GenerationCheckDisposition, GenerationCheckOutcome,
    GenerationLeaseV1, HealthProbeDisposition, HealthProbeLeaseV1, HealthProbeResult,
    HealthProbeSettlementV1, ProcessLeaseV1, QuarantineClearRequest, RuntimeAttemptId,
    RuntimeAuthorityClass, RuntimeCausalLink, RuntimeCausalLinkKind, RuntimeComponentHealthV1,
    RuntimeErrorPhase, RuntimeEventActorKind, RuntimeEventEnvelopeV2, RuntimeEventId,
    RuntimeEventName, RuntimeEventPayloadRef, RuntimeEventRedactionClass, RuntimeGeneration,
    RuntimeGenerationLane, RuntimeGenerationTransitionKind, RuntimeHandleDescriptorV1,
    RuntimeHandleKind, RuntimeHandleState, RuntimeHealthState, RuntimeIdentityKind,
    RuntimeIdentityRef, RuntimeIdentitySetV1, RuntimeInstanceId, RuntimeRetryability, RuntimeRunId,
    RuntimeSessionId, RuntimeStateAdmissionPosture, RuntimeStateCompatibilityFinding,
    RuntimeStateCompatibilityOutcome, RuntimeStateCompatibilityReport, RuntimeSubsystem,
    RuntimeTraceId, SideEffectFenceState, SideEffectFenceV1, SideEffectRetryDecision,
    StaleEventDisposition, MAX_RUNTIME_COMPATIBILITY_FINDINGS,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::delegation::DelegationSnapshot;

use super::{
    append_or_replay_orchestrator_tape_event_tx, compute_hash, current_unix_ms, redact_value,
    runtime_kernel::ensure_runtime_rollback_allows_new_side_effect_tx, sanitize_payload,
    JournalAppendRequest, JournalError, JournalStore, OrchestratorTapeAppendRequest,
    NETWORKED_WORKER_DISPATCH_CLAIM_MAX_ENTRIES,
    NETWORKED_WORKER_DISPATCH_TERMINAL_EVIDENCE_MAX_ENTRIES, NETWORKED_WORKER_EXPIRY_MAX_ENTRIES,
    NETWORKED_WORKER_FLEET_MAX_ENTRIES,
};

const MAX_CLEANUP_REASON_DIAGNOSTIC_ENTRIES: usize = 32;
const OTHER_CLEANUP_REASON_DIAGNOSTIC_KEY: &str = "runtime.cleanup.other_reason_codes";
const QUARANTINE_CLEAR_AUTHORIZATION_EVIDENCE_KEY: &str = "authorization_evidence_sha256";

/// Migration 45: generation ownership, stale diagnostics, and ordered runtime events.
pub(super) const MIGRATION_45_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_generation_leases (
        session_ulid TEXT NOT NULL,
        lane TEXT NOT NULL,
        lease_ulid TEXT NOT NULL UNIQUE,
        run_ulid TEXT,
        generation INTEGER NOT NULL CHECK (generation > 0),
        owner TEXT NOT NULL,
        acquired_at_unix_ms INTEGER NOT NULL,
        expires_at_unix_ms INTEGER NOT NULL,
        PRIMARY KEY(session_ulid, lane),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_generation_leases_expiry
        ON runtime_generation_leases(expires_at_unix_ms);

    CREATE TABLE IF NOT EXISTS runtime_generation_events (
        event_ulid TEXT PRIMARY KEY,
        session_ulid TEXT NOT NULL,
        run_ulid TEXT,
        lane TEXT NOT NULL,
        from_generation INTEGER,
        to_generation INTEGER,
        transition_kind TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_generation_events_lane
        ON runtime_generation_events(session_ulid, lane, created_at_unix_ms ASC);

    CREATE TABLE IF NOT EXISTS runtime_stale_event_diagnostics (
        diagnostic_ulid TEXT PRIMARY KEY,
        session_ulid TEXT NOT NULL,
        run_ulid TEXT,
        lane TEXT NOT NULL,
        expected_generation INTEGER,
        observed_generation INTEGER NOT NULL,
        subsystem TEXT NOT NULL,
        disposition TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        payload_sha256 TEXT,
        payload_bytes INTEGER NOT NULL,
        created_at_unix_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_stale_event_diagnostics_subsystem
        ON runtime_stale_event_diagnostics(subsystem, created_at_unix_ms DESC);

    CREATE TABLE IF NOT EXISTS runtime_events_v2 (
        event_ulid TEXT PRIMARY KEY,
        session_ulid TEXT NOT NULL,
        run_ulid TEXT NOT NULL,
        lane TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK (generation > 0),
        sequence INTEGER NOT NULL CHECK (sequence >= 0),
        terminal INTEGER NOT NULL CHECK (terminal IN (0, 1)),
        event_name TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        envelope_json TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        UNIQUE(session_ulid, lane, generation, sequence),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_events_v2_terminal_once
        ON runtime_events_v2(session_ulid, lane, generation)
        WHERE terminal = 1;
    CREATE INDEX IF NOT EXISTS idx_runtime_events_v2_run_sequence
        ON runtime_events_v2(run_ulid, generation, sequence ASC);

    CREATE TRIGGER IF NOT EXISTS trg_runtime_generation_events_prevent_update
    BEFORE UPDATE ON runtime_generation_events BEGIN
        SELECT RAISE(ABORT, 'runtime_generation_events is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_generation_events_prevent_delete
    BEFORE DELETE ON runtime_generation_events BEGIN
        SELECT RAISE(ABORT, 'runtime_generation_events is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_stale_events_prevent_update
    BEFORE UPDATE ON runtime_stale_event_diagnostics BEGIN
        SELECT RAISE(ABORT, 'runtime_stale_event_diagnostics is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_stale_events_prevent_delete
    BEFORE DELETE ON runtime_stale_event_diagnostics BEGIN
        SELECT RAISE(ABORT, 'runtime_stale_event_diagnostics is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_events_v2_prevent_update
    BEFORE UPDATE ON runtime_events_v2 BEGIN
        SELECT RAISE(ABORT, 'runtime_events_v2 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_events_v2_prevent_delete
    BEFORE DELETE ON runtime_events_v2 BEGIN
        SELECT RAISE(ABORT, 'runtime_events_v2 is append-only');
    END;
"#;

/// Migration 46: durable side-effect fences and append-only transition evidence.
pub(super) const MIGRATION_46_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_side_effect_fences (
        operation_ulid TEXT PRIMARY KEY,
        tool_execution_ulid TEXT NOT NULL UNIQUE,
        session_ulid TEXT NOT NULL,
        run_ulid TEXT NOT NULL,
        intent_generation INTEGER NOT NULL,
        observed_generation INTEGER NOT NULL,
        state TEXT NOT NULL,
        intent_sha256 TEXT NOT NULL,
        fence_json TEXT NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid),
        FOREIGN KEY(run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_side_effect_fences_state
        ON runtime_side_effect_fences(state, updated_at_unix_ms ASC);

    CREATE TABLE IF NOT EXISTS runtime_side_effect_fence_events (
        event_ulid TEXT PRIMARY KEY,
        operation_ulid TEXT NOT NULL,
        from_state TEXT,
        to_state TEXT NOT NULL,
        generation INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        evidence_sha256 TEXT,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(operation_ulid) REFERENCES runtime_side_effect_fences(operation_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_side_effect_fence_events_operation
        ON runtime_side_effect_fence_events(operation_ulid, created_at_unix_ms ASC);
    CREATE TRIGGER IF NOT EXISTS trg_runtime_side_effect_events_prevent_update
    BEFORE UPDATE ON runtime_side_effect_fence_events BEGIN
        SELECT RAISE(ABORT, 'runtime_side_effect_fence_events is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_side_effect_events_prevent_delete
    BEFORE DELETE ON runtime_side_effect_fence_events BEGIN
        SELECT RAISE(ABORT, 'runtime_side_effect_fence_events is append-only');
    END;
"#;

/// Migration 47: shared component health and circuit-breaker evidence.
pub(super) const MIGRATION_47_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_component_health (
        component_ulid TEXT PRIMARY KEY,
        generation INTEGER NOT NULL,
        state TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        health_json TEXT NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_component_health_state
        ON runtime_component_health(state, updated_at_unix_ms ASC);

    CREATE TABLE IF NOT EXISTS runtime_component_health_events (
        event_ulid TEXT PRIMARY KEY,
        component_ulid TEXT NOT NULL,
        from_state TEXT,
        to_state TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(component_ulid) REFERENCES runtime_component_health(component_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_component_health_events_component
        ON runtime_component_health_events(component_ulid, created_at_unix_ms ASC);
    CREATE TRIGGER IF NOT EXISTS trg_runtime_health_events_prevent_update
    BEFORE UPDATE ON runtime_component_health_events BEGIN
        SELECT RAISE(ABORT, 'runtime_component_health_events is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_health_events_prevent_delete
    BEFORE DELETE ON runtime_component_health_events BEGIN
        SELECT RAISE(ABORT, 'runtime_component_health_events is append-only');
    END;
"#;

/// Migration 48: runtime handles, process leases, and cleanup reports.
pub(super) const MIGRATION_48_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_handles (
        instance_ulid TEXT PRIMARY KEY,
        session_ulid TEXT,
        run_ulid TEXT,
        generation INTEGER NOT NULL,
        kind TEXT NOT NULL,
        state TEXT NOT NULL,
        descriptor_json TEXT NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_handles_state
        ON runtime_handles(state, updated_at_unix_ms ASC);

    CREATE TABLE IF NOT EXISTS runtime_process_leases (
        lease_ulid TEXT PRIMARY KEY,
        instance_ulid TEXT NOT NULL UNIQUE,
        pid INTEGER NOT NULL,
        generation INTEGER NOT NULL,
        provenance_json TEXT NOT NULL,
        issued_at_unix_ms INTEGER NOT NULL,
        expires_at_unix_ms INTEGER NOT NULL,
        verified_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(instance_ulid) REFERENCES runtime_handles(instance_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_process_leases_expiry
        ON runtime_process_leases(expires_at_unix_ms);

    CREATE TABLE IF NOT EXISTS runtime_cleanup_reports (
        report_ulid TEXT PRIMARY KEY,
        instance_ulid TEXT NOT NULL,
        lease_ulid TEXT,
        outcome TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        report_json TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        FOREIGN KEY(instance_ulid) REFERENCES runtime_handles(instance_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_cleanup_reports_instance
        ON runtime_cleanup_reports(instance_ulid, created_at_unix_ms DESC);

    CREATE TABLE IF NOT EXISTS runtime_cleanup_steps (
        report_ulid TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        step TEXT NOT NULL,
        disposition TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        evidence_sha256 TEXT,
        created_at_unix_ms INTEGER NOT NULL,
        PRIMARY KEY(report_ulid, ordinal),
        FOREIGN KEY(report_ulid) REFERENCES runtime_cleanup_reports(report_ulid)
    );
    CREATE TRIGGER IF NOT EXISTS trg_runtime_cleanup_reports_prevent_update
    BEFORE UPDATE ON runtime_cleanup_reports BEGIN
        SELECT RAISE(ABORT, 'runtime_cleanup_reports is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_cleanup_reports_prevent_delete
    BEFORE DELETE ON runtime_cleanup_reports BEGIN
        SELECT RAISE(ABORT, 'runtime_cleanup_reports is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_cleanup_steps_prevent_update
    BEFORE UPDATE ON runtime_cleanup_steps BEGIN
        SELECT RAISE(ABORT, 'runtime_cleanup_steps is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_cleanup_steps_prevent_delete
    BEFORE DELETE ON runtime_cleanup_steps BEGIN
        SELECT RAISE(ABORT, 'runtime_cleanup_steps is append-only');
    END;
"#;

/// Migration 49: startup compatibility findings and corruption quarantine.
pub(super) const MIGRATION_49_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_state_quarantine (
        quarantine_ulid TEXT PRIMARY KEY,
        contract_name TEXT NOT NULL,
        record_ref_sha256 TEXT NOT NULL,
        observed_schema_version INTEGER,
        supported_schema_version INTEGER NOT NULL,
        outcome TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        blocks_admission INTEGER NOT NULL,
        payload_bytes INTEGER NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        UNIQUE(contract_name, record_ref_sha256, outcome)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_state_quarantine_blockers
        ON runtime_state_quarantine(blocks_admission, created_at_unix_ms DESC);
    CREATE TRIGGER IF NOT EXISTS trg_runtime_state_quarantine_prevent_update
    BEFORE UPDATE ON runtime_state_quarantine BEGIN
        SELECT RAISE(ABORT, 'runtime_state_quarantine is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_state_quarantine_prevent_delete
    BEFORE DELETE ON runtime_state_quarantine BEGIN
        SELECT RAISE(ABORT, 'runtime_state_quarantine is append-only');
    END;
"#;

/// Migration 50: explicit schema versions for durable lease records.
pub(super) const MIGRATION_50_SQL: &str = r#"
    ALTER TABLE runtime_generation_leases
        ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
    ALTER TABLE runtime_process_leases
        ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
"#;

/// Migration 51: bounded, single-flight health probe leases.
pub(super) const MIGRATION_51_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_health_probe_leases (
        lease_ulid TEXT PRIMARY KEY,
        component_ulid TEXT NOT NULL UNIQUE,
        expected_generation INTEGER NOT NULL CHECK (expected_generation > 0),
        authority_class TEXT NOT NULL,
        lease_json TEXT NOT NULL,
        issued_at_unix_ms INTEGER NOT NULL,
        expires_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY(component_ulid) REFERENCES runtime_component_health(component_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_health_probe_leases_expiry
        ON runtime_health_probe_leases(expires_at_unix_ms);
"#;

/// Migration 58: explicit versions for append-only shared-runtime evidence.
pub(super) const MIGRATION_58_SQL: &str = r#"
    ALTER TABLE runtime_generation_events
        ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
    ALTER TABLE runtime_stale_event_diagnostics
        ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
    ALTER TABLE runtime_side_effect_fence_events
        ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
    ALTER TABLE runtime_component_health_events
        ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1;
"#;

/// Migration 53: restart-safe progress for bounded process-lease reconciliation.
pub(super) const MIGRATION_53_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_process_reconciliation_checkpoint (
        checkpoint_key TEXT PRIMARY KEY,
        after_lease_ulid TEXT,
        updated_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1
    );
"#;

/// Migration 54: durable exact evidence for networked-worker lease expiry.
pub(super) const MIGRATION_54_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_networked_worker_expiry_outbox (
        event_ulid TEXT PRIMARY KEY,
        worker_id TEXT NOT NULL,
        run_ulid TEXT NOT NULL,
        lease_ulid TEXT NOT NULL,
        event_json TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1,
        UNIQUE(lease_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_networked_worker_expiry_outbox_created
        ON runtime_networked_worker_expiry_outbox(created_at_unix_ms ASC, event_ulid ASC);
    CREATE TRIGGER IF NOT EXISTS trg_runtime_networked_worker_expiry_outbox_prevent_update
    BEFORE UPDATE ON runtime_networked_worker_expiry_outbox BEGIN
        SELECT RAISE(ABORT, 'runtime_networked_worker_expiry_outbox is immutable');
    END;
"#;

/// Migration 55: durable networked-worker fleet state, including active leases.
pub(super) const MIGRATION_55_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_networked_worker_fleet (
        worker_id TEXT PRIMARY KEY,
        record_json TEXT NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1
    );
"#;

/// Migration 56: monotonic compare-and-swap authority for the durable worker fleet.
pub(super) const MIGRATION_56_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_networked_worker_fleet_meta (
        singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
        generation INTEGER NOT NULL CHECK (generation >= 0),
        updated_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1
    );
    INSERT OR IGNORE INTO runtime_networked_worker_fleet_meta (
        singleton_key, generation, updated_at_unix_ms, schema_version
    ) VALUES (1, 0, 0, 1);
"#;

/// Migration 57: exact lease-bound authority for node-backed worker dispatch.
pub(super) const MIGRATION_57_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_networked_worker_dispatch_claims (
        remote_request_ulid TEXT PRIMARY KEY,
        node_request_ulid TEXT NOT NULL UNIQUE,
        worker_id TEXT NOT NULL,
        lease_ulid TEXT NOT NULL,
        run_ulid TEXT NOT NULL,
        issued_fleet_generation INTEGER NOT NULL CHECK (issued_fleet_generation >= 0),
        dispatch_fleet_generation INTEGER CHECK (
            dispatch_fleet_generation IS NULL OR dispatch_fleet_generation >= 0
        ),
        revoked_fleet_generation INTEGER CHECK (
            revoked_fleet_generation IS NULL OR revoked_fleet_generation >= 0
        ),
        lease_expires_at_unix_ms INTEGER NOT NULL,
        capability TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK (
            state IN ('queued', 'in_flight', 'reconciling', 'settled', 'cancelled', 'failed_closed')
        ),
        reconciliation_disposition TEXT,
        terminal_reason_code TEXT,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        completed_at_unix_ms INTEGER,
        schema_version INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_networked_worker_dispatch_claims_lease
        ON runtime_networked_worker_dispatch_claims(worker_id, lease_ulid, run_ulid, state);
    CREATE INDEX IF NOT EXISTS idx_runtime_networked_worker_dispatch_claims_state
        ON runtime_networked_worker_dispatch_claims(state, updated_at_unix_ms);
"#;

/// Migration 59: immutable terminal evidence outside the bounded dispatch-authority table.
pub(super) const MIGRATION_59_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_networked_worker_dispatch_claim_terminal_evidence (
        remote_request_ulid TEXT PRIMARY KEY,
        node_request_ulid TEXT NOT NULL UNIQUE,
        worker_id TEXT NOT NULL,
        lease_ulid TEXT NOT NULL,
        run_ulid TEXT NOT NULL,
        issued_fleet_generation INTEGER NOT NULL CHECK (issued_fleet_generation >= 0),
        dispatch_fleet_generation INTEGER CHECK (
            dispatch_fleet_generation IS NULL OR dispatch_fleet_generation >= 0
        ),
        revoked_fleet_generation INTEGER CHECK (
            revoked_fleet_generation IS NULL OR revoked_fleet_generation >= 0
        ),
        lease_expires_at_unix_ms INTEGER NOT NULL,
        capability TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('settled', 'cancelled', 'failed_closed')),
        reconciliation_disposition TEXT,
        terminal_reason_code TEXT,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        completed_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_networked_worker_dispatch_claim_terminal_state
        ON runtime_networked_worker_dispatch_claim_terminal_evidence(state, updated_at_unix_ms);

    INSERT INTO runtime_networked_worker_dispatch_claim_terminal_evidence (
        remote_request_ulid, node_request_ulid, worker_id, lease_ulid, run_ulid,
        issued_fleet_generation, dispatch_fleet_generation, revoked_fleet_generation,
        lease_expires_at_unix_ms, capability, request_sha256, state,
        reconciliation_disposition, terminal_reason_code, created_at_unix_ms,
        updated_at_unix_ms, completed_at_unix_ms, schema_version
    )
    SELECT remote_request_ulid, node_request_ulid, worker_id, lease_ulid, run_ulid,
           issued_fleet_generation, dispatch_fleet_generation, revoked_fleet_generation,
           lease_expires_at_unix_ms, capability, request_sha256, state,
           reconciliation_disposition, terminal_reason_code, created_at_unix_ms,
           updated_at_unix_ms, completed_at_unix_ms, schema_version
    FROM runtime_networked_worker_dispatch_claims
    WHERE schema_version = 1
      AND state IN ('settled', 'cancelled', 'failed_closed')
      AND completed_at_unix_ms IS NOT NULL;

    DELETE FROM runtime_networked_worker_dispatch_claims
    WHERE schema_version = 1
      AND state IN ('settled', 'cancelled', 'failed_closed')
      AND completed_at_unix_ms IS NOT NULL;

    CREATE TRIGGER IF NOT EXISTS trg_runtime_networked_worker_dispatch_claim_terminal_prevent_update
    BEFORE UPDATE ON runtime_networked_worker_dispatch_claim_terminal_evidence BEGIN
        SELECT RAISE(ABORT, 'runtime_networked_worker_dispatch_claim_terminal_evidence is immutable');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_networked_worker_dispatch_claim_terminal_prevent_delete
    BEFORE DELETE ON runtime_networked_worker_dispatch_claim_terminal_evidence BEGIN
        SELECT RAISE(ABORT, 'runtime_networked_worker_dispatch_claim_terminal_evidence is immutable');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_networked_worker_dispatch_claims_reject_archived_remote
    BEFORE INSERT ON runtime_networked_worker_dispatch_claims
    WHEN EXISTS (
        SELECT 1 FROM runtime_networked_worker_dispatch_claim_terminal_evidence
        WHERE remote_request_ulid = NEW.remote_request_ulid
    ) BEGIN
        SELECT RAISE(ABORT, 'networked worker dispatch remote request identity is archived');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_networked_worker_dispatch_claims_reject_archived_node
    BEFORE INSERT ON runtime_networked_worker_dispatch_claims
    WHEN EXISTS (
        SELECT 1 FROM runtime_networked_worker_dispatch_claim_terminal_evidence
        WHERE node_request_ulid = NEW.node_request_ulid
    ) BEGIN
        SELECT RAISE(ABORT, 'networked worker dispatch node request identity is archived');
    END;
"#;

/// Migration 60: exact reservation, release, and acknowledgement evidence for worker payloads.
pub(super) const MIGRATION_60_SQL: &str = r#"
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN delivery_attempt_ulid TEXT;
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN delivery_token_sha256 TEXT;
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN delivery_reserved_at_unix_ms INTEGER;
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN payload_released_at_unix_ms INTEGER;
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN payload_release_fleet_generation INTEGER CHECK (
            payload_release_fleet_generation IS NULL OR payload_release_fleet_generation >= 0
        );
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN payload_acknowledged_at_unix_ms INTEGER;
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN delivery_disposition TEXT CHECK (
            delivery_disposition IS NULL OR delivery_disposition IN (
                'reserved_unreleased',
                'released_unacknowledged',
                'acknowledged',
                'legacy_unfenced_unknown'
            )
        );
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN delivery_payload_present INTEGER CHECK (
            delivery_payload_present IS NULL OR delivery_payload_present IN (0, 1)
        );

    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN delivery_attempt_ulid TEXT;
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN delivery_token_sha256 TEXT;
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN delivery_reserved_at_unix_ms INTEGER;
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN payload_released_at_unix_ms INTEGER;
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN payload_release_fleet_generation INTEGER CHECK (
            payload_release_fleet_generation IS NULL OR payload_release_fleet_generation >= 0
        );
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN payload_acknowledged_at_unix_ms INTEGER;
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN delivery_disposition TEXT CHECK (
            delivery_disposition IS NULL OR delivery_disposition IN (
                'reserved_unreleased',
                'released_unacknowledged',
                'acknowledged',
                'legacy_unfenced_unknown'
            )
        );
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN delivery_payload_present INTEGER CHECK (
            delivery_payload_present IS NULL OR delivery_payload_present IN (0, 1)
        );

    UPDATE runtime_networked_worker_dispatch_claims
    SET delivery_payload_present = 0, schema_version = 2
    WHERE schema_version = 1 AND state = 'queued';

    UPDATE runtime_networked_worker_dispatch_claims
    SET state = 'reconciling',
        reconciliation_disposition = 'legacy_unfenced_unknown',
        terminal_reason_code = 'worker.dispatch.legacy_unfenced_unknown',
        delivery_disposition = 'legacy_unfenced_unknown',
        delivery_payload_present = NULL,
        updated_at_unix_ms = CASE
            WHEN updated_at_unix_ms < created_at_unix_ms THEN created_at_unix_ms
            ELSE updated_at_unix_ms
        END,
        completed_at_unix_ms = NULL,
        schema_version = 2
    WHERE schema_version = 1 AND state IN ('in_flight', 'reconciling');

    UPDATE runtime_networked_worker_dispatch_claims
    SET delivery_disposition = CASE
            WHEN dispatch_fleet_generation IS NULL THEN NULL
            ELSE 'legacy_unfenced_unknown'
        END,
        delivery_payload_present = NULL,
        schema_version = 2
    WHERE schema_version = 1 AND state IN ('settled', 'cancelled', 'failed_closed');
"#;

/// Migration 61: bounded validated-result receipt evidence for exact replay and late settlement.
pub(super) const MIGRATION_61_SQL: &str = r#"
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN validated_result_sha256 TEXT CHECK (
            validated_result_sha256 IS NULL
            OR (
                length(validated_result_sha256) = 64
                AND validated_result_sha256 NOT GLOB '*[^0-9a-f]*'
            )
        );
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN result_observed_at_unix_ms INTEGER CHECK (
            result_observed_at_unix_ms IS NULL OR result_observed_at_unix_ms >= 0
        );

    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN validated_result_sha256 TEXT CHECK (
            validated_result_sha256 IS NULL
            OR (
                length(validated_result_sha256) = 64
                AND validated_result_sha256 NOT GLOB '*[^0-9a-f]*'
            )
        );
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN result_observed_at_unix_ms INTEGER CHECK (
            result_observed_at_unix_ms IS NULL OR result_observed_at_unix_ms >= 0
        );

    UPDATE runtime_networked_worker_dispatch_claims
    SET schema_version = 3
    WHERE schema_version = 2 AND state IN ('queued', 'in_flight', 'reconciling');
"#;

/// Migration 62: metadata-only cancellation authority for durable child tasks.
pub(super) const MIGRATION_62_SQL: &str = r#"
    ALTER TABLE orchestrator_background_tasks
        ADD COLUMN cancellation_context_json TEXT;

    UPDATE orchestrator_background_tasks
    SET state = 'failed',
        last_error = 'legacy delegation is missing durable ChildTask cancellation authority',
        result_json = json_object(
            'status', 'failed',
            'task_id', task_ulid,
            'reason', 'legacy_missing_child_task_context'
        ),
        updated_at_unix_ms = CASE
            WHEN updated_at_unix_ms < created_at_unix_ms THEN created_at_unix_ms
            ELSE updated_at_unix_ms
        END,
        completed_at_unix_ms = CASE
            WHEN updated_at_unix_ms < created_at_unix_ms THEN created_at_unix_ms
            ELSE updated_at_unix_ms
        END
    WHERE LOWER(TRIM(task_kind)) = 'delegation_prompt'
      AND delegation_json IS NOT NULL
      AND cancellation_context_json IS NULL
      AND LOWER(TRIM(state)) IN (
          'queued', 'pending', 'running', 'in_progress', 'paused', 'cancel_requested'
      );
"#;

/// Migration 63: monotonic component generations and immutable probe lifecycle evidence.
pub(super) const MIGRATION_63_SQL: &str = r#"
    UPDATE runtime_component_health
    SET health_json = json_set(health_json, '$.policy.max_probe_concurrency', 1)
    WHERE json_valid(health_json)
      AND json_extract(health_json, '$.schema_version') = 1
      AND json_extract(health_json, '$.policy.max_probe_concurrency') BETWEEN 2 AND 16;

    CREATE TABLE IF NOT EXISTS runtime_component_generation_heads (
        component_ulid TEXT PRIMARY KEY,
        last_generation INTEGER NOT NULL CHECK (last_generation > 0),
        updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= 0),
        schema_version INTEGER NOT NULL DEFAULT 1
    );
    INSERT INTO runtime_component_generation_heads (
        component_ulid, last_generation, updated_at_unix_ms, schema_version
    )
    SELECT component_ulid, generation, updated_at_unix_ms, 1
    FROM runtime_component_health
    WHERE generation > 0
      AND updated_at_unix_ms >= 0
      AND json_valid(health_json)
      AND json_extract(health_json, '$.schema_version') = 1;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_component_generation_heads_monotonic
    BEFORE UPDATE ON runtime_component_generation_heads
    WHEN NEW.last_generation < OLD.last_generation
    BEGIN
        SELECT RAISE(ABORT, 'runtime component generation head must advance monotonically');
    END;

    CREATE TABLE IF NOT EXISTS runtime_health_probe_begins (
        lease_ulid TEXT PRIMARY KEY CHECK (length(lease_ulid) BETWEEN 1 AND 128),
        component_ulid TEXT NOT NULL CHECK (length(component_ulid) BETWEEN 1 AND 128),
        expected_generation INTEGER NOT NULL CHECK (expected_generation > 0),
        authority_class TEXT NOT NULL CHECK (authority_class IN (
            'observe_only', 'read_only', 'scoped_mutation', 'privileged_mutation'
        )),
        source_state TEXT NOT NULL CHECK (source_state IN ('cooldown', 'quarantined')),
        security_quarantine_before INTEGER NOT NULL CHECK (security_quarantine_before IN (0, 1)),
        reason_code TEXT NOT NULL CHECK (
            length(reason_code) BETWEEN 1 AND 128 AND trim(reason_code) = reason_code
        ),
        authorization_evidence_sha256 TEXT CHECK (
            authorization_evidence_sha256 IS NULL
            OR (
                length(authorization_evidence_sha256) = 64
                AND authorization_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
            )
        ),
        lease_json TEXT NOT NULL CHECK (length(lease_json) BETWEEN 2 AND 4096),
        begun_at_unix_ms INTEGER NOT NULL CHECK (begun_at_unix_ms >= 0),
        schema_version INTEGER NOT NULL DEFAULT 1,
        UNIQUE(lease_ulid, component_ulid, expected_generation)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_health_probe_begins_component_generation
        ON runtime_health_probe_begins(component_ulid, expected_generation, begun_at_unix_ms ASC);
    CREATE TRIGGER IF NOT EXISTS trg_runtime_health_probe_begins_prevent_update
    BEFORE UPDATE ON runtime_health_probe_begins BEGIN
        SELECT RAISE(ABORT, 'runtime_health_probe_begins is immutable');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_health_probe_begins_prevent_delete
    BEFORE DELETE ON runtime_health_probe_begins BEGIN
        SELECT RAISE(ABORT, 'runtime_health_probe_begins is immutable');
    END;

    UPDATE runtime_health_probe_leases
    SET schema_version = 2
    WHERE schema_version = 1;

    CREATE TABLE IF NOT EXISTS runtime_health_probe_terminal_evidence (
        lease_ulid TEXT PRIMARY KEY CHECK (length(lease_ulid) BETWEEN 1 AND 128),
        component_ulid TEXT NOT NULL CHECK (length(component_ulid) BETWEEN 1 AND 128),
        expected_generation INTEGER NOT NULL CHECK (expected_generation > 0),
        authority_class TEXT NOT NULL CHECK (authority_class IN (
            'observe_only', 'read_only', 'scoped_mutation', 'privileged_mutation'
        )),
        source_state TEXT NOT NULL CHECK (source_state IN ('cooldown', 'quarantined')),
        result_state TEXT NOT NULL CHECK (result_state IN ('healthy', 'quarantined')),
        disposition TEXT NOT NULL CHECK (disposition IN (
            'passed', 'failed', 'inconclusive', 'denied_mutating_probe'
        )),
        mutation_attempted INTEGER NOT NULL CHECK (mutation_attempted IN (0, 1)),
        security_quarantine_before INTEGER NOT NULL CHECK (security_quarantine_before IN (0, 1)),
        security_quarantine_after INTEGER NOT NULL CHECK (security_quarantine_after IN (0, 1)),
        health_mutated INTEGER NOT NULL CHECK (health_mutated IN (0, 1)),
        terminal_kind TEXT NOT NULL CHECK (terminal_kind IN ('settlement', 'reconciliation')),
        reason_code TEXT NOT NULL CHECK (
            length(reason_code) BETWEEN 1 AND 128 AND trim(reason_code) = reason_code
        ),
        settlement_json TEXT CHECK (
            settlement_json IS NULL OR length(settlement_json) BETWEEN 2 AND 4096
        ),
        probe_evidence_sha256 TEXT CHECK (
            probe_evidence_sha256 IS NULL
            OR (
                length(probe_evidence_sha256) = 64
                AND probe_evidence_sha256 NOT GLOB '*[^0-9a-f]*'
            )
        ),
        completed_at_unix_ms INTEGER NOT NULL CHECK (completed_at_unix_ms >= 0),
        settled_at_unix_ms INTEGER NOT NULL CHECK (settled_at_unix_ms >= 0),
        schema_version INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY(lease_ulid) REFERENCES runtime_health_probe_begins(lease_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_health_probe_terminal_component_generation
        ON runtime_health_probe_terminal_evidence(
            component_ulid, expected_generation, settled_at_unix_ms DESC
        );
    CREATE INDEX IF NOT EXISTS idx_runtime_health_probe_terminal_disposition
        ON runtime_health_probe_terminal_evidence(disposition, settled_at_unix_ms DESC);
    CREATE TRIGGER IF NOT EXISTS trg_runtime_health_probe_terminal_prevent_update
    BEFORE UPDATE ON runtime_health_probe_terminal_evidence BEGIN
        SELECT RAISE(ABORT, 'runtime_health_probe_terminal_evidence is immutable');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_health_probe_terminal_prevent_delete
    BEFORE DELETE ON runtime_health_probe_terminal_evidence BEGIN
        SELECT RAISE(ABORT, 'runtime_health_probe_terminal_evidence is immutable');
    END;
"#;

/// Migration 64: immutable resulting-health snapshots for exact probe-settlement replay.
pub(super) const MIGRATION_64_SQL: &str = r#"
    ALTER TABLE runtime_health_probe_terminal_evidence
        ADD COLUMN result_health_json TEXT CHECK (
            result_health_json IS NULL OR length(result_health_json) BETWEEN 2 AND 4096
        );
"#;

/// Migration 65: host-derived actor attribution for quarantine-recovery probe begins.
pub(super) const MIGRATION_65_SQL: &str = r#"
    ALTER TABLE runtime_health_probe_begins
        ADD COLUMN authorized_actor_id_sha256 TEXT CHECK (
            authorized_actor_id_sha256 IS NULL
            OR (
                length(authorized_actor_id_sha256) = 64
                AND authorized_actor_id_sha256 NOT GLOB '*[^0-9a-f]*'
            )
        );
"#;

// Migration 66: dedicated child-session identity for delegated background runs.
pub(super) const MIGRATION_66_SQL: &str = r#"
    ALTER TABLE orchestrator_background_tasks
        ADD COLUMN child_session_ulid TEXT REFERENCES orchestrator_sessions(session_ulid);

    CREATE INDEX IF NOT EXISTS idx_orchestrator_background_tasks_child_session
        ON orchestrator_background_tasks(child_session_ulid, created_at_unix_ms DESC);

    UPDATE orchestrator_background_tasks
    SET state = 'failed',
        last_error = 'legacy delegation is missing dedicated child-session authority',
        result_json = json_object(
            'status', 'failed',
            'task_id', task_ulid,
            'reason', 'legacy_missing_child_session'
        ),
        updated_at_unix_ms = CASE
            WHEN updated_at_unix_ms < created_at_unix_ms THEN created_at_unix_ms
            ELSE updated_at_unix_ms
        END,
        completed_at_unix_ms = CASE
            WHEN updated_at_unix_ms < created_at_unix_ms THEN created_at_unix_ms
            ELSE updated_at_unix_ms
        END
    WHERE LOWER(TRIM(task_kind)) = 'delegation_prompt'
      AND delegation_json IS NOT NULL
      AND child_session_ulid IS NULL
      AND LOWER(TRIM(state)) IN (
          'queued', 'pending', 'running', 'in_progress', 'paused', 'cancel_requested'
      );
"#;

/// Migration 67: controller revision and worker execution-generation fences.
pub(super) const MIGRATION_67_SQL: &str = r#"
    ALTER TABLE orchestrator_background_tasks
        ADD COLUMN revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0);

    ALTER TABLE orchestrator_background_tasks
        ADD COLUMN execution_generation INTEGER NOT NULL DEFAULT 0
        CHECK (execution_generation >= 0);
"#;

/// Migration 68: durable provider-configuration authority and reload evidence.
pub(super) const MIGRATION_68_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_provider_configuration_head (
        singleton_key TEXT PRIMARY KEY CHECK (singleton_key = 'model_provider'),
        epoch INTEGER NOT NULL CHECK (epoch > 0),
        updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= 0),
        schema_version INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS runtime_provider_configuration_events (
        event_ulid TEXT PRIMARY KEY,
        from_epoch INTEGER,
        to_epoch INTEGER NOT NULL CHECK (to_epoch > 0),
        transition_kind TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
        schema_version INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_provider_configuration_events_epoch
        ON runtime_provider_configuration_events(to_epoch, created_at_unix_ms ASC);
    CREATE TRIGGER IF NOT EXISTS trg_runtime_provider_configuration_events_prevent_update
    BEFORE UPDATE ON runtime_provider_configuration_events BEGIN
        SELECT RAISE(ABORT, 'runtime_provider_configuration_events is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_provider_configuration_events_prevent_delete
    BEFORE DELETE ON runtime_provider_configuration_events BEGIN
        SELECT RAISE(ABORT, 'runtime_provider_configuration_events is append-only');
    END;
"#;

/// Migration 69: durable configuration-scoped provider attempts for runless effects.
pub(super) const MIGRATION_69_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_provider_attempt_starts (
        attempt_ulid TEXT PRIMARY KEY,
        configuration_epoch INTEGER NOT NULL CHECK (configuration_epoch > 0),
        provider_id TEXT NOT NULL,
        model_id TEXT NOT NULL,
        started_at_unix_ms INTEGER NOT NULL CHECK (started_at_unix_ms >= 0),
        schema_version INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_provider_attempt_starts_epoch
        ON runtime_provider_attempt_starts(configuration_epoch, started_at_unix_ms ASC);

    CREATE TABLE IF NOT EXISTS runtime_provider_attempt_completions (
        attempt_ulid TEXT PRIMARY KEY,
        configuration_epoch INTEGER NOT NULL CHECK (configuration_epoch > 0),
        provider_id TEXT NOT NULL,
        model_id TEXT NOT NULL,
        outcome TEXT NOT NULL CHECK (outcome IN ('success', 'failure')),
        error_class TEXT,
        completed_at_unix_ms INTEGER NOT NULL CHECK (completed_at_unix_ms >= 0),
        schema_version INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY(attempt_ulid) REFERENCES runtime_provider_attempt_starts(attempt_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_provider_attempt_completions_epoch
        ON runtime_provider_attempt_completions(configuration_epoch, completed_at_unix_ms ASC);

    CREATE TRIGGER IF NOT EXISTS trg_runtime_provider_attempt_starts_prevent_update
    BEFORE UPDATE ON runtime_provider_attempt_starts BEGIN
        SELECT RAISE(ABORT, 'runtime_provider_attempt_starts is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_provider_attempt_starts_prevent_delete
    BEFORE DELETE ON runtime_provider_attempt_starts BEGIN
        SELECT RAISE(ABORT, 'runtime_provider_attempt_starts is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_provider_attempt_completions_prevent_update
    BEFORE UPDATE ON runtime_provider_attempt_completions BEGIN
        SELECT RAISE(ABORT, 'runtime_provider_attempt_completions is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_provider_attempt_completions_prevent_delete
    BEFORE DELETE ON runtime_provider_attempt_completions BEGIN
        SELECT RAISE(ABORT, 'runtime_provider_attempt_completions is append-only');
    END;
"#;

/// Migration 70: explicit row versions for cleanup and persisted quarantine evidence.
pub(super) const MIGRATION_70_SQL: &str = r#"
    ALTER TABLE runtime_cleanup_reports
        ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1
        CHECK (schema_version > 0);
    ALTER TABLE runtime_state_quarantine
        ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1
        CHECK (schema_version > 0);
"#;

/// Migration 71: durable run-generation authority for networked-worker callbacks.
///
/// Existing rows remain readable for reconciliation, but nullable legacy bindings can never
/// authorize a new callback through the generation-fenced result path.
pub(super) const MIGRATION_71_SQL: &str = r#"
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN session_ulid TEXT;
    ALTER TABLE runtime_networked_worker_dispatch_claims
        ADD COLUMN run_generation INTEGER CHECK (
            run_generation IS NULL OR run_generation > 0
        );
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN session_ulid TEXT;
    ALTER TABLE runtime_networked_worker_dispatch_claim_terminal_evidence
        ADD COLUMN run_generation INTEGER CHECK (
            run_generation IS NULL OR run_generation > 0
        );

    -- Preserve any impossible queued delivery evidence so the compatibility scan can quarantine it.
    UPDATE runtime_networked_worker_dispatch_claims
    SET state = 'reconciling',
        reconciliation_disposition = 'legacy_missing_run_generation',
        terminal_reason_code = 'worker.dispatch.legacy_missing_run_generation',
        delivery_attempt_ulid = NULL,
        delivery_token_sha256 = NULL,
        delivery_reserved_at_unix_ms = NULL,
        payload_released_at_unix_ms = NULL,
        payload_release_fleet_generation = NULL,
        payload_acknowledged_at_unix_ms = NULL,
        delivery_disposition = 'legacy_unfenced_unknown',
        delivery_payload_present = NULL
    WHERE state = 'queued'
      AND schema_version = 3
      AND dispatch_fleet_generation IS NULL
      AND revoked_fleet_generation IS NULL
      AND reconciliation_disposition IS NULL
      AND terminal_reason_code IS NULL
      AND completed_at_unix_ms IS NULL
      AND delivery_attempt_ulid IS NULL
      AND delivery_token_sha256 IS NULL
      AND delivery_reserved_at_unix_ms IS NULL
      AND payload_released_at_unix_ms IS NULL
      AND payload_release_fleet_generation IS NULL
      AND payload_acknowledged_at_unix_ms IS NULL
      AND delivery_disposition IS NULL
      AND delivery_payload_present IN (0, 1)
      AND validated_result_sha256 IS NULL
      AND result_observed_at_unix_ms IS NULL;

    -- Migrate only an exact v70 in-flight shape. Existing reconciling rows retain their original
    -- disposition and reason, while malformed in-flight evidence remains visible to quarantine.
    UPDATE runtime_networked_worker_dispatch_claims
    SET state = 'reconciling',
        reconciliation_disposition = 'legacy_missing_run_generation',
        terminal_reason_code = 'worker.dispatch.legacy_missing_run_generation'
    WHERE state = 'in_flight'
      AND schema_version = 3
      AND dispatch_fleet_generation IS NOT NULL
      AND issued_fleet_generation <= dispatch_fleet_generation
      AND revoked_fleet_generation IS NULL
      AND reconciliation_disposition IS NULL
      AND terminal_reason_code IS NULL
      AND completed_at_unix_ms IS NULL
      AND created_at_unix_ms >= 0
      AND updated_at_unix_ms >= created_at_unix_ms
      AND lease_expires_at_unix_ms > created_at_unix_ms
      AND length(remote_request_ulid) BETWEEN 1 AND 128
      AND remote_request_ulid = trim(remote_request_ulid)
      AND remote_request_ulid NOT GLOB '*[^-A-Za-z0-9_./:]*'
      AND length(node_request_ulid) BETWEEN 1 AND 128
      AND node_request_ulid = trim(node_request_ulid)
      AND node_request_ulid NOT GLOB '*[^-A-Za-z0-9_./:]*'
      AND length(lease_ulid) BETWEEN 1 AND 128
      AND lease_ulid = trim(lease_ulid)
      AND lease_ulid NOT GLOB '*[^-A-Za-z0-9_./:]*'
      AND length(run_ulid) BETWEEN 1 AND 128
      AND run_ulid = trim(run_ulid)
      AND run_ulid NOT GLOB '*[^-A-Za-z0-9_./:]*'
      AND length(worker_id) BETWEEN 1 AND 128
      AND worker_id = trim(worker_id)
      AND worker_id NOT GLOB '*[^-A-Za-z0-9_.:]*'
      AND length(capability) BETWEEN 1 AND 256
      AND capability = trim(capability)
      AND capability NOT GLOB '*[^-A-Za-z0-9_.:]*'
      AND length(request_sha256) = 64
      AND request_sha256 NOT GLOB '*[^0-9A-Fa-f]*'
      AND length(delivery_attempt_ulid) BETWEEN 1 AND 128
      AND delivery_attempt_ulid = trim(delivery_attempt_ulid)
      AND delivery_attempt_ulid NOT GLOB '*[^-A-Za-z0-9_./:]*'
      AND length(delivery_token_sha256) = 64
      AND delivery_token_sha256 NOT GLOB '*[^0-9A-Fa-f]*'
      AND delivery_reserved_at_unix_ms BETWEEN created_at_unix_ms AND updated_at_unix_ms
      AND validated_result_sha256 IS NULL
      AND result_observed_at_unix_ms IS NULL
      AND (
            (
                delivery_disposition = 'reserved_unreleased'
                AND payload_released_at_unix_ms IS NULL
                AND payload_release_fleet_generation IS NULL
                AND payload_acknowledged_at_unix_ms IS NULL
                AND delivery_payload_present = 1
            )
            OR (
                delivery_disposition = 'released_unacknowledged'
                AND payload_released_at_unix_ms BETWEEN
                    delivery_reserved_at_unix_ms AND updated_at_unix_ms
                AND payload_release_fleet_generation = dispatch_fleet_generation
                AND payload_acknowledged_at_unix_ms IS NULL
                AND delivery_payload_present = 0
            )
            OR (
                delivery_disposition = 'acknowledged'
                AND payload_released_at_unix_ms BETWEEN
                    delivery_reserved_at_unix_ms AND updated_at_unix_ms
                AND payload_release_fleet_generation = dispatch_fleet_generation
                AND payload_acknowledged_at_unix_ms BETWEEN
                    payload_released_at_unix_ms AND updated_at_unix_ms
                AND delivery_payload_present = 0
            )
      );
"#;

/// Migration 77: honest completion evidence for cancelled runless provider effects.
///
/// SQLite cannot widen a CHECK constraint in place. Rebuilding the child table preserves
/// every immutable v69 completion while retaining its foreign key, index, and append-only
/// triggers. The only contract change is the additional `outcome_unknown` value.
pub(super) const MIGRATION_77_SQL: &str = r#"
    CREATE TABLE runtime_provider_attempt_completions_v77 (
        attempt_ulid TEXT PRIMARY KEY,
        configuration_epoch INTEGER NOT NULL CHECK (configuration_epoch > 0),
        provider_id TEXT NOT NULL,
        model_id TEXT NOT NULL,
        outcome TEXT NOT NULL CHECK (
            outcome IN ('success', 'failure', 'outcome_unknown')
        ),
        error_class TEXT,
        completed_at_unix_ms INTEGER NOT NULL CHECK (completed_at_unix_ms >= 0),
        schema_version INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY(attempt_ulid) REFERENCES runtime_provider_attempt_starts(attempt_ulid)
    );

    INSERT INTO runtime_provider_attempt_completions_v77 (
        attempt_ulid, configuration_epoch, provider_id, model_id,
        outcome, error_class, completed_at_unix_ms, schema_version
    )
    SELECT
        attempt_ulid, configuration_epoch, provider_id, model_id,
        outcome, error_class, completed_at_unix_ms, schema_version
    FROM runtime_provider_attempt_completions;

    DROP TABLE runtime_provider_attempt_completions;
    ALTER TABLE runtime_provider_attempt_completions_v77
        RENAME TO runtime_provider_attempt_completions;

    CREATE INDEX idx_runtime_provider_attempt_completions_epoch
        ON runtime_provider_attempt_completions(
            configuration_epoch,
            completed_at_unix_ms ASC
        );
    CREATE TRIGGER trg_runtime_provider_attempt_completions_prevent_update
    BEFORE UPDATE ON runtime_provider_attempt_completions BEGIN
        SELECT RAISE(ABORT, 'runtime_provider_attempt_completions is append-only');
    END;
    CREATE TRIGGER trg_runtime_provider_attempt_completions_prevent_delete
    BEFORE DELETE ON runtime_provider_attempt_completions BEGIN
        SELECT RAISE(ABORT, 'runtime_provider_attempt_completions is append-only');
    END;
"#;

/// Current durable schema for networked-worker dispatch claims.
const NETWORKED_WORKER_DISPATCH_CLAIM_SCHEMA_VERSION: u32 = 3;
const RUNTIME_CLEANUP_REPORT_ROW_SCHEMA_VERSION: u32 = 1;
const RUNTIME_STATE_QUARANTINE_ROW_SCHEMA_VERSION: u32 = 1;
/// Current durable row schema for active health-probe leases.
const RUNTIME_HEALTH_PROBE_ACTIVE_ROW_SCHEMA_VERSION: u32 = 2;
const RUNTIME_HEALTH_PROBE_BEGIN_ROW_SCHEMA_VERSION: u32 = 2;
const RUNTIME_HEALTH_PROBE_TERMINAL_ROW_SCHEMA_VERSION: u32 = 2;
const RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION: u32 = 1;
const RUNTIME_HEALTH_RECONCILIATION_MAX_RECORDS: usize = 256;
const RUNTIME_HEALTH_CONTRACT_MAX_BYTES: usize = 4_096;

/// One component contract to activate under a fresh durable generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthComponentActivation {
    pub component_id: RuntimeInstanceId,
    pub authority_class: RuntimeAuthorityClass,
    pub fallback_component_id: Option<RuntimeInstanceId>,
    pub fallback_authority_class: Option<RuntimeAuthorityClass>,
    pub policy: palyra_common::runtime_contracts::CircuitBreakerPolicy,
    pub reason_code: String,
}

/// Outcome of an atomic component activation batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthActivationOutcome {
    pub generations: BTreeMap<String, RuntimeGeneration>,
}

/// Outcome of atomically activating provider health and advancing configuration authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRuntimeActivationOutcome {
    pub configuration_epoch: RuntimeGeneration,
    pub health: RuntimeHealthActivationOutcome,
    pub superseded_provider_lanes: u64,
}

/// Ordinary serving observation applied to one exact component generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthObservationRequest {
    pub component_id: RuntimeInstanceId,
    pub expected_generation: RuntimeGeneration,
    pub succeeded: bool,
    pub reason_code: String,
    pub observed_at_unix_ms: i64,
}

/// Resulting durable health projection after an ordinary observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthObservationOutcome {
    pub health: RuntimeComponentHealthV1,
}

/// Result of an ordinary health observation fenced by one exact run generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunScopedRuntimeHealthObservationOutcome {
    /// The run fence matched and the health observation was durably applied.
    Applied(RuntimeHealthObservationOutcome),
    /// The run fence changed before the health observation could be applied.
    Stale { expected_generation: Option<RuntimeGeneration> },
}

/// Exact request to begin one host-owned non-mutating health probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthProbeBeginRequest {
    pub lease: HealthProbeLeaseV1,
    pub reason_code: String,
    /// Legacy caller-supplied digest retained only as bounded audit metadata.
    pub authorization_evidence_sha256: Option<String>,
    /// Host-derived digest of the principal bound to the authenticated admin credential.
    pub authorized_actor_id_sha256: Option<String>,
}

/// Durable begin outcome, including idempotent replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthProbeBeginOutcome {
    pub health: RuntimeComponentHealthV1,
    pub lease: HealthProbeLeaseV1,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RuntimeHealthProbeBeginEvidence {
    lease: HealthProbeLeaseV1,
    source_state: RuntimeHealthState,
    security_quarantine_before: bool,
    reason_code: String,
    authorization_evidence_sha256: Option<String>,
    authorized_actor_id_sha256: Option<String>,
}

/// Exact request to settle one active health-probe lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthProbeSettlementRequest {
    pub settlement: HealthProbeSettlementV1,
    pub probe_evidence_sha256: Option<String>,
}

/// Durable terminal health-probe outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthProbeSettlementOutcome {
    pub health: RuntimeComponentHealthV1,
    pub disposition: HealthProbeDisposition,
    pub completed_at_unix_ms: i64,
    pub replayed: bool,
    pub health_mutated: bool,
}

/// Atomic operator quarantine-clear request and its hash-only audit event.
#[derive(Debug, Clone)]
pub struct RuntimeHealthQuarantineClearRequest {
    pub clear: QuarantineClearRequest,
    pub audit_event: JournalAppendRequest,
    pub cleared_at_unix_ms: i64,
}

/// Durable projection returned after an exact operator quarantine clear.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthQuarantineClearOutcome {
    pub health: RuntimeComponentHealthV1,
    pub audit_event_sha256: Option<String>,
    pub audit_payload_redacted: bool,
}

/// Bounded reconciliation mode for supported health-probe residue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeHealthProbeReconciliationMode {
    Startup,
    Periodic,
}

/// Aggregate result of one bounded health-probe reconciliation pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeHealthProbeReconciliationOutcome {
    pub examined: usize,
    pub settled_inconclusive: usize,
    pub repaired_stranded_health: usize,
    pub retired_orphan_leases: usize,
    pub skipped_generation_mismatches: usize,
    pub remaining: bool,
}

/// One atomic durable networked-worker fleet snapshot and its write generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkerFleetSnapshot {
    pub generation: u64,
    pub records: std::collections::BTreeMap<String, palyra_workerd::WorkerFleetRecord>,
}

/// Lifecycle state of one exact node-backed worker dispatch claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkedWorkerDispatchClaimState {
    Queued,
    InFlight,
    Reconciling,
    Settled,
    Cancelled,
    FailedClosed,
}

impl NetworkedWorkerDispatchClaimState {
    fn parse(value: &str) -> Result<Self, JournalError> {
        match value {
            "queued" => Ok(Self::Queued),
            "in_flight" => Ok(Self::InFlight),
            "reconciling" => Ok(Self::Reconciling),
            "settled" => Ok(Self::Settled),
            "cancelled" => Ok(Self::Cancelled),
            "failed_closed" => Ok(Self::FailedClosed),
            _ => Err(JournalError::InvalidArgument(
                "networked worker dispatch claim state is unsupported".to_owned(),
            )),
        }
    }
}

/// Metadata-only authority binding a remote request to an exact durable worker lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkerDispatchClaim {
    pub schema_version: u32,
    pub remote_request_id: String,
    pub node_request_id: String,
    pub worker_id: String,
    pub lease_id: String,
    pub session_id: Option<String>,
    pub run_id: String,
    pub run_generation: Option<RuntimeGeneration>,
    pub issued_fleet_generation: u64,
    pub dispatch_fleet_generation: Option<u64>,
    pub revoked_fleet_generation: Option<u64>,
    pub lease_expires_at_unix_ms: i64,
    pub capability: String,
    pub request_sha256: String,
    pub state: NetworkedWorkerDispatchClaimState,
    pub delivery_attempt_id: Option<String>,
    pub(crate) delivery_token_sha256: Option<String>,
    pub delivery_reserved_at_unix_ms: Option<i64>,
    pub payload_released_at_unix_ms: Option<i64>,
    pub payload_release_fleet_generation: Option<u64>,
    pub payload_acknowledged_at_unix_ms: Option<i64>,
    pub delivery_disposition: Option<String>,
    pub delivery_payload_present: Option<bool>,
    pub validated_result_sha256: Option<String>,
    pub result_observed_at_unix_ms: Option<i64>,
    pub reconciliation_disposition: Option<String>,
    pub terminal_reason_code: Option<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub completed_at_unix_ms: Option<i64>,
}

/// Request to establish a dispatch claim after the exact lease is durably assigned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkerDispatchClaimCreateRequest {
    pub remote_request_id: String,
    pub node_request_id: String,
    pub worker_id: String,
    pub lease_id: String,
    pub session_id: String,
    pub run_id: String,
    pub run_generation: RuntimeGeneration,
    pub lease_expires_at_unix_ms: i64,
    pub capability: String,
    pub request_sha256: String,
}

/// Exact metadata required to create or rotate an unreleased delivery reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkerDeliveryReservationRequest {
    pub remote_request_id: String,
    pub node_request_id: String,
    pub request_sha256: String,
    pub delivery_attempt_id: String,
    pub delivery_token_sha256: String,
    pub observed_at_unix_ms: i64,
}

/// Durable result of creating one metadata-only delivery reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkedWorkerDeliveryReservationOutcome {
    Authorized { fleet_generation: u64 },
    Rejected,
}

/// Compatibility alias retained for test fixtures using the pre-fence dispatch helper.
#[cfg(test)]
pub type NetworkedWorkerDispatchBeginOutcome = NetworkedWorkerDeliveryReservationOutcome;

/// Exact payload-release authorization presented by the authenticated node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkerPayloadReleaseRequest {
    pub node_request_id: String,
    pub delivery_attempt_id: String,
    pub delivery_token: String,
    pub reporting_worker_id: String,
    pub observed_at_unix_ms: i64,
}

/// Durable result of the one-time payload-release compare-and-swap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkedWorkerPayloadReleaseOutcome {
    Released,
    AlreadyReleased,
    Rejected,
}

/// Exact delivery acknowledgement presented by the authenticated node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkerPayloadAcknowledgementRequest {
    pub node_request_id: String,
    pub delivery_attempt_id: String,
    pub delivery_token: String,
    pub reporting_worker_id: String,
    pub observed_at_unix_ms: i64,
}

/// Durable result of acknowledging one exact released payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkedWorkerPayloadAcknowledgementOutcome {
    Acknowledged,
    AlreadyAcknowledged,
    Rejected,
}

/// Whether a node-returned result matches the exact active durable dispatch claim.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkedWorkerResultAuthorizationOutcome {
    Authorized,
    Rejected,
}

/// Outcome of rolling back an exact dispatch before the raw payload leaves the daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkedWorkerDispatchAbortBeforeReleaseOutcome {
    Aborted,
    AlreadyAborted,
    NotAbortable,
    Missing,
}

/// Outcome of cancelling a claim before the node receives its payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkedWorkerDispatchCancelOutcome {
    Cancelled,
    AlreadyCancelled,
    InFlight,
    Missing,
}

/// Exact lease authority revoked by a fleet lifecycle transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkerLeaseRevocation {
    pub worker_id: String,
    pub lease_id: String,
    pub run_id: String,
    pub reason_code: String,
}

/// Counts claim transitions committed with one exact lease revocation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NetworkedWorkerDispatchRevocationOutcome {
    pub cancelled_queued: usize,
    pub reconciling_in_flight: usize,
}

/// Metadata required to settle the exact claim that authorized a remote effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkerDispatchSettlement {
    pub remote_request_id: String,
    pub worker_id: String,
    pub lease_id: String,
    pub session_id: String,
    pub run_id: String,
    pub run_generation: RuntimeGeneration,
    pub delivery_attempt_id: Option<String>,
    pub validated_result_sha256: String,
    /// Host-observed receipt time; worker-supplied clocks never authorize settlement.
    pub observed_at_unix_ms: i64,
}

/// One durable networked-worker expiry outbox record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkedWorkerExpiryOutboxRecord {
    pub event_id: String,
    pub event: palyra_workerd::WorkerLifecycleEvent,
}

impl NetworkedWorkerExpiryOutboxRecord {
    /// Builds a validated record with the deterministic exact-lease event identity.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the event is not exact TTL-expiry evidence.
    #[cfg(test)]
    pub fn from_event(event: palyra_workerd::WorkerLifecycleEvent) -> Result<Self, JournalError> {
        let event_id = palyra_workerd::networked_worker_expiry_event_id(&event)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        Ok(Self { event_id, event })
    }

    fn validate(&self) -> Result<(), JournalError> {
        let expected = palyra_workerd::networked_worker_expiry_event_id(&self.event)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        if self.event_id != expected {
            return Err(JournalError::InvalidArgument(
                "networked worker expiry outbox event id does not match exact evidence".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Stable authority returned after a concrete provider effect is durably started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAttemptRuntimeAuthority {
    /// Owning session bound to the active Provider lane.
    pub session_id: RuntimeSessionId,
    /// Owning run bound to the active Provider lane.
    pub run_id: RuntimeRunId,
    /// Stable concrete provider-attempt identity.
    pub attempt_id: RuntimeAttemptId,
    /// Exact Provider lane generation authorized to settle completion.
    pub generation: RuntimeGeneration,
    /// Durable provider-configuration epoch that authorized effect start.
    pub configuration_epoch: RuntimeGeneration,
    /// Canonical start event referenced by completion evidence.
    pub started_event_id: RuntimeEventId,
    /// Redacted credential binding copied into terminal attempt evidence.
    pub credential: Option<ProviderCredentialAttemptMetadata>,
}

/// Redacted per-attempt credential metadata safe for runtime evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCredentialAttemptMetadata {
    pub profile_id_sha256: String,
    pub auth_class: String,
    pub selection_reason: String,
}

/// Exact Provider lane pre-acquired for one authoritative kernel Run lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeProviderLaneAuthority {
    run_lease: GenerationLeaseV1,
    provider_lease: GenerationLeaseV1,
    configuration_epoch: RuntimeGeneration,
}

impl RuntimeProviderLaneAuthority {
    pub(super) fn from_journal(
        run_lease: GenerationLeaseV1,
        provider_lease: GenerationLeaseV1,
        configuration_epoch: RuntimeGeneration,
    ) -> Self {
        Self { run_lease, provider_lease, configuration_epoch }
    }

    /// Returns the exact active Run parent verified during acquisition.
    #[must_use]
    pub(crate) const fn run_lease(&self) -> &GenerationLeaseV1 {
        &self.run_lease
    }

    /// Returns the exact Provider lease used by the phase contract.
    #[must_use]
    pub(crate) const fn provider_lease(&self) -> &GenerationLeaseV1 {
        &self.provider_lease
    }
}

/// Request to start one concrete provider effect under the Provider lane.
#[derive(Debug, Clone)]
pub struct ProviderAttemptStartRequest {
    /// Owning orchestrator session.
    pub session_id: String,
    /// Owning active orchestrator run.
    pub run_id: String,
    /// Stable attempt identity minted before effect start.
    pub attempt_id: RuntimeAttemptId,
    /// Exact durable provider-configuration epoch captured with the in-memory runtime.
    pub expected_configuration_epoch: RuntimeGeneration,
    /// Exact pre-acquired V2 authority; legacy callers leave this absent.
    pub(crate) runtime_authority: Option<RuntimeProviderLaneAuthority>,
    /// Bounded provider identifier; never a credential or secret.
    pub provider_id: String,
    /// Bounded model identifier.
    pub model_id: String,
    /// Redacted credential selection; raw profile ids and secrets are forbidden.
    pub credential: Option<ProviderCredentialAttemptMetadata>,
}

/// Metadata-only result used to close one exact provider attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderAttemptCompletionRequest {
    /// Exact start authority returned by [`JournalStore::start_provider_attempt`].
    pub authority: ProviderAttemptRuntimeAuthority,
    /// Provider identifier that must match the started candidate.
    pub provider_id: String,
    /// Model identifier that must match the started candidate.
    pub model_id: String,
    /// Closed outcome vocabulary: `success`, `failure`, or `outcome_unknown`.
    pub outcome: String,
    /// Optional bounded provider failure class; raw error text is forbidden.
    pub error_class: Option<String>,
}

/// Result of generation-authoritative provider completion persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderAttemptCompletionOutcome {
    /// Completion evidence was appended at this host-owned sequence.
    Appended { sequence: u64 },
    /// Matching completion evidence already existed at this sequence.
    AlreadyAppended { sequence: u64 },
    /// The attempt no longer owns the Provider lane and cannot affect current state.
    StaleSuppressed,
}

/// Durable authority for one provider effect without an owning orchestrator run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderConfigurationAttemptRuntimeAuthority {
    /// Stable concrete provider-attempt identity.
    pub attempt_id: RuntimeAttemptId,
    /// Exact durable provider-configuration epoch that authorized effect start.
    pub configuration_epoch: RuntimeGeneration,
}

/// Request to durably start one configuration-scoped provider effect.
#[derive(Debug, Clone)]
pub struct ProviderConfigurationAttemptStartRequest {
    /// Stable attempt identity minted before effect start.
    pub attempt_id: RuntimeAttemptId,
    /// Exact durable provider-configuration epoch captured with the in-memory runtime.
    pub expected_configuration_epoch: RuntimeGeneration,
    /// Bounded provider identifier; never a credential or secret.
    pub provider_id: String,
    /// Bounded model identifier.
    pub model_id: String,
}

/// Metadata-only result used to close one configuration-scoped provider attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderConfigurationAttemptCompletionRequest {
    /// Exact durable authority returned when the runless effect started.
    pub authority: ProviderConfigurationAttemptRuntimeAuthority,
    /// Provider identifier that must match the immutable start record.
    pub provider_id: String,
    /// Model identifier that must match the immutable start record.
    pub model_id: String,
    /// Closed outcome vocabulary: `success`, `failure`, or `outcome_unknown`.
    pub outcome: String,
    /// Optional bounded provider failure class; raw error text is forbidden.
    pub error_class: Option<String>,
}

/// Result of settling one configuration-scoped provider completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderConfigurationAttemptCompletionOutcome {
    /// Canonical completion evidence was appended by this call.
    Appended,
    /// Matching canonical completion evidence already existed.
    AlreadyAppended,
    /// Provider configuration changed after this effect started.
    StaleSuppressed,
}

/// Request to activate or supersede one generation lane.
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct RuntimeGenerationActivateRequest {
    pub session_id: String,
    pub run_id: Option<String>,
    pub lane: RuntimeGenerationLane,
    pub owner: String,
    pub ttl_ms: i64,
    pub transition_kind: RuntimeGenerationTransitionKind,
    pub reason_code: String,
}

/// Request to close one active generation lane.
#[derive(Debug, Clone)]
pub struct RuntimeGenerationInvalidateRequest {
    pub session_id: String,
    pub run_id: Option<String>,
    pub lane: RuntimeGenerationLane,
    pub transition_kind: RuntimeGenerationTransitionKind,
    pub reason_code: String,
}

/// Result of idempotently closing one active generation lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeGenerationInvalidateOutcome {
    Invalidated,
    AlreadyInactive,
    RunMismatch,
}

/// Request to check and append a generation-safe runtime event.
#[derive(Debug, Clone)]
pub struct RuntimeEventAppendRequest {
    pub lane: RuntimeGenerationLane,
    pub envelope: RuntimeEventEnvelopeV2,
}

/// Result of generation-safe append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeEventAppendOutcome {
    Appended { sequence: u64 },
    AlreadyAppended { sequence: u64 },
    StaleSuppressed,
}

/// Result of an atomic tool-effect observation, including actual trace writes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ToolEffectObservationCommitOutcome {
    pub(crate) fence: SideEffectFenceV1,
    pub(crate) metadata_trace_events_appended: u64,
}

/// Metadata-only stale callback evidence that carries no producer payload.
#[derive(Debug, Clone)]
pub struct RuntimeStaleEventDiagnosticRequest {
    pub session_id: String,
    pub run_id: Option<String>,
    pub lane: RuntimeGenerationLane,
    pub expected_generation: Option<RuntimeGeneration>,
    pub observed_generation: RuntimeGeneration,
    pub subsystem: palyra_common::runtime_contracts::RuntimeSubsystem,
    pub disposition: StaleEventDisposition,
    pub reason_code: String,
}

/// Operator evidence used to close an uncertain side-effect fence without dispatching it again.
#[derive(Debug, Clone)]
pub struct SideEffectFenceOperatorResolutionRequest {
    pub operation_id: String,
    pub expected_intent_sha256: String,
    pub resolution: SideEffectFenceState,
    pub reason_code: String,
    pub evidence_sha256: String,
    pub actor_id_sha256: String,
}

/// Host-owned late cleanup observation for a fence already marked `effect_unknown`.
#[derive(Debug, Clone)]
pub struct SideEffectFenceCleanupOutcomeRequest {
    pub operation_id: String,
    pub observed_generation: RuntimeGeneration,
    pub outcome_observed: bool,
    pub reason_code: String,
    pub evidence_sha256: Option<String>,
}

/// One durable process handle joined with the lease that controls ownership-sensitive actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedProcessLeaseRecord {
    pub descriptor: RuntimeHandleDescriptorV1,
    pub lease: ProcessLeaseV1,
}

impl JournalStore {
    /// Atomically starts one run-scoped provider effect and records canonical start evidence.
    ///
    /// The active Run lane must still belong to the exact session/run and the captured
    /// provider-configuration epoch must remain current. Attempts under one configuration
    /// reuse the same session/run Provider lane generation.
    ///
    /// # Errors
    /// Returns [`JournalError`] when identities are malformed, the run or provider
    /// configuration is no longer active, or durable generation/event persistence fails.
    pub fn start_provider_attempt(
        &self,
        request: &ProviderAttemptStartRequest,
    ) -> Result<ProviderAttemptRuntimeAuthority, JournalError> {
        validate_provider_attempt_metadata(
            request.session_id.as_str(),
            request.run_id.as_str(),
            request.provider_id.as_str(),
            request.model_id.as_str(),
        )?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active_run = active_runtime_generation_tx(
            &transaction,
            request.session_id.as_str(),
            request.run_id.as_str(),
            RuntimeGenerationLane::Run,
            now,
        )?
        .ok_or_else(|| {
            JournalError::InvalidArgument(format!(
                "provider attempt cannot start because run {} has no active generation",
                request.run_id
            ))
        })?;
        let stored_state = transaction
            .query_row(
                "SELECT state FROM orchestrator_runs WHERE run_ulid = ?1 AND session_ulid = ?2",
                params![request.run_id, request.session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if !stored_state.as_deref().is_some_and(|state| {
            matches!(
                crate::orchestrator::RunLifecycleState::from_str(state),
                Some(
                    crate::orchestrator::RunLifecycleState::Accepted
                        | crate::orchestrator::RunLifecycleState::InProgress
                )
            )
        }) {
            return Err(JournalError::InvalidArgument(format!(
                "provider attempt cannot start because run {} is not active",
                request.run_id
            )));
        }
        let configuration_epoch = current_provider_configuration_epoch_tx(&transaction)?
            .ok_or_else(|| {
                JournalError::InvalidArgument(
                    "provider attempt cannot start because configuration authority is missing"
                        .to_owned(),
                )
            })?;
        if configuration_epoch != request.expected_configuration_epoch {
            return Err(JournalError::InvalidArgument(
                "provider attempt was superseded before effect start".to_owned(),
            ));
        }
        if let Some(authority) = request.runtime_authority.as_ref() {
            if authority.configuration_epoch != configuration_epoch
                || authority.run_lease != active_run
                || authority.run_lease.session_id.as_str() != request.session_id
                || authority.run_lease.run_id.as_ref().map(RuntimeRunId::as_str)
                    != Some(request.run_id.as_str())
            {
                return Err(JournalError::InvalidArgument(
                    "provider attempt pre-acquired runtime authority is stale".to_owned(),
                ));
            }
        }
        let lease = match load_generation_tx(
            &transaction,
            request.session_id.as_str(),
            RuntimeGenerationLane::Provider,
        )?
        .filter(|lease| now < lease.expires_at_unix_ms)
        {
            Some(lease)
                if lease
                    .run_id
                    .as_ref()
                    .is_some_and(|active_run_id| active_run_id.as_str() == request.run_id)
                    && provider_generation_owner_epoch(lease.owner.as_str())?
                        == configuration_epoch
                    && request
                        .runtime_authority
                        .as_ref()
                        .is_none_or(|authority| authority.provider_lease.eq(&lease))
                    && lease.expires_at_unix_ms <= active_run.expires_at_unix_ms =>
            {
                lease
            }
            Some(_) => {
                return Err(JournalError::InvalidArgument(
                    "provider attempt conflicts with active provider authority".to_owned(),
                ));
            }
            None => {
                if request.runtime_authority.is_some() {
                    return Err(JournalError::InvalidArgument(
                        "provider attempt pre-acquired lane is no longer active".to_owned(),
                    ));
                }
                activate_or_refresh_generation_tx(
                    &transaction,
                    request.session_id.as_str(),
                    Some(request.run_id.as_str()),
                    RuntimeGenerationLane::Provider,
                    provider_generation_owner(configuration_epoch).as_str(),
                    active_run.expires_at_unix_ms.saturating_sub(now),
                    RuntimeGenerationTransitionKind::Activated,
                    "runtime.generation.provider_configuration_bound",
                    now,
                )?
            }
        };
        let started_event_id = provider_attempt_event_id(&request.attempt_id, "started")?;
        let event = provider_attempt_runtime_event(
            request.session_id.as_str(),
            request.run_id.as_str(),
            request.attempt_id.clone(),
            lease.generation,
            configuration_epoch,
            started_event_id.clone(),
            None,
            RuntimeEventName::ProviderAttemptStarted,
            "provider.attempt.effect_started",
            request.provider_id.as_str(),
            request.model_id.as_str(),
            "started",
            None,
            request.credential.as_ref(),
            now,
        )?;
        if matches!(
            append_runtime_event_tx(&transaction, self.config.max_payload_bytes, &event, now)?,
            RuntimeEventAppendOutcome::StaleSuppressed
        ) {
            return Err(JournalError::InvalidArgument(
                "provider attempt lost generation authority during start persistence".to_owned(),
            ));
        }
        transaction.commit()?;
        Ok(ProviderAttemptRuntimeAuthority {
            session_id: lease.session_id,
            run_id: lease.run_id.ok_or_else(|| {
                JournalError::InvalidArgument(
                    "provider attempt generation is missing run identity".to_owned(),
                )
            })?,
            attempt_id: request.attempt_id.clone(),
            generation: lease.generation,
            configuration_epoch,
            started_event_id,
            credential: request.credential.clone(),
        })
    }

    /// Records canonical completion evidence only while the exact Provider generation is current.
    ///
    /// # Errors
    /// Returns [`JournalError`] when identities or bounded metadata are malformed, or storage fails.
    pub fn complete_provider_attempt(
        &self,
        request: &ProviderAttemptCompletionRequest,
    ) -> Result<ProviderAttemptCompletionOutcome, JournalError> {
        validate_provider_attempt_metadata(
            request.authority.session_id.as_str(),
            request.authority.run_id.as_str(),
            request.provider_id.as_str(),
            request.model_id.as_str(),
        )?;
        validate_provider_attempt_outcome(
            request.outcome.as_str(),
            request.error_class.as_deref(),
        )?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current_configuration_epoch = current_provider_configuration_epoch_tx(&transaction)?;
        let active_provider = load_generation_tx(
            &transaction,
            request.authority.session_id.as_str(),
            RuntimeGenerationLane::Provider,
        )?
        .filter(|lease| now < lease.expires_at_unix_ms);
        let authority_is_current = current_configuration_epoch
            .is_some_and(|epoch| epoch == request.authority.configuration_epoch)
            && active_provider.as_ref().is_some_and(|lease| {
                lease.generation == request.authority.generation
                    && lease
                        .run_id
                        .as_ref()
                        .is_some_and(|run_id| run_id.as_str() == request.authority.run_id.as_str())
                    && provider_generation_owner_epoch(lease.owner.as_str())
                        .is_ok_and(|epoch| epoch == request.authority.configuration_epoch)
            });
        if !authority_is_current {
            record_provider_attempt_stale_diagnostic_tx(
                &transaction,
                &request.authority,
                active_provider.as_ref().map(|lease| lease.generation),
                "runtime.generation.provider_reconfigured",
                now,
            )?;
            transaction.commit()?;
            return Ok(ProviderAttemptCompletionOutcome::StaleSuppressed);
        }
        validate_provider_attempt_start_parent_tx(
            &transaction,
            &request.authority,
            request.provider_id.as_str(),
            request.model_id.as_str(),
        )?;
        let event = provider_attempt_runtime_event(
            request.authority.session_id.as_str(),
            request.authority.run_id.as_str(),
            request.authority.attempt_id.clone(),
            request.authority.generation,
            request.authority.configuration_epoch,
            provider_attempt_event_id(&request.authority.attempt_id, "completed")?,
            Some(request.authority.started_event_id.clone()),
            RuntimeEventName::ProviderAttemptCompleted,
            match request.outcome.as_str() {
                "success" => "provider.attempt.succeeded",
                "failure" => "provider.attempt.failed",
                "outcome_unknown" => "provider.attempt.outcome_unknown",
                _ => unreachable!("provider attempt outcome was validated before event creation"),
            },
            request.provider_id.as_str(),
            request.model_id.as_str(),
            request.outcome.as_str(),
            request.error_class.as_deref(),
            request.authority.credential.as_ref(),
            now,
        )?;
        let outcome = match append_runtime_event_tx(
            &transaction,
            self.config.max_payload_bytes,
            &event,
            now,
        )? {
            RuntimeEventAppendOutcome::Appended { sequence } => {
                ProviderAttemptCompletionOutcome::Appended { sequence }
            }
            RuntimeEventAppendOutcome::AlreadyAppended { sequence } => {
                ProviderAttemptCompletionOutcome::AlreadyAppended { sequence }
            }
            RuntimeEventAppendOutcome::StaleSuppressed => {
                ProviderAttemptCompletionOutcome::StaleSuppressed
            }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Durably starts one provider effect that has no owning orchestrator run.
    ///
    /// The immutable start row binds the attempt to the current global provider
    /// configuration epoch. A concurrent reload serializes through the same
    /// journal transaction and therefore either precedes the start or supersedes it.
    ///
    /// # Errors
    /// Returns [`JournalError`] when metadata is invalid, configuration authority
    /// changed, or an attempt identity conflicts with different durable evidence.
    pub fn start_provider_configuration_attempt(
        &self,
        request: &ProviderConfigurationAttemptStartRequest,
    ) -> Result<ProviderConfigurationAttemptRuntimeAuthority, JournalError> {
        validate_provider_candidate_metadata(
            request.provider_id.as_str(),
            request.model_id.as_str(),
        )?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let configuration_epoch = current_provider_configuration_epoch_tx(&transaction)?
            .ok_or_else(|| {
                JournalError::InvalidArgument(
                    "provider attempt cannot start because configuration authority is missing"
                        .to_owned(),
                )
            })?;
        if configuration_epoch != request.expected_configuration_epoch {
            return Err(JournalError::InvalidArgument(
                "provider attempt was superseded before effect start".to_owned(),
            ));
        }
        let configuration_epoch_sql = runtime_generation_sql(configuration_epoch)?;
        let inserted = transaction.execute(
            r#"
                INSERT OR IGNORE INTO runtime_provider_attempt_starts (
                    attempt_ulid, configuration_epoch, provider_id, model_id,
                    started_at_unix_ms, schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, 1)
            "#,
            params![
                request.attempt_id.as_str(),
                configuration_epoch_sql,
                request.provider_id,
                request.model_id,
                now,
            ],
        )?;
        if inserted == 0 {
            let stored = transaction
                .query_row(
                    r#"
                        SELECT configuration_epoch, provider_id, model_id
                        FROM runtime_provider_attempt_starts
                        WHERE attempt_ulid = ?1
                    "#,
                    params![request.attempt_id.as_str()],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )
                .optional()?;
            if stored
                != Some((
                    configuration_epoch_sql,
                    request.provider_id.clone(),
                    request.model_id.clone(),
                ))
            {
                return Err(JournalError::InvalidArgument(
                    "provider attempt start conflicts with existing durable evidence".to_owned(),
                ));
            }
        }
        transaction.commit()?;
        Ok(ProviderConfigurationAttemptRuntimeAuthority {
            attempt_id: request.attempt_id.clone(),
            configuration_epoch,
        })
    }

    /// Settles one configuration-scoped provider attempt exactly once.
    ///
    /// Completion is stale when the durable configuration head advanced after
    /// effect start. Matching retries replay without appending another completion.
    ///
    /// # Errors
    /// Returns [`JournalError`] when metadata or parent evidence conflicts, or
    /// durable persistence fails.
    pub fn complete_provider_configuration_attempt(
        &self,
        request: &ProviderConfigurationAttemptCompletionRequest,
    ) -> Result<ProviderConfigurationAttemptCompletionOutcome, JournalError> {
        validate_provider_candidate_metadata(
            request.provider_id.as_str(),
            request.model_id.as_str(),
        )?;
        validate_provider_attempt_outcome(
            request.outcome.as_str(),
            request.error_class.as_deref(),
        )?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current_configuration_epoch = current_provider_configuration_epoch_tx(&transaction)?;
        if current_configuration_epoch != Some(request.authority.configuration_epoch) {
            transaction.commit()?;
            return Ok(ProviderConfigurationAttemptCompletionOutcome::StaleSuppressed);
        }
        let configuration_epoch_sql =
            runtime_generation_sql(request.authority.configuration_epoch)?;
        let started = transaction
            .query_row(
                r#"
                    SELECT configuration_epoch, provider_id, model_id
                    FROM runtime_provider_attempt_starts
                    WHERE attempt_ulid = ?1
                "#,
                params![request.authority.attempt_id.as_str()],
                |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
                },
            )
            .optional()?;
        if started
            != Some((
                configuration_epoch_sql,
                request.provider_id.clone(),
                request.model_id.clone(),
            ))
        {
            return Err(JournalError::InvalidArgument(
                "provider attempt completion parent evidence is missing or mismatched".to_owned(),
            ));
        }
        let inserted = transaction.execute(
            r#"
                INSERT OR IGNORE INTO runtime_provider_attempt_completions (
                    attempt_ulid, configuration_epoch, provider_id, model_id,
                    outcome, error_class, completed_at_unix_ms, schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)
            "#,
            params![
                request.authority.attempt_id.as_str(),
                configuration_epoch_sql,
                request.provider_id,
                request.model_id,
                request.outcome,
                request.error_class,
                now,
            ],
        )?;
        let outcome = if inserted == 1 {
            ProviderConfigurationAttemptCompletionOutcome::Appended
        } else {
            let stored = transaction
                .query_row(
                    r#"
                        SELECT configuration_epoch, provider_id, model_id, outcome, error_class
                        FROM runtime_provider_attempt_completions
                        WHERE attempt_ulid = ?1
                    "#,
                    params![request.authority.attempt_id.as_str()],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, Option<String>>(4)?,
                        ))
                    },
                )
                .optional()?;
            if stored
                != Some((
                    configuration_epoch_sql,
                    request.provider_id.clone(),
                    request.model_id.clone(),
                    request.outcome.clone(),
                    request.error_class.clone(),
                ))
            {
                return Err(JournalError::InvalidArgument(
                    "provider attempt completion conflicts with existing durable evidence"
                        .to_owned(),
                ));
            }
            ProviderConfigurationAttemptCompletionOutcome::AlreadyAppended
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Atomically activates provider health and advances durable configuration authority.
    ///
    /// Every active Provider lane is superseded in the same transaction, so a start
    /// authorized by the previous configuration cannot race the health/configuration swap.
    ///
    /// # Errors
    /// Returns [`JournalError`] when activation metadata is invalid or durable persistence fails.
    pub fn activate_provider_runtime(
        &self,
        components: &[RuntimeHealthComponentActivation],
        activated_at_unix_ms: i64,
    ) -> Result<ProviderRuntimeActivationOutcome, JournalError> {
        let sorted =
            validated_runtime_health_activation_inventory(components, activated_at_unix_ms)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let configuration_epoch =
            advance_provider_configuration_epoch_tx(&transaction, activated_at_unix_ms)?;
        let superseded_provider_lanes =
            supersede_active_provider_lanes_tx(&transaction, activated_at_unix_ms)?;
        let health = activate_runtime_health_components_tx(
            &transaction,
            sorted.as_slice(),
            activated_at_unix_ms,
        )?;
        transaction.commit()?;
        Ok(ProviderRuntimeActivationOutcome {
            configuration_epoch,
            health,
            superseded_provider_lanes,
        })
    }

    /// Persists a bounded metadata-only stale callback diagnostic.
    pub fn record_runtime_stale_event_diagnostic(
        &self,
        request: &RuntimeStaleEventDiagnosticRequest,
    ) -> Result<(), JournalError> {
        validate_runtime_stale_event_diagnostic_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        record_runtime_stale_event_diagnostic_tx(&transaction, request, now)?;
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn runtime_stale_event_diagnostic_count_for_scope(
        &self,
        session_id: &str,
        diagnostic_scope_id: &str,
        reason_code: &str,
    ) -> Result<u64, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let count = guard.query_row(
            r#"
                SELECT COUNT(*)
                FROM runtime_stale_event_diagnostics
                WHERE session_ulid = ?1 AND run_ulid = ?2 AND reason_code = ?3
            "#,
            params![session_id, diagnostic_scope_id, reason_code],
            |row| row.get::<_, i64>(0),
        )?;
        u64::try_from(count).map_err(|_| {
            JournalError::InvalidArgument(
                "runtime stale-event diagnostic count cannot be negative".to_owned(),
            )
        })
    }

    /// Atomically replaces one active run generation after accepted steering.
    ///
    /// The replacement keeps the original lease expiry and owner. It fails
    /// closed unless the supplied session and run still own the active lane.
    pub fn supersede_run_runtime_generation(
        &self,
        session_id: &str,
        run_id: &str,
        reason_code: &str,
    ) -> Result<GenerationLeaseV1, JournalError> {
        if session_id.trim().is_empty() || run_id.trim().is_empty() || reason_code.trim().is_empty()
        {
            return Err(JournalError::InvalidArgument(
                "run generation supersession request is invalid".to_owned(),
            ));
        }
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stored_state = transaction
            .query_row(
                "SELECT state FROM orchestrator_runs WHERE run_ulid = ?1 AND session_ulid = ?2",
                params![run_id, session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if !stored_state.as_deref().is_some_and(|state| {
            matches!(
                crate::orchestrator::RunLifecycleState::from_str(state),
                Some(
                    crate::orchestrator::RunLifecycleState::Accepted
                        | crate::orchestrator::RunLifecycleState::InProgress
                )
            )
        }) {
            return Err(JournalError::InvalidArgument(format!(
                "run generation supersession requires active run {run_id}"
            )));
        }
        let active = load_generation_tx(&transaction, session_id, RuntimeGenerationLane::Run)?
            .filter(|lease| now < lease.expires_at_unix_ms)
            .ok_or_else(|| {
                JournalError::InvalidArgument(
                    "run generation supersession requires an active generation".to_owned(),
                )
            })?;
        if active.run_id.as_ref().map(RuntimeRunId::as_str) != Some(run_id) {
            return Err(JournalError::InvalidArgument(
                "run generation supersession does not own the active session lane".to_owned(),
            ));
        }
        let remaining_ttl_ms = active.expires_at_unix_ms.saturating_sub(now);
        let replacement = activate_or_refresh_run_generation_tx(
            &transaction,
            session_id,
            run_id,
            active.owner.as_str(),
            remaining_ttl_ms,
            RuntimeGenerationTransitionKind::SteerSuperseded,
            reason_code,
            now,
        )?;
        transaction.commit()?;
        Ok(replacement)
    }

    /// Activates the next generation for a session lane atomically.
    #[cfg(test)]
    pub fn activate_runtime_generation(
        &self,
        request: &RuntimeGenerationActivateRequest,
    ) -> Result<GenerationLeaseV1, JournalError> {
        if request.session_id.trim().is_empty()
            || request.owner.trim().is_empty()
            || request.reason_code.trim().is_empty()
            || request.ttl_ms <= 0
        {
            return Err(JournalError::InvalidArgument(
                "runtime generation activation request is invalid".to_owned(),
            ));
        }
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let lease = activate_or_refresh_generation_tx(
            &transaction,
            request.session_id.as_str(),
            request.run_id.as_deref(),
            request.lane,
            request.owner.as_str(),
            request.ttl_ms,
            request.transition_kind,
            request.reason_code.as_str(),
            now,
        )?;
        transaction.commit()?;
        Ok(lease)
    }

    /// Closes an active generation lane and records the transition atomically.
    #[cfg(test)]
    pub fn invalidate_runtime_generation(
        &self,
        request: &RuntimeGenerationInvalidateRequest,
    ) -> Result<RuntimeGenerationInvalidateOutcome, JournalError> {
        if request.session_id.trim().is_empty()
            || request.reason_code.trim().is_empty()
            || !matches!(
                request.transition_kind,
                RuntimeGenerationTransitionKind::Cancelled
                    | RuntimeGenerationTransitionKind::Released
            )
        {
            return Err(JournalError::InvalidArgument(
                "runtime generation invalidation request is invalid".to_owned(),
            ));
        }
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let outcome = invalidate_runtime_generation_tx(&transaction, request, now)?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Returns the active generation lease for a run and lane, if it still owns the lane.
    pub fn active_runtime_generation_for_run(
        &self,
        run_id: &str,
        lane: RuntimeGenerationLane,
    ) -> Result<Option<GenerationLeaseV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let session_id = guard
            .query_row(
                "SELECT session_ulid FROM orchestrator_runs WHERE run_ulid = ?1",
                params![run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(session_id) = session_id else {
            return Ok(None);
        };
        let now = current_unix_ms()?;
        Ok(load_generation_tx(&guard, session_id.as_str(), lane)?.filter(|lease| {
            now < lease.expires_at_unix_ms
                && lease
                    .run_id
                    .as_ref()
                    .is_some_and(|active_run_id| active_run_id.as_str() == run_id)
        }))
    }

    /// Counts exact durable generation transitions for a focused regression assertion.
    #[cfg(test)]
    pub(crate) fn runtime_generation_transition_count_for_test(
        &self,
        session_id: &str,
        run_id: &str,
        lane: RuntimeGenerationLane,
        transition_kind: RuntimeGenerationTransitionKind,
    ) -> Result<u64, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let count = guard.query_row(
            r#"
                SELECT COUNT(*)
                FROM runtime_generation_events
                WHERE session_ulid = ?1
                  AND run_ulid = ?2
                  AND lane = ?3
                  AND transition_kind = ?4
            "#,
            params![session_id, run_id, lane.as_str(), transition_kind.as_str()],
            |row| row.get::<_, i64>(0),
        )?;
        u64::try_from(count).map_err(|_| {
            JournalError::InvalidArgument(
                "runtime generation transition count cannot be represented as u64".to_owned(),
            )
        })
    }

    /// Returns the latest persisted event generation for one run and lane.
    ///
    /// Unlike [`Self::active_runtime_generation_for_run`], this read-only projection remains
    /// available after terminal settlement invalidates the live lease. Replay and QA adapters use
    /// it to retain the same generation correlation as the original runtime stream.
    pub fn latest_persisted_runtime_generation_for_run(
        &self,
        run_id: &str,
        lane: RuntimeGenerationLane,
    ) -> Result<Option<(RuntimeSessionId, RuntimeGeneration)>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let row = guard
            .query_row(
                r#"
                    SELECT session_ulid, generation
                    FROM runtime_events_v2
                    WHERE run_ulid = ?1 AND lane = ?2
                    ORDER BY generation DESC, sequence DESC
                    LIMIT 1
                "#,
                params![run_id, lane.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        row.map(|(session_id, generation)| {
            let generation = u64::try_from(generation).map_err(|_| {
                JournalError::InvalidArgument(
                    "persisted runtime event generation cannot be represented as u64".to_owned(),
                )
            })?;
            Ok((
                RuntimeSessionId::parse(session_id.as_str())
                    .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
                RuntimeGeneration::new(generation)
                    .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
            ))
        })
        .transpose()
    }

    /// Loads the exact canonical V2 projection paired with one legacy tape row.
    ///
    /// The deterministic event id is the join key written by the atomic tape
    /// append. Missing projections are expected for legacy-only event families.
    pub fn persisted_runtime_event_for_tape_sequence(
        &self,
        run_id: &str,
        tape_sequence: i64,
    ) -> Result<Option<RuntimeEventEnvelopeV2>, JournalError> {
        let source_sequence = u64::try_from(tape_sequence).map_err(|_| {
            JournalError::InvalidArgument(
                "runtime event source sequence must be a non-negative sqlite integer".to_owned(),
            )
        })?;
        let event_id =
            RuntimeEventId::parse(format!("run_stream:{run_id}:{source_sequence}").as_str())
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let envelope_json = guard
            .query_row(
                r#"
                    SELECT envelope_json
                    FROM runtime_events_v2
                    WHERE event_ulid = ?1 AND run_ulid = ?2
                "#,
                params![event_id.as_str(), run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        envelope_json
            .map(|envelope_json| {
                let envelope =
                    serde_json::from_str::<RuntimeEventEnvelopeV2>(envelope_json.as_str())?;
                envelope
                    .validate()
                    .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
                if envelope.event_id != event_id || envelope.identities.run_id.as_str() != run_id {
                    return Err(JournalError::InvalidArgument(
                        "persisted runtime event projection identity is inconsistent".to_owned(),
                    ));
                }
                Ok(envelope)
            })
            .transpose()
    }

    /// Checks a callback generation against the active lane.
    #[cfg(test)]
    pub fn check_runtime_generation(
        &self,
        session_id: &str,
        lane: RuntimeGenerationLane,
        observed: RuntimeGeneration,
    ) -> Result<GenerationCheckOutcome, JournalError> {
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        Ok(
            match load_generation_tx(&guard, session_id, lane)?
                .filter(|lease| now < lease.expires_at_unix_ms)
            {
                Some(lease) => lease.check(observed),
                None => GenerationCheckOutcome {
                    schema_version: 1,
                    expected: None,
                    observed,
                    disposition: GenerationCheckDisposition::MissingActiveGeneration,
                    reason_code: "runtime.generation.missing_active".to_owned(),
                },
            },
        )
    }

    /// Appends a V2 event only while its generation owns the lane.
    #[cfg(test)]
    pub fn append_runtime_event(
        &self,
        request: &RuntimeEventAppendRequest,
    ) -> Result<RuntimeEventAppendOutcome, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome =
            append_runtime_event_tx(&transaction, self.config.max_payload_bytes, request, now)?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Atomically validates or records a side-effect intent before dispatch.
    ///
    /// An existing operation is reusable only when its durable scope, stable
    /// execution identity, normalized intent digest, semantics, and external
    /// idempotency binding exactly match the incoming intent.
    pub fn prepare_side_effect_fence(
        &self,
        session_id: &str,
        run_id: &str,
        fence: &SideEffectFenceV1,
    ) -> Result<SideEffectRetryDecision, JournalError> {
        validate_new_side_effect_fence(fence)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        authorize_side_effect_generation_tx(
            &transaction,
            session_id,
            run_id,
            fence.observed_generation,
            current_unix_ms()?,
        )?;
        if let Some(existing) = load_scoped_fence_tx(&transaction, fence.operation_id.as_str())? {
            validate_matching_side_effect_intent(session_id, run_id, fence, &existing)?;
            return Ok(existing.fence.retry_decision());
        }
        ensure_runtime_rollback_allows_new_side_effect_tx(
            &transaction,
            run_id,
            fence.observed_generation,
        )?;
        insert_side_effect_fence_tx(&transaction, session_id, run_id, fence)?;
        append_side_effect_runtime_event_tx(
            &transaction,
            self.config.max_payload_bytes,
            session_id,
            run_id,
            fence,
            current_unix_ms()?,
        )?;
        transaction.commit()?;
        Ok(SideEffectRetryDecision::Safe)
    }

    /// Persists a new side-effect intent.
    #[cfg(test)]
    pub fn record_side_effect_fence(
        &self,
        session_id: &str,
        run_id: &str,
        fence: &SideEffectFenceV1,
    ) -> Result<(), JournalError> {
        validate_new_side_effect_fence(fence)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        authorize_side_effect_generation_tx(
            &transaction,
            session_id,
            run_id,
            fence.observed_generation,
            current_unix_ms()?,
        )?;
        ensure_runtime_rollback_allows_new_side_effect_tx(
            &transaction,
            run_id,
            fence.observed_generation,
        )?;
        insert_side_effect_fence_tx(&transaction, session_id, run_id, fence)?;
        append_side_effect_runtime_event_tx(
            &transaction,
            self.config.max_payload_bytes,
            session_id,
            run_id,
            fence,
            current_unix_ms()?,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Transitions an existing side-effect fence with compare-and-transition semantics.
    pub fn transition_side_effect_fence(
        &self,
        operation_id: &str,
        next: SideEffectFenceState,
        generation: RuntimeGeneration,
        reason_code: &str,
        evidence_sha256: Option<String>,
    ) -> Result<SideEffectFenceV1, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let scoped = load_scoped_fence_tx(&transaction, operation_id)?.ok_or_else(|| {
            JournalError::InvalidArgument(format!("side-effect fence not found: {operation_id}"))
        })?;
        authorize_side_effect_generation_tx(
            &transaction,
            scoped.session_id.as_str(),
            scoped.run_id.as_str(),
            generation,
            now,
        )?;
        let mut fence = scoped.fence;
        let from = fence.state;
        fence
            .transition(next, generation, reason_code, evidence_sha256, now)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        let raw = serde_json::to_vec(&fence)?;
        let (json, _) = sanitize_payload(raw.as_slice())?;
        let updated = transaction.execute(
            r#"
                UPDATE runtime_side_effect_fences SET
                    observed_generation = ?2,
                    state = ?3,
                    fence_json = ?4,
                    updated_at_unix_ms = ?5
                WHERE operation_ulid = ?1 AND state = ?6
            "#,
            params![
                operation_id,
                i64::try_from(generation.get()).unwrap_or(i64::MAX),
                next.as_str(),
                json,
                now,
                from.as_str(),
            ],
        )?;
        if updated != 1 {
            return Err(JournalError::InvalidArgument(
                "side-effect fence changed concurrently".to_owned(),
            ));
        }
        append_fence_event_tx(&transaction, Some(from), &fence)?;
        append_side_effect_runtime_event_tx(
            &transaction,
            self.config.max_payload_bytes,
            scoped.session_id.as_str(),
            scoped.run_id.as_str(),
            &fence,
            now,
        )?;
        transaction.commit()?;
        Ok(fence)
    }

    /// Atomically appends canonical result evidence and observes one started tool effect.
    pub fn commit_tool_effect_observation(
        &self,
        request: &super::ToolEffectObservationCommitRequest,
        runtime_events: &[Option<RuntimeEventAppendRequest>],
    ) -> Result<ToolEffectObservationCommitOutcome, JournalError> {
        if request.tape_events.is_empty()
            || request.tape_events.len() != runtime_events.len()
            || !is_sha256_hex(request.evidence_sha256.as_str())
            || !matches!(
                request.tape_events.as_slice(),
                [result, attestation, legacy]
                    if result.event_type == "tool_result"
                        && attestation.event_type == "tool_attestation"
                        && legacy.event_type == "tool.executed"
            )
        {
            return Err(JournalError::InvalidArgument(
                "tool effect observation evidence batch is invalid".to_owned(),
            ));
        }
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let scoped = load_scoped_fence_tx(&transaction, request.operation_id.as_str())?
            .ok_or_else(|| JournalError::ToolSideEffectFenceNotFound {
                operation_id: request.operation_id.as_str().to_owned(),
            })?;
        authorize_side_effect_generation_tx(
            &transaction,
            scoped.session_id.as_str(),
            scoped.run_id.as_str(),
            request.generation,
            now,
        )?;
        if scoped.fence.observed_generation != request.generation {
            return Err(JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.as_str().to_owned(),
                reason: "tool effect observation generation no longer matches".to_owned(),
            });
        }
        if runtime_events.first().and_then(Option::as_ref).is_none()
            || runtime_events.get(1).and_then(Option::as_ref).is_none()
            || runtime_events.get(2).is_none_or(Option::is_some)
        {
            return Err(JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.as_str().to_owned(),
                reason:
                    "tool effect observation requires canonical result and attestation projections"
                        .to_owned(),
            });
        }
        if scoped.fence.state == SideEffectFenceState::EffectObserved
            && scoped.fence.evidence_sha256.as_deref() == Some(request.evidence_sha256.as_str())
        {
            let mut metadata_trace_events_appended = 0_u64;
            for (tape_event, runtime_event) in request.tape_events.iter().zip(runtime_events.iter())
            {
                validate_tool_effect_observation_evidence(
                    request,
                    &scoped,
                    tape_event,
                    runtime_event.as_ref(),
                )?;
                append_or_replay_orchestrator_tape_event_tx(
                    &transaction,
                    self.config.max_payload_bytes,
                    tape_event,
                    now,
                )?;
                if let Some(runtime_event) = runtime_event.as_ref() {
                    match append_runtime_event_tx(
                        &transaction,
                        self.config.max_payload_bytes,
                        runtime_event,
                        now,
                    )? {
                        RuntimeEventAppendOutcome::Appended { .. } => {
                            metadata_trace_events_appended = metadata_trace_events_appended
                                .saturating_add(u64::from(
                                    runtime_event_metadata_trace_was_persisted_tx(
                                        &transaction,
                                        runtime_event,
                                    )?,
                                ));
                        }
                        RuntimeEventAppendOutcome::AlreadyAppended { .. } => {}
                        RuntimeEventAppendOutcome::StaleSuppressed => {
                            return Err(JournalError::ToolSideEffectFencePrecondition {
                                operation_id: request.operation_id.as_str().to_owned(),
                                reason: "tool result evidence lost generation authority".to_owned(),
                            });
                        }
                    }
                }
            }
            transaction.commit()?;
            return Ok(ToolEffectObservationCommitOutcome {
                fence: scoped.fence,
                metadata_trace_events_appended,
            });
        }
        if scoped.fence.state != SideEffectFenceState::EffectStarted {
            return Err(JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.as_str().to_owned(),
                reason: "tool effect observation requires the exact started fence".to_owned(),
            });
        }

        let mut metadata_trace_events_appended = 0_u64;
        for (tape_event, runtime_event) in request.tape_events.iter().zip(runtime_events.iter()) {
            validate_tool_effect_observation_evidence(
                request,
                &scoped,
                tape_event,
                runtime_event.as_ref(),
            )?;
            append_or_replay_orchestrator_tape_event_tx(
                &transaction,
                self.config.max_payload_bytes,
                tape_event,
                now,
            )?;
            if let Some(runtime_event) = runtime_event.as_ref() {
                match append_runtime_event_tx(
                    &transaction,
                    self.config.max_payload_bytes,
                    runtime_event,
                    now,
                )? {
                    RuntimeEventAppendOutcome::Appended { .. } => {
                        metadata_trace_events_appended = metadata_trace_events_appended
                            .saturating_add(u64::from(
                                runtime_event_metadata_trace_was_persisted_tx(
                                    &transaction,
                                    runtime_event,
                                )?,
                            ));
                    }
                    RuntimeEventAppendOutcome::AlreadyAppended { .. } => {}
                    RuntimeEventAppendOutcome::StaleSuppressed => {
                        return Err(JournalError::ToolSideEffectFencePrecondition {
                            operation_id: request.operation_id.as_str().to_owned(),
                            reason: "tool result evidence lost generation authority".to_owned(),
                        });
                    }
                }
            }
        }

        let mut fence = scoped.fence;
        let from = fence.state;
        fence
            .transition(
                SideEffectFenceState::EffectObserved,
                request.generation,
                "tool.effect.observed",
                Some(request.evidence_sha256.clone()),
                now,
            )
            .map_err(|error| JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.as_str().to_owned(),
                reason: error.to_string(),
            })?;
        let raw = serde_json::to_vec(&fence)?;
        let (json, _) = sanitize_payload(raw.as_slice())?;
        let updated = transaction.execute(
            r#"
                UPDATE runtime_side_effect_fences SET
                    observed_generation = ?2,
                    state = ?3,
                    fence_json = ?4,
                    updated_at_unix_ms = ?5
                WHERE operation_ulid = ?1
                  AND state = ?6
                  AND observed_generation = ?7
            "#,
            params![
                request.operation_id.as_str(),
                i64::try_from(request.generation.get()).unwrap_or(i64::MAX),
                fence.state.as_str(),
                json,
                now,
                from.as_str(),
                i64::try_from(request.generation.get()).unwrap_or(i64::MAX),
            ],
        )?;
        if updated != 1 {
            return Err(JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.as_str().to_owned(),
                reason: "tool effect observation changed concurrently".to_owned(),
            });
        }
        append_fence_event_tx(&transaction, Some(from), &fence)?;
        let (_, observed_metadata_trace_appended) = append_side_effect_runtime_event_tx(
            &transaction,
            self.config.max_payload_bytes,
            scoped.session_id.as_str(),
            scoped.run_id.as_str(),
            &fence,
            now,
        )?;
        metadata_trace_events_appended = metadata_trace_events_appended
            .saturating_add(u64::from(observed_metadata_trace_appended));
        transaction.commit()?;
        Ok(ToolEffectObservationCommitOutcome { fence, metadata_trace_events_appended })
    }

    /// Records a late cleanup-owner observation without reviving execution authority.
    ///
    /// A definitive observed outcome reconciles the exact unknown fence. An ambiguous or timed-out
    /// cleanup leaves the fence unknown and appends host evidence only. Matching replay is
    /// idempotent; changed evidence for the same operation fails closed.
    pub fn record_side_effect_cleanup_outcome(
        &self,
        request: &SideEffectFenceCleanupOutcomeRequest,
    ) -> Result<SideEffectFenceV1, JournalError> {
        validate_side_effect_cleanup_outcome(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let scoped = load_scoped_fence_tx(&transaction, request.operation_id.as_str())?
            .ok_or_else(|| JournalError::ToolSideEffectFenceNotFound {
                operation_id: request.operation_id.clone(),
            })?;
        if scoped.fence.observed_generation != request.observed_generation {
            return Err(JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.clone(),
                reason: "cleanup generation no longer matches the uncertain effect".to_owned(),
            });
        }
        let event_name = if request.outcome_observed {
            RuntimeEventName::ToolEffectCleanupReconciled
        } else {
            RuntimeEventName::ToolEffectCleanupUnknown
        };
        if scoped.fence.state != SideEffectFenceState::EffectUnknown {
            if request.outcome_observed
                && scoped.fence.state == SideEffectFenceState::Reconciled
                && scoped.fence.reason_code == request.reason_code
                && scoped.fence.evidence_sha256 == request.evidence_sha256
            {
                return Ok(scoped.fence);
            }
            return Err(JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.clone(),
                reason: "cleanup outcome requires the exact uncertain side-effect fence".to_owned(),
            });
        }

        let mut fence = scoped.fence;
        if request.outcome_observed {
            let from = fence.state;
            fence
                .transition(
                    SideEffectFenceState::Reconciled,
                    request.observed_generation,
                    request.reason_code.clone(),
                    request.evidence_sha256.clone(),
                    now,
                )
                .map_err(|error| JournalError::ToolSideEffectFencePrecondition {
                    operation_id: request.operation_id.clone(),
                    reason: error.to_string(),
                })?;
            let raw = serde_json::to_vec(&fence)?;
            let (json, _) = sanitize_payload(raw.as_slice())?;
            let updated = transaction.execute(
                r#"
                    UPDATE runtime_side_effect_fences SET
                        state = ?2,
                        fence_json = ?3,
                        updated_at_unix_ms = ?4
                    WHERE operation_ulid = ?1
                      AND state = ?5
                      AND observed_generation = ?6
                "#,
                params![
                    request.operation_id,
                    fence.state.as_str(),
                    json,
                    now,
                    from.as_str(),
                    i64::try_from(request.observed_generation.get()).unwrap_or(i64::MAX),
                ],
            )?;
            if updated != 1 {
                return Err(JournalError::ToolSideEffectFencePrecondition {
                    operation_id: request.operation_id.clone(),
                    reason: "side-effect cleanup outcome changed concurrently".to_owned(),
                });
            }
            append_fence_event_tx(&transaction, Some(from), &fence)?;
        }
        append_host_side_effect_cleanup_runtime_event_tx(
            &transaction,
            self.config.max_payload_bytes,
            scoped.session_id.as_str(),
            scoped.run_id.as_str(),
            &fence,
            event_name,
            request.reason_code.as_str(),
            request.evidence_sha256.as_deref(),
            now,
        )?;
        transaction.commit()?;
        Ok(fence)
    }

    /// Resolves an `effect_unknown` fence from independently authenticated operator evidence.
    ///
    /// This path never checks or revives execution generation authority and never dispatches the
    /// tool. It compare-and-sets the expected intent digest and uncertain state, then appends an
    /// operator-attributed V2 event in the same immediate transaction.
    pub fn resolve_side_effect_fence_as_operator(
        &self,
        request: &SideEffectFenceOperatorResolutionRequest,
    ) -> Result<SideEffectFenceV1, JournalError> {
        validate_operator_side_effect_resolution(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let scoped = load_scoped_fence_tx(&transaction, request.operation_id.as_str())?
            .ok_or_else(|| JournalError::ToolSideEffectFenceNotFound {
                operation_id: request.operation_id.clone(),
            })?;
        if scoped.fence.state != SideEffectFenceState::EffectUnknown
            || scoped.fence.intent_sha256 != request.expected_intent_sha256
        {
            return Err(JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.clone(),
                reason: "uncertain state or intent digest no longer matches".to_owned(),
            });
        }
        let mut fence = scoped.fence;
        let from = fence.state;
        fence
            .transition(
                request.resolution,
                fence.observed_generation,
                request.reason_code.clone(),
                Some(request.evidence_sha256.clone()),
                now,
            )
            .map_err(|error| JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.clone(),
                reason: error.to_string(),
            })?;
        let raw = serde_json::to_vec(&fence)?;
        let (json, _) = sanitize_payload(raw.as_slice())?;
        let updated = transaction.execute(
            r#"
                UPDATE runtime_side_effect_fences SET
                    state = ?2,
                    fence_json = ?3,
                    updated_at_unix_ms = ?4
                WHERE operation_ulid = ?1
                  AND state = ?5
                  AND intent_sha256 = ?6
            "#,
            params![
                request.operation_id,
                fence.state.as_str(),
                json,
                now,
                from.as_str(),
                request.expected_intent_sha256,
            ],
        )?;
        if updated != 1 {
            return Err(JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.clone(),
                reason: "side-effect fence changed concurrently".to_owned(),
            });
        }
        append_fence_event_tx(&transaction, Some(from), &fence)?;
        append_operator_side_effect_runtime_event_tx(
            &transaction,
            self.config.max_payload_bytes,
            scoped.session_id.as_str(),
            scoped.run_id.as_str(),
            &fence,
            request.actor_id_sha256.as_str(),
            now,
        )?;
        transaction.commit()?;
        Ok(fence)
    }

    /// Returns the current retry decision for one side-effect fence.
    #[cfg(test)]
    pub fn side_effect_retry_decision(
        &self,
        operation_id: &str,
    ) -> Result<Option<SideEffectRetryDecision>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        Ok(load_fence_tx(&guard, operation_id)?.map(|fence| fence.retry_decision()))
    }

    /// Returns one durable component-health projection.
    pub fn runtime_component_health(
        &self,
        component_id: &str,
    ) -> Result<Option<RuntimeComponentHealthV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_component_health_tx(&guard, component_id)
    }

    /// Reconciles supported probe residue after compatibility admission.
    ///
    /// Startup treats every inherited V1 probe as orphaned because volatile execution
    /// authority does not survive a daemon restart. Periodic reconciliation settles
    /// only probes whose leases have expired.
    pub fn reconcile_runtime_health_probes(
        &self,
        mode: RuntimeHealthProbeReconciliationMode,
        now_unix_ms: i64,
    ) -> Result<RuntimeHealthProbeReconciliationOutcome, JournalError> {
        if now_unix_ms < 0 {
            return Err(JournalError::InvalidArgument(
                "health probe reconciliation timestamp is invalid".to_owned(),
            ));
        }
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = reconcile_runtime_health_probes_tx(&transaction, mode, now_unix_ms)?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Removes expired supported leases after conservatively reconciling their health state.
    ///
    /// This compatibility wrapper never deletes future-schema rows and never leaves a
    /// supported component in `Probing` without terminal evidence.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "periodic reconciliation loop follows startup adoption")
    )]
    pub fn reap_expired_runtime_health_probe_leases(
        &self,
        now_unix_ms: i64,
    ) -> Result<usize, JournalError> {
        self.reconcile_runtime_health_probes(
            RuntimeHealthProbeReconciliationMode::Periodic,
            now_unix_ms,
        )
        .map(|outcome| outcome.settled_inconclusive.saturating_add(outcome.retired_orphan_leases))
    }

    /// Returns the active health probe lease for one component, if present.
    #[cfg(test)]
    pub fn runtime_health_probe_lease(
        &self,
        component_id: &str,
    ) -> Result<Option<HealthProbeLeaseV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_health_probe_lease_tx(&guard, component_id)
    }

    /// Returns the immutable begin reason for one probe lease.
    #[cfg(test)]
    pub fn runtime_health_probe_begin_reason(
        &self,
        lease_id: &str,
    ) -> Result<Option<String>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_runtime_health_probe_begin_tx(&guard, lease_id)
            .map(|begin| begin.map(|evidence| evidence.reason_code))
    }

    /// Begins one exact non-mutating probe and atomically enters `Probing`.
    ///
    /// # Errors
    /// Returns an error for missing or stale component authority, unauthorized
    /// quarantine recovery, active single-flight capacity, or conflicting replay.
    pub fn begin_runtime_health_probe(
        &self,
        request: &RuntimeHealthProbeBeginRequest,
    ) -> Result<RuntimeHealthProbeBeginOutcome, JournalError> {
        validate_runtime_health_probe_begin_request(request)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = begin_runtime_health_probe_tx(&transaction, request)?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Atomically settles one exact active probe and records immutable terminal evidence.
    ///
    /// Matching replay returns the committed outcome. Conflicting settlement is
    /// rejected; a late non-mutating result is normalized to `Inconclusive` while
    /// preserving its actual completion timestamp.
    pub fn settle_runtime_health_probe(
        &self,
        request: &RuntimeHealthProbeSettlementRequest,
    ) -> Result<RuntimeHealthProbeSettlementOutcome, JournalError> {
        validate_optional_health_evidence_sha256(request.probe_evidence_sha256.as_deref())?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = settle_runtime_health_probe_tx(&transaction, request)?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Clears one exact quarantined component under an authenticated operator decision.
    ///
    /// The health transition and hash-only audit event commit in one transaction.
    /// Optional probe evidence must identify an immutable, successful, non-mutating
    /// settlement for the same component generation.
    pub fn clear_runtime_component_quarantine(
        &self,
        request: &RuntimeHealthQuarantineClearRequest,
    ) -> Result<RuntimeHealthQuarantineClearOutcome, JournalError> {
        request
            .clear
            .validate()
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        validate_reason_code(request.clear.reason_code.as_str())?;
        validate_optional_health_evidence_sha256(Some(request.clear.actor_id.as_str()))?;
        validate_optional_health_evidence_sha256(Some(
            request.clear.authorization_evidence_sha256.as_str(),
        ))?;
        validate_optional_health_evidence_sha256(request.clear.probe_evidence_sha256.as_deref())?;
        if request.cleared_at_unix_ms < 0
            || request.audit_event.timestamp_unix_ms != request.cleared_at_unix_ms
            || request.audit_event.event_id.trim().is_empty()
            || request.audit_event.session_id.trim().is_empty()
            || request.audit_event.run_id.trim().is_empty()
            || request.audit_event.principal.trim().is_empty()
            || request.audit_event.device_id.trim().is_empty()
        {
            return Err(JournalError::InvalidArgument(
                "runtime quarantine clear audit identity is invalid".to_owned(),
            ));
        }
        self.ensure_hash_chain_writes_allowed()?;
        if request.audit_event.payload_json.len() > self.config.max_payload_bytes {
            return Err(JournalError::PayloadTooLarge {
                payload_kind: "journal",
                actual_bytes: request.audit_event.payload_json.len(),
                max_bytes: self.config.max_payload_bytes,
            });
        }
        let (audit_payload_json, audit_payload_redacted) =
            sanitize_runtime_health_quarantine_clear_audit_payload(
                request.audit_event.payload_json.as_slice(),
                request.clear.authorization_evidence_sha256.as_str(),
            )?;
        let created_at_unix_ms = current_unix_ms()?;

        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if transaction
            .query_row(
                "SELECT 1 FROM journal_events WHERE event_ulid = ?1",
                params![request.audit_event.event_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Err(JournalError::DuplicateEventId {
                event_id: request.audit_event.event_id.clone(),
            });
        }
        let current_events: i64 =
            transaction.query_row("SELECT COUNT(*) FROM journal_events", [], |row| row.get(0))?;
        let current_events = current_events.max(0) as usize;
        if current_events >= self.config.max_events {
            return Err(JournalError::JournalCapacityExceeded {
                current_events,
                max_events: self.config.max_events,
            });
        }

        let mut health =
            load_component_health_tx(&transaction, request.clear.component_id.as_str())?
                .ok_or_else(|| {
                    JournalError::InvalidArgument(
                        "runtime quarantine clear requires an active component".to_owned(),
                    )
                })?;
        if health.generation != request.clear.expected_generation {
            return Err(JournalError::InvalidArgument(
                "runtime quarantine clear was rejected for a stale component generation".to_owned(),
            ));
        }
        if health.state != RuntimeHealthState::Quarantined {
            return Err(JournalError::InvalidArgument(
                "runtime quarantine clear requires quarantined component posture".to_owned(),
            ));
        }
        if request.cleared_at_unix_ms < health.updated_at_unix_ms {
            return Err(JournalError::InvalidArgument(
                "runtime quarantine clear timestamp predates durable component health".to_owned(),
            ));
        }
        if load_health_probe_lease_tx(&transaction, request.clear.component_id.as_str())?.is_some()
        {
            return Err(JournalError::InvalidArgument(
                "runtime quarantine clear cannot race an active health probe".to_owned(),
            ));
        }
        validate_successful_probe_evidence_for_quarantine_clear_tx(&transaction, &request.clear)?;

        health.state = RuntimeHealthState::Degraded;
        health.strike_count = 0;
        health.reason_code.clone_from(&request.clear.reason_code);
        health.expires_at_unix_ms = None;
        health.security_quarantine = false;
        health.updated_at_unix_ms = request.cleared_at_unix_ms;
        persist_component_health_update_tx(&transaction, RuntimeHealthState::Quarantined, &health)?;

        let prev_hash = if self.config.hash_chain_enabled {
            transaction
                .query_row("SELECT hash FROM journal_events ORDER BY seq DESC LIMIT 1", [], |row| {
                    row.get::<_, Option<String>>(0)
                })
                .optional()?
                .flatten()
        } else {
            None
        };
        let audit_event_sha256 = self.config.hash_chain_enabled.then(|| {
            compute_hash(prev_hash.as_deref(), &request.audit_event, audit_payload_json.as_str())
        });
        transaction.execute(
            r#"
                INSERT INTO journal_events (
                    event_ulid, session_ulid, run_ulid, kind, actor,
                    timestamp_unix_ms, payload_json, redacted, hash, prev_hash,
                    principal, device_id, channel, created_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14
                )
            "#,
            params![
                request.audit_event.event_id,
                request.audit_event.session_id,
                request.audit_event.run_id,
                request.audit_event.kind,
                request.audit_event.actor,
                request.audit_event.timestamp_unix_ms,
                audit_payload_json,
                i64::from(audit_payload_redacted),
                audit_event_sha256,
                prev_hash,
                request.audit_event.principal,
                request.audit_event.device_id,
                request.audit_event.channel,
                created_at_unix_ms,
            ],
        )?;
        transaction.commit()?;
        Ok(RuntimeHealthQuarantineClearOutcome {
            health,
            audit_event_sha256,
            audit_payload_redacted,
        })
    }

    /// Activates a deterministic component inventory under fresh monotonic generations.
    ///
    /// Components are sorted by identity before one immediate transaction allocates
    /// generations. Any invalid component aborts the complete batch.
    pub fn activate_runtime_health_components(
        &self,
        components: &[RuntimeHealthComponentActivation],
        activated_at_unix_ms: i64,
    ) -> Result<RuntimeHealthActivationOutcome, JournalError> {
        let sorted =
            validated_runtime_health_activation_inventory(components, activated_at_unix_ms)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = activate_runtime_health_components_tx(
            &transaction,
            sorted.as_slice(),
            activated_at_unix_ms,
        )?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Records one ordinary success or failure against an exact active generation.
    ///
    /// Success clears degraded strikes but never recovers cooldown, probing,
    /// quarantine, or disabled posture. Failure degrades healthy state, increments
    /// strikes in degraded state, and enters cooldown at the configured threshold.
    pub fn record_runtime_health_observation(
        &self,
        request: &RuntimeHealthObservationRequest,
    ) -> Result<RuntimeHealthObservationOutcome, JournalError> {
        validate_reason_code(request.reason_code.as_str())?;
        if request.observed_at_unix_ms < 0 {
            return Err(JournalError::InvalidArgument(
                "runtime health observation timestamp is invalid".to_owned(),
            ));
        }
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = record_runtime_health_observation_tx(&transaction, request)?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Records one health observation only while an exact run generation remains authoritative.
    ///
    /// The run fence and component-health update share one immediate transaction,
    /// so a concurrent steer cannot land between authorization and mutation.
    pub fn record_runtime_health_observation_for_run(
        &self,
        request: &RuntimeHealthObservationRequest,
        session_id: &RuntimeSessionId,
        run_id: &RuntimeRunId,
        run_generation: RuntimeGeneration,
    ) -> Result<RunScopedRuntimeHealthObservationOutcome, JournalError> {
        validate_reason_code(request.reason_code.as_str())?;
        if request.observed_at_unix_ms < 0 {
            return Err(JournalError::InvalidArgument(
                "runtime health observation timestamp is invalid".to_owned(),
            ));
        }
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (generation_matches, expected_generation) = runtime_generation_fence_matches_tx(
            &transaction,
            session_id.as_str(),
            run_id.as_str(),
            RuntimeGenerationLane::Run,
            run_generation,
        )?;
        let outcome = if generation_matches {
            RunScopedRuntimeHealthObservationOutcome::Applied(record_runtime_health_observation_tx(
                &transaction,
                request,
            )?)
        } else {
            RunScopedRuntimeHealthObservationOutcome::Stale { expected_generation }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Upserts a validated component-health projection and records the transition.
    ///
    /// This bootstrap/test compatibility path cannot roll back the durable generation
    /// head; production lifecycle changes use the exact atomic operations above.
    #[cfg(test)]
    pub fn upsert_runtime_component_health(
        &self,
        health: &RuntimeComponentHealthV1,
    ) -> Result<(), JournalError> {
        health.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        let raw = serde_json::to_vec(health)?;
        let (json, _) = sanitize_payload(raw.as_slice())?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let previous_json = transaction
            .query_row(
                "SELECT health_json FROM runtime_component_health WHERE component_ulid = ?1",
                params![health.component_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let previous = previous_json
            .as_deref()
            .map(serde_json::from_str::<RuntimeComponentHealthV1>)
            .transpose()?;
        if let Some(previous) = previous.as_ref() {
            previous
                .can_transition_to(health.state)
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
            if health.generation < previous.generation
                || health.authority_class != previous.authority_class
                || health.security_quarantine != previous.security_quarantine
                || health.policy != previous.policy
            {
                return Err(JournalError::InvalidArgument(
                    "runtime health update changed protected durable invariants".to_owned(),
                ));
            }
        }
        transaction.execute(
            r#"
                INSERT INTO runtime_component_health (
                    component_ulid, generation, state, reason_code, health_json, updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(component_ulid) DO UPDATE SET
                    generation = excluded.generation,
                    state = excluded.state,
                    reason_code = excluded.reason_code,
                    health_json = excluded.health_json,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                health.component_id.as_str(),
                i64::try_from(health.generation.get()).unwrap_or(i64::MAX),
                health.state.as_str(),
                health.reason_code,
                json,
                health.updated_at_unix_ms,
            ],
        )?;
        transaction.execute(
            r#"
                INSERT INTO runtime_component_health_events (
                    event_ulid, component_ulid, from_state, to_state, reason_code,
                    created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                Ulid::new().to_string(),
                health.component_id.as_str(),
                previous.as_ref().map(|value| value.state.as_str()),
                health.state.as_str(),
                health.reason_code,
                health.updated_at_unix_ms,
            ],
        )?;
        ensure_component_generation_head_tx(
            &transaction,
            health.component_id.as_str(),
            health.generation,
            health.updated_at_unix_ms,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Persists one runtime handle descriptor.
    #[cfg(test)]
    pub fn upsert_runtime_handle(
        &self,
        descriptor: &RuntimeHandleDescriptorV1,
    ) -> Result<(), JournalError> {
        descriptor.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        let raw = serde_json::to_vec(descriptor)?;
        let (json, _) = sanitize_payload(raw.as_slice())?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard.execute(
            r#"
                INSERT INTO runtime_handles (
                    instance_ulid, session_ulid, run_ulid, generation, kind, state,
                    descriptor_json, updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ON CONFLICT(instance_ulid) DO UPDATE SET
                    generation = excluded.generation,
                    state = excluded.state,
                    descriptor_json = excluded.descriptor_json,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                descriptor.instance_id.as_str(),
                descriptor.session_id.as_ref().map(|value| value.as_str()),
                descriptor.run_id.as_ref().map(|value| value.as_str()),
                i64::try_from(descriptor.generation.get()).unwrap_or(i64::MAX),
                descriptor.kind.as_str(),
                descriptor.state.as_str(),
                json,
                descriptor.updated_at_unix_ms,
            ],
        )?;
        Ok(())
    }

    /// Atomically authorizes the active run generation and persists a process handle and lease.
    pub fn register_process_handle_and_lease_for_active_generation(
        &self,
        descriptor: &RuntimeHandleDescriptorV1,
        lease: &ProcessLeaseV1,
    ) -> Result<(), JournalError> {
        self.register_process_handle_and_lease_inner(descriptor, lease, true)
    }

    /// Atomically persists a process handle and its exact durable lease.
    pub fn register_process_handle_and_lease(
        &self,
        descriptor: &RuntimeHandleDescriptorV1,
        lease: &ProcessLeaseV1,
    ) -> Result<(), JournalError> {
        self.register_process_handle_and_lease_inner(descriptor, lease, false)
    }

    fn register_process_handle_and_lease_inner(
        &self,
        descriptor: &RuntimeHandleDescriptorV1,
        lease: &ProcessLeaseV1,
        require_active_generation: bool,
    ) -> Result<(), JournalError> {
        descriptor.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        lease.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        if descriptor.kind != palyra_common::runtime_contracts::RuntimeHandleKind::Process
            || descriptor.instance_id != lease.instance_id
            || descriptor.generation != lease.generation
        {
            return Err(JournalError::InvalidArgument(
                "process handle registration requires matching process ownership".to_owned(),
            ));
        }
        let descriptor_raw = serde_json::to_vec(descriptor)?;
        let (descriptor_json, _) = sanitize_payload(descriptor_raw.as_slice())?;
        let provenance_json = serde_json::to_string(&lease.provenance)?;
        if provenance_json.len() > self.config.max_payload_bytes {
            return Err(JournalError::PayloadTooLarge {
                payload_kind: "runtime_process_provenance",
                actual_bytes: provenance_json.len(),
                max_bytes: self.config.max_payload_bytes,
            });
        }
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if require_active_generation {
            let session_id = descriptor.session_id.as_ref().ok_or_else(|| {
                JournalError::InvalidArgument(
                    "active-generation process registration requires a session id".to_owned(),
                )
            })?;
            let run_id = descriptor.run_id.as_ref().ok_or_else(|| {
                JournalError::InvalidArgument(
                    "active-generation process registration requires a run id".to_owned(),
                )
            })?;
            authorize_side_effect_generation_tx(
                &transaction,
                session_id.as_str(),
                run_id.as_str(),
                descriptor.generation,
                current_unix_ms()?,
            )?;
        }
        transaction.execute(
            r#"
                INSERT INTO runtime_handles (
                    instance_ulid, session_ulid, run_ulid, generation, kind, state,
                    descriptor_json, updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ON CONFLICT(instance_ulid) DO UPDATE SET
                    generation = excluded.generation,
                    state = excluded.state,
                    descriptor_json = excluded.descriptor_json,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                descriptor.instance_id.as_str(),
                descriptor.session_id.as_ref().map(|value| value.as_str()),
                descriptor.run_id.as_ref().map(|value| value.as_str()),
                i64::try_from(descriptor.generation.get()).unwrap_or(i64::MAX),
                descriptor.kind.as_str(),
                descriptor.state.as_str(),
                descriptor_json,
                descriptor.updated_at_unix_ms,
            ],
        )?;
        transaction.execute(
            r#"
                INSERT INTO runtime_process_leases (
                    lease_ulid, instance_ulid, pid, generation, provenance_json,
                    issued_at_unix_ms, expires_at_unix_ms, verified_at_unix_ms,
                    schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1)
                ON CONFLICT(instance_ulid) DO UPDATE SET
                    lease_ulid = excluded.lease_ulid,
                    pid = excluded.pid,
                    generation = excluded.generation,
                    provenance_json = excluded.provenance_json,
                    issued_at_unix_ms = excluded.issued_at_unix_ms,
                    expires_at_unix_ms = excluded.expires_at_unix_ms,
                    verified_at_unix_ms = excluded.verified_at_unix_ms,
                    schema_version = excluded.schema_version
            "#,
            params![
                lease.lease_id.as_str(),
                lease.instance_id.as_str(),
                i64::from(lease.pid),
                i64::try_from(lease.generation.get()).unwrap_or(i64::MAX),
                provenance_json,
                lease.issued_at_unix_ms,
                lease.expires_at_unix_ms,
                lease.verified_at_unix_ms,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Loads the first bounded page of process handles that still retain durable leases.
    #[cfg(test)]
    pub fn list_persisted_process_leases(
        &self,
        limit: usize,
    ) -> Result<Vec<PersistedProcessLeaseRecord>, JournalError> {
        self.list_persisted_process_leases_after(None, limit)
    }

    /// Loads a bounded keyset page of durable process leases after an optional lease identity.
    pub fn list_persisted_process_leases_after(
        &self,
        after_lease_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PersistedProcessLeaseRecord>, JournalError> {
        if limit == 0 || limit > 1_000 {
            return Err(JournalError::InvalidArgument(
                "persisted process lease limit must be between 1 and 1000".to_owned(),
            ));
        }
        let after_lease_id = after_lease_id
            .map(|value| {
                palyra_common::runtime_contracts::RuntimeLeaseId::parse(value)
                    .map(|lease_id| lease_id.into_inner())
                    .map_err(|error| JournalError::InvalidArgument(error.to_string()))
            })
            .transpose()?;
        let sqlite_limit = i64::try_from(limit).map_err(|_| {
            JournalError::InvalidArgument(
                "persisted process lease limit exceeds sqlite integer range".to_owned(),
            )
        })?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT h.descriptor_json, h.instance_ulid, h.session_ulid, h.run_ulid,
                       h.generation, h.kind, h.state, h.updated_at_unix_ms,
                       l.lease_ulid, l.instance_ulid, l.pid, l.generation,
                       l.provenance_json, l.issued_at_unix_ms, l.expires_at_unix_ms,
                       l.verified_at_unix_ms, l.schema_version
                FROM runtime_handles h
                INNER JOIN runtime_process_leases l ON l.instance_ulid = h.instance_ulid
                WHERE (?1 IS NULL OR l.lease_ulid > ?1)
                ORDER BY l.lease_ulid ASC
                LIMIT ?2
            "#,
        )?;
        let rows =
            statement.query_map(params![after_lease_id.as_deref(), sqlite_limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, String>(12)?,
                    row.get::<_, i64>(13)?,
                    row.get::<_, i64>(14)?,
                    row.get::<_, i64>(15)?,
                    row.get::<_, i64>(16)?,
                ))
            })?;
        let mut records = Vec::new();
        for row in rows {
            let (
                descriptor_json,
                handle_instance_id,
                handle_session_id,
                handle_run_id,
                handle_generation,
                handle_kind,
                handle_state,
                handle_updated_at_unix_ms,
                lease_id,
                lease_instance_id,
                pid,
                lease_generation,
                provenance_json,
                issued_at_unix_ms,
                expires_at_unix_ms,
                verified_at_unix_ms,
                schema_version,
            ) = row?;
            let descriptor: RuntimeHandleDescriptorV1 = serde_json::from_str(&descriptor_json)?;
            descriptor
                .validate()
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
            let relational_handle_generation =
                RuntimeGeneration::new(u64::try_from(handle_generation).map_err(|_| {
                    JournalError::InvalidArgument(
                        "persisted runtime handle generation is invalid".to_owned(),
                    )
                })?)
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
            if descriptor.instance_id.as_str() != handle_instance_id
                || descriptor.session_id.as_ref().map(|value| value.as_str())
                    != handle_session_id.as_deref()
                || descriptor.run_id.as_ref().map(|value| value.as_str())
                    != handle_run_id.as_deref()
                || descriptor.generation != relational_handle_generation
                || descriptor.kind.as_str() != handle_kind
                || descriptor.state.as_str() != handle_state
                || descriptor.updated_at_unix_ms != handle_updated_at_unix_ms
            {
                return Err(JournalError::InvalidArgument(
                    "persisted runtime handle columns do not match descriptor JSON".to_owned(),
                ));
            }
            let lease = ProcessLeaseV1 {
                schema_version: u32::try_from(schema_version).map_err(|_| {
                    JournalError::InvalidArgument(
                        "persisted process lease schema version is invalid".to_owned(),
                    )
                })?,
                lease_id: palyra_common::runtime_contracts::RuntimeLeaseId::parse(
                    lease_id.as_str(),
                )
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
                instance_id: palyra_common::runtime_contracts::RuntimeInstanceId::parse(
                    lease_instance_id.as_str(),
                )
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
                generation: RuntimeGeneration::new(u64::try_from(lease_generation).map_err(
                    |_| {
                        JournalError::InvalidArgument(
                            "persisted process lease generation is invalid".to_owned(),
                        )
                    },
                )?)
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
                pid: u32::try_from(pid).map_err(|_| {
                    JournalError::InvalidArgument("persisted process pid is invalid".to_owned())
                })?,
                provenance: serde_json::from_str(provenance_json.as_str())?,
                issued_at_unix_ms,
                expires_at_unix_ms,
                verified_at_unix_ms,
            };
            lease.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
            if descriptor.instance_id != lease.instance_id
                || descriptor.generation != lease.generation
                || descriptor.kind != palyra_common::runtime_contracts::RuntimeHandleKind::Process
            {
                return Err(JournalError::InvalidArgument(
                    "persisted process handle and lease ownership do not match".to_owned(),
                ));
            }
            records.push(PersistedProcessLeaseRecord { descriptor, lease });
        }
        Ok(records)
    }

    /// Loads the durable keyset cursor for bounded process-lease reconciliation.
    pub fn process_reconciliation_checkpoint(
        &self,
        checkpoint_key: &str,
    ) -> Result<Option<String>, JournalError> {
        let checkpoint_key = checkpoint_key.trim();
        if checkpoint_key.is_empty() || checkpoint_key.len() > 128 {
            return Err(JournalError::InvalidArgument(
                "process reconciliation checkpoint key is invalid".to_owned(),
            ));
        }
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let after_lease_id = guard
            .query_row(
                r#"
                    SELECT after_lease_ulid
                    FROM runtime_process_reconciliation_checkpoint
                    WHERE checkpoint_key = ?1 AND schema_version = 1
                "#,
                params![checkpoint_key],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;
        let Some(after_lease_id) = after_lease_id.flatten() else {
            return Ok(None);
        };
        let lease_id =
            palyra_common::runtime_contracts::RuntimeLeaseId::parse(after_lease_id.as_str())
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        Ok(Some(lease_id.into_inner()))
    }

    /// Persists the next keyset cursor only after a reconciliation page commits successfully.
    pub fn update_process_reconciliation_checkpoint(
        &self,
        checkpoint_key: &str,
        after_lease_id: Option<&str>,
        updated_at_unix_ms: i64,
    ) -> Result<(), JournalError> {
        let checkpoint_key = checkpoint_key.trim();
        if checkpoint_key.is_empty() || checkpoint_key.len() > 128 || updated_at_unix_ms < 0 {
            return Err(JournalError::InvalidArgument(
                "process reconciliation checkpoint update is invalid".to_owned(),
            ));
        }
        let after_lease_id = after_lease_id
            .map(|value| {
                palyra_common::runtime_contracts::RuntimeLeaseId::parse(value)
                    .map(|lease_id| lease_id.into_inner())
                    .map_err(|error| JournalError::InvalidArgument(error.to_string()))
            })
            .transpose()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        guard.execute(
            r#"
                INSERT INTO runtime_process_reconciliation_checkpoint (
                    checkpoint_key, after_lease_ulid, updated_at_unix_ms, schema_version
                ) VALUES (?1, ?2, ?3, 1)
                ON CONFLICT(checkpoint_key) DO UPDATE SET
                    after_lease_ulid = excluded.after_lease_ulid,
                    updated_at_unix_ms = excluded.updated_at_unix_ms,
                    schema_version = excluded.schema_version
            "#,
            params![checkpoint_key, after_lease_id.as_deref(), updated_at_unix_ms],
        )?;
        Ok(())
    }

    /// Atomically fences worker expiry evidence and compare-and-swaps the durable fleet snapshot.
    ///
    /// Empty plans are a true no-op and do not advance the fleet generation.
    ///
    /// # Errors
    /// Returns [`JournalError`] when either exact expiry evidence or the candidate fleet is
    /// malformed, exceeds its bound, conflicts with existing evidence or fleet generation, or
    /// storage fails.
    pub fn commit_networked_worker_expiry_plan(
        &self,
        records: &[NetworkedWorkerExpiryOutboxRecord],
        fleet: &std::collections::BTreeMap<String, palyra_workerd::WorkerFleetRecord>,
        expected_generation: u64,
        max_outbox_entries: usize,
        max_fleet_entries: usize,
        updated_at_unix_ms: i64,
    ) -> Result<u64, JournalError> {
        if records.is_empty() {
            return Ok(expected_generation);
        }
        validate_networked_worker_fleet_snapshot(fleet, max_fleet_entries, updated_at_unix_ms)?;
        for record in records {
            record.validate()?;
        }
        let revocations = records
            .iter()
            .map(|record| {
                Ok(NetworkedWorkerLeaseRevocation {
                    worker_id: record.event.worker_id.clone(),
                    lease_id: record.event.lease_id.clone().ok_or_else(|| {
                        JournalError::InvalidArgument(
                            "networked worker expiry evidence has no lease identity".to_owned(),
                        )
                    })?,
                    run_id: record.event.run_id.clone().ok_or_else(|| {
                        JournalError::InvalidArgument(
                            "networked worker expiry evidence has no run identity".to_owned(),
                        )
                    })?,
                    reason_code: record.event.reason_code.clone(),
                })
            })
            .collect::<Result<Vec<_>, JournalError>>()?;
        let encoded_fleet = encode_networked_worker_fleet_records(fleet)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        enqueue_networked_worker_expiry_outbox_batch_tx(&transaction, records, max_outbox_entries)?;
        let generation = replace_networked_worker_fleet_records_tx(
            &transaction,
            encoded_fleet.as_slice(),
            expected_generation,
            updated_at_unix_ms,
        )?;
        revoke_networked_worker_dispatch_claims_tx(
            &transaction,
            revocations.as_slice(),
            generation,
            updated_at_unix_ms,
        )?;
        transaction.commit()?;
        Ok(generation)
    }

    /// Creates a queued dispatch claim under the exact current durable worker lease.
    ///
    /// Exact duplicate requests are idempotent. Identity conflicts, absent or changed leases, and
    /// expired authority fail closed before any node payload can be queued.
    ///
    /// # Errors
    /// Returns [`JournalError`] when request metadata is malformed, claim capacity is exhausted,
    /// durable lease authority does not match, or SQLite cannot commit.
    pub fn create_networked_worker_dispatch_claim(
        &self,
        request: &NetworkedWorkerDispatchClaimCreateRequest,
        max_entries: usize,
        created_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerDispatchClaim, JournalError> {
        validate_networked_worker_dispatch_claim_create_request(request, created_at_unix_ms)?;
        validate_networked_worker_dispatch_claim_bound(max_entries)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = load_networked_worker_dispatch_claim_by_remote_request_tx(
            &transaction,
            request.remote_request_id.as_str(),
        )? {
            if networked_worker_dispatch_claim_matches_create(&existing, request) {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(JournalError::NetworkedWorkerDispatchClaimConflict {
                remote_request_id: request.remote_request_id.clone(),
            });
        }
        if let Some(existing) = load_archived_networked_worker_dispatch_claim_by_remote_request_tx(
            &transaction,
            request.remote_request_id.as_str(),
        )? {
            if archived_networked_worker_dispatch_claim_matches_create(&existing, request) {
                transaction.commit()?;
                return Ok(existing);
            }
            return Err(JournalError::NetworkedWorkerDispatchClaimConflict {
                remote_request_id: request.remote_request_id.clone(),
            });
        }
        if let Some(existing_remote_request_id) = transaction
            .query_row(
                r#"
                    SELECT remote_request_ulid
                    FROM (
                        SELECT remote_request_ulid, node_request_ulid
                        FROM runtime_networked_worker_dispatch_claims
                        UNION ALL
                        SELECT remote_request_ulid, node_request_ulid
                        FROM runtime_networked_worker_dispatch_claim_terminal_evidence
                    )
                    WHERE node_request_ulid = ?1
                    LIMIT 1
                "#,
                params![request.node_request_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            return Err(JournalError::NetworkedWorkerDispatchClaimConflict {
                remote_request_id: existing_remote_request_id,
            });
        }
        let entry_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM runtime_networked_worker_dispatch_claims",
            [],
            |row| row.get(0),
        )?;
        let mut current_entries = usize::try_from(entry_count).unwrap_or(usize::MAX);
        if current_entries >= max_entries {
            let required_reclamation =
                current_entries.saturating_sub(max_entries).saturating_add(1);
            let reclaimed =
                archive_networked_worker_dispatch_claims_tx(&transaction, required_reclamation)?;
            current_entries = current_entries.saturating_sub(reclaimed);
        }
        if current_entries >= max_entries {
            return Err(JournalError::NetworkedWorkerDispatchClaimCapacityExceeded {
                current_entries,
                max_entries,
            });
        }
        let (generation_matches, _) = runtime_generation_fence_matches_tx(
            &transaction,
            request.session_id.as_str(),
            request.run_id.as_str(),
            RuntimeGenerationLane::Run,
            request.run_generation,
        )?;
        if !generation_matches {
            return Err(JournalError::NetworkedWorkerDispatchAuthorityRejected {
                remote_request_id: request.remote_request_id.clone(),
            });
        }
        let (fleet_generation, fleet_schema_version) = transaction.query_row(
            r#"
                SELECT generation, schema_version
                FROM runtime_networked_worker_fleet_meta
                WHERE singleton_key = 1
            "#,
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        if fleet_schema_version != 1 {
            return Err(JournalError::InvalidArgument(
                "networked worker fleet generation schema version is unsupported".to_owned(),
            ));
        }
        let fleet_generation = u64::try_from(fleet_generation).map_err(|_| {
            JournalError::InvalidArgument("networked worker fleet generation is invalid".to_owned())
        })?;
        let record_json = transaction
            .query_row(
                r#"
                    SELECT record_json
                    FROM runtime_networked_worker_fleet
                    WHERE worker_id = ?1 AND schema_version = 1
                "#,
                params![request.worker_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| JournalError::NetworkedWorkerDispatchAuthorityRejected {
                remote_request_id: request.remote_request_id.clone(),
            })?;
        let record: palyra_workerd::WorkerFleetRecord = serde_json::from_str(record_json.as_str())?;
        let active_lease = record.lease.as_ref().ok_or_else(|| {
            JournalError::NetworkedWorkerDispatchAuthorityRejected {
                remote_request_id: request.remote_request_id.clone(),
            }
        })?;
        if active_lease.worker_id != request.worker_id
            || active_lease.lease_id != request.lease_id
            || active_lease.run_id != request.run_id
            || active_lease.expires_at_unix_ms != request.lease_expires_at_unix_ms
            || active_lease.expires_at_unix_ms <= created_at_unix_ms
            || !matches!(
                record.state,
                palyra_workerd::WorkerLifecycleState::Assigned
                    | palyra_workerd::WorkerLifecycleState::Busy
                    | palyra_workerd::WorkerLifecycleState::Degraded
                    | palyra_workerd::WorkerLifecycleState::Draining
            )
        {
            return Err(JournalError::NetworkedWorkerDispatchAuthorityRejected {
                remote_request_id: request.remote_request_id.clone(),
            });
        }
        let fleet_generation_sql = i64::try_from(fleet_generation).map_err(|_| {
            JournalError::InvalidArgument(
                "networked worker fleet generation exceeds sqlite integer range".to_owned(),
            )
        })?;
        transaction.execute(
            r#"
                INSERT INTO runtime_networked_worker_dispatch_claims (
                    remote_request_ulid, node_request_ulid, worker_id, lease_ulid, run_ulid,
                    issued_fleet_generation, dispatch_fleet_generation,
                    revoked_fleet_generation, lease_expires_at_unix_ms,
                    capability, request_sha256, state,
                    reconciliation_disposition, terminal_reason_code, created_at_unix_ms,
                    updated_at_unix_ms, completed_at_unix_ms, schema_version,
                    session_ulid, run_generation
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7, ?8, ?9, 'queued',
                    NULL, NULL, ?10, ?10, NULL, 3, ?11, ?12
                )
            "#,
            params![
                request.remote_request_id,
                request.node_request_id,
                request.worker_id,
                request.lease_id,
                request.run_id,
                fleet_generation_sql,
                request.lease_expires_at_unix_ms,
                request.capability,
                request.request_sha256,
                created_at_unix_ms,
                request.session_id,
                i64::try_from(request.run_generation.get()).map_err(|_| {
                    JournalError::InvalidArgument(
                        "networked worker run generation exceeds sqlite integer range".to_owned(),
                    )
                })?,
            ],
        )?;
        transaction.execute(
            r#"
                UPDATE runtime_networked_worker_dispatch_claims
                SET delivery_payload_present = 1
                WHERE remote_request_ulid = ?1 AND schema_version = 3
            "#,
            params![request.remote_request_id],
        )?;
        let claim = load_networked_worker_dispatch_claim_by_remote_request_tx(
            &transaction,
            request.remote_request_id.as_str(),
        )?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "networked worker dispatch claim insert was not observable".to_owned(),
            )
        })?;
        transaction.commit()?;
        Ok(claim)
    }

    /// Test-only adapter for legacy dispatch tests that modeled reservation and release as one step.
    ///
    /// Production callers must use [`Self::reserve_networked_worker_delivery`] followed by
    /// [`Self::release_networked_worker_payload`], because only the latter may authorize bytes to
    /// leave the daemon.
    ///
    /// # Errors
    /// Returns [`JournalError`] when claim metadata is malformed, storage is invalid, or SQLite
    /// cannot commit. Lease revocation, expiry, or payload mismatch returns `Rejected`.
    #[cfg(test)]
    pub fn begin_networked_worker_dispatch(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        request_sha256: &str,
        observed_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerDispatchBeginOutcome, JournalError> {
        let delivery_attempt_id = Ulid::new().to_string();
        let delivery_token = format!("{}{}", Ulid::new(), Ulid::new());
        let reservation =
            self.reserve_networked_worker_delivery(&NetworkedWorkerDeliveryReservationRequest {
                remote_request_id: remote_request_id.to_owned(),
                node_request_id: node_request_id.to_owned(),
                request_sha256: request_sha256.to_owned(),
                delivery_attempt_id: delivery_attempt_id.clone(),
                delivery_token_sha256: sha256_hex(delivery_token.as_bytes()),
                observed_at_unix_ms,
            })?;
        let NetworkedWorkerDeliveryReservationOutcome::Authorized { fleet_generation } =
            reservation
        else {
            return Ok(NetworkedWorkerDeliveryReservationOutcome::Rejected);
        };
        let Some(claim) = self.networked_worker_dispatch_claim(remote_request_id)? else {
            return Ok(NetworkedWorkerDeliveryReservationOutcome::Rejected);
        };
        match self.release_networked_worker_payload(&NetworkedWorkerPayloadReleaseRequest {
            node_request_id: node_request_id.to_owned(),
            delivery_attempt_id,
            delivery_token,
            reporting_worker_id: claim.worker_id,
            observed_at_unix_ms,
        })? {
            NetworkedWorkerPayloadReleaseOutcome::Released
            | NetworkedWorkerPayloadReleaseOutcome::AlreadyReleased => {
                Ok(NetworkedWorkerDeliveryReservationOutcome::Authorized { fleet_generation })
            }
            NetworkedWorkerPayloadReleaseOutcome::Rejected => {
                Ok(NetworkedWorkerDeliveryReservationOutcome::Rejected)
            }
        }
    }

    /// Reserves one exact metadata-only worker delivery attempt.
    ///
    /// # Errors
    /// Returns [`JournalError`] when claim metadata is malformed, storage is invalid, or SQLite
    /// cannot commit. Lease revocation, expiry, or payload mismatch returns `Rejected`.
    pub fn reserve_networked_worker_delivery(
        &self,
        request: &NetworkedWorkerDeliveryReservationRequest,
    ) -> Result<NetworkedWorkerDeliveryReservationOutcome, JournalError> {
        validate_networked_worker_dispatch_lookup(
            request.remote_request_id.as_str(),
            request.node_request_id.as_str(),
            request.request_sha256.as_str(),
            request.observed_at_unix_ms,
        )?;
        validate_runtime_identity(request.delivery_attempt_id.as_str(), "delivery attempt id")?;
        validate_sha256(request.delivery_token_sha256.as_str())?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(claim) = load_networked_worker_dispatch_claim_by_remote_request_tx(
            &transaction,
            request.remote_request_id.as_str(),
        )?
        else {
            transaction.commit()?;
            return Ok(NetworkedWorkerDeliveryReservationOutcome::Rejected);
        };
        if claim.node_request_id != request.node_request_id
            || claim.request_sha256 != request.request_sha256
            || claim.state != NetworkedWorkerDispatchClaimState::Queued
            || claim.schema_version != NETWORKED_WORKER_DISPATCH_CLAIM_SCHEMA_VERSION
            || claim.lease_expires_at_unix_ms <= request.observed_at_unix_ms
        {
            transaction.commit()?;
            return Ok(NetworkedWorkerDeliveryReservationOutcome::Rejected);
        }
        let (fleet_generation, fleet_schema_version) = transaction.query_row(
            r#"
                SELECT generation, schema_version
                FROM runtime_networked_worker_fleet_meta
                WHERE singleton_key = 1
            "#,
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        if fleet_schema_version != 1 {
            return Err(JournalError::InvalidArgument(
                "networked worker fleet generation schema version is unsupported".to_owned(),
            ));
        }
        let fleet_generation = u64::try_from(fleet_generation).map_err(|_| {
            JournalError::InvalidArgument("networked worker fleet generation is invalid".to_owned())
        })?;
        let record_json = transaction
            .query_row(
                r#"
                    SELECT record_json
                    FROM runtime_networked_worker_fleet
                    WHERE worker_id = ?1 AND schema_version = 1
                "#,
                params![claim.worker_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let lease_matches = record_json
            .as_deref()
            .map(serde_json::from_str::<palyra_workerd::WorkerFleetRecord>)
            .transpose()?
            .is_some_and(|record| {
                matches!(
                    record.state,
                    palyra_workerd::WorkerLifecycleState::Assigned
                        | palyra_workerd::WorkerLifecycleState::Busy
                        | palyra_workerd::WorkerLifecycleState::Degraded
                        | palyra_workerd::WorkerLifecycleState::Draining
                ) && record.lease.as_ref().is_some_and(|lease| {
                    lease.worker_id == claim.worker_id
                        && lease.lease_id == claim.lease_id
                        && lease.run_id == claim.run_id
                        && lease.expires_at_unix_ms == claim.lease_expires_at_unix_ms
                        && lease.expires_at_unix_ms > request.observed_at_unix_ms
                })
            });
        if !lease_matches {
            transaction.commit()?;
            return Ok(NetworkedWorkerDeliveryReservationOutcome::Rejected);
        }
        let fleet_generation_sql = i64::try_from(fleet_generation).map_err(|_| {
            JournalError::InvalidArgument(
                "networked worker fleet generation exceeds sqlite integer range".to_owned(),
            )
        })?;
        let persisted_at_unix_ms = request.observed_at_unix_ms.max(claim.updated_at_unix_ms);
        let updated = transaction.execute(
            r#"
                UPDATE runtime_networked_worker_dispatch_claims
                SET state = 'in_flight', dispatch_fleet_generation = ?1,
                    delivery_attempt_ulid = ?2, delivery_token_sha256 = ?3,
                    delivery_reserved_at_unix_ms = ?4,
                    payload_released_at_unix_ms = NULL,
                    payload_release_fleet_generation = NULL,
                    payload_acknowledged_at_unix_ms = NULL,
                    delivery_disposition = 'reserved_unreleased',
                    delivery_payload_present = 1,
                    updated_at_unix_ms = ?4
                WHERE remote_request_ulid = ?5
                  AND node_request_ulid = ?6
                  AND request_sha256 = ?7
                  AND state = 'queued'
                  AND schema_version = 3
            "#,
            params![
                fleet_generation_sql,
                request.delivery_attempt_id,
                request.delivery_token_sha256,
                persisted_at_unix_ms,
                request.remote_request_id,
                request.node_request_id,
                request.request_sha256,
            ],
        )?;
        if updated != 1 {
            transaction.commit()?;
            return Ok(NetworkedWorkerDeliveryReservationOutcome::Rejected);
        }
        transaction.commit()?;
        Ok(NetworkedWorkerDeliveryReservationOutcome::Authorized { fleet_generation })
    }

    /// Verifies that a reporting worker owns the exact active node-backed dispatch claim.
    ///
    /// Both in-flight and reconciling claims can return a result; semantic validation and the
    /// dedicated completion or late-reconciliation transaction remain responsible for settlement.
    ///
    /// # Errors
    /// Returns [`JournalError`] when lookup metadata is malformed or SQLite cannot inspect state.
    #[cfg(test)]
    pub fn authorize_networked_worker_result(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        reporting_worker_id: &str,
    ) -> Result<NetworkedWorkerResultAuthorizationOutcome, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let claim = guard
            .query_row(
                format!(
                    "{NETWORKED_WORKER_DISPATCH_CLAIM_SELECT} WHERE remote_request_ulid = ?1 \
                     AND schema_version = 3"
                )
                .as_str(),
                params![remote_request_id],
                hydrate_networked_worker_dispatch_claim,
            )
            .optional()?;
        drop(guard);
        let Some(claim) = claim else {
            return Ok(NetworkedWorkerResultAuthorizationOutcome::Rejected);
        };
        let Some(delivery_attempt_id) = claim.delivery_attempt_id.as_deref() else {
            return Ok(NetworkedWorkerResultAuthorizationOutcome::Rejected);
        };
        let Some(run_generation) = claim.run_generation else {
            return Ok(NetworkedWorkerResultAuthorizationOutcome::Rejected);
        };
        self.authorize_networked_worker_result_attempt(
            remote_request_id,
            node_request_id,
            delivery_attempt_id,
            run_generation,
            reporting_worker_id,
            current_unix_ms()?,
        )
    }

    /// Verifies that a reporting worker owns the exact released delivery attempt.
    ///
    /// # Errors
    /// Returns [`JournalError`] when lookup metadata is malformed or SQLite cannot inspect state.
    #[cfg(test)]
    pub fn authorize_networked_worker_result_attempt(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        delivery_attempt_id: &str,
        run_generation: RuntimeGeneration,
        reporting_worker_id: &str,
        observed_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerResultAuthorizationOutcome, JournalError> {
        validate_runtime_identity(remote_request_id, "remote request id")?;
        validate_runtime_identity(node_request_id, "node request id")?;
        validate_runtime_identity(delivery_attempt_id, "delivery attempt id")?;
        validate_worker_id(reporting_worker_id)?;
        if observed_at_unix_ms < 0 {
            return Err(JournalError::InvalidArgument(
                "networked worker result observation timestamp is invalid".to_owned(),
            ));
        }
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let claim = guard
            .query_row(
                format!(
                    "{NETWORKED_WORKER_DISPATCH_CLAIM_SELECT} WHERE remote_request_ulid = ?1 \
                     AND schema_version = 3"
                )
                .as_str(),
                params![remote_request_id],
                hydrate_networked_worker_dispatch_claim,
            )
            .optional()?;
        let Some(claim) = claim else {
            return Ok(NetworkedWorkerResultAuthorizationOutcome::Rejected);
        };
        if claim.node_request_id != node_request_id
            || claim.worker_id != reporting_worker_id
            || claim.delivery_attempt_id.as_deref() != Some(delivery_attempt_id)
            || claim.run_generation != Some(run_generation)
            || claim.payload_released_at_unix_ms.is_none()
            || claim.lease_expires_at_unix_ms <= observed_at_unix_ms
        {
            return Ok(NetworkedWorkerResultAuthorizationOutcome::Rejected);
        }
        validate_networked_worker_dispatch_claim_evidence(
            &claim,
            NetworkedWorkerDispatchClaimEvidenceLocation::Active,
        )?;
        let Some(session_id) = claim.session_id.as_deref() else {
            return Ok(NetworkedWorkerResultAuthorizationOutcome::Rejected);
        };
        let (generation_matches, _) = runtime_generation_fence_matches_tx(
            &guard,
            session_id,
            claim.run_id.as_str(),
            RuntimeGenerationLane::Run,
            run_generation,
        )?;
        if !generation_matches {
            return Ok(NetworkedWorkerResultAuthorizationOutcome::Rejected);
        }
        Ok(
            if matches!(
                claim.state,
                NetworkedWorkerDispatchClaimState::InFlight
                    | NetworkedWorkerDispatchClaimState::Reconciling
            ) {
                NetworkedWorkerResultAuthorizationOutcome::Authorized
            } else {
                NetworkedWorkerResultAuthorizationOutcome::Rejected
            },
        )
    }

    /// Atomically marks one exact reserved payload as released before bytes leave the daemon.
    ///
    /// # Errors
    /// Returns [`JournalError`] when request metadata is malformed or SQLite cannot commit.
    pub fn release_networked_worker_payload(
        &self,
        request: &NetworkedWorkerPayloadReleaseRequest,
    ) -> Result<NetworkedWorkerPayloadReleaseOutcome, JournalError> {
        validate_runtime_identity(request.node_request_id.as_str(), "node request id")?;
        validate_runtime_identity(request.delivery_attempt_id.as_str(), "delivery attempt id")?;
        if request.delivery_token.len() < 32 || request.delivery_token.len() > 256 {
            return Err(JournalError::InvalidArgument(
                "networked worker delivery token length is invalid".to_owned(),
            ));
        }
        let delivery_token_sha256 = sha256_hex(request.delivery_token.as_bytes());
        validate_worker_id(request.reporting_worker_id.as_str())?;
        if request.observed_at_unix_ms < 0 {
            return Err(JournalError::InvalidArgument(
                "networked worker payload release timestamp is invalid".to_owned(),
            ));
        }
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let claim = transaction
            .query_row(
                format!(
                    "{NETWORKED_WORKER_DISPATCH_CLAIM_SELECT} WHERE node_request_ulid = ?1 \
                     AND schema_version = 3"
                )
                .as_str(),
                params![request.node_request_id],
                hydrate_networked_worker_dispatch_claim,
            )
            .optional()?;
        let Some(claim) = claim else {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadReleaseOutcome::Rejected);
        };
        validate_networked_worker_dispatch_claim_evidence(
            &claim,
            NetworkedWorkerDispatchClaimEvidenceLocation::Active,
        )?;
        let exact_binding = claim.worker_id == request.reporting_worker_id
            && claim.delivery_attempt_id.as_deref() == Some(request.delivery_attempt_id.as_str())
            && claim.delivery_token_sha256.as_deref() == Some(delivery_token_sha256.as_str());
        if !exact_binding {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadReleaseOutcome::Rejected);
        }
        if claim.state == NetworkedWorkerDispatchClaimState::Cancelled
            && claim.reconciliation_disposition.as_deref() == Some("payload_not_released")
            && claim.payload_released_at_unix_ms.is_none()
        {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadReleaseOutcome::Rejected);
        }
        if claim.payload_released_at_unix_ms.is_some() {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadReleaseOutcome::AlreadyReleased);
        }
        if claim.state != NetworkedWorkerDispatchClaimState::InFlight
            || claim.delivery_disposition.as_deref() != Some("reserved_unreleased")
            || claim.delivery_payload_present != Some(true)
            || claim.revoked_fleet_generation.is_some()
            || claim.lease_expires_at_unix_ms <= request.observed_at_unix_ms
        {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadReleaseOutcome::Rejected);
        }
        let (fleet_generation, fleet_schema_version) = transaction.query_row(
            r#"
                SELECT generation, schema_version
                FROM runtime_networked_worker_fleet_meta
                WHERE singleton_key = 1
            "#,
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        if fleet_schema_version != 1
            || u64::try_from(fleet_generation).ok() != claim.dispatch_fleet_generation
        {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadReleaseOutcome::Rejected);
        }
        let record_json = transaction
            .query_row(
                r#"
                    SELECT record_json
                    FROM runtime_networked_worker_fleet
                    WHERE worker_id = ?1 AND schema_version = 1
                "#,
                params![claim.worker_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let lease_matches = record_json
            .as_deref()
            .map(serde_json::from_str::<palyra_workerd::WorkerFleetRecord>)
            .transpose()?
            .is_some_and(|record| {
                matches!(
                    record.state,
                    palyra_workerd::WorkerLifecycleState::Assigned
                        | palyra_workerd::WorkerLifecycleState::Busy
                        | palyra_workerd::WorkerLifecycleState::Degraded
                        | palyra_workerd::WorkerLifecycleState::Draining
                ) && record.lease.as_ref().is_some_and(|lease| {
                    lease.worker_id == claim.worker_id
                        && lease.lease_id == claim.lease_id
                        && lease.run_id == claim.run_id
                        && lease.expires_at_unix_ms == claim.lease_expires_at_unix_ms
                        && lease.expires_at_unix_ms > request.observed_at_unix_ms
                })
            });
        if !lease_matches {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadReleaseOutcome::Rejected);
        }
        let persisted_at_unix_ms = request.observed_at_unix_ms.max(claim.updated_at_unix_ms);
        let updated = transaction.execute(
            r#"
                UPDATE runtime_networked_worker_dispatch_claims
                SET payload_released_at_unix_ms = ?1,
                    payload_release_fleet_generation = ?2,
                    delivery_disposition = 'released_unacknowledged',
                    delivery_payload_present = 0,
                    updated_at_unix_ms = ?1
                WHERE node_request_ulid = ?3
                  AND delivery_attempt_ulid = ?4
                  AND delivery_token_sha256 = ?5
                  AND state = 'in_flight'
                  AND payload_released_at_unix_ms IS NULL
                  AND delivery_disposition = 'reserved_unreleased'
                  AND delivery_payload_present = 1
                  AND schema_version = 3
            "#,
            params![
                persisted_at_unix_ms,
                fleet_generation,
                request.node_request_id,
                request.delivery_attempt_id,
                delivery_token_sha256,
            ],
        )?;
        transaction.commit()?;
        Ok(if updated == 1 {
            NetworkedWorkerPayloadReleaseOutcome::Released
        } else {
            NetworkedWorkerPayloadReleaseOutcome::Rejected
        })
    }

    /// Records one exact payload acknowledgement idempotently.
    ///
    /// # Errors
    /// Returns [`JournalError`] when request metadata is malformed or SQLite cannot commit.
    pub fn acknowledge_networked_worker_payload(
        &self,
        request: &NetworkedWorkerPayloadAcknowledgementRequest,
    ) -> Result<NetworkedWorkerPayloadAcknowledgementOutcome, JournalError> {
        validate_runtime_identity(request.node_request_id.as_str(), "node request id")?;
        validate_runtime_identity(request.delivery_attempt_id.as_str(), "delivery attempt id")?;
        if request.delivery_token.len() < 32 || request.delivery_token.len() > 256 {
            return Err(JournalError::InvalidArgument(
                "networked worker delivery token length is invalid".to_owned(),
            ));
        }
        let delivery_token_sha256 = sha256_hex(request.delivery_token.as_bytes());
        validate_worker_id(request.reporting_worker_id.as_str())?;
        if request.observed_at_unix_ms < 0 {
            return Err(JournalError::InvalidArgument(
                "networked worker payload acknowledgement timestamp is invalid".to_owned(),
            ));
        }
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let claim = transaction
            .query_row(
                format!(
                    "{NETWORKED_WORKER_DISPATCH_CLAIM_SELECT} WHERE node_request_ulid = ?1 \
                     AND schema_version = 3"
                )
                .as_str(),
                params![request.node_request_id],
                hydrate_networked_worker_dispatch_claim,
            )
            .optional()?;
        let Some(claim) = claim else {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadAcknowledgementOutcome::Rejected);
        };
        validate_networked_worker_dispatch_claim_evidence(
            &claim,
            NetworkedWorkerDispatchClaimEvidenceLocation::Active,
        )?;
        let exact_binding = claim.worker_id == request.reporting_worker_id
            && claim.delivery_attempt_id.as_deref() == Some(request.delivery_attempt_id.as_str())
            && claim.delivery_token_sha256.as_deref() == Some(delivery_token_sha256.as_str());
        if !exact_binding || claim.payload_released_at_unix_ms.is_none() {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadAcknowledgementOutcome::Rejected);
        }
        if claim.payload_acknowledged_at_unix_ms.is_some() {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadAcknowledgementOutcome::AlreadyAcknowledged);
        }
        if !matches!(
            claim.state,
            NetworkedWorkerDispatchClaimState::InFlight
                | NetworkedWorkerDispatchClaimState::Reconciling
        ) || claim.lease_expires_at_unix_ms <= request.observed_at_unix_ms
        {
            transaction.commit()?;
            return Ok(NetworkedWorkerPayloadAcknowledgementOutcome::Rejected);
        }
        let persisted_at_unix_ms = request.observed_at_unix_ms.max(claim.updated_at_unix_ms);
        let updated = transaction.execute(
            r#"
                UPDATE runtime_networked_worker_dispatch_claims
                SET payload_acknowledged_at_unix_ms = ?1,
                    delivery_disposition = 'acknowledged',
                    updated_at_unix_ms = ?1
                WHERE node_request_ulid = ?2
                  AND delivery_attempt_ulid = ?3
                  AND delivery_token_sha256 = ?4
                  AND payload_released_at_unix_ms IS NOT NULL
                  AND payload_acknowledged_at_unix_ms IS NULL
                  AND state IN ('in_flight', 'reconciling')
                  AND schema_version = 3
            "#,
            params![
                persisted_at_unix_ms,
                request.node_request_id,
                request.delivery_attempt_id,
                delivery_token_sha256,
            ],
        )?;
        transaction.commit()?;
        Ok(if updated == 1 {
            NetworkedWorkerPayloadAcknowledgementOutcome::Acknowledged
        } else {
            NetworkedWorkerPayloadAcknowledgementOutcome::Rejected
        })
    }

    /// Cancels an exact in-flight claim before its raw payload leaves the daemon.
    ///
    /// The exact dispatch generation and request digest make this unsuitable as a general in-flight
    /// cancellation path. A mismatch preserves uncertainty for normal reconciliation.
    ///
    /// # Errors
    /// Returns [`JournalError`] when metadata or stored evidence is malformed, or SQLite cannot
    /// commit the exact transition.
    pub fn abort_networked_worker_dispatch_before_payload_release(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        request_sha256: &str,
        dispatch_fleet_generation: u64,
        observed_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerDispatchAbortBeforeReleaseOutcome, JournalError> {
        validate_networked_worker_dispatch_lookup(
            remote_request_id,
            node_request_id,
            request_sha256,
            observed_at_unix_ms,
        )?;
        let dispatch_fleet_generation = i64::try_from(dispatch_fleet_generation).map_err(|_| {
            JournalError::InvalidArgument(
                "networked worker dispatch generation exceeds sqlite integer range".to_owned(),
            )
        })?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(claim) = load_networked_worker_dispatch_claim_by_remote_request_tx(
            &transaction,
            remote_request_id,
        )?
        else {
            transaction.commit()?;
            return Ok(NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Missing);
        };
        if claim.node_request_id != node_request_id || claim.request_sha256 != request_sha256 {
            transaction.commit()?;
            return Ok(NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Missing);
        }
        if claim.state == NetworkedWorkerDispatchClaimState::Cancelled
            && claim.reconciliation_disposition.as_deref() == Some("payload_not_released")
            && claim.terminal_reason_code.as_deref()
                == Some("worker.dispatch.aborted_before_payload_release.local_audit_persist_failed")
            && claim.dispatch_fleet_generation == u64::try_from(dispatch_fleet_generation).ok()
        {
            transaction.commit()?;
            return Ok(NetworkedWorkerDispatchAbortBeforeReleaseOutcome::AlreadyAborted);
        }
        validate_networked_worker_dispatch_claim_evidence(
            &claim,
            NetworkedWorkerDispatchClaimEvidenceLocation::Active,
        )?;
        if claim.state != NetworkedWorkerDispatchClaimState::InFlight
            || claim.dispatch_fleet_generation != u64::try_from(dispatch_fleet_generation).ok()
            || claim.payload_released_at_unix_ms.is_some()
            || claim.delivery_disposition.as_deref() != Some("reserved_unreleased")
            || claim.delivery_payload_present != Some(true)
        {
            transaction.commit()?;
            return Ok(NetworkedWorkerDispatchAbortBeforeReleaseOutcome::NotAbortable);
        }
        let persisted_at_unix_ms = observed_at_unix_ms.max(claim.updated_at_unix_ms);
        let updated = transaction.execute(
            r#"
                UPDATE runtime_networked_worker_dispatch_claims
                SET state = 'cancelled',
                    reconciliation_disposition = 'payload_not_released',
                    terminal_reason_code =
                        'worker.dispatch.aborted_before_payload_release.local_audit_persist_failed',
                    delivery_disposition = NULL,
                    delivery_attempt_ulid = NULL,
                    delivery_token_sha256 = NULL,
                    delivery_reserved_at_unix_ms = NULL,
                    payload_released_at_unix_ms = NULL,
                    payload_release_fleet_generation = NULL,
                    payload_acknowledged_at_unix_ms = NULL,
                    delivery_payload_present = 0,
                    updated_at_unix_ms = ?1, completed_at_unix_ms = ?1
                WHERE remote_request_ulid = ?2
                  AND node_request_ulid = ?3
                  AND request_sha256 = ?4
                  AND dispatch_fleet_generation = ?5
                  AND state = 'in_flight'
                  AND payload_released_at_unix_ms IS NULL
                  AND delivery_disposition = 'reserved_unreleased'
                  AND delivery_payload_present = 1
                  AND schema_version = 3
            "#,
            params![
                persisted_at_unix_ms,
                remote_request_id,
                node_request_id,
                request_sha256,
                dispatch_fleet_generation,
            ],
        )?;
        if updated != 1 {
            transaction.commit()?;
            return Ok(NetworkedWorkerDispatchAbortBeforeReleaseOutcome::NotAbortable);
        }
        transaction.commit()?;
        Ok(NetworkedWorkerDispatchAbortBeforeReleaseOutcome::Aborted)
    }

    /// Cancels a queued dispatch claim without reviving or altering worker lease authority.
    ///
    /// # Errors
    /// Returns [`JournalError`] for malformed metadata or SQLite failures.
    pub fn cancel_networked_worker_dispatch_claim(
        &self,
        remote_request_id: &str,
        node_request_id: &str,
        reason_code: &str,
        observed_at_unix_ms: i64,
    ) -> Result<NetworkedWorkerDispatchCancelOutcome, JournalError> {
        validate_networked_worker_dispatch_reasoned_lookup(
            remote_request_id,
            node_request_id,
            reason_code,
            observed_at_unix_ms,
        )?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(claim) = load_networked_worker_dispatch_claim_by_remote_request_tx(
            &transaction,
            remote_request_id,
        )?
        else {
            let archived = load_archived_networked_worker_dispatch_claim_by_remote_request_tx(
                &transaction,
                remote_request_id,
            )?;
            let outcome = match archived {
                Some(claim) if claim.node_request_id == node_request_id => {
                    validate_networked_worker_dispatch_claim_evidence(
                        &claim,
                        NetworkedWorkerDispatchClaimEvidenceLocation::TerminalArchive,
                    )?;
                    if claim.state == NetworkedWorkerDispatchClaimState::Cancelled {
                        NetworkedWorkerDispatchCancelOutcome::AlreadyCancelled
                    } else {
                        NetworkedWorkerDispatchCancelOutcome::Missing
                    }
                }
                Some(_) | None => NetworkedWorkerDispatchCancelOutcome::Missing,
            };
            transaction.commit()?;
            return Ok(outcome);
        };
        validate_networked_worker_dispatch_claim_evidence(
            &claim,
            NetworkedWorkerDispatchClaimEvidenceLocation::Active,
        )?;
        if claim.node_request_id != node_request_id {
            transaction.commit()?;
            return Ok(NetworkedWorkerDispatchCancelOutcome::Missing);
        }
        let outcome = match claim.state {
            NetworkedWorkerDispatchClaimState::Queued => {
                let persisted_at_unix_ms = observed_at_unix_ms.max(claim.updated_at_unix_ms);
                transaction.execute(
                    r#"
                        UPDATE runtime_networked_worker_dispatch_claims
                        SET state = 'cancelled', terminal_reason_code = ?1,
                            delivery_payload_present = 0,
                            updated_at_unix_ms = ?2, completed_at_unix_ms = ?2
                        WHERE remote_request_ulid = ?3
                          AND node_request_ulid = ?4
                          AND state = 'queued'
                          AND schema_version = 3
                    "#,
                    params![reason_code, persisted_at_unix_ms, remote_request_id, node_request_id],
                )?;
                NetworkedWorkerDispatchCancelOutcome::Cancelled
            }
            NetworkedWorkerDispatchClaimState::Cancelled => {
                NetworkedWorkerDispatchCancelOutcome::AlreadyCancelled
            }
            NetworkedWorkerDispatchClaimState::InFlight
            | NetworkedWorkerDispatchClaimState::Reconciling => {
                NetworkedWorkerDispatchCancelOutcome::InFlight
            }
            NetworkedWorkerDispatchClaimState::Settled
            | NetworkedWorkerDispatchClaimState::FailedClosed => {
                NetworkedWorkerDispatchCancelOutcome::Missing
            }
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Settles one verified reconciling dispatch claim after fleet authority was revoked.
    ///
    /// # Errors
    /// Returns [`JournalError`] when settlement metadata is malformed, authority does not match,
    /// completion is not strictly before lease expiry, or SQLite cannot commit.
    pub fn settle_networked_worker_dispatch_claim(
        &self,
        settlement: &NetworkedWorkerDispatchSettlement,
    ) -> Result<(), JournalError> {
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        settle_networked_worker_dispatch_claim_tx(
            &transaction,
            settlement,
            NetworkedWorkerDispatchClaimState::Reconciling,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Loads one dispatch claim by its stable remote request identity.
    ///
    /// # Errors
    /// Returns [`JournalError`] when stored claim state is malformed or SQLite fails.
    pub fn networked_worker_dispatch_claim(
        &self,
        remote_request_id: &str,
    ) -> Result<Option<NetworkedWorkerDispatchClaim>, JournalError> {
        validate_runtime_identity(remote_request_id, "remote request id")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let active = guard
            .query_row(
                format!("{NETWORKED_WORKER_DISPATCH_CLAIM_SELECT} WHERE remote_request_ulid = ?1")
                    .as_str(),
                params![remote_request_id],
                hydrate_networked_worker_dispatch_claim,
            )
            .optional()?;
        if let Some(claim) = active {
            validate_networked_worker_dispatch_claim_evidence(
                &claim,
                NetworkedWorkerDispatchClaimEvidenceLocation::Active,
            )?;
            return Ok(Some(claim));
        }
        let archived = guard
            .query_row(
                format!(
                    "{NETWORKED_WORKER_DISPATCH_CLAIM_TERMINAL_SELECT} WHERE remote_request_ulid = ?1"
                )
                .as_str(),
                params![remote_request_id],
                hydrate_networked_worker_dispatch_claim,
            )
            .optional()?;
        if let Some(claim) = archived.as_ref() {
            validate_networked_worker_dispatch_claim_evidence(
                claim,
                NetworkedWorkerDispatchClaimEvidenceLocation::TerminalArchive,
            )?;
        }
        Ok(archived)
    }

    /// Cancels claims whose process-local payload was provably never released before restart.
    ///
    /// Released and legacy-unfenced claims are preserved for reconciliation because restart cannot
    /// prove whether their remote effect occurred. No raw payload is reconstructed or replayed.
    ///
    /// # Errors
    /// Returns [`JournalError`] when durable claim state is malformed, exceeds `max_entries`, or
    /// SQLite cannot commit the bounded reconciliation update.
    pub fn reconcile_networked_worker_dispatch_claims_after_restart(
        &self,
        max_entries: usize,
        reconciled_at_unix_ms: i64,
    ) -> Result<usize, JournalError> {
        validate_networked_worker_dispatch_claim_bound(max_entries)?;
        if reconciled_at_unix_ms < 0 {
            return Err(JournalError::InvalidArgument(
                "networked worker dispatch restart reconciliation timestamp is invalid".to_owned(),
            ));
        }
        let sqlite_limit = i64::try_from(max_entries.saturating_add(1)).map_err(|_| {
            JournalError::InvalidArgument(
                "networked worker dispatch restart reconciliation limit exceeds sqlite integer range"
                    .to_owned(),
            )
        })?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let claims = {
            let mut statement = transaction.prepare(
                format!(
                    "{NETWORKED_WORKER_DISPATCH_CLAIM_SELECT} ORDER BY remote_request_ulid ASC LIMIT ?1"
                )
                .as_str(),
            )?;
            let rows = statement.query_map(params![sqlite_limit], |row| {
                hydrate_networked_worker_dispatch_claim(row)
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        if claims.len() > max_entries {
            return Err(JournalError::NetworkedWorkerDispatchClaimCapacityExceeded {
                current_entries: claims.len(),
                max_entries,
            });
        }
        for claim in &claims {
            validate_networked_worker_dispatch_claim_evidence(
                claim,
                NetworkedWorkerDispatchClaimEvidenceLocation::Active,
            )?;
        }
        let unreleased_claims = claims
            .iter()
            .filter(|claim| {
                matches!(claim.state, NetworkedWorkerDispatchClaimState::Queued)
                    || (claim.state == NetworkedWorkerDispatchClaimState::InFlight
                        && claim.payload_released_at_unix_ms.is_none()
                        && claim.delivery_disposition.as_deref() == Some("reserved_unreleased"))
            })
            .collect::<Vec<_>>();
        for claim in &unreleased_claims {
            let persisted_at_unix_ms = reconciled_at_unix_ms.max(claim.updated_at_unix_ms);
            let updated = transaction.execute(
                r#"
                    UPDATE runtime_networked_worker_dispatch_claims
                    SET state = 'cancelled',
                        reconciliation_disposition = 'payload_lost_on_restart',
                        terminal_reason_code = 'worker.dispatch.cancelled_after_restart',
                        delivery_payload_present = 0,
                        updated_at_unix_ms = ?1,
                        completed_at_unix_ms = ?1
                    WHERE remote_request_ulid = ?2
                      AND state IN ('queued', 'in_flight')
                      AND payload_released_at_unix_ms IS NULL
                      AND schema_version = 3
                "#,
                params![persisted_at_unix_ms, claim.remote_request_id],
            )?;
            if updated != 1 {
                return Err(JournalError::NetworkedWorkerDispatchAuthorityRejected {
                    remote_request_id: claim.remote_request_id.clone(),
                });
            }
        }
        transaction.commit()?;
        Ok(unreleased_claims.len())
    }

    /// Loads the complete bounded durable networked-worker fleet and its write generation.
    ///
    /// Generation metadata and worker rows are read under one SQLite transaction so callers never
    /// pair authority from different durable snapshots.
    ///
    /// # Errors
    /// Returns [`JournalError`] when storage exceeds `max_entries` or contains unsupported,
    /// malformed, or identity-conflicting records or generation metadata.
    pub fn load_networked_worker_fleet_snapshot(
        &self,
        max_entries: usize,
    ) -> Result<NetworkedWorkerFleetSnapshot, JournalError> {
        if max_entries == 0 || max_entries > 1_000 {
            return Err(JournalError::InvalidArgument(
                "networked worker fleet snapshot bounds are invalid".to_owned(),
            ));
        }
        let sqlite_limit = i64::try_from(max_entries.saturating_add(1)).map_err(|_| {
            JournalError::InvalidArgument(
                "networked worker fleet snapshot limit exceeds sqlite integer range".to_owned(),
            )
        })?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let (generation, meta_schema_version) = transaction.query_row(
            r#"
                SELECT generation, schema_version
                FROM runtime_networked_worker_fleet_meta
                WHERE singleton_key = 1
            "#,
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        if meta_schema_version != 1 {
            return Err(JournalError::InvalidArgument(
                "networked worker fleet generation schema version is unsupported".to_owned(),
            ));
        }
        let generation = u64::try_from(generation).map_err(|_| {
            JournalError::InvalidArgument("networked worker fleet generation is invalid".to_owned())
        })?;
        let entry_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM runtime_networked_worker_fleet",
            [],
            |row| row.get(0),
        )?;
        let current_entries = usize::try_from(entry_count).unwrap_or(usize::MAX);
        if current_entries > max_entries {
            return Err(JournalError::NetworkedWorkerFleetCapacityExceeded {
                current_entries,
                max_entries,
            });
        }
        let mut statement = transaction.prepare(
            r#"
                SELECT worker_id, record_json, schema_version
                FROM runtime_networked_worker_fleet
                ORDER BY worker_id ASC
                LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map(params![sqlite_limit], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?))
        })?;
        let mut records = std::collections::BTreeMap::new();
        for row in rows {
            let (worker_id, record_json, schema_version) = row?;
            if records.len() == max_entries {
                return Err(JournalError::NetworkedWorkerFleetCapacityExceeded {
                    current_entries: max_entries.saturating_add(1),
                    max_entries,
                });
            }
            if schema_version != 1 {
                return Err(JournalError::InvalidArgument(
                    "networked worker fleet schema version is unsupported".to_owned(),
                ));
            }
            let record: palyra_workerd::WorkerFleetRecord =
                serde_json::from_str(record_json.as_str())?;
            if records.insert(worker_id, record).is_some() {
                return Err(JournalError::InvalidArgument(
                    "networked worker fleet contains duplicate worker identity".to_owned(),
                ));
            }
        }
        drop(statement);
        transaction.commit()?;
        palyra_workerd::WorkerFleetManager::from_durable_records(records.clone())
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        Ok(NetworkedWorkerFleetSnapshot { generation, records })
    }

    /// Loads only the records from the complete bounded durable worker-fleet snapshot.
    ///
    /// # Errors
    /// Returns the same errors as [`Self::load_networked_worker_fleet_snapshot`].
    pub fn list_networked_worker_fleet_records(
        &self,
        max_entries: usize,
    ) -> Result<std::collections::BTreeMap<String, palyra_workerd::WorkerFleetRecord>, JournalError>
    {
        self.load_networked_worker_fleet_snapshot(max_entries).map(|snapshot| snapshot.records)
    }

    /// Atomically inserts exact networked-worker expiry evidence before leases are revoked.
    ///
    /// # Errors
    /// Returns [`JournalError`] for malformed evidence, capacity exhaustion, conflicts, or storage
    /// failures. Re-inserting the same exact records is idempotent, and a failed batch inserts no
    /// rows.
    #[cfg(test)]
    pub fn enqueue_networked_worker_expiry_outbox_batch(
        &self,
        records: &[NetworkedWorkerExpiryOutboxRecord],
        max_entries: usize,
    ) -> Result<(), JournalError> {
        for record in records {
            record.validate()?;
        }
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        enqueue_networked_worker_expiry_outbox_batch_tx(&transaction, records, max_entries)?;
        transaction.commit()?;
        Ok(())
    }

    /// Loads a bounded ordered page of pending networked-worker expiry evidence.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the limit or stored exact evidence is invalid.
    pub fn list_networked_worker_expiry_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<NetworkedWorkerExpiryOutboxRecord>, JournalError> {
        if limit == 0 || limit > 1_000 {
            return Err(JournalError::InvalidArgument(
                "networked worker expiry outbox limit must be between 1 and 1000".to_owned(),
            ));
        }
        let sqlite_limit = i64::try_from(limit.saturating_add(1)).map_err(|_| {
            JournalError::InvalidArgument(
                "networked worker expiry outbox limit exceeds sqlite integer range".to_owned(),
            )
        })?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT event_ulid, worker_id, run_ulid, lease_ulid, event_json,
                       created_at_unix_ms, schema_version
                FROM runtime_networked_worker_expiry_outbox
                ORDER BY created_at_unix_ms ASC, event_ulid ASC
                LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map(params![sqlite_limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;
        let mut records = Vec::new();
        for row in rows {
            let (
                event_id,
                worker_id,
                run_id,
                lease_id,
                event_json,
                created_at_unix_ms,
                schema_version,
            ) = row?;
            if records.len() == limit {
                return Err(JournalError::NetworkedWorkerExpiryOutboxCapacityExceeded {
                    current_entries: limit.saturating_add(1),
                    max_entries: limit,
                });
            }
            if schema_version != 1 {
                return Err(JournalError::InvalidArgument(
                    "networked worker expiry outbox schema version is unsupported".to_owned(),
                ));
            }
            let event: palyra_workerd::WorkerLifecycleEvent =
                serde_json::from_str(event_json.as_str())?;
            if event.worker_id != worker_id
                || event.run_id.as_deref() != Some(run_id.as_str())
                || event.lease_id.as_deref() != Some(lease_id.as_str())
                || event.timestamp_unix_ms != created_at_unix_ms
            {
                return Err(JournalError::InvalidArgument(
                    "networked worker expiry outbox columns do not match exact event evidence"
                        .to_owned(),
                ));
            }
            let record = NetworkedWorkerExpiryOutboxRecord { event_id, event };
            record.validate()?;
            records.push(record);
        }
        Ok(records)
    }

    /// Removes exact expiry outbox evidence after the matching journal row exists.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the stored row conflicts or storage deletion fails.
    pub fn remove_networked_worker_expiry_outbox(
        &self,
        event_id: &str,
        event: &palyra_workerd::WorkerLifecycleEvent,
    ) -> Result<(), JournalError> {
        let event_json = serde_json::to_string(event)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let removed = guard.execute(
            r#"
                DELETE FROM runtime_networked_worker_expiry_outbox
                WHERE event_ulid = ?1
                  AND worker_id = ?2
                  AND run_ulid = ?3
                  AND lease_ulid = ?4
                  AND event_json = ?5
                  AND created_at_unix_ms = ?6
                  AND schema_version = 1
            "#,
            params![
                event_id,
                event.worker_id,
                event.run_id.as_deref(),
                event.lease_id.as_deref(),
                event_json,
                event.timestamp_unix_ms,
            ],
        )?;
        if removed == 1 {
            return Ok(());
        }
        let exists = guard
            .query_row(
                "SELECT 1 FROM runtime_networked_worker_expiry_outbox WHERE event_ulid = ?1",
                params![event_id],
                |_| Ok(()),
            )
            .optional()?;
        if exists.is_none() {
            Ok(())
        } else {
            Err(JournalError::InvalidArgument(
                "networked worker expiry outbox removal evidence does not match".to_owned(),
            ))
        }
    }

    /// Appends one validated cleanup report and its ordered steps idempotently.
    #[cfg(test)]
    pub fn append_cleanup_report(&self, report: &CleanupReportV1) -> Result<(), JournalError> {
        report.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        let raw = serde_json::to_vec(report)?;
        let (json, _) = sanitize_payload(raw.as_slice())?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        transaction.execute(
            r#"
                INSERT OR IGNORE INTO runtime_cleanup_reports (
                    report_ulid, instance_ulid, lease_ulid, outcome, reason_code,
                    report_json, created_at_unix_ms, schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                report.report_id,
                report.instance_id.as_str(),
                report.lease_id.as_ref().map(|value| value.as_str()),
                report.outcome.as_str(),
                report.reason_code,
                json,
                report.completed_at_unix_ms,
                i64::from(report.schema_version),
            ],
        )?;
        for step in &report.steps {
            transaction.execute(
                r#"
                    INSERT OR IGNORE INTO runtime_cleanup_steps (
                        report_ulid, ordinal, step, disposition, reason_code,
                        evidence_sha256, created_at_unix_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    report.report_id,
                    i64::from(step.ordinal),
                    step.step.as_str(),
                    step.disposition.as_str(),
                    step.reason_code,
                    step.evidence_sha256,
                    step.completed_at_unix_ms,
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Loads and revalidates immutable cleanup evidence for an exact retry.
    pub(crate) fn cleanup_report_for_exact_replay(
        &self,
        report_id: &str,
    ) -> Result<Option<CleanupReportV1>, JournalError> {
        if report_id.trim().is_empty() {
            return Err(JournalError::InvalidArgument(
                "cleanup report replay id must not be empty".to_owned(),
            ));
        }
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let report_json = transaction
            .query_row(
                "SELECT report_json FROM runtime_cleanup_reports WHERE report_ulid = ?1",
                params![report_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(report_json) = report_json else {
            transaction.commit()?;
            return Ok(None);
        };
        let report: CleanupReportV1 = serde_json::from_str(report_json.as_str())?;
        report.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        if report.report_id != report_id {
            return Err(JournalError::InvalidArgument(
                "cleanup report replay id conflicts with its durable payload".to_owned(),
            ));
        }
        if !validate_cleanup_report_replay_tx(&transaction, &report, report_json.as_str())? {
            return Err(JournalError::InvalidArgument(
                "cleanup report disappeared during exact replay validation".to_owned(),
            ));
        }
        transaction.commit()?;
        Ok(Some(report))
    }

    /// Atomically records cleanup evidence, transitions the handle state, and retires its lease.
    pub fn finalize_process_cleanup(
        &self,
        descriptor: &RuntimeHandleDescriptorV1,
        report: &CleanupReportV1,
    ) -> Result<(), JournalError> {
        descriptor.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        report.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        if descriptor.instance_id != report.instance_id
            || descriptor.kind != RuntimeHandleKind::Process
            || !matches!(
                descriptor.state,
                RuntimeHandleState::Closed
                    | RuntimeHandleState::Orphaned
                    | RuntimeHandleState::Quarantined
            )
        {
            return Err(JournalError::InvalidArgument(
                "process cleanup finalization requires a matching terminal handle descriptor"
                    .to_owned(),
            ));
        }
        let descriptor_raw = serde_json::to_vec(descriptor)?;
        let (descriptor_json, _) = sanitize_payload(descriptor_raw.as_slice())?;
        let report_raw = serde_json::to_vec(report)?;
        let (report_json, _) = sanitize_payload(report_raw.as_slice())?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let report_replayed =
            validate_cleanup_report_replay_tx(&transaction, report, report_json.as_str())?;
        let stored_handle = transaction
            .query_row(
                r#"
                    SELECT
                        session_ulid, run_ulid, generation, kind, state,
                        descriptor_json, updated_at_unix_ms
                    FROM runtime_handles
                    WHERE instance_ulid = ?1
                "#,
                params![descriptor.instance_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            stored_session_id,
            stored_run_id,
            stored_generation,
            stored_kind,
            stored_state,
            stored_descriptor_json,
            stored_updated_at,
        )) = stored_handle
        else {
            return Err(JournalError::InvalidArgument(format!(
                "runtime handle not found for process cleanup: {}",
                descriptor.instance_id.as_str()
            )));
        };
        let ownership_matches = stored_session_id.as_deref()
            == descriptor.session_id.as_ref().map(|value| value.as_str())
            && stored_run_id.as_deref() == descriptor.run_id.as_ref().map(|value| value.as_str())
            && stored_generation == i64::try_from(descriptor.generation.get()).unwrap_or(i64::MAX)
            && stored_kind == RuntimeHandleKind::Process.as_str();
        if !ownership_matches {
            return Err(JournalError::InvalidArgument(
                "process cleanup descriptor does not match durable handle ownership".to_owned(),
            ));
        }
        if report_replayed {
            if stored_state != descriptor.state.as_str()
                || stored_descriptor_json != descriptor_json
                || stored_updated_at != descriptor.updated_at_unix_ms
            {
                return Err(JournalError::InvalidArgument(
                    "cleanup report replay conflicts with terminal handle evidence".to_owned(),
                ));
            }
        } else {
            let updated = transaction.execute(
                r#"
                    UPDATE runtime_handles SET
                        state = ?2,
                        descriptor_json = ?3,
                        updated_at_unix_ms = ?4
                    WHERE instance_ulid = ?1 AND generation = ?5
                "#,
                params![
                    descriptor.instance_id.as_str(),
                    descriptor.state.as_str(),
                    descriptor_json,
                    descriptor.updated_at_unix_ms,
                    stored_generation,
                ],
            )?;
            if updated != 1 {
                return Err(JournalError::InvalidArgument(
                    "process cleanup lost durable handle ownership".to_owned(),
                ));
            }
            insert_cleanup_report_tx(&transaction, report, report_json.as_str())?;
        }
        let absence_verified = report.steps.iter().any(|step| {
            step.step == palyra_common::runtime_contracts::CleanupStepKind::VerifyAbsence
                && step.disposition
                    == palyra_common::runtime_contracts::CleanupStepDisposition::Completed
        });
        if descriptor.state == RuntimeHandleState::Closed {
            if report.outcome != CleanupOutcome::Completed || !absence_verified {
                return Err(JournalError::InvalidArgument(
                    "closed process cleanup requires verified absence".to_owned(),
                ));
            }
            let lease_id = report.lease_id.as_ref().ok_or_else(|| {
                JournalError::InvalidArgument(
                    "closed process cleanup requires an exact process lease".to_owned(),
                )
            })?;
            let stored_lease = transaction
                .query_row(
                    "SELECT instance_ulid, generation FROM runtime_process_leases WHERE lease_ulid = ?1",
                    params![lease_id.as_str()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
                )
                .optional()?;
            match stored_lease {
                Some((instance_id, generation))
                    if instance_id == descriptor.instance_id.as_str()
                        && generation == stored_generation =>
                {
                    let retired = transaction.execute(
                        "DELETE FROM runtime_process_leases WHERE lease_ulid = ?1",
                        params![lease_id.as_str()],
                    )?;
                    if retired != 1 {
                        return Err(JournalError::InvalidArgument(
                            "process cleanup lost exact lease retirement authority".to_owned(),
                        ));
                    }
                }
                None if report_replayed => {}
                _ => {
                    return Err(JournalError::InvalidArgument(
                        "process cleanup lease did not match durable ownership".to_owned(),
                    ));
                }
            }
        }
        append_cleanup_runtime_event_tx(
            &transaction,
            self.config.max_payload_bytes,
            descriptor,
            report,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Scans shared runtime records and returns a fail-closed compatibility report.
    ///
    /// This inspection is read-only. Call
    /// [`Self::persist_runtime_state_quarantine_findings`] explicitly at a startup mutation
    /// boundary when the findings must become durable quarantine evidence.
    pub fn runtime_state_compatibility_report(
        &self,
    ) -> Result<RuntimeStateCompatibilityReport, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        let mut detected = RuntimeCompatibilityFindingCollector::default();
        scan_json_table(
            &transaction,
            "runtime_events_v2",
            "event_ulid",
            "envelope_json",
            2,
            RuntimeJsonContract::EventEnvelope,
            &mut detected,
        )?;
        scan_json_table(
            &transaction,
            "runtime_side_effect_fences",
            "operation_ulid",
            "fence_json",
            1,
            RuntimeJsonContract::SideEffectFence,
            &mut detected,
        )?;
        scan_background_task_cancellation_contracts(&transaction, &mut detected)?;
        scan_json_table(
            &transaction,
            "runtime_component_health",
            "component_ulid",
            "health_json",
            1,
            RuntimeJsonContract::ComponentHealth,
            &mut detected,
        )?;
        scan_runtime_component_health_exact_evidence(&transaction, &mut detected)?;
        scan_json_table(
            &transaction,
            "runtime_handles",
            "instance_ulid",
            "descriptor_json",
            1,
            RuntimeJsonContract::HandleDescriptor,
            &mut detected,
        )?;
        scan_json_table(
            &transaction,
            "runtime_cleanup_reports",
            "report_ulid",
            "report_json",
            1,
            RuntimeJsonContract::CleanupReport,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_cleanup_reports",
            "report_ulid",
            "schema_version",
            RUNTIME_CLEANUP_REPORT_ROW_SCHEMA_VERSION,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_state_quarantine",
            "quarantine_ulid",
            "schema_version",
            RUNTIME_STATE_QUARANTINE_ROW_SCHEMA_VERSION,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_generation_leases",
            "lease_ulid",
            "schema_version",
            1,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_generation_events",
            "event_ulid",
            "schema_version",
            1,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_stale_event_diagnostics",
            "diagnostic_ulid",
            "schema_version",
            1,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_provider_configuration_head",
            "singleton_key",
            "schema_version",
            1,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_provider_configuration_events",
            "event_ulid",
            "schema_version",
            1,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_side_effect_fence_events",
            "event_ulid",
            "schema_version",
            1,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_component_health_events",
            "event_ulid",
            "schema_version",
            1,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_process_reconciliation_checkpoint",
            "checkpoint_key",
            "schema_version",
            1,
            &mut detected,
        )?;
        scan_versioned_table(
            &transaction,
            "runtime_process_leases",
            "lease_ulid",
            "schema_version",
            1,
            &mut detected,
        )?;
        scan_json_table(
            &transaction,
            "runtime_process_leases",
            "lease_ulid",
            "provenance_json",
            1,
            RuntimeJsonContract::ProcessProvenance,
            &mut detected,
        )?;
        scan_cleanup_steps(&transaction, &mut detected)?;
        scan_versioned_table(
            &transaction,
            "runtime_component_generation_heads",
            "component_ulid",
            "schema_version",
            RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION,
            &mut detected,
        )?;
        scan_runtime_component_generation_heads(&transaction, &mut detected)?;
        scan_versioned_table(
            &transaction,
            "runtime_health_probe_leases",
            "lease_ulid",
            "schema_version",
            RUNTIME_HEALTH_PROBE_ACTIVE_ROW_SCHEMA_VERSION,
            &mut detected,
        )?;
        scan_json_table(
            &transaction,
            "runtime_health_probe_leases",
            "lease_ulid",
            "lease_json",
            1,
            RuntimeJsonContract::HealthProbeLease,
            &mut detected,
        )?;
        scan_runtime_health_probe_active_exact_evidence(&transaction, &mut detected)?;
        scan_versioned_table(
            &transaction,
            "runtime_health_probe_begins",
            "lease_ulid",
            "schema_version",
            RUNTIME_HEALTH_PROBE_BEGIN_ROW_SCHEMA_VERSION,
            &mut detected,
        )?;
        scan_json_table(
            &transaction,
            "runtime_health_probe_begins",
            "lease_ulid",
            "lease_json",
            1,
            RuntimeJsonContract::HealthProbeLease,
            &mut detected,
        )?;
        scan_runtime_health_probe_begin_exact_evidence(&transaction, &mut detected)?;
        scan_versioned_table(
            &transaction,
            "runtime_health_probe_terminal_evidence",
            "lease_ulid",
            "schema_version",
            RUNTIME_HEALTH_PROBE_TERMINAL_ROW_SCHEMA_VERSION,
            &mut detected,
        )?;
        scan_runtime_health_probe_terminal_exact_evidence(&transaction, &mut detected)?;
        let worker_outbox_within_bounds = scan_bounded_runtime_table(
            &transaction,
            "runtime_networked_worker_expiry_outbox",
            NETWORKED_WORKER_EXPIRY_MAX_ENTRIES,
            &mut detected,
        )?;
        if worker_outbox_within_bounds {
            scan_versioned_table(
                &transaction,
                "runtime_networked_worker_expiry_outbox",
                "event_ulid",
                "schema_version",
                1,
                &mut detected,
            )?;
            scan_json_table(
                &transaction,
                "runtime_networked_worker_expiry_outbox",
                "event_ulid",
                "event_json",
                1,
                RuntimeJsonContract::WorkerLifecycleEvent,
                &mut detected,
            )?;
            scan_networked_worker_expiry_outbox_exact_evidence(&transaction, &mut detected)?;
        }
        let worker_fleet_within_bounds = scan_bounded_runtime_table(
            &transaction,
            "runtime_networked_worker_fleet",
            NETWORKED_WORKER_FLEET_MAX_ENTRIES,
            &mut detected,
        )?;
        if worker_fleet_within_bounds {
            scan_versioned_table(
                &transaction,
                "runtime_networked_worker_fleet",
                "worker_id",
                "schema_version",
                1,
                &mut detected,
            )?;
            scan_networked_worker_fleet_exact_evidence(&transaction, &mut detected)?;
        }
        scan_networked_worker_fleet_generation(&transaction, &mut detected)?;
        let worker_dispatch_claims_within_bounds = scan_bounded_runtime_table(
            &transaction,
            "runtime_networked_worker_dispatch_claims",
            NETWORKED_WORKER_DISPATCH_CLAIM_MAX_ENTRIES,
            &mut detected,
        )?;
        if worker_dispatch_claims_within_bounds {
            scan_versioned_table(
                &transaction,
                "runtime_networked_worker_dispatch_claims",
                "remote_request_ulid",
                "schema_version",
                NETWORKED_WORKER_DISPATCH_CLAIM_SCHEMA_VERSION,
                &mut detected,
            )?;
            scan_networked_worker_dispatch_claim_exact_evidence(
                &transaction,
                "runtime_networked_worker_dispatch_claims",
                NETWORKED_WORKER_DISPATCH_CLAIM_SELECT,
                false,
                &mut detected,
            )?;
        }
        let worker_dispatch_terminal_evidence_within_bounds = scan_bounded_runtime_table(
            &transaction,
            "runtime_networked_worker_dispatch_claim_terminal_evidence",
            NETWORKED_WORKER_DISPATCH_TERMINAL_EVIDENCE_MAX_ENTRIES,
            &mut detected,
        )?;
        if worker_dispatch_terminal_evidence_within_bounds {
            scan_versioned_table(
                &transaction,
                "runtime_networked_worker_dispatch_claim_terminal_evidence",
                "remote_request_ulid",
                "schema_version",
                NETWORKED_WORKER_DISPATCH_CLAIM_SCHEMA_VERSION,
                &mut detected,
            )?;
            scan_networked_worker_dispatch_claim_exact_evidence(
                &transaction,
                "runtime_networked_worker_dispatch_claim_terminal_evidence",
                NETWORKED_WORKER_DISPATCH_CLAIM_TERMINAL_SELECT,
                true,
                &mut detected,
            )?;
        }
        scan_networked_worker_dispatch_claim_cross_table_conflicts(&transaction, &mut detected)?;
        let report = RuntimeStateCompatibilityReport::from_findings(detected.into_findings(), now)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        Ok(report)
    }

    /// Persists a previously collected compatibility report as append-only quarantine evidence.
    ///
    /// Collection remains separate so diagnostics and offline inspection never write merely by
    /// observing compatibility state. Repeated persistence is idempotent for the same contract,
    /// record hash, and outcome.
    ///
    /// # Errors
    /// Returns [`JournalError`] if the report is not canonical or SQLite cannot commit the
    /// quarantine evidence.
    pub(crate) fn persist_runtime_state_quarantine_findings(
        &self,
        report: &RuntimeStateCompatibilityReport,
    ) -> Result<(), JournalError> {
        let canonical = RuntimeStateCompatibilityReport::from_findings(
            report.findings.clone(),
            report.generated_at_unix_ms,
        )
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
        if canonical != *report {
            return Err(JournalError::InvalidArgument(
                "runtime compatibility report is not canonical".to_owned(),
            ));
        }
        if canonical.findings.is_empty() {
            return Ok(());
        }

        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction()?;
        for finding in &canonical.findings {
            persist_quarantine_finding(&transaction, finding, canonical.schema_version, now)?;
        }
        transaction.commit()?;
        Ok(())
    }
}

fn validate_networked_worker_dispatch_claim_bound(max_entries: usize) -> Result<(), JournalError> {
    if max_entries == 0 || max_entries > 10_000 {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch claim bounds are invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_networked_worker_dispatch_claim_create_request(
    request: &NetworkedWorkerDispatchClaimCreateRequest,
    created_at_unix_ms: i64,
) -> Result<(), JournalError> {
    validate_runtime_identity(request.remote_request_id.as_str(), "remote request id")?;
    validate_runtime_identity(request.node_request_id.as_str(), "node request id")?;
    validate_worker_id(request.worker_id.as_str())?;
    validate_runtime_identity(request.lease_id.as_str(), "lease id")?;
    validate_runtime_identity(request.session_id.as_str(), "session id")?;
    validate_runtime_identity(request.run_id.as_str(), "run id")?;
    validate_capability(request.capability.as_str())?;
    validate_sha256(request.request_sha256.as_str())?;
    if created_at_unix_ms < 0 || request.lease_expires_at_unix_ms <= created_at_unix_ms {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch claim timestamps are invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_networked_worker_dispatch_lookup(
    remote_request_id: &str,
    node_request_id: &str,
    request_sha256: &str,
    observed_at_unix_ms: i64,
) -> Result<(), JournalError> {
    validate_runtime_identity(remote_request_id, "remote request id")?;
    validate_runtime_identity(node_request_id, "node request id")?;
    validate_sha256(request_sha256)?;
    if observed_at_unix_ms < 0 {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch observation timestamp is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_networked_worker_dispatch_reasoned_lookup(
    remote_request_id: &str,
    node_request_id: &str,
    reason_code: &str,
    observed_at_unix_ms: i64,
) -> Result<(), JournalError> {
    validate_runtime_identity(remote_request_id, "remote request id")?;
    validate_runtime_identity(node_request_id, "node request id")?;
    validate_reason_code(reason_code)?;
    if observed_at_unix_ms < 0 {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch observation timestamp is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_runtime_identity(value: &str, field: &'static str) -> Result<(), JournalError> {
    palyra_common::runtime_contracts::RuntimeOperationId::parse(value)
        .map(|_| ())
        .map_err(|error| JournalError::InvalidArgument(format!("{field} is invalid: {error}")))
}

fn validate_worker_id(value: &str) -> Result<(), JournalError> {
    if !value.is_empty()
        && value == value.trim()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Ok(());
    }
    Err(JournalError::InvalidArgument("networked worker dispatch worker id is invalid".to_owned()))
}

fn validate_capability(value: &str) -> Result<(), JournalError> {
    if !value.is_empty()
        && value == value.trim()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Ok(());
    }
    Err(JournalError::InvalidArgument("networked worker dispatch capability is invalid".to_owned()))
}

fn validate_sha256(value: &str) -> Result<(), JournalError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(());
    }
    Err(JournalError::InvalidArgument(
        "networked worker dispatch request digest is invalid".to_owned(),
    ))
}

fn validate_reason_code(value: &str) -> Result<(), JournalError> {
    if !value.is_empty()
        && value == value.trim()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Ok(());
    }
    Err(JournalError::InvalidArgument(
        "networked worker dispatch reason code is invalid".to_owned(),
    ))
}

fn networked_worker_dispatch_claim_matches_create(
    claim: &NetworkedWorkerDispatchClaim,
    request: &NetworkedWorkerDispatchClaimCreateRequest,
) -> bool {
    claim.node_request_id == request.node_request_id
        && claim.worker_id == request.worker_id
        && claim.lease_id == request.lease_id
        && claim.session_id.as_deref() == Some(request.session_id.as_str())
        && claim.run_id == request.run_id
        && claim.run_generation == Some(request.run_generation)
        && claim.lease_expires_at_unix_ms == request.lease_expires_at_unix_ms
        && claim.capability == request.capability
        && claim.request_sha256 == request.request_sha256
}

fn archived_networked_worker_dispatch_claim_matches_create(
    claim: &NetworkedWorkerDispatchClaim,
    request: &NetworkedWorkerDispatchClaimCreateRequest,
) -> bool {
    networked_worker_dispatch_claim_matches_create(claim, request)
        && matches!(
            claim.state,
            NetworkedWorkerDispatchClaimState::Settled
                | NetworkedWorkerDispatchClaimState::Cancelled
                | NetworkedWorkerDispatchClaimState::FailedClosed
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NetworkedWorkerDispatchClaimEvidenceLocation {
    Active,
    TerminalArchive,
}

fn validate_networked_worker_dispatch_claim_evidence(
    claim: &NetworkedWorkerDispatchClaim,
    location: NetworkedWorkerDispatchClaimEvidenceLocation,
) -> Result<(), JournalError> {
    if !matches!(claim.schema_version, 1..=3) {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch claim schema version is unsupported".to_owned(),
        ));
    }
    let has_session_authority = claim.session_id.is_some();
    let has_generation_authority = claim.run_generation.is_some();
    if has_session_authority != has_generation_authority {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch claim has partial run generation authority".to_owned(),
        ));
    }
    let legacy_missing_run_generation = claim.state
        == NetworkedWorkerDispatchClaimState::Reconciling
        && claim.reconciliation_disposition.as_deref() == Some("legacy_missing_run_generation")
        && !has_session_authority;
    // Migration 71 leaves already-reconciling v70 audit rows unchanged; their missing authority
    // keeps callbacks fail-closed without erasing the original disposition or reason.
    let legacy_reconciling_without_run_generation = claim.schema_version == 3
        && claim.state == NetworkedWorkerDispatchClaimState::Reconciling
        && !has_session_authority
        && matches!(
            claim.reconciliation_disposition.as_deref(),
            Some("legacy_missing_run_generation" | "lease_revoked" | "legacy_unfenced_unknown")
        );
    if claim.reconciliation_disposition.as_deref() == Some("legacy_missing_run_generation")
        && !legacy_missing_run_generation
    {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch claim has invalid legacy run generation posture".to_owned(),
        ));
    }
    if location == NetworkedWorkerDispatchClaimEvidenceLocation::Active
        && !has_session_authority
        && !legacy_reconciling_without_run_generation
    {
        return Err(JournalError::InvalidArgument(
            "active networked worker dispatch claim is missing run generation authority".to_owned(),
        ));
    }
    validate_runtime_identity(claim.remote_request_id.as_str(), "remote request id")?;
    validate_runtime_identity(claim.node_request_id.as_str(), "node request id")?;
    validate_worker_id(claim.worker_id.as_str())?;
    validate_runtime_identity(claim.lease_id.as_str(), "lease id")?;
    if let Some(session_id) = claim.session_id.as_deref() {
        validate_runtime_identity(session_id, "session id")?;
    }
    validate_runtime_identity(claim.run_id.as_str(), "run id")?;
    validate_capability(claim.capability.as_str())?;
    validate_sha256(claim.request_sha256.as_str())?;
    if claim.created_at_unix_ms < 0
        || claim.updated_at_unix_ms < claim.created_at_unix_ms
        || claim.lease_expires_at_unix_ms <= claim.created_at_unix_ms
        || claim.completed_at_unix_ms.is_some_and(|completed| {
            completed < claim.created_at_unix_ms || completed != claim.updated_at_unix_ms
        })
    {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch claim timestamp evidence is invalid".to_owned(),
        ));
    }
    let has_validated_result_evidence =
        claim.validated_result_sha256.is_some() || claim.result_observed_at_unix_ms.is_some();
    if claim.schema_version == 1 {
        let has_v2_evidence = claim.delivery_attempt_id.is_some()
            || claim.delivery_token_sha256.is_some()
            || claim.delivery_reserved_at_unix_ms.is_some()
            || claim.payload_released_at_unix_ms.is_some()
            || claim.payload_release_fleet_generation.is_some()
            || claim.payload_acknowledged_at_unix_ms.is_some()
            || claim.delivery_disposition.is_some()
            || claim.delivery_payload_present.is_some();
        if has_v2_evidence || has_validated_result_evidence {
            return Err(JournalError::InvalidArgument(
                "legacy networked worker dispatch claim contains newer evidence".to_owned(),
            ));
        }
    } else {
        validate_networked_worker_delivery_evidence(claim, legacy_missing_run_generation)?;
        if claim.schema_version == 2 && has_validated_result_evidence {
            return Err(JournalError::InvalidArgument(
                "schema 2 networked worker dispatch claim contains validated-result evidence"
                    .to_owned(),
            ));
        }
    }
    if claim.schema_version == 3 {
        validate_networked_worker_validated_result_evidence(claim)?;
    }
    if location == NetworkedWorkerDispatchClaimEvidenceLocation::TerminalArchive
        && !matches!(
            claim.state,
            NetworkedWorkerDispatchClaimState::Settled
                | NetworkedWorkerDispatchClaimState::Cancelled
                | NetworkedWorkerDispatchClaimState::FailedClosed
        )
    {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch archive contains nonterminal evidence".to_owned(),
        ));
    }
    let valid = match claim.state {
        NetworkedWorkerDispatchClaimState::Queued => {
            location == NetworkedWorkerDispatchClaimEvidenceLocation::Active
                && claim.dispatch_fleet_generation.is_none()
                && claim.revoked_fleet_generation.is_none()
                && claim.reconciliation_disposition.is_none()
                && claim.terminal_reason_code.is_none()
                && claim.completed_at_unix_ms.is_none()
        }
        NetworkedWorkerDispatchClaimState::InFlight => {
            location == NetworkedWorkerDispatchClaimEvidenceLocation::Active
                && claim
                    .dispatch_fleet_generation
                    .is_some_and(|dispatch| claim.issued_fleet_generation <= dispatch)
                && claim.revoked_fleet_generation.is_none()
                && claim.reconciliation_disposition.is_none()
                && claim.terminal_reason_code.is_none()
                && claim.completed_at_unix_ms.is_none()
        }
        NetworkedWorkerDispatchClaimState::Reconciling => {
            location == NetworkedWorkerDispatchClaimEvidenceLocation::Active
                && match claim.reconciliation_disposition.as_deref() {
                    Some("lease_revoked") => {
                        claim.dispatch_fleet_generation.is_some_and(|dispatch| {
                            claim.revoked_fleet_generation.is_some_and(|revoked| {
                                claim.issued_fleet_generation <= dispatch && dispatch < revoked
                            })
                        })
                    }
                    Some("legacy_unfenced_unknown") => {
                        claim.delivery_disposition.as_deref() == Some("legacy_unfenced_unknown")
                    }
                    Some("legacy_missing_run_generation") => true,
                    _ => false,
                }
                && claim
                    .terminal_reason_code
                    .as_deref()
                    .is_some_and(|reason| validate_reason_code(reason).is_ok())
                && claim.completed_at_unix_ms.is_none()
        }
        NetworkedWorkerDispatchClaimState::Settled => {
            claim.dispatch_fleet_generation.is_some_and(|dispatch| {
                claim.issued_fleet_generation <= dispatch
                    && match claim.reconciliation_disposition.as_deref() {
                        None => claim.revoked_fleet_generation.is_none(),
                        Some("late_result_verified") => {
                            claim.revoked_fleet_generation.is_some_and(|revoked| dispatch < revoked)
                        }
                        Some(_) => false,
                    }
            }) && claim.terminal_reason_code.as_deref() == Some("worker.dispatch.settled")
                && claim.completed_at_unix_ms.is_some()
                && (claim.schema_version < 3 || claim.validated_result_sha256.is_some())
        }
        NetworkedWorkerDispatchClaimState::Cancelled => {
            claim.completed_at_unix_ms.is_some()
                && match claim.reconciliation_disposition.as_deref() {
                    None => {
                        claim.dispatch_fleet_generation.is_none()
                            && claim.revoked_fleet_generation.is_none()
                            && claim
                                .terminal_reason_code
                                .as_deref()
                                .is_some_and(|reason| validate_reason_code(reason).is_ok())
                    }
                    Some("payload_lost_on_restart") => {
                        claim
                            .dispatch_fleet_generation
                            .is_none_or(|dispatch| claim.issued_fleet_generation <= dispatch)
                            && claim.revoked_fleet_generation.is_none()
                            && claim.terminal_reason_code.as_deref()
                                == Some("worker.dispatch.cancelled_after_restart")
                    }
                    Some("payload_not_released") => {
                        claim
                            .dispatch_fleet_generation
                            .is_some_and(|dispatch| claim.issued_fleet_generation <= dispatch)
                            && claim
                                .terminal_reason_code
                                .as_deref()
                                .is_some_and(|reason| validate_reason_code(reason).is_ok())
                    }
                    Some(_) => false,
                }
        }
        NetworkedWorkerDispatchClaimState::FailedClosed => false,
    };
    if !valid {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch claim state evidence is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_networked_worker_validated_result_evidence(
    claim: &NetworkedWorkerDispatchClaim,
) -> Result<(), JournalError> {
    let receipt = match (claim.validated_result_sha256.as_deref(), claim.result_observed_at_unix_ms)
    {
        (None, None) => None,
        (Some(digest), Some(observed_at_unix_ms)) => {
            validate_sha256(digest)?;
            if digest.bytes().any(|byte| byte.is_ascii_uppercase())
                || observed_at_unix_ms < 0
                || observed_at_unix_ms >= claim.lease_expires_at_unix_ms
                || observed_at_unix_ms > claim.updated_at_unix_ms
                || claim
                    .payload_released_at_unix_ms
                    .is_some_and(|released| observed_at_unix_ms < released)
            {
                return Err(JournalError::InvalidArgument(
                    "networked worker validated-result receipt evidence is invalid".to_owned(),
                ));
            }
            Some(())
        }
        _ => {
            return Err(JournalError::InvalidArgument(
                "networked worker validated-result receipt is incomplete".to_owned(),
            ));
        }
    };
    let valid = match claim.state {
        NetworkedWorkerDispatchClaimState::Settled => receipt.is_some(),
        NetworkedWorkerDispatchClaimState::Queued
        | NetworkedWorkerDispatchClaimState::InFlight
        | NetworkedWorkerDispatchClaimState::Reconciling
        | NetworkedWorkerDispatchClaimState::Cancelled
        | NetworkedWorkerDispatchClaimState::FailedClosed => receipt.is_none(),
    };
    if valid {
        Ok(())
    } else {
        Err(JournalError::InvalidArgument(
            "networked worker validated-result receipt does not match claim state".to_owned(),
        ))
    }
}

fn validate_networked_worker_delivery_evidence(
    claim: &NetworkedWorkerDispatchClaim,
    legacy_missing_run_generation: bool,
) -> Result<(), JournalError> {
    let valid = match claim.delivery_disposition.as_deref() {
        None => {
            claim.delivery_attempt_id.is_none()
                && claim.delivery_token_sha256.is_none()
                && claim.delivery_reserved_at_unix_ms.is_none()
                && claim.payload_released_at_unix_ms.is_none()
                && claim.payload_release_fleet_generation.is_none()
                && claim.payload_acknowledged_at_unix_ms.is_none()
                && match claim.state {
                    NetworkedWorkerDispatchClaimState::Queued => {
                        matches!(claim.delivery_payload_present, Some(true) | Some(false))
                    }
                    NetworkedWorkerDispatchClaimState::Cancelled => {
                        claim.delivery_payload_present.is_none()
                            || claim.delivery_payload_present == Some(false)
                    }
                    NetworkedWorkerDispatchClaimState::Settled
                    | NetworkedWorkerDispatchClaimState::FailedClosed => {
                        claim.delivery_payload_present.is_none()
                            || claim.delivery_payload_present == Some(false)
                    }
                    NetworkedWorkerDispatchClaimState::InFlight
                    | NetworkedWorkerDispatchClaimState::Reconciling => false,
                }
        }
        Some("reserved_unreleased") => {
            (matches!(
                claim.state,
                NetworkedWorkerDispatchClaimState::InFlight
                    | NetworkedWorkerDispatchClaimState::Cancelled
            ) || (claim.state == NetworkedWorkerDispatchClaimState::Reconciling
                && legacy_missing_run_generation))
                && claim
                    .delivery_attempt_id
                    .as_deref()
                    .is_some_and(|id| validate_runtime_identity(id, "delivery attempt id").is_ok())
                && claim
                    .delivery_token_sha256
                    .as_deref()
                    .is_some_and(|digest| validate_sha256(digest).is_ok())
                && claim.delivery_reserved_at_unix_ms.is_some_and(|reserved| {
                    reserved >= claim.created_at_unix_ms && reserved <= claim.updated_at_unix_ms
                })
                && claim.payload_released_at_unix_ms.is_none()
                && claim.payload_release_fleet_generation.is_none()
                && claim.payload_acknowledged_at_unix_ms.is_none()
                && match claim.state {
                    NetworkedWorkerDispatchClaimState::InFlight => {
                        claim.delivery_payload_present == Some(true)
                    }
                    NetworkedWorkerDispatchClaimState::Cancelled => {
                        match claim.reconciliation_disposition.as_deref() {
                            Some("payload_not_released") => {
                                claim.revoked_fleet_generation.is_some()
                                    && claim.delivery_payload_present == Some(false)
                            }
                            Some("payload_lost_on_restart") => {
                                claim.revoked_fleet_generation.is_none()
                                    && claim.delivery_payload_present == Some(false)
                            }
                            _ => false,
                        }
                    }
                    NetworkedWorkerDispatchClaimState::Reconciling => {
                        // Migration 71 preserves an unreleased reservation for audit only when the
                        // legacy row has no callback authority to consume it.
                        legacy_missing_run_generation
                            && claim.delivery_payload_present == Some(true)
                    }
                    NetworkedWorkerDispatchClaimState::Queued
                    | NetworkedWorkerDispatchClaimState::Settled
                    | NetworkedWorkerDispatchClaimState::FailedClosed => false,
                }
        }
        Some("released_unacknowledged") => {
            matches!(
                claim.state,
                NetworkedWorkerDispatchClaimState::InFlight
                    | NetworkedWorkerDispatchClaimState::Reconciling
                    | NetworkedWorkerDispatchClaimState::Settled
            ) && claim.delivery_attempt_id.is_some()
                && claim
                    .delivery_token_sha256
                    .as_deref()
                    .is_some_and(|digest| validate_sha256(digest).is_ok())
                && claim.delivery_reserved_at_unix_ms.is_some()
                && claim.payload_released_at_unix_ms.is_some_and(|released| {
                    claim.delivery_reserved_at_unix_ms.is_some_and(|reserved| {
                        reserved <= released && released <= claim.updated_at_unix_ms
                    })
                })
                && claim.payload_release_fleet_generation.is_some_and(|released_generation| {
                    claim.dispatch_fleet_generation == Some(released_generation)
                })
                && claim.payload_acknowledged_at_unix_ms.is_none()
                && claim.delivery_payload_present == Some(false)
        }
        Some("acknowledged") => {
            matches!(
                claim.state,
                NetworkedWorkerDispatchClaimState::InFlight
                    | NetworkedWorkerDispatchClaimState::Reconciling
                    | NetworkedWorkerDispatchClaimState::Settled
            ) && claim.delivery_attempt_id.is_some()
                && claim
                    .delivery_token_sha256
                    .as_deref()
                    .is_some_and(|digest| validate_sha256(digest).is_ok())
                && claim.delivery_reserved_at_unix_ms.is_some()
                && claim.payload_released_at_unix_ms.is_some()
                && claim.payload_release_fleet_generation == claim.dispatch_fleet_generation
                && claim.payload_acknowledged_at_unix_ms.is_some_and(|acknowledged| {
                    claim.payload_released_at_unix_ms.is_some_and(|released| {
                        released <= acknowledged && acknowledged <= claim.updated_at_unix_ms
                    })
                })
                && claim.delivery_payload_present == Some(false)
        }
        Some("legacy_unfenced_unknown") => {
            matches!(
                claim.state,
                NetworkedWorkerDispatchClaimState::Reconciling
                    | NetworkedWorkerDispatchClaimState::Settled
                    | NetworkedWorkerDispatchClaimState::Cancelled
                    | NetworkedWorkerDispatchClaimState::FailedClosed
            ) && claim.delivery_attempt_id.is_none()
                && claim.delivery_token_sha256.is_none()
                && claim.delivery_reserved_at_unix_ms.is_none()
                && claim.payload_released_at_unix_ms.is_none()
                && claim.payload_release_fleet_generation.is_none()
                && claim.payload_acknowledged_at_unix_ms.is_none()
                && claim.delivery_payload_present.is_none()
        }
        Some(_) => false,
    };
    if valid {
        Ok(())
    } else {
        Err(JournalError::InvalidArgument(
            "networked worker delivery-fence evidence is invalid".to_owned(),
        ))
    }
}

fn hydrate_networked_worker_dispatch_claim(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<NetworkedWorkerDispatchClaim> {
    let issued_fleet_generation = row.get::<_, i64>(5)?;
    let dispatch_fleet_generation = row.get::<_, Option<i64>>(6)?;
    let revoked_fleet_generation = row.get::<_, Option<i64>>(7)?;
    let state = row.get::<_, String>(11)?;
    let schema_version = row.get::<_, i64>(17)?;
    let schema_version = u32::try_from(schema_version).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            17,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    if !matches!(schema_version, 1..=3) {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            17,
            rusqlite::types::Type::Integer,
            "unsupported networked worker dispatch claim schema version".into(),
        ));
    }
    let payload_release_fleet_generation = row.get::<_, Option<i64>>(22)?;
    let run_generation = row.get::<_, Option<i64>>(29)?;
    Ok(NetworkedWorkerDispatchClaim {
        schema_version,
        remote_request_id: row.get(0)?,
        node_request_id: row.get(1)?,
        worker_id: row.get(2)?,
        lease_id: row.get(3)?,
        session_id: row.get(28)?,
        run_id: row.get(4)?,
        run_generation: run_generation
            .map(|generation| {
                u64::try_from(generation)
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            29,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })
                    .and_then(|generation| {
                        RuntimeGeneration::new(generation).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                29,
                                rusqlite::types::Type::Integer,
                                Box::new(error),
                            )
                        })
                    })
            })
            .transpose()?,
        issued_fleet_generation: u64::try_from(issued_fleet_generation).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                5,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?,
        dispatch_fleet_generation: dispatch_fleet_generation
            .map(u64::try_from)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })?,
        revoked_fleet_generation: revoked_fleet_generation.map(u64::try_from).transpose().map_err(
            |error| {
                rusqlite::Error::FromSqlConversionFailure(
                    7,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            },
        )?,
        lease_expires_at_unix_ms: row.get(8)?,
        capability: row.get(9)?,
        request_sha256: row.get(10)?,
        state: NetworkedWorkerDispatchClaimState::parse(state.as_str()).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        delivery_attempt_id: row.get(18)?,
        delivery_token_sha256: row.get(19)?,
        delivery_reserved_at_unix_ms: row.get(20)?,
        payload_released_at_unix_ms: row.get(21)?,
        payload_release_fleet_generation: payload_release_fleet_generation
            .map(u64::try_from)
            .transpose()
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    22,
                    rusqlite::types::Type::Integer,
                    Box::new(error),
                )
            })?,
        payload_acknowledged_at_unix_ms: row.get(23)?,
        delivery_disposition: row.get(24)?,
        delivery_payload_present: row.get::<_, Option<i64>>(25)?.map(|value| value != 0),
        validated_result_sha256: row.get(26)?,
        result_observed_at_unix_ms: row.get(27)?,
        reconciliation_disposition: row.get(12)?,
        terminal_reason_code: row.get(13)?,
        created_at_unix_ms: row.get(14)?,
        updated_at_unix_ms: row.get(15)?,
        completed_at_unix_ms: row.get(16)?,
    })
}

const NETWORKED_WORKER_DISPATCH_CLAIM_COLUMNS: &str = r#"
    remote_request_ulid, node_request_ulid, worker_id, lease_ulid, run_ulid,
    issued_fleet_generation, dispatch_fleet_generation, revoked_fleet_generation,
    lease_expires_at_unix_ms, capability, request_sha256, state,
    reconciliation_disposition, terminal_reason_code, created_at_unix_ms,
    updated_at_unix_ms, completed_at_unix_ms, schema_version,
    delivery_attempt_ulid, delivery_token_sha256, delivery_reserved_at_unix_ms,
    payload_released_at_unix_ms, payload_release_fleet_generation,
    payload_acknowledged_at_unix_ms, delivery_disposition, delivery_payload_present,
    validated_result_sha256, result_observed_at_unix_ms,
    session_ulid, run_generation
"#;

const NETWORKED_WORKER_DISPATCH_CLAIM_SELECT: &str = r#"
    SELECT remote_request_ulid, node_request_ulid, worker_id, lease_ulid, run_ulid,
           issued_fleet_generation, dispatch_fleet_generation, revoked_fleet_generation,
           lease_expires_at_unix_ms, capability, request_sha256, state,
           reconciliation_disposition, terminal_reason_code, created_at_unix_ms,
           updated_at_unix_ms, completed_at_unix_ms, schema_version,
           delivery_attempt_ulid, delivery_token_sha256, delivery_reserved_at_unix_ms,
           payload_released_at_unix_ms, payload_release_fleet_generation,
           payload_acknowledged_at_unix_ms, delivery_disposition, delivery_payload_present,
           validated_result_sha256, result_observed_at_unix_ms,
           session_ulid, run_generation
    FROM runtime_networked_worker_dispatch_claims
"#;

const NETWORKED_WORKER_DISPATCH_CLAIM_TERMINAL_SELECT: &str = r#"
    SELECT remote_request_ulid, node_request_ulid, worker_id, lease_ulid, run_ulid,
           issued_fleet_generation, dispatch_fleet_generation, revoked_fleet_generation,
           lease_expires_at_unix_ms, capability, request_sha256, state,
           reconciliation_disposition, terminal_reason_code, created_at_unix_ms,
           updated_at_unix_ms, completed_at_unix_ms, schema_version,
           delivery_attempt_ulid, delivery_token_sha256, delivery_reserved_at_unix_ms,
           payload_released_at_unix_ms, payload_release_fleet_generation,
           payload_acknowledged_at_unix_ms, delivery_disposition, delivery_payload_present,
           validated_result_sha256, result_observed_at_unix_ms,
           session_ulid, run_generation
    FROM runtime_networked_worker_dispatch_claim_terminal_evidence
"#;

fn load_networked_worker_dispatch_claim_by_remote_request_tx(
    transaction: &rusqlite::Transaction<'_>,
    remote_request_id: &str,
) -> Result<Option<NetworkedWorkerDispatchClaim>, JournalError> {
    transaction
        .query_row(
            format!("{NETWORKED_WORKER_DISPATCH_CLAIM_SELECT} WHERE remote_request_ulid = ?1")
                .as_str(),
            params![remote_request_id],
            hydrate_networked_worker_dispatch_claim,
        )
        .optional()
        .map_err(Into::into)
}

fn load_archived_networked_worker_dispatch_claim_by_remote_request_tx(
    transaction: &rusqlite::Transaction<'_>,
    remote_request_id: &str,
) -> Result<Option<NetworkedWorkerDispatchClaim>, JournalError> {
    transaction
        .query_row(
            format!(
                "{NETWORKED_WORKER_DISPATCH_CLAIM_TERMINAL_SELECT} WHERE remote_request_ulid = ?1"
            )
            .as_str(),
            params![remote_request_id],
            hydrate_networked_worker_dispatch_claim,
        )
        .optional()
        .map_err(Into::into)
}

fn archive_networked_worker_dispatch_claims_tx(
    transaction: &rusqlite::Transaction<'_>,
    reclaim_count: usize,
) -> Result<usize, JournalError> {
    if reclaim_count == 0 {
        return Ok(0);
    }
    let reclaim_count = i64::try_from(reclaim_count).map_err(|_| {
        JournalError::InvalidArgument(
            "networked worker dispatch reclamation count exceeds sqlite integer range".to_owned(),
        )
    })?;
    let request_ids = {
        let mut statement = transaction.prepare(
            r#"
                SELECT remote_request_ulid
                FROM runtime_networked_worker_dispatch_claims
                WHERE schema_version IN (2, 3)
                  AND state IN ('settled', 'cancelled', 'failed_closed')
                ORDER BY updated_at_unix_ms ASC, remote_request_ulid ASC
                LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map(params![reclaim_count], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    let archived_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM runtime_networked_worker_dispatch_claim_terminal_evidence",
        [],
        |row| row.get(0),
    )?;
    let archived_count = usize::try_from(archived_count).unwrap_or(usize::MAX);
    if archived_count.saturating_add(request_ids.len())
        > NETWORKED_WORKER_DISPATCH_TERMINAL_EVIDENCE_MAX_ENTRIES
    {
        return Err(JournalError::NetworkedWorkerDispatchTerminalEvidenceCapacityExceeded {
            current_entries: archived_count,
            requested_entries: request_ids.len(),
            max_entries: NETWORKED_WORKER_DISPATCH_TERMINAL_EVIDENCE_MAX_ENTRIES,
        });
    }
    for remote_request_id in &request_ids {
        let claim = load_networked_worker_dispatch_claim_by_remote_request_tx(
            transaction,
            remote_request_id,
        )?
        .ok_or_else(|| JournalError::NetworkedWorkerDispatchAuthorityRejected {
            remote_request_id: remote_request_id.clone(),
        })?;
        validate_networked_worker_dispatch_claim_evidence(
            &claim,
            NetworkedWorkerDispatchClaimEvidenceLocation::Active,
        )?;
        transaction.execute(
            format!(
                "INSERT INTO runtime_networked_worker_dispatch_claim_terminal_evidence ({NETWORKED_WORKER_DISPATCH_CLAIM_COLUMNS}) \
                 SELECT {NETWORKED_WORKER_DISPATCH_CLAIM_COLUMNS} FROM runtime_networked_worker_dispatch_claims \
                 WHERE remote_request_ulid = ?1 AND schema_version IN (2, 3) \
                 AND state IN ('settled', 'cancelled', 'failed_closed')"
            )
            .as_str(),
            params![remote_request_id],
        )?;
        let deleted = transaction.execute(
            r#"
                DELETE FROM runtime_networked_worker_dispatch_claims
                WHERE remote_request_ulid = ?1
                  AND schema_version IN (2, 3)
                  AND state IN ('settled', 'cancelled', 'failed_closed')
            "#,
            params![remote_request_id],
        )?;
        if deleted != 1 {
            return Err(JournalError::NetworkedWorkerDispatchAuthorityRejected {
                remote_request_id: remote_request_id.clone(),
            });
        }
    }
    Ok(request_ids.len())
}

/// Settles the in-flight claim in the same transaction as verified fleet completion evidence.
pub(super) fn settle_networked_worker_dispatch_claim_during_completion_tx(
    transaction: &rusqlite::Transaction<'_>,
    settlement: &NetworkedWorkerDispatchSettlement,
) -> Result<(), JournalError> {
    settle_networked_worker_dispatch_claim_tx(
        transaction,
        settlement,
        NetworkedWorkerDispatchClaimState::InFlight,
    )
}

fn settle_networked_worker_dispatch_claim_tx(
    transaction: &rusqlite::Transaction<'_>,
    settlement: &NetworkedWorkerDispatchSettlement,
    permitted_state: NetworkedWorkerDispatchClaimState,
) -> Result<(), JournalError> {
    validate_runtime_identity(settlement.remote_request_id.as_str(), "remote request id")?;
    validate_worker_id(settlement.worker_id.as_str())?;
    validate_runtime_identity(settlement.lease_id.as_str(), "lease id")?;
    validate_runtime_identity(settlement.session_id.as_str(), "session id")?;
    validate_runtime_identity(settlement.run_id.as_str(), "run id")?;
    if let Some(delivery_attempt_id) = settlement.delivery_attempt_id.as_deref() {
        validate_runtime_identity(delivery_attempt_id, "delivery attempt id")?;
    }
    validate_sha256(settlement.validated_result_sha256.as_str())?;
    if settlement.validated_result_sha256.bytes().any(|byte| byte.is_ascii_uppercase())
        || settlement.observed_at_unix_ms < 0
    {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch settlement evidence is invalid".to_owned(),
        ));
    }
    let (permitted_state, reconciliation_disposition) = match permitted_state {
        NetworkedWorkerDispatchClaimState::InFlight => ("in_flight", None),
        NetworkedWorkerDispatchClaimState::Reconciling => {
            ("reconciling", Some("late_result_verified"))
        }
        NetworkedWorkerDispatchClaimState::Queued
        | NetworkedWorkerDispatchClaimState::Settled
        | NetworkedWorkerDispatchClaimState::Cancelled
        | NetworkedWorkerDispatchClaimState::FailedClosed => {
            return Err(JournalError::InvalidArgument(
                "networked worker settlement source state is invalid".to_owned(),
            ));
        }
    };
    let claim = load_networked_worker_dispatch_claim_by_remote_request_tx(
        transaction,
        settlement.remote_request_id.as_str(),
    )?
    .ok_or_else(|| JournalError::NetworkedWorkerDispatchSettlementRejected {
        remote_request_id: settlement.remote_request_id.clone(),
    })?;
    validate_networked_worker_dispatch_claim_evidence(
        &claim,
        NetworkedWorkerDispatchClaimEvidenceLocation::Active,
    )?;
    if claim.session_id.as_deref() != Some(settlement.session_id.as_str())
        || claim.run_generation != Some(settlement.run_generation)
    {
        return Err(JournalError::NetworkedWorkerDispatchSettlementRejected {
            remote_request_id: settlement.remote_request_id.clone(),
        });
    }
    let (generation_matches, _) = runtime_generation_fence_matches_tx(
        transaction,
        settlement.session_id.as_str(),
        settlement.run_id.as_str(),
        RuntimeGenerationLane::Run,
        settlement.run_generation,
    )?;
    if !generation_matches {
        return Err(JournalError::NetworkedWorkerDispatchSettlementRejected {
            remote_request_id: settlement.remote_request_id.clone(),
        });
    }
    let delivery_attempt_matches = match claim.delivery_attempt_id.as_deref() {
        Some(claim_delivery_attempt_id) => {
            settlement.delivery_attempt_id.as_deref() == Some(claim_delivery_attempt_id)
        }
        None => settlement.delivery_attempt_id.is_none(),
    };
    if !delivery_attempt_matches {
        return Err(JournalError::NetworkedWorkerDispatchSettlementRejected {
            remote_request_id: settlement.remote_request_id.clone(),
        });
    }
    let persisted_at_unix_ms = settlement.observed_at_unix_ms.max(claim.updated_at_unix_ms);
    let updated = transaction.execute(
        r#"
            UPDATE runtime_networked_worker_dispatch_claims
            SET state = 'settled', reconciliation_disposition = ?1,
                terminal_reason_code = 'worker.dispatch.settled',
                validated_result_sha256 = ?2,
                result_observed_at_unix_ms = ?3,
                updated_at_unix_ms = ?4, completed_at_unix_ms = ?4
            WHERE remote_request_ulid = ?5
              AND worker_id = ?6
              AND lease_ulid = ?7
              AND run_ulid = ?8
              AND state = ?9
              AND lease_expires_at_unix_ms > ?3
              AND schema_version = 3
              AND session_ulid = ?11
              AND run_generation = ?12
              AND (
                    (?10 IS NULL AND delivery_attempt_ulid IS NULL)
                    OR delivery_attempt_ulid = ?10
                  )
        "#,
        params![
            reconciliation_disposition,
            settlement.validated_result_sha256,
            settlement.observed_at_unix_ms,
            persisted_at_unix_ms,
            settlement.remote_request_id,
            settlement.worker_id,
            settlement.lease_id,
            settlement.run_id,
            permitted_state,
            settlement.delivery_attempt_id,
            settlement.session_id,
            i64::try_from(settlement.run_generation.get()).map_err(|_| {
                JournalError::InvalidArgument(
                    "networked worker settlement generation exceeds sqlite integer range"
                        .to_owned(),
                )
            })?,
        ],
    )?;
    if updated != 1 {
        return Err(JournalError::NetworkedWorkerDispatchSettlementRejected {
            remote_request_id: settlement.remote_request_id.clone(),
        });
    }
    Ok(())
}

pub(super) fn revoke_networked_worker_dispatch_claims_tx(
    transaction: &rusqlite::Transaction<'_>,
    revocations: &[NetworkedWorkerLeaseRevocation],
    revoked_fleet_generation: u64,
    observed_at_unix_ms: i64,
) -> Result<NetworkedWorkerDispatchRevocationOutcome, JournalError> {
    if revocations.is_empty() {
        return Ok(NetworkedWorkerDispatchRevocationOutcome::default());
    }
    if observed_at_unix_ms < 0 {
        return Err(JournalError::InvalidArgument(
            "networked worker dispatch revocation timestamp is invalid".to_owned(),
        ));
    }
    let revoked_fleet_generation = i64::try_from(revoked_fleet_generation).map_err(|_| {
        JournalError::InvalidArgument(
            "networked worker dispatch revocation generation exceeds sqlite integer range"
                .to_owned(),
        )
    })?;
    let mut outcome = NetworkedWorkerDispatchRevocationOutcome::default();
    for revocation in revocations {
        validate_worker_id(revocation.worker_id.as_str())?;
        validate_runtime_identity(revocation.lease_id.as_str(), "lease id")?;
        validate_runtime_identity(revocation.run_id.as_str(), "run id")?;
        validate_reason_code(revocation.reason_code.as_str())?;
        let persisted_at_unix_ms: i64 = transaction
            .query_row(
                r#"
                SELECT MAX(updated_at_unix_ms)
                FROM runtime_networked_worker_dispatch_claims
                WHERE worker_id = ?1 AND lease_ulid = ?2 AND run_ulid = ?3
                  AND state IN ('queued', 'in_flight') AND schema_version = 3
            "#,
                params![revocation.worker_id, revocation.lease_id, revocation.run_id],
                |row| row.get::<_, Option<i64>>(0),
            )?
            .map_or(observed_at_unix_ms, |updated| observed_at_unix_ms.max(updated));
        let cancelled_queued = transaction.execute(
            r#"
                UPDATE runtime_networked_worker_dispatch_claims
                SET state = 'cancelled', terminal_reason_code = ?1,
                    delivery_payload_present = 0,
                    updated_at_unix_ms = ?2, completed_at_unix_ms = ?2
                WHERE worker_id = ?3 AND lease_ulid = ?4 AND run_ulid = ?5
                  AND state = 'queued' AND schema_version = 3
            "#,
            params![
                revocation.reason_code,
                persisted_at_unix_ms,
                revocation.worker_id,
                revocation.lease_id,
                revocation.run_id,
            ],
        )?;
        let cancelled_unreleased = transaction.execute(
            r#"
                UPDATE runtime_networked_worker_dispatch_claims
                SET state = 'cancelled',
                    reconciliation_disposition = 'payload_not_released',
                    terminal_reason_code = ?1,
                    delivery_payload_present = 0,
                    updated_at_unix_ms = ?2,
                    completed_at_unix_ms = ?2,
                    revoked_fleet_generation = ?3
                WHERE worker_id = ?4 AND lease_ulid = ?5 AND run_ulid = ?6
                  AND state = 'in_flight'
                  AND payload_released_at_unix_ms IS NULL
                  AND delivery_disposition = 'reserved_unreleased'
                  AND schema_version = 3
            "#,
            params![
                revocation.reason_code,
                persisted_at_unix_ms,
                revoked_fleet_generation,
                revocation.worker_id,
                revocation.lease_id,
                revocation.run_id,
            ],
        )?;
        let reconciling = transaction.execute(
            r#"
                UPDATE runtime_networked_worker_dispatch_claims
                SET state = 'reconciling', reconciliation_disposition = 'lease_revoked',
                    terminal_reason_code = ?1, updated_at_unix_ms = ?2,
                    revoked_fleet_generation = ?3
                WHERE worker_id = ?4 AND lease_ulid = ?5 AND run_ulid = ?6
                  AND state = 'in_flight'
                  AND payload_released_at_unix_ms IS NOT NULL
                  AND schema_version = 3
            "#,
            params![
                revocation.reason_code,
                persisted_at_unix_ms,
                revoked_fleet_generation,
                revocation.worker_id,
                revocation.lease_id,
                revocation.run_id,
            ],
        )?;
        outcome.cancelled_queued = outcome
            .cancelled_queued
            .saturating_add(cancelled_queued)
            .saturating_add(cancelled_unreleased);
        outcome.reconciling_in_flight = outcome.reconciling_in_flight.saturating_add(reconciling);
    }
    Ok(outcome)
}

pub(super) fn validate_networked_worker_fleet_snapshot(
    records: &std::collections::BTreeMap<String, palyra_workerd::WorkerFleetRecord>,
    max_entries: usize,
    updated_at_unix_ms: i64,
) -> Result<(), JournalError> {
    if max_entries == 0 || max_entries > 1_000 || records.len() > max_entries {
        return Err(JournalError::InvalidArgument(
            "networked worker fleet snapshot bounds are invalid".to_owned(),
        ));
    }
    if updated_at_unix_ms < 0 {
        return Err(JournalError::InvalidArgument(
            "networked worker fleet snapshot timestamp is invalid".to_owned(),
        ));
    }
    palyra_workerd::WorkerFleetManager::from_durable_records(records.clone())
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    Ok(())
}

pub(super) fn encode_networked_worker_fleet_records(
    records: &std::collections::BTreeMap<String, palyra_workerd::WorkerFleetRecord>,
) -> Result<Vec<(String, String)>, JournalError> {
    records
        .iter()
        .map(|(worker_id, record)| {
            serde_json::to_string(record).map(|json| (worker_id.clone(), json)).map_err(Into::into)
        })
        .collect()
}

pub(super) fn replace_networked_worker_fleet_records_tx(
    transaction: &rusqlite::Transaction<'_>,
    records: &[(String, String)],
    expected_generation: u64,
    updated_at_unix_ms: i64,
) -> Result<u64, JournalError> {
    let (actual_generation, schema_version) = transaction.query_row(
        r#"
            SELECT generation, schema_version
            FROM runtime_networked_worker_fleet_meta
            WHERE singleton_key = 1
        "#,
        [],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
    )?;
    if schema_version != 1 {
        return Err(JournalError::InvalidArgument(
            "networked worker fleet generation schema version is unsupported".to_owned(),
        ));
    }
    let actual_generation = u64::try_from(actual_generation).map_err(|_| {
        JournalError::InvalidArgument("networked worker fleet generation is invalid".to_owned())
    })?;
    if actual_generation != expected_generation {
        return Err(JournalError::NetworkedWorkerFleetGenerationConflict {
            expected_generation,
            actual_generation,
        });
    }
    let next_generation = actual_generation.checked_add(1).ok_or_else(|| {
        JournalError::InvalidArgument("networked worker fleet generation overflow".to_owned())
    })?;
    let next_generation_sql = i64::try_from(next_generation).map_err(|_| {
        JournalError::InvalidArgument(
            "networked worker fleet generation exceeds sqlite integer range".to_owned(),
        )
    })?;
    let expected_generation_sql = i64::try_from(expected_generation).map_err(|_| {
        JournalError::InvalidArgument(
            "expected networked worker fleet generation exceeds sqlite integer range".to_owned(),
        )
    })?;

    transaction.execute("DELETE FROM runtime_networked_worker_fleet", [])?;
    for (worker_id, record_json) in records {
        transaction.execute(
            r#"
                INSERT INTO runtime_networked_worker_fleet (
                    worker_id, record_json, updated_at_unix_ms, schema_version
                ) VALUES (?1, ?2, ?3, 1)
            "#,
            params![worker_id, record_json, updated_at_unix_ms],
        )?;
    }
    let updated = transaction.execute(
        r#"
            UPDATE runtime_networked_worker_fleet_meta
            SET generation = ?1, updated_at_unix_ms = ?2
            WHERE singleton_key = 1 AND generation = ?3 AND schema_version = 1
        "#,
        params![next_generation_sql, updated_at_unix_ms, expected_generation_sql],
    )?;
    if updated != 1 {
        return Err(JournalError::InvalidArgument(
            "networked worker fleet generation metadata is missing or invalid".to_owned(),
        ));
    }
    Ok(next_generation)
}

fn enqueue_networked_worker_expiry_outbox_batch_tx(
    transaction: &rusqlite::Transaction<'_>,
    records: &[NetworkedWorkerExpiryOutboxRecord],
    max_entries: usize,
) -> Result<(), JournalError> {
    if max_entries == 0 || max_entries > 1_000 {
        return Err(JournalError::InvalidArgument(
            "networked worker expiry outbox batch bounds are invalid".to_owned(),
        ));
    }
    let entry_count: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM runtime_networked_worker_expiry_outbox",
        [],
        |row| row.get(0),
    )?;
    let current_entries = usize::try_from(entry_count).unwrap_or(usize::MAX);
    let mut new_entries = 0_usize;
    let mut new_records = Vec::new();
    let mut seen_event_ids = std::collections::BTreeSet::new();
    let mut seen_lease_ids = std::collections::BTreeSet::new();
    for record in records {
        if !seen_event_ids.insert(record.event_id.as_str()) {
            return Err(JournalError::InvalidArgument(
                "networked worker expiry outbox batch repeats an event id".to_owned(),
            ));
        }
        let run_id = record.event.run_id.as_deref().ok_or_else(|| {
            JournalError::InvalidArgument(
                "networked worker expiry outbox event is missing run identity".to_owned(),
            )
        })?;
        let lease_id = record.event.lease_id.as_deref().ok_or_else(|| {
            JournalError::InvalidArgument(
                "networked worker expiry outbox event is missing lease identity".to_owned(),
            )
        })?;
        if !seen_lease_ids.insert(lease_id) {
            return Err(JournalError::InvalidArgument(
                "networked worker expiry outbox batch repeats a lease id".to_owned(),
            ));
        }
        let event_json = serde_json::to_string(&record.event)?;
        let existing = transaction
            .query_row(
                r#"
                    SELECT worker_id, run_ulid, lease_ulid, event_json,
                           created_at_unix_ms, schema_version
                    FROM runtime_networked_worker_expiry_outbox
                    WHERE event_ulid = ?1 OR lease_ulid = ?2
                "#,
                params![record.event_id, lease_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()?;
        if let Some((
            existing_worker_id,
            existing_run_id,
            existing_lease_id,
            existing_event_json,
            existing_created_at,
            existing_schema_version,
        )) = existing
        {
            if existing_worker_id == record.event.worker_id
                && existing_run_id == run_id
                && existing_lease_id == lease_id
                && existing_event_json == event_json
                && existing_created_at == record.event.timestamp_unix_ms
                && existing_schema_version == 1
            {
                continue;
            }
            return Err(JournalError::InvalidArgument(
                "networked worker expiry outbox identity conflicts with different evidence"
                    .to_owned(),
            ));
        }
        new_entries = new_entries.saturating_add(1);
        new_records.push((record, event_json));
    }
    let projected_entries = current_entries.saturating_add(new_entries);
    if projected_entries > max_entries {
        return Err(JournalError::NetworkedWorkerExpiryOutboxCapacityExceeded {
            current_entries: projected_entries,
            max_entries,
        });
    }
    for (record, event_json) in new_records {
        let run_id = record.event.run_id.as_deref().ok_or_else(|| {
            JournalError::InvalidArgument(
                "networked worker expiry outbox event is missing run identity".to_owned(),
            )
        })?;
        let lease_id = record.event.lease_id.as_deref().ok_or_else(|| {
            JournalError::InvalidArgument(
                "networked worker expiry outbox event is missing lease identity".to_owned(),
            )
        })?;
        transaction.execute(
            r#"
                INSERT INTO runtime_networked_worker_expiry_outbox (
                    event_ulid, worker_id, run_ulid, lease_ulid, event_json,
                    created_at_unix_ms, schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
            "#,
            params![
                record.event_id,
                record.event.worker_id,
                run_id,
                lease_id,
                event_json,
                record.event.timestamp_unix_ms,
            ],
        )?;
    }
    Ok(())
}

pub(super) fn active_runtime_generation_tx(
    connection: &Connection,
    session_id: &str,
    run_id: &str,
    lane: RuntimeGenerationLane,
    now: i64,
) -> Result<Option<GenerationLeaseV1>, JournalError> {
    Ok(load_generation_tx(connection, session_id, lane)?.filter(|lease| {
        now < lease.expires_at_unix_ms
            && lease.run_id.as_ref().is_some_and(|active_run_id| active_run_id.as_str() == run_id)
    }))
}

pub(super) fn active_runtime_generation_for_session_lane_tx(
    connection: &Connection,
    session_id: &str,
    lane: RuntimeGenerationLane,
    now: i64,
) -> Result<Option<GenerationLeaseV1>, JournalError> {
    Ok(load_generation_tx(connection, session_id, lane)?
        .filter(|lease| now < lease.expires_at_unix_ms))
}

const PROVIDER_ATTEMPT_METADATA_MAX_BYTES: usize = 128;
const PROVIDER_CONFIGURATION_SINGLETON_KEY: &str = "model_provider";
const PROVIDER_GENERATION_OWNER_PREFIX: &str = "provider_configuration_epoch:";

pub(super) fn provider_generation_owner(epoch: RuntimeGeneration) -> String {
    format!("{PROVIDER_GENERATION_OWNER_PREFIX}{}", epoch.get())
}

fn provider_generation_owner_epoch(owner: &str) -> Result<RuntimeGeneration, JournalError> {
    let raw = owner.strip_prefix(PROVIDER_GENERATION_OWNER_PREFIX).ok_or_else(|| {
        JournalError::InvalidArgument(
            "active Provider generation is missing configuration authority".to_owned(),
        )
    })?;
    let epoch = raw.parse::<u64>().map_err(|_| {
        JournalError::InvalidArgument(
            "active Provider generation has malformed configuration authority".to_owned(),
        )
    })?;
    RuntimeGeneration::new(epoch).map_err(|error| JournalError::InvalidArgument(error.to_string()))
}

pub(super) fn current_provider_configuration_epoch_tx(
    connection: &Connection,
) -> Result<Option<RuntimeGeneration>, JournalError> {
    let row = connection
        .query_row(
            r#"
                SELECT epoch, schema_version
                FROM runtime_provider_configuration_head
                WHERE singleton_key = ?1
            "#,
            params![PROVIDER_CONFIGURATION_SINGLETON_KEY],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((epoch, schema_version)) = row else {
        return Ok(None);
    };
    if schema_version != 1 {
        return Err(JournalError::InvalidArgument(
            "provider configuration authority uses an unsupported schema".to_owned(),
        ));
    }
    let epoch = u64::try_from(epoch).map_err(|_| {
        JournalError::InvalidArgument(
            "provider configuration epoch exceeds supported range".to_owned(),
        )
    })?;
    RuntimeGeneration::new(epoch)
        .map(Some)
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))
}

fn advance_provider_configuration_epoch_tx(
    connection: &Connection,
    now: i64,
) -> Result<RuntimeGeneration, JournalError> {
    let previous = current_provider_configuration_epoch_tx(connection)?;
    if previous.is_none()
        && connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_provider_configuration_head)",
            [],
            |row| row.get::<_, bool>(0),
        )?
    {
        return Err(JournalError::InvalidArgument(
            "provider configuration authority uses an unsupported schema".to_owned(),
        ));
    }
    let epoch = match previous {
        Some(epoch) => {
            epoch.next().map_err(|error| JournalError::InvalidArgument(error.to_string()))?
        }
        None => RuntimeGeneration::new(1)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
    };
    connection.execute(
        r#"
            INSERT INTO runtime_provider_configuration_head (
                singleton_key, epoch, updated_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, 1)
            ON CONFLICT(singleton_key) DO UPDATE SET
                epoch = excluded.epoch,
                updated_at_unix_ms = excluded.updated_at_unix_ms,
                schema_version = excluded.schema_version
        "#,
        params![PROVIDER_CONFIGURATION_SINGLETON_KEY, runtime_generation_sql(epoch)?, now,],
    )?;
    connection.execute(
        r#"
            INSERT INTO runtime_provider_configuration_events (
                event_ulid, from_epoch, to_epoch, transition_kind, reason_code,
                created_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
        "#,
        params![
            Ulid::new().to_string(),
            previous.map(runtime_generation_sql).transpose()?,
            runtime_generation_sql(epoch)?,
            if previous.is_some() {
                RuntimeGenerationTransitionKind::ModelSwitchSuperseded.as_str()
            } else {
                RuntimeGenerationTransitionKind::Activated.as_str()
            },
            if previous.is_some() {
                "runtime.generation.provider_model_switch"
            } else {
                "runtime.generation.provider_configuration_activated"
            },
            now,
        ],
    )?;
    Ok(epoch)
}

fn validate_provider_attempt_metadata(
    session_id: &str,
    run_id: &str,
    provider_id: &str,
    model_id: &str,
) -> Result<(), JournalError> {
    RuntimeSessionId::parse(session_id)
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    RuntimeRunId::parse(run_id)
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    validate_provider_candidate_metadata(provider_id, model_id)
}

fn validate_provider_candidate_metadata(
    provider_id: &str,
    model_id: &str,
) -> Result<(), JournalError> {
    if provider_id.trim().is_empty()
        || provider_id.len() > PROVIDER_ATTEMPT_METADATA_MAX_BYTES
        || model_id.trim().is_empty()
        || model_id.len() > PROVIDER_ATTEMPT_METADATA_MAX_BYTES
    {
        return Err(JournalError::InvalidArgument(
            "provider attempt metadata is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_provider_attempt_outcome(
    outcome: &str,
    error_class: Option<&str>,
) -> Result<(), JournalError> {
    if !matches!(outcome, "success" | "failure" | "outcome_unknown")
        || error_class.is_some_and(|value| {
            value.trim().is_empty() || value.len() > PROVIDER_ATTEMPT_METADATA_MAX_BYTES
        })
        || (outcome == "success" && error_class.is_some())
        || (outcome == "outcome_unknown" && error_class.is_none())
    {
        return Err(JournalError::InvalidArgument(
            "provider attempt completion metadata is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn provider_attempt_event_id(
    attempt_id: &RuntimeAttemptId,
    suffix: &str,
) -> Result<RuntimeEventId, JournalError> {
    RuntimeEventId::parse(format!("provider_attempt:{attempt_id}:{suffix}").as_str())
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn provider_attempt_runtime_event(
    session_id: &str,
    run_id: &str,
    attempt_id: RuntimeAttemptId,
    generation: RuntimeGeneration,
    configuration_epoch: RuntimeGeneration,
    event_id: RuntimeEventId,
    causal_parent_event_id: Option<RuntimeEventId>,
    event_name: RuntimeEventName,
    reason_code: &str,
    provider_id: &str,
    model_id: &str,
    outcome: &str,
    error_class: Option<&str>,
    credential: Option<&ProviderCredentialAttemptMetadata>,
    now: i64,
) -> Result<RuntimeEventAppendRequest, JournalError> {
    let session_identity = RuntimeSessionId::parse(session_id)
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let run_identity = RuntimeRunId::parse(run_id)
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let mut identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse(run_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        session_identity,
        run_identity,
        generation,
    );
    identities.attempt_id = Some(attempt_id.clone());
    identities.causal_links.push(RuntimeCausalLink {
        relation: RuntimeCausalLinkKind::ChildOf,
        source: RuntimeIdentityRef::new(RuntimeIdentityKind::Attempt, attempt_id.as_str())
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        target: RuntimeIdentityRef::new(RuntimeIdentityKind::Run, run_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
    });
    if event_name == RuntimeEventName::ProviderAttemptCompleted && reason_code.contains("supersed")
    {
        if let Some(parent_event_id) = causal_parent_event_id.as_ref() {
            identities.causal_links.push(RuntimeCausalLink {
                relation: RuntimeCausalLinkKind::Supersedes,
                source: RuntimeIdentityRef::new(RuntimeIdentityKind::Event, event_id.as_str())
                    .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
                target: RuntimeIdentityRef::new(
                    RuntimeIdentityKind::Event,
                    parent_event_id.as_str(),
                )
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
            });
        }
    }
    let descriptor = event_name.descriptor();
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "configuration_epoch".to_owned(),
        serde_json::Value::Number(configuration_epoch.get().into()),
    );
    metadata.insert("provider_id".to_owned(), serde_json::Value::String(provider_id.to_owned()));
    metadata.insert("model_id".to_owned(), serde_json::Value::String(model_id.to_owned()));
    metadata.insert("outcome".to_owned(), serde_json::Value::String(outcome.to_owned()));
    if let Some(error_class) = error_class {
        metadata
            .insert("error_class".to_owned(), serde_json::Value::String(error_class.to_owned()));
    }
    if let Some(credential) = credential {
        metadata.insert(
            "auth_profile_binding".to_owned(),
            serde_json::json!({
                "profile_id_sha256": credential.profile_id_sha256,
                "auth_class": credential.auth_class,
                "selection_reason": credential.selection_reason,
            }),
        );
    }
    let mut envelope = RuntimeEventEnvelopeV2 {
        schema_version: 2,
        event_id,
        identities,
        sequence: 0,
        causal_parent_event_id,
        subsystem: descriptor.subsystem,
        phase: descriptor.phase,
        event_name,
        reason_code: reason_code.to_owned(),
        actor_kind: descriptor.actor_kind,
        retryability: descriptor.retryability,
        redaction_class: descriptor.redaction_class,
        terminal: descriptor.terminal,
        payload: RuntimeEventPayloadRef::Inline { metadata: serde_json::Value::Object(metadata) },
        occurred_at_unix_ms: now,
        extensions: BTreeMap::new(),
    };
    if !envelope.identities.causal_links.is_empty() {
        envelope.extensions.insert(
            "runtime_identity_diagnostics_v1".to_owned(),
            serde_json::to_value(envelope.identities.redacted_diagnostics())?,
        );
    }
    Ok(RuntimeEventAppendRequest { lane: RuntimeGenerationLane::Provider, envelope })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn activate_or_refresh_run_generation_tx(
    connection: &Connection,
    session_id: &str,
    run_id: &str,
    owner: &str,
    ttl_ms: i64,
    transition_kind: RuntimeGenerationTransitionKind,
    reason_code: &str,
    now: i64,
) -> Result<GenerationLeaseV1, JournalError> {
    activate_or_refresh_generation_tx(
        connection,
        session_id,
        Some(run_id),
        RuntimeGenerationLane::Run,
        owner,
        ttl_ms,
        transition_kind,
        reason_code,
        now,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn activate_or_refresh_generation_tx(
    connection: &Connection,
    session_id: &str,
    run_id: Option<&str>,
    lane: RuntimeGenerationLane,
    owner: &str,
    ttl_ms: i64,
    transition_kind: RuntimeGenerationTransitionKind,
    reason_code: &str,
    now: i64,
) -> Result<GenerationLeaseV1, JournalError> {
    let current = load_generation_tx(connection, session_id, lane)?
        .filter(|lease| now < lease.expires_at_unix_ms);
    if transition_kind == RuntimeGenerationTransitionKind::Activated
        && current.as_ref().is_some_and(|lease| {
            lease.run_id.as_ref().map(|active_run_id| active_run_id.as_str()) == run_id
        })
    {
        return current.ok_or_else(|| {
            JournalError::InvalidArgument("active runtime generation disappeared".to_owned())
        });
    }
    let last_generation = max_generation_tx(connection, session_id, lane)?;
    let generation = match last_generation {
        Some(generation) => {
            generation.next().map_err(|error| JournalError::InvalidArgument(error.to_string()))?
        }
        None => RuntimeGeneration::new(1)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
    };
    let lease_id = Ulid::new().to_string();
    let expires_at_unix_ms = now.saturating_add(ttl_ms);
    connection.execute(
        r#"
            INSERT INTO runtime_generation_leases (
                session_ulid, lane, lease_ulid, run_ulid, generation, owner,
                acquired_at_unix_ms, expires_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1)
            ON CONFLICT(session_ulid, lane) DO UPDATE SET
                lease_ulid = excluded.lease_ulid,
                run_ulid = excluded.run_ulid,
                generation = excluded.generation,
                owner = excluded.owner,
                acquired_at_unix_ms = excluded.acquired_at_unix_ms,
                expires_at_unix_ms = excluded.expires_at_unix_ms,
                schema_version = excluded.schema_version
        "#,
        params![
            session_id,
            lane.as_str(),
            lease_id,
            run_id,
            i64::try_from(generation.get()).map_err(|_| JournalError::InvalidArgument(
                "runtime generation exceeds sqlite integer range".to_owned()
            ))?,
            owner,
            now,
            expires_at_unix_ms,
        ],
    )?;
    connection.execute(
        r#"
            INSERT INTO runtime_generation_events (
                event_ulid, session_ulid, run_ulid, lane, from_generation,
                to_generation, transition_kind, reason_code, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        "#,
        params![
            Ulid::new().to_string(),
            session_id,
            run_id,
            lane.as_str(),
            current.as_ref().map(|lease| i64::try_from(lease.generation.get()).unwrap_or(i64::MAX)),
            i64::try_from(generation.get()).unwrap_or(i64::MAX),
            transition_kind.as_str(),
            reason_code,
            now,
        ],
    )?;
    let lease = GenerationLeaseV1 {
        schema_version: 1,
        lease_id: palyra_common::runtime_contracts::RuntimeLeaseId::parse(lease_id.as_str())
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        session_id: palyra_common::runtime_contracts::RuntimeSessionId::parse(session_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        run_id: run_id
            .map(palyra_common::runtime_contracts::RuntimeRunId::parse)
            .transpose()
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        lane,
        generation,
        owner: owner.to_owned(),
        acquired_at_unix_ms: now,
        expires_at_unix_ms,
    };
    lease.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    Ok(lease)
}

fn supersede_active_provider_lanes_tx(
    connection: &Connection,
    now: i64,
) -> Result<u64, JournalError> {
    let active = {
        let mut statement = connection.prepare(
            r#"
                SELECT session_ulid, run_ulid, generation
                FROM runtime_generation_leases
                WHERE lane = ?1 AND expires_at_unix_ms > ?2
                ORDER BY session_ulid ASC
            "#,
        )?;
        let rows =
            statement.query_map(params![RuntimeGenerationLane::Provider.as_str(), now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for (session_id, run_id, generation) in &active {
        connection.execute(
            r#"
                INSERT INTO runtime_generation_events (
                    event_ulid, session_ulid, run_ulid, lane, from_generation,
                    to_generation, transition_kind, reason_code, created_at_unix_ms,
                    schema_version
                ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, 1)
            "#,
            params![
                Ulid::new().to_string(),
                session_id,
                run_id,
                RuntimeGenerationLane::Provider.as_str(),
                generation,
                RuntimeGenerationTransitionKind::ModelSwitchSuperseded.as_str(),
                "runtime.generation.provider_model_switch",
                now,
            ],
        )?;
    }
    connection.execute(
        "DELETE FROM runtime_generation_leases WHERE lane = ?1 AND expires_at_unix_ms > ?2",
        params![RuntimeGenerationLane::Provider.as_str(), now],
    )?;
    u64::try_from(active.len()).map_err(|_| {
        JournalError::InvalidArgument(
            "active provider generation count exceeds supported range".to_owned(),
        )
    })
}

fn record_provider_attempt_stale_diagnostic_tx(
    connection: &Connection,
    authority: &ProviderAttemptRuntimeAuthority,
    expected_generation: Option<RuntimeGeneration>,
    reason_code: &str,
    now: i64,
) -> Result<(), JournalError> {
    let diagnostic_id = Ulid::new().to_string();
    connection.execute(
        r#"
            INSERT INTO runtime_stale_event_diagnostics (
                diagnostic_ulid, session_ulid, run_ulid, lane, expected_generation,
                observed_generation, subsystem, disposition, reason_code, payload_sha256,
                payload_bytes, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, 0, ?10)
        "#,
        params![
            diagnostic_id,
            authority.session_id.as_str(),
            authority.run_id.as_str(),
            RuntimeGenerationLane::Provider.as_str(),
            expected_generation.map(runtime_generation_sql).transpose()?,
            runtime_generation_sql(authority.generation)?,
            RuntimeSubsystem::Provider.as_str(),
            StaleEventDisposition::PersistedDiagnostic.as_str(),
            reason_code,
            now,
        ],
    )?;
    super::metadata_trace::append_stale_suppression_metadata_trace_tx(
        connection,
        authority.run_id.as_str(),
        diagnostic_id.as_str(),
        reason_code,
        now,
    )?;
    Ok(())
}

fn validate_runtime_stale_event_diagnostic_request(
    request: &RuntimeStaleEventDiagnosticRequest,
) -> Result<(), JournalError> {
    if request.session_id.trim().is_empty()
        || request.reason_code.trim().is_empty()
        || request.run_id.as_deref().is_some_and(|run_id| run_id.trim().is_empty())
    {
        return Err(JournalError::InvalidArgument(
            "runtime stale-event diagnostic request is invalid".to_owned(),
        ));
    }
    Ok(())
}

/// Appends one metadata-only stale-event diagnostic inside the caller's transaction.
pub(super) fn record_runtime_stale_event_diagnostic_tx(
    connection: &Connection,
    request: &RuntimeStaleEventDiagnosticRequest,
    now: i64,
) -> Result<(), JournalError> {
    validate_runtime_stale_event_diagnostic_request(request)?;
    let diagnostic_id = Ulid::new().to_string();
    connection.execute(
        r#"
            INSERT INTO runtime_stale_event_diagnostics (
                diagnostic_ulid, session_ulid, run_ulid, lane, expected_generation,
                observed_generation, subsystem, disposition, reason_code, payload_sha256,
                payload_bytes, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, 0, ?10)
        "#,
        params![
            diagnostic_id,
            request.session_id,
            request.run_id,
            request.lane.as_str(),
            request.expected_generation.map(runtime_generation_sql).transpose()?,
            runtime_generation_sql(request.observed_generation)?,
            request.subsystem.as_str(),
            request.disposition.as_str(),
            request.reason_code,
            now,
        ],
    )?;
    if let Some(run_id) = request.run_id.as_deref() {
        super::metadata_trace::append_stale_suppression_metadata_trace_tx(
            connection,
            run_id,
            diagnostic_id.as_str(),
            request.reason_code.as_str(),
            now,
        )?;
    }
    Ok(())
}

fn validate_provider_attempt_start_parent_tx(
    connection: &Connection,
    authority: &ProviderAttemptRuntimeAuthority,
    provider_id: &str,
    model_id: &str,
) -> Result<(), JournalError> {
    let envelope_json = connection
        .query_row(
            r#"
                SELECT envelope_json
                FROM runtime_events_v2
                WHERE event_ulid = ?1
                  AND session_ulid = ?2
                  AND run_ulid = ?3
                  AND lane = ?4
                  AND generation = ?5
                  AND event_name = ?6
            "#,
            params![
                authority.started_event_id.as_str(),
                authority.session_id.as_str(),
                authority.run_id.as_str(),
                RuntimeGenerationLane::Provider.as_str(),
                runtime_generation_sql(authority.generation)?,
                RuntimeEventName::ProviderAttemptStarted.as_str(),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "provider attempt completion parent evidence is missing or mismatched".to_owned(),
            )
        })?;
    let envelope: RuntimeEventEnvelopeV2 = serde_json::from_str(envelope_json.as_str())?;
    let metadata = match &envelope.payload {
        RuntimeEventPayloadRef::Inline { metadata } => metadata,
        RuntimeEventPayloadRef::Artifact { .. } | RuntimeEventPayloadRef::Omitted { .. } => {
            return Err(JournalError::InvalidArgument(
                "provider attempt start evidence has an unsupported payload".to_owned(),
            ));
        }
    };
    if envelope.event_id != authority.started_event_id
        || envelope.identities.session_id != authority.session_id
        || envelope.identities.run_id != authority.run_id
        || envelope.identities.attempt_id.as_ref() != Some(&authority.attempt_id)
        || envelope.identities.generation != authority.generation
        || envelope.subsystem != RuntimeSubsystem::Provider
        || envelope.event_name != RuntimeEventName::ProviderAttemptStarted
        || envelope.causal_parent_event_id.is_some()
        || metadata.get("configuration_epoch").and_then(serde_json::Value::as_u64)
            != Some(authority.configuration_epoch.get())
        || metadata.get("provider_id").and_then(serde_json::Value::as_str) != Some(provider_id)
        || metadata.get("model_id").and_then(serde_json::Value::as_str) != Some(model_id)
        || metadata.get("outcome").and_then(serde_json::Value::as_str) != Some("started")
    {
        return Err(JournalError::InvalidArgument(
            "provider attempt completion parent evidence conflicts with start authority".to_owned(),
        ));
    }
    Ok(())
}

fn validate_runtime_event_append_request(
    request: &RuntimeEventAppendRequest,
) -> Result<(), JournalError> {
    request
        .envelope
        .validate()
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let descriptor_lane = request.envelope.event_name.descriptor().generation_lane;
    if request.lane != descriptor_lane {
        return Err(JournalError::InvalidArgument(format!(
            "runtime event {} requires generation lane {}, observed {}",
            request.envelope.event_name.as_str(),
            descriptor_lane.as_str(),
            request.lane.as_str()
        )));
    }
    Ok(())
}

pub(super) fn append_runtime_event_tx(
    connection: &Connection,
    max_payload_bytes: usize,
    request: &RuntimeEventAppendRequest,
    now: i64,
) -> Result<RuntimeEventAppendOutcome, JournalError> {
    validate_runtime_event_append_request(request)?;
    let session_id = request.envelope.identities.session_id.as_str();
    let run_id = request.envelope.identities.run_id.as_str();
    let observed = request.envelope.identities.generation;
    let active_lease = load_generation_tx(connection, session_id, request.lane)?
        .filter(|lease| now < lease.expires_at_unix_ms);
    let check = match active_lease.as_ref() {
        Some(lease)
            if lease
                .run_id
                .as_ref()
                .is_some_and(|active_run_id| active_run_id.as_str() == run_id) =>
        {
            lease.check(observed)
        }
        Some(lease) => GenerationCheckOutcome {
            schema_version: 1,
            expected: Some(lease.generation),
            observed,
            disposition: GenerationCheckDisposition::Stale,
            reason_code: "runtime.generation.run_mismatch".to_owned(),
        },
        None => GenerationCheckOutcome {
            schema_version: 1,
            expected: None,
            observed,
            disposition: GenerationCheckDisposition::MissingActiveGeneration,
            reason_code: "runtime.generation.missing_active".to_owned(),
        },
    };
    if !check.permits_mutation() {
        let diagnostic_id = Ulid::new().to_string();
        connection.execute(
            r#"
                INSERT INTO runtime_stale_event_diagnostics (
                    diagnostic_ulid, session_ulid, run_ulid, lane, expected_generation,
                    observed_generation, subsystem, disposition, reason_code, payload_sha256,
                    payload_bytes, created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, 0, ?10)
            "#,
            params![
                diagnostic_id,
                session_id,
                run_id,
                request.lane.as_str(),
                check.expected.map(|value| i64::try_from(value.get()).unwrap_or(i64::MAX)),
                i64::try_from(observed.get()).unwrap_or(i64::MAX),
                request.envelope.subsystem.as_str(),
                StaleEventDisposition::PersistedDiagnostic.as_str(),
                check.reason_code,
                now,
            ],
        )?;
        super::metadata_trace::append_stale_suppression_metadata_trace_tx(
            connection,
            run_id,
            diagnostic_id.as_str(),
            check.reason_code.as_str(),
            now,
        )?;
        return Ok(RuntimeEventAppendOutcome::StaleSuppressed);
    }
    persist_validated_runtime_event_tx(connection, max_payload_bytes, request, now)
}

fn persist_validated_runtime_event_tx(
    connection: &Connection,
    max_payload_bytes: usize,
    request: &RuntimeEventAppendRequest,
    now: i64,
) -> Result<RuntimeEventAppendOutcome, JournalError> {
    let session_id = request.envelope.identities.session_id.as_str();
    let run_id = request.envelope.identities.run_id.as_str();
    let observed = request.envelope.identities.generation;
    let existing = connection
        .query_row(
            r#"
                SELECT
                    session_ulid,
                    run_ulid,
                    lane,
                    generation,
                    sequence,
                    terminal,
                    event_name,
                    reason_code,
                    envelope_json
                FROM runtime_events_v2
                WHERE event_ulid = ?1
            "#,
            params![request.envelope.event_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                ))
            },
        )
        .optional()?;
    if let Some((
        existing_session_id,
        existing_run_id,
        existing_lane,
        existing_generation,
        existing_sequence,
        existing_terminal,
        existing_event_name,
        existing_reason_code,
        existing_envelope_json,
    )) = existing
    {
        let existing_sequence = u64::try_from(existing_sequence).map_err(|_| {
            JournalError::InvalidArgument(
                "stored runtime event sequence exceeds supported range".to_owned(),
            )
        })?;
        let mut replay_envelope = request.envelope.clone();
        replay_envelope.sequence = existing_sequence;
        let replay_raw = serde_json::to_vec(&replay_envelope)?;
        if replay_raw.len() > max_payload_bytes {
            return Err(JournalError::PayloadTooLarge {
                payload_kind: "runtime_event_v2",
                actual_bytes: replay_raw.len(),
                max_bytes: max_payload_bytes,
            });
        }
        let (replay_envelope_json, _) = sanitize_payload(replay_raw.as_slice())?;
        if existing_session_id == session_id
            && existing_run_id == run_id
            && existing_lane == request.lane.as_str()
            && existing_generation == i64::try_from(observed.get()).unwrap_or(i64::MAX)
            && existing_terminal == i64::from(replay_envelope.terminal)
            && existing_event_name == replay_envelope.event_name.as_str()
            && existing_reason_code == replay_envelope.reason_code
            && runtime_event_envelopes_match_for_replay(
                existing_envelope_json.as_str(),
                replay_envelope_json.as_str(),
            )?
        {
            return Ok(RuntimeEventAppendOutcome::AlreadyAppended { sequence: existing_sequence });
        }
        return Err(JournalError::InvalidArgument(
            "runtime event id is already bound to conflicting durable evidence".to_owned(),
        ));
    }
    let sequence = next_runtime_event_sequence_tx(connection, session_id, request.lane, observed)?;
    let mut envelope = request.envelope.clone();
    envelope.sequence = sequence;
    let raw = serde_json::to_vec(&envelope)?;
    if raw.len() > max_payload_bytes {
        return Err(JournalError::PayloadTooLarge {
            payload_kind: "runtime_event_v2",
            actual_bytes: raw.len(),
            max_bytes: max_payload_bytes,
        });
    }
    let (envelope_json, _) = sanitize_payload(raw.as_slice())?;
    connection.execute(
        r#"
            INSERT INTO runtime_events_v2 (
                event_ulid, session_ulid, run_ulid, lane, generation, sequence,
                terminal, event_name, reason_code, envelope_json, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        "#,
        params![
            envelope.event_id.as_str(),
            session_id,
            run_id,
            request.lane.as_str(),
            i64::try_from(observed.get()).unwrap_or(i64::MAX),
            i64::try_from(sequence).map_err(|_| {
                JournalError::InvalidArgument(
                    "runtime event sequence exceeds sqlite integer range".to_owned(),
                )
            })?,
            i64::from(envelope.terminal),
            envelope.event_name.as_str(),
            envelope.reason_code,
            envelope_json,
            now,
        ],
    )?;
    super::metadata_trace::append_runtime_event_metadata_trace_tx(connection, &envelope, now)?;
    Ok(RuntimeEventAppendOutcome::Appended { sequence })
}

fn runtime_event_envelopes_match_for_replay(
    existing_json: &str,
    replay_json: &str,
) -> Result<bool, JournalError> {
    let mut existing: RuntimeEventEnvelopeV2 = serde_json::from_str(existing_json)?;
    let mut replay: RuntimeEventEnvelopeV2 = serde_json::from_str(replay_json)?;
    // The occurrence time is host observation metadata, not producer-owned
    // identity. A retry of the same deterministic event may cross a clock tick.
    existing.occurred_at_unix_ms = 0;
    replay.occurred_at_unix_ms = 0;
    Ok(existing == replay)
}

pub(super) fn invalidate_provider_generation_for_run_tx(
    connection: &Connection,
    session_id: &str,
    run_id: &str,
    transition_kind: RuntimeGenerationTransitionKind,
    reason_code: &str,
    now: i64,
) -> Result<RuntimeGenerationInvalidateOutcome, JournalError> {
    invalidate_runtime_generation_tx(
        connection,
        &RuntimeGenerationInvalidateRequest {
            session_id: session_id.to_owned(),
            run_id: Some(run_id.to_owned()),
            lane: RuntimeGenerationLane::Provider,
            transition_kind,
            reason_code: reason_code.to_owned(),
        },
        now,
    )
}

pub(super) fn invalidate_runtime_kernel_child_generations_for_run_tx(
    connection: &Connection,
    session_id: &str,
    run_id: &str,
    transition_kind: RuntimeGenerationTransitionKind,
    now: i64,
) -> Result<(), JournalError> {
    for (lane, reason_code) in [
        (RuntimeGenerationLane::Harness, "runtime.generation.kernel_harness_run_terminal"),
        (RuntimeGenerationLane::Tool, "runtime.generation.kernel_tool_run_terminal"),
        (RuntimeGenerationLane::Delivery, "runtime.generation.kernel_delivery_run_terminal"),
    ] {
        // A mismatched lane belongs to a newer or concurrent run and must remain
        // untouched. Exact run-owned lanes are released before their Run parent.
        let _ = invalidate_runtime_generation_tx(
            connection,
            &RuntimeGenerationInvalidateRequest {
                session_id: session_id.to_owned(),
                run_id: Some(run_id.to_owned()),
                lane,
                transition_kind,
                reason_code: reason_code.to_owned(),
            },
            now,
        )?;
    }
    Ok(())
}

pub(super) fn invalidate_runtime_generation_tx(
    connection: &Connection,
    request: &RuntimeGenerationInvalidateRequest,
    now: i64,
) -> Result<RuntimeGenerationInvalidateOutcome, JournalError> {
    let Some(current) = load_generation_tx(connection, request.session_id.as_str(), request.lane)?
    else {
        return Ok(RuntimeGenerationInvalidateOutcome::AlreadyInactive);
    };
    if request.run_id.as_deref().is_some_and(|run_id| {
        current.run_id.as_ref().map(|active_run_id| active_run_id.as_str()) != Some(run_id)
    }) {
        return Ok(RuntimeGenerationInvalidateOutcome::RunMismatch);
    }
    let deleted = connection.execute(
        "DELETE FROM runtime_generation_leases WHERE session_ulid = ?1 AND lane = ?2 AND generation = ?3",
        params![
            request.session_id,
            request.lane.as_str(),
            i64::try_from(current.generation.get()).unwrap_or(i64::MAX),
        ],
    )?;
    if deleted == 0 {
        return Ok(RuntimeGenerationInvalidateOutcome::AlreadyInactive);
    }
    connection.execute(
        r#"
            INSERT INTO runtime_generation_events (
                event_ulid, session_ulid, run_ulid, lane, from_generation,
                to_generation, transition_kind, reason_code, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8)
        "#,
        params![
            Ulid::new().to_string(),
            request.session_id,
            request.run_id,
            request.lane.as_str(),
            i64::try_from(current.generation.get()).unwrap_or(i64::MAX),
            request.transition_kind.as_str(),
            request.reason_code,
            now,
        ],
    )?;
    Ok(RuntimeGenerationInvalidateOutcome::Invalidated)
}

fn next_runtime_event_sequence_tx(
    connection: &Connection,
    session_id: &str,
    lane: RuntimeGenerationLane,
    generation: RuntimeGeneration,
) -> Result<u64, JournalError> {
    let generation = i64::try_from(generation.get()).map_err(|_| {
        JournalError::InvalidArgument("runtime generation exceeds sqlite integer range".to_owned())
    })?;
    let last_sequence = connection.query_row(
        r#"
            SELECT MAX(sequence)
            FROM runtime_events_v2
            WHERE session_ulid = ?1 AND lane = ?2 AND generation = ?3
        "#,
        params![session_id, lane.as_str(), generation],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    match last_sequence {
        Some(sequence) => u64::try_from(sequence)
            .map_err(|_| {
                JournalError::InvalidArgument(
                    "stored runtime event sequence exceeds supported range".to_owned(),
                )
            })?
            .checked_add(1)
            .ok_or_else(|| {
                JournalError::InvalidArgument("runtime event sequence is exhausted".to_owned())
            }),
        None => Ok(1),
    }
}

pub(super) fn max_generation_tx(
    connection: &Connection,
    session_id: &str,
    lane: RuntimeGenerationLane,
) -> Result<Option<RuntimeGeneration>, JournalError> {
    let max_generation = connection.query_row(
        r#"
            SELECT MAX(generation) FROM (
                SELECT generation
                FROM runtime_generation_leases
                WHERE session_ulid = ?1 AND lane = ?2
                UNION ALL
                SELECT to_generation AS generation
                FROM runtime_generation_events
                WHERE session_ulid = ?1 AND lane = ?2 AND to_generation IS NOT NULL
            )
        "#,
        params![session_id, lane.as_str()],
        |row| row.get::<_, Option<i64>>(0),
    )?;
    max_generation
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| {
                    JournalError::InvalidArgument(
                        "stored runtime generation exceeds supported range".to_owned(),
                    )
                })
                .and_then(|value| {
                    RuntimeGeneration::new(value)
                        .map_err(|error| JournalError::InvalidArgument(error.to_string()))
                })
        })
        .transpose()
}

/// Checks one run-scoped generation fence against the latest durable lane authority.
///
/// Runtime-generation closure deliberately removes the live lease, so late child
/// completion cannot rely on an active-lease lookup after its parent terminalizes.
/// Generation transition history remains durable and is serialized by the caller's
/// write transaction. Requiring both the lane maximum and an exact run-owned
/// transition prevents a newer steer or a different run in the same session from
/// accepting an older callback.
pub(super) fn runtime_generation_fence_matches_tx(
    connection: &Connection,
    session_id: &str,
    run_id: &str,
    lane: RuntimeGenerationLane,
    expected_generation: RuntimeGeneration,
) -> Result<(bool, Option<RuntimeGeneration>), JournalError> {
    let actual_generation = max_generation_tx(connection, session_id, lane)?;
    if actual_generation != Some(expected_generation) {
        return Ok((false, actual_generation));
    }
    let expected_generation = i64::try_from(expected_generation.get()).map_err(|_| {
        JournalError::InvalidArgument(
            "expected runtime generation exceeds sqlite integer range".to_owned(),
        )
    })?;
    let owned_by_run = connection.query_row(
        r#"
            SELECT EXISTS(
                SELECT 1
                FROM runtime_generation_events
                WHERE session_ulid = ?1
                  AND run_ulid = ?2
                  AND lane = ?3
                  AND to_generation = ?4
            )
        "#,
        params![session_id, run_id, lane.as_str(), expected_generation],
        |row| row.get::<_, i64>(0),
    )? == 1;
    Ok((owned_by_run, actual_generation))
}

fn load_generation_tx(
    connection: &Connection,
    session_id: &str,
    lane: RuntimeGenerationLane,
) -> Result<Option<GenerationLeaseV1>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT lease_ulid, session_ulid, run_ulid, lane, generation, owner,
                       acquired_at_unix_ms, expires_at_unix_ms, schema_version
                FROM runtime_generation_leases
                WHERE session_ulid = ?1 AND lane = ?2
            "#,
            params![session_id, lane.as_str()],
            hydrate_generation_lease,
        )
        .optional()
        .map_err(Into::into)
}

fn hydrate_generation_lease(row: &rusqlite::Row<'_>) -> rusqlite::Result<GenerationLeaseV1> {
    let lane: String = row.get(3)?;
    let generation = u64::try_from(row.get::<_, i64>(4)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    Ok(GenerationLeaseV1 {
        schema_version: u32::try_from(row.get::<_, i64>(8)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })?,
        lease_id: palyra_common::runtime_contracts::RuntimeLeaseId::parse(
            row.get::<_, String>(0)?.as_str(),
        )
        .map_err(to_from_sql_error)?,
        session_id: palyra_common::runtime_contracts::RuntimeSessionId::parse(
            row.get::<_, String>(1)?.as_str(),
        )
        .map_err(to_from_sql_error)?,
        run_id: row
            .get::<_, Option<String>>(2)?
            .as_deref()
            .map(palyra_common::runtime_contracts::RuntimeRunId::parse)
            .transpose()
            .map_err(to_from_sql_error)?,
        lane: RuntimeGenerationLane::parse(lane.as_str()).ok_or_else(|| {
            to_from_sql_error(std::io::Error::other(format!("unknown runtime lane: {lane}")))
        })?,
        generation: RuntimeGeneration::new(generation).map_err(to_from_sql_error)?,
        owner: row.get(5)?,
        acquired_at_unix_ms: row.get(6)?,
        expires_at_unix_ms: row.get(7)?,
    })
}

fn to_from_sql_error(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn runtime_generation_sql(generation: RuntimeGeneration) -> Result<i64, JournalError> {
    i64::try_from(generation.get()).map_err(|_| {
        JournalError::InvalidArgument("runtime generation exceeds sqlite integer range".to_owned())
    })
}

fn validate_runtime_health_component_activation(
    activation: &RuntimeHealthComponentActivation,
) -> Result<(), JournalError> {
    validate_reason_code(activation.reason_code.as_str())?;
    activation
        .policy
        .validate()
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    if activation.fallback_component_id.is_some() != activation.fallback_authority_class.is_some() {
        return Err(JournalError::InvalidArgument(
            "runtime health activation fallback identity and authority must be paired".to_owned(),
        ));
    }
    if activation
        .fallback_authority_class
        .is_some_and(|fallback| !activation.authority_class.permits_fallback(fallback))
    {
        return Err(JournalError::InvalidArgument(
            "runtime health activation fallback would increase authority".to_owned(),
        ));
    }
    Ok(())
}

fn validated_runtime_health_activation_inventory(
    components: &[RuntimeHealthComponentActivation],
    activated_at_unix_ms: i64,
) -> Result<Vec<RuntimeHealthComponentActivation>, JournalError> {
    if activated_at_unix_ms < 0 {
        return Err(JournalError::InvalidArgument(
            "runtime health activation timestamp is invalid".to_owned(),
        ));
    }
    let mut sorted = components.to_vec();
    sorted.sort_by(|left, right| left.component_id.as_str().cmp(right.component_id.as_str()));
    if sorted.windows(2).any(|pair| pair[0].component_id == pair[1].component_id) {
        return Err(JournalError::InvalidArgument(
            "runtime health activation inventory contains duplicate components".to_owned(),
        ));
    }
    for component in &sorted {
        validate_runtime_health_component_activation(component)?;
    }
    Ok(sorted)
}

fn activate_runtime_health_components_tx(
    connection: &Connection,
    components: &[RuntimeHealthComponentActivation],
    activated_at_unix_ms: i64,
) -> Result<RuntimeHealthActivationOutcome, JournalError> {
    let mut generations = BTreeMap::new();
    for component in components {
        let generation =
            activate_runtime_health_component_tx(connection, component, activated_at_unix_ms)?;
        generations.insert(component.component_id.as_str().to_owned(), generation);
    }
    Ok(RuntimeHealthActivationOutcome { generations })
}

fn activate_runtime_health_component_tx(
    connection: &Connection,
    activation: &RuntimeHealthComponentActivation,
    activated_at_unix_ms: i64,
) -> Result<RuntimeGeneration, JournalError> {
    let previous = load_component_health_tx(connection, activation.component_id.as_str())?;
    let has_probe_authority = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM runtime_health_probe_leases WHERE component_ulid = ?1)",
        params![activation.component_id.as_str()],
        |row| row.get::<_, bool>(0),
    )?;
    if has_probe_authority
        || previous.as_ref().is_some_and(|health| health.state == RuntimeHealthState::Probing)
    {
        return Err(JournalError::InvalidArgument(
            "runtime health activation requires probe reconciliation before generation advance"
                .to_owned(),
        ));
    }
    let activation_time = previous
        .as_ref()
        .map_or(activated_at_unix_ms, |health| activated_at_unix_ms.max(health.updated_at_unix_ms));
    let stored_head = connection
        .query_row(
            "SELECT last_generation FROM runtime_component_generation_heads WHERE component_ulid = ?1 AND schema_version = ?2",
            params![
                activation.component_id.as_str(),
                i64::from(RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION),
            ],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    if stored_head.is_none()
        && connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_component_generation_heads WHERE component_ulid = ?1)",
            params![activation.component_id.as_str()],
            |row| row.get::<_, bool>(0),
        )?
    {
        return Err(JournalError::InvalidArgument(
            "runtime component generation head uses an unsupported schema".to_owned(),
        ));
    }
    let previous_generation = previous.as_ref().map(|health| health.generation.get()).unwrap_or(0);
    let head_generation = stored_head
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0)
        .max(previous_generation);
    let next_generation = head_generation.checked_add(1).ok_or_else(|| {
        JournalError::InvalidArgument("runtime component generation is exhausted".to_owned())
    })?;
    let generation = RuntimeGeneration::new(next_generation)
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    if let Some(previous) = previous.as_ref() {
        if previous.authority_class != activation.authority_class
            || previous.fallback_component_id != activation.fallback_component_id
            || previous.fallback_authority_class != activation.fallback_authority_class
        {
            return Err(JournalError::InvalidArgument(
                "runtime health activation changed protected component authority".to_owned(),
            ));
        }
    }
    let health = if let Some(previous) = previous.as_ref() {
        RuntimeComponentHealthV1 {
            schema_version:
                palyra_common::runtime_contracts::RUNTIME_COMPONENT_HEALTH_SCHEMA_VERSION,
            component_id: activation.component_id.clone(),
            generation,
            state: previous.state,
            authority_class: previous.authority_class,
            strike_count: previous.strike_count,
            reason_code: previous.reason_code.clone(),
            first_failure_at_unix_ms: previous.first_failure_at_unix_ms,
            last_failure_at_unix_ms: previous.last_failure_at_unix_ms,
            expires_at_unix_ms: previous.expires_at_unix_ms,
            fallback_component_id: previous.fallback_component_id.clone(),
            fallback_authority_class: previous.fallback_authority_class,
            security_quarantine: previous.security_quarantine,
            policy: activation.policy.clone(),
            updated_at_unix_ms: activation_time,
        }
    } else {
        RuntimeComponentHealthV1 {
            schema_version:
                palyra_common::runtime_contracts::RUNTIME_COMPONENT_HEALTH_SCHEMA_VERSION,
            component_id: activation.component_id.clone(),
            generation,
            state: RuntimeHealthState::Healthy,
            authority_class: activation.authority_class,
            strike_count: 0,
            reason_code: activation.reason_code.clone(),
            first_failure_at_unix_ms: None,
            last_failure_at_unix_ms: None,
            expires_at_unix_ms: None,
            fallback_component_id: activation.fallback_component_id.clone(),
            fallback_authority_class: activation.fallback_authority_class,
            security_quarantine: false,
            policy: activation.policy.clone(),
            updated_at_unix_ms: activation_time,
        }
    };
    health.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let raw = serde_json::to_vec(&health)?;
    let (json, _) = sanitize_payload(raw.as_slice())?;
    if json.len() > RUNTIME_HEALTH_CONTRACT_MAX_BYTES {
        return Err(JournalError::PayloadTooLarge {
            payload_kind: "runtime component health",
            actual_bytes: json.len(),
            max_bytes: RUNTIME_HEALTH_CONTRACT_MAX_BYTES,
        });
    }
    connection.execute(
        r#"
            INSERT INTO runtime_component_health (
                component_ulid, generation, state, reason_code, health_json, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(component_ulid) DO UPDATE SET
                generation = excluded.generation,
                state = excluded.state,
                reason_code = excluded.reason_code,
                health_json = excluded.health_json,
                updated_at_unix_ms = excluded.updated_at_unix_ms
        "#,
        params![
            health.component_id.as_str(),
            runtime_generation_sql(generation)?,
            health.state.as_str(),
            health.reason_code,
            json,
            activation_time,
        ],
    )?;
    let updated_head = connection.execute(
        r#"
            INSERT INTO runtime_component_generation_heads (
                component_ulid, last_generation, updated_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(component_ulid) DO UPDATE SET
                last_generation = excluded.last_generation,
                updated_at_unix_ms = excluded.updated_at_unix_ms
            WHERE runtime_component_generation_heads.schema_version = 1
        "#,
        params![
            activation.component_id.as_str(),
            runtime_generation_sql(generation)?,
            activation_time,
            i64::from(RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION),
        ],
    )?;
    if updated_head != 1 {
        return Err(JournalError::InvalidArgument(
            "runtime component generation head changed to an unsupported schema".to_owned(),
        ));
    }
    connection.execute(
        r#"
            INSERT INTO runtime_component_health_events (
                event_ulid, component_ulid, from_state, to_state, reason_code,
                created_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
        "#,
        params![
            Ulid::new().to_string(),
            activation.component_id.as_str(),
            previous.as_ref().map(|health| health.state.as_str()),
            health.state.as_str(),
            activation.reason_code,
            activation_time,
        ],
    )?;
    Ok(generation)
}

#[cfg(test)]
fn ensure_component_generation_head_tx(
    connection: &Connection,
    component_id: &str,
    generation: RuntimeGeneration,
    updated_at_unix_ms: i64,
) -> Result<(), JournalError> {
    let existing = connection
        .query_row(
            "SELECT last_generation, updated_at_unix_ms FROM runtime_component_generation_heads WHERE component_ulid = ?1 AND schema_version = ?2",
            params![component_id, i64::from(RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION)],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    if existing.is_none()
        && connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_component_generation_heads WHERE component_ulid = ?1)",
            params![component_id],
            |row| row.get::<_, bool>(0),
        )?
    {
        return Err(JournalError::InvalidArgument(
            "runtime component generation head uses an unsupported schema".to_owned(),
        ));
    }
    if let Some((stored_generation, stored_updated_at)) = existing {
        let stored_generation = u64::try_from(stored_generation).map_err(|_| {
            JournalError::InvalidArgument("runtime component generation head is invalid".to_owned())
        })?;
        if generation.get() < stored_generation {
            return Err(JournalError::InvalidArgument(
                "runtime health update would roll back the durable generation head".to_owned(),
            ));
        }
        if generation.get() == stored_generation {
            if updated_at_unix_ms < stored_updated_at {
                return Err(JournalError::InvalidArgument(
                    "runtime health update timestamp predates the durable generation head"
                        .to_owned(),
                ));
            }
            return Ok(());
        }
        connection.execute(
            r#"
                UPDATE runtime_component_generation_heads
                SET last_generation = ?2, updated_at_unix_ms = ?3
                WHERE component_ulid = ?1 AND schema_version = ?4
            "#,
            params![
                component_id,
                runtime_generation_sql(generation)?,
                updated_at_unix_ms,
                i64::from(RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION),
            ],
        )?;
        return Ok(());
    }
    connection.execute(
        r#"
            INSERT INTO runtime_component_generation_heads (
                component_ulid, last_generation, updated_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4)
        "#,
        params![
            component_id,
            runtime_generation_sql(generation)?,
            updated_at_unix_ms,
            i64::from(RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION),
        ],
    )?;
    Ok(())
}

fn validate_runtime_health_probe_begin_request(
    request: &RuntimeHealthProbeBeginRequest,
) -> Result<(), JournalError> {
    request.lease.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    validate_reason_code(request.reason_code.as_str())?;
    validate_optional_health_evidence_sha256(request.authorization_evidence_sha256.as_deref())?;
    validate_optional_health_evidence_sha256(request.authorized_actor_id_sha256.as_deref())
}

fn validate_optional_health_evidence_sha256(value: Option<&str>) -> Result<(), JournalError> {
    if value.is_none_or(|digest| {
        digest.len() == 64
            && digest.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    }) {
        return Ok(());
    }
    Err(JournalError::InvalidArgument("runtime health evidence digest is invalid".to_owned()))
}

fn sanitize_runtime_health_quarantine_clear_audit_payload(
    raw_payload: &[u8],
    expected_authorization_evidence_sha256: &str,
) -> Result<(String, bool), JournalError> {
    validate_optional_health_evidence_sha256(Some(expected_authorization_evidence_sha256))?;
    let mut payload = serde_json::from_slice::<serde_json::Value>(raw_payload).map_err(|_| {
        JournalError::InvalidArgument(
            "runtime quarantine clear audit payload must be a JSON object".to_owned(),
        )
    })?;
    let payload_object = payload.as_object_mut().ok_or_else(|| {
        JournalError::InvalidArgument(
            "runtime quarantine clear audit payload must be a JSON object".to_owned(),
        )
    })?;
    let authorization_evidence =
        payload_object.remove(QUARANTINE_CLEAR_AUTHORIZATION_EVIDENCE_KEY).ok_or_else(|| {
            JournalError::InvalidArgument(
                "runtime quarantine clear audit authorization evidence is missing".to_owned(),
            )
        })?;
    if authorization_evidence.as_str() != Some(expected_authorization_evidence_sha256) {
        return Err(JournalError::InvalidArgument(
            "runtime quarantine clear audit authorization evidence is not bound to the clear request"
                .to_owned(),
        ));
    }

    // The generic sanitizer must continue treating authorization-named fields
    // as sensitive. This boundary preserves only the validated digest already
    // bound to the typed quarantine-clear request.
    let redacted = redact_value(&mut payload, None);
    let payload_object = payload.as_object_mut().ok_or_else(|| {
        JournalError::InvalidArgument(
            "runtime quarantine clear audit payload lost its JSON object shape".to_owned(),
        )
    })?;
    payload_object
        .insert(QUARANTINE_CLEAR_AUTHORIZATION_EVIDENCE_KEY.to_owned(), authorization_evidence);
    Ok((serde_json::to_string(&payload)?, redacted))
}

fn validate_successful_probe_evidence_for_quarantine_clear_tx(
    connection: &Connection,
    clear: &QuarantineClearRequest,
) -> Result<(), JournalError> {
    let Some(expected_lease) = clear.probe_lease.as_ref() else {
        return Ok(());
    };
    let expected_evidence = clear.probe_evidence_sha256.as_deref().ok_or_else(|| {
        JournalError::InvalidArgument(
            "runtime quarantine clear probe evidence must be paired".to_owned(),
        )
    })?;
    let begin = load_runtime_health_probe_begin_tx(connection, expected_lease.lease_id.as_str())?
        .ok_or_else(|| {
        JournalError::InvalidArgument(
            "runtime quarantine clear probe lease has no immutable begin evidence".to_owned(),
        )
    })?;
    if begin.lease != *expected_lease {
        return Err(JournalError::InvalidArgument(
            "runtime quarantine clear probe lease conflicts with immutable begin evidence"
                .to_owned(),
        ));
    }
    let terminal = connection
        .query_row(
            r#"
                SELECT component_ulid, expected_generation, disposition,
                       mutation_attempted, result_state, probe_evidence_sha256
                FROM runtime_health_probe_terminal_evidence
                WHERE lease_ulid = ?1
                  AND schema_version = ?2
            "#,
            params![
                expected_lease.lease_id.as_str(),
                i64::from(RUNTIME_HEALTH_PROBE_TERMINAL_ROW_SCHEMA_VERSION),
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)? != 0,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "runtime quarantine clear probe lease has no terminal evidence".to_owned(),
            )
        })?;
    let generation = u64::try_from(terminal.1).map_err(|_| {
        JournalError::InvalidArgument(
            "runtime quarantine clear probe generation is invalid".to_owned(),
        )
    })?;
    if terminal.0 != clear.component_id.as_str()
        || generation != clear.expected_generation.get()
        || terminal.2 != HealthProbeDisposition::Passed.as_str()
        || terminal.3
        || terminal.4 != RuntimeHealthState::Healthy.as_str()
        || terminal.5.as_deref() != Some(expected_evidence)
    {
        return Err(JournalError::InvalidArgument(
            "runtime quarantine clear requires exact successful non-mutating probe evidence"
                .to_owned(),
        ));
    }
    Ok(())
}

fn begin_runtime_health_probe_tx(
    connection: &Connection,
    request: &RuntimeHealthProbeBeginRequest,
) -> Result<RuntimeHealthProbeBeginOutcome, JournalError> {
    if let Some(existing) =
        load_runtime_health_probe_begin_tx(connection, request.lease.lease_id.as_str())?
    {
        if existing.lease == request.lease
            && existing.reason_code == request.reason_code
            && existing.authorization_evidence_sha256 == request.authorization_evidence_sha256
            && existing.authorized_actor_id_sha256 == request.authorized_actor_id_sha256
        {
            let health =
                load_component_health_tx(connection, existing.lease.component_id.as_str())?
                    .ok_or_else(|| {
                        JournalError::InvalidArgument(
                            "replayed health probe begin lost component health".to_owned(),
                        )
                    })?;
            if health.state != RuntimeHealthState::Probing
                || health.generation != existing.lease.expected_generation
            {
                return Err(JournalError::InvalidArgument(
                    "replayed health probe begin no longer owns active authority".to_owned(),
                ));
            }
            let active =
                load_health_probe_lease_tx(connection, existing.lease.component_id.as_str())?;
            if active.as_ref() != Some(&existing.lease) {
                return Err(JournalError::InvalidArgument(
                    "replayed health probe begin lost its active lease".to_owned(),
                ));
            }
            return Ok(RuntimeHealthProbeBeginOutcome {
                health,
                lease: existing.lease,
                replayed: true,
            });
        }
        return Err(JournalError::InvalidArgument(
            "runtime health probe begin conflicts with immutable evidence".to_owned(),
        ));
    }

    let mut health = load_component_health_tx(connection, request.lease.component_id.as_str())?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "health probe begin requires an existing component health record".to_owned(),
            )
        })?;
    if request.lease.expected_generation != health.generation
        || request.lease.authority_class != health.authority_class
        || request.lease.issued_at_unix_ms < health.updated_at_unix_ms
    {
        return Err(JournalError::InvalidArgument(
            "health probe begin does not match durable component authority".to_owned(),
        ));
    }
    let source_state = health.state;
    let eligible_cooldown = source_state == RuntimeHealthState::Cooldown
        && health
            .expires_at_unix_ms
            .is_some_and(|expiry| request.lease.issued_at_unix_ms >= expiry)
        && !health.security_quarantine;
    let eligible_quarantine = source_state == RuntimeHealthState::Quarantined
        && !health.security_quarantine
        && request.authorized_actor_id_sha256.is_some();
    if !eligible_cooldown && !eligible_quarantine {
        return Err(JournalError::InvalidArgument(
            "health probe begin requires expired cooldown or authorized quarantine recovery"
                .to_owned(),
        ));
    }
    if load_health_probe_lease_tx(connection, request.lease.component_id.as_str())?.is_some() {
        return Err(JournalError::InvalidArgument(
            "runtime health probe capacity is already leased".to_owned(),
        ));
    }
    let lease_json = serialize_runtime_health_contract(&request.lease, "health probe lease")?;
    connection.execute(
        r#"
            INSERT INTO runtime_health_probe_begins (
                lease_ulid, component_ulid, expected_generation, authority_class,
                source_state, security_quarantine_before, reason_code,
                authorization_evidence_sha256, authorized_actor_id_sha256,
                lease_json, begun_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        "#,
        params![
            request.lease.lease_id.as_str(),
            request.lease.component_id.as_str(),
            runtime_generation_sql(request.lease.expected_generation)?,
            request.lease.authority_class.as_str(),
            source_state.as_str(),
            i64::from(health.security_quarantine),
            request.reason_code,
            request.authorization_evidence_sha256,
            request.authorized_actor_id_sha256,
            lease_json,
            request.lease.issued_at_unix_ms,
            i64::from(RUNTIME_HEALTH_PROBE_BEGIN_ROW_SCHEMA_VERSION),
        ],
    )?;
    connection.execute(
        r#"
            INSERT INTO runtime_health_probe_leases (
                lease_ulid, component_ulid, expected_generation, authority_class,
                lease_json, issued_at_unix_ms, expires_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#,
        params![
            request.lease.lease_id.as_str(),
            request.lease.component_id.as_str(),
            runtime_generation_sql(request.lease.expected_generation)?,
            request.lease.authority_class.as_str(),
            serialize_runtime_health_contract(&request.lease, "health probe lease")?,
            request.lease.issued_at_unix_ms,
            request.lease.expires_at_unix_ms,
            i64::from(RUNTIME_HEALTH_PROBE_ACTIVE_ROW_SCHEMA_VERSION),
        ],
    )?;
    health.state = RuntimeHealthState::Probing;
    health.reason_code = request.reason_code.clone();
    health.expires_at_unix_ms = None;
    health.updated_at_unix_ms = request.lease.issued_at_unix_ms;
    persist_component_health_update_tx(connection, source_state, &health)?;
    Ok(RuntimeHealthProbeBeginOutcome { health, lease: request.lease.clone(), replayed: false })
}

fn load_runtime_health_probe_begin_tx(
    connection: &Connection,
    lease_id: &str,
) -> Result<Option<RuntimeHealthProbeBeginEvidence>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT lease_json, source_state, security_quarantine_before,
                       reason_code, authorization_evidence_sha256,
                       authorized_actor_id_sha256
                FROM runtime_health_probe_begins
                WHERE lease_ulid = ?1 AND schema_version IN (1, 2)
            "#,
            params![lease_id],
            |row| {
                let lease_json: String = row.get(0)?;
                let source_state: String = row.get(1)?;
                Ok((
                    lease_json,
                    source_state,
                    row.get::<_, i64>(2)? != 0,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?
        .map(
            |(
                lease_json,
                source_state,
                security_quarantine,
                reason_code,
                evidence,
                authorized_actor,
            )| {
                let lease = serde_json::from_str::<HealthProbeLeaseV1>(lease_json.as_str())?;
                lease
                    .validate()
                    .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
                let source_state =
                    RuntimeHealthState::from_str(source_state.as_str()).ok_or_else(|| {
                        JournalError::InvalidArgument(
                            "runtime health probe begin source state is invalid".to_owned(),
                        )
                    })?;
                Ok(RuntimeHealthProbeBeginEvidence {
                    lease,
                    source_state,
                    security_quarantine_before: security_quarantine,
                    reason_code,
                    authorization_evidence_sha256: evidence,
                    authorized_actor_id_sha256: authorized_actor,
                })
            },
        )
        .transpose()
}

fn settle_runtime_health_probe_tx(
    connection: &Connection,
    request: &RuntimeHealthProbeSettlementRequest,
) -> Result<RuntimeHealthProbeSettlementOutcome, JournalError> {
    let begin =
        load_runtime_health_probe_begin_tx(connection, request.settlement.lease_id.as_str())?;
    let settlement = begin.as_ref().map_or_else(
        || request.settlement.clone(),
        |begin| request.settlement.normalized_for_lease(&begin.lease),
    );
    if let Some(outcome) = load_runtime_health_probe_terminal_outcome_tx(
        connection,
        settlement.lease_id.as_str(),
        Some(&settlement),
        request.probe_evidence_sha256.as_deref(),
    )? {
        return Ok(outcome);
    }
    let begin = begin.ok_or_else(|| {
        JournalError::InvalidArgument("health probe settlement has no begin evidence".to_owned())
    })?;
    let lease = begin.lease;
    let source_state = begin.source_state;
    let security_before = begin.security_quarantine_before;
    let mut health = load_component_health_tx(connection, lease.component_id.as_str())?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "health probe settlement lost component health".to_owned(),
            )
        })?;
    settlement
        .validate_for(&lease, &health)
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let (result_state, security_after) = settlement
        .resulting_posture(&lease, &health)
        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let settlement_json =
        serialize_runtime_health_contract(&settlement, "health probe settlement")?;
    let result = settlement.result.clone();
    health.state = result_state;
    health.security_quarantine = security_after;
    health.reason_code = result.reason_code.clone();
    health.strike_count =
        if result_state == RuntimeHealthState::Healthy { 0 } else { health.strike_count.max(1) };
    if result_state == RuntimeHealthState::Healthy {
        health.first_failure_at_unix_ms = None;
        health.last_failure_at_unix_ms = None;
    } else {
        health.first_failure_at_unix_ms =
            health.first_failure_at_unix_ms.or(Some(result.completed_at_unix_ms));
        health.last_failure_at_unix_ms = Some(result.completed_at_unix_ms);
    }
    health.expires_at_unix_ms = None;
    health.updated_at_unix_ms = result.completed_at_unix_ms;
    let result_health_json =
        serialize_runtime_health_contract(&health, "health probe resulting health")?;
    persist_component_health_update_tx(connection, RuntimeHealthState::Probing, &health)?;
    let deleted = connection.execute(
        r#"
            DELETE FROM runtime_health_probe_leases
            WHERE lease_ulid = ?1 AND component_ulid = ?2
              AND expected_generation = ?3 AND schema_version = ?4
        "#,
        params![
            lease.lease_id.as_str(),
            lease.component_id.as_str(),
            runtime_generation_sql(lease.expected_generation)?,
            i64::from(RUNTIME_HEALTH_PROBE_ACTIVE_ROW_SCHEMA_VERSION),
        ],
    )?;
    if deleted != 1 {
        return Err(JournalError::InvalidArgument(
            "health probe settlement lost exact active lease authority".to_owned(),
        ));
    }
    insert_runtime_health_probe_terminal_evidence_tx(
        connection,
        &lease,
        source_state,
        security_before,
        &result,
        result_state,
        security_after,
        true,
        "settlement",
        Some(settlement_json.as_str()),
        Some(result_health_json.as_str()),
        request.probe_evidence_sha256.as_deref(),
        result.completed_at_unix_ms,
    )?;
    Ok(RuntimeHealthProbeSettlementOutcome {
        health,
        disposition: result.disposition,
        completed_at_unix_ms: result.completed_at_unix_ms,
        replayed: false,
        health_mutated: true,
    })
}

#[allow(clippy::too_many_arguments)]
fn insert_runtime_health_probe_terminal_evidence_tx(
    connection: &Connection,
    lease: &HealthProbeLeaseV1,
    source_state: RuntimeHealthState,
    security_before: bool,
    result: &HealthProbeResult,
    result_state: RuntimeHealthState,
    security_after: bool,
    health_mutated: bool,
    terminal_kind: &str,
    settlement_json: Option<&str>,
    result_health_json: Option<&str>,
    probe_evidence_sha256: Option<&str>,
    settled_at_unix_ms: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO runtime_health_probe_terminal_evidence (
                lease_ulid, component_ulid, expected_generation, authority_class,
                source_state, result_state, disposition, mutation_attempted,
                security_quarantine_before, security_quarantine_after, health_mutated,
                terminal_kind, reason_code, settlement_json, result_health_json,
                probe_evidence_sha256, completed_at_unix_ms, settled_at_unix_ms,
                schema_version
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                ?13, ?14, ?15, ?16, ?17, ?18, ?19
            )
        "#,
        params![
            lease.lease_id.as_str(),
            lease.component_id.as_str(),
            runtime_generation_sql(lease.expected_generation)?,
            lease.authority_class.as_str(),
            source_state.as_str(),
            result_state.as_str(),
            result.disposition.as_str(),
            i64::from(result.mutation_attempted),
            i64::from(security_before),
            i64::from(security_after),
            i64::from(health_mutated),
            terminal_kind,
            result.reason_code,
            settlement_json,
            result_health_json,
            probe_evidence_sha256,
            result.completed_at_unix_ms,
            settled_at_unix_ms,
            i64::from(RUNTIME_HEALTH_PROBE_TERMINAL_ROW_SCHEMA_VERSION),
        ],
    )?;
    Ok(())
}

fn load_runtime_health_probe_terminal_outcome_tx(
    connection: &Connection,
    lease_id: &str,
    expected_settlement: Option<&HealthProbeSettlementV1>,
    expected_probe_evidence_sha256: Option<&str>,
) -> Result<Option<RuntimeHealthProbeSettlementOutcome>, JournalError> {
    let row = connection
        .query_row(
            r#"
                SELECT component_ulid, expected_generation, disposition, completed_at_unix_ms,
                       settlement_json, result_health_json, probe_evidence_sha256,
                       health_mutated, schema_version
                FROM runtime_health_probe_terminal_evidence
                WHERE lease_ulid = ?1
            "#,
            params![lease_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)? != 0,
                    row.get::<_, i64>(8)?,
                ))
            },
        )
        .optional()?;
    let Some((
        component_id,
        generation,
        disposition,
        completed_at,
        settlement_json,
        result_health_json,
        evidence,
        health_mutated,
        schema_version,
    )) = row
    else {
        return Ok(None);
    };
    let expected_json = expected_settlement
        .map(|settlement| serialize_runtime_health_contract(settlement, "health probe settlement"))
        .transpose()?;
    if expected_json.as_deref() != settlement_json.as_deref()
        || expected_probe_evidence_sha256 != evidence.as_deref()
    {
        return Err(JournalError::InvalidArgument(
            "health probe settlement conflicts with immutable terminal evidence".to_owned(),
        ));
    }
    if schema_version != i64::from(RUNTIME_HEALTH_PROBE_TERMINAL_ROW_SCHEMA_VERSION) {
        return Err(JournalError::InvalidArgument(
            "health probe terminal evidence cannot replay without an immutable result snapshot"
                .to_owned(),
        ));
    }
    let result_health_json = result_health_json.ok_or_else(|| {
        JournalError::InvalidArgument(
            "health probe terminal evidence is missing its immutable result snapshot".to_owned(),
        )
    })?;
    let health = serde_json::from_str::<RuntimeComponentHealthV1>(result_health_json.as_str())?;
    health.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let generation = u64::try_from(generation).map_err(|_| {
        JournalError::InvalidArgument("health probe terminal generation is invalid".to_owned())
    })?;
    if health.component_id.as_str() != component_id || health.generation.get() != generation {
        return Err(JournalError::InvalidArgument(
            "health probe terminal result snapshot is not bound to immutable scalar evidence"
                .to_owned(),
        ));
    }
    let disposition = HealthProbeDisposition::from_str(disposition.as_str()).ok_or_else(|| {
        JournalError::InvalidArgument("health probe terminal disposition is invalid".to_owned())
    })?;
    Ok(Some(RuntimeHealthProbeSettlementOutcome {
        health,
        disposition,
        completed_at_unix_ms: completed_at,
        replayed: true,
        health_mutated,
    }))
}

fn serialize_runtime_health_contract<T: serde::Serialize>(
    value: &T,
    payload_kind: &'static str,
) -> Result<String, JournalError> {
    let raw = serde_json::to_vec(value)?;
    let (json, _) = sanitize_payload(raw.as_slice())?;
    if json.len() > RUNTIME_HEALTH_CONTRACT_MAX_BYTES {
        return Err(JournalError::PayloadTooLarge {
            payload_kind,
            actual_bytes: json.len(),
            max_bytes: RUNTIME_HEALTH_CONTRACT_MAX_BYTES,
        });
    }
    Ok(json)
}

fn reconcile_runtime_health_probes_tx(
    connection: &Connection,
    mode: RuntimeHealthProbeReconciliationMode,
    now_unix_ms: i64,
) -> Result<RuntimeHealthProbeReconciliationOutcome, JournalError> {
    let predicate = match mode {
        RuntimeHealthProbeReconciliationMode::Startup => "schema_version IN (1, 2)",
        RuntimeHealthProbeReconciliationMode::Periodic => {
            "schema_version IN (1, 2) AND expires_at_unix_ms <= ?1"
        }
    };
    let sql = format!(
        "SELECT component_ulid FROM runtime_health_probe_leases WHERE {predicate} \
         ORDER BY component_ulid ASC LIMIT ?2"
    );
    let mut statement = connection.prepare(sql.as_str())?;
    let rows = statement.query_map(
        params![
            now_unix_ms,
            i64::try_from(RUNTIME_HEALTH_RECONCILIATION_MAX_RECORDS).unwrap_or(i64::MAX)
        ],
        |row| row.get::<_, String>(0),
    )?;
    let component_ids = rows.collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    let mut outcome = RuntimeHealthProbeReconciliationOutcome {
        examined: component_ids.len(),
        settled_inconclusive: 0,
        repaired_stranded_health: 0,
        retired_orphan_leases: 0,
        skipped_generation_mismatches: 0,
        remaining: false,
    };
    for component_id in component_ids {
        let Some(lease) = load_health_probe_lease_tx(connection, component_id.as_str())? else {
            continue;
        };
        let health = load_component_health_tx(connection, component_id.as_str())?;
        let begin = load_runtime_health_probe_begin_tx(connection, lease.lease_id.as_str())?;
        match (health, begin) {
            (Some(mut health), Some(begin))
                if begin.lease == lease
                    && health.generation == lease.expected_generation
                    && health.authority_class == lease.authority_class
                    && health.state == RuntimeHealthState::Probing =>
            {
                let completed_at_unix_ms = now_unix_ms
                    .max(lease.issued_at_unix_ms)
                    .min(lease.expires_at_unix_ms.saturating_sub(1));
                let result = HealthProbeResult {
                    schema_version:
                        palyra_common::runtime_contracts::HEALTH_PROBE_RESULT_SCHEMA_VERSION,
                    component_id: lease.component_id.clone(),
                    disposition: HealthProbeDisposition::Inconclusive,
                    reason_code: match mode {
                        RuntimeHealthProbeReconciliationMode::Startup => {
                            "runtime.health.probe_orphaned_restart".to_owned()
                        }
                        RuntimeHealthProbeReconciliationMode::Periodic => {
                            "runtime.health.probe_expired".to_owned()
                        }
                    },
                    mutation_attempted: false,
                    completed_at_unix_ms,
                };
                health.state = RuntimeHealthState::Quarantined;
                health.reason_code = result.reason_code.clone();
                health.security_quarantine = begin.security_quarantine_before;
                health.strike_count = health.strike_count.max(1);
                health.first_failure_at_unix_ms =
                    health.first_failure_at_unix_ms.or(Some(completed_at_unix_ms));
                health.last_failure_at_unix_ms = Some(completed_at_unix_ms);
                health.expires_at_unix_ms = None;
                health.updated_at_unix_ms = completed_at_unix_ms;
                let result_health_json =
                    serialize_runtime_health_contract(&health, "health probe resulting health")?;
                persist_component_health_update_tx(
                    connection,
                    RuntimeHealthState::Probing,
                    &health,
                )?;
                insert_runtime_health_probe_terminal_evidence_tx(
                    connection,
                    &lease,
                    begin.source_state,
                    begin.security_quarantine_before,
                    &result,
                    RuntimeHealthState::Quarantined,
                    begin.security_quarantine_before,
                    true,
                    "reconciliation",
                    None,
                    Some(result_health_json.as_str()),
                    None,
                    now_unix_ms.max(completed_at_unix_ms),
                )?;
                delete_exact_supported_health_probe_lease_tx(connection, &lease)?;
                outcome.settled_inconclusive = outcome.settled_inconclusive.saturating_add(1);
            }
            (Some(mut health), _) if health.generation == lease.expected_generation => {
                if health.state == RuntimeHealthState::Probing {
                    let security_after = true;
                    health.state = RuntimeHealthState::Quarantined;
                    health.security_quarantine = security_after;
                    health.reason_code = "runtime.health.probe_authority_inconsistent".to_owned();
                    health.strike_count = health.strike_count.max(1);
                    health.first_failure_at_unix_ms =
                        health.first_failure_at_unix_ms.or(Some(now_unix_ms));
                    health.last_failure_at_unix_ms = Some(now_unix_ms);
                    health.expires_at_unix_ms = None;
                    health.updated_at_unix_ms = now_unix_ms;
                    persist_component_health_update_tx(
                        connection,
                        RuntimeHealthState::Probing,
                        &health,
                    )?;
                    outcome.repaired_stranded_health =
                        outcome.repaired_stranded_health.saturating_add(1);
                }
                delete_exact_supported_health_probe_lease_tx(connection, &lease)?;
                outcome.retired_orphan_leases = outcome.retired_orphan_leases.saturating_add(1);
            }
            (Some(_), _) => {
                delete_exact_supported_health_probe_lease_tx(connection, &lease)?;
                outcome.skipped_generation_mismatches =
                    outcome.skipped_generation_mismatches.saturating_add(1);
                outcome.retired_orphan_leases = outcome.retired_orphan_leases.saturating_add(1);
            }
            (None, _) => {
                delete_exact_supported_health_probe_lease_tx(connection, &lease)?;
                outcome.retired_orphan_leases = outcome.retired_orphan_leases.saturating_add(1);
            }
        }
    }
    let remaining_predicate = match mode {
        RuntimeHealthProbeReconciliationMode::Startup => "schema_version IN (1, 2)",
        RuntimeHealthProbeReconciliationMode::Periodic => {
            "schema_version IN (1, 2) AND expires_at_unix_ms <= ?1"
        }
    };
    let remaining_sql =
        format!("SELECT COUNT(*) FROM runtime_health_probe_leases WHERE {remaining_predicate}");
    let remaining: i64 = match mode {
        RuntimeHealthProbeReconciliationMode::Startup => {
            connection.query_row(remaining_sql.as_str(), [], |row| row.get(0))?
        }
        RuntimeHealthProbeReconciliationMode::Periodic => {
            connection.query_row(remaining_sql.as_str(), params![now_unix_ms], |row| row.get(0))?
        }
    };
    outcome.remaining = remaining > 0;
    Ok(outcome)
}

fn delete_exact_supported_health_probe_lease_tx(
    connection: &Connection,
    lease: &HealthProbeLeaseV1,
) -> Result<(), JournalError> {
    let deleted = connection.execute(
        r#"
            DELETE FROM runtime_health_probe_leases
            WHERE lease_ulid = ?1 AND component_ulid = ?2
              AND expected_generation = ?3 AND schema_version IN (1, 2)
        "#,
        params![
            lease.lease_id.as_str(),
            lease.component_id.as_str(),
            runtime_generation_sql(lease.expected_generation)?,
        ],
    )?;
    if deleted != 1 {
        return Err(JournalError::InvalidArgument(
            "runtime health reconciliation lost exact probe lease".to_owned(),
        ));
    }
    Ok(())
}

fn load_component_health_tx(
    connection: &Connection,
    component_id: &str,
) -> Result<Option<RuntimeComponentHealthV1>, JournalError> {
    let row = connection
        .query_row(
            r#"
                SELECT component_ulid, generation, state, reason_code, health_json,
                       updated_at_unix_ms
                FROM runtime_component_health
                WHERE component_ulid = ?1
            "#,
            params![component_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((
        scalar_component,
        scalar_generation,
        scalar_state,
        scalar_reason,
        json,
        scalar_updated,
    )) = row
    else {
        return Ok(None);
    };
    let health = serde_json::from_str::<RuntimeComponentHealthV1>(json.as_str())?;
    health.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let exact = health.component_id.as_str() == scalar_component
        && runtime_generation_sql(health.generation)? == scalar_generation
        && health.state.as_str() == scalar_state
        && health.reason_code == scalar_reason
        && health.updated_at_unix_ms == scalar_updated;
    if !exact {
        return Err(JournalError::InvalidArgument(
            "runtime component health scalar evidence does not match its JSON contract".to_owned(),
        ));
    }
    Ok(Some(health))
}

fn load_health_probe_lease_tx(
    connection: &Connection,
    component_id: &str,
) -> Result<Option<HealthProbeLeaseV1>, JournalError> {
    let lease = connection
        .query_row(
            "SELECT lease_json FROM runtime_health_probe_leases WHERE component_ulid = ?1",
            params![component_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str::<HealthProbeLeaseV1>(json.as_str()))
        .transpose()?;
    if let Some(lease) = lease.as_ref() {
        lease.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    }
    Ok(lease)
}

fn record_runtime_health_observation_tx(
    connection: &Connection,
    request: &RuntimeHealthObservationRequest,
) -> Result<RuntimeHealthObservationOutcome, JournalError> {
    let mut health = load_component_health_tx(connection, request.component_id.as_str())?
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "runtime health observation requires an active component".to_owned(),
            )
        })?;
    if health.generation != request.expected_generation {
        return Err(JournalError::InvalidArgument(
            "runtime health observation was rejected for a stale component generation".to_owned(),
        ));
    }
    if !matches!(health.state, RuntimeHealthState::Healthy | RuntimeHealthState::Degraded) {
        return Err(JournalError::InvalidArgument(
            "runtime health observation cannot mutate protected component posture".to_owned(),
        ));
    }
    let expected_state = health.state;
    let observed_at_unix_ms = request.observed_at_unix_ms.max(health.updated_at_unix_ms);
    if request.succeeded {
        health.state = RuntimeHealthState::Healthy;
        health.strike_count = 0;
        health.first_failure_at_unix_ms = None;
        health.last_failure_at_unix_ms = None;
        health.expires_at_unix_ms = None;
    } else {
        health.strike_count = health.strike_count.saturating_add(1);
        health.first_failure_at_unix_ms =
            health.first_failure_at_unix_ms.or(Some(observed_at_unix_ms));
        health.last_failure_at_unix_ms = Some(observed_at_unix_ms);
        if health.strike_count >= health.policy.strike_threshold {
            health.state = RuntimeHealthState::Cooldown;
            let cooldown_ms = i64::try_from(health.policy.cooldown_ms).unwrap_or(i64::MAX);
            health.expires_at_unix_ms = Some(observed_at_unix_ms.saturating_add(cooldown_ms));
        } else {
            health.state = RuntimeHealthState::Degraded;
            health.expires_at_unix_ms = None;
        }
    }
    health.reason_code.clone_from(&request.reason_code);
    health.updated_at_unix_ms = observed_at_unix_ms;
    persist_component_health_update_tx(connection, expected_state, &health)?;
    Ok(RuntimeHealthObservationOutcome { health })
}

fn persist_component_health_update_tx(
    connection: &Connection,
    expected_state: RuntimeHealthState,
    health: &RuntimeComponentHealthV1,
) -> Result<(), JournalError> {
    health.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let raw = serde_json::to_vec(health)?;
    let (json, _) = sanitize_payload(raw.as_slice())?;
    let updated = connection.execute(
        r#"
            UPDATE runtime_component_health
            SET generation = ?2, state = ?3, reason_code = ?4,
                health_json = ?5, updated_at_unix_ms = ?6
            WHERE component_ulid = ?1 AND generation = ?2 AND state = ?7
        "#,
        params![
            health.component_id.as_str(),
            runtime_generation_sql(health.generation)?,
            health.state.as_str(),
            health.reason_code,
            json,
            health.updated_at_unix_ms,
            expected_state.as_str(),
        ],
    )?;
    if updated != 1 {
        return Err(JournalError::InvalidArgument(
            "runtime component health changed concurrently".to_owned(),
        ));
    }
    connection.execute(
        r#"
            INSERT INTO runtime_component_health_events (
                event_ulid, component_ulid, from_state, to_state, reason_code,
                created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        "#,
        params![
            Ulid::new().to_string(),
            health.component_id.as_str(),
            expected_state.as_str(),
            health.state.as_str(),
            health.reason_code,
            health.updated_at_unix_ms,
        ],
    )?;
    Ok(())
}

fn validate_tool_effect_observation_evidence(
    request: &super::ToolEffectObservationCommitRequest,
    scoped: &ScopedSideEffectFence,
    tape_event: &OrchestratorTapeAppendRequest,
    runtime_event: Option<&RuntimeEventAppendRequest>,
) -> Result<(), JournalError> {
    if tape_event.run_id != scoped.run_id {
        return Err(JournalError::ToolSideEffectFencePrecondition {
            operation_id: request.operation_id.as_str().to_owned(),
            reason: "tool result evidence does not match the fenced run".to_owned(),
        });
    }
    if let Some(runtime_event) = runtime_event {
        if runtime_event.envelope.identities.session_id.as_str() != scoped.session_id
            || runtime_event.envelope.identities.run_id.as_str() != scoped.run_id
            || runtime_event.envelope.identities.generation != request.generation
            || runtime_event.envelope.identities.operation_id.as_ref()
                != Some(&scoped.fence.operation_id)
            || runtime_event.envelope.identities.tool_execution_id.as_ref()
                != Some(&scoped.fence.tool_execution_id)
        {
            return Err(JournalError::ToolSideEffectFencePrecondition {
                operation_id: request.operation_id.as_str().to_owned(),
                reason: "tool result event does not match the fenced execution".to_owned(),
            });
        }
    }
    Ok(())
}

fn authorize_side_effect_generation_tx(
    connection: &Connection,
    session_id: &str,
    run_id: &str,
    generation: RuntimeGeneration,
    now_unix_ms: i64,
) -> Result<(), JournalError> {
    let lease = load_generation_tx(connection, session_id, RuntimeGenerationLane::Run)?
        .filter(|lease| now_unix_ms < lease.expires_at_unix_ms)
        .ok_or_else(|| {
            JournalError::InvalidArgument(
                "side-effect mutation requires an active runtime generation".to_owned(),
            )
        })?;
    if lease.generation != generation
        || lease.run_id.as_ref().is_none_or(|active_run_id| active_run_id.as_str() != run_id)
    {
        return Err(JournalError::InvalidArgument(
            "side-effect mutation was rejected for a stale runtime generation".to_owned(),
        ));
    }
    Ok(())
}

fn validate_new_side_effect_fence(fence: &SideEffectFenceV1) -> Result<(), JournalError> {
    fence.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    if fence.state != SideEffectFenceState::IntentRecorded {
        return Err(JournalError::InvalidArgument(
            "new side-effect fence must start at intent_recorded".to_owned(),
        ));
    }
    Ok(())
}

fn insert_side_effect_fence_tx(
    connection: &Connection,
    session_id: &str,
    run_id: &str,
    fence: &SideEffectFenceV1,
) -> Result<(), JournalError> {
    let raw = serde_json::to_vec(fence)?;
    let (json, _) = sanitize_payload(raw.as_slice())?;
    connection.execute(
        r#"
            INSERT INTO runtime_side_effect_fences (
                operation_ulid, tool_execution_ulid, session_ulid, run_ulid,
                intent_generation, observed_generation, state, intent_sha256,
                fence_json, updated_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        "#,
        params![
            fence.operation_id.as_str(),
            fence.tool_execution_id.as_str(),
            session_id,
            run_id,
            i64::try_from(fence.intent_generation.get()).unwrap_or(i64::MAX),
            i64::try_from(fence.observed_generation.get()).unwrap_or(i64::MAX),
            fence.state.as_str(),
            fence.intent_sha256,
            json,
            fence.updated_at_unix_ms,
        ],
    )?;
    append_fence_event_tx(connection, None, fence)
}

struct ScopedSideEffectFence {
    session_id: String,
    run_id: String,
    fence: SideEffectFenceV1,
}

fn validate_matching_side_effect_intent(
    session_id: &str,
    run_id: &str,
    incoming: &SideEffectFenceV1,
    existing: &ScopedSideEffectFence,
) -> Result<(), JournalError> {
    if existing.session_id != session_id || existing.run_id != run_id {
        return Err(JournalError::InvalidArgument(
            "side-effect fence operation is bound to a different session or run".to_owned(),
        ));
    }
    if existing.fence.tool_execution_id != incoming.tool_execution_id {
        return Err(JournalError::InvalidArgument(
            "side-effect fence operation is bound to a different tool execution".to_owned(),
        ));
    }
    if existing.fence.intent_sha256 != incoming.intent_sha256 {
        return Err(JournalError::InvalidArgument(
            "side-effect fence operation is bound to a different normalized intent".to_owned(),
        ));
    }
    if existing.fence.semantics != incoming.semantics {
        return Err(JournalError::InvalidArgument(
            "side-effect fence operation is bound to different tool execution semantics".to_owned(),
        ));
    }
    if existing.fence.external_idempotency_key_sha256 != incoming.external_idempotency_key_sha256 {
        return Err(JournalError::InvalidArgument(
            "side-effect fence operation is bound to a different external idempotency key"
                .to_owned(),
        ));
    }
    Ok(())
}

fn append_fence_event_tx(
    connection: &Connection,
    from: Option<SideEffectFenceState>,
    fence: &SideEffectFenceV1,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO runtime_side_effect_fence_events (
                event_ulid, operation_ulid, from_state, to_state, generation,
                reason_code, evidence_sha256, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#,
        params![
            Ulid::new().to_string(),
            fence.operation_id.as_str(),
            from.map(|state| state.as_str()),
            fence.state.as_str(),
            i64::try_from(fence.observed_generation.get()).unwrap_or(i64::MAX),
            fence.reason_code,
            fence.evidence_sha256,
            fence.updated_at_unix_ms,
        ],
    )?;
    Ok(())
}

fn validate_side_effect_cleanup_outcome(
    request: &SideEffectFenceCleanupOutcomeRequest,
) -> Result<(), JournalError> {
    if request.operation_id.trim().is_empty()
        || request.operation_id.len() > 256
        || request.reason_code.trim().is_empty()
        || request.reason_code.len() > 128
        || request.evidence_sha256.as_deref().is_some_and(|value| !is_sha256_hex(value))
        || (request.outcome_observed && request.evidence_sha256.is_none())
    {
        return Err(JournalError::InvalidArgument(
            "side-effect cleanup outcome request is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_operator_side_effect_resolution(
    request: &SideEffectFenceOperatorResolutionRequest,
) -> Result<(), JournalError> {
    if request.operation_id.trim().is_empty()
        || request.operation_id.len() > 256
        || request.reason_code.trim().is_empty()
        || request.reason_code.len() > 128
        || !is_sha256_hex(request.expected_intent_sha256.as_str())
        || !is_sha256_hex(request.evidence_sha256.as_str())
        || !is_sha256_hex(request.actor_id_sha256.as_str())
        || !matches!(
            request.resolution,
            SideEffectFenceState::Reconciled | SideEffectFenceState::Abandoned
        )
    {
        return Err(JournalError::InvalidArgument(
            "operator side-effect resolution request is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn append_side_effect_runtime_event_tx(
    connection: &Connection,
    max_payload_bytes: usize,
    session_id: &str,
    run_id: &str,
    fence: &SideEffectFenceV1,
    now_unix_ms: i64,
) -> Result<(u64, bool), JournalError> {
    let event_name = side_effect_runtime_event_name(fence.state).ok_or_else(|| {
        JournalError::InvalidArgument(format!(
            "side-effect fence state {} has no execution-owned runtime event",
            fence.state.as_str()
        ))
    })?;
    let descriptor = event_name.descriptor();
    let mut identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse(run_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        RuntimeSessionId::parse(session_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        RuntimeRunId::parse(run_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        fence.observed_generation,
    );
    identities.tool_execution_id = Some(fence.tool_execution_id.clone());
    identities.operation_id = Some(fence.operation_id.clone());
    let request = RuntimeEventAppendRequest {
        lane: RuntimeGenerationLane::Run,
        envelope: RuntimeEventEnvelopeV2 {
            schema_version: 2,
            event_id: RuntimeEventId::parse(format!("tool_effect:{}", Ulid::new()).as_str())
                .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
            identities,
            sequence: 0,
            causal_parent_event_id: None,
            subsystem: descriptor.subsystem,
            phase: descriptor.phase,
            event_name,
            reason_code: fence.reason_code.clone(),
            actor_kind: descriptor.actor_kind,
            retryability: descriptor.retryability,
            redaction_class: descriptor.redaction_class,
            terminal: descriptor.terminal,
            payload: RuntimeEventPayloadRef::Inline {
                metadata: serde_json::json!({
                    "state": fence.state.as_str(),
                    "intent_sha256": fence.intent_sha256,
                    "evidence_sha256": fence.evidence_sha256,
                }),
            },
            occurred_at_unix_ms: fence.updated_at_unix_ms,
            extensions: BTreeMap::new(),
        },
    };
    match append_runtime_event_tx(connection, max_payload_bytes, &request, now_unix_ms)? {
        RuntimeEventAppendOutcome::Appended { sequence } => {
            Ok((sequence, runtime_event_metadata_trace_was_persisted_tx(connection, &request)?))
        }
        RuntimeEventAppendOutcome::AlreadyAppended { sequence } => Ok((sequence, false)),
        RuntimeEventAppendOutcome::StaleSuppressed => Err(JournalError::InvalidArgument(
            "side-effect fence lost generation authority before runtime event persistence"
                .to_owned(),
        )),
    }
}

fn runtime_event_metadata_trace_was_persisted_tx(
    connection: &Connection,
    request: &RuntimeEventAppendRequest,
) -> Result<bool, JournalError> {
    let event_id_sha256 = super::metadata_trace::hash_identifier(
        request.envelope.identities.run_id.as_str(),
        palyra_common::metadata_trace::MetadataTraceIdDomainV1::Event,
        request.envelope.event_id.as_str(),
    )?;
    Ok(connection.query_row(
        r#"
            SELECT EXISTS(
                SELECT 1
                FROM metadata_trace_events
                WHERE run_ulid = ?1
                  AND event_id_sha256 = ?2
            )
        "#,
        params![request.envelope.identities.run_id.as_str(), event_id_sha256],
        |row| row.get::<_, i64>(0),
    )? != 0)
}

#[expect(
    clippy::too_many_arguments,
    reason = "cleanup evidence must bind the exact durable scope, event classification, and timestamp"
)]
fn append_host_side_effect_cleanup_runtime_event_tx(
    connection: &Connection,
    max_payload_bytes: usize,
    session_id: &str,
    run_id: &str,
    fence: &SideEffectFenceV1,
    event_name: RuntimeEventName,
    reason_code: &str,
    evidence_sha256: Option<&str>,
    now_unix_ms: i64,
) -> Result<u64, JournalError> {
    if !matches!(
        event_name,
        RuntimeEventName::ToolEffectCleanupReconciled | RuntimeEventName::ToolEffectCleanupUnknown
    ) {
        return Err(JournalError::InvalidArgument(
            "host side-effect cleanup event name is invalid".to_owned(),
        ));
    }
    let mut identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse(run_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        RuntimeSessionId::parse(session_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        RuntimeRunId::parse(run_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        fence.observed_generation,
    );
    identities.tool_execution_id = Some(fence.tool_execution_id.clone());
    identities.operation_id = Some(fence.operation_id.clone());
    let event_binding = format!("{}:{}", fence.operation_id.as_str(), event_name.as_str());
    let request = RuntimeEventAppendRequest {
        lane: RuntimeGenerationLane::Run,
        envelope: RuntimeEventEnvelopeV2 {
            schema_version: 2,
            event_id: RuntimeEventId::parse(
                format!("tool_effect_cleanup:{}", sha256_hex(event_binding.as_bytes())).as_str(),
            )
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
            identities,
            sequence: 0,
            causal_parent_event_id: None,
            subsystem: RuntimeSubsystem::Tool,
            phase: RuntimeErrorPhase::Recovery,
            event_name,
            reason_code: reason_code.to_owned(),
            actor_kind: RuntimeEventActorKind::Host,
            retryability: if event_name == RuntimeEventName::ToolEffectCleanupUnknown {
                RuntimeRetryability::RequiresOperatorReview
            } else {
                RuntimeRetryability::NotRetryable
            },
            redaction_class: RuntimeEventRedactionClass::HashOnly,
            terminal: false,
            payload: RuntimeEventPayloadRef::Inline {
                metadata: serde_json::json!({
                    "state": fence.state.as_str(),
                    "intent_sha256": fence.intent_sha256,
                    "evidence_sha256": evidence_sha256,
                    "cleanup_outcome_observed": event_name
                        == RuntimeEventName::ToolEffectCleanupReconciled,
                }),
            },
            occurred_at_unix_ms: now_unix_ms,
            extensions: BTreeMap::new(),
        },
    };
    validate_runtime_event_append_request(&request)?;
    match persist_validated_runtime_event_tx(connection, max_payload_bytes, &request, now_unix_ms)?
    {
        RuntimeEventAppendOutcome::Appended { sequence }
        | RuntimeEventAppendOutcome::AlreadyAppended { sequence } => Ok(sequence),
        RuntimeEventAppendOutcome::StaleSuppressed => Err(JournalError::InvalidArgument(
            "host side-effect cleanup event persistence returned stale unexpectedly".to_owned(),
        )),
    }
}

fn append_operator_side_effect_runtime_event_tx(
    connection: &Connection,
    max_payload_bytes: usize,
    session_id: &str,
    run_id: &str,
    fence: &SideEffectFenceV1,
    actor_id_sha256: &str,
    now_unix_ms: i64,
) -> Result<u64, JournalError> {
    let event_name = match fence.state {
        SideEffectFenceState::Reconciled => RuntimeEventName::ToolEffectReconciled,
        SideEffectFenceState::Abandoned => RuntimeEventName::ToolEffectAbandoned,
        _ => {
            return Err(JournalError::InvalidArgument(
                "operator side-effect event requires a terminal resolution".to_owned(),
            ));
        }
    };
    let mut identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse(run_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        RuntimeSessionId::parse(session_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        RuntimeRunId::parse(run_id)
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        fence.observed_generation,
    );
    identities.tool_execution_id = Some(fence.tool_execution_id.clone());
    identities.operation_id = Some(fence.operation_id.clone());
    let sequence = next_runtime_event_sequence_tx(
        connection,
        session_id,
        RuntimeGenerationLane::Run,
        fence.observed_generation,
    )?;
    let envelope = RuntimeEventEnvelopeV2 {
        schema_version: 2,
        event_id: RuntimeEventId::parse(format!("tool_effect_resolution:{}", Ulid::new()).as_str())
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        identities,
        sequence,
        causal_parent_event_id: None,
        subsystem: RuntimeSubsystem::Tool,
        phase: RuntimeErrorPhase::Recovery,
        event_name,
        reason_code: fence.reason_code.clone(),
        actor_kind: RuntimeEventActorKind::Operator,
        retryability: RuntimeRetryability::NotRetryable,
        redaction_class: RuntimeEventRedactionClass::HashOnly,
        terminal: false,
        payload: RuntimeEventPayloadRef::Inline {
            metadata: serde_json::json!({
                "state": fence.state.as_str(),
                "intent_sha256": fence.intent_sha256,
                "evidence_sha256": fence.evidence_sha256,
                "actor_id_sha256": actor_id_sha256,
            }),
        },
        occurred_at_unix_ms: fence.updated_at_unix_ms,
        extensions: BTreeMap::new(),
    };
    envelope.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
    let raw = serde_json::to_vec(&envelope)?;
    if raw.len() > max_payload_bytes {
        return Err(JournalError::PayloadTooLarge {
            payload_kind: "runtime_event_v2",
            actual_bytes: raw.len(),
            max_bytes: max_payload_bytes,
        });
    }
    let (envelope_json, _) = sanitize_payload(raw.as_slice())?;
    connection.execute(
        r#"
            INSERT INTO runtime_events_v2 (
                event_ulid, session_ulid, run_ulid, lane, generation, sequence,
                terminal, event_name, reason_code, envelope_json, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10)
        "#,
        params![
            envelope.event_id.as_str(),
            session_id,
            run_id,
            RuntimeGenerationLane::Run.as_str(),
            i64::try_from(fence.observed_generation.get()).unwrap_or(i64::MAX),
            i64::try_from(sequence).map_err(|_| {
                JournalError::InvalidArgument(
                    "runtime event sequence exceeds sqlite integer range".to_owned(),
                )
            })?,
            envelope.event_name.as_str(),
            envelope.reason_code,
            envelope_json,
            now_unix_ms,
        ],
    )?;
    Ok(sequence)
}

const fn side_effect_runtime_event_name(state: SideEffectFenceState) -> Option<RuntimeEventName> {
    match state {
        SideEffectFenceState::IntentRecorded => Some(RuntimeEventName::ToolIntentRecorded),
        SideEffectFenceState::EffectStarted => Some(RuntimeEventName::ToolEffectStarted),
        SideEffectFenceState::EffectObserved => Some(RuntimeEventName::ToolEffectObserved),
        SideEffectFenceState::EffectUnknown => Some(RuntimeEventName::ToolEffectUnknown),
        SideEffectFenceState::Reconciled => Some(RuntimeEventName::ToolEffectReceiptReconciled),
        SideEffectFenceState::Abandoned => None,
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
fn load_fence_tx(
    connection: &Connection,
    operation_id: &str,
) -> Result<Option<SideEffectFenceV1>, JournalError> {
    Ok(load_scoped_fence_tx(connection, operation_id)?.map(|record| record.fence))
}

fn load_scoped_fence_tx(
    connection: &Connection,
    operation_id: &str,
) -> Result<Option<ScopedSideEffectFence>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT session_ulid, run_ulid, fence_json
                FROM runtime_side_effect_fences
                WHERE operation_ulid = ?1
            "#,
            params![operation_id],
            |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
            },
        )
        .optional()?
        .map(|(session_id, run_id, json)| {
            Ok(ScopedSideEffectFence {
                session_id,
                run_id,
                fence: serde_json::from_str(json.as_str())?,
            })
        })
        .transpose()
}

fn persist_quarantine_finding(
    connection: &Connection,
    finding: &RuntimeStateCompatibilityFinding,
    schema_version: u32,
    created_at_unix_ms: i64,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT OR IGNORE INTO runtime_state_quarantine (
                quarantine_ulid, contract_name, record_ref_sha256, observed_schema_version,
                supported_schema_version, outcome, reason_code, blocks_admission,
                payload_bytes, created_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        "#,
        params![
            Ulid::new().to_string(),
            finding.contract,
            finding.record_ref_sha256,
            finding.observed_schema_version.map(i64::from),
            i64::from(finding.supported_schema_version),
            finding.outcome.as_str(),
            finding.reason_code,
            i64::from(finding.blocks_admission),
            i64::try_from(finding.payload_bytes).unwrap_or(i64::MAX),
            created_at_unix_ms,
            i64::from(schema_version),
        ],
    )?;
    Ok(())
}

#[derive(Default)]
struct RuntimeCompatibilityFindingCollector {
    findings: Vec<RuntimeStateCompatibilityFinding>,
    truncated: bool,
}

impl RuntimeCompatibilityFindingCollector {
    fn push(&mut self, finding: RuntimeStateCompatibilityFinding) {
        if self.findings.iter().any(|existing| {
            existing.contract == finding.contract
                && existing.record_ref_sha256 == finding.record_ref_sha256
                && existing.outcome == finding.outcome
        }) {
            return;
        }
        let retained_limit = MAX_RUNTIME_COMPATIBILITY_FINDINGS.saturating_sub(1);
        if retained_limit == 0 {
            self.truncated = true;
            return;
        }
        let insertion_index = self
            .findings
            .binary_search_by(|candidate| runtime_compatibility_finding_cmp(candidate, &finding))
            .unwrap_or_else(|index| index);
        if self.findings.len() < retained_limit {
            self.findings.insert(insertion_index, finding);
            return;
        }
        self.truncated = true;
        if insertion_index < retained_limit {
            self.findings.insert(insertion_index, finding);
            self.findings.pop();
        }
    }

    fn into_findings(mut self) -> Vec<RuntimeStateCompatibilityFinding> {
        if self.truncated {
            self.findings.push(runtime_compatibility_truncation_finding());
        }
        self.findings
    }
}

fn runtime_compatibility_finding_cmp(
    left: &RuntimeStateCompatibilityFinding,
    right: &RuntimeStateCompatibilityFinding,
) -> std::cmp::Ordering {
    left.contract
        .cmp(&right.contract)
        .then_with(|| left.record_ref_sha256.cmp(&right.record_ref_sha256))
        .then_with(|| left.reason_code.cmp(&right.reason_code))
}

fn push_runtime_compatibility_finding(
    findings: &mut RuntimeCompatibilityFindingCollector,
    finding: RuntimeStateCompatibilityFinding,
) {
    findings.push(finding);
}

fn runtime_compatibility_truncation_finding() -> RuntimeStateCompatibilityFinding {
    RuntimeStateCompatibilityFinding {
        contract: "runtime_state_compatibility_report".to_owned(),
        record_ref_sha256: sha256_hex(b"runtime_state_compatibility_report:truncated"),
        observed_schema_version: None,
        supported_schema_version: 1,
        outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
        reason_code: "runtime.compatibility.findings_truncated".to_owned(),
        blocks_admission: true,
        payload_bytes: u64::try_from(MAX_RUNTIME_COMPATIBILITY_FINDINGS).unwrap_or(u64::MAX),
    }
}

#[cfg(test)]
mod compatibility_collector_tests {
    use super::*;

    fn finding(contract: &str, record_ref: &str) -> RuntimeStateCompatibilityFinding {
        RuntimeStateCompatibilityFinding {
            contract: contract.to_owned(),
            record_ref_sha256: sha256_hex(record_ref.as_bytes()),
            observed_schema_version: None,
            supported_schema_version: 1,
            outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
            reason_code: "runtime.compatibility.invalid_contract".to_owned(),
            blocks_admission: true,
            payload_bytes: 0,
        }
    }

    #[test]
    fn bounded_finding_collector_retains_same_prefix_for_any_scan_order() {
        let total = MAX_RUNTIME_COMPATIBILITY_FINDINGS + 64;
        let ascending = (0..total)
            .map(|index| finding("runtime_component_health", format!("record-{index:04}").as_str()))
            .collect::<Vec<_>>();
        let mut forward = RuntimeCompatibilityFindingCollector::default();
        for finding in ascending.iter().cloned() {
            forward.push(finding);
        }
        let mut reverse = RuntimeCompatibilityFindingCollector::default();
        for finding in ascending.iter().rev().cloned() {
            reverse.push(finding);
        }

        let forward = forward.into_findings();
        let reverse = reverse.into_findings();
        assert_eq!(forward, reverse);
        assert_eq!(forward.len(), MAX_RUNTIME_COMPATIBILITY_FINDINGS);
        assert_eq!(
            forward.last().map(|finding| finding.reason_code.as_str()),
            Some("runtime.compatibility.findings_truncated")
        );
    }
}

fn scan_bounded_runtime_table(
    connection: &Connection,
    table: &str,
    max_entries: usize,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<bool, JournalError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let entry_count: i64 = connection.query_row(sql.as_str(), [], |row| row.get(0))?;
    let current_entries = usize::try_from(entry_count).unwrap_or(usize::MAX);
    if current_entries <= max_entries {
        return Ok(true);
    }
    push_runtime_compatibility_finding(
        findings,
        RuntimeStateCompatibilityFinding {
            contract: table.to_owned(),
            record_ref_sha256: sha256_hex(format!("{table}:capacity").as_bytes()),
            observed_schema_version: None,
            supported_schema_version: 1,
            outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
            reason_code: "runtime.compatibility.capacity_exceeded".to_owned(),
            blocks_admission: true,
            payload_bytes: u64::try_from(current_entries).unwrap_or(u64::MAX),
        },
    );
    Ok(false)
}

fn scan_versioned_table(
    connection: &Connection,
    table: &str,
    id_column: &str,
    version_column: &str,
    supported_schema_version: u32,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let sql = format!("SELECT {id_column}, {version_column} FROM {table}");
    let mut statement = connection.prepare(sql.as_str())?;
    let rows =
        statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))?;
    for row in rows {
        let (record_id, observed_raw) = row?;
        let digest = sha256_hex(record_id.as_bytes());
        let observed = u32::try_from(observed_raw).ok();
        if observed.is_none_or(|version| version == 0 || version > supported_schema_version) {
            push_runtime_compatibility_finding(
                findings,
                RuntimeStateCompatibilityFinding {
                    contract: table.to_owned(),
                    record_ref_sha256: digest,
                    observed_schema_version: observed,
                    supported_schema_version,
                    outcome: RuntimeStateCompatibilityOutcome::BlockedNewerSchema,
                    reason_code: "runtime.compatibility.unsupported_schema".to_owned(),
                    blocks_admission: true,
                    payload_bytes: 0,
                },
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum RuntimeJsonContract {
    EventEnvelope,
    SideEffectFence,
    CancellationContext,
    ComponentHealth,
    HealthProbeLease,
    HandleDescriptor,
    ProcessProvenance,
    CleanupReport,
    WorkerLifecycleEvent,
}

impl RuntimeJsonContract {
    const fn has_embedded_schema_version(self) -> bool {
        !matches!(self, Self::ProcessProvenance | Self::WorkerLifecycleEvent)
    }

    fn validate(self, json: &str) -> Result<(), JournalError> {
        match self {
            Self::EventEnvelope => serde_json::from_str::<RuntimeEventEnvelopeV2>(json)
                .map_err(Into::into)
                .and_then(|record| {
                    record
                        .validate()
                        .map_err(|error| JournalError::InvalidArgument(error.to_string()))
                }),
            Self::SideEffectFence => serde_json::from_str::<SideEffectFenceV1>(json)
                .map_err(Into::into)
                .and_then(|record| {
                    record
                        .validate()
                        .map_err(|error| JournalError::InvalidArgument(error.to_string()))
                }),
            Self::CancellationContext => serde_json::from_str::<CancellationContextV1>(json)
                .map_err(Into::into)
                .and_then(|record| {
                    record
                        .validate()
                        .map_err(|error| JournalError::InvalidArgument(error.to_string()))?;
                    if record.scope != CancellationScopeKind::ChildTask
                        || record.parent_scope_id.is_none()
                    {
                        return Err(JournalError::InvalidArgument(
                            "background task cancellation context must be a parented ChildTask scope"
                                .to_owned(),
                        ));
                    }
                    Ok(())
                }),
            Self::ComponentHealth => serde_json::from_str::<RuntimeComponentHealthV1>(json)
                .map_err(Into::into)
                .and_then(|record| {
                    record
                        .validate()
                        .map_err(|error| JournalError::InvalidArgument(error.to_string()))
                }),
            Self::HealthProbeLease => serde_json::from_str::<HealthProbeLeaseV1>(json)
                .map_err(Into::into)
                .and_then(|record| {
                    record
                        .validate()
                        .map_err(|error| JournalError::InvalidArgument(error.to_string()))
                }),
            Self::HandleDescriptor => serde_json::from_str::<RuntimeHandleDescriptorV1>(json)
                .map_err(Into::into)
                .and_then(|record| {
                    record
                        .validate()
                        .map_err(|error| JournalError::InvalidArgument(error.to_string()))
                }),
            Self::ProcessProvenance => serde_json::from_str::<
                palyra_common::runtime_contracts::ProcessProvenance,
            >(json)
            .map_err(Into::into)
            .and_then(|record| {
                record.validate().map_err(|error| JournalError::InvalidArgument(error.to_string()))
            }),
            Self::CleanupReport => serde_json::from_str::<CleanupReportV1>(json)
                .map_err(Into::into)
                .and_then(|record| {
                    record
                        .validate()
                        .map_err(|error| JournalError::InvalidArgument(error.to_string()))
                }),
            Self::WorkerLifecycleEvent => {
                let record = serde_json::from_str::<palyra_workerd::WorkerLifecycleEvent>(json)?;
                if !palyra_workerd::is_exact_networked_worker_expiry_event(&record) {
                    return Err(JournalError::InvalidArgument(
                        "networked worker expiry outbox event is invalid".to_owned(),
                    ));
                }
                Ok(())
            }
        }
    }
}

fn scan_json_table(
    connection: &Connection,
    table: &str,
    id_column: &str,
    json_column: &str,
    supported_schema_version: u32,
    contract: RuntimeJsonContract,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    scan_json_table_where(
        connection,
        table,
        id_column,
        json_column,
        supported_schema_version,
        contract,
        None,
        findings,
    )
}

struct BackgroundTaskAuthorityContractRow {
    record_id: String,
    task_kind_raw: String,
    state_raw: String,
    delegation_json: Option<String>,
    cancellation_json: Option<String>,
    child_session_id: Option<String>,
    last_error: Option<String>,
    result_json: Option<String>,
    parent_session_id: String,
    parent_run_id: Option<String>,
    owner_principal: String,
    device_id: String,
    channel: Option<String>,
    child_record_id: Option<String>,
    child_principal: Option<String>,
    child_device_id: Option<String>,
    child_channel: Option<String>,
    child_branch_state: Option<String>,
    child_parent_session_id: Option<String>,
    child_branch_origin_run_id: Option<String>,
}

fn scan_background_task_cancellation_contracts(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    const SUPPORTED_SCHEMA_VERSION: u32 = 1;

    let mut statement = connection.prepare(
        r#"
            SELECT
                tasks.task_ulid,
                tasks.task_kind,
                tasks.state,
                tasks.delegation_json,
                tasks.cancellation_context_json,
                tasks.child_session_ulid,
                tasks.last_error,
                tasks.result_json,
                tasks.session_ulid,
                tasks.parent_run_ulid,
                tasks.owner_principal,
                tasks.device_id,
                tasks.channel,
                children.session_ulid,
                children.principal,
                children.device_id,
                children.channel,
                children.branch_state,
                children.parent_session_ulid,
                children.branch_origin_run_ulid
            FROM orchestrator_background_tasks AS tasks
            LEFT JOIN orchestrator_sessions AS children
                ON children.session_ulid = tasks.child_session_ulid
            WHERE LOWER(TRIM(tasks.task_kind)) = 'delegation_prompt'
               OR tasks.delegation_json IS NOT NULL
               OR tasks.cancellation_context_json IS NOT NULL
               OR tasks.child_session_ulid IS NOT NULL
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok(BackgroundTaskAuthorityContractRow {
            record_id: row.get(0)?,
            task_kind_raw: row.get(1)?,
            state_raw: row.get(2)?,
            delegation_json: row.get(3)?,
            cancellation_json: row.get(4)?,
            child_session_id: row.get(5)?,
            last_error: row.get(6)?,
            result_json: row.get(7)?,
            parent_session_id: row.get(8)?,
            parent_run_id: row.get(9)?,
            owner_principal: row.get(10)?,
            device_id: row.get(11)?,
            channel: row.get(12)?,
            child_record_id: row.get(13)?,
            child_principal: row.get(14)?,
            child_device_id: row.get(15)?,
            child_channel: row.get(16)?,
            child_branch_state: row.get(17)?,
            child_parent_session_id: row.get(18)?,
            child_branch_origin_run_id: row.get(19)?,
        })
    })?;
    for row in rows {
        let row = row?;
        let digest = sha256_hex(row.record_id.as_bytes());
        let task_kind = AuxiliaryTaskKind::from_str(row.task_kind_raw.as_str());
        let task_state = AuxiliaryTaskState::from_str(row.state_raw.as_str());
        let is_delegation = task_kind == Some(AuxiliaryTaskKind::DelegationPrompt);
        let delegation_valid = row
            .delegation_json
            .as_deref()
            .is_some_and(|json| serde_json::from_str::<DelegationSnapshot>(json).is_ok());
        let observed = row.cancellation_json.as_ref().and_then(|json| {
            serde_json::from_str::<serde_json::Value>(json)
                .ok()
                .and_then(|value| value.get("schema_version").and_then(serde_json::Value::as_u64))
                .and_then(|version| u32::try_from(version).ok())
        });
        if observed.is_some_and(|version| version > SUPPORTED_SCHEMA_VERSION) {
            push_runtime_compatibility_finding(
                findings,
                RuntimeStateCompatibilityFinding {
                    contract: "orchestrator_background_tasks".to_owned(),
                    record_ref_sha256: digest,
                    observed_schema_version: observed,
                    supported_schema_version: SUPPORTED_SCHEMA_VERSION,
                    outcome: RuntimeStateCompatibilityOutcome::BlockedNewerSchema,
                    reason_code: "runtime.compatibility.newer_schema".to_owned(),
                    blocks_admission: true,
                    payload_bytes: row
                        .cancellation_json
                        .as_ref()
                        .map_or(0, |json| u64::try_from(json.len()).unwrap_or(u64::MAX)),
                },
            );
            continue;
        }
        let cancellation_valid = row.cancellation_json.as_deref().is_some_and(|json| {
            observed == Some(SUPPORTED_SCHEMA_VERSION)
                && RuntimeJsonContract::CancellationContext.validate(json).is_ok()
        });
        let child_session_identity_valid = row.child_session_id.as_deref().is_some_and(|value| {
            RuntimeSessionId::parse(value).is_ok() && value != row.parent_session_id
        });
        let child_lineage_valid = row.child_session_id.as_deref().is_some_and(|child_session_id| {
            row.child_record_id.as_deref() == Some(child_session_id)
                && row.child_principal.as_deref() == Some(row.owner_principal.as_str())
                && row.child_device_id.as_deref() == Some(row.device_id.as_str())
                && row.child_channel == row.channel
                && row.child_branch_state.as_deref() == Some("delegated")
                && row.child_parent_session_id.as_deref() == Some(row.parent_session_id.as_str())
                && row.child_branch_origin_run_id.as_deref() == row.parent_run_id.as_deref()
        });
        let legacy_missing_cancellation_terminalized = is_delegation
            && task_state == Some(AuxiliaryTaskState::Failed)
            && delegation_valid
            && row.cancellation_json.is_none()
            && row.last_error.as_deref()
                == Some("legacy delegation is missing durable ChildTask cancellation authority")
            && row.result_json.as_deref().is_some_and(|json| {
                serde_json::from_str::<serde_json::Value>(json).ok().is_some_and(|value| {
                    value.get("status").and_then(serde_json::Value::as_str) == Some("failed")
                        && value.get("task_id").and_then(serde_json::Value::as_str)
                            == Some(row.record_id.as_str())
                        && value.get("reason").and_then(serde_json::Value::as_str)
                            == Some("legacy_missing_child_task_context")
                })
            });
        let legacy_missing_child_session_terminalized = is_delegation
            && task_state == Some(AuxiliaryTaskState::Failed)
            && delegation_valid
            && row.child_session_id.is_none()
            && row.last_error.as_deref()
                == Some("legacy delegation is missing dedicated child-session authority")
            && row.result_json.as_deref().is_some_and(|json| {
                serde_json::from_str::<serde_json::Value>(json).ok().is_some_and(|value| {
                    value.get("status").and_then(serde_json::Value::as_str) == Some("failed")
                        && value.get("task_id").and_then(serde_json::Value::as_str)
                            == Some(row.record_id.as_str())
                        && value.get("reason").and_then(serde_json::Value::as_str)
                            == Some("legacy_missing_child_session")
                })
            });
        let historical_terminal_legacy_delegation = is_delegation
            && task_state.is_some_and(AuxiliaryTaskState::is_terminal)
            && delegation_valid
            && (row.cancellation_json.is_none() || row.child_session_id.is_none());
        let valid = task_kind.is_some()
            && task_state.is_some()
            && is_delegation == row.delegation_json.is_some()
            && (!is_delegation || delegation_valid)
            && (legacy_missing_cancellation_terminalized
                || legacy_missing_child_session_terminalized
                || historical_terminal_legacy_delegation
                || (is_delegation == row.cancellation_json.is_some()
                    && is_delegation == row.child_session_id.is_some()
                    && (!is_delegation
                        || (cancellation_valid
                            && child_session_identity_valid
                            && child_lineage_valid))));
        if !valid {
            let payload_bytes = row
                .delegation_json
                .as_ref()
                .map_or(0_u64, |json| u64::try_from(json.len()).unwrap_or(u64::MAX))
                .saturating_add(
                    row.cancellation_json
                        .as_ref()
                        .map_or(0_u64, |json| u64::try_from(json.len()).unwrap_or(u64::MAX)),
                )
                .saturating_add(
                    row.child_session_id
                        .as_ref()
                        .map_or(0_u64, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                );
            push_runtime_compatibility_finding(
                findings,
                RuntimeStateCompatibilityFinding {
                    contract: "orchestrator_background_tasks".to_owned(),
                    record_ref_sha256: digest,
                    observed_schema_version: observed,
                    supported_schema_version: SUPPORTED_SCHEMA_VERSION,
                    outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                    reason_code: "runtime.compatibility.invalid_contract".to_owned(),
                    blocks_admission: true,
                    payload_bytes,
                },
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_json_table_where(
    connection: &Connection,
    table: &str,
    id_column: &str,
    json_column: &str,
    supported_schema_version: u32,
    contract: RuntimeJsonContract,
    predicate: Option<String>,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let mut sql = format!("SELECT {id_column}, {json_column} FROM {table}");
    if let Some(predicate) = predicate {
        sql.push_str(" WHERE ");
        sql.push_str(predicate.as_str());
    }
    let mut statement = connection.prepare(sql.as_str())?;
    let rows =
        statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
    for row in rows {
        let (record_id, json) = row?;
        let digest = sha256_hex(record_id.as_bytes());
        match serde_json::from_str::<serde_json::Value>(json.as_str()) {
            Ok(value) => {
                let observed = value
                    .get("schema_version")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|version| u32::try_from(version).ok());
                if observed.is_some_and(|version| version > supported_schema_version) {
                    push_runtime_compatibility_finding(
                        findings,
                        RuntimeStateCompatibilityFinding {
                            contract: table.to_owned(),
                            record_ref_sha256: digest,
                            observed_schema_version: observed,
                            supported_schema_version,
                            outcome: RuntimeStateCompatibilityOutcome::BlockedNewerSchema,
                            reason_code: "runtime.compatibility.newer_schema".to_owned(),
                            blocks_admission: true,
                            payload_bytes: u64::try_from(json.len()).unwrap_or(u64::MAX),
                        },
                    );
                } else if (contract.has_embedded_schema_version()
                    && observed != Some(supported_schema_version))
                    || contract.validate(json.as_str()).is_err()
                {
                    push_runtime_compatibility_finding(
                        findings,
                        RuntimeStateCompatibilityFinding {
                            contract: table.to_owned(),
                            record_ref_sha256: digest,
                            observed_schema_version: observed,
                            supported_schema_version,
                            outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                            reason_code: "runtime.compatibility.invalid_contract".to_owned(),
                            blocks_admission: true,
                            payload_bytes: u64::try_from(json.len()).unwrap_or(u64::MAX),
                        },
                    );
                }
            }
            Err(_) => push_runtime_compatibility_finding(
                findings,
                RuntimeStateCompatibilityFinding {
                    contract: table.to_owned(),
                    record_ref_sha256: digest,
                    observed_schema_version: None,
                    supported_schema_version,
                    outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                    reason_code: "runtime.compatibility.corrupt_quarantined".to_owned(),
                    blocks_admission: true,
                    payload_bytes: u64::try_from(json.len()).unwrap_or(u64::MAX),
                },
            ),
        }
    }
    Ok(())
}

fn scan_runtime_component_generation_heads(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT heads.component_ulid, heads.last_generation, heads.updated_at_unix_ms,
                   heads.schema_version, health.generation
            FROM runtime_component_generation_heads heads
            LEFT JOIN runtime_component_health health
              ON health.component_ulid = heads.component_ulid
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Option<i64>>(4)?,
        ))
    })?;
    for row in rows {
        let (component_id, generation, updated_at, schema_version, health_generation) = row?;
        if schema_version == i64::from(RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION)
            && (generation <= 0
                || updated_at < 0
                || health_generation.is_some_and(|health| generation < health))
        {
            push_invalid_exact_evidence_finding(
                findings,
                "runtime_component_generation_heads",
                component_id.as_str(),
                Some(RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION),
                RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION,
                0,
            );
        }
    }
    let missing_head: i64 = connection.query_row(
        r#"
            SELECT COUNT(*)
            FROM runtime_component_health health
            LEFT JOIN runtime_component_generation_heads heads
              ON heads.component_ulid = health.component_ulid
            WHERE heads.component_ulid IS NULL
        "#,
        [],
        |row| row.get(0),
    )?;
    if missing_head > 0 {
        push_invalid_exact_evidence_finding(
            findings,
            "runtime_component_generation_heads",
            "missing-health-head",
            Some(RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION),
            RUNTIME_HEALTH_LIFECYCLE_SCHEMA_VERSION,
            u64::try_from(missing_head).unwrap_or(u64::MAX),
        );
    }
    Ok(())
}

fn scan_runtime_health_probe_active_exact_evidence(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT lease_ulid, component_ulid, expected_generation, authority_class,
                   lease_json, issued_at_unix_ms, expires_at_unix_ms, schema_version
            FROM runtime_health_probe_leases
            WHERE schema_version IN (1, 2)
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;
    for row in rows {
        let (lease_id, component_id, generation, authority, json, issued_at, expires_at, schema) =
            row?;
        let valid =
            serde_json::from_str::<HealthProbeLeaseV1>(json.as_str()).ok().is_some_and(|lease| {
                lease.validate().is_ok()
                    && lease.lease_id.as_str() == lease_id
                    && lease.component_id.as_str() == component_id
                    && runtime_generation_sql(lease.expected_generation).ok() == Some(generation)
                    && lease.authority_class.as_str() == authority
                    && lease.issued_at_unix_ms == issued_at
                    && lease.expires_at_unix_ms == expires_at
                    && matches!(schema, 1 | 2)
            });
        if !valid {
            push_invalid_exact_evidence_finding(
                findings,
                "runtime_health_probe_leases",
                lease_id.as_str(),
                u32::try_from(schema).ok(),
                RUNTIME_HEALTH_PROBE_ACTIVE_ROW_SCHEMA_VERSION,
                u64::try_from(json.len()).unwrap_or(u64::MAX),
            );
        }
    }
    Ok(())
}

fn scan_runtime_component_health_exact_evidence(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT component_ulid, generation, state, reason_code, health_json,
                   updated_at_unix_ms
            FROM runtime_component_health
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    for row in rows {
        let (component_id, generation, state, reason, json, updated_at) = row?;
        let observed_schema = serde_json::from_str::<serde_json::Value>(json.as_str())
            .ok()
            .and_then(|value| value.get("schema_version").and_then(serde_json::Value::as_u64))
            .and_then(|version| u32::try_from(version).ok());
        if observed_schema.is_some_and(|version| version > 1) {
            continue;
        }
        let valid = serde_json::from_str::<RuntimeComponentHealthV1>(json.as_str())
            .ok()
            .is_some_and(|health| {
                health.validate().is_ok()
                    && health.component_id.as_str() == component_id
                    && runtime_generation_sql(health.generation).ok() == Some(generation)
                    && health.state.as_str() == state
                    && health.reason_code == reason
                    && health.updated_at_unix_ms == updated_at
            });
        if !valid {
            push_invalid_exact_evidence_finding(
                findings,
                "runtime_component_health",
                component_id.as_str(),
                observed_schema,
                palyra_common::runtime_contracts::RUNTIME_COMPONENT_HEALTH_SCHEMA_VERSION,
                u64::try_from(json.len()).unwrap_or(u64::MAX),
            );
        }
    }
    Ok(())
}

fn scan_runtime_health_probe_begin_exact_evidence(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT lease_ulid, component_ulid, expected_generation, authority_class,
                   source_state, security_quarantine_before, reason_code,
                   authorization_evidence_sha256, authorized_actor_id_sha256,
                   lease_json, begun_at_unix_ms, schema_version
            FROM runtime_health_probe_begins
            WHERE schema_version IN (1, 2)
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Option<String>>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, String>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, i64>(11)?,
        ))
    })?;
    for row in rows {
        let (
            lease_id,
            component_id,
            generation,
            authority,
            source_state,
            security_before,
            reason,
            authorization_evidence,
            authorized_actor,
            lease_json,
            begun_at,
            schema_version,
        ) = row?;
        let valid = matches!(
            RuntimeHealthState::from_str(source_state.as_str()),
            Some(RuntimeHealthState::Cooldown | RuntimeHealthState::Quarantined)
        ) && matches!(security_before, 0 | 1)
            && validate_reason_code(reason.as_str()).is_ok()
            && validate_optional_health_evidence_sha256(authorization_evidence.as_deref()).is_ok()
            && validate_optional_health_evidence_sha256(authorized_actor.as_deref()).is_ok()
            && (schema_version == 2 || authorized_actor.is_none())
            && serde_json::from_str::<HealthProbeLeaseV1>(lease_json.as_str()).ok().is_some_and(
                |lease| {
                    lease.validate().is_ok()
                        && lease.lease_id.as_str() == lease_id
                        && lease.component_id.as_str() == component_id
                        && runtime_generation_sql(lease.expected_generation).ok()
                            == Some(generation)
                        && lease.authority_class.as_str() == authority
                        && lease.issued_at_unix_ms == begun_at
                },
            );
        if !valid {
            push_invalid_exact_evidence_finding(
                findings,
                "runtime_health_probe_begins",
                lease_id.as_str(),
                u32::try_from(schema_version).ok(),
                RUNTIME_HEALTH_PROBE_BEGIN_ROW_SCHEMA_VERSION,
                u64::try_from(lease_json.len()).unwrap_or(u64::MAX),
            );
        }
    }
    Ok(())
}

fn scan_runtime_health_probe_terminal_exact_evidence(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT terminal.lease_ulid, terminal.component_ulid,
                   terminal.expected_generation, terminal.authority_class,
                   terminal.source_state, terminal.result_state, terminal.disposition,
                   terminal.mutation_attempted, terminal.security_quarantine_before,
                   terminal.security_quarantine_after, terminal.health_mutated,
                   terminal.terminal_kind, terminal.reason_code, terminal.settlement_json,
                   terminal.result_health_json, terminal.probe_evidence_sha256,
                   terminal.completed_at_unix_ms, terminal.settled_at_unix_ms,
                   terminal.schema_version, begins.lease_json, begins.source_state,
                   begins.security_quarantine_before
            FROM runtime_health_probe_terminal_evidence terminal
            LEFT JOIN runtime_health_probe_begins begins
              ON begins.lease_ulid = terminal.lease_ulid
            WHERE terminal.schema_version IN (1, 2)
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i64>(8)?,
            row.get::<_, i64>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, String>(11)?,
            row.get::<_, String>(12)?,
            row.get::<_, Option<String>>(13)?,
            row.get::<_, Option<String>>(14)?,
            row.get::<_, Option<String>>(15)?,
            row.get::<_, i64>(16)?,
            row.get::<_, i64>(17)?,
            row.get::<_, i64>(18)?,
            row.get::<_, Option<String>>(19)?,
            row.get::<_, Option<String>>(20)?,
            row.get::<_, Option<i64>>(21)?,
        ))
    })?;
    for row in rows {
        let (
            lease_id,
            component_id,
            generation,
            authority,
            source_state,
            result_state,
            disposition,
            mutation_attempted,
            security_before,
            security_after,
            health_mutated,
            terminal_kind,
            reason,
            settlement_json,
            result_health_json,
            probe_evidence,
            completed_at,
            settled_at,
            schema_version,
            begin_lease_json,
            begin_source_state,
            begin_security_before,
        ) = row?;
        let disposition = HealthProbeDisposition::from_str(disposition.as_str());
        let expected_mutation = disposition == Some(HealthProbeDisposition::DeniedMutatingProbe);
        let settlement = settlement_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<HealthProbeSettlementV1>(json).ok());
        let result_health = result_health_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<RuntimeComponentHealthV1>(json).ok());
        let valid =
            begin_lease_json.as_deref().is_some_and(|json| {
                serde_json::from_str::<HealthProbeLeaseV1>(json).ok().is_some_and(|lease| {
                    lease.validate().is_ok()
                        && lease.lease_id.as_str() == lease_id
                        && lease.component_id.as_str() == component_id
                        && runtime_generation_sql(lease.expected_generation).ok()
                            == Some(generation)
                        && lease.authority_class.as_str() == authority
                })
            }) && begin_source_state.as_deref() == Some(source_state.as_str())
                && begin_security_before == Some(security_before)
                && matches!(
                    RuntimeHealthState::from_str(source_state.as_str()),
                    Some(RuntimeHealthState::Cooldown | RuntimeHealthState::Quarantined)
                )
                && matches!(
                    RuntimeHealthState::from_str(result_state.as_str()),
                    Some(RuntimeHealthState::Healthy | RuntimeHealthState::Quarantined)
                )
                && disposition.is_some()
                && (mutation_attempted != 0) == expected_mutation
                && matches!(security_before, 0 | 1)
                && matches!(security_after, 0 | 1)
                && matches!(health_mutated, 0 | 1)
                && matches!(terminal_kind.as_str(), "settlement" | "reconciliation")
                && validate_reason_code(reason.as_str()).is_ok()
                && validate_optional_health_evidence_sha256(probe_evidence.as_deref()).is_ok()
                && completed_at >= 0
                && settled_at >= completed_at
                && match schema_version {
                    1 => result_health_json.is_none(),
                    2 => result_health.as_ref().is_some_and(|health| {
                        health.validate().is_ok()
                            && health.component_id.as_str() == component_id
                            && runtime_generation_sql(health.generation).ok() == Some(generation)
                            && health.authority_class.as_str() == authority
                            && health.state.as_str() == result_state
                            && health.reason_code == reason
                            && health.security_quarantine == (security_after != 0)
                            && (health.state != RuntimeHealthState::Healthy
                                || (health.strike_count == 0
                                    && health.first_failure_at_unix_ms.is_none()
                                    && health.last_failure_at_unix_ms.is_none()))
                            && (health.state != RuntimeHealthState::Quarantined
                                || (health.strike_count >= 1
                                    && health.last_failure_at_unix_ms == Some(completed_at)))
                            && health.updated_at_unix_ms == completed_at
                    }),
                    _ => false,
                }
                && match (schema_version, terminal_kind.as_str(), settlement.as_ref()) {
                    (2, "settlement", Some(settlement)) => settlement.schema_version
                        == palyra_common::runtime_contracts::HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION
                        && health_mutated == 1
                        && settlement.lease_id.as_str() == lease_id
                        && runtime_generation_sql(settlement.expected_generation).ok()
                            == Some(generation)
                        && settlement.result.component_id.as_str() == component_id
                        && Some(settlement.result.disposition) == disposition
                        && settlement.result.mutation_attempted == (mutation_attempted != 0)
                        && settlement.result.reason_code == reason
                        && settlement.result.completed_at_unix_ms == completed_at
                        && settlement.result.validate().is_ok()
                        && disposition.is_some_and(|disposition| {
                            terminal_posture_matches(
                                disposition,
                                security_before != 0,
                                result_state.as_str(),
                                security_after != 0,
                            )
                        }),
                    (2, "reconciliation", None) => {
                        health_mutated == 1
                            && disposition == Some(HealthProbeDisposition::Inconclusive)
                            && mutation_attempted == 0
                            && probe_evidence.is_none()
                            && result_state == RuntimeHealthState::Quarantined.as_str()
                    }
                    (1, "settlement", Some(settlement)) => settlement.schema_version
                        == palyra_common::runtime_contracts::HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION
                        && health_mutated == 1
                        && settlement.lease_id.as_str() == lease_id
                        && runtime_generation_sql(settlement.expected_generation).ok()
                            == Some(generation)
                        && settlement.result.component_id.as_str() == component_id
                        && Some(settlement.result.disposition) == disposition
                        && settlement.result.mutation_attempted == (mutation_attempted != 0)
                        && settlement.result.reason_code == reason
                        && settlement.result.completed_at_unix_ms == completed_at
                        && settlement.result.validate().is_ok()
                        && disposition.is_some_and(|disposition| {
                            terminal_posture_matches(
                                disposition,
                                security_before != 0,
                                result_state.as_str(),
                                security_after != 0,
                            )
                        }),
                    (1, "reconciliation", None) => {
                        health_mutated == 1
                            && disposition == Some(HealthProbeDisposition::Inconclusive)
                            && mutation_attempted == 0
                            && probe_evidence.is_none()
                            && result_state == RuntimeHealthState::Quarantined.as_str()
                    }
                    _ => false,
                };
        if !valid {
            push_invalid_exact_evidence_finding(
                findings,
                "runtime_health_probe_terminal_evidence",
                lease_id.as_str(),
                u32::try_from(schema_version).ok(),
                RUNTIME_HEALTH_PROBE_TERMINAL_ROW_SCHEMA_VERSION,
                settlement_json
                    .as_ref()
                    .into_iter()
                    .chain(result_health_json.as_ref())
                    .map(|json| u64::try_from(json.len()).unwrap_or(u64::MAX))
                    .fold(0_u64, u64::saturating_add),
            );
        }
    }
    Ok(())
}

fn terminal_posture_matches(
    disposition: HealthProbeDisposition,
    security_before: bool,
    result_state: &str,
    security_after: bool,
) -> bool {
    match disposition {
        HealthProbeDisposition::Passed => {
            result_state == RuntimeHealthState::Healthy.as_str() && !security_after
        }
        HealthProbeDisposition::Failed | HealthProbeDisposition::Inconclusive => {
            result_state == RuntimeHealthState::Quarantined.as_str()
                && security_after == security_before
        }
        HealthProbeDisposition::DeniedMutatingProbe => {
            result_state == RuntimeHealthState::Quarantined.as_str() && security_after
        }
    }
}

fn push_invalid_exact_evidence_finding(
    findings: &mut RuntimeCompatibilityFindingCollector,
    contract: &str,
    record_id: &str,
    observed_schema_version: Option<u32>,
    supported_schema_version: u32,
    payload_bytes: u64,
) {
    push_runtime_compatibility_finding(
        findings,
        RuntimeStateCompatibilityFinding {
            contract: contract.to_owned(),
            record_ref_sha256: sha256_hex(record_id.as_bytes()),
            observed_schema_version,
            supported_schema_version,
            outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
            reason_code: "runtime.compatibility.invalid_exact_evidence".to_owned(),
            blocks_admission: true,
            payload_bytes,
        },
    );
}

fn scan_networked_worker_expiry_outbox_exact_evidence(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT event_ulid, worker_id, run_ulid, lease_ulid, event_json,
                   created_at_unix_ms, schema_version
            FROM runtime_networked_worker_expiry_outbox
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
        ))
    })?;
    for row in rows {
        let (event_id, worker_id, run_id, lease_id, event_json, created_at, schema_version) = row?;
        let valid = schema_version == 1
            && serde_json::from_str::<palyra_workerd::WorkerLifecycleEvent>(event_json.as_str())
                .ok()
                .is_some_and(|event| {
                    let record =
                        NetworkedWorkerExpiryOutboxRecord { event_id: event_id.clone(), event };
                    record.event.worker_id == worker_id
                        && record.event.run_id.as_deref() == Some(run_id.as_str())
                        && record.event.lease_id.as_deref() == Some(lease_id.as_str())
                        && record.event.timestamp_unix_ms == created_at
                        && record.validate().is_ok()
                });
        if schema_version == 1 && !valid {
            push_runtime_compatibility_finding(
                findings,
                RuntimeStateCompatibilityFinding {
                    contract: "runtime_networked_worker_expiry_outbox".to_owned(),
                    record_ref_sha256: sha256_hex(event_id.as_bytes()),
                    observed_schema_version: Some(1),
                    supported_schema_version: 1,
                    outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                    reason_code: "runtime.compatibility.invalid_exact_evidence".to_owned(),
                    blocks_admission: true,
                    payload_bytes: u64::try_from(event_json.len()).unwrap_or(u64::MAX),
                },
            );
        }
    }
    Ok(())
}

fn scan_networked_worker_fleet_exact_evidence(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT worker_id, record_json, schema_version
            FROM runtime_networked_worker_fleet
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?))
    })?;
    for row in rows {
        let (worker_id, record_json, schema_version) = row?;
        let valid = schema_version == 1
            && serde_json::from_str::<palyra_workerd::WorkerFleetRecord>(record_json.as_str())
                .ok()
                .is_some_and(|record| {
                    let records = std::collections::BTreeMap::from([(worker_id.clone(), record)]);
                    palyra_workerd::WorkerFleetManager::from_durable_records(records).is_ok()
                });
        if schema_version == 1 && !valid {
            push_runtime_compatibility_finding(
                findings,
                RuntimeStateCompatibilityFinding {
                    contract: "runtime_networked_worker_fleet".to_owned(),
                    record_ref_sha256: sha256_hex(worker_id.as_bytes()),
                    observed_schema_version: Some(1),
                    supported_schema_version: 1,
                    outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                    reason_code: "runtime.compatibility.invalid_exact_evidence".to_owned(),
                    blocks_admission: true,
                    payload_bytes: u64::try_from(record_json.len()).unwrap_or(u64::MAX),
                },
            );
        }
    }
    Ok(())
}

fn scan_networked_worker_dispatch_claim_exact_evidence(
    connection: &Connection,
    contract: &str,
    select_sql: &str,
    require_terminal: bool,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let sql =
        format!("{select_sql} WHERE schema_version IN (1, 2, 3) ORDER BY remote_request_ulid ASC");
    let mut statement = connection.prepare(sql.as_str())?;
    let rows = statement.query_map([], hydrate_networked_worker_dispatch_claim)?;
    let location = if require_terminal {
        NetworkedWorkerDispatchClaimEvidenceLocation::TerminalArchive
    } else {
        NetworkedWorkerDispatchClaimEvidenceLocation::Active
    };
    for row in rows {
        match row {
            Ok(claim)
                if validate_networked_worker_dispatch_claim_evidence(&claim, location).is_ok() => {}
            Ok(claim) => push_runtime_compatibility_finding(
                findings,
                RuntimeStateCompatibilityFinding {
                    contract: contract.to_owned(),
                    record_ref_sha256: sha256_hex(claim.remote_request_id.as_bytes()),
                    observed_schema_version: Some(claim.schema_version),
                    supported_schema_version: NETWORKED_WORKER_DISPATCH_CLAIM_SCHEMA_VERSION,
                    outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                    reason_code: "runtime.compatibility.invalid_exact_evidence".to_owned(),
                    blocks_admission: true,
                    payload_bytes: 0,
                },
            ),
            Err(error) => push_runtime_compatibility_finding(
                findings,
                RuntimeStateCompatibilityFinding {
                    contract: contract.to_owned(),
                    record_ref_sha256: sha256_hex(error.to_string().as_bytes()),
                    observed_schema_version: None,
                    supported_schema_version: NETWORKED_WORKER_DISPATCH_CLAIM_SCHEMA_VERSION,
                    outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                    reason_code: "runtime.compatibility.invalid_exact_evidence".to_owned(),
                    blocks_admission: true,
                    payload_bytes: 0,
                },
            ),
        }
    }
    Ok(())
}

fn scan_networked_worker_dispatch_claim_cross_table_conflicts(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let conflicts = connection.query_row(
        r#"
            SELECT COUNT(*)
            FROM runtime_networked_worker_dispatch_claims active
            JOIN runtime_networked_worker_dispatch_claim_terminal_evidence archived
              ON active.remote_request_ulid = archived.remote_request_ulid
              OR active.node_request_ulid = archived.node_request_ulid
        "#,
        [],
        |row| row.get::<_, i64>(0),
    )?;
    if conflicts > 0 {
        push_runtime_compatibility_finding(
            findings,
            RuntimeStateCompatibilityFinding {
                contract: "runtime_networked_worker_dispatch_claim_identity".to_owned(),
                record_ref_sha256: sha256_hex(b"active-terminal-cross-table-conflict"),
                observed_schema_version: Some(1),
                supported_schema_version: 1,
                outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                reason_code: "runtime.compatibility.identity_conflict".to_owned(),
                blocks_admission: true,
                payload_bytes: u64::try_from(conflicts).unwrap_or(u64::MAX),
            },
        );
    }
    Ok(())
}

fn scan_networked_worker_fleet_generation(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let rows = connection.query_row(
        r#"
            SELECT COUNT(*), MIN(singleton_key), MAX(singleton_key),
                   MIN(generation), MIN(updated_at_unix_ms), MIN(schema_version)
            FROM runtime_networked_worker_fleet_meta
        "#,
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ))
        },
    )?;
    let valid = matches!(
        &rows,
        (1, Some(1), Some(1), Some(generation), Some(updated_at), Some(1))
            if *generation >= 0 && *updated_at >= 0
    );
    if !valid {
        push_runtime_compatibility_finding(
            findings,
            RuntimeStateCompatibilityFinding {
                contract: "runtime_networked_worker_fleet_meta".to_owned(),
                record_ref_sha256: sha256_hex(b"singleton:1"),
                observed_schema_version: rows.5.and_then(|version| u32::try_from(version).ok()),
                supported_schema_version: 1,
                outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                reason_code: "runtime.compatibility.invalid_generation_metadata".to_owned(),
                blocks_admission: true,
                payload_bytes: u64::try_from(rows.0.max(0)).unwrap_or(u64::MAX),
            },
        );
    }
    Ok(())
}

fn scan_cleanup_steps(
    connection: &Connection,
    findings: &mut RuntimeCompatibilityFindingCollector,
) -> Result<(), JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT report_ulid, ordinal, step, disposition, reason_code,
                   evidence_sha256, created_at_unix_ms
            FROM runtime_cleanup_steps
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, i64>(6)?,
        ))
    })?;
    for row in rows {
        let (report_id, ordinal, step, disposition, reason_code, evidence_sha256, completed_at) =
            row?;
        let record_ref = format!("{report_id}:{ordinal}");
        let valid = u32::try_from(ordinal).ok().is_some()
            && palyra_common::runtime_contracts::CleanupStepKind::from_str(step.as_str()).is_some()
            && palyra_common::runtime_contracts::CleanupStepDisposition::from_str(
                disposition.as_str(),
            )
            .is_some()
            && !reason_code.trim().is_empty()
            && completed_at >= 0
            && evidence_sha256.as_deref().is_none_or(|digest| {
                digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            });
        if !valid {
            push_runtime_compatibility_finding(
                findings,
                RuntimeStateCompatibilityFinding {
                    contract: "runtime_cleanup_steps".to_owned(),
                    record_ref_sha256: sha256_hex(record_ref.as_bytes()),
                    observed_schema_version: None,
                    supported_schema_version: 1,
                    outcome: RuntimeStateCompatibilityOutcome::QuarantinedCorrupt,
                    reason_code: "runtime.compatibility.invalid_cleanup_step".to_owned(),
                    blocks_admission: true,
                    payload_bytes: 0,
                },
            );
        }
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn validate_cleanup_report_replay_tx(
    connection: &Connection,
    report: &CleanupReportV1,
    report_json: &str,
) -> Result<bool, JournalError> {
    let existing = connection
        .query_row(
            r#"
                SELECT
                    instance_ulid, lease_ulid, outcome, reason_code,
                    report_json, created_at_unix_ms, schema_version
                FROM runtime_cleanup_reports
                WHERE report_ulid = ?1
            "#,
            params![report.report_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        instance_id,
        lease_id,
        outcome,
        reason_code,
        stored_report_json,
        created_at_unix_ms,
        schema_version,
    )) = existing
    else {
        let orphan_step_exists = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_cleanup_steps WHERE report_ulid = ?1)",
            params![report.report_id],
            |row| row.get::<_, i64>(0),
        )? != 0;
        if orphan_step_exists {
            return Err(JournalError::InvalidArgument(
                "cleanup report steps exist without their parent report".to_owned(),
            ));
        }
        return Ok(false);
    };
    let report_matches = instance_id == report.instance_id.as_str()
        && lease_id.as_deref() == report.lease_id.as_ref().map(|value| value.as_str())
        && outcome == report.outcome.as_str()
        && reason_code == report.reason_code
        && stored_report_json == report_json
        && created_at_unix_ms == report.completed_at_unix_ms
        && schema_version == i64::from(report.schema_version);
    if !report_matches {
        return Err(JournalError::InvalidArgument(
            "cleanup report id is already bound to conflicting durable evidence".to_owned(),
        ));
    }

    let mut statement = connection.prepare(
        r#"
            SELECT
                ordinal, step, disposition, reason_code,
                evidence_sha256, created_at_unix_ms
            FROM runtime_cleanup_steps
            WHERE report_ulid = ?1
            ORDER BY ordinal ASC
        "#,
    )?;
    let stored_steps = statement
        .query_map(params![report.report_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if stored_steps.len() != report.steps.len()
        || stored_steps.iter().zip(&report.steps).any(
            |(
                (ordinal, step, disposition, reason_code, evidence_sha256, created_at_unix_ms),
                expected,
            )| {
                *ordinal != i64::from(expected.ordinal)
                    || step != expected.step.as_str()
                    || disposition != expected.disposition.as_str()
                    || reason_code != &expected.reason_code
                    || evidence_sha256.as_deref() != expected.evidence_sha256.as_deref()
                    || *created_at_unix_ms != expected.completed_at_unix_ms
            },
        )
    {
        return Err(JournalError::InvalidArgument(
            "cleanup report replay has conflicting ordered step evidence".to_owned(),
        ));
    }
    Ok(true)
}

fn insert_cleanup_report_tx(
    connection: &Connection,
    report: &CleanupReportV1,
    report_json: &str,
) -> Result<(), JournalError> {
    connection.execute(
        r#"
            INSERT INTO runtime_cleanup_reports (
                report_ulid, instance_ulid, lease_ulid, outcome, reason_code,
                report_json, created_at_unix_ms, schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        "#,
        params![
            report.report_id,
            report.instance_id.as_str(),
            report.lease_id.as_ref().map(|value| value.as_str()),
            report.outcome.as_str(),
            report.reason_code,
            report_json,
            report.completed_at_unix_ms,
            i64::from(report.schema_version),
        ],
    )?;
    for step in &report.steps {
        connection.execute(
            r#"
                INSERT INTO runtime_cleanup_steps (
                    report_ulid, ordinal, step, disposition, reason_code,
                    evidence_sha256, created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                report.report_id,
                i64::from(step.ordinal),
                step.step.as_str(),
                step.disposition.as_str(),
                step.reason_code,
                step.evidence_sha256,
                step.completed_at_unix_ms,
            ],
        )?;
    }
    Ok(())
}

fn append_cleanup_runtime_event_tx(
    connection: &Connection,
    max_payload_bytes: usize,
    descriptor: &RuntimeHandleDescriptorV1,
    report: &CleanupReportV1,
) -> Result<(), JournalError> {
    let (Some(session_id), Some(run_id)) = (&descriptor.session_id, &descriptor.run_id) else {
        return Ok(());
    };
    let event_name = match report.outcome {
        CleanupOutcome::Completed => RuntimeEventName::CleanupCompleted,
        CleanupOutcome::Partial => RuntimeEventName::CleanupPartial,
        CleanupOutcome::Unknown => RuntimeEventName::CleanupUnknown,
    };
    let event_descriptor = event_name.descriptor();
    let mut identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse(run_id.as_str())
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
        session_id.clone(),
        run_id.clone(),
        descriptor.generation,
    );
    identities.runtime_instance_id = Some(descriptor.instance_id.clone());
    let event_binding = format!("{}:{}", report.report_id, event_name.as_str());
    let request = RuntimeEventAppendRequest {
        lane: RuntimeGenerationLane::Process,
        envelope: RuntimeEventEnvelopeV2 {
            schema_version: 2,
            event_id: RuntimeEventId::parse(
                format!("cleanup:{}", sha256_hex(event_binding.as_bytes())).as_str(),
            )
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))?,
            identities,
            sequence: 0,
            causal_parent_event_id: None,
            subsystem: event_descriptor.subsystem,
            phase: event_descriptor.phase,
            event_name,
            reason_code: report.reason_code.clone(),
            actor_kind: event_descriptor.actor_kind,
            retryability: event_descriptor.retryability,
            redaction_class: event_descriptor.redaction_class,
            terminal: event_descriptor.terminal,
            payload: RuntimeEventPayloadRef::Inline {
                metadata: serde_json::json!({
                    "instance_id_sha256": sha256_hex(descriptor.instance_id.as_str().as_bytes()),
                    "report_id_sha256": sha256_hex(report.report_id.as_bytes()),
                    "lease_id_sha256": report
                        .lease_id
                        .as_ref()
                        .map(|lease_id| sha256_hex(lease_id.as_str().as_bytes())),
                    "outcome": report.outcome.as_str(),
                    "step_count": report.steps.len(),
                }),
            },
            occurred_at_unix_ms: report.completed_at_unix_ms,
            extensions: BTreeMap::new(),
        },
    };
    validate_runtime_event_append_request(&request)?;
    match persist_validated_runtime_event_tx(
        connection,
        max_payload_bytes,
        &request,
        report.completed_at_unix_ms,
    )? {
        RuntimeEventAppendOutcome::Appended { .. }
        | RuntimeEventAppendOutcome::AlreadyAppended { .. } => Ok(()),
        RuntimeEventAppendOutcome::StaleSuppressed => Err(JournalError::InvalidArgument(
            "host cleanup event persistence returned stale unexpectedly".to_owned(),
        )),
    }
}

/// Bounded diagnostics aggregate derived from durable shared runtime state.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SharedRuntimeDiagnostics {
    pub schema_version: u32,
    pub active_generations: usize,
    pub active_process_leases: usize,
    pub active_health_probe_leases: usize,
    pub health_probe_begins: usize,
    pub health_probe_settlements_by_disposition: BTreeMap<String, u64>,
    pub health_probe_stranded_components: usize,
    pub health_probe_orphan_leases: usize,
    pub health_probe_generation_mismatches: usize,
    pub stale_events_by_subsystem: BTreeMap<String, u64>,
    pub side_effect_fences_by_state: BTreeMap<String, u64>,
    pub component_health_by_state: BTreeMap<String, u64>,
    pub handles_by_state: BTreeMap<String, u64>,
    pub cleanup_reports_by_outcome: BTreeMap<String, u64>,
    pub cleanup_reports_by_reason: BTreeMap<String, u64>,
    pub networked_worker_dispatch_active_by_state: BTreeMap<String, u64>,
    pub networked_worker_dispatch_archived_by_state: BTreeMap<String, u64>,
    pub networked_worker_dispatch_delivery_by_disposition: BTreeMap<String, u64>,
    pub networked_worker_dispatch_reclaimable_terminal: usize,
    pub networked_worker_dispatch_unresolved: usize,
    pub networked_worker_dispatch_maximum: usize,
    pub networked_worker_dispatch_headroom_after_reclaim: usize,
    pub compatibility_admission: RuntimeStateAdmissionPosture,
    pub compatibility_findings_by_reason: BTreeMap<String, u64>,
}

impl JournalStore {
    /// Builds bounded aggregate shared-runtime diagnostics without raw identities or payloads.
    pub fn shared_runtime_diagnostics(&self) -> Result<SharedRuntimeDiagnostics, JournalError> {
        let compatibility = self.runtime_state_compatibility_report()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let networked_worker_dispatch_active_by_state =
            grouped_counts(&guard, "runtime_networked_worker_dispatch_claims", "state")?;
        let networked_worker_dispatch_archived_by_state = grouped_counts(
            &guard,
            "runtime_networked_worker_dispatch_claim_terminal_evidence",
            "state",
        )?;
        let networked_worker_dispatch_reclaimable_terminal = count_rows_matching(
            &guard,
            "runtime_networked_worker_dispatch_claims",
            "schema_version IN (2, 3) AND state IN ('settled', 'cancelled', 'failed_closed')",
        )?;
        let networked_worker_dispatch_unresolved = count_rows_matching(
            &guard,
            "runtime_networked_worker_dispatch_claims",
            "schema_version = 3 AND state IN ('queued', 'in_flight', 'reconciling') \
             AND created_at_unix_ms >= 0 AND updated_at_unix_ms >= created_at_unix_ms \
             AND lease_expires_at_unix_ms > created_at_unix_ms \
             AND completed_at_unix_ms IS NULL",
        )?;
        Ok(SharedRuntimeDiagnostics {
            schema_version: 1,
            active_generations: count_rows(&guard, "runtime_generation_leases")?,
            active_process_leases: count_rows(&guard, "runtime_process_leases")?,
            active_health_probe_leases: count_unexpired_rows_matching(
                &guard,
                "runtime_health_probe_leases",
                current_unix_ms()?,
                "schema_version IN (1, 2)",
            )?,
            health_probe_begins: count_rows(&guard, "runtime_health_probe_begins")?,
            health_probe_settlements_by_disposition: grouped_counts(
                &guard,
                "runtime_health_probe_terminal_evidence",
                "disposition",
            )?,
            health_probe_stranded_components: count_rows_matching(
                &guard,
                "runtime_component_health",
                "state = 'probing' AND NOT EXISTS (SELECT 1 FROM runtime_health_probe_leases leases WHERE leases.component_ulid = runtime_component_health.component_ulid AND leases.expected_generation = runtime_component_health.generation AND leases.schema_version IN (1, 2))",
            )?,
            health_probe_orphan_leases: count_rows_matching(
                &guard,
                "runtime_health_probe_leases",
                "schema_version IN (1, 2) AND NOT EXISTS (SELECT 1 FROM runtime_component_health health WHERE health.component_ulid = runtime_health_probe_leases.component_ulid AND health.generation = runtime_health_probe_leases.expected_generation AND health.state = 'probing')",
            )?,
            health_probe_generation_mismatches: count_rows_matching(
                &guard,
                "runtime_health_probe_leases",
                "schema_version IN (1, 2) AND EXISTS (SELECT 1 FROM runtime_component_health health WHERE health.component_ulid = runtime_health_probe_leases.component_ulid AND health.generation <> runtime_health_probe_leases.expected_generation)",
            )?,
            stale_events_by_subsystem: grouped_counts(
                &guard,
                "runtime_stale_event_diagnostics",
                "subsystem",
            )?,
            side_effect_fences_by_state: grouped_counts(
                &guard,
                "runtime_side_effect_fences",
                "state",
            )?,
            component_health_by_state: grouped_counts(&guard, "runtime_component_health", "state")?,
            handles_by_state: grouped_counts(&guard, "runtime_handles", "state")?,
            cleanup_reports_by_outcome: grouped_counts(
                &guard,
                "runtime_cleanup_reports",
                "outcome",
            )?,
            cleanup_reports_by_reason: grouped_counts_bounded(
                &guard,
                "runtime_cleanup_reports",
                "reason_code",
                MAX_CLEANUP_REASON_DIAGNOSTIC_ENTRIES,
                OTHER_CLEANUP_REASON_DIAGNOSTIC_KEY,
            )?,
            networked_worker_dispatch_active_by_state,
            networked_worker_dispatch_archived_by_state,
            networked_worker_dispatch_delivery_by_disposition: grouped_nonnull_counts(
                &guard,
                "runtime_networked_worker_dispatch_claims",
                "delivery_disposition",
            )?,
            networked_worker_dispatch_reclaimable_terminal,
            networked_worker_dispatch_unresolved,
            networked_worker_dispatch_maximum: NETWORKED_WORKER_DISPATCH_CLAIM_MAX_ENTRIES,
            networked_worker_dispatch_headroom_after_reclaim:
                NETWORKED_WORKER_DISPATCH_CLAIM_MAX_ENTRIES
                    .saturating_sub(networked_worker_dispatch_unresolved),
            compatibility_admission: compatibility.admission,
            compatibility_findings_by_reason: compatibility.findings.iter().fold(
                BTreeMap::new(),
                |mut counts, finding| {
                    let count = counts.entry(finding.reason_code.clone()).or_insert(0_u64);
                    *count = count.saturating_add(1);
                    counts
                },
            ),
        })
    }
}

fn count_rows(connection: &Connection, table: &str) -> Result<usize, JournalError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let count = connection.query_row(sql.as_str(), [], |row| row.get::<_, i64>(0))?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn count_rows_matching(
    connection: &Connection,
    table: &str,
    predicate: &str,
) -> Result<usize, JournalError> {
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE {predicate}");
    let count = connection.query_row(sql.as_str(), [], |row| row.get::<_, i64>(0))?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn count_unexpired_rows_matching(
    connection: &Connection,
    table: &str,
    now_unix_ms: i64,
    predicate: &str,
) -> Result<usize, JournalError> {
    let sql =
        format!("SELECT COUNT(*) FROM {table} WHERE expires_at_unix_ms > ?1 AND ({predicate})");
    let count =
        connection.query_row(sql.as_str(), params![now_unix_ms], |row| row.get::<_, i64>(0))?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn grouped_nonnull_counts(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<BTreeMap<String, u64>, JournalError> {
    let sql = format!(
        "SELECT {column}, COUNT(*) FROM {table} WHERE {column} IS NOT NULL GROUP BY {column}"
    );
    let mut statement = connection.prepare(sql.as_str())?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, u64::try_from(row.get::<_, i64>(1)?).unwrap_or(u64::MAX)))
    })?;
    rows.collect::<Result<BTreeMap<_, _>, _>>().map_err(Into::into)
}

fn grouped_counts(
    connection: &Connection,
    table: &str,
    column: &str,
) -> Result<BTreeMap<String, u64>, JournalError> {
    let sql = format!("SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}");
    let mut statement = connection.prepare(sql.as_str())?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, u64::try_from(row.get::<_, i64>(1)?).unwrap_or(u64::MAX)))
    })?;
    rows.collect::<Result<BTreeMap<_, _>, _>>().map_err(Into::into)
}

fn grouped_counts_bounded(
    connection: &Connection,
    table: &str,
    column: &str,
    maximum_entries: usize,
    other_key: &str,
) -> Result<BTreeMap<String, u64>, JournalError> {
    if maximum_entries == 0 {
        return Ok(BTreeMap::new());
    }
    let sql =
        format!("SELECT {column}, COUNT(*) FROM {table} GROUP BY {column} ORDER BY {column} ASC");
    let mut statement = connection.prepare(sql.as_str())?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, u64::try_from(row.get::<_, i64>(1)?).unwrap_or(u64::MAX)))
    })?;
    let mut counts = BTreeMap::new();
    let mut other_count = 0_u64;
    for row in rows {
        let (key, count) = row?;
        if counts.len() < maximum_entries.saturating_sub(1) || counts.contains_key(&key) {
            counts.insert(key, count);
        } else {
            other_count = other_count.saturating_add(count);
        }
    }
    if other_count > 0 {
        let count = counts.entry(other_key.to_owned()).or_insert(0);
        *count = count.saturating_add(other_count);
    }
    Ok(counts)
}
