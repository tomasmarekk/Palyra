//! Durable profile-rollback requests for active RuntimeKernelV2 generations.
//!
//! Effect posture is reconstructed from validated journal evidence. Suspension
//! advances the existing kernel head and never changes its persisted authority.

use std::collections::BTreeMap;

use palyra_common::runtime_contracts::{
    RuntimeEventEnvelopeV2, RuntimeEventId, RuntimeEventName, RuntimeEventPayloadRef,
    RuntimeGeneration, RuntimeGenerationLane, SideEffectFenceState, SideEffectFenceV1,
    RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
};
use rusqlite::{params, OptionalExtension, Row, Transaction, TransactionBehavior};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;

use crate::{
    application::runtime_kernel_v2::{
        rollback::{
            plan_runtime_rollback, ActiveRunEffectPosture, RollbackBoundary, RuntimeRollbackAction,
            VerifiedRuntimeRollbackSafeBoundary,
        },
        KernelLaneAuthoritySet, KernelState, KernelStateSnapshot, KernelTransition,
        RuntimeKernelV2, RuntimeKernelVersion, TransitionOutcome,
    },
    config::RuntimeKernelRollbackPolicy,
    journal::runtime_finalization::{
        runtime_delivery_state_for_run_generation_tx, RuntimeDeliveryState,
    },
};

use super::{
    canonical_bounded_json, generation_from_sql, generation_sql, load_runtime_kernel_head_tx,
    next_runtime_kernel_event_sequence_tx, revision_sql, sha256_hex, JournalError, JournalStore,
    RuntimeKernelTransitionCommitOutcome,
};
use crate::journal::{current_unix_ms, shared_runtime::active_runtime_generation_tx};

const RUNTIME_ROLLBACK_SCHEMA_VERSION: i64 = 1;
const MAX_ACTIVE_ROLLBACK_GENERATIONS: usize = 4_096;

/// Migration 76: immutable rollback evidence with a generation-fenced resolution.
pub(crate) const MIGRATION_76_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS runtime_kernel_rollback_requests (
        run_ulid TEXT NOT NULL,
        session_ulid TEXT NOT NULL,
        run_generation INTEGER NOT NULL CHECK (run_generation > 0),
        requested_head_revision INTEGER NOT NULL CHECK (requested_head_revision >= 0),
        authority_sha256 TEXT NOT NULL,
        policy TEXT NOT NULL CHECK (
            policy IN (
                'finish_read_only_suspend_mutating',
                'suspend_all_at_safe_boundary'
            )
        ),
        effect_posture TEXT NOT NULL CHECK (
            effect_posture IN ('read_only', 'mutating', 'outcome_unknown')
        ),
        action TEXT NOT NULL CHECK (
            action IN (
                'finish_with_persisted_authority',
                'await_safe_boundary_then_suspend',
                'suspend_at_safe_boundary'
            )
        ),
        reason_code TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        evidence_sha256 TEXT NOT NULL,
        request_json TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        state TEXT NOT NULL CHECK (
            state IN ('finish_allowed', 'awaiting_safe_boundary', 'suspended')
        ),
        state_revision INTEGER NOT NULL DEFAULT 0 CHECK (state_revision >= 0),
        resolution_head_revision INTEGER CHECK (resolution_head_revision >= 0),
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        PRIMARY KEY(run_ulid, run_generation),
        FOREIGN KEY(run_ulid) REFERENCES runtime_kernel_heads(run_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_runtime_kernel_rollback_state
        ON runtime_kernel_rollback_requests(state, created_at_unix_ms ASC);

    CREATE TRIGGER IF NOT EXISTS trg_runtime_kernel_rollback_validate_update
    BEFORE UPDATE ON runtime_kernel_rollback_requests
    WHEN NEW.run_ulid != OLD.run_ulid
      OR NEW.session_ulid != OLD.session_ulid
      OR NEW.run_generation != OLD.run_generation
      OR NEW.requested_head_revision != OLD.requested_head_revision
      OR NEW.authority_sha256 != OLD.authority_sha256
      OR NEW.policy != OLD.policy
      OR NEW.effect_posture != OLD.effect_posture
      OR NEW.action != OLD.action
      OR NEW.reason_code != OLD.reason_code
      OR NEW.evidence_json != OLD.evidence_json
      OR NEW.evidence_sha256 != OLD.evidence_sha256
      OR NEW.request_json != OLD.request_json
      OR NEW.request_sha256 != OLD.request_sha256
      OR NEW.created_at_unix_ms != OLD.created_at_unix_ms
      OR NEW.schema_version != OLD.schema_version
      OR OLD.state != 'awaiting_safe_boundary'
      OR NEW.state != 'suspended'
      OR NEW.state_revision != OLD.state_revision + 1
      OR OLD.resolution_head_revision IS NOT NULL
      OR NEW.resolution_head_revision IS NULL
      OR NEW.updated_at_unix_ms < OLD.updated_at_unix_ms
    BEGIN
        SELECT RAISE(ABORT, 'runtime kernel rollback update violates immutable evidence or CAS state');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_runtime_kernel_rollback_prevent_delete
    BEFORE DELETE ON runtime_kernel_rollback_requests BEGIN
        SELECT RAISE(ABORT, 'runtime kernel rollback requests cannot be deleted');
    END;
"#;

/// Aggregate result of one idempotent active-generation downgrade scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RuntimeRollbackActuationReportV1 {
    /// Nonterminal V2 heads evaluated from durable state.
    pub(crate) evaluated: usize,
    /// Read-only generations allowed to finish under unchanged authority.
    pub(crate) finish_allowed: usize,
    /// Generations waiting for a verified safe-boundary suspension.
    pub(crate) suspension_pending: usize,
    /// Generations already durably suspended.
    pub(crate) suspended: usize,
    /// Existing exact requests reused without mutation.
    pub(crate) replayed: usize,
}

/// Result of consuming one pending request at a host-sealed safe boundary.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RuntimeRollbackBoundaryOutcome {
    /// The generation has no rollback request.
    NoRequest,
    /// The generation is pinned to V2 and may finish without new mutating effects.
    FinishAllowed,
    /// The exact head was advanced to `Suspended`, or that result was replayed.
    Suspended {
        /// Newly authoritative suspended snapshot.
        snapshot: Box<KernelStateSnapshot>,
        /// Whether the same suspension had already committed.
        replayed: bool,
    },
    /// The caller's snapshot no longer matches the durable compare-and-set head.
    StaleDenied {
        /// Snapshot revision carried by the sealed handoff.
        expected_revision: u64,
        /// Current durable head revision when one exists.
        actual_revision: Option<u64>,
    },
    /// The generation became terminal before suspension was needed.
    TerminalNoAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoredRollbackState {
    FinishAllowed,
    AwaitingSafeBoundary,
    Suspended,
}

impl StoredRollbackState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::FinishAllowed => "finish_allowed",
            Self::AwaitingSafeBoundary => "awaiting_safe_boundary",
            Self::Suspended => "suspended",
        }
    }

    fn parse(value: &str, run_id: &str) -> Result<Self, JournalError> {
        match value {
            "finish_allowed" => Ok(Self::FinishAllowed),
            "awaiting_safe_boundary" => Ok(Self::AwaitingSafeBoundary),
            "suspended" => Ok(Self::Suspended),
            _ => Err(invalid_rollback_request(run_id, "stored state is unsupported")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeRollbackEvidenceV1 {
    schema_version: u32,
    requested_head_revision: u64,
    kernel_state: String,
    kernel_snapshot_sha256: String,
    authority_sha256: String,
    side_effect_state_counts: BTreeMap<String, u64>,
    delivery_state: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeRollbackRequestV1 {
    schema_version: u32,
    session_id: String,
    run_id: String,
    run_generation: u64,
    requested_head_revision: u64,
    authority_sha256: String,
    policy: String,
    effect_posture: String,
    action: String,
    reason_code: String,
    evidence_sha256: String,
}

#[derive(Debug, Clone)]
struct PreparedRollbackRequest {
    request: RuntimeRollbackRequestV1,
    evidence_json: String,
    request_json: String,
    request_sha256: String,
    initial_state: StoredRollbackState,
    resolution_head_revision: Option<u64>,
}

#[derive(Debug, Clone)]
struct StoredRollbackRequest {
    session_id: String,
    run_generation: RuntimeGeneration,
    requested_head_revision: u64,
    authority_sha256: String,
    policy: String,
    effect_posture: String,
    action: String,
    reason_code: String,
    evidence_json: String,
    evidence_sha256: String,
    request_json: String,
    request_sha256: String,
    state: StoredRollbackState,
    state_revision: u64,
    resolution_head_revision: Option<u64>,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    schema_version: i64,
}

impl JournalStore {
    /// Persists rollback decisions for every nonterminal V2 kernel head.
    ///
    /// The scan and inserts share one immediate transaction. Effect posture is
    /// reconstructed from validated side-effect fences and delivery records;
    /// callers cannot supply or downgrade that evidence.
    ///
    /// # Errors
    /// Returns [`JournalError`] when durable evidence is malformed, the active
    /// set exceeds its control-plane bound, or an exact request conflicts.
    pub(crate) fn request_runtime_kernel_profile_downgrade(
        &self,
        policy: RuntimeKernelRollbackPolicy,
    ) -> Result<RuntimeRollbackActuationReportV1, JournalError> {
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let run_ids = nonterminal_runtime_kernel_run_ids_tx(&transaction)?;
        if run_ids.len() > MAX_ACTIVE_ROLLBACK_GENERATIONS {
            return Err(JournalError::RuntimeRollbackCapacityExceeded {
                current_entries: run_ids.len(),
                max_entries: MAX_ACTIVE_ROLLBACK_GENERATIONS,
            });
        }

        let mut report = RuntimeRollbackActuationReportV1::default();
        for run_id in run_ids {
            let head =
                load_runtime_kernel_head_tx(&transaction, run_id.as_str())?.ok_or_else(|| {
                    JournalError::RuntimeKernelHeadNotFound { run_id: run_id.clone() }
                })?;
            if head.snapshot.state().is_terminal()
                || head.snapshot.runtime_authority_decision().selected_runtime()
                    != Some(crate::application::runtime_kernel_v2::selection::RuntimeAuthority::V2)
            {
                continue;
            }
            let identities = head.snapshot.base_identities();
            let active_run = active_runtime_generation_tx(
                &transaction,
                identities.session_id.as_str(),
                identities.run_id.as_str(),
                RuntimeGenerationLane::Run,
                now,
            )?;
            if active_run.as_ref().is_none_or(|lease| {
                lease.generation != head.snapshot.run_generation()
                    || lease.lease_id != head.snapshot.run_lease().lease_id
            }) {
                continue;
            }
            report.evaluated = report.evaluated.saturating_add(1);
            let prepared = prepare_rollback_request(
                &transaction,
                self.config.max_payload_bytes,
                policy,
                &head,
            )?;
            if let Some(existing) = load_rollback_request_tx(
                &transaction,
                run_id.as_str(),
                head.snapshot.run_generation(),
                self.config.max_payload_bytes,
            )? {
                validate_replayed_request(&existing, &prepared)?;
                report.replayed = report.replayed.saturating_add(1);
                accumulate_state(&mut report, existing.state);
                continue;
            }
            insert_rollback_request_tx(&transaction, &prepared, now)?;
            accumulate_state(&mut report, prepared.initial_state);
        }
        transaction.commit()?;
        Ok(report)
    }

    /// Applies a pending suspension at one host-sealed event handoff.
    ///
    /// The journal revalidates the exact head, generation, authority, effect
    /// evidence, Run lease, and request before appending the suspension event
    /// and advancing both CAS records in one transaction.
    ///
    /// # Errors
    /// Returns [`JournalError`] for malformed durable evidence, inactive
    /// generation authority, or an invalid kernel transition.
    pub(crate) fn apply_pending_runtime_rollback_at_safe_boundary(
        &self,
        boundary: &VerifiedRuntimeRollbackSafeBoundary,
    ) -> Result<RuntimeRollbackBoundaryOutcome, JournalError> {
        let expected = boundary.snapshot();
        expected.validate().map_err(|_| JournalError::InvalidRuntimeKernelSnapshot)?;
        let run_id = expected.base_identities().run_id.as_str();
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(request) = load_rollback_request_tx(
            &transaction,
            run_id,
            expected.run_generation(),
            self.config.max_payload_bytes,
        )?
        else {
            return Ok(RuntimeRollbackBoundaryOutcome::NoRequest);
        };
        if request.run_generation != expected.run_generation()
            || request.session_id != expected.base_identities().session_id.as_str()
        {
            return Ok(RuntimeRollbackBoundaryOutcome::StaleDenied {
                expected_revision: expected.revision(),
                actual_revision: None,
            });
        }
        match request.state {
            StoredRollbackState::FinishAllowed => {
                return Ok(RuntimeRollbackBoundaryOutcome::FinishAllowed);
            }
            StoredRollbackState::Suspended => {
                let head = load_runtime_kernel_head_tx(&transaction, run_id)?;
                return match head {
                    Some(head) if head.snapshot.state() == KernelState::Suspended => {
                        Ok(RuntimeRollbackBoundaryOutcome::Suspended {
                            snapshot: Box::new(head.snapshot),
                            replayed: true,
                        })
                    }
                    Some(head) => Ok(RuntimeRollbackBoundaryOutcome::StaleDenied {
                        expected_revision: expected.revision(),
                        actual_revision: Some(head.revision),
                    }),
                    None => Ok(RuntimeRollbackBoundaryOutcome::StaleDenied {
                        expected_revision: expected.revision(),
                        actual_revision: None,
                    }),
                };
            }
            StoredRollbackState::AwaitingSafeBoundary => {}
        }

        let Some(head) = load_runtime_kernel_head_tx(&transaction, run_id)? else {
            return Ok(RuntimeRollbackBoundaryOutcome::StaleDenied {
                expected_revision: expected.revision(),
                actual_revision: None,
            });
        };
        if head.snapshot != *expected || head.revision != expected.revision() {
            return Ok(RuntimeRollbackBoundaryOutcome::StaleDenied {
                expected_revision: expected.revision(),
                actual_revision: Some(head.revision),
            });
        }
        if head.snapshot.state().is_terminal() {
            return Ok(RuntimeRollbackBoundaryOutcome::TerminalNoAction);
        }
        if head.snapshot.state() == KernelState::Suspended {
            mark_rollback_suspended_tx(&transaction, run_id, &request, head.revision)?;
            transaction.commit()?;
            return Ok(RuntimeRollbackBoundaryOutcome::Suspended {
                snapshot: Box::new(head.snapshot),
                replayed: true,
            });
        }

        let effect_posture =
            derive_effect_posture_tx(&transaction, &head.snapshot, self.config.max_payload_bytes)?
                .0;
        let policy = parse_policy(request.policy.as_str(), run_id)?;
        let plan = plan_runtime_rollback(
            policy,
            head.snapshot.runtime_authority_decision(),
            effect_posture,
            RollbackBoundary::Safe,
        )
        .map_err(|_| invalid_rollback_request(run_id, "safe-boundary plan is invalid"))?;
        if plan.action() != RuntimeRollbackAction::SuspendAtSafeBoundary {
            return Err(invalid_rollback_request(
                run_id,
                "current durable evidence no longer requires the pending suspension",
            ));
        }

        let now = current_unix_ms()?;
        let identities = head.snapshot.base_identities();
        let active_run = active_runtime_generation_tx(
            &transaction,
            identities.session_id.as_str(),
            identities.run_id.as_str(),
            RuntimeGenerationLane::Run,
            now,
        )?;
        let Some(active_run) = active_run else {
            return Ok(RuntimeRollbackBoundaryOutcome::StaleDenied {
                expected_revision: expected.revision(),
                actual_revision: Some(head.revision),
            });
        };
        if active_run.generation != expected.run_generation()
            || active_run.lease_id != expected.run_lease().lease_id
        {
            return Ok(RuntimeRollbackBoundaryOutcome::StaleDenied {
                expected_revision: expected.revision(),
                actual_revision: Some(head.revision),
            });
        }
        let lane_authority = KernelLaneAuthoritySet::new(identities, vec![active_run])
            .map_err(|_| JournalError::InvalidPreparedRuntimeKernelTransition)?;
        let descriptor = RuntimeEventName::BackpressureApplied.descriptor();
        let sequence = next_runtime_kernel_event_sequence_tx(
            &transaction,
            identities.session_id.as_str(),
            RuntimeGenerationLane::Run,
            identities.generation,
        )?;
        let event_digest = sha256_hex(
            format!(
                "{}:{}:{}",
                request.request_sha256,
                expected.revision(),
                boundary.event_name().as_str()
            )
            .as_bytes(),
        );
        let event = RuntimeEventEnvelopeV2 {
            schema_version: RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
            event_id: RuntimeEventId::parse(format!("runtime_rollback_{event_digest}").as_str())
                .map_err(|_| JournalError::InvalidPreparedRuntimeKernelTransition)?,
            identities: identities.clone(),
            sequence,
            causal_parent_event_id: None,
            subsystem: descriptor.subsystem,
            phase: descriptor.phase,
            event_name: RuntimeEventName::BackpressureApplied,
            reason_code: request.reason_code.clone(),
            actor_kind: descriptor.actor_kind,
            retryability: descriptor.retryability,
            redaction_class: descriptor.redaction_class,
            terminal: descriptor.terminal,
            payload: RuntimeEventPayloadRef::Inline {
                metadata: json!({
                    "rollback_request_sha256": request.request_sha256,
                    "effect_evidence_sha256": request.evidence_sha256,
                    "safe_boundary_event": boundary.event_name().as_str(),
                }),
            },
            occurred_at_unix_ms: now,
            extensions: BTreeMap::new(),
        };
        let kernel = RuntimeKernelV2::restore_from_journal(head.snapshot.clone())
            .map_err(|_| JournalError::InvalidRuntimeKernelSnapshot)?;
        let prepared = kernel
            .prepare_transition(
                expected.run_generation(),
                &lane_authority,
                format!("runtime-rollback/{}", request.request_sha256).as_str(),
                event,
                KernelTransition::Suspend,
            )
            .map_err(|_| JournalError::InvalidPreparedRuntimeKernelTransition)?;
        if !matches!(prepared.outcome(), TransitionOutcome::Applied { .. }) {
            return Err(JournalError::InvalidPreparedRuntimeKernelTransition);
        }
        let outcome =
            self.commit_prepared_runtime_kernel_transition_tx(&transaction, &prepared, now, None)?;
        let (snapshot, replayed) = match outcome {
            RuntimeKernelTransitionCommitOutcome::Applied { snapshot, .. } => (snapshot, false),
            RuntimeKernelTransitionCommitOutcome::AlreadyApplied { snapshot, .. } => {
                (snapshot, true)
            }
            RuntimeKernelTransitionCommitOutcome::StaleSuppressed { .. } => {
                return Ok(RuntimeRollbackBoundaryOutcome::StaleDenied {
                    expected_revision: expected.revision(),
                    actual_revision: Some(head.revision),
                });
            }
        };
        mark_rollback_suspended_tx(&transaction, run_id, &request, snapshot.revision())?;
        transaction.commit()?;
        Ok(RuntimeRollbackBoundaryOutcome::Suspended { snapshot: Box::new(snapshot), replayed })
    }
}

/// Rejects a new mutating effect after a profile rollback has fenced the Run.
///
/// This helper must execute inside the same immediate transaction that would
/// create the side-effect intent.
pub(crate) fn ensure_runtime_rollback_allows_new_side_effect_tx(
    connection: &rusqlite::Connection,
    run_id: &str,
    generation: RuntimeGeneration,
) -> Result<(), JournalError> {
    let state = connection
        .query_row(
            r#"
                SELECT state, run_generation
                FROM runtime_kernel_rollback_requests
                WHERE run_ulid = ?1 AND run_generation = ?2
            "#,
            params![run_id, generation_sql(generation)?],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((state, stored_generation)) = state else {
        return Ok(());
    };
    let stored_generation = generation_from_sql(stored_generation, run_id)?;
    if stored_generation != generation {
        return Err(JournalError::RuntimeRollbackNewSideEffectDenied {
            run_id: run_id.to_owned(),
            generation: generation.get(),
        });
    }
    StoredRollbackState::parse(state.as_str(), run_id)?;
    Err(JournalError::RuntimeRollbackNewSideEffectDenied {
        run_id: run_id.to_owned(),
        generation: generation.get(),
    })
}

fn nonterminal_runtime_kernel_run_ids_tx(
    connection: &rusqlite::Connection,
) -> Result<Vec<String>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT run_ulid
            FROM runtime_kernel_heads
            WHERE runtime_version IN ('v2', 'v2_canary')
              AND json_extract(snapshot_json, '$.state')
                    NOT IN ('done', 'failed', 'cancelled')
            ORDER BY initialized_at_unix_ms ASC, run_ulid ASC
            LIMIT ?1
        "#,
    )?;
    let limit = i64::try_from(MAX_ACTIVE_ROLLBACK_GENERATIONS.saturating_add(1))
        .map_err(|_| JournalError::InvalidRuntimeKernelSnapshot)?;
    let rows = statement.query_map(params![limit], |row| row.get::<_, String>(0))?;
    rows.collect::<Result<Vec<_>, _>>().map_err(JournalError::from)
}

fn prepare_rollback_request(
    connection: &rusqlite::Connection,
    max_payload_bytes: usize,
    policy: RuntimeKernelRollbackPolicy,
    head: &super::RuntimeKernelHeadRecord,
) -> Result<PreparedRollbackRequest, JournalError> {
    let snapshot = &head.snapshot;
    let run_id = snapshot.base_identities().run_id.as_str();
    if !matches!(snapshot.version(), RuntimeKernelVersion::V2 | RuntimeKernelVersion::V2Canary) {
        return Err(invalid_rollback_request(run_id, "head is not an authoritative V2 runtime"));
    }
    let authority_json = canonical_bounded_json(
        snapshot.runtime_authority_decision(),
        "runtime_rollback_authority",
        max_payload_bytes,
    )?;
    let authority_sha256 = sha256_hex(authority_json.as_bytes());
    let snapshot_json =
        canonical_bounded_json(snapshot, "runtime_rollback_snapshot", max_payload_bytes)?;
    let snapshot_sha256 = sha256_hex(snapshot_json.as_bytes());
    let (effect_posture, side_effect_state_counts, delivery_state) =
        derive_effect_posture_tx(connection, snapshot, max_payload_bytes)?;
    let boundary = if snapshot.state() == KernelState::Suspended {
        RollbackBoundary::Safe
    } else {
        RollbackBoundary::Unsafe
    };
    let plan = plan_runtime_rollback(
        policy,
        snapshot.runtime_authority_decision(),
        effect_posture,
        boundary,
    )
    .map_err(|_| invalid_rollback_request(run_id, "rollback planner rejected durable authority"))?;
    let initial_state = match plan.action() {
        RuntimeRollbackAction::FinishWithPersistedAuthority => StoredRollbackState::FinishAllowed,
        RuntimeRollbackAction::AwaitSafeBoundaryThenSuspend => {
            StoredRollbackState::AwaitingSafeBoundary
        }
        RuntimeRollbackAction::SuspendAtSafeBoundary => StoredRollbackState::Suspended,
        RuntimeRollbackAction::NoActionTerminal
        | RuntimeRollbackAction::NoActionLegacyAuthority => {
            return Err(invalid_rollback_request(
                run_id,
                "nonterminal V2 head produced a no-action rollback plan",
            ));
        }
    };
    let evidence = RuntimeRollbackEvidenceV1 {
        schema_version: 1,
        requested_head_revision: head.revision,
        kernel_state: kernel_state_str(snapshot.state()).to_owned(),
        kernel_snapshot_sha256: snapshot_sha256,
        authority_sha256: authority_sha256.clone(),
        side_effect_state_counts,
        delivery_state: delivery_state.map(delivery_state_str).map(str::to_owned),
    };
    let evidence_json =
        canonical_bounded_json(&evidence, "runtime_rollback_evidence", max_payload_bytes)?;
    let evidence_sha256 = sha256_hex(evidence_json.as_bytes());
    let request = RuntimeRollbackRequestV1 {
        schema_version: 1,
        session_id: snapshot.base_identities().session_id.to_string(),
        run_id: run_id.to_owned(),
        run_generation: snapshot.run_generation().get(),
        requested_head_revision: head.revision,
        authority_sha256,
        policy: policy.as_str().to_owned(),
        effect_posture: effect_posture_str(effect_posture).to_owned(),
        action: action_str(plan.action()).to_owned(),
        reason_code: plan.reason_code().to_owned(),
        evidence_sha256,
    };
    let request_json =
        canonical_bounded_json(&request, "runtime_rollback_request", max_payload_bytes)?;
    let request_sha256 = sha256_hex(request_json.as_bytes());
    Ok(PreparedRollbackRequest {
        request,
        evidence_json,
        request_json,
        request_sha256,
        initial_state,
        resolution_head_revision: (initial_state == StoredRollbackState::Suspended)
            .then_some(head.revision),
    })
}

type EffectPostureProjection =
    (ActiveRunEffectPosture, BTreeMap<String, u64>, Option<RuntimeDeliveryState>);

fn derive_effect_posture_tx(
    connection: &rusqlite::Connection,
    snapshot: &KernelStateSnapshot,
    max_payload_bytes: usize,
) -> Result<EffectPostureProjection, JournalError> {
    if snapshot.state().is_terminal() {
        return Ok((ActiveRunEffectPosture::Terminal, BTreeMap::new(), None));
    }
    let identities = snapshot.base_identities();
    let states = validated_side_effect_states_tx(
        connection,
        identities.session_id.as_str(),
        identities.run_id.as_str(),
        identities.generation,
        max_payload_bytes,
    )?;
    let mut counts = BTreeMap::new();
    for state in &states {
        let count = counts.entry(state.as_str().to_owned()).or_insert(0_u64);
        *count = count.saturating_add(1);
    }
    let delivery_state = runtime_delivery_state_for_run_generation_tx(
        connection,
        identities.session_id.as_str(),
        identities.run_id.as_str(),
        identities.generation,
    )?;
    let has_unknown_effect = states.iter().any(|state| {
        matches!(
            state,
            SideEffectFenceState::EffectStarted
                | SideEffectFenceState::EffectUnknown
                | SideEffectFenceState::Abandoned
        )
    }) || delivery_state == Some(RuntimeDeliveryState::OutcomeUnknown)
        || matches!(snapshot.state(), KernelState::ExecutingTool | KernelState::RecoveryPending)
            && states.is_empty()
        || snapshot.state() == KernelState::AwaitingDelivery && delivery_state.is_none();
    if has_unknown_effect {
        return Ok((ActiveRunEffectPosture::OutcomeUnknown, counts, delivery_state));
    }
    let has_mutation = !states.is_empty() || delivery_state.is_some();
    Ok((
        if has_mutation {
            ActiveRunEffectPosture::Mutating
        } else {
            ActiveRunEffectPosture::ReadOnly
        },
        counts,
        delivery_state,
    ))
}

fn validated_side_effect_states_tx(
    connection: &rusqlite::Connection,
    session_id: &str,
    run_id: &str,
    generation: RuntimeGeneration,
    max_payload_bytes: usize,
) -> Result<Vec<SideEffectFenceState>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT operation_ulid, tool_execution_ulid, session_ulid, run_ulid,
                   intent_generation, observed_generation, state, intent_sha256,
                   fence_json, updated_at_unix_ms
            FROM runtime_side_effect_fences
            WHERE run_ulid = ?1
            ORDER BY operation_ulid ASC
        "#,
    )?;
    let rows = statement.query_map(params![run_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, String>(8)?,
            row.get::<_, i64>(9)?,
        ))
    })?;
    let mut states = Vec::new();
    for row in rows {
        let (
            operation_id,
            tool_execution_id,
            stored_session_id,
            stored_run_id,
            intent_generation,
            observed_generation,
            state,
            intent_sha256,
            fence_json,
            updated_at_unix_ms,
        ) = row?;
        let fence: SideEffectFenceV1 = decode_bounded_canonical_json(
            fence_json.as_str(),
            "runtime_rollback_side_effect_fence",
            max_payload_bytes,
        )?;
        fence
            .validate()
            .map_err(|_| invalid_rollback_request(run_id, "stored side-effect fence is invalid"))?;
        let stored_intent_generation = generation_from_sql(intent_generation, run_id)?;
        let stored_observed_generation = generation_from_sql(observed_generation, run_id)?;
        if stored_session_id != session_id
            || stored_run_id != run_id
            || fence.operation_id.as_str() != operation_id
            || fence.tool_execution_id.as_str() != tool_execution_id
            || fence.intent_generation != stored_intent_generation
            || fence.observed_generation != stored_observed_generation
            || stored_intent_generation.get() > stored_observed_generation.get()
            || stored_observed_generation.get() > generation.get()
            || fence.state.as_str() != state
            || fence.intent_sha256 != intent_sha256
            || fence.updated_at_unix_ms != updated_at_unix_ms
        {
            return Err(invalid_rollback_request(
                run_id,
                "side-effect fence columns contradict canonical evidence",
            ));
        }
        states.push(fence.state);
    }
    Ok(states)
}

fn insert_rollback_request_tx(
    transaction: &Transaction<'_>,
    prepared: &PreparedRollbackRequest,
    now: i64,
) -> Result<(), JournalError> {
    transaction.execute(
        r#"
            INSERT INTO runtime_kernel_rollback_requests (
                run_ulid, session_ulid, run_generation, requested_head_revision,
                authority_sha256, policy, effect_posture, action, reason_code,
                evidence_json, evidence_sha256, request_json, request_sha256,
                state, state_revision, resolution_head_revision,
                created_at_unix_ms, updated_at_unix_ms, schema_version
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, 0, ?15, ?16, ?16, ?17
            )
        "#,
        params![
            prepared.request.run_id,
            prepared.request.session_id,
            generation_sql(
                RuntimeGeneration::new(prepared.request.run_generation)
                    .map_err(|_| JournalError::InvalidRuntimeKernelSnapshot)?
            )?,
            revision_sql(prepared.request.requested_head_revision)?,
            prepared.request.authority_sha256,
            prepared.request.policy,
            prepared.request.effect_posture,
            prepared.request.action,
            prepared.request.reason_code,
            prepared.evidence_json,
            prepared.request.evidence_sha256,
            prepared.request_json,
            prepared.request_sha256,
            prepared.initial_state.as_str(),
            prepared.resolution_head_revision.map(revision_sql).transpose()?,
            now,
            RUNTIME_ROLLBACK_SCHEMA_VERSION,
        ],
    )?;
    Ok(())
}

fn load_rollback_request_tx(
    connection: &rusqlite::Connection,
    run_id: &str,
    generation: RuntimeGeneration,
    max_payload_bytes: usize,
) -> Result<Option<StoredRollbackRequest>, JournalError> {
    let row = connection
        .query_row(
            r#"
                SELECT session_ulid, run_generation, requested_head_revision,
                       authority_sha256, policy, effect_posture, action, reason_code,
                       evidence_json, evidence_sha256, request_json, request_sha256,
                       state, state_revision, resolution_head_revision,
                       created_at_unix_ms, updated_at_unix_ms, schema_version
                FROM runtime_kernel_rollback_requests
                WHERE run_ulid = ?1 AND run_generation = ?2
            "#,
            params![run_id, generation_sql(generation)?],
            hydrate_rollback_request,
        )
        .optional()?;
    row.map(|row| validate_stored_rollback_request(row, run_id, max_payload_bytes)).transpose()
}

fn hydrate_rollback_request(row: &Row<'_>) -> rusqlite::Result<StoredRollbackRequest> {
    let run_generation = row.get::<_, i64>(1)?;
    let run_generation_u64 = u64::try_from(run_generation)
        .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, run_generation))?;
    let requested_head_revision = row.get::<_, i64>(2)?;
    let state_revision = row.get::<_, i64>(13)?;
    let resolution_head_revision = row.get::<_, Option<i64>>(14)?;
    Ok(StoredRollbackRequest {
        session_id: row.get(0)?,
        run_generation: RuntimeGeneration::new(run_generation_u64)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, run_generation))?,
        requested_head_revision: u64::try_from(requested_head_revision)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(2, requested_head_revision))?,
        authority_sha256: row.get(3)?,
        policy: row.get(4)?,
        effect_posture: row.get(5)?,
        action: row.get(6)?,
        reason_code: row.get(7)?,
        evidence_json: row.get(8)?,
        evidence_sha256: row.get(9)?,
        request_json: row.get(10)?,
        request_sha256: row.get(11)?,
        state: StoredRollbackState::parse(row.get::<_, String>(12)?.as_str(), "unknown")
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
        state_revision: u64::try_from(state_revision)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(13, state_revision))?,
        resolution_head_revision: resolution_head_revision
            .map(|value| {
                u64::try_from(value)
                    .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(14, value))
            })
            .transpose()?,
        created_at_unix_ms: row.get(15)?,
        updated_at_unix_ms: row.get(16)?,
        schema_version: row.get(17)?,
    })
}

fn validate_stored_rollback_request(
    stored: StoredRollbackRequest,
    run_id: &str,
    max_payload_bytes: usize,
) -> Result<StoredRollbackRequest, JournalError> {
    let evidence: RuntimeRollbackEvidenceV1 = decode_bounded_canonical_json(
        stored.evidence_json.as_str(),
        "runtime_rollback_evidence",
        max_payload_bytes,
    )?;
    let request: RuntimeRollbackRequestV1 = decode_bounded_canonical_json(
        stored.request_json.as_str(),
        "runtime_rollback_request",
        max_payload_bytes,
    )?;
    if stored.schema_version != RUNTIME_ROLLBACK_SCHEMA_VERSION
        || evidence.schema_version != 1
        || request.schema_version != 1
        || request.run_id != run_id
        || request.session_id != stored.session_id
        || request.run_generation != stored.run_generation.get()
        || request.requested_head_revision != stored.requested_head_revision
        || request.authority_sha256 != stored.authority_sha256
        || request.policy != stored.policy
        || request.effect_posture != stored.effect_posture
        || request.action != stored.action
        || request.reason_code != stored.reason_code
        || request.evidence_sha256 != stored.evidence_sha256
        || evidence.requested_head_revision != stored.requested_head_revision
        || evidence.authority_sha256 != stored.authority_sha256
        || sha256_hex(stored.evidence_json.as_bytes()) != stored.evidence_sha256
        || sha256_hex(stored.request_json.as_bytes()) != stored.request_sha256
        || stored.created_at_unix_ms < 0
        || stored.updated_at_unix_ms < stored.created_at_unix_ms
        || stored.state_revision > 1
        || (stored.state == StoredRollbackState::Suspended)
            != stored.resolution_head_revision.is_some()
        || (stored.state != StoredRollbackState::Suspended && stored.state_revision != 0)
    {
        return Err(invalid_rollback_request(
            run_id,
            "stored columns contradict canonical rollback evidence",
        ));
    }
    parse_policy(stored.policy.as_str(), run_id)?;
    parse_effect_posture(stored.effect_posture.as_str(), run_id)?;
    let action = parse_action(stored.action.as_str(), run_id)?;
    let state_matches_action = matches!(
        (stored.state, action),
        (StoredRollbackState::FinishAllowed, RuntimeRollbackAction::FinishWithPersistedAuthority)
            | (
                StoredRollbackState::AwaitingSafeBoundary,
                RuntimeRollbackAction::AwaitSafeBoundaryThenSuspend
            )
            | (
                StoredRollbackState::Suspended,
                RuntimeRollbackAction::AwaitSafeBoundaryThenSuspend
                    | RuntimeRollbackAction::SuspendAtSafeBoundary
            )
    );
    if !state_matches_action {
        return Err(invalid_rollback_request(
            run_id,
            "stored state contradicts the requested rollback action",
        ));
    }
    Ok(stored)
}

fn validate_replayed_request(
    existing: &StoredRollbackRequest,
    prepared: &PreparedRollbackRequest,
) -> Result<(), JournalError> {
    if existing.run_generation.get() != prepared.request.run_generation
        || existing.session_id != prepared.request.session_id
        || existing.authority_sha256 != prepared.request.authority_sha256
    {
        return Err(invalid_rollback_request(
            prepared.request.run_id.as_str(),
            "existing request belongs to different immutable authority",
        ));
    }
    // A later scan must not overwrite the first rollback posture. The original
    // request remains the fence that prevents new mutating effects.
    Ok(())
}

fn mark_rollback_suspended_tx(
    transaction: &Transaction<'_>,
    run_id: &str,
    request: &StoredRollbackRequest,
    resolution_revision: u64,
) -> Result<(), JournalError> {
    let updated = transaction.execute(
        r#"
            UPDATE runtime_kernel_rollback_requests
            SET state = 'suspended',
                state_revision = state_revision + 1,
                resolution_head_revision = ?1,
                updated_at_unix_ms = ?2
            WHERE run_ulid = ?3
              AND run_generation = ?4
              AND state = 'awaiting_safe_boundary'
              AND state_revision = ?5
              AND request_sha256 = ?6
        "#,
        params![
            revision_sql(resolution_revision)?,
            current_unix_ms()?,
            run_id,
            generation_sql(request.run_generation)?,
            revision_sql(request.state_revision)?,
            request.request_sha256,
        ],
    )?;
    if updated != 1 {
        return Err(JournalError::RuntimeKernelHeadConflict { run_id: run_id.to_owned() });
    }
    Ok(())
}

fn accumulate_state(report: &mut RuntimeRollbackActuationReportV1, state: StoredRollbackState) {
    match state {
        StoredRollbackState::FinishAllowed => {
            report.finish_allowed = report.finish_allowed.saturating_add(1);
        }
        StoredRollbackState::AwaitingSafeBoundary => {
            report.suspension_pending = report.suspension_pending.saturating_add(1);
        }
        StoredRollbackState::Suspended => {
            report.suspended = report.suspended.saturating_add(1);
        }
    }
}

fn invalid_rollback_request(run_id: &str, reason: &str) -> JournalError {
    JournalError::InvalidRuntimeRollbackRequest {
        run_id: run_id.to_owned(),
        reason: reason.to_owned(),
    }
}

fn decode_bounded_canonical_json<T: DeserializeOwned + Serialize>(
    raw: &str,
    payload_kind: &'static str,
    max_payload_bytes: usize,
) -> Result<T, JournalError> {
    if raw.len() > max_payload_bytes {
        return Err(JournalError::PayloadTooLarge {
            payload_kind,
            actual_bytes: raw.len(),
            max_bytes: max_payload_bytes,
        });
    }
    super::decode_canonical_json(raw, payload_kind)
}

fn parse_policy(value: &str, run_id: &str) -> Result<RuntimeKernelRollbackPolicy, JournalError> {
    match value {
        "finish_read_only_suspend_mutating" => {
            Ok(RuntimeKernelRollbackPolicy::FinishReadOnlySuspendMutating)
        }
        "suspend_all_at_safe_boundary" => Ok(RuntimeKernelRollbackPolicy::SuspendAllAtSafeBoundary),
        _ => Err(invalid_rollback_request(run_id, "stored rollback policy is unsupported")),
    }
}

fn parse_effect_posture(value: &str, run_id: &str) -> Result<ActiveRunEffectPosture, JournalError> {
    match value {
        "read_only" => Ok(ActiveRunEffectPosture::ReadOnly),
        "mutating" => Ok(ActiveRunEffectPosture::Mutating),
        "outcome_unknown" => Ok(ActiveRunEffectPosture::OutcomeUnknown),
        _ => Err(invalid_rollback_request(run_id, "stored effect posture is unsupported")),
    }
}

fn parse_action(value: &str, run_id: &str) -> Result<RuntimeRollbackAction, JournalError> {
    match value {
        "finish_with_persisted_authority" => {
            Ok(RuntimeRollbackAction::FinishWithPersistedAuthority)
        }
        "await_safe_boundary_then_suspend" => {
            Ok(RuntimeRollbackAction::AwaitSafeBoundaryThenSuspend)
        }
        "suspend_at_safe_boundary" => Ok(RuntimeRollbackAction::SuspendAtSafeBoundary),
        _ => Err(invalid_rollback_request(run_id, "stored rollback action is unsupported")),
    }
}

const fn effect_posture_str(posture: ActiveRunEffectPosture) -> &'static str {
    match posture {
        ActiveRunEffectPosture::ReadOnly => "read_only",
        ActiveRunEffectPosture::Mutating => "mutating",
        ActiveRunEffectPosture::OutcomeUnknown => "outcome_unknown",
        ActiveRunEffectPosture::Terminal => "terminal",
    }
}

const fn action_str(action: RuntimeRollbackAction) -> &'static str {
    match action {
        RuntimeRollbackAction::FinishWithPersistedAuthority => "finish_with_persisted_authority",
        RuntimeRollbackAction::SuspendAtSafeBoundary => "suspend_at_safe_boundary",
        RuntimeRollbackAction::AwaitSafeBoundaryThenSuspend => "await_safe_boundary_then_suspend",
        RuntimeRollbackAction::NoActionTerminal => "no_action_terminal",
        RuntimeRollbackAction::NoActionLegacyAuthority => "no_action_legacy_authority",
    }
}

const fn kernel_state_str(state: KernelState) -> &'static str {
    match state {
        KernelState::Admitted => "admitted",
        KernelState::SelectingRuntime => "selecting_runtime",
        KernelState::AssemblingContext => "assembling_context",
        KernelState::CallingProvider => "calling_provider",
        KernelState::AwaitingToolGate => "awaiting_tool_gate",
        KernelState::AwaitingApproval => "awaiting_approval",
        KernelState::ExecutingTool => "executing_tool",
        KernelState::ProjectingResult => "projecting_result",
        KernelState::Compacting => "compacting",
        KernelState::Finalizing => "finalizing",
        KernelState::AwaitingDelivery => "awaiting_delivery",
        KernelState::Done => "done",
        KernelState::Failed => "failed",
        KernelState::Cancelled => "cancelled",
        KernelState::Suspended => "suspended",
        KernelState::RecoveryPending => "recovery_pending",
    }
}

const fn delivery_state_str(state: RuntimeDeliveryState) -> &'static str {
    match state {
        RuntimeDeliveryState::IntentRecorded => "intent_recorded",
        RuntimeDeliveryState::Queued => "queued",
        RuntimeDeliveryState::OutcomeUnknown => "outcome_unknown",
        RuntimeDeliveryState::Delivered => "delivered",
    }
}

#[cfg(test)]
pub(super) fn rollback_request_state_for_test(
    store: &JournalStore,
    run_id: &str,
) -> Result<Option<String>, JournalError> {
    let connection = store.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
    connection
        .query_row(
            r#"
                SELECT state
                FROM runtime_kernel_rollback_requests
                WHERE run_ulid = ?1
                ORDER BY run_generation DESC
                LIMIT 1
            "#,
            params![run_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(JournalError::from)
}
