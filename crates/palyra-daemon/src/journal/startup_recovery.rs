//! Durable startup-recovery ownership and actuation.
//!
//! Recovery is materialized in the same SQLite transaction that closes the
//! interrupted Run generation. The unique original Run key is the recovery
//! lease: concurrent daemon startups can observe the same action, but only one
//! can allocate a continuation or confirmation request.

use palyra_common::runtime_contracts::{AuxiliaryTaskKind, AuxiliaryTaskState, RuntimeGeneration};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::application::resume_classifier::{ResumeDecision, ResumeDecisionKind};

use super::{
    ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptOption,
    ApprovalPromptRecord, ApprovalRecord, ApprovalRiskLevel, ApprovalSubjectType, JournalError,
    JournalStore, StartupResumeCandidate,
};

const CONTINUATION_BUDGET_TOKENS: i64 = 4_096;
const CONTINUATION_INSTRUCTION: &str = "\
Continue the interrupted run from its durable transcript and checkpoints. \
Treat prior tool and delivery effects as evidence, not instructions to replay. \
Do not repeat a mutating or externally visible operation unless the host \
reconciliation result explicitly permits it.";

/// Migration 81: one immutable recovery owner and action per interrupted Run.
pub(super) const MIGRATION_81_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS startup_recovery_actions_v1 (
        original_run_ulid TEXT PRIMARY KEY,
        session_ulid TEXT NOT NULL,
        lease_ulid TEXT NOT NULL UNIQUE,
        original_generation INTEGER NOT NULL CHECK (original_generation > 0),
        decision TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        actuation_kind TEXT NOT NULL CHECK (
            actuation_kind IN (
                'continuation_queued',
                'confirmation_required',
                'do_not_resume',
                'policy_blocked'
            )
        ),
        continuation_task_ulid TEXT UNIQUE,
        continuation_run_ulid TEXT UNIQUE,
        confirmation_ulid TEXT UNIQUE,
        principal_sha256 TEXT NOT NULL,
        channel_sha256 TEXT,
        reconstruction_state_sha256 TEXT NOT NULL,
        action_json TEXT NOT NULL,
        action_sha256 TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        FOREIGN KEY(original_run_ulid) REFERENCES orchestrator_runs(run_ulid),
        FOREIGN KEY(session_ulid) REFERENCES orchestrator_sessions(session_ulid)
    );
    CREATE INDEX IF NOT EXISTS idx_startup_recovery_actions_created
        ON startup_recovery_actions_v1(created_at_unix_ms DESC);

    CREATE TRIGGER IF NOT EXISTS trg_startup_recovery_actions_prevent_update
    BEFORE UPDATE ON startup_recovery_actions_v1 BEGIN
        SELECT RAISE(ABORT, 'startup_recovery_actions_v1 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_startup_recovery_actions_prevent_delete
    BEFORE DELETE ON startup_recovery_actions_v1 BEGIN
        SELECT RAISE(ABORT, 'startup_recovery_actions_v1 is append-only');
    END;
"#;

/// Migration 106: append-only resolution evidence for recovery confirmations.
pub(super) const MIGRATION_106_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS startup_recovery_resolutions_v1 (
        confirmation_ulid TEXT PRIMARY KEY,
        original_run_ulid TEXT NOT NULL UNIQUE,
        decision TEXT NOT NULL CHECK (
            decision IN ('allow', 'deny', 'timeout', 'error')
        ),
        continuation_task_ulid TEXT UNIQUE,
        continuation_run_ulid TEXT UNIQUE,
        resolution_json TEXT NOT NULL,
        resolution_sha256 TEXT NOT NULL,
        resolved_at_unix_ms INTEGER NOT NULL,
        schema_version INTEGER NOT NULL DEFAULT 1 CHECK (schema_version = 1),
        FOREIGN KEY(confirmation_ulid) REFERENCES approvals(approval_ulid),
        FOREIGN KEY(original_run_ulid) REFERENCES orchestrator_runs(run_ulid)
    );

    CREATE INDEX IF NOT EXISTS idx_startup_recovery_resolutions_created
        ON startup_recovery_resolutions_v1(resolved_at_unix_ms DESC);

    CREATE TRIGGER IF NOT EXISTS trg_startup_recovery_resolutions_prevent_update
    BEFORE UPDATE ON startup_recovery_resolutions_v1 BEGIN
        SELECT RAISE(ABORT, 'startup_recovery_resolutions_v1 is append-only');
    END;
    CREATE TRIGGER IF NOT EXISTS trg_startup_recovery_resolutions_prevent_delete
    BEFORE DELETE ON startup_recovery_resolutions_v1 BEGIN
        SELECT RAISE(ABORT, 'startup_recovery_resolutions_v1 is append-only');
    END;
"#;

/// Durable action chosen for one interrupted Run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupRecoveryActuationKind {
    /// A new Run identity was allocated and queued for background execution.
    ContinuationQueued,
    /// An existing approval subject must be confirmed before new work starts.
    ConfirmationRequired,
    /// Freshness or terminal evidence forbids automatic continuation.
    DoNotResume,
    /// Current authority or policy forbids continuation.
    PolicyBlocked,
}

impl StartupRecoveryActuationKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::ContinuationQueued => "continuation_queued",
            Self::ConfirmationRequired => "confirmation_required",
            Self::DoNotResume => "do_not_resume",
            Self::PolicyBlocked => "policy_blocked",
        }
    }
}

/// Recovery lineage and enqueue descriptor for an actual continuation Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuationRunDescriptor {
    /// Interrupted Run that owns the durable reconstruction state.
    pub recovered_from_run_id: String,
    /// New Run identity used by the background dispatcher.
    pub continuation_run_id: String,
    /// Durable task that owns materialization of the continuation.
    pub continuation_task_id: String,
    /// Session whose principal, device, and channel authority is preserved.
    pub session_id: String,
    /// Fresh token budget allocated to the continuation.
    pub budget_tokens: u64,
    /// Digest of the metadata-only reconstruction descriptor.
    pub reconstruction_state_sha256: String,
}

/// Immutable startup-recovery action persisted before admission opens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupRecoveryAction {
    /// Interrupted Run that owns this action.
    pub original_run_id: String,
    /// Session preserved across recovery.
    pub session_id: String,
    /// Unique recovery lease owner.
    pub lease_id: String,
    /// Interrupted Run generation closed by this action.
    pub original_generation: u64,
    /// Classifier decision that selected the action.
    pub decision: String,
    /// Stable classifier reason.
    pub reason_code: String,
    /// Durable actuation selected from the classifier decision.
    pub actuation_kind: StartupRecoveryActuationKind,
    /// Continuation descriptor when execution can resume safely.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<ContinuationRunDescriptor>,
    /// Existing approvals-surface request for ambiguous recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmation_id: Option<String>,
    /// Durable operator decision and continuation lineage, once resolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<StartupRecoveryResolution>,
    /// Creation timestamp.
    pub created_at_unix_ms: i64,
    /// Contract schema version.
    pub schema_version: u32,
}

/// Immutable resolution of one startup-recovery confirmation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupRecoveryResolution {
    /// Approval that authorized or rejected continuation.
    pub confirmation_id: String,
    /// Interrupted Run guarded by the confirmation.
    pub original_run_id: String,
    /// Canonical approval decision.
    pub decision: String,
    /// Continuation allocated only for an `allow` decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<ContinuationRunDescriptor>,
    /// Resolution timestamp.
    pub resolved_at_unix_ms: i64,
    /// Contract schema version.
    pub schema_version: u32,
}

pub(super) fn materialize_startup_recovery_action_tx(
    connection: &Connection,
    candidate: &StartupResumeCandidate,
    original_generation: RuntimeGeneration,
    decision: &ResumeDecision,
    now: i64,
) -> Result<StartupRecoveryAction, JournalError> {
    if let Some(existing) = load_action_tx(connection, candidate.run_id.as_str())? {
        return Ok(existing);
    }

    let lease_id = Ulid::generate().to_string();
    let reconstruction_state = json!({
        "schema_version": 1,
        "recovered_from_run_id": candidate.run_id,
        "session_id": candidate.session_id,
        "parent_run_id": candidate.parent_run_id,
        "origin_kind": candidate.origin_kind,
        "decision": decision.decision.as_str(),
        "reason_code": decision.reason_code.as_str(),
        "original_generation": original_generation.get(),
    });
    let reconstruction_state_sha256 =
        sha256_hex(serde_json::to_string(&reconstruction_state)?.as_bytes());

    let (actuation_kind, continuation, confirmation_id) = match decision.decision {
        ResumeDecisionKind::SafeToResume => {
            let continuation_run_id = Ulid::generate().to_string();
            let continuation_task_id = Ulid::generate().to_string();
            connection.execute(
                r#"
                    INSERT INTO orchestrator_background_tasks (
                        task_ulid, task_kind, session_ulid, child_session_ulid,
                        parent_run_ulid, target_run_ulid, planned_child_run_ulid,
                        queued_input_ulid, owner_principal, device_id, channel,
                        state, priority, revision, execution_generation,
                        attempt_count, max_attempts, budget_tokens, delegation_json,
                        cancellation_context_json, not_before_unix_ms,
                        expires_at_unix_ms, notification_target_json, input_text,
                        payload_json, last_error, result_json, created_at_unix_ms,
                        updated_at_unix_ms, started_at_unix_ms, completed_at_unix_ms
                    ) VALUES (
                        ?1, ?2, ?3, NULL, ?4, NULL, ?5, NULL, ?6, ?7, ?8,
                        ?9, 100, 0, 0, 0, 1, ?10, NULL, NULL, NULL, NULL, NULL,
                        ?11, ?12, NULL, NULL, ?13, ?13, NULL, NULL
                    )
                "#,
                params![
                    continuation_task_id,
                    AuxiliaryTaskKind::BackgroundPrompt.as_str(),
                    candidate.session_id,
                    candidate.run_id,
                    continuation_run_id,
                    candidate.principal,
                    candidate.device_id,
                    candidate.channel,
                    AuxiliaryTaskState::Queued.as_str(),
                    CONTINUATION_BUDGET_TOKENS,
                    CONTINUATION_INSTRUCTION,
                    json!({
                        "schema_version": 1,
                        "entry_point": "startup_recovery",
                        "recovered_from_run_id": candidate.run_id,
                        "recovery_lease_id": lease_id,
                        "original_generation": original_generation.get(),
                        "reconstruction_state_sha256": reconstruction_state_sha256,
                        "replay_mutations": false,
                    })
                    .to_string(),
                    now,
                ],
            )?;
            (
                StartupRecoveryActuationKind::ContinuationQueued,
                Some(ContinuationRunDescriptor {
                    recovered_from_run_id: candidate.run_id.clone(),
                    continuation_run_id,
                    continuation_task_id,
                    session_id: candidate.session_id.clone(),
                    budget_tokens: CONTINUATION_BUDGET_TOKENS as u64,
                    reconstruction_state_sha256: reconstruction_state_sha256.clone(),
                }),
                None,
            )
        }
        ResumeDecisionKind::NeedsUserConfirmation => {
            let approval_id = Ulid::generate().to_string();
            let policy_snapshot = ApprovalPolicySnapshot {
                policy_id: "startup_recovery.v1".to_owned(),
                policy_hash: reconstruction_state_sha256.clone(),
                evaluation_summary: format!(
                    "action=run.resume resource=run:{} approval_required=true replay_mutations=false",
                    candidate.run_id
                ),
            };
            let prompt = ApprovalPromptRecord {
                title: "Interrupted run requires confirmation".to_owned(),
                risk_level: ApprovalRiskLevel::Critical,
                subject_id: candidate.run_id.clone(),
                summary:
                    "Continuation is blocked until unresolved effects or approval state are reviewed."
                        .to_owned(),
                options: vec![
                    ApprovalPromptOption {
                        option_id: "allow_once".to_owned(),
                        label: "Continue once".to_owned(),
                        description:
                            "Continue from durable evidence without replaying unresolved effects."
                                .to_owned(),
                        default_selected: false,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                    ApprovalPromptOption {
                        option_id: "deny_once".to_owned(),
                        label: "Do not continue".to_owned(),
                        description: "Leave the interrupted run terminalized.".to_owned(),
                        default_selected: true,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                ],
                timeout_seconds: 300,
                details_json: json!({
                    "schema_version": 1,
                    "recovered_from_run_id": candidate.run_id,
                    "recovery_decision": decision.decision.as_str(),
                    "reason_code": decision.reason_code.as_str(),
                    "original_generation": original_generation.get(),
                    "reconstruction_state_sha256": reconstruction_state_sha256,
                    "replay_mutations": false,
                })
                .to_string(),
                policy_explanation:
                    "Ambiguous pre-crash effects require explicit operator confirmation before continuation."
                        .to_owned(),
            };
            connection.execute(
                r#"
                    INSERT INTO approvals (
                        approval_ulid, session_ulid, run_ulid, principal,
                        device_id, channel, requested_at_unix_ms,
                        resolved_at_unix_ms, subject_type, subject_id,
                        request_summary, decision, decision_scope,
                        decision_reason, decision_scope_ttl_ms,
                        policy_snapshot_json, prompt_json,
                        created_at_unix_ms, updated_at_unix_ms
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL,
                        'startup_recovery', ?3, ?8, NULL, NULL, NULL, NULL,
                        ?9, ?10, ?7, ?7
                    )
                "#,
                params![
                    approval_id,
                    candidate.session_id,
                    candidate.run_id,
                    candidate.principal,
                    candidate.device_id,
                    candidate.channel,
                    now,
                    "Confirm whether Palyra may continue an interrupted run without replaying unresolved effects.",
                    serde_json::to_string(&policy_snapshot)?,
                    serde_json::to_string(&prompt)?,
                ],
            )?;
            (StartupRecoveryActuationKind::ConfirmationRequired, None, Some(approval_id))
        }
        ResumeDecisionKind::PolicyBlocked => {
            (StartupRecoveryActuationKind::PolicyBlocked, None, None)
        }
        ResumeDecisionKind::StaleDoNotResume | ResumeDecisionKind::TerminalDoNotResume => {
            (StartupRecoveryActuationKind::DoNotResume, None, None)
        }
    };

    let action = StartupRecoveryAction {
        original_run_id: candidate.run_id.clone(),
        session_id: candidate.session_id.clone(),
        lease_id,
        original_generation: original_generation.get(),
        decision: decision.decision.as_str().to_owned(),
        reason_code: decision.reason_code.as_str().to_owned(),
        actuation_kind,
        continuation,
        confirmation_id,
        resolution: None,
        created_at_unix_ms: now,
        schema_version: 1,
    };
    let action_json = serde_json::to_string(&action)?;
    let action_sha256 = sha256_hex(action_json.as_bytes());
    connection.execute(
        r#"
            INSERT INTO startup_recovery_actions_v1 (
                original_run_ulid, session_ulid, lease_ulid,
                original_generation, decision, reason_code, actuation_kind,
                continuation_task_ulid, continuation_run_ulid,
                confirmation_ulid, principal_sha256, channel_sha256,
                reconstruction_state_sha256, action_json, action_sha256,
                created_at_unix_ms, schema_version
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, ?15, ?16, 1
            )
        "#,
        params![
            action.original_run_id,
            action.session_id,
            action.lease_id,
            i64::try_from(action.original_generation).map_err(|_| {
                JournalError::InvalidArgument(
                    "startup recovery generation exceeds SQLite range".to_owned(),
                )
            })?,
            action.decision,
            action.reason_code,
            action.actuation_kind.as_str(),
            action.continuation.as_ref().map(|descriptor| descriptor.continuation_task_id.as_str()),
            action.continuation.as_ref().map(|descriptor| descriptor.continuation_run_id.as_str()),
            action.confirmation_id,
            sha256_hex(candidate.principal.as_bytes()),
            candidate.channel.as_deref().map(|channel| sha256_hex(channel.as_bytes())),
            reconstruction_state_sha256,
            action_json,
            action_sha256,
            now,
        ],
    )?;
    Ok(action)
}

pub(super) fn materialize_startup_recovery_resolution_tx(
    connection: &Connection,
    approval: &ApprovalRecord,
    now: i64,
) -> Result<Option<StartupRecoveryResolution>, JournalError> {
    if approval.subject_type != ApprovalSubjectType::StartupRecovery {
        return Ok(None);
    }
    let Some(decision) = approval.decision else {
        return Ok(None);
    };
    if let Some(existing) = load_resolution_tx(connection, approval.approval_id.as_str())? {
        return Ok(Some(existing));
    }
    let recovery = connection
        .query_row(
            r#"
                SELECT
                    original_run_ulid,
                    session_ulid,
                    lease_ulid,
                    original_generation,
                    reconstruction_state_sha256
                FROM startup_recovery_actions_v1
                WHERE confirmation_ulid = ?1
            "#,
            params![approval.approval_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(|| {
            JournalError::InvalidArgument(format!(
                "startup recovery approval {} has no matching recovery action",
                approval.approval_id
            ))
        })?;
    let (original_run_id, session_id, lease_id, original_generation, reconstruction_sha256) =
        recovery;
    if approval.subject_id != original_run_id
        || approval.run_id != original_run_id
        || approval.session_id != session_id
    {
        return Err(JournalError::InvalidArgument(
            "startup recovery approval scope does not match its recovery action".to_owned(),
        ));
    }

    let continuation = if decision == ApprovalDecision::Allow {
        let continuation_run_id = Ulid::generate().to_string();
        let continuation_task_id = Ulid::generate().to_string();
        connection.execute(
            r#"
                INSERT INTO orchestrator_background_tasks (
                    task_ulid, task_kind, session_ulid, child_session_ulid,
                    parent_run_ulid, target_run_ulid, planned_child_run_ulid,
                    queued_input_ulid, owner_principal, device_id, channel,
                    state, priority, revision, execution_generation,
                    attempt_count, max_attempts, budget_tokens, delegation_json,
                    cancellation_context_json, not_before_unix_ms,
                    expires_at_unix_ms, notification_target_json, input_text,
                    payload_json, last_error, result_json, created_at_unix_ms,
                    updated_at_unix_ms, started_at_unix_ms, completed_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, NULL, ?4, NULL, ?5, NULL, ?6, ?7, ?8,
                    ?9, 100, 0, 0, 0, 1, ?10, NULL, NULL, NULL, NULL, NULL,
                    ?11, ?12, NULL, NULL, ?13, ?13, NULL, NULL
                )
            "#,
            params![
                continuation_task_id,
                AuxiliaryTaskKind::BackgroundPrompt.as_str(),
                session_id,
                original_run_id,
                continuation_run_id,
                approval.principal,
                approval.device_id,
                approval.channel,
                AuxiliaryTaskState::Queued.as_str(),
                CONTINUATION_BUDGET_TOKENS,
                CONTINUATION_INSTRUCTION,
                json!({
                    "schema_version": 1,
                    "entry_point": "startup_recovery_confirmation",
                    "confirmation_id": approval.approval_id,
                    "recovered_from_run_id": original_run_id,
                    "recovery_lease_id": lease_id,
                    "original_generation": original_generation,
                    "reconstruction_state_sha256": reconstruction_sha256,
                    "replay_mutations": false,
                })
                .to_string(),
                now,
            ],
        )?;
        Some(ContinuationRunDescriptor {
            recovered_from_run_id: original_run_id.clone(),
            continuation_run_id,
            continuation_task_id,
            session_id: session_id.clone(),
            budget_tokens: CONTINUATION_BUDGET_TOKENS as u64,
            reconstruction_state_sha256: reconstruction_sha256,
        })
    } else {
        None
    };
    let resolution = StartupRecoveryResolution {
        confirmation_id: approval.approval_id.clone(),
        original_run_id: original_run_id.clone(),
        decision: decision.as_str().to_owned(),
        continuation,
        resolved_at_unix_ms: now,
        schema_version: 1,
    };
    let resolution_json = serde_json::to_string(&resolution)?;
    let resolution_sha256 = sha256_hex(resolution_json.as_bytes());
    connection.execute(
        r#"
            INSERT INTO startup_recovery_resolutions_v1 (
                confirmation_ulid, original_run_ulid, decision,
                continuation_task_ulid, continuation_run_ulid,
                resolution_json, resolution_sha256, resolved_at_unix_ms,
                schema_version
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1)
        "#,
        params![
            resolution.confirmation_id,
            resolution.original_run_id,
            resolution.decision,
            resolution.continuation.as_ref().map(|value| value.continuation_task_id.as_str()),
            resolution.continuation.as_ref().map(|value| value.continuation_run_id.as_str()),
            resolution_json,
            resolution_sha256,
            now,
        ],
    )?;
    Ok(Some(resolution))
}

impl JournalStore {
    /// Returns recent immutable recovery actions for diagnostics and QA.
    ///
    /// # Errors
    /// Returns a journal error when an action digest or contract is invalid.
    pub(crate) fn recent_startup_recovery_actions(
        &self,
        limit: usize,
    ) -> Result<Vec<StartupRecoveryAction>, JournalError> {
        let limit = i64::try_from(limit.clamp(1, 64)).unwrap_or(64);
        let connection = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = connection.prepare(
            r#"
                SELECT action_json, action_sha256
                FROM startup_recovery_actions_v1
                ORDER BY created_at_unix_ms DESC, original_run_ulid DESC
                LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map(params![limit], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let stored = rows.collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        let mut actions = Vec::new();
        for (action_json, expected_sha256) in stored {
            if sha256_hex(action_json.as_bytes()) != expected_sha256 {
                return Err(JournalError::InvalidArgument(
                    "startup recovery action digest mismatch".to_owned(),
                ));
            }
            let mut action = serde_json::from_str::<StartupRecoveryAction>(action_json.as_str())?;
            if action.schema_version != 1 {
                return Err(JournalError::InvalidArgument(
                    "startup recovery action schema is unsupported".to_owned(),
                ));
            }
            if let Some(confirmation_id) = action.confirmation_id.as_deref() {
                action.resolution = load_resolution_tx(&connection, confirmation_id)?;
            }
            actions.push(action);
        }
        Ok(actions)
    }
}

fn load_resolution_tx(
    connection: &Connection,
    confirmation_id: &str,
) -> Result<Option<StartupRecoveryResolution>, JournalError> {
    let stored = connection
        .query_row(
            r#"
                SELECT resolution_json, resolution_sha256
                FROM startup_recovery_resolutions_v1
                WHERE confirmation_ulid = ?1
            "#,
            params![confirmation_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((resolution_json, expected_sha256)) = stored else {
        return Ok(None);
    };
    if sha256_hex(resolution_json.as_bytes()) != expected_sha256 {
        return Err(JournalError::InvalidArgument(
            "startup recovery resolution digest mismatch".to_owned(),
        ));
    }
    let resolution = serde_json::from_str::<StartupRecoveryResolution>(resolution_json.as_str())?;
    if resolution.schema_version != 1 || resolution.confirmation_id != confirmation_id {
        return Err(JournalError::InvalidArgument(
            "startup recovery resolution contract is invalid".to_owned(),
        ));
    }
    Ok(Some(resolution))
}

fn load_action_tx(
    connection: &Connection,
    original_run_id: &str,
) -> Result<Option<StartupRecoveryAction>, JournalError> {
    let stored = connection
        .query_row(
            r#"
                SELECT action_json, action_sha256
                FROM startup_recovery_actions_v1
                WHERE original_run_ulid = ?1
            "#,
            params![original_run_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    let Some((action_json, expected_sha256)) = stored else {
        return Ok(None);
    };
    if sha256_hex(action_json.as_bytes()) != expected_sha256 {
        return Err(JournalError::InvalidArgument(
            "startup recovery action digest mismatch".to_owned(),
        ));
    }
    let action = serde_json::from_str::<StartupRecoveryAction>(action_json.as_str())?;
    if action.schema_version != 1 || action.original_run_id != original_run_id {
        return Err(JournalError::InvalidArgument(
            "startup recovery action contract is invalid".to_owned(),
        ));
    }
    Ok(Some(action))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
