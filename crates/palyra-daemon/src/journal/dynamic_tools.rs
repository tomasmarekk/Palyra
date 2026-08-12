//! Durable signed dynamic-tool versions and activation pointers.

use palyra_skills::{
    decide_dynamic_tool_activation, decide_dynamic_tool_rollback,
    dynamic_tool_runtime_eval_evidence_sha256, verify_signed_dynamic_tool_artifact,
    DynamicToolHostGate, DynamicToolRuntimeEvalEvidenceV1, SignedToolArtifact,
    ToolActivationDecision,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    ApprovalCreateRequest, ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot,
    ApprovalPromptOption, ApprovalPromptRecord, ApprovalRecord, ApprovalRiskLevel,
    ApprovalSubjectType, JournalError, JournalStore,
};

const DYNAMIC_TOOL_REVIEW_TIMEOUT_SECONDS: u32 = 900;
const DYNAMIC_TOOL_REVIEW_VALIDITY_MS: i64 = 900_000;

pub(super) const MIGRATION_104_SQL: &str = r#"
    CREATE TABLE dynamic_tool_versions_v1 (
        artifact_sha256 TEXT PRIMARY KEY NOT NULL,
        tool_name TEXT NOT NULL,
        proposal_sha256 TEXT NOT NULL,
        implementation_sha256 TEXT NOT NULL,
        eval_pack_sha256 TEXT NOT NULL,
        runtime_eval_sha256 TEXT NOT NULL,
        runtime_eval_reason_codes_json TEXT NOT NULL,
        runtime_eval_passed INTEGER NOT NULL CHECK (runtime_eval_passed IN (0, 1)),
        artifact_json TEXT NOT NULL,
        lifecycle_state TEXT NOT NULL CHECK (
            lifecycle_state IN ('proposed', 'active', 'superseded', 'rolled_back')
        ),
        approval_subject_id TEXT NOT NULL,
        approval_policy_sha256 TEXT NOT NULL,
        host_policy_sha256 TEXT NOT NULL,
        review_session_id TEXT NOT NULL,
        review_run_id TEXT NOT NULL,
        review_principal TEXT NOT NULL,
        review_device_id TEXT NOT NULL,
        review_channel TEXT,
        activated_from_sha256 TEXT,
        activation_decision_json TEXT,
        approval_id TEXT,
        approval_generation INTEGER,
        catalog_epoch INTEGER,
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX idx_dynamic_tool_versions_proposal
        ON dynamic_tool_versions_v1(proposal_sha256, implementation_sha256);
    CREATE INDEX idx_dynamic_tool_versions_name_epoch
        ON dynamic_tool_versions_v1(tool_name, catalog_epoch);

    CREATE TABLE dynamic_tool_active_v1 (
        tool_name TEXT PRIMARY KEY NOT NULL,
        artifact_sha256 TEXT NOT NULL REFERENCES dynamic_tool_versions_v1(artifact_sha256),
        approval_generation INTEGER NOT NULL,
        catalog_epoch INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL
    );

    CREATE TABLE dynamic_tool_catalog_state_v1 (
        singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
        catalog_epoch INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL
    );
    INSERT INTO dynamic_tool_catalog_state_v1(singleton, catalog_epoch, updated_at_unix_ms)
        VALUES (1, 1, 0);

    CREATE TABLE dynamic_tool_lifecycle_events_v1 (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_name TEXT NOT NULL,
        artifact_sha256 TEXT NOT NULL,
        previous_artifact_sha256 TEXT,
        lifecycle_state TEXT NOT NULL,
        approval_id TEXT,
        approval_generation INTEGER,
        catalog_epoch INTEGER NOT NULL,
        reason_code TEXT NOT NULL,
        runtime_eval_sha256 TEXT NOT NULL,
        occurred_at_unix_ms INTEGER NOT NULL
    );
    CREATE INDEX idx_dynamic_tool_lifecycle_name_event
        ON dynamic_tool_lifecycle_events_v1(tool_name, event_id);
    CREATE TRIGGER dynamic_tool_lifecycle_events_no_update
        BEFORE UPDATE ON dynamic_tool_lifecycle_events_v1
        BEGIN SELECT RAISE(ABORT, 'dynamic tool lifecycle events are append-only'); END;
    CREATE TRIGGER dynamic_tool_lifecycle_events_no_delete
        BEFORE DELETE ON dynamic_tool_lifecycle_events_v1
        BEGIN SELECT RAISE(ABORT, 'dynamic tool lifecycle events are append-only'); END;
"#;

/// Durable projection loaded by standard catalog and dispatch paths.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DynamicToolActiveRecord {
    pub(crate) artifact: SignedToolArtifact,
    pub(crate) decision: ToolActivationDecision,
    pub(crate) approval_id: String,
    pub(crate) approval_subject_id: String,
    pub(crate) runtime_eval: DynamicToolRuntimeEvalEvidenceV1,
    pub(crate) registry_catalog_epoch: u64,
    pub(crate) activated_at_unix_ms: i64,
}

/// Generation and digest authority used to derive a host-owned decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynamicToolActivationContext {
    pub(crate) catalog_epoch: u64,
    pub(crate) approval_generation: u64,
    pub(crate) active_artifact_sha256: Option<String>,
}

/// Durable operator identity bound to one exact proposal review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynamicToolReviewAuthority {
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) host_policy_sha256: String,
}

/// Redacted operator diagnostics for the dynamic registry.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct DynamicToolRegistryDiagnostics {
    pub(crate) catalog_epoch: u64,
    pub(crate) active_tools: usize,
    pub(crate) retained_versions: usize,
    pub(crate) latest_reason_code: Option<String>,
}

/// Binds an approval to the exact signed payload, capabilities, and generation.
#[must_use]
pub(crate) fn dynamic_tool_approval_subject(
    artifact: &SignedToolArtifact,
    context: &DynamicToolActivationContext,
    runtime_eval_sha256: &str,
) -> String {
    let capabilities_sha256 =
        dynamic_tool_capabilities_sha256(artifact.proposal.capability_needs.as_slice());
    format!(
        "dynamic-tool:{}:{}:{}:{}:{}",
        artifact.artifact_sha256,
        capabilities_sha256,
        runtime_eval_sha256,
        context.catalog_epoch,
        context.approval_generation
    )
}

/// Computes the exact host-review policy snapshot bound to the proposal.
#[must_use]
pub(crate) fn dynamic_tool_review_policy_hash(
    artifact: &SignedToolArtifact,
    context: &DynamicToolActivationContext,
    runtime_eval_sha256: &str,
    host_policy_sha256: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.dynamic-tool.review-policy.v1\0");
    update_hash_field(&mut hasher, artifact.artifact_sha256.as_bytes());
    update_hash_field(
        &mut hasher,
        dynamic_tool_capabilities_sha256(artifact.proposal.capability_needs.as_slice()).as_bytes(),
    );
    update_hash_field(&mut hasher, runtime_eval_sha256.as_bytes());
    update_hash_field(&mut hasher, host_policy_sha256.as_bytes());
    hasher.update(context.catalog_epoch.to_le_bytes());
    hasher.update(context.approval_generation.to_le_bytes());
    hex::encode(hasher.finalize())
}

/// Builds the only approval prompt accepted by the activation transaction.
#[must_use]
pub(crate) fn dynamic_tool_approval_request(
    approval_id: String,
    artifact: &SignedToolArtifact,
    context: &DynamicToolActivationContext,
    runtime_eval: &DynamicToolRuntimeEvalEvidenceV1,
    authority: &DynamicToolReviewAuthority,
) -> ApprovalCreateRequest {
    let subject_id =
        dynamic_tool_approval_subject(artifact, context, runtime_eval.evidence_sha256.as_str());
    ApprovalCreateRequest {
        approval_id,
        session_id: authority.session_id.clone(),
        run_id: authority.run_id.clone(),
        principal: authority.principal.clone(),
        device_id: authority.device_id.clone(),
        channel: authority.channel.clone(),
        subject_type: ApprovalSubjectType::Tool,
        subject_id: subject_id.clone(),
        request_summary: format!("Review signed dynamic tool {}", artifact.proposal.tool_name),
        policy_snapshot: ApprovalPolicySnapshot {
            policy_id: "dynamic_tool.host_review.v1".to_owned(),
            policy_hash: dynamic_tool_review_policy_hash(
                artifact,
                context,
                runtime_eval.evidence_sha256.as_str(),
                authority.host_policy_sha256.as_str(),
            ),
            evaluation_summary:
                "signature, trust, rollout, allowlist, capabilities, and runtime eval passed"
                    .to_owned(),
        },
        prompt: ApprovalPromptRecord {
            title: "Activate signed dynamic tool".to_owned(),
            risk_level: ApprovalRiskLevel::High,
            subject_id: subject_id.clone(),
            summary: format!(
                "Activate {} at catalog epoch {}",
                artifact.proposal.tool_name,
                context.catalog_epoch.saturating_add(1)
            ),
            options: vec![
                ApprovalPromptOption {
                    option_id: "allow_once".to_owned(),
                    label: "Activate this version".to_owned(),
                    description: "Allow only this exact signed digest and generation.".to_owned(),
                    default_selected: false,
                    decision_scope: ApprovalDecisionScope::Once,
                    timebox_ttl_ms: None,
                },
                ApprovalPromptOption {
                    option_id: "deny".to_owned(),
                    label: "Deny".to_owned(),
                    description: "Keep the proposal inert.".to_owned(),
                    default_selected: true,
                    decision_scope: ApprovalDecisionScope::Once,
                    timebox_ttl_ms: None,
                },
            ],
            timeout_seconds: DYNAMIC_TOOL_REVIEW_TIMEOUT_SECONDS,
            details_json: serde_json::json!({
                "schema_version": 1,
                "tool_name": artifact.proposal.tool_name,
                "artifact_sha256": artifact.artifact_sha256,
                "proposal_sha256": artifact.provenance.proposal_sha256,
                "static_preflight_sha256": artifact.eval_pack.pack_sha256,
                "runtime_eval_sha256": runtime_eval.evidence_sha256,
                "runtime_eval_reason_codes": runtime_eval.case_reason_codes,
                "capability_names": artifact.proposal.capability_needs,
                "approval_generation": context.approval_generation,
                "catalog_epoch": context.catalog_epoch,
            })
            .to_string(),
            policy_explanation:
                "Activation requires a fresh one-shot operator decision for this exact digest."
                    .to_owned(),
        },
    }
}

impl JournalStore {
    /// Returns the current catalog and per-tool generation authority.
    pub(crate) fn dynamic_tool_activation_context(
        &self,
        tool_name: &str,
    ) -> Result<DynamicToolActivationContext, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let catalog_epoch = guard.query_row(
            "SELECT catalog_epoch FROM dynamic_tool_catalog_state_v1 WHERE singleton = 1",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let active = guard
            .query_row(
                r#"
                    SELECT artifact_sha256, approval_generation
                    FROM dynamic_tool_active_v1
                    WHERE tool_name = ?1
                "#,
                params![tool_name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        let (active_artifact_sha256, approval_generation) = active
            .map(|(digest, generation)| {
                Ok::<_, JournalError>((
                    Some(digest),
                    u64::try_from(generation)
                        .map_err(|_| {
                            JournalError::InvalidArgument(
                                "invalid dynamic tool approval generation".to_owned(),
                            )
                        })?
                        .checked_add(1)
                        .ok_or_else(|| {
                            JournalError::InvalidArgument(
                                "dynamic tool approval generation exhausted".to_owned(),
                            )
                        })?,
                ))
            })
            .transpose()?
            .unwrap_or((None, 1));
        Ok(DynamicToolActivationContext {
            catalog_epoch: u64::try_from(catalog_epoch).map_err(|_| {
                JournalError::InvalidArgument("invalid dynamic tool catalog epoch".to_owned())
            })?,
            approval_generation,
            active_artifact_sha256,
        })
    }

    /// Persists a verified proposal before an operator decision exists.
    pub(crate) fn record_dynamic_tool_proposal(
        &self,
        artifact: &SignedToolArtifact,
        runtime_eval: &DynamicToolRuntimeEvalEvidenceV1,
        authority: &DynamicToolReviewAuthority,
        approval_subject_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), JournalError> {
        verify_signed_dynamic_tool_artifact(artifact).map_err(|_| {
            JournalError::InvalidArgument("dynamic_tool.artifact_verification_failed".to_owned())
        })?;
        validate_review_authority(authority)?;
        runtime_eval.validate().map_err(|_| {
            JournalError::InvalidArgument("dynamic_tool.runtime_eval_failed".to_owned())
        })?;
        if runtime_eval.evidence_sha256
            != dynamic_tool_runtime_eval_evidence_sha256(
                artifact,
                runtime_eval.case_reason_codes.as_slice(),
            )
        {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.runtime_eval_evidence_mismatch".to_owned(),
            ));
        }
        let context = self.dynamic_tool_activation_context(artifact.proposal.tool_name.as_str())?;
        if approval_subject_id
            != dynamic_tool_approval_subject(
                artifact,
                &context,
                runtime_eval.evidence_sha256.as_str(),
            )
        {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.approval_subject_mismatch".to_owned(),
            ));
        }
        let artifact_json = serde_json::to_string(artifact)?;
        let runtime_eval_reasons_json = serde_json::to_string(&runtime_eval.case_reason_codes)?;
        let approval_policy_sha256 = dynamic_tool_review_policy_hash(
            artifact,
            &context,
            runtime_eval.evidence_sha256.as_str(),
            authority.host_policy_sha256.as_str(),
        );
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let inserted = transaction.execute(
            r#"
                INSERT OR IGNORE INTO dynamic_tool_versions_v1 (
                    artifact_sha256, tool_name, proposal_sha256,
                    implementation_sha256, eval_pack_sha256, runtime_eval_sha256,
                    runtime_eval_reason_codes_json, runtime_eval_passed, artifact_json,
                    lifecycle_state, approval_subject_id, approval_policy_sha256,
                    host_policy_sha256, review_session_id, review_run_id, review_principal,
                    review_device_id, review_channel,
                    activated_from_sha256, activation_decision_json, approval_id,
                    approval_generation, catalog_epoch, reason_code,
                    created_at_unix_ms, updated_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, 'proposed', ?9, ?10,
                    ?11, ?12, ?13, ?14, ?15, ?16, NULL, NULL, NULL, NULL, NULL,
                    'dynamic_tool.proposed', ?17, ?17
                )
            "#,
            params![
                artifact.artifact_sha256,
                artifact.proposal.tool_name,
                artifact.provenance.proposal_sha256,
                artifact.implementation_sha256,
                artifact.eval_pack.pack_sha256,
                runtime_eval.evidence_sha256,
                runtime_eval_reasons_json,
                artifact_json,
                approval_subject_id,
                approval_policy_sha256,
                authority.host_policy_sha256,
                authority.session_id,
                authority.run_id,
                authority.principal,
                authority.device_id,
                authority.channel,
                now_unix_ms,
            ],
        )?;
        let (
            existing_subject,
            lifecycle_state,
            existing_session_id,
            existing_run_id,
            existing_principal,
            existing_device_id,
            existing_channel,
            existing_host_policy_sha256,
        ) = transaction.query_row(
            r#"
                SELECT approval_subject_id, lifecycle_state, review_session_id,
                       review_run_id, review_principal, review_device_id, review_channel,
                       host_policy_sha256
                FROM dynamic_tool_versions_v1
                WHERE artifact_sha256 = ?1
            "#,
            params![artifact.artifact_sha256],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, String>(7)?,
                ))
            },
        )?;
        let authority_changed = existing_session_id != authority.session_id
            || existing_run_id != authority.run_id
            || existing_principal != authority.principal
            || existing_device_id != authority.device_id
            || existing_channel != authority.channel
            || existing_host_policy_sha256 != authority.host_policy_sha256;
        let reproposed = inserted == 0
            && (existing_subject != approval_subject_id || authority_changed)
            && lifecycle_state != "active";
        if inserted == 0
            && (existing_subject != approval_subject_id || authority_changed)
            && !reproposed
        {
            return Err(JournalError::InvalidArgument("dynamic_tool.version_is_active".to_owned()));
        }
        if reproposed {
            transaction.execute(
                r#"
                    UPDATE dynamic_tool_versions_v1
                    SET lifecycle_state = 'proposed',
                        approval_subject_id = ?2,
                        runtime_eval_sha256 = ?3,
                        runtime_eval_reason_codes_json = ?4,
                        runtime_eval_passed = 1,
                        approval_policy_sha256 = ?5,
                        host_policy_sha256 = ?6,
                        review_session_id = ?7,
                        review_run_id = ?8,
                        review_principal = ?9,
                        review_device_id = ?10,
                        review_channel = ?11,
                        activated_from_sha256 = NULL,
                        activation_decision_json = NULL,
                        approval_id = NULL,
                        approval_generation = NULL,
                        catalog_epoch = NULL,
                        reason_code = 'dynamic_tool.rollback_proposed',
                        updated_at_unix_ms = ?12
                    WHERE artifact_sha256 = ?1
                "#,
                params![
                    artifact.artifact_sha256,
                    approval_subject_id,
                    runtime_eval.evidence_sha256,
                    runtime_eval_reasons_json,
                    approval_policy_sha256,
                    authority.host_policy_sha256,
                    authority.session_id,
                    authority.run_id,
                    authority.principal,
                    authority.device_id,
                    authority.channel,
                    now_unix_ms
                ],
            )?;
        }
        if inserted == 1 || reproposed {
            let catalog_epoch = i64::try_from(context.catalog_epoch).map_err(|_| {
                JournalError::InvalidArgument("invalid dynamic tool catalog epoch".to_owned())
            })?;
            transaction.execute(
                r#"
                    INSERT INTO dynamic_tool_lifecycle_events_v1 (
                        tool_name, artifact_sha256, previous_artifact_sha256,
                        lifecycle_state, approval_id, approval_generation,
                        catalog_epoch, reason_code, runtime_eval_sha256, occurred_at_unix_ms
                    ) VALUES (?1, ?2, ?3, 'proposed', NULL, NULL, ?4, ?5, ?6, ?7)
                "#,
                params![
                    artifact.proposal.tool_name,
                    artifact.artifact_sha256,
                    artifact.proposal.previous_artifact_sha256,
                    catalog_epoch,
                    if reproposed {
                        "dynamic_tool.rollback_proposed"
                    } else {
                        "dynamic_tool.proposed"
                    },
                    runtime_eval.evidence_sha256,
                    now_unix_ms,
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Activates a proposal only under an exact durable approval and CAS fence.
    pub(crate) fn activate_dynamic_tool(
        &self,
        artifact: &SignedToolArtifact,
        decision: &ToolActivationDecision,
        runtime_eval: &DynamicToolRuntimeEvalEvidenceV1,
        authority: &DynamicToolReviewAuthority,
        approval_id: &str,
        now_unix_ms: i64,
    ) -> Result<DynamicToolActiveRecord, JournalError> {
        verify_signed_dynamic_tool_artifact(artifact).map_err(|_| {
            JournalError::InvalidArgument("dynamic_tool.artifact_verification_failed".to_owned())
        })?;
        runtime_eval.validate().map_err(|_| {
            JournalError::InvalidArgument("dynamic_tool.runtime_eval_failed".to_owned())
        })?;
        if runtime_eval.evidence_sha256
            != dynamic_tool_runtime_eval_evidence_sha256(
                artifact,
                runtime_eval.case_reason_codes.as_slice(),
            )
        {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.runtime_eval_evidence_mismatch".to_owned(),
            ));
        }
        validate_review_authority(authority)?;
        if let Some(active) =
            self.active_dynamic_tool(artifact.proposal.tool_name.as_str())?.filter(|active| {
                active.artifact.artifact_sha256 == artifact.artifact_sha256
                    && active.approval_id == approval_id
                    && active.runtime_eval == *runtime_eval
                    && active.decision == *decision
            })
        {
            return Ok(active);
        }
        let context = self.dynamic_tool_activation_context(artifact.proposal.tool_name.as_str())?;
        let approval_subject_id = dynamic_tool_approval_subject(
            artifact,
            &context,
            runtime_eval.evidence_sha256.as_str(),
        );
        let artifact_json = serde_json::to_string(artifact)?;
        let decision_json = serde_json::to_string(decision)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let proposed = transaction
            .query_row(
                r#"
                    SELECT approval_subject_id, runtime_eval_sha256,
                           runtime_eval_reason_codes_json, runtime_eval_passed,
                           approval_policy_sha256, host_policy_sha256,
                           review_session_id, review_run_id, review_principal,
                           review_device_id, review_channel
                    FROM dynamic_tool_versions_v1
                    WHERE artifact_sha256 = ?1
                "#,
                params![artifact.artifact_sha256],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, String>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, Option<String>>(10)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            stored_subject,
            stored_eval_sha256,
            stored_eval_reasons_json,
            stored_eval_passed,
            stored_policy_sha256,
            stored_host_policy_sha256,
            stored_session_id,
            stored_run_id,
            stored_principal,
            stored_device_id,
            stored_channel,
        )) = proposed
        else {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.proposal_missing_or_stale".to_owned(),
            ));
        };
        if stored_subject != approval_subject_id
            || stored_eval_sha256 != runtime_eval.evidence_sha256
            || stored_eval_reasons_json != serde_json::to_string(&runtime_eval.case_reason_codes)?
            || stored_eval_passed != 1
            || stored_host_policy_sha256 != authority.host_policy_sha256
            || stored_session_id != authority.session_id
            || stored_run_id != authority.run_id
            || stored_principal != authority.principal
            || stored_device_id != authority.device_id
            || stored_channel != authority.channel
        {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.runtime_eval_evidence_mismatch".to_owned(),
            ));
        }
        let approval = super::load_approval_by_id(&transaction, approval_id)?.ok_or_else(|| {
            JournalError::InvalidArgument("dynamic_tool.host_approval_missing_or_stale".to_owned())
        })?;
        let current = transaction
            .query_row(
                r#"
                    SELECT active.artifact_sha256, versions.activation_decision_json
                    FROM dynamic_tool_active_v1 AS active
                    INNER JOIN dynamic_tool_versions_v1 AS versions
                        ON versions.artifact_sha256 = active.artifact_sha256
                    WHERE active.tool_name = ?1
                "#,
                params![artifact.proposal.tool_name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?;
        let (current_artifact_sha256, current_decision) = current
            .map(|(digest, decision_json)| {
                let decision = decision_json
                    .map(|json| serde_json::from_str::<ToolActivationDecision>(json.as_str()))
                    .transpose()?;
                Ok::<_, JournalError>((Some(digest), decision))
            })
            .transpose()?
            .unwrap_or((None, None));
        if current_artifact_sha256 != context.active_artifact_sha256 {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.active_generation_stale".to_owned(),
            ));
        }
        validate_activation_authority(DynamicActivationAuthorityCheck {
            artifact,
            decision,
            approval: &approval,
            runtime_eval,
            authority,
            context: &context,
            approval_subject_id: approval_subject_id.as_str(),
            approval_policy_sha256: stored_policy_sha256.as_str(),
            resolution_deadline_unix_ms: now_unix_ms,
            current_decision: current_decision.as_ref(),
        })?;
        let actual_epoch = transaction.query_row(
            "SELECT catalog_epoch FROM dynamic_tool_catalog_state_v1 WHERE singleton = 1",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if u64::try_from(actual_epoch).ok() != Some(context.catalog_epoch) {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.catalog_epoch_stale".to_owned(),
            ));
        }
        let consumed = transaction.execute(
            r#"
                INSERT INTO approval_consumptions (
                    approval_ulid, consumed_at_unix_ms, consume_reason
                )
                SELECT approval_ulid, ?2, 'dynamic_tool.activation_committed'
                FROM approvals
                WHERE approval_ulid = ?1
                    AND decision = 'allow'
                    AND decision_scope = 'once'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM approval_consumptions
                        WHERE approval_consumptions.approval_ulid = approvals.approval_ulid
                    )
            "#,
            params![approval.approval_id, now_unix_ms],
        )?;
        if consumed != 1 {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.host_approval_consumed".to_owned(),
            ));
        }
        if let Some(previous) = &current_artifact_sha256 {
            transaction.execute(
                r#"
                    UPDATE dynamic_tool_versions_v1
                    SET lifecycle_state = CASE
                            WHEN ?2 = 'dynamic_tool.rollback_activated' THEN 'rolled_back'
                            ELSE 'superseded'
                        END,
                        updated_at_unix_ms = ?3
                    WHERE artifact_sha256 = ?1
                "#,
                params![previous, decision.reason_code, now_unix_ms],
            )?;
        }
        let updated_version = transaction.execute(
            r#"
                UPDATE dynamic_tool_versions_v1
                SET artifact_json = ?2,
                    lifecycle_state = 'active',
                    approval_id = ?3,
                    activated_from_sha256 = ?4,
                    activation_decision_json = ?5,
                    approval_generation = ?6,
                    catalog_epoch = ?7,
                    reason_code = ?8,
                    updated_at_unix_ms = ?9
                WHERE artifact_sha256 = ?1
            "#,
            params![
                artifact.artifact_sha256,
                artifact_json,
                approval.approval_id,
                current_artifact_sha256,
                decision_json,
                i64::try_from(decision.approval_generation).map_err(|_| {
                    JournalError::InvalidArgument(
                        "dynamic tool approval generation exceeds storage range".to_owned(),
                    )
                })?,
                i64::try_from(decision.catalog_epoch).map_err(|_| {
                    JournalError::InvalidArgument(
                        "dynamic tool catalog epoch exceeds storage range".to_owned(),
                    )
                })?,
                decision.reason_code,
                now_unix_ms,
            ],
        )?;
        if updated_version != 1 {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.proposal_missing_or_stale".to_owned(),
            ));
        }
        transaction.execute(
            r#"
                INSERT INTO dynamic_tool_active_v1 (
                    tool_name, artifact_sha256, approval_generation,
                    catalog_epoch, updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(tool_name) DO UPDATE SET
                    artifact_sha256 = excluded.artifact_sha256,
                    approval_generation = excluded.approval_generation,
                    catalog_epoch = excluded.catalog_epoch,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                artifact.proposal.tool_name,
                artifact.artifact_sha256,
                i64::try_from(decision.approval_generation).unwrap_or(i64::MAX),
                i64::try_from(decision.catalog_epoch).unwrap_or(i64::MAX),
                now_unix_ms,
            ],
        )?;
        let advanced_epoch = transaction.execute(
            r#"
                UPDATE dynamic_tool_catalog_state_v1
                SET catalog_epoch = ?1, updated_at_unix_ms = ?2
                WHERE singleton = 1 AND catalog_epoch = ?3
            "#,
            params![
                i64::try_from(decision.catalog_epoch).unwrap_or(i64::MAX),
                now_unix_ms,
                actual_epoch,
            ],
        )?;
        if advanced_epoch != 1 {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.catalog_epoch_stale".to_owned(),
            ));
        }
        transaction.execute(
            r#"
                INSERT INTO dynamic_tool_lifecycle_events_v1 (
                    tool_name, artifact_sha256, previous_artifact_sha256,
                    lifecycle_state, approval_id, approval_generation,
                    catalog_epoch, reason_code, runtime_eval_sha256, occurred_at_unix_ms
                ) VALUES (?1, ?2, ?3, 'active', ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            params![
                artifact.proposal.tool_name,
                artifact.artifact_sha256,
                current_artifact_sha256,
                approval.approval_id,
                i64::try_from(decision.approval_generation).unwrap_or(i64::MAX),
                i64::try_from(decision.catalog_epoch).unwrap_or(i64::MAX),
                decision.reason_code,
                runtime_eval.evidence_sha256,
                now_unix_ms,
            ],
        )?;
        transaction.commit()?;
        Ok(DynamicToolActiveRecord {
            artifact: artifact.clone(),
            decision: decision.clone(),
            approval_id: approval.approval_id.clone(),
            approval_subject_id,
            runtime_eval: runtime_eval.clone(),
            registry_catalog_epoch: decision.catalog_epoch,
            activated_at_unix_ms: now_unix_ms,
        })
    }

    /// Loads one exact active version; malformed durable state fails closed.
    pub(crate) fn active_dynamic_tool(
        &self,
        tool_name: &str,
    ) -> Result<Option<DynamicToolActiveRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_active_record(&guard, Some(tool_name)).map(|mut records| records.pop())
    }

    /// Lists active versions for projection into the standard registry.
    pub(crate) fn active_dynamic_tools(
        &self,
    ) -> Result<Vec<DynamicToolActiveRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        load_active_record(&guard, None)
    }

    /// Returns hash-only lifecycle diagnostics without artifact payloads.
    pub(crate) fn dynamic_tool_registry_diagnostics(
        &self,
    ) -> Result<DynamicToolRegistryDiagnostics, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let catalog_epoch = guard.query_row(
            "SELECT catalog_epoch FROM dynamic_tool_catalog_state_v1 WHERE singleton = 1",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let active_tools =
            guard.query_row("SELECT COUNT(*) FROM dynamic_tool_active_v1", [], |row| {
                row.get::<_, i64>(0)
            })?;
        let retained_versions =
            guard.query_row("SELECT COUNT(*) FROM dynamic_tool_versions_v1", [], |row| {
                row.get::<_, i64>(0)
            })?;
        let latest_reason_code = guard
            .query_row(
                r#"
                    SELECT reason_code
                    FROM dynamic_tool_lifecycle_events_v1
                    ORDER BY event_id DESC LIMIT 1
                "#,
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(DynamicToolRegistryDiagnostics {
            catalog_epoch: u64::try_from(catalog_epoch).unwrap_or(0),
            active_tools: usize::try_from(active_tools).unwrap_or(0),
            retained_versions: usize::try_from(retained_versions).unwrap_or(0),
            latest_reason_code,
        })
    }
}

struct DynamicActivationAuthorityCheck<'a> {
    artifact: &'a SignedToolArtifact,
    decision: &'a ToolActivationDecision,
    approval: &'a ApprovalRecord,
    runtime_eval: &'a DynamicToolRuntimeEvalEvidenceV1,
    authority: &'a DynamicToolReviewAuthority,
    context: &'a DynamicToolActivationContext,
    approval_subject_id: &'a str,
    approval_policy_sha256: &'a str,
    resolution_deadline_unix_ms: i64,
    current_decision: Option<&'a ToolActivationDecision>,
}

fn validate_activation_authority(
    check: DynamicActivationAuthorityCheck<'_>,
) -> Result<(), JournalError> {
    let DynamicActivationAuthorityCheck {
        artifact,
        decision,
        approval,
        runtime_eval,
        authority,
        context,
        approval_subject_id,
        approval_policy_sha256,
        resolution_deadline_unix_ms,
        current_decision,
    } = check;
    let gate = DynamicToolHostGate {
        host_validated: true,
        policy_approved: true,
        capability_review_approved: true,
        eval_approved: true,
        expected_catalog_epoch: context.catalog_epoch,
        current_catalog_epoch: context.catalog_epoch,
        approval_generation: context.approval_generation,
        trusted_publisher: artifact.signature.publisher.clone(),
        trusted_public_key_base64: artifact.signature.public_key_base64.clone(),
        previous_active_artifact_sha256: context.active_artifact_sha256.clone(),
    };
    let expected_decision = current_decision.map_or_else(
        || decide_dynamic_tool_activation(artifact, &gate),
        |current| {
            if current.rollback_artifact_sha256.as_deref()
                == Some(artifact.artifact_sha256.as_str())
            {
                decide_dynamic_tool_rollback(current, artifact, &gate)
            } else {
                decide_dynamic_tool_activation(artifact, &gate)
            }
        },
    );
    if *decision != expected_decision {
        return Err(JournalError::InvalidArgument(
            "dynamic_tool.activation_decision_stale".to_owned(),
        ));
    }
    let expected = dynamic_tool_approval_request(
        approval.approval_id.clone(),
        artifact,
        context,
        runtime_eval,
        authority,
    );
    let expected_details: serde_json::Value =
        serde_json::from_str(expected.prompt.details_json.as_str())?;
    let observed_details: serde_json::Value =
        serde_json::from_str(approval.prompt.details_json.as_str())?;
    let resolved_at_unix_ms = approval.resolved_at_unix_ms.unwrap_or(i64::MAX);
    if approval.subject_type != ApprovalSubjectType::Tool
        || approval.subject_id != approval_subject_id
        || approval.session_id != expected.session_id
        || approval.run_id != expected.run_id
        || approval.principal != expected.principal
        || approval.device_id != expected.device_id
        || approval.channel != expected.channel
        || approval.request_summary != expected.request_summary
        || approval.decision != Some(ApprovalDecision::Allow)
        || approval.decision_scope != Some(ApprovalDecisionScope::Once)
        || approval.decision_scope_ttl_ms.is_some()
        || resolved_at_unix_ms < approval.requested_at_unix_ms
        || resolved_at_unix_ms > resolution_deadline_unix_ms
        || resolved_at_unix_ms.saturating_sub(approval.requested_at_unix_ms)
            > DYNAMIC_TOOL_REVIEW_VALIDITY_MS
        || resolution_deadline_unix_ms.saturating_sub(resolved_at_unix_ms)
            > DYNAMIC_TOOL_REVIEW_VALIDITY_MS
        || approval.created_at_unix_ms != approval.requested_at_unix_ms
        || approval.updated_at_unix_ms < resolved_at_unix_ms
        || approval.decision_reason.as_deref().is_none_or(str::is_empty)
        || approval.policy_snapshot.policy_id != expected.policy_snapshot.policy_id
        || approval.policy_snapshot.policy_hash != approval_policy_sha256
        || approval.policy_snapshot != expected.policy_snapshot
        || approval.prompt.title != expected.prompt.title
        || approval.prompt.risk_level != expected.prompt.risk_level
        || approval.prompt.subject_id != expected.prompt.subject_id
        || approval.prompt.summary != expected.prompt.summary
        || approval.prompt.options != expected.prompt.options
        || approval.prompt.timeout_seconds != expected.prompt.timeout_seconds
        || observed_details != expected_details
        || approval.prompt.policy_explanation != expected.prompt.policy_explanation
    {
        return Err(JournalError::InvalidArgument(
            "dynamic_tool.host_approval_missing_or_stale".to_owned(),
        ));
    }
    Ok(())
}

fn validate_review_authority(authority: &DynamicToolReviewAuthority) -> Result<(), JournalError> {
    if authority.session_id.trim().is_empty()
        || authority.run_id.trim().is_empty()
        || authority.principal.trim().is_empty()
        || authority.device_id.trim().is_empty()
        || authority.host_policy_sha256.len() != 64
        || !authority
            .host_policy_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(JournalError::InvalidArgument(
            "dynamic_tool.review_authority_invalid".to_owned(),
        ));
    }
    Ok(())
}

fn load_active_record(
    connection: &rusqlite::Connection,
    tool_name: Option<&str>,
) -> Result<Vec<DynamicToolActiveRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                versions.artifact_json,
                versions.approval_id,
                versions.approval_subject_id,
                versions.updated_at_unix_ms,
                versions.artifact_sha256,
                versions.approval_generation,
                versions.catalog_epoch,
                versions.reason_code,
                versions.lifecycle_state,
                versions.runtime_eval_sha256,
                versions.runtime_eval_reason_codes_json,
                versions.runtime_eval_passed,
                active.approval_generation,
                active.catalog_epoch,
                (SELECT catalog_epoch FROM dynamic_tool_catalog_state_v1 WHERE singleton = 1),
                versions.approval_policy_sha256
                , versions.activated_from_sha256,
                versions.activation_decision_json,
                versions.review_session_id,
                versions.review_run_id,
                versions.review_principal,
                versions.review_device_id,
                versions.review_channel,
                versions.host_policy_sha256
            FROM dynamic_tool_active_v1 AS active
            INNER JOIN dynamic_tool_versions_v1 AS versions
                ON versions.artifact_sha256 = active.artifact_sha256
            WHERE (?1 IS NULL OR active.tool_name = ?1)
            ORDER BY active.tool_name
        "#,
    )?;
    let rows = statement.query_map(params![tool_name], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, String>(8)?,
            row.get::<_, String>(9)?,
            row.get::<_, String>(10)?,
            row.get::<_, i64>(11)?,
            row.get::<_, i64>(12)?,
            row.get::<_, i64>(13)?,
            row.get::<_, i64>(14)?,
            row.get::<_, String>(15)?,
            row.get::<_, Option<String>>(16)?,
            row.get::<_, String>(17)?,
            row.get::<_, String>(18)?,
            row.get::<_, String>(19)?,
            row.get::<_, String>(20)?,
            row.get::<_, String>(21)?,
            row.get::<_, Option<String>>(22)?,
            row.get::<_, String>(23)?,
        ))
    })?;
    let mut records = Vec::new();
    for row in rows {
        let (
            artifact_json,
            approval_id,
            approval_subject_id,
            activated_at_unix_ms,
            artifact_sha256,
            approval_generation,
            catalog_epoch,
            reason_code,
            lifecycle_state,
            runtime_eval_sha256,
            runtime_eval_reason_codes_json,
            runtime_eval_passed,
            active_approval_generation,
            active_catalog_epoch,
            global_catalog_epoch,
            approval_policy_sha256,
            activated_from_sha256,
            activation_decision_json,
            review_session_id,
            review_run_id,
            review_principal,
            review_device_id,
            review_channel,
            host_policy_sha256,
        ) = row?;
        let artifact: SignedToolArtifact = serde_json::from_str(artifact_json.as_str())?;
        verify_signed_dynamic_tool_artifact(&artifact).map_err(|_| {
            JournalError::InvalidArgument("dynamic_tool.durable_artifact_invalid".to_owned())
        })?;
        if artifact.artifact_sha256 != artifact_sha256
            || lifecycle_state != "active"
            || approval_generation != active_approval_generation
            || catalog_epoch != active_catalog_epoch
            || catalog_epoch > global_catalog_epoch
        {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.durable_pointer_mismatch".to_owned(),
            ));
        }
        let runtime_eval = DynamicToolRuntimeEvalEvidenceV1 {
            v: 1,
            passed: runtime_eval_passed == 1,
            evidence_sha256: runtime_eval_sha256,
            case_reason_codes: serde_json::from_str(runtime_eval_reason_codes_json.as_str())?,
        };
        runtime_eval.validate().map_err(|_| {
            JournalError::InvalidArgument("dynamic_tool.durable_runtime_eval_invalid".to_owned())
        })?;
        if runtime_eval.evidence_sha256
            != dynamic_tool_runtime_eval_evidence_sha256(
                &artifact,
                runtime_eval.case_reason_codes.as_slice(),
            )
        {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.durable_runtime_eval_mismatch".to_owned(),
            ));
        }
        let activation_context = DynamicToolActivationContext {
            catalog_epoch: u64::try_from(catalog_epoch)
                .ok()
                .and_then(|epoch| epoch.checked_sub(1))
                .ok_or_else(|| {
                    JournalError::InvalidArgument(
                        "dynamic_tool.durable_catalog_epoch_invalid".to_owned(),
                    )
                })?,
            approval_generation: u64::try_from(approval_generation).map_err(|_| {
                JournalError::InvalidArgument(
                    "dynamic_tool.durable_approval_generation_invalid".to_owned(),
                )
            })?,
            active_artifact_sha256: activated_from_sha256.clone(),
        };
        if approval_subject_id
            != dynamic_tool_approval_subject(
                &artifact,
                &activation_context,
                runtime_eval.evidence_sha256.as_str(),
            )
            || approval_policy_sha256
                != dynamic_tool_review_policy_hash(
                    &artifact,
                    &activation_context,
                    runtime_eval.evidence_sha256.as_str(),
                    host_policy_sha256.as_str(),
                )
        {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.durable_authority_binding_invalid".to_owned(),
            ));
        }
        let decision: ToolActivationDecision =
            serde_json::from_str(activation_decision_json.as_str())?;
        if decision.artifact_sha256 != artifact_sha256
            || decision.tool_name != artifact.proposal.tool_name
            || i64::try_from(decision.approval_generation).ok() != Some(approval_generation)
            || i64::try_from(decision.catalog_epoch).ok() != Some(catalog_epoch)
            || decision.reason_code != reason_code
        {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.durable_decision_mismatch".to_owned(),
            ));
        }
        let authority = DynamicToolReviewAuthority {
            session_id: review_session_id,
            run_id: review_run_id,
            principal: review_principal,
            device_id: review_device_id,
            channel: review_channel,
            host_policy_sha256,
        };
        validate_review_authority(&authority)?;
        let approval =
            super::load_approval_by_id(connection, approval_id.as_str())?.ok_or_else(|| {
                JournalError::InvalidArgument("dynamic_tool.durable_approval_missing".to_owned())
            })?;
        let current_decision = activated_from_sha256
            .as_deref()
            .map(|digest| {
                connection.query_row(
                    r#"
                        SELECT activation_decision_json
                        FROM dynamic_tool_versions_v1
                        WHERE artifact_sha256 = ?1
                    "#,
                    params![digest],
                    |row| row.get::<_, Option<String>>(0),
                )
            })
            .transpose()?
            .flatten()
            .map(|json| serde_json::from_str::<ToolActivationDecision>(json.as_str()))
            .transpose()?;
        validate_activation_authority(DynamicActivationAuthorityCheck {
            artifact: &artifact,
            decision: &decision,
            approval: &approval,
            runtime_eval: &runtime_eval,
            authority: &authority,
            context: &activation_context,
            approval_subject_id: approval_subject_id.as_str(),
            approval_policy_sha256: approval_policy_sha256.as_str(),
            resolution_deadline_unix_ms: activated_at_unix_ms,
            current_decision: current_decision.as_ref(),
        })?;
        let approval_consumed = connection.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1 FROM approval_consumptions
                    WHERE approval_ulid = ?1
                        AND consume_reason = 'dynamic_tool.activation_committed'
                )
            "#,
            params![approval_id],
            |row| row.get::<_, bool>(0),
        )?;
        if !approval_consumed {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.durable_approval_unconsumed".to_owned(),
            ));
        }
        let lifecycle_event_matches = connection.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM dynamic_tool_lifecycle_events_v1
                    WHERE tool_name = ?1
                        AND artifact_sha256 = ?2
                        AND previous_artifact_sha256 IS ?3
                        AND lifecycle_state = 'active'
                        AND approval_id = ?4
                        AND approval_generation = ?5
                        AND catalog_epoch = ?6
                        AND reason_code = ?7
                        AND runtime_eval_sha256 = ?8
                        AND occurred_at_unix_ms = ?9
                )
            "#,
            params![
                artifact.proposal.tool_name,
                artifact_sha256,
                activation_context.active_artifact_sha256,
                approval_id,
                approval_generation,
                catalog_epoch,
                decision.reason_code,
                runtime_eval.evidence_sha256,
                activated_at_unix_ms,
            ],
            |row| row.get::<_, bool>(0),
        )?;
        if !lifecycle_event_matches {
            return Err(JournalError::InvalidArgument(
                "dynamic_tool.durable_lifecycle_mismatch".to_owned(),
            ));
        }
        records.push(DynamicToolActiveRecord {
            artifact: artifact.clone(),
            decision,
            approval_id,
            approval_subject_id,
            runtime_eval,
            registry_catalog_epoch: u64::try_from(global_catalog_epoch).map_err(|_| {
                JournalError::InvalidArgument(
                    "dynamic_tool.durable_catalog_epoch_invalid".to_owned(),
                )
            })?,
            activated_at_unix_ms,
        });
    }
    Ok(records)
}

fn dynamic_tool_capabilities_sha256(capabilities: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.dynamic-tool.capabilities.v1\0");
    hasher.update((capabilities.len() as u64).to_le_bytes());
    for capability in capabilities {
        update_hash_field(&mut hasher, capability.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn update_hash_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use palyra_skills::{
        build_signed_dynamic_tool_artifact, decide_dynamic_tool_activation,
        decide_dynamic_tool_rollback, DeclarativeToolPlanV1, DeclarativeToolStepV1,
        DynamicToolBuildRequest, DynamicToolHostGate, DynamicToolImplementationType,
        DynamicToolProposalV1, DynamicToolSemanticsV1, SignedToolArtifact,
    };
    use serde_json::{json, Value};
    use ulid::Ulid;

    use super::*;
    use crate::{
        application::tool_registry::{
            dynamic_tool_registry_entry, ToolApprovalPosture, ToolReplaySafetyClass,
        },
        journal::{
            current_unix_ms, ApprovalDecision, ApprovalDecisionScope, ApprovalResolveRequest,
            JournalConfig,
        },
    };

    const SIGNING_KEY: [u8; 32] = [27; 32];
    const RUNTIME_REASONS: [&str; 6] = [
        "dynamic_tool.eval.authority_bounded",
        "dynamic_tool.eval.happy_path_passed",
        "dynamic_tool.eval.malformed_input_rejected",
        "dynamic_tool.eval.rollback_pointer_fenced",
        "dynamic_tool.eval.secret_output_clean",
        "dynamic_tool.eval.timeout_cancel_fenced",
    ];

    fn temp_db_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should follow the Unix epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("palyra-dynamic-tools-{}-{nonce}.sqlite3", std::process::id()))
    }

    fn config(path: PathBuf) -> JournalConfig {
        JournalConfig {
            db_path: path,
            hash_chain_enabled: true,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        }
    }

    fn schema(variant: bool) -> Value {
        if variant {
            json!({
                "type": "object",
                "properties": {
                    "value": {"type": "string"},
                    "tag": {"type": "string"}
                },
                "required": ["value"],
                "additionalProperties": false
            })
        } else {
            json!({
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "additionalProperties": false
            })
        }
    }

    fn artifact(previous: Option<String>, variant: bool) -> SignedToolArtifact {
        named_artifact("dynamic.echo", previous, variant)
    }

    fn named_artifact(
        tool_name: &str,
        previous: Option<String>,
        variant: bool,
    ) -> SignedToolArtifact {
        let proposal = DynamicToolProposalV1 {
            v: 1,
            tool_name: tool_name.to_owned(),
            description: "Echoes a bounded value through an approved child tool.".to_owned(),
            input_schema: schema(variant),
            output_schema: schema(false),
            capability_needs: vec!["tool:palyra.echo".to_owned()],
            deterministic_constraints: vec!["bounded_output".to_owned()],
            implementation_type: DynamicToolImplementationType::DeclarativeComposition,
            semantics: DynamicToolSemanticsV1 {
                mutating: false,
                idempotent: true,
                requires_approval: false,
                max_execution_ms: 1_000,
            },
            previous_artifact_sha256: previous,
        };
        let implementation = serde_json::to_vec(&DeclarativeToolPlanV1 {
            v: 1,
            steps: vec![DeclarativeToolStepV1 {
                step_id: "echo".to_owned(),
                tool_name: "palyra.echo".to_owned(),
                input_template: json!({"value": "${input.value}"}),
                timeout_ms: 100,
            }],
        })
        .expect("declarative fixture should serialize");
        build_signed_dynamic_tool_artifact(DynamicToolBuildRequest {
            proposal,
            implementation_bytes: implementation,
            allowed_capabilities: vec!["tool:palyra.echo".to_owned()],
            builder_id: "host-builder".to_owned(),
            publisher: "palyra.local".to_owned(),
            signing_key: SIGNING_KEY,
            built_at_unix_ms: 100_000,
        })
        .expect("signed fixture should build")
    }

    fn runtime_eval(artifact: &SignedToolArtifact) -> DynamicToolRuntimeEvalEvidenceV1 {
        let reasons = RUNTIME_REASONS.iter().map(ToString::to_string).collect::<Vec<_>>();
        DynamicToolRuntimeEvalEvidenceV1 {
            v: 1,
            passed: true,
            evidence_sha256: dynamic_tool_runtime_eval_evidence_sha256(
                artifact,
                reasons.as_slice(),
            ),
            case_reason_codes: reasons,
        }
    }

    fn authority() -> DynamicToolReviewAuthority {
        DynamicToolReviewAuthority {
            session_id: Ulid::generate().to_string(),
            run_id: Ulid::generate().to_string(),
            principal: "user:operator".to_owned(),
            device_id: Ulid::generate().to_string(),
            channel: Some("console".to_owned()),
            host_policy_sha256: "d".repeat(64),
        }
    }

    fn prepare_approval(
        store: &JournalStore,
        artifact: &SignedToolArtifact,
    ) -> (
        DynamicToolActivationContext,
        DynamicToolRuntimeEvalEvidenceV1,
        DynamicToolReviewAuthority,
        String,
    ) {
        let context = store
            .dynamic_tool_activation_context(artifact.proposal.tool_name.as_str())
            .expect("activation context should load");
        let runtime_eval = runtime_eval(artifact);
        let authority = authority();
        let subject = dynamic_tool_approval_subject(
            artifact,
            &context,
            runtime_eval.evidence_sha256.as_str(),
        );
        store
            .record_dynamic_tool_proposal(
                artifact,
                &runtime_eval,
                &authority,
                subject.as_str(),
                current_unix_ms().expect("clock should be available"),
            )
            .expect("proposal should persist");
        let approval_id = Ulid::generate().to_string();
        store
            .create_approval(&dynamic_tool_approval_request(
                approval_id.clone(),
                artifact,
                &context,
                &runtime_eval,
                &authority,
            ))
            .expect("approval should persist");
        store
            .resolve_approval(&ApprovalResolveRequest {
                approval_id: approval_id.clone(),
                decision: ApprovalDecision::Allow,
                decision_scope: ApprovalDecisionScope::Once,
                decision_reason: "operator_reviewed_exact_artifact".to_owned(),
                decision_scope_ttl_ms: None,
            })
            .expect("approval should resolve");
        (context, runtime_eval, authority, approval_id)
    }

    fn gate(
        artifact: &SignedToolArtifact,
        context: &DynamicToolActivationContext,
    ) -> DynamicToolHostGate {
        DynamicToolHostGate {
            host_validated: true,
            policy_approved: true,
            capability_review_approved: true,
            eval_approved: true,
            expected_catalog_epoch: context.catalog_epoch,
            current_catalog_epoch: context.catalog_epoch,
            approval_generation: context.approval_generation,
            trusted_publisher: artifact.signature.publisher.clone(),
            trusted_public_key_base64: artifact.signature.public_key_base64.clone(),
            previous_active_artifact_sha256: context.active_artifact_sha256.clone(),
        }
    }

    fn activate(
        store: &JournalStore,
        artifact: &SignedToolArtifact,
        current_for_rollback: Option<&ToolActivationDecision>,
    ) -> DynamicToolActiveRecord {
        let (context, runtime_eval, authority, approval_id) = prepare_approval(store, artifact);
        let decision = current_for_rollback.map_or_else(
            || decide_dynamic_tool_activation(artifact, &gate(artifact, &context)),
            |current| decide_dynamic_tool_rollback(current, artifact, &gate(artifact, &context)),
        );
        assert!(decision.activated, "fixture decision should activate");
        store
            .activate_dynamic_tool(
                artifact,
                &decision,
                &runtime_eval,
                &authority,
                approval_id.as_str(),
                current_unix_ms().expect("clock should be available"),
            )
            .expect("activation should commit")
    }

    #[test]
    fn activation_is_restart_safe_replayable_and_uses_fail_closed_standard_posture() {
        let path = temp_db_path();
        let journal_config = config(path.clone());
        let store =
            JournalStore::open(journal_config.clone()).expect("journal store should initialize");
        let original = artifact(None, false);
        let active = activate(&store, &original, None);
        let registry = dynamic_tool_registry_entry(&active);
        assert_eq!(registry.approval_posture, ToolApprovalPosture::ApprovalRequired);
        assert_eq!(registry.replay_safety_class, ToolReplaySafetyClass::RequiresHumanConfirmation);

        let approval = store
            .approval(active.approval_id.as_str())
            .expect("approval should load")
            .expect("approval should exist");
        let replayed = store
            .activate_dynamic_tool(
                &original,
                &active.decision,
                &active.runtime_eval,
                &DynamicToolReviewAuthority {
                    session_id: approval.session_id,
                    run_id: approval.run_id,
                    principal: "user:operator".to_owned(),
                    device_id: approval.device_id,
                    channel: Some("console".to_owned()),
                    host_policy_sha256: "d".repeat(64),
                },
                active.approval_id.as_str(),
                current_unix_ms().expect("clock should be available"),
            )
            .expect("exact replay should return the committed version");
        assert_eq!(replayed, active);
        assert!(!store
            .consume_approval_once(active.approval_id.as_str(), "duplicate")
            .expect("consumption lookup should succeed"));
        drop(store);

        let reopened = JournalStore::open(journal_config).expect("journal should reopen");
        let restored = reopened
            .active_dynamic_tool(original.proposal.tool_name.as_str())
            .expect("active tool should load")
            .expect("active tool should survive restart");
        assert_eq!(restored, active);
        reopened
            .connection
            .lock()
            .expect("journal lock should be available")
            .execute(
                "UPDATE dynamic_tool_versions_v1 SET runtime_eval_sha256 = ?1 WHERE artifact_sha256 = ?2",
                rusqlite::params!["b".repeat(64), original.artifact_sha256],
            )
            .expect("tamper fixture should persist");
        assert!(
            reopened.active_dynamic_tool(original.proposal.tool_name.as_str()).is_err(),
            "runtime evidence tampering must make dispatch projection unavailable"
        );
        drop(reopened);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn consumed_or_policy_mismatched_approval_never_advances_catalog() {
        let path = temp_db_path();
        let store = JournalStore::open(config(path.clone())).expect("journal should initialize");
        let artifact = artifact(None, false);
        let mut fabricated_eval = runtime_eval(&artifact);
        fabricated_eval.evidence_sha256 = "c".repeat(64);
        let fabricated_authority = authority();
        let fabricated_context = store
            .dynamic_tool_activation_context(artifact.proposal.tool_name.as_str())
            .expect("activation context should load");
        let fabricated_subject = dynamic_tool_approval_subject(
            &artifact,
            &fabricated_context,
            fabricated_eval.evidence_sha256.as_str(),
        );
        assert!(store
            .record_dynamic_tool_proposal(
                &artifact,
                &fabricated_eval,
                &fabricated_authority,
                fabricated_subject.as_str(),
                current_unix_ms().expect("clock should be available"),
            )
            .is_err());

        let (context, runtime_eval, authority, approval_id) = prepare_approval(&store, &artifact);
        let decision = decide_dynamic_tool_activation(&artifact, &gate(&artifact, &context));
        assert!(store
            .consume_approval_once(approval_id.as_str(), "simulated_racing_consumer")
            .expect("race consumption should persist"));
        let error = store
            .activate_dynamic_tool(
                &artifact,
                &decision,
                &runtime_eval,
                &authority,
                approval_id.as_str(),
                current_unix_ms().expect("clock should be available"),
            )
            .expect_err("consumed approval must not activate");
        assert!(error.to_string().contains("host_approval_consumed"));
        assert!(store
            .active_dynamic_tool(artifact.proposal.tool_name.as_str())
            .expect("active lookup should succeed")
            .is_none());
        assert_eq!(
            store
                .dynamic_tool_activation_context(artifact.proposal.tool_name.as_str())
                .expect("context should remain available")
                .catalog_epoch,
            1
        );

        let (context, runtime_eval, authority, policy_approval_id) =
            prepare_approval(&store, &artifact);
        let decision = decide_dynamic_tool_activation(&artifact, &gate(&artifact, &context));
        let mut changed_policy_authority = authority.clone();
        changed_policy_authority.host_policy_sha256 = "e".repeat(64);
        assert!(store
            .activate_dynamic_tool(
                &artifact,
                &decision,
                &runtime_eval,
                &changed_policy_authority,
                policy_approval_id.as_str(),
                current_unix_ms().expect("clock should be available"),
            )
            .is_err());
        assert!(store
            .consume_approval_once(policy_approval_id.as_str(), "policy-drift consumption probe",)
            .expect("policy-drift approval consumption lookup should succeed"));

        let bad_approval_id = Ulid::generate().to_string();
        let mut request = dynamic_tool_approval_request(
            bad_approval_id.clone(),
            &artifact,
            &context,
            &runtime_eval,
            &authority,
        );
        request.policy_snapshot.evaluation_summary = "caller supplied approval".to_owned();
        store.create_approval(&request).expect("malformed durable approval fixture should persist");
        store
            .resolve_approval(&ApprovalResolveRequest {
                approval_id: bad_approval_id.clone(),
                decision: ApprovalDecision::Allow,
                decision_scope: ApprovalDecisionScope::Once,
                decision_reason: "synthetic".to_owned(),
                decision_scope_ttl_ms: None,
            })
            .expect("malformed approval should resolve");
        assert!(store
            .activate_dynamic_tool(
                &artifact,
                &decision,
                &runtime_eval,
                &authority,
                bad_approval_id.as_str(),
                current_unix_ms().expect("clock should be available"),
            )
            .is_err());
        assert!(store
            .active_dynamic_tool(artifact.proposal.tool_name.as_str())
            .expect("active lookup should succeed")
            .is_none());
        assert_eq!(
            store
                .dynamic_tool_activation_context(artifact.proposal.tool_name.as_str())
                .expect("context should remain available")
                .catalog_epoch,
            1
        );
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn replacement_and_rollback_restore_verified_catalog_version() {
        let path = temp_db_path();
        let store = JournalStore::open(config(path.clone())).expect("journal should initialize");
        let original = artifact(None, false);
        let first = activate(&store, &original, None);
        assert_eq!(first.decision.catalog_epoch, 2);
        let original_schema_hash = dynamic_tool_registry_entry(&first).schema_hash;

        let replacement = artifact(Some(original.artifact_sha256.clone()), true);
        let (context, runtime_eval, authority, approval_id) =
            prepare_approval(&store, &replacement);
        let decision = decide_dynamic_tool_activation(&replacement, &gate(&replacement, &context));
        let mut forged = decision.clone();
        forged.rollback_artifact_sha256 = None;
        assert!(store
            .activate_dynamic_tool(
                &replacement,
                &forged,
                &runtime_eval,
                &authority,
                approval_id.as_str(),
                current_unix_ms().expect("clock should be available"),
            )
            .is_err());
        assert_eq!(
            store
                .active_dynamic_tool(original.proposal.tool_name.as_str())
                .expect("active lookup should succeed")
                .expect("original should remain active"),
            first
        );
        let second = store
            .activate_dynamic_tool(
                &replacement,
                &decision,
                &runtime_eval,
                &authority,
                approval_id.as_str(),
                current_unix_ms().expect("clock should be available"),
            )
            .expect("exact replacement decision should commit");
        assert_eq!(second.decision.catalog_epoch, 3);
        assert_ne!(dynamic_tool_registry_entry(&second).schema_hash, original_schema_hash);

        let rolled_back = activate(&store, &original, Some(&second.decision));
        assert_eq!(rolled_back.decision.catalog_epoch, 4);
        assert_eq!(rolled_back.decision.reason_code, "dynamic_tool.rollback_activated");
        assert_eq!(
            rolled_back.decision.rollback_artifact_sha256,
            Some(replacement.artifact_sha256.clone())
        );
        assert_eq!(dynamic_tool_registry_entry(&rolled_back).schema_hash, original_schema_hash);
        let loaded = store
            .active_dynamic_tool(original.proposal.tool_name.as_str())
            .expect("active lookup should succeed")
            .expect("rollback target should be active");
        assert_eq!(loaded, rolled_back);
        drop(store);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn unrelated_catalog_change_invalidates_prepared_provenance() {
        let path = temp_db_path();
        let store = JournalStore::open(config(path.clone())).expect("journal should initialize");
        let first = activate(&store, &artifact(None, false), None);
        let prepared_provenance = dynamic_tool_registry_entry(&first).provenance;

        let other = named_artifact("dynamic.other", None, false);
        let second = activate(&store, &other, None);
        assert_eq!(second.registry_catalog_epoch, 3);
        let refreshed = store
            .active_dynamic_tool("dynamic.echo")
            .expect("active lookup should succeed")
            .expect("first tool should remain active");
        assert_eq!(refreshed.decision.catalog_epoch, 2);
        assert_eq!(refreshed.registry_catalog_epoch, 3);
        assert_ne!(dynamic_tool_registry_entry(&refreshed).provenance, prepared_provenance);
        drop(store);
        let _ = fs::remove_file(path);
    }
}
