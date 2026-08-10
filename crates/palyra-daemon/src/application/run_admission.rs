//! V2-only application controller for normalized Run admission.
//!
//! Every ingress adapter converges here before provider, tool, or harness work;
//! durable admission and revision-zero kernel initialization share one transaction.

#[cfg(test)]
mod tests;

use palyra_common::runtime_contracts::{
    GenerationLeaseV1, RuntimeIdentitySetV1, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
};
use rusqlite::{params, Transaction};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::runtime_kernel_v2::{
    runtime_selection::HostVerifiedRunAdmission,
    selection::{
        ResolvedRuntimeAuthorityIntent, RuntimeAuthority, RuntimeAuthorityDecisionV1,
        RuntimeAuthorityReason,
    },
    RuntimeKernelV2, RuntimeKernelVersion,
};
use crate::journal::{
    run_admission::{
        run_admission_request_sha256, JournalRunAdmissionEvidenceHook,
        JournalRunAdmissionEvidenceHookInput, JournalRunAdmissionHookContext,
        JournalRunAdmissionOutcome, JournalRunAdmissionPersistedEvidence,
        JournalRunAdmissionPolicy, JournalRunAdmissionQueueInput, JournalRunAdmissionRequest,
        JournalRunAdmissionSessionSelector, JournalRuntimeAuthority, JournalRuntimeAuthorityReason,
        JournalRuntimeProfile, JournalSessionAuthorityIntent, RunAdmissionDisposition,
        RunAdmissionOriginKind,
    },
    runtime_kernel::initialize_runtime_kernel_state_tx,
    JournalError, JournalStore,
};

/// Normalized source accepted by the V2 dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AdmissionOrigin {
    Console,
    Channel,
    Cron,
    Internal,
    Delegation,
}

impl AdmissionOrigin {
    const fn journal_kind(self) -> RunAdmissionOriginKind {
        match self {
            Self::Console => RunAdmissionOriginKind::Console,
            Self::Channel => RunAdmissionOriginKind::Channel,
            Self::Cron => RunAdmissionOriginKind::Cron,
            Self::Internal => RunAdmissionOriginKind::Internal,
            Self::Delegation => RunAdmissionOriginKind::Delegation,
        }
    }
}

/// Exact caller identity checked against the resolved session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AdmissionCaller {
    principal: String,
    device_id: String,
    channel: Option<String>,
}

impl AdmissionCaller {
    /// Captures identity that a trusted ingress adapter already authenticated.
    #[must_use]
    pub(crate) fn authenticated(
        principal: String,
        device_id: String,
        channel: Option<String>,
    ) -> Self {
        Self { principal, device_id, channel }
    }
}

/// Immutable workspace, policy, queue, and host admission observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AdmissionEnvironmentSnapshot {
    workspace_sha256: String,
    access_policy_json: String,
    queue_policy_json: String,
    draining: bool,
    drain_reason: Option<String>,
    ingress_block_reason: Option<String>,
    max_pending_queue_depth: u64,
}

impl AdmissionEnvironmentSnapshot {
    /// Captures host-loaded policy and daemon state without accepting an allow bit.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub(crate) fn host_snapshot(
        workspace_sha256: String,
        access_policy_json: String,
        queue_policy_json: String,
        draining: bool,
        drain_reason: Option<String>,
        max_pending_queue_depth: u64,
    ) -> Self {
        Self {
            workspace_sha256,
            access_policy_json,
            queue_policy_json,
            draining,
            drain_reason,
            ingress_block_reason: None,
            max_pending_queue_depth,
        }
    }

    /// Adds a host-owned fail-closed ingress reason before any Run allocation.
    ///
    /// This is distinct from daemon drain and access denial so durable
    /// admission evidence retains the operational cause.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_ingress_block(mut self, reason: String) -> Self {
        self.ingress_block_reason = Some(reason);
        self
    }
}

/// Optional active-Run input routed according to the immutable queue policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct AdmissionQueueIntent {
    queued_input_id: String,
    text: String,
    requested_mode: String,
    policy_agent: String,
    safe_boundary_flags_json: String,
    disposition: RunAdmissionDisposition,
}

impl AdmissionQueueIntent {
    /// Creates a host-policy-derived queue action.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub(crate) fn verified(
        queued_input_id: String,
        text: String,
        requested_mode: String,
        policy_agent: String,
        safe_boundary_flags_json: String,
        disposition: RunAdmissionDisposition,
    ) -> Self {
        Self {
            queued_input_id,
            text,
            requested_mode,
            policy_agent,
            safe_boundary_flags_json,
            disposition,
        }
    }
}

/// One normalized request accepted from any new V2 ingress.
#[derive(Debug)]
pub(crate) struct RunAdmissionCommand {
    admission_id: String,
    idempotency_scope: String,
    idempotency_key: String,
    trace_id: String,
    requested_run_id: String,
    initial_attempt_id: String,
    session: JournalRunAdmissionSessionSelector,
    verified: HostVerifiedRunAdmission,
}

impl RunAdmissionCommand {
    /// Binds request identities to a sealed host-issued admission decision.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub(crate) fn from_verified(
        admission_id: String,
        idempotency_scope: String,
        idempotency_key: String,
        trace_id: String,
        requested_run_id: String,
        initial_attempt_id: String,
        session: JournalRunAdmissionSessionSelector,
        verified: HostVerifiedRunAdmission,
    ) -> Self {
        Self {
            admission_id,
            idempotency_scope,
            idempotency_key,
            trace_id,
            requested_run_id,
            initial_attempt_id,
            session,
            verified,
        }
    }
}

/// Persisted V2 authority issued only after the journal transaction commits.
#[derive(Debug)]
pub(crate) struct PersistedV2AdmissionToken {
    identities: RuntimeIdentitySetV1,
    initial_attempt_id: String,
    run_lease: GenerationLeaseV1,
    authority_decision: RuntimeAuthorityDecisionV1,
    authority_decision_sha256: String,
    session_authority_pin_revision: u64,
    session_authority_pin_sha256: String,
    admission_snapshot_sha256: String,
    kernel_head_sha256: String,
    policy_sha256: String,
}

impl PersistedV2AdmissionToken {
    #[must_use]
    pub(crate) const fn identities(&self) -> &RuntimeIdentitySetV1 {
        &self.identities
    }

    #[must_use]
    pub(crate) fn run_id(&self) -> &str {
        self.identities.run_id.as_str()
    }

    #[must_use]
    pub(crate) fn initial_attempt_id(&self) -> &str {
        self.initial_attempt_id.as_str()
    }

    #[must_use]
    pub(crate) const fn run_lease(&self) -> &GenerationLeaseV1 {
        &self.run_lease
    }

    #[must_use]
    pub(crate) const fn authority_decision(&self) -> &RuntimeAuthorityDecisionV1 {
        &self.authority_decision
    }

    #[must_use]
    pub(crate) fn admission_snapshot_sha256(&self) -> &str {
        self.admission_snapshot_sha256.as_str()
    }

    pub(crate) fn into_parts(self) -> PersistedV2AdmissionParts {
        PersistedV2AdmissionParts {
            identities: self.identities,
            initial_attempt_id: self.initial_attempt_id,
            run_lease: self.run_lease,
            authority_decision: self.authority_decision,
            authority_decision_sha256: self.authority_decision_sha256,
            session_authority_pin_revision: self.session_authority_pin_revision,
            session_authority_pin_sha256: self.session_authority_pin_sha256,
            admission_snapshot_sha256: self.admission_snapshot_sha256,
            kernel_head_sha256: self.kernel_head_sha256,
            policy_sha256: self.policy_sha256,
        }
    }

    #[cfg(test)]
    pub(crate) fn tamper_authority_digest_for_test(&mut self) {
        self.authority_decision_sha256 = "0".repeat(64);
    }

    #[cfg(test)]
    pub(crate) fn tamper_run_lease_generation_for_test(&mut self) {
        self.run_lease.generation = palyra_common::runtime_contracts::RuntimeGeneration::new(
            self.run_lease.generation.get().saturating_add(1),
        )
        .expect("incremented test generation should remain valid");
    }
}

pub(crate) struct PersistedV2AdmissionParts {
    pub(crate) identities: RuntimeIdentitySetV1,
    pub(crate) initial_attempt_id: String,
    pub(crate) run_lease: GenerationLeaseV1,
    pub(crate) authority_decision: RuntimeAuthorityDecisionV1,
    pub(crate) authority_decision_sha256: String,
    pub(crate) session_authority_pin_revision: u64,
    pub(crate) session_authority_pin_sha256: String,
    pub(crate) admission_snapshot_sha256: String,
    pub(crate) kernel_head_sha256: String,
    pub(crate) policy_sha256: String,
}

/// Controller result; only `Admitted` carries executable V2 admission evidence.
#[derive(Debug)]
pub(crate) enum RunAdmissionControllerOutcome {
    Rejected {
        journal: Box<JournalRunAdmissionOutcome>,
    },
    Queued {
        journal: Box<JournalRunAdmissionOutcome>,
    },
    Admitted {
        #[cfg(test)]
        journal: Box<JournalRunAdmissionOutcome>,
        token: Box<PersistedV2AdmissionToken>,
    },
}

/// Single application admission controller for all new V2 inputs.
pub(crate) struct RunAdmissionController<'a> {
    journal: &'a JournalStore,
}

impl<'a> RunAdmissionController<'a> {
    #[must_use]
    pub(crate) const fn new(journal: &'a JournalStore) -> Self {
        Self { journal }
    }

    /// Commits normalized admission and returns authority only from committed evidence.
    ///
    /// # Errors
    /// Returns [`RunAdmissionControllerError`] when canonicalization, identity,
    /// V2 selection, kernel initialization, or the durable transaction fails.
    pub(crate) fn admit(
        &self,
        command: RunAdmissionCommand,
    ) -> Result<RunAdmissionControllerOutcome, RunAdmissionControllerError> {
        let RunAdmissionCommand {
            admission_id,
            idempotency_scope,
            idempotency_key,
            trace_id,
            requested_run_id,
            initial_attempt_id,
            session,
            verified,
        } = command;
        let verified = verified.into_parts();
        validate_environment(&verified.environment)?;
        validate_v2_intent(&verified.authority_intent)?;
        let access_digest =
            canonical_json_digest(verified.environment.access_policy_json.as_str())?;
        let queue_digest = canonical_json_digest(verified.environment.queue_policy_json.as_str())?;
        let policy_digest = sha256_hex(format!("{access_digest}:{queue_digest}").as_bytes());
        let access_allowed =
            access_policy_allows(verified.environment.access_policy_json.as_str())?;
        let forced_rejection_reason = if verified.environment.draining {
            Some(
                verified
                    .environment
                    .drain_reason
                    .clone()
                    .unwrap_or_else(|| "run_admission.daemon_draining".to_owned()),
            )
        } else if verified.environment.ingress_block_reason.is_some() {
            verified.environment.ingress_block_reason.clone()
        } else if !access_allowed {
            Some("run_admission.access_denied".to_owned())
        } else {
            None
        };
        let queue_input =
            verified.queue_intent.as_ref().map(|intent| JournalRunAdmissionQueueInput {
                queued_input_id: intent.queued_input_id.clone(),
                text: intent.text.clone(),
                requested_mode: intent.requested_mode.clone(),
                policy_channel: verified
                    .caller
                    .channel
                    .clone()
                    .unwrap_or_else(|| "internal".to_owned()),
                policy_agent: intent.policy_agent.clone(),
                safe_boundary_flags_json: intent.safe_boundary_flags_json.clone(),
            });
        let disposition = verified
            .queue_intent
            .as_ref()
            .map_or(RunAdmissionDisposition::DurableQueue, |intent| intent.disposition);
        let snapshot_json = canonical_json(&AdmissionSnapshotEvidence {
            origin: verified.origin,
            caller: &verified.caller,
            session: &session,
            workspace_sha256: verified.environment.workspace_sha256.as_str(),
            access_policy_sha256: access_digest.as_str(),
            queue_policy_sha256: queue_digest.as_str(),
            policy_sha256: policy_digest.as_str(),
            access_allowed,
            draining: verified.environment.draining,
            drain_reason: &verified.environment.drain_reason,
            ingress_block_reason: &verified.environment.ingress_block_reason,
        })?;
        let snapshot_digest = sha256_hex(snapshot_json.as_bytes());
        let authority_input_json = canonical_json(&AuthorityHookEvidence {
            profile: profile_name(&verified.authority_intent),
            admission_snapshot_sha256: snapshot_digest.as_str(),
            policy_sha256: policy_digest.as_str(),
        })?;
        let authority_input_digest = sha256_hex(authority_input_json.as_bytes());
        let mut journal_request = JournalRunAdmissionRequest {
            admission_id,
            idempotency_scope,
            idempotency_key,
            request_sha256: String::new(),
            trace_id,
            run_id: requested_run_id,
            initial_attempt_id,
            session,
            caller_principal: verified.caller.principal,
            caller_device_id: verified.caller.device_id,
            caller_channel: verified.caller.channel,
            origin_kind: verified.origin.journal_kind(),
            origin_run_id: verified.origin_run_id,
            delegated_admission_json: verified.delegated_admission_json,
            queue_input,
            fresh_run_intent: verified.queue_intent.is_none(),
            policy: JournalRunAdmissionPolicy {
                access_policy_json: verified.environment.access_policy_json,
                queue_policy_json: verified.environment.queue_policy_json,
                access_policy_sha256: access_digest,
                queue_policy_sha256: queue_digest,
                policy_sha256: policy_digest.clone(),
                max_pending_queue_depth: verified.environment.max_pending_queue_depth,
                active_run_disposition: disposition,
                forced_rejection_reason,
            },
            evidence_hook_input: JournalRunAdmissionEvidenceHookInput {
                authority_input_json,
                authority_input_sha256: authority_input_digest,
                kernel_input_json: snapshot_json,
                kernel_input_sha256: snapshot_digest,
            },
            session_authority_intent: journal_authority_intent(&verified.authority_intent)?,
        };
        journal_request.request_sha256 = run_admission_request_sha256(&journal_request)?;
        let mut hook = V2AdmissionEvidenceIssuer { intent: verified.authority_intent };
        let journal = self.journal.commit_run_admission(&journal_request, &mut hook)?;
        match journal.disposition {
            RunAdmissionDisposition::Reject => {
                Ok(RunAdmissionControllerOutcome::Rejected { journal: Box::new(journal) })
            }
            RunAdmissionDisposition::DurableQueue
            | RunAdmissionDisposition::Merge
            | RunAdmissionDisposition::SteerCandidate => {
                Ok(RunAdmissionControllerOutcome::Queued { journal: Box::new(journal) })
            }
            RunAdmissionDisposition::AdmitNow => {
                let token = token_from_committed(
                    self.journal,
                    &journal,
                    journal_request.trace_id.as_str(),
                    policy_digest,
                    &hook.intent,
                )?;
                Ok(RunAdmissionControllerOutcome::Admitted {
                    #[cfg(test)]
                    journal: Box::new(journal),
                    token: Box::new(token),
                })
            }
        }
    }

    /// Runs downstream work only after an admitted token is backed by committed evidence.
    ///
    /// Reject and queue outcomes never invoke `downstream`.
    #[cfg(test)]
    pub(crate) fn admit_and_then<T>(
        &self,
        command: RunAdmissionCommand,
        downstream: impl FnOnce(&PersistedV2AdmissionToken) -> T,
    ) -> Result<(RunAdmissionControllerOutcome, Option<T>), RunAdmissionControllerError> {
        let outcome = self.admit(command)?;
        let downstream_result = match &outcome {
            RunAdmissionControllerOutcome::Admitted { token, .. } => Some(downstream(token)),
            RunAdmissionControllerOutcome::Rejected { .. }
            | RunAdmissionControllerOutcome::Queued { .. } => None,
        };
        Ok((outcome, downstream_result))
    }
}

/// Admits one internal V2 run through the canonical controller for integration tests.
#[cfg(test)]
pub(crate) fn admit_test_v2_run(
    journal: &JournalStore,
    session_id: &str,
    run_id: &str,
    principal: &str,
    device_id: &str,
) -> Result<PersistedV2AdmissionToken, String> {
    use super::runtime_kernel_v2::{
        profile::{RuntimeKernelCompatibilityOverridesV1, RuntimeKernelProfileConfigV1},
        runtime_selection::HostVerifiedRunAdmission,
        selection::{
            resolve_runtime_authority_intent_for_principal, RuntimeAuthorityProgressEvidence,
            V2RuntimeAvailability,
        },
    };

    let session_identity =
        RuntimeSessionId::parse(session_id).map_err(|error| error.to_string())?;
    let profile = RuntimeKernelProfileConfigV1::new(
        RuntimeKernelVersion::V2,
        0,
        RuntimeKernelCompatibilityOverridesV1::none(),
    )
    .map_err(|error| error.to_string())?;
    let intent = resolve_runtime_authority_intent_for_principal(
        &profile,
        &session_identity,
        None,
        V2RuntimeAvailability::Ready,
        RuntimeAuthorityProgressEvidence::pristine(),
        None,
    )
    .map_err(|error| error.to_string())?;
    let verified = HostVerifiedRunAdmission::test_only(
        AdmissionOrigin::Internal,
        AdmissionCaller::authenticated(
            principal.to_owned(),
            device_id.to_owned(),
            Some("internal".to_owned()),
        ),
        AdmissionEnvironmentSnapshot::host_snapshot(
            "a".repeat(64),
            r#"{"allow":true}"#.to_owned(),
            r#"{"mode":"followup"}"#.to_owned(),
            false,
            None,
            8,
        ),
        intent,
        None,
        None,
        None,
    );
    let command = RunAdmissionCommand::from_verified(
        format!("admission_{run_id}"),
        format!("test_scope_{session_id}"),
        format!("test_key_{run_id}"),
        format!("trace_{run_id}"),
        run_id.to_owned(),
        format!("attempt_{run_id}"),
        JournalRunAdmissionSessionSelector {
            session_id: Some(session_id.to_owned()),
            session_key: Some(format!("v2-terminal:{session_id}")),
            session_label: Some("V2 terminal convergence test".to_owned()),
            require_existing: false,
            reset_session: false,
        },
        verified,
    );
    match RunAdmissionController::new(journal).admit(command).map_err(|error| error.to_string())? {
        RunAdmissionControllerOutcome::Admitted { token, .. } => Ok(*token),
        RunAdmissionControllerOutcome::Rejected { .. } => {
            Err("test V2 admission was rejected".to_owned())
        }
        RunAdmissionControllerOutcome::Queued { .. } => {
            Err("test V2 admission was queued".to_owned())
        }
    }
}

#[derive(Serialize)]
struct AdmissionSnapshotEvidence<'a> {
    origin: AdmissionOrigin,
    caller: &'a AdmissionCaller,
    session: &'a JournalRunAdmissionSessionSelector,
    workspace_sha256: &'a str,
    access_policy_sha256: &'a str,
    queue_policy_sha256: &'a str,
    policy_sha256: &'a str,
    access_allowed: bool,
    draining: bool,
    drain_reason: &'a Option<String>,
    ingress_block_reason: &'a Option<String>,
}

#[derive(Serialize)]
struct AuthorityHookEvidence<'a> {
    profile: &'a str,
    admission_snapshot_sha256: &'a str,
    policy_sha256: &'a str,
}

struct V2AdmissionEvidenceIssuer {
    intent: ResolvedRuntimeAuthorityIntent,
}

impl JournalRunAdmissionEvidenceHook for V2AdmissionEvidenceIssuer {
    fn persist_admit_now_evidence(
        &mut self,
        transaction: &Transaction<'_>,
        context: &JournalRunAdmissionHookContext<'_>,
        input: &JournalRunAdmissionEvidenceHookInput,
    ) -> Result<JournalRunAdmissionPersistedEvidence, JournalError> {
        let run_generation =
            i64::try_from(context.run_lease.generation.get()).map_err(|error| {
                JournalError::RunAdmissionEvidenceHook {
                    reason: format!("run generation cannot be persisted: {error}"),
                }
            })?;
        let reservation_matches = transaction.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM runtime_run_initial_attempt_reservations
                    WHERE attempt_ulid = ?1
                      AND admission_ulid = ?2
                      AND session_ulid = ?3
                      AND run_ulid = ?4
                      AND run_generation = ?5
                      AND run_lease_ulid = ?6
                      AND state = 'reserved'
                )
            "#,
            params![
                context.initial_attempt_id,
                context.admission_id,
                context.session.session_id,
                context.run_id,
                run_generation,
                context.run_lease.lease_id.as_str(),
            ],
            |row| row.get::<_, bool>(0),
        )?;
        if !reservation_matches {
            return Err(JournalError::RunAdmissionEvidenceHook {
                reason: "initial attempt reservation does not match admission authority".to_owned(),
            });
        }
        let identities = RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse(context.trace_id).map_err(|error| {
                JournalError::RunAdmissionEvidenceHook { reason: error.to_string() }
            })?,
            RuntimeSessionId::parse(context.session.session_id.as_str()).map_err(|error| {
                JournalError::RunAdmissionEvidenceHook { reason: error.to_string() }
            })?,
            RuntimeRunId::parse(context.run_id).map_err(|error| {
                JournalError::RunAdmissionEvidenceHook { reason: error.to_string() }
            })?,
            context.run_lease.generation,
        );
        let decision = self.intent.bind_generation(identities.generation).map_err(hook_error)?;
        let kernel = RuntimeKernelV2::admit(
            decision.clone(),
            identities,
            context.run_lease.clone(),
            context.run_lease.acquired_at_unix_ms,
        )
        .map_err(hook_error)?;
        let head = initialize_runtime_kernel_state_tx(
            transaction,
            kernel.snapshot(),
            context.max_payload_bytes,
            context.run_lease.acquired_at_unix_ms,
        )?;
        let authority_decision_json = canonical_json(&decision).map_err(|error| {
            JournalError::RunAdmissionEvidenceHook { reason: error.to_string() }
        })?;
        Ok(JournalRunAdmissionPersistedEvidence {
            authority_decision_sha256: sha256_hex(authority_decision_json.as_bytes()),
            authority_decision_json,
            admission_snapshot_json: input.kernel_input_json.clone(),
            admission_snapshot_sha256: input.kernel_input_sha256.clone(),
            kernel_head_sha256: head.snapshot_sha256,
        })
    }
}

fn token_from_committed(
    journal: &JournalStore,
    outcome: &JournalRunAdmissionOutcome,
    trace_id: &str,
    policy_sha256: String,
    authority_intent: &ResolvedRuntimeAuthorityIntent,
) -> Result<PersistedV2AdmissionToken, RunAdmissionControllerError> {
    let run_id = outcome
        .allocated_run_id
        .clone()
        .ok_or(RunAdmissionControllerError::MissingCommittedEvidence)?;
    let run_lease =
        outcome.run_lease.clone().ok_or(RunAdmissionControllerError::MissingCommittedEvidence)?;
    let authority_decision = authority_intent
        .bind_generation(run_lease.generation)
        .map_err(|error| RunAdmissionControllerError::InvalidRuntimeDecision(error.to_string()))?;
    let authority_decision_sha256 = outcome
        .authority_decision_sha256
        .clone()
        .ok_or(RunAdmissionControllerError::MissingCommittedEvidence)?;
    let exact_decision_sha256 = sha256_hex(canonical_json(&authority_decision)?.as_bytes());
    if authority_decision_sha256 != exact_decision_sha256 {
        return Err(RunAdmissionControllerError::CommittedEvidenceMismatch(
            "runtime authority decision digest",
        ));
    }
    let session_authority_pin = outcome
        .session_authority_pin
        .as_ref()
        .ok_or(RunAdmissionControllerError::MissingCommittedEvidence)?;
    let expected_pin_intent = journal_authority_intent(authority_intent)?;
    if session_authority_pin.schema_version != 1
        || session_authority_pin.revision == 0
        || session_authority_pin.configured_profile != expected_pin_intent.configured_profile
        || session_authority_pin.selected_runtime != expected_pin_intent.selected_runtime
        || session_authority_pin.reason != expected_pin_intent.reason
        || session_authority_pin.shadow_evaluation_enabled
            != expected_pin_intent.shadow_evaluation_enabled
        || session_authority_pin.created_after_run_generation > run_lease.generation.get()
    {
        return Err(RunAdmissionControllerError::CommittedEvidenceMismatch(
            "session runtime authority pin",
        ));
    }
    let identities = RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse(trace_id).map_err(|error| {
            RunAdmissionControllerError::InvalidSnapshot(format!(
                "committed trace identity is invalid: {error}"
            ))
        })?,
        RuntimeSessionId::parse(outcome.session.session_id.as_str()).map_err(|error| {
            RunAdmissionControllerError::InvalidSnapshot(format!(
                "committed session identity is invalid: {error}"
            ))
        })?,
        RuntimeRunId::parse(run_id.as_str()).map_err(|error| {
            RunAdmissionControllerError::InvalidSnapshot(format!(
                "committed run identity is invalid: {error}"
            ))
        })?,
        run_lease.generation,
    );
    if authority_decision.generation() != identities.generation
        || run_lease.session_id != identities.session_id
        || run_lease.run_id.as_ref() != Some(&identities.run_id)
    {
        return Err(RunAdmissionControllerError::CommittedEvidenceMismatch(
            "runtime authority identity",
        ));
    }
    let active_lease = journal
        .active_runtime_generation_for_run(run_id.as_str(), run_lease.lane)?
        .ok_or(RunAdmissionControllerError::CommittedLeaseInactive)?;
    if active_lease != run_lease {
        return Err(RunAdmissionControllerError::CommittedLeaseInactive);
    }
    Ok(PersistedV2AdmissionToken {
        identities,
        initial_attempt_id: outcome
            .initial_attempt_id
            .clone()
            .ok_or(RunAdmissionControllerError::MissingCommittedEvidence)?,
        run_lease,
        authority_decision,
        authority_decision_sha256,
        session_authority_pin_revision: session_authority_pin.revision,
        session_authority_pin_sha256: session_authority_pin.pin_sha256.clone(),
        admission_snapshot_sha256: outcome
            .admission_snapshot_sha256
            .clone()
            .ok_or(RunAdmissionControllerError::MissingCommittedEvidence)?,
        kernel_head_sha256: outcome
            .kernel_head_sha256
            .clone()
            .ok_or(RunAdmissionControllerError::MissingCommittedEvidence)?,
        policy_sha256,
    })
}

fn validate_environment(
    environment: &AdmissionEnvironmentSnapshot,
) -> Result<(), RunAdmissionControllerError> {
    if environment.workspace_sha256.len() != 64
        || !environment
            .workspace_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RunAdmissionControllerError::InvalidSnapshot(
            "workspace_sha256 must be SHA-256 hex".to_owned(),
        ));
    }
    if environment.draining && environment.drain_reason.as_deref().is_none_or(str::is_empty) {
        return Err(RunAdmissionControllerError::InvalidSnapshot(
            "drain denial requires a reason".to_owned(),
        ));
    }
    if environment.ingress_block_reason.as_deref().is_some_and(|reason| reason.trim().is_empty()) {
        return Err(RunAdmissionControllerError::InvalidSnapshot(
            "ingress block requires a non-empty reason".to_owned(),
        ));
    }
    Ok(())
}

fn canonical_json<T: Serialize>(value: &T) -> Result<String, RunAdmissionControllerError> {
    let value = serde_json::to_value(value)?;
    Ok(serde_json::to_string(&canonicalize(value))?)
}

fn canonical_json_digest(raw: &str) -> Result<String, RunAdmissionControllerError> {
    let value: serde_json::Value = serde_json::from_str(raw)?;
    let canonical = serde_json::to_string(&canonicalize(value))?;
    if canonical != raw {
        return Err(RunAdmissionControllerError::InvalidSnapshot(
            "policy JSON must be canonical".to_owned(),
        ));
    }
    Ok(sha256_hex(canonical.as_bytes()))
}

fn access_policy_allows(raw: &str) -> Result<bool, RunAdmissionControllerError> {
    let value: serde_json::Value = serde_json::from_str(raw)?;
    Ok(value.get("allow").and_then(serde_json::Value::as_bool).unwrap_or(false))
}

fn canonicalize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(values) => {
            let mut values = values.into_iter().collect::<Vec<_>>();
            values.sort_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                values.into_iter().map(|(key, value)| (key, canonicalize(value))).collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize).collect())
        }
        other => other,
    }
}

fn validate_v2_intent(
    intent: &ResolvedRuntimeAuthorityIntent,
) -> Result<(), RunAdmissionControllerError> {
    if intent.selected_runtime() != Some(RuntimeAuthority::V2)
        || intent.shadow_evaluation_enabled()
        || !matches!(intent.profile(), RuntimeKernelVersion::V2 | RuntimeKernelVersion::V2Canary)
    {
        return Err(RunAdmissionControllerError::InvalidRuntimeDecision(
            "admission requires an authoritative V2 or V2-canary intent".to_owned(),
        ));
    }
    Ok(())
}

fn profile_name(intent: &ResolvedRuntimeAuthorityIntent) -> &'static str {
    match intent.profile() {
        RuntimeKernelVersion::V2 => "v2",
        RuntimeKernelVersion::V2Canary => "v2_canary",
        RuntimeKernelVersion::Legacy | RuntimeKernelVersion::V2Shadow => {
            unreachable!("validated V2 admission intent")
        }
    }
}

/// Projects a generation-free host decision into the closed journal contract.
///
/// # Errors
/// Returns [`RunAdmissionControllerError::InvalidRuntimeDecision`] when the
/// intent is blocked or its closed reason cannot be represented by the journal.
pub(crate) fn journal_authority_intent(
    intent: &ResolvedRuntimeAuthorityIntent,
) -> Result<JournalSessionAuthorityIntent, RunAdmissionControllerError> {
    let configured_profile = match intent.profile() {
        RuntimeKernelVersion::Legacy => JournalRuntimeProfile::Legacy,
        RuntimeKernelVersion::V2Shadow => JournalRuntimeProfile::V2Shadow,
        RuntimeKernelVersion::V2Canary => JournalRuntimeProfile::V2Canary,
        RuntimeKernelVersion::V2 => JournalRuntimeProfile::V2,
    };
    let selected_runtime = match intent.selected_runtime() {
        Some(RuntimeAuthority::Legacy) => JournalRuntimeAuthority::Legacy,
        Some(RuntimeAuthority::V2) => JournalRuntimeAuthority::V2,
        None => {
            return Err(RunAdmissionControllerError::InvalidRuntimeDecision(
                "blocked runtime intent cannot be persisted as a session pin".to_owned(),
            ));
        }
    };
    let reason = match intent.reason() {
        RuntimeAuthorityReason::LegacyProfileSelected => {
            JournalRuntimeAuthorityReason::LegacyProfileSelected
        }
        RuntimeAuthorityReason::V2ShadowLegacyAuthority => {
            JournalRuntimeAuthorityReason::V2ShadowLegacyAuthority
        }
        RuntimeAuthorityReason::V2CanarySessionExcluded => {
            JournalRuntimeAuthorityReason::V2CanarySessionExcluded
        }
        RuntimeAuthorityReason::V2CanarySessionSelected => {
            JournalRuntimeAuthorityReason::V2CanarySessionSelected
        }
        RuntimeAuthorityReason::V2ProfileSelected => {
            JournalRuntimeAuthorityReason::V2ProfileSelected
        }
        RuntimeAuthorityReason::V2UnavailableNoLegacyFallback
        | RuntimeAuthorityReason::V2UnavailableAfterPartialOutput
        | RuntimeAuthorityReason::V2UnavailableAfterSideEffectBoundary => {
            return Err(RunAdmissionControllerError::InvalidRuntimeDecision(
                "blocked runtime reason cannot be persisted as a session pin".to_owned(),
            ));
        }
    };
    Ok(JournalSessionAuthorityIntent {
        configured_profile,
        selected_runtime,
        reason,
        shadow_evaluation_enabled: intent.shadow_evaluation_enabled(),
    })
}

fn sha256_hex(payload: &[u8]) -> String {
    hex::encode(Sha256::digest(payload))
}

fn hook_error(error: impl std::fmt::Display) -> JournalError {
    JournalError::RunAdmissionEvidenceHook { reason: error.to_string() }
}

/// Fail-closed controller error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RunAdmissionControllerError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("invalid admission snapshot: {0}")]
    InvalidSnapshot(String),
    #[error("committed V2 admission evidence is incomplete")]
    MissingCommittedEvidence,
    #[error("committed V2 admission evidence mismatch: {0}")]
    CommittedEvidenceMismatch(&'static str),
    #[error("committed V2 admission lease is stale or inactive")]
    CommittedLeaseInactive,
    #[error("invalid host runtime decision: {0}")]
    InvalidRuntimeDecision(String),
    #[error("failed to serialize canonical admission evidence: {0}")]
    Serialize(#[from] serde_json::Error),
}
