//! Admission request, outcome, and in-transaction evidence-port contracts.
//!
//! These types carry only immutable identities, canonical policy evidence, and
//! redacted hook input; raw credentials never cross the journal boundary.

use palyra_common::runtime_contracts::GenerationLeaseV1;
use rusqlite::Transaction;
use serde::{Deserialize, Serialize};

use super::{JournalError, OrchestratorSessionRecord};

/// Closed configured profile persisted without rollout identities or key data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JournalRuntimeProfile {
    Legacy,
    V2Shadow,
    V2Canary,
    V2,
}

impl JournalRuntimeProfile {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::V2Shadow => "v2_shadow",
            Self::V2Canary => "v2_canary",
            Self::V2 => "v2",
        }
    }
}

/// Runtime selected by one durable session authority pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JournalRuntimeAuthority {
    Legacy,
    V2,
}

impl JournalRuntimeAuthority {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::V2 => "v2",
        }
    }
}

/// Stable low-cardinality reason retained across run generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JournalRuntimeAuthorityReason {
    LegacyProfileSelected,
    V2ShadowLegacyAuthority,
    V2CanarySessionExcluded,
    V2CanarySessionSelected,
    V2ProfileSelected,
}

impl JournalRuntimeAuthorityReason {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyProfileSelected => "legacy_profile_selected",
            Self::V2ShadowLegacyAuthority => "v2_shadow_legacy_authority",
            Self::V2CanarySessionExcluded => "v2_canary_session_excluded",
            Self::V2CanarySessionSelected => "v2_canary_session_selected",
            Self::V2ProfileSelected => "v2_profile_selected",
        }
    }
}

/// Generation-free authority selected once for an entire session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JournalSessionAuthorityIntent {
    pub configured_profile: JournalRuntimeProfile,
    pub selected_runtime: JournalRuntimeAuthority,
    pub reason: JournalRuntimeAuthorityReason,
    pub shadow_evaluation_enabled: bool,
}

/// Append-only durable session authority record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JournalSessionAuthorityPin {
    pub schema_version: u32,
    pub revision: u64,
    pub configured_profile: JournalRuntimeProfile,
    pub selected_runtime: JournalRuntimeAuthority,
    pub reason: JournalRuntimeAuthorityReason,
    pub shadow_evaluation_enabled: bool,
    pub created_after_run_generation: u64,
    pub created_at_unix_ms: i64,
    pub migration_reason_code: String,
    pub safe_boundary_evidence: Option<serde_json::Value>,
    pub pin_sha256: String,
}

/// Compare-and-swap request for the first authority pin of an idle session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JournalInitialSessionAuthorityPinRequest {
    pub session_id: String,
    pub expected_revision: u64,
    pub intent: JournalSessionAuthorityIntent,
    pub migration_reason_code: String,
}

/// Result of an idempotent initial pin or appended migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JournalSessionAuthorityPinOutcome {
    Created(JournalSessionAuthorityPin),
    Existing(JournalSessionAuthorityPin),
}

/// Closed ingress origins accepted by durable admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunAdmissionOriginKind {
    Console,
    Channel,
    Cron,
    Internal,
    Delegation,
}

impl RunAdmissionOriginKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Console => "console",
            Self::Channel => "channel",
            Self::Cron => "cron",
            Self::Internal => "internal",
            Self::Delegation => "delegation",
        }
    }
}

/// Immutable disposition committed by admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunAdmissionDisposition {
    Reject,
    DurableQueue,
    Merge,
    SteerCandidate,
    AdmitNow,
}

impl RunAdmissionDisposition {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::DurableQueue => "durable_queue",
            Self::Merge => "merge",
            Self::SteerCandidate => "steer_candidate",
            Self::AdmitNow => "admit_now",
        }
    }

    pub(super) fn queue_state(self) -> &'static str {
        match self {
            Self::DurableQueue | Self::SteerCandidate => "pending",
            Self::Merge => "merged",
            Self::Reject | Self::AdmitNow => unreachable!("not a queue disposition"),
        }
    }
}

/// Exact session selector and reset policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JournalRunAdmissionSessionSelector {
    pub session_id: Option<String>,
    pub session_key: Option<String>,
    pub session_label: Option<String>,
    pub require_existing: bool,
    pub reset_session: bool,
}

/// Redacted input persisted for an active Run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JournalRunAdmissionQueueInput {
    pub queued_input_id: String,
    pub text: String,
    pub requested_mode: String,
    pub policy_channel: String,
    pub policy_agent: String,
    pub safe_boundary_flags_json: String,
}

/// Immutable policy inputs evaluated in the transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JournalRunAdmissionPolicy {
    pub access_policy_json: String,
    pub queue_policy_json: String,
    pub access_policy_sha256: String,
    pub queue_policy_sha256: String,
    pub policy_sha256: String,
    pub max_pending_queue_depth: u64,
    pub active_run_disposition: RunAdmissionDisposition,
    pub forced_rejection_reason: Option<String>,
}

/// Opaque, redacted input passed to the V2 persistence hook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JournalRunAdmissionEvidenceHookInput {
    pub authority_input_json: String,
    pub authority_input_sha256: String,
    pub kernel_input_json: String,
    pub kernel_input_sha256: String,
}

/// One canonical request shared by all ingress adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JournalRunAdmissionRequest {
    pub admission_id: String,
    pub idempotency_scope: String,
    pub idempotency_key: String,
    pub request_sha256: String,
    pub trace_id: String,
    pub run_id: String,
    pub initial_attempt_id: String,
    pub session: JournalRunAdmissionSessionSelector,
    pub caller_principal: String,
    pub caller_device_id: String,
    pub caller_channel: Option<String>,
    pub origin_kind: RunAdmissionOriginKind,
    pub origin_run_id: Option<String>,
    pub delegated_admission_json: Option<String>,
    pub queue_input: Option<JournalRunAdmissionQueueInput>,
    pub fresh_run_intent: bool,
    pub policy: JournalRunAdmissionPolicy,
    pub evidence_hook_input: JournalRunAdmissionEvidenceHookInput,
    pub session_authority_intent: JournalSessionAuthorityIntent,
}

/// Evidence returned after the hook persists V2 authority and kernel state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JournalRunAdmissionPersistedEvidence {
    pub authority_decision_json: String,
    pub authority_decision_sha256: String,
    pub admission_snapshot_json: String,
    pub admission_snapshot_sha256: String,
    pub kernel_head_sha256: String,
}

/// Immutable context supplied to the V2 persistence hook.
pub(crate) struct JournalRunAdmissionHookContext<'a> {
    pub admission_id: &'a str,
    pub trace_id: &'a str,
    pub session: &'a OrchestratorSessionRecord,
    pub run_id: &'a str,
    pub initial_attempt_id: &'a str,
    pub run_lease: &'a GenerationLeaseV1,
    pub max_payload_bytes: usize,
}

/// Required in-transaction V2 evidence persistence port.
pub(crate) trait JournalRunAdmissionEvidenceHook {
    fn persist_admit_now_evidence(
        &mut self,
        transaction: &Transaction<'_>,
        context: &JournalRunAdmissionHookContext<'_>,
        input: &JournalRunAdmissionEvidenceHookInput,
    ) -> Result<JournalRunAdmissionPersistedEvidence, JournalError>;
}

/// Immutable result returned for a new commit or exact replay.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JournalRunAdmissionOutcome {
    pub session: OrchestratorSessionRecord,
    pub disposition: RunAdmissionDisposition,
    pub reason_code: String,
    #[cfg(test)]
    pub target_active_run_id: Option<String>,
    #[cfg(test)]
    pub queued_input_id: Option<String>,
    pub allocated_run_id: Option<String>,
    pub run_lease: Option<GenerationLeaseV1>,
    pub initial_attempt_id: Option<String>,
    pub authority_decision_sha256: Option<String>,
    pub admission_snapshot_sha256: Option<String>,
    pub kernel_head_sha256: Option<String>,
    pub session_authority_pin: Option<JournalSessionAuthorityPin>,
    #[cfg(test)]
    pub replayed: bool,
}
