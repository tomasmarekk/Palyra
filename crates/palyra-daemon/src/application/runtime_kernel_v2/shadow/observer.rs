//! Deterministic enrollment and observe-only execution boundary for V2 shadow planning.

use std::fmt;

use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    compare_shadow_plans, RuntimeDifferentialClassification, RuntimeDifferentialError,
    RuntimeDifferentialOutcome, RuntimeDifferentialReportV1, ShadowAuthorityToken,
    ShadowCandidatePlannerV1, ShadowPlanSnapshotV1, RUNTIME_DIFFERENTIAL_SCHEMA_VERSION,
};
use crate::application::runtime_kernel_v2::{
    selection::{RuntimeAuthority, RuntimeAuthorityDecisionV1, RuntimeAuthorityReason},
    RuntimeKernelVersion,
};

const SHADOW_SAMPLE_DOMAIN: &[u8] = b"palyra.runtime_kernel_v2.shadow.sample.v1\0";
const SHADOW_SAMPLE_DENOMINATOR_BPS: u16 = 10_000;
const MAX_SHADOW_SAMPLING_IDENTITY_BYTES: usize = 512;
const SHADOW_DIFFERENTIAL_EVENT_NAME: &str = "runtime.shadow.differential";

/// Fixed sampling posture for shadow evaluation.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShadowSamplingPolicyV1 {
    sample_rate_bps: u16,
    sampling_key: [u8; 32],
}

impl fmt::Debug for ShadowSamplingPolicyV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShadowSamplingPolicyV1")
            .field("sample_rate_bps", &self.sample_rate_bps)
            .field("sampling_key", &"<redacted>")
            .finish()
    }
}

impl ShadowSamplingPolicyV1 {
    /// Creates a deterministic sampling policy.
    ///
    /// # Errors
    /// Returns [`ShadowObserverError::InvalidSamplingPolicy`] when the rate is
    /// greater than 10,000 basis points.
    pub(crate) const fn new(
        sample_rate_bps: u16,
        sampling_key: [u8; 32],
    ) -> Result<Self, ShadowObserverError> {
        if sample_rate_bps > SHADOW_SAMPLE_DENOMINATOR_BPS
            || is_all_zero_sampling_key(&sampling_key)
        {
            return Err(ShadowObserverError::InvalidSamplingPolicy);
        }
        Ok(Self { sample_rate_bps, sampling_key })
    }
}

/// Low-cardinality reason that one run entered shadow evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShadowEnrollment {
    /// The identity hash fell inside the configured deterministic sample.
    DeterministicSample,
    /// The host explicitly selected this session for shadow observation.
    ExplicitSession,
}

impl ShadowEnrollment {
    const fn reason_code(self) -> &'static str {
        match self {
            Self::DeterministicSample => "runtime.shadow.enrollment.deterministic_sample",
            Self::ExplicitSession => "runtime.shadow.enrollment.explicit_session",
        }
    }
}

/// Side-effect service families that shadow authority can never acquire.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShadowForbiddenService {
    /// Model-provider request execution.
    Provider,
    /// Tool proposal or execution.
    Tool,
    /// Approval creation or resolution.
    Approval,
    /// Client or channel delivery.
    Delivery,
}

#[cfg(test)]
impl ShadowForbiddenService {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::Tool => "tool",
            Self::Approval => "approval",
            Self::Delivery => "delivery",
        }
    }
}

/// Stable denial returned when shadow code requests side-effect authority.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("runtime shadow side-effect service denied: {service}")]
pub(crate) struct ShadowAuthorityDenied {
    service: ShadowForbiddenService,
}

#[cfg(test)]
impl ShadowAuthorityDenied {
    pub(super) const fn new(service: ShadowForbiddenService) -> Self {
        Self { service }
    }

    /// Returns the denied bounded service family.
    #[must_use]
    pub(crate) const fn service(self) -> ShadowForbiddenService {
        self.service
    }

    /// Returns the metadata-only reason code suitable for diagnostics.
    #[must_use]
    pub(crate) const fn reason_code(self) -> &'static str {
        "runtime.shadow.side_effect_authority_denied"
    }
}

/// Metadata-only event projection for one selected shadow comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShadowDifferentialMetadataEventV1 {
    schema_version: u32,
    event_name: &'static str,
    redaction_level: &'static str,
    authoritative_runtime: RuntimeAuthority,
    shadow_side_effect_free: bool,
    enrollment: ShadowEnrollment,
    enrollment_reason_code: &'static str,
    classification: RuntimeDifferentialClassification,
    reason_code: String,
    runtime_selection: RuntimeDifferentialOutcome,
    context_segments: RuntimeDifferentialOutcome,
    context_safety: RuntimeDifferentialOutcome,
    token_budget: RuntimeDifferentialOutcome,
    tool_catalog: RuntimeDifferentialOutcome,
    policy_input: RuntimeDifferentialOutcome,
    phase_plan: RuntimeDifferentialOutcome,
    promotion_blocked: bool,
}

impl ShadowDifferentialMetadataEventV1 {
    fn from_report(enrollment: ShadowEnrollment, report: &RuntimeDifferentialReportV1) -> Self {
        Self {
            schema_version: RUNTIME_DIFFERENTIAL_SCHEMA_VERSION,
            event_name: SHADOW_DIFFERENTIAL_EVENT_NAME,
            redaction_level: "metadata_only",
            authoritative_runtime: RuntimeAuthority::Legacy,
            shadow_side_effect_free: true,
            enrollment,
            enrollment_reason_code: enrollment.reason_code(),
            classification: report.classification(),
            reason_code: report.reason_code().to_owned(),
            runtime_selection: report.runtime_selection,
            context_segments: report.context_segments,
            context_safety: report.context_safety,
            token_budget: report.token_budget,
            tool_catalog: report.tool_catalog,
            policy_input: report.policy_input,
            phase_plan: report.phase_plan,
            promotion_blocked: report.blocks_promotion(),
        }
    }
}

/// One completed observe-only comparison with explicit legacy authority evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShadowObservedDifferentialV1 {
    authoritative_runtime: RuntimeAuthority,
    enrollment: ShadowEnrollment,
    report: RuntimeDifferentialReportV1,
    metadata_event: ShadowDifferentialMetadataEventV1,
}

impl ShadowObservedDifferentialV1 {
    /// Returns the authoritative runtime, which is always legacy in shadow mode.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn authoritative_runtime(&self) -> RuntimeAuthority {
        self.authoritative_runtime
    }

    /// Returns the bounded enrollment class without the sampled identity or bucket.
    #[must_use]
    pub(crate) const fn enrollment(&self) -> ShadowEnrollment {
        self.enrollment
    }

    /// Returns the fixed-size differential report.
    #[must_use]
    pub(crate) const fn report(&self) -> &RuntimeDifferentialReportV1 {
        &self.report
    }

    /// Returns the redacted metadata event projection.
    #[must_use]
    pub(crate) const fn metadata_event(&self) -> &ShadowDifferentialMetadataEventV1 {
        &self.metadata_event
    }
}

/// Result of applying shadow enrollment before invoking a candidate planner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum ShadowObservationResult {
    /// The deterministic sample excluded this identity; no candidate planner ran.
    NotSelected {
        /// Stable, identity-free reason.
        reason_code: &'static str,
    },
    /// A selected candidate was compared with the authoritative legacy plan.
    Observed(ShadowObservedDifferentialV1),
}

impl ShadowObservationResult {
    /// Returns the completed observation, or `None` when sampling excluded the session.
    #[must_use]
    pub(crate) const fn observed(&self) -> Option<&ShadowObservedDifferentialV1> {
        match self {
            Self::NotSelected { .. } => None,
            Self::Observed(observed) => Some(observed),
        }
    }
}

/// Executes an optional pure candidate planner and compares its sanitized output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShadowDifferentialObserver {
    sampling: ShadowSamplingPolicyV1,
}

impl ShadowDifferentialObserver {
    /// Creates an observer with a fixed deterministic sampling policy.
    #[must_use]
    pub(crate) const fn new(sampling: ShadowSamplingPolicyV1) -> Self {
        Self { sampling }
    }

    /// Consumes the data-only candidate planner for sampled or explicitly selected sessions.
    ///
    /// # Errors
    /// Returns [`ShadowObserverError`] for invalid sampling identities, rejected
    /// plans or generation mismatches.
    pub(crate) fn observe(
        &self,
        sampling_identity: &[u8],
        explicit_session: bool,
        authority_decision: &RuntimeAuthorityDecisionV1,
        authoritative: &ShadowPlanSnapshotV1,
        candidate_planner: ShadowCandidatePlannerV1,
    ) -> Result<ShadowObservationResult, ShadowObserverError> {
        authority_decision.validate().map_err(|_| ShadowObserverError::InvalidAuthorityDecision)?;
        if authority_decision.profile() != RuntimeKernelVersion::V2Shadow
            || authority_decision.selected_runtime() != Some(RuntimeAuthority::Legacy)
            || !authority_decision.shadow_evaluation_enabled()
            || authority_decision.reason() != RuntimeAuthorityReason::V2ShadowLegacyAuthority
            || authority_decision.generation() != authoritative.generation()
        {
            return Err(ShadowObserverError::InvalidAuthorityDecision);
        }
        authoritative.validate()?;
        let Some(enrollment) = self.enrollment(sampling_identity, explicit_session)? else {
            return Ok(ShadowObservationResult::NotSelected {
                reason_code: "runtime.shadow.enrollment.sample_excluded",
            });
        };

        let authority = ShadowAuthorityToken::new(authoritative.generation());
        let candidate = candidate_planner.plan(&authority)?;
        let report = compare_shadow_plans(
            authority.selection(),
            authority.plan(),
            authoritative,
            &candidate,
        )?;
        let metadata_event = ShadowDifferentialMetadataEventV1::from_report(enrollment, &report);
        Ok(ShadowObservationResult::Observed(ShadowObservedDifferentialV1 {
            authoritative_runtime: RuntimeAuthority::Legacy,
            enrollment,
            report,
            metadata_event,
        }))
    }

    fn enrollment(
        &self,
        sampling_identity: &[u8],
        explicit_session: bool,
    ) -> Result<Option<ShadowEnrollment>, ShadowObserverError> {
        if explicit_session {
            return Ok(Some(ShadowEnrollment::ExplicitSession));
        }
        if sampling_identity.is_empty()
            || sampling_identity.len() > MAX_SHADOW_SAMPLING_IDENTITY_BYTES
        {
            return Err(ShadowObserverError::InvalidSamplingIdentity);
        }
        if self.sampling.sample_rate_bps == 0 {
            return Ok(None);
        }

        let bucket = deterministic_bucket(&self.sampling.sampling_key, sampling_identity);
        Ok((bucket < self.sampling.sample_rate_bps)
            .then_some(ShadowEnrollment::DeterministicSample))
    }
}

fn deterministic_bucket(sampling_key: &[u8; 32], identity: &[u8]) -> u16 {
    let mut hasher = Sha256::new();
    hasher.update(SHADOW_SAMPLE_DOMAIN);
    hasher.update(sampling_key);
    hasher.update(identity);
    let digest = hasher.finalize();
    u16::from_be_bytes([digest[0], digest[1]]) % SHADOW_SAMPLE_DENOMINATOR_BPS
}

const fn is_all_zero_sampling_key(sampling_key: &[u8; 32]) -> bool {
    let mut index = 0;
    while index < sampling_key.len() {
        if sampling_key[index] != 0 {
            return false;
        }
        index += 1;
    }
    true
}

/// Fail-closed shadow enrollment or comparison error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum ShadowObserverError {
    /// The sampling rate exceeded 100%.
    #[error("runtime shadow sampling policy is invalid")]
    InvalidSamplingPolicy,
    /// The sampling identity was empty or exceeded its transient input bound.
    #[error("runtime shadow sampling identity is invalid")]
    InvalidSamplingIdentity,
    /// Runtime selection did not prove a legacy-authoritative V2 shadow generation.
    #[error("runtime shadow authority decision is invalid")]
    InvalidAuthorityDecision,
    /// The sanitized plan or report contract failed validation.
    #[error(transparent)]
    Differential(#[from] RuntimeDifferentialError),
}

#[cfg(test)]
impl fmt::Display for ShadowForbiddenService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{RuntimeErrorPhase, RuntimeGeneration};
    use serde_json::{json, Value};

    use super::*;
    use crate::application::runtime_kernel_v2::shadow::{
        ShadowCandidatePlanInputsV1, ShadowContextSegmentSemanticV1, ShadowPlanSemanticInputsV1,
        ShadowPolicySemanticV1, ShadowSelectionSemanticV1, ShadowToolCatalogSemanticV1,
    };
    fn generation() -> RuntimeGeneration {
        RuntimeGeneration::new(23).expect("test generation is non-zero")
    }

    fn semantics(selection_seed: char, instruction_seed: char) -> ShadowPlanSemanticInputsV1 {
        ShadowPlanSemanticInputsV1::new(
            ShadowSelectionSemanticV1::new(
                format!("provider-{selection_seed}"),
                format!("model-{selection_seed}"),
                format!("credential-{selection_seed}"),
                "healthy".to_owned(),
            )
            .expect("test selection should validate"),
            vec![ShadowContextSegmentSemanticV1::new(
                "current_turn".to_owned(),
                "a".repeat(64),
                42,
                "trusted".to_owned(),
                "volatile".to_owned(),
                None,
            )
            .expect("test segment should validate")],
            Some(instruction_seed.to_string().repeat(64)),
            None,
            8_192,
            ShadowToolCatalogSemanticV1::new("d".repeat(64), "direct".to_owned(), 0)
                .expect("test catalog should validate"),
            ShadowPolicySemanticV1::new(false, false, 4, None, Some(1_024))
                .expect("test policy should validate"),
        )
        .expect("test semantic inputs should validate")
    }

    fn plan(seed: char) -> ShadowPlanSnapshotV1 {
        semantics(seed, 'c')
            .into_authoritative_snapshot(
                generation(),
                vec![
                    RuntimeErrorPhase::Admission,
                    RuntimeErrorPhase::RuntimeSelection,
                    RuntimeErrorPhase::ContextAssembly,
                    RuntimeErrorPhase::ProviderCall,
                    RuntimeErrorPhase::Verification,
                    RuntimeErrorPhase::Finalization,
                    RuntimeErrorPhase::DeliveryIntent,
                ],
            )
            .expect("test authoritative plan should project")
    }

    fn planner(selection_seed: char, instruction_seed: char) -> ShadowCandidatePlannerV1 {
        ShadowCandidatePlannerV1::new(ShadowCandidatePlanInputsV1::new(
            generation(),
            semantics(selection_seed, instruction_seed),
        ))
    }

    fn authority_decision() -> RuntimeAuthorityDecisionV1 {
        serde_json::from_value(json!({
            "schema_version": 1,
            "profile": "v2_shadow",
            "generation": 23,
            "disposition": "selected",
            "selected_runtime": "legacy",
            "shadow_evaluation_enabled": true,
            "reason": "v2_shadow_legacy_authority",
            "reason_code": "runtime.selection.v2_shadow_legacy_authority",
            "v2_unavailability": null
        }))
        .expect("test authority decision should validate")
    }

    #[test]
    fn zero_sample_skips_candidate_planning_and_retains_no_identity() {
        let observer = ShadowDifferentialObserver::new(
            ShadowSamplingPolicyV1::new(0, [7; 32]).expect("zero rate is valid"),
        );
        let result = observer
            .observe(
                b"session-private",
                false,
                &authority_decision(),
                &plan('a'),
                planner('b', 'c'),
            )
            .expect("sampling should succeed");

        assert_eq!(
            result,
            ShadowObservationResult::NotSelected {
                reason_code: "runtime.shadow.enrollment.sample_excluded"
            }
        );
        assert!(!serde_json::to_string(&result)
            .expect("result should serialize")
            .contains("session-private"));
    }

    #[test]
    fn deterministic_sampling_is_stable_and_identity_free() {
        let first = deterministic_bucket(&[7; 32], b"same-session");
        let second = deterministic_bucket(&[7; 32], b"same-session");
        let other = deterministic_bucket(&[7; 32], b"different-session");
        let different_deployment = deterministic_bucket(&[8; 32], b"same-session");

        assert_eq!(first, second);
        assert!(first < SHADOW_SAMPLE_DENOMINATOR_BPS);
        assert!(other < SHADOW_SAMPLE_DENOMINATOR_BPS);
        assert!(different_deployment < SHADOW_SAMPLE_DENOMINATOR_BPS);
    }

    #[test]
    fn explicit_session_runs_even_when_sampling_is_disabled() {
        let observer = ShadowDifferentialObserver::new(
            ShadowSamplingPolicyV1::new(0, [7; 32]).expect("zero rate is valid"),
        );
        let result = observer
            .observe(&[], true, &authority_decision(), &plan('a'), planner('a', 'c'))
            .expect("explicit observation should succeed");
        let ShadowObservationResult::Observed(observed) = result else {
            panic!("explicit sessions must be observed");
        };

        assert_eq!(observed.authoritative_runtime(), RuntimeAuthority::Legacy);
        assert_eq!(observed.enrollment(), ShadowEnrollment::ExplicitSession);
        assert_eq!(observed.report().classification(), RuntimeDifferentialClassification::Expected);
    }

    #[test]
    fn provider_tool_approval_and_delivery_authority_are_all_denied() {
        let authority = ShadowAuthorityToken::new(generation());

        for service in [
            ShadowForbiddenService::Provider,
            ShadowForbiddenService::Tool,
            ShadowForbiddenService::Approval,
            ShadowForbiddenService::Delivery,
        ] {
            let denied = authority
                .request_side_effect(service)
                .expect_err("shadow authority must never grant side-effect services");
            assert_eq!(denied.service(), service);
            assert_eq!(denied.reason_code(), "runtime.shadow.side_effect_authority_denied");
        }
    }

    #[test]
    fn metadata_event_is_bounded_redacted_and_blocks_invariant_divergence() {
        let observer = ShadowDifferentialObserver::new(
            ShadowSamplingPolicyV1::new(0, [7; 32]).expect("zero rate is valid"),
        );
        let result = observer
            .observe(
                b"never-serialized",
                true,
                &authority_decision(),
                &plan('a'),
                planner('a', 'b'),
            )
            .expect("comparison should succeed");
        let ShadowObservationResult::Observed(observed) = result else {
            panic!("explicit sessions must be observed");
        };
        let event = serde_json::to_value(observed.metadata_event())
            .expect("metadata event should serialize");

        assert_eq!(event["authoritative_runtime"], "legacy");
        assert_eq!(event["shadow_side_effect_free"], true);
        assert_eq!(event["promotion_blocked"], true);
        assert_eq!(event["redaction_level"], "metadata_only");
        assert_eq!(event["reason_code"], "runtime.shadow.differential_invariant_violation");
        assert!(!contains_sensitive_field(&event));
        assert!(!event.to_string().contains(&"a".repeat(64)));
        assert!(!event.to_string().contains(&"b".repeat(64)));
    }

    #[test]
    fn invalid_sampling_inputs_fail_closed_before_planning() {
        assert_eq!(
            ShadowSamplingPolicyV1::new(10_001, [7; 32]),
            Err(ShadowObserverError::InvalidSamplingPolicy)
        );
        assert_eq!(
            ShadowSamplingPolicyV1::new(1, [0; 32]),
            Err(ShadowObserverError::InvalidSamplingPolicy)
        );
        let observer = ShadowDifferentialObserver::new(
            ShadowSamplingPolicyV1::new(1, [7; 32]).expect("one basis point is valid"),
        );
        assert_eq!(
            observer.observe(&[], false, &authority_decision(), &plan('a'), planner('a', 'c'),),
            Err(ShadowObserverError::InvalidSamplingIdentity)
        );
        assert_eq!(
            observer.observe(
                &vec![b'x'; MAX_SHADOW_SAMPLING_IDENTITY_BYTES + 1],
                false,
                &authority_decision(),
                &plan('a'),
                planner('a', 'c'),
            ),
            Err(ShadowObserverError::InvalidSamplingIdentity)
        );
    }

    #[test]
    fn observer_rejects_any_decision_without_legacy_shadow_authority() {
        let observer = ShadowDifferentialObserver::new(
            ShadowSamplingPolicyV1::new(1, [7; 32]).expect("one basis point is valid"),
        );
        let invalid: RuntimeAuthorityDecisionV1 = serde_json::from_value(json!({
            "schema_version": 1,
            "profile": "legacy",
            "generation": 23,
            "disposition": "selected",
            "selected_runtime": "legacy",
            "shadow_evaluation_enabled": false,
            "reason": "legacy_profile_selected",
            "reason_code": "runtime.selection.legacy_profile_selected",
            "v2_unavailability": null
        }))
        .expect("legacy decision should validate");

        assert_eq!(
            observer.observe(b"sampled-session", true, &invalid, &plan('a'), planner('a', 'c'),),
            Err(ShadowObserverError::InvalidAuthorityDecision)
        );
    }

    fn contains_sensitive_field(value: &Value) -> bool {
        match value {
            Value::Object(fields) => fields.iter().any(|(key, nested)| {
                matches!(
                    key.as_str(),
                    "prompt"
                        | "raw_prompt"
                        | "secret"
                        | "session_id"
                        | "sampling_identity"
                        | "sample_bucket"
                        | "journal"
                ) || contains_sensitive_field(nested)
            }),
            Value::Array(values) => values.iter().any(contains_sensitive_field),
            _ => false,
        }
    }
}
