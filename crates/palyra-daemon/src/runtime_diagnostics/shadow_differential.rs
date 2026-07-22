//! Identity-free counters and dashboard projection for RuntimeKernelV2 shadow comparisons.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use serde::Serialize;
use serde_json::{json, Value};

#[cfg(test)]
use crate::application::runtime_kernel_v2::shadow::{
    ShadowAuthorityDenied, ShadowForbiddenService,
};
use crate::{
    application::runtime_kernel_v2::{
        dispatcher::{RuntimeDispatchDecision, RuntimeKernelDispatcher},
        shadow::{
            RuntimeDifferentialClassification, RuntimeDifferentialOutcome, ShadowComparisonPlansV1,
            ShadowEnrollment, ShadowObservationResult,
        },
    },
    gateway::GatewayRuntimeState,
    journal::OrchestratorTapeAppendRequest,
};

/// Schema version for shadow differential diagnostics.
pub(crate) const SHADOW_DIFFERENTIAL_DIAGNOSTICS_SCHEMA_VERSION: u32 = 1;
/// Durable tape event projected into the bounded metadata trace.
pub(crate) const RUNTIME_SHADOW_DIFFERENTIAL_EVENT: &str = "runtime.shadow.differential";

/// Process-local aggregation for fixed-cardinality shadow outcomes.
#[derive(Debug, Default)]
pub(crate) struct ShadowDifferentialDiagnostics {
    not_selected: AtomicU64,
    deterministic_sample: AtomicU64,
    explicit_session: AtomicU64,
    expected: AtomicU64,
    benign: AtomicU64,
    risky: AtomicU64,
    invariant_violation: AtomicU64,
    context_safety_invariant_violation: AtomicU64,
    side_effect_denied: AtomicU64,
    provider_denied: AtomicU64,
    tool_denied: AtomicU64,
    approval_denied: AtomicU64,
    delivery_denied: AtomicU64,
    observation_failed: AtomicU64,
    persistence_failed: AtomicU64,
}

impl ShadowDifferentialDiagnostics {
    /// Records one sampling or comparison result without retaining run identity.
    pub(crate) fn record_observation(&self, result: &ShadowObservationResult) {
        let Some(observed) = result.observed() else {
            saturating_increment(&self.not_selected);
            return;
        };

        match observed.enrollment() {
            ShadowEnrollment::DeterministicSample => {
                saturating_increment(&self.deterministic_sample);
            }
            ShadowEnrollment::ExplicitSession => {
                saturating_increment(&self.explicit_session);
            }
        }
        match observed.report().classification() {
            RuntimeDifferentialClassification::Expected => {
                saturating_increment(&self.expected);
            }
            RuntimeDifferentialClassification::Benign => {
                saturating_increment(&self.benign);
            }
            RuntimeDifferentialClassification::Risky => {
                saturating_increment(&self.risky);
            }
            RuntimeDifferentialClassification::InvariantViolation => {
                saturating_increment(&self.invariant_violation);
            }
        }
        if observed.report().context_safety() == RuntimeDifferentialOutcome::InvariantViolation {
            saturating_increment(&self.context_safety_invariant_violation);
        }
    }

    /// Records an observe-only authority denial by bounded service family.
    #[cfg(test)]
    pub(crate) fn record_authority_denial(&self, denial: ShadowAuthorityDenied) {
        saturating_increment(&self.side_effect_denied);
        match denial.service() {
            ShadowForbiddenService::Provider => saturating_increment(&self.provider_denied),
            ShadowForbiddenService::Tool => saturating_increment(&self.tool_denied),
            ShadowForbiddenService::Approval => saturating_increment(&self.approval_denied),
            ShadowForbiddenService::Delivery => saturating_increment(&self.delivery_denied),
        }
    }

    /// Records a bounded production observation failure without run identity.
    pub(crate) fn record_failure(&self, failure: RuntimeShadowFailureKind) {
        match failure {
            RuntimeShadowFailureKind::Observation => {
                saturating_increment(&self.observation_failed);
            }
            RuntimeShadowFailureKind::Persistence => {
                saturating_increment(&self.persistence_failed);
            }
        }
    }

    /// Returns a point-in-time fixed-size counter snapshot.
    #[must_use]
    pub(crate) fn snapshot(&self) -> ShadowDifferentialDiagnosticsSnapshotV1 {
        let snapshot = ShadowDifferentialDiagnosticsSnapshotV1 {
            schema_version: SHADOW_DIFFERENTIAL_DIAGNOSTICS_SCHEMA_VERSION,
            not_selected: load(&self.not_selected),
            deterministic_sample: load(&self.deterministic_sample),
            explicit_session: load(&self.explicit_session),
            expected: load(&self.expected),
            benign: load(&self.benign),
            risky: load(&self.risky),
            invariant_violation: load(&self.invariant_violation),
            context_safety_invariant_violation: load(&self.context_safety_invariant_violation),
            side_effect_denied: load(&self.side_effect_denied),
            provider_denied: load(&self.provider_denied),
            tool_denied: load(&self.tool_denied),
            approval_denied: load(&self.approval_denied),
            delivery_denied: load(&self.delivery_denied),
            observation_failed: load(&self.observation_failed),
            persistence_failed: load(&self.persistence_failed),
        };
        debug_assert!(snapshot.validate().is_ok());
        snapshot
    }
}

/// Fixed-size, identity-free shadow metrics consumed by diagnostics and promotion tooling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShadowDifferentialDiagnosticsSnapshotV1 {
    schema_version: u32,
    not_selected: u64,
    deterministic_sample: u64,
    explicit_session: u64,
    expected: u64,
    benign: u64,
    risky: u64,
    invariant_violation: u64,
    context_safety_invariant_violation: u64,
    side_effect_denied: u64,
    provider_denied: u64,
    tool_denied: u64,
    approval_denied: u64,
    delivery_denied: u64,
    observation_failed: u64,
    persistence_failed: u64,
}

impl ShadowDifferentialDiagnosticsSnapshotV1 {
    /// Returns the total number of sampled and explicitly selected observations.
    #[must_use]
    pub(crate) const fn selected_total(&self) -> u64 {
        self.deterministic_sample.saturating_add(self.explicit_session)
    }

    /// Returns whether invariant evidence or a side-effect attempt blocks promotion.
    #[must_use]
    pub(crate) const fn promotion_blocked(&self) -> bool {
        self.invariant_violation > 0
            || self.context_safety_invariant_violation > 0
            || self.side_effect_denied > 0
            || self.observation_failed > 0
            || self.persistence_failed > 0
    }

    /// Validates internal counter relationships before publishing a dashboard payload.
    ///
    /// # Errors
    /// Returns a stable error when component counters exceed their aggregate.
    pub(crate) const fn validate(&self) -> Result<(), ShadowDiagnosticsError> {
        let selected = self.selected_total();
        let classified = self
            .expected
            .saturating_add(self.benign)
            .saturating_add(self.risky)
            .saturating_add(self.invariant_violation);
        let denied_by_service = self
            .provider_denied
            .saturating_add(self.tool_denied)
            .saturating_add(self.approval_denied)
            .saturating_add(self.delivery_denied);
        if classified != selected
            || self.context_safety_invariant_violation > self.invariant_violation
            || denied_by_service != self.side_effect_denied
        {
            return Err(ShadowDiagnosticsError::InconsistentCounters);
        }
        Ok(())
    }
}

/// Builds the redacted operator and promotion-gate input payload.
///
/// The projection has fixed keys and no metric labels, hashes, sampled
/// identities, prompts, secrets, or journal payloads.
#[must_use]
pub(crate) fn build_shadow_differential_diagnostics(
    snapshot: ShadowDifferentialDiagnosticsSnapshotV1,
) -> Value {
    let invariant_violation =
        snapshot.invariant_violation > 0 || snapshot.context_safety_invariant_violation > 0;
    let status = if snapshot.promotion_blocked() {
        "promotion_blocked"
    } else if snapshot.risky > 0 {
        "review_required"
    } else {
        "ready"
    };
    json!({
        "schema_version": snapshot.schema_version,
        "status": status,
        "reason_code": match (invariant_violation, snapshot.side_effect_denied > 0, status) {
            (true, _, _) => "runtime.shadow.promotion.invariant_violation",
            (false, true, _) => "runtime.shadow.promotion.side_effect_attempt",
            (false, false, _) if snapshot.observation_failed > 0 =>
                "runtime.shadow.promotion.observation_failed",
            (false, false, _) if snapshot.persistence_failed > 0 =>
                "runtime.shadow.promotion.persistence_failed",
            (false, false, "review_required") => "runtime.shadow.promotion.review_required",
            (false, false, _) => "runtime.shadow.promotion.ready",
        },
        "promotion_blocked": snapshot.promotion_blocked(),
        "authority": {
            "authoritative_runtime": "legacy",
            "shadow_side_effect_free": true,
        },
        "cardinality_policy": {
            "aggregation": "fixed_counters",
            "identity_labels_allowed": false,
            "forbidden_fields": [
                "run_id",
                "session_id",
                "principal",
                "sampling_identity",
                "sample_bucket",
                "prompt",
                "secret",
                "journal_payload"
            ],
        },
        "enrollment": {
            "not_selected_total": snapshot.not_selected,
            "selected_observations_total": snapshot.selected_total(),
            "deterministic_sample_total": snapshot.deterministic_sample,
            "explicit_session_total": snapshot.explicit_session,
        },
        "classifications": {
            "expected_total": snapshot.expected,
            "benign_total": snapshot.benign,
            "risky_total": snapshot.risky,
            "invariant_violation_total": snapshot.invariant_violation,
            "context_safety_invariant_violation_total":
                snapshot.context_safety_invariant_violation,
        },
        "authority_denials": {
            "total": snapshot.side_effect_denied,
            "provider_total": snapshot.provider_denied,
            "tool_total": snapshot.tool_denied,
            "approval_total": snapshot.approval_denied,
            "delivery_total": snapshot.delivery_denied,
        },
        "failures": {
            "observation_total": snapshot.observation_failed,
            "persistence_total": snapshot.persistence_failed,
        },
    })
}

/// Bounded failure classes for the production shadow observation boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeShadowFailureKind {
    /// Authority, sampling, or sanitized-plan validation failed.
    Observation,
    /// The redacted differential event could not be durably appended.
    Persistence,
}

/// Observable result of the production shadow helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeShadowProductionOutcome {
    /// The authoritative route was not `LegacyWithShadow`.
    NotApplicable,
    /// Deterministic sampling excluded the session.
    NotSelected,
    /// One differential event and its matching counter update completed.
    Observed,
    /// Shadow evidence degraded without changing the authoritative legacy run.
    Degraded(RuntimeShadowFailureKind),
}

/// Runs and records the sole production shadow comparison for one legacy decision.
///
/// Non-shadow routes are strict no-ops. Shadow failures update only bounded
/// promotion diagnostics and never terminate or reroute the authoritative run.
pub(crate) async fn observe_and_record_runtime_shadow(
    runtime_state: &Arc<GatewayRuntimeState>,
    dispatcher: &RuntimeKernelDispatcher,
    run_id: &str,
    tape_seq: &mut i64,
    sampling_identity: &[u8],
    decision: &RuntimeDispatchDecision,
    comparison: Option<ShadowComparisonPlansV1>,
) -> RuntimeShadowProductionOutcome {
    let Some(authority) = decision.shadow_authority() else {
        return RuntimeShadowProductionOutcome::NotApplicable;
    };
    let Some(comparison) = comparison else {
        runtime_state.record_runtime_shadow_failure(RuntimeShadowFailureKind::Observation);
        return RuntimeShadowProductionOutcome::Degraded(RuntimeShadowFailureKind::Observation);
    };
    let (authoritative, candidate) = comparison.into_parts();
    let observation =
        match dispatcher.observe_shadow(sampling_identity, authority, &authoritative, candidate) {
            Ok(observation) => observation,
            Err(_) => {
                runtime_state.record_runtime_shadow_failure(RuntimeShadowFailureKind::Observation);
                return RuntimeShadowProductionOutcome::Degraded(
                    RuntimeShadowFailureKind::Observation,
                );
            }
        };
    let Some(observed) = observation.observed() else {
        runtime_state.record_runtime_shadow_observation(&observation);
        return RuntimeShadowProductionOutcome::NotSelected;
    };
    let payload_json = match serde_json::to_string(observed.metadata_event()) {
        Ok(payload_json) => payload_json,
        Err(_) => {
            runtime_state.record_runtime_shadow_failure(RuntimeShadowFailureKind::Persistence);
            return RuntimeShadowProductionOutcome::Degraded(RuntimeShadowFailureKind::Persistence);
        }
    };
    if runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: RUNTIME_SHADOW_DIFFERENTIAL_EVENT.to_owned(),
            payload_json,
        })
        .await
        .is_err()
    {
        runtime_state.record_runtime_shadow_failure(RuntimeShadowFailureKind::Persistence);
        return RuntimeShadowProductionOutcome::Degraded(RuntimeShadowFailureKind::Persistence);
    }
    *tape_seq = tape_seq.saturating_add(1);
    runtime_state.record_runtime_shadow_observation(&observation);
    RuntimeShadowProductionOutcome::Observed
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn saturating_increment(counter: &AtomicU64) {
    // Promotion blockers must remain asserted instead of wrapping to zero.
    let _ = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| Some(value.saturating_add(1)));
}

/// Invalid diagnostics aggregation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ShadowDiagnosticsError {
    /// Fixed component counters do not agree with their aggregate counter.
    #[error("runtime shadow diagnostics counters are inconsistent")]
    InconsistentCounters,
}

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{RuntimeErrorPhase, RuntimeGeneration};
    use serde_json::json;

    use super::*;
    use crate::application::runtime_kernel_v2::shadow::{
        ShadowAuthorityToken, ShadowCandidatePlanInputsV1, ShadowCandidatePlannerV1,
        ShadowComparisonPlansV1, ShadowContextSegmentSemanticV1, ShadowDifferentialObserver,
        ShadowPlanSemanticInputsV1, ShadowPlanSnapshotV1, ShadowPolicySemanticV1,
        ShadowSamplingPolicyV1, ShadowSelectionSemanticV1, ShadowToolCatalogSemanticV1,
    };
    use crate::application::runtime_kernel_v2::{
        dispatcher::RuntimeKernelDispatcher,
        selection::{RuntimeAuthorityDecisionV1, V2RuntimeAvailability},
    };
    use crate::config::{
        FeatureRolloutsConfig, RuntimeKernelConfig, RuntimeKernelProfile, RuntimeKernelSamplingKey,
        RuntimeKernelSamplingKeySource,
    };
    use crate::gateway::runtime::tests::test_runtime_state;
    use crate::journal::{OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest};

    fn plan(seed: char) -> ShadowPlanSnapshotV1 {
        plan_for(RuntimeGeneration::new(23).expect("test generation is non-zero"), seed)
    }

    fn plan_for(generation: RuntimeGeneration, seed: char) -> ShadowPlanSnapshotV1 {
        semantic_inputs(seed, seed, seed, seed, 8_192)
            .into_authoritative_snapshot(
                generation,
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

    fn candidate_planner_for(
        generation: RuntimeGeneration,
        seed: char,
        token_budget: u32,
    ) -> ShadowCandidatePlannerV1 {
        candidate_planner(
            generation,
            semantic_inputs(seed, seed, seed, seed, u64::from(token_budget)),
        )
    }

    fn candidate_planner(
        generation: RuntimeGeneration,
        semantics: ShadowPlanSemanticInputsV1,
    ) -> ShadowCandidatePlannerV1 {
        ShadowCandidatePlannerV1::new(ShadowCandidatePlanInputsV1::new(generation, semantics))
    }

    fn semantic_inputs(
        selection_seed: char,
        context_seed: char,
        catalog_seed: char,
        instruction_seed: char,
        token_budget: u64,
    ) -> ShadowPlanSemanticInputsV1 {
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
                context_seed.to_string().repeat(64),
                42,
                "trusted".to_owned(),
                "volatile".to_owned(),
                None,
            )
            .expect("test context segment should validate")],
            Some(instruction_seed.to_string().repeat(64)),
            None,
            token_budget,
            ShadowToolCatalogSemanticV1::new(
                catalog_seed.to_string().repeat(64),
                "direct".to_owned(),
                0,
            )
            .expect("test catalog should validate"),
            ShadowPolicySemanticV1::new(false, false, 4, None, Some(1_024))
                .expect("test policy should validate"),
        )
        .expect("test semantic inputs should validate")
    }

    fn observe(candidate_seed: char, token_budget: u32) -> ShadowObservationResult {
        let generation = RuntimeGeneration::new(23).expect("test generation is non-zero");
        ShadowDifferentialObserver::new(
            ShadowSamplingPolicyV1::new(0, [7; 32]).expect("zero rate is valid"),
        )
        .observe(
            b"not-retained",
            true,
            &authority_decision(),
            &plan('a'),
            candidate_planner_for(generation, candidate_seed, token_budget),
        )
        .expect("explicit observation should succeed")
    }

    fn authority_decision() -> RuntimeAuthorityDecisionV1 {
        authority_decision_for(
            RuntimeGeneration::new(23).expect("test generation is non-zero"),
            "v2_shadow",
            "legacy",
            true,
            "v2_shadow_legacy_authority",
            "runtime.selection.v2_shadow_legacy_authority",
        )
    }

    fn authority_decision_for(
        generation: RuntimeGeneration,
        profile: &str,
        selected_runtime: &str,
        shadow_evaluation_enabled: bool,
        reason: &str,
        reason_code: &str,
    ) -> RuntimeAuthorityDecisionV1 {
        serde_json::from_value(json!({
            "schema_version": 1,
            "profile": profile,
            "generation": generation,
            "disposition": "selected",
            "selected_runtime": selected_runtime,
            "shadow_evaluation_enabled": shadow_evaluation_enabled,
            "reason": reason,
            "reason_code": reason_code,
            "v2_unavailability": null
        }))
        .expect("test authority decision should validate")
    }

    fn shadow_dispatcher() -> RuntimeKernelDispatcher {
        let config = RuntimeKernelConfig {
            profile: RuntimeKernelProfile::V2Shadow,
            shadow_sample_basis_points: 10_000,
            sampling_key_source: Some(RuntimeKernelSamplingKeySource::Inline(
                RuntimeKernelSamplingKey::parse_hex(&"07".repeat(32), "test sampling key")
                    .expect("test sampling key should validate"),
            )),
            ..RuntimeKernelConfig::default()
        };
        RuntimeKernelDispatcher::resolve(
            &config,
            &FeatureRolloutsConfig::default(),
            None,
            false,
            V2RuntimeAvailability::Ready,
        )
        .expect("shadow dispatcher should resolve")
    }

    #[test]
    fn diagnostics_aggregate_fixed_classifications_and_block_invariant_promotion() {
        let diagnostics = ShadowDifferentialDiagnostics::default();
        diagnostics.record_observation(&observe('a', 8_192));
        diagnostics.record_observation(&observe('b', 8_192));

        let snapshot = diagnostics.snapshot();
        assert!(snapshot.validate().is_ok());
        assert!(snapshot.promotion_blocked());
        let payload = build_shadow_differential_diagnostics(snapshot);
        assert_eq!(payload["status"], "promotion_blocked");
        assert_eq!(payload["authority"]["authoritative_runtime"], "legacy");
        assert_eq!(payload["enrollment"]["selected_observations_total"], 2);
        assert_eq!(payload["classifications"]["expected_total"], 1);
        assert_eq!(payload["classifications"]["invariant_violation_total"], 1);
        assert_eq!(payload["classifications"]["context_safety_invariant_violation_total"], 1);
        let encoded = payload.to_string();
        assert!(!encoded.contains("not-retained"));
        assert!(!encoded.contains(&"a".repeat(64)));
        assert!(!encoded.contains(&"b".repeat(64)));
    }

    #[test]
    fn authority_denials_are_aggregated_only_by_bounded_service_family() {
        let diagnostics = ShadowDifferentialDiagnostics::default();
        let authority = ShadowAuthorityToken::new(
            RuntimeGeneration::new(23).expect("test generation is non-zero"),
        );
        for service in [
            ShadowForbiddenService::Provider,
            ShadowForbiddenService::Tool,
            ShadowForbiddenService::Approval,
            ShadowForbiddenService::Delivery,
        ] {
            diagnostics.record_authority_denial(
                authority
                    .request_side_effect(service)
                    .expect_err("shadow side effects must be denied"),
            );
        }

        let payload = build_shadow_differential_diagnostics(diagnostics.snapshot());
        assert_eq!(payload["status"], "promotion_blocked");
        assert_eq!(payload["reason_code"], "runtime.shadow.promotion.side_effect_attempt");
        assert_eq!(payload["authority_denials"]["total"], 4);
        assert_eq!(payload["authority_denials"]["provider_total"], 1);
        assert_eq!(payload["authority_denials"]["tool_total"], 1);
        assert_eq!(payload["authority_denials"]["approval_total"], 1);
        assert_eq!(payload["authority_denials"]["delivery_total"], 1);
    }

    #[tokio::test]
    async fn production_helper_persists_one_risky_event_and_matching_counter() {
        let runtime = test_runtime_state();
        let run_id = "01ARZ3NDEKTSV4RRFFQ69G5FS1";
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FS2";
        runtime
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: session_id.to_owned(),
                session_key: session_id.to_owned(),
                session_label: None,
                principal: "user:test".to_owned(),
                device_id: "device:runtime-shadow-test".to_owned(),
                channel: Some("test".to_owned()),
            })
            .expect("test session should persist before its run starts");
        runtime
            .start_orchestrator_run(OrchestratorRunStartRequest {
                run_id: run_id.to_owned(),
                session_id: session_id.to_owned(),
                origin_kind: "runtime-shadow-test".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:test".to_owned()),
                parameter_delta_json: None,
                delegated_admission: None,
            })
            .await
            .expect("test run should start");
        let (_, generation) = runtime
            .runtime_generation_for_run(run_id.to_owned())
            .await
            .expect("generation lookup should succeed")
            .expect("test run should have a generation");
        let dispatcher = shadow_dispatcher();
        let decision = dispatcher
            .dispatch_decision(authority_decision_for(
                generation,
                "v2_shadow",
                "legacy",
                true,
                "v2_shadow_legacy_authority",
                "runtime.selection.v2_shadow_legacy_authority",
            ))
            .expect("shadow decision should dispatch");
        let comparison = ShadowComparisonPlansV1::new(
            plan_for(generation, 'a'),
            candidate_planner(generation, semantic_inputs('b', 'a', 'a', 'a', 8_192)),
        )
        .expect("comparison plans should validate");
        let mut tape_seq = 0;

        let outcome = observe_and_record_runtime_shadow(
            &runtime,
            &dispatcher,
            run_id,
            &mut tape_seq,
            session_id.as_bytes(),
            &decision,
            Some(comparison),
        )
        .await;

        assert_eq!(outcome, RuntimeShadowProductionOutcome::Observed);
        assert_eq!(tape_seq, 1);
        let trace = runtime
            .metadata_trace_snapshot(run_id.to_owned())
            .await
            .expect("metadata trace should load");
        let shadow_events = trace
            .segments
            .iter()
            .flat_map(|segment| segment.events.iter())
            .filter(|event| event.kind() == "runtime_shadow_differential")
            .collect::<Vec<_>>();
        assert_eq!(shadow_events.len(), 1);
        let palyra_common::metadata_trace::MetadataTraceEventDataV1::RuntimeShadowDifferential(
            metadata,
        ) = &shadow_events[0].event
        else {
            panic!("shadow event must use the typed metadata variant");
        };
        assert_eq!(
            metadata.classification,
            palyra_common::metadata_trace::MetadataTraceShadowClassificationV1::Risky
        );
        assert_eq!(metadata.reason_code, "runtime.shadow.differential_risky");
        let payload =
            build_shadow_differential_diagnostics(runtime.runtime_shadow_diagnostics_snapshot());
        assert_eq!(payload["enrollment"]["selected_observations_total"], 1);
        assert_eq!(payload["classifications"]["risky_total"], 1);
        assert_eq!(payload["failures"]["observation_total"], 0);
        assert_eq!(payload["failures"]["persistence_total"], 0);
    }

    #[tokio::test]
    async fn production_helper_does_not_observe_legacy_or_v2_routes() {
        let runtime = test_runtime_state();
        let dispatcher =
            RuntimeKernelDispatcher::legacy_default().expect("legacy dispatcher should resolve");
        let generation = RuntimeGeneration::new(1).expect("test generation is non-zero");
        let legacy = dispatcher
            .dispatch_decision(authority_decision_for(
                generation,
                "legacy",
                "legacy",
                false,
                "legacy_profile_selected",
                "runtime.selection.legacy_profile_selected",
            ))
            .expect("legacy decision should dispatch");
        let v2 = dispatcher
            .dispatch_decision(authority_decision_for(
                generation,
                "v2",
                "v2",
                false,
                "v2_profile_selected",
                "runtime.selection.v2_profile_selected",
            ))
            .expect("V2 decision should dispatch");
        let mut tape_seq = 7;

        for decision in [&legacy, &v2] {
            assert_eq!(
                observe_and_record_runtime_shadow(
                    &runtime,
                    &dispatcher,
                    "01ARZ3NDEKTSV4RRFFQ69G5FT1",
                    &mut tape_seq,
                    b"session",
                    decision,
                    None,
                )
                .await,
                RuntimeShadowProductionOutcome::NotApplicable
            );
        }

        assert_eq!(tape_seq, 7);
        let payload =
            build_shadow_differential_diagnostics(runtime.runtime_shadow_diagnostics_snapshot());
        assert_eq!(payload["enrollment"]["selected_observations_total"], 0);
        assert_eq!(payload["enrollment"]["not_selected_total"], 0);
        assert_eq!(payload["failures"]["observation_total"], 0);
        assert_eq!(payload["failures"]["persistence_total"], 0);
    }
}
