use palyra_common::runtime_contracts::{
    CircuitBreakerPolicy, GenerationLeaseV1, RuntimeAuthorityClass, RuntimeComponentHealthV1,
    RuntimeGeneration, RuntimeGenerationLane, RuntimeHealthState, RuntimeIdentitySetV1,
    RuntimeInstanceId, RuntimeLeaseId, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
    SideEffectFenceState,
};

use super::{
    authority::{OutputProgressV1, RuntimeFallbackTriggerV1, RuntimeSelectionProgressV1},
    bounded::{BoundedVec, SafeLabel},
    candidates::{
        AuthCandidatePolicyReferenceV1, AuthSelectionModeV1, ContextEngineBindingV1,
        ContextEngineRegistryCandidateV1, HarnessBindingV1, HarnessRegistryCandidateV1,
        HostCandidateRegistryProof, ProviderRegistryCandidateV1, ProviderRouteBindingV1,
        ProviderRouteClassV1, RuntimeHealthAuthoritySourceV1, SealedRuntimeCandidateRegistryV1,
    },
    catalog::SealedToolCatalogSelectionV1,
    digest::SelectionDigest,
    health::{
        HealthSnapshotWireV1, HostHealthSnapshotProof, HostResidentReadinessProof,
        HostResidentReadinessV1, ImmutableHealthSnapshotV1,
    },
    policies::{
        FallbackPermissionV1, RuntimeCapabilityRequirementsV1, RuntimeFallbackPolicyV1,
        SelectionEpochsV1, SessionOverridePolicyV1,
    },
    projection::{CandidateSelectedReasonV1, RuntimeSelectionV1},
    service::{
        AdmissionSnapshotReferenceV1, ExecutionProfileBindingV1, MiddlewareChainBindingV1,
        RuntimeSelectionRequest, RuntimeSelectionService,
    },
    HostRuntimeSelectionAuthorityProof,
};
use crate::application::{
    agent_harness::AgentHarnessDescriptor, context_engine::ContextEngineDescriptor,
    runtime_kernel_v2::selection::RuntimeAuthorityDecisionV1,
};

fn label(value: &str) -> SafeLabel {
    SafeLabel::parse(value.to_owned()).expect("safe test label")
}

fn digest(value: &str) -> SelectionDigest {
    SelectionDigest::from_domain_bytes(b"test\0", value.as_bytes())
}

fn generation() -> RuntimeGeneration {
    RuntimeGeneration::new(7).expect("generation")
}

fn instance(value: &str) -> RuntimeInstanceId {
    RuntimeInstanceId::parse(value).expect("instance")
}

fn identities() -> RuntimeIdentitySetV1 {
    RuntimeIdentitySetV1::for_run(
        RuntimeTraceId::parse("trace_selection_01").expect("trace"),
        RuntimeSessionId::parse("session_selection_01").expect("session"),
        RuntimeRunId::parse("run_selection_01").expect("run"),
        generation(),
    )
}

fn lease() -> GenerationLeaseV1 {
    let identities = identities();
    GenerationLeaseV1 {
        schema_version: 1,
        lease_id: RuntimeLeaseId::parse("lease_selection_01").expect("lease"),
        session_id: identities.session_id,
        run_id: Some(identities.run_id),
        lane: RuntimeGenerationLane::Run,
        generation: generation(),
        owner: "runtime-selection-test".to_owned(),
        acquired_at_unix_ms: 10,
        expires_at_unix_ms: 1_000,
    }
}

fn authority_decision() -> RuntimeAuthorityDecisionV1 {
    serde_json::from_value(serde_json::json!({
        "schema_version": 1,
        "profile": "v2",
        "generation": 7,
        "disposition": "selected",
        "selected_runtime": "v2",
        "shadow_evaluation_enabled": false,
        "reason": "v2_profile_selected",
        "reason_code": "runtime.selection.v2_profile_selected"
    }))
    .expect("valid authority decision")
}

fn epochs() -> SelectionEpochsV1 {
    SelectionEpochsV1::new(4, 9).expect("epochs")
}

fn proof() -> HostRuntimeSelectionAuthorityProof {
    HostRuntimeSelectionAuthorityProof::test_only(
        identities(),
        lease(),
        authority_decision(),
        digest("admission"),
        digest("persisted-admission-token"),
        epochs(),
    )
    .expect("authority proof")
}

fn health(component_id: &str, state: RuntimeHealthState) -> RuntimeComponentHealthV1 {
    RuntimeComponentHealthV1 {
        schema_version: 1,
        component_id: instance(component_id),
        generation: generation(),
        state,
        authority_class: RuntimeAuthorityClass::ScopedMutation,
        strike_count: u32::from(state != RuntimeHealthState::Healthy),
        reason_code: if state == RuntimeHealthState::Healthy {
            "runtime.health.healthy".to_owned()
        } else {
            "runtime.health.unavailable".to_owned()
        },
        first_failure_at_unix_ms: None,
        last_failure_at_unix_ms: None,
        expires_at_unix_ms: None,
        fallback_component_id: None,
        fallback_authority_class: None,
        security_quarantine: state == RuntimeHealthState::Quarantined,
        policy: CircuitBreakerPolicy {
            strike_threshold: 3,
            cooldown_ms: 100,
            max_probe_concurrency: 1,
            security_quarantine_auto_clear: false,
        },
        updated_at_unix_ms: 20,
    }
}

fn harness_candidate(
    id: &str,
    embedded: bool,
    health_id: &str,
    rank: u16,
) -> HarnessRegistryCandidateV1 {
    let descriptor = AgentHarnessDescriptor::new(id, format!("{id} label"), embedded);
    HarnessRegistryCandidateV1::new(
        HarnessBindingV1::from_registry_descriptor(
            &descriptor,
            label("1.0.0"),
            RuntimeAuthorityClass::ScopedMutation,
        )
        .expect("harness binding"),
        instance(health_id),
        Vec::new(),
        rank,
    )
    .expect("harness candidate")
}

fn context_candidate() -> ContextEngineRegistryCandidateV1 {
    let descriptor = ContextEngineDescriptor {
        engine_id: "context-main".to_owned(),
        label: "Context main".to_owned(),
        version: "1.0.0".to_owned(),
        lifecycle_hooks: vec!["prepare_context".to_owned()],
    };
    ContextEngineRegistryCandidateV1::new(
        ContextEngineBindingV1::from_registry_descriptor(
            &descriptor,
            11,
            RuntimeAuthorityClass::ScopedMutation,
        )
        .expect("context binding"),
        instance("health-context-main"),
        Vec::new(),
        0,
    )
    .expect("context candidate")
}

fn provider_candidate(
    id: &str,
    class: ProviderRouteClassV1,
    health_id: &str,
    rank: u16,
    auth_mode: AuthSelectionModeV1,
) -> ProviderRegistryCandidateV1 {
    ProviderRegistryCandidateV1::new(
        ProviderRouteBindingV1::new(
            label(id),
            label("provider-a"),
            label("model-a"),
            class,
            AuthCandidatePolicyReferenceV1::new(
                auth_mode,
                digest(&format!("auth-candidates-{id}")),
                digest("auth-policy"),
            ),
            RuntimeAuthorityClass::ScopedMutation,
        ),
        instance(health_id),
        Vec::new(),
        rank,
    )
    .expect("provider candidate")
}

fn candidate_registry(
    harnesses: Vec<HarnessRegistryCandidateV1>,
    providers: Vec<ProviderRegistryCandidateV1>,
) -> SealedRuntimeCandidateRegistryV1 {
    SealedRuntimeCandidateRegistryV1::seal(
        HostCandidateRegistryProof::test_only(3),
        harnesses,
        vec![context_candidate()],
        providers,
    )
    .expect("candidate registry")
}

fn request(
    external_state: RuntimeHealthState,
    primary_state: RuntimeHealthState,
    fallback_auth_mode: AuthSelectionModeV1,
) -> RuntimeSelectionRequest {
    let candidates = candidate_registry(
        vec![
            harness_candidate("harness-external", false, "health-harness-external", 0),
            harness_candidate("harness-embedded", true, "health-harness-embedded", 1),
        ],
        vec![
            provider_candidate(
                "route-primary",
                ProviderRouteClassV1::Primary,
                "health-provider-primary",
                0,
                AuthSelectionModeV1::HostPolicy,
            ),
            provider_candidate(
                "route-fallback",
                ProviderRouteClassV1::Fallback,
                "health-provider-fallback",
                1,
                fallback_auth_mode,
            ),
        ],
    );
    RuntimeSelectionRequest {
        admission_snapshot: AdmissionSnapshotReferenceV1::new(
            label("admission-snapshot"),
            digest("admission"),
            generation(),
            RuntimeAuthorityClass::ScopedMutation,
        )
        .expect("admission"),
        override_policy: SessionOverridePolicyV1::deny_all(RuntimeAuthorityClass::ScopedMutation)
            .expect("override policy"),
        capability_requirements: RuntimeCapabilityRequirementsV1::new(
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("requirements"),
        fallback_policy: RuntimeFallbackPolicyV1::new(
            FallbackPermissionV1::BeforeProgress,
            FallbackPermissionV1::BeforeProgress,
        )
        .expect("fallback policy"),
        candidates,
        health: ImmutableHealthSnapshotV1::capture(
            HostHealthSnapshotProof::test_only(5),
            30,
            vec![
                health("health-harness-external", external_state),
                health("health-harness-embedded", RuntimeHealthState::Healthy),
                health("health-context-main", RuntimeHealthState::Healthy),
                health("health-provider-primary", primary_state),
                health("health-provider-fallback", RuntimeHealthState::Healthy),
            ],
        )
        .expect("health snapshot"),
        tool_catalog: SealedToolCatalogSelectionV1::test_only(
            label("toolcat-test"),
            12,
            Vec::new(),
        ),
        middleware_chain: MiddlewareChainBindingV1::new(vec![label("safety")]).expect("middleware"),
        execution_profile: ExecutionProfileBindingV1::new(
            label("production"),
            RuntimeAuthorityClass::ScopedMutation,
        )
        .expect("execution"),
        epochs: epochs(),
    }
}

#[test]
fn bounded_vec_rejects_oversized_sequence_before_accepting_it() {
    let json = serde_json::to_string(&(0..33).collect::<Vec<_>>()).expect("json");
    assert!(serde_json::from_str::<BoundedVec<u8, 32>>(&json).is_err());
}

#[test]
fn health_wire_rejects_more_than_total_bound() {
    let records = (0..97)
        .map(|index| health(&format!("health-{index:03}"), RuntimeHealthState::Healthy))
        .collect::<Vec<_>>();
    let value = serde_json::json!({
        "observed_at_unix_ms": 30,
        "registry_epoch": 1,
        "records": records,
        "snapshot_digest": digest("irrelevant")
    });
    assert!(serde_json::from_value::<HealthSnapshotWireV1>(value).is_err());
}

#[test]
fn progress_fence_matches_m024_side_effect_predicate() {
    assert!(!RuntimeSelectionProgressV1::new(
        OutputProgressV1::NoOutput,
        Some(SideEffectFenceState::IntentRecorded)
    )
    .blocks_fallback());
    for state in [
        SideEffectFenceState::EffectStarted,
        SideEffectFenceState::EffectObserved,
        SideEffectFenceState::EffectUnknown,
        SideEffectFenceState::Reconciled,
        SideEffectFenceState::Abandoned,
    ] {
        assert!(RuntimeSelectionProgressV1::new(OutputProgressV1::NoOutput, Some(state))
            .blocks_fallback());
    }
    assert!(
        RuntimeSelectionProgressV1::new(OutputProgressV1::PartialOutput, None).blocks_fallback()
    );
}

#[test]
fn duplicate_candidate_ids_fail_even_when_input_order_is_reversed() {
    for reverse in [false, true] {
        let mut harnesses = vec![
            harness_candidate("same-harness", false, "health-a", 0),
            harness_candidate("same-harness", true, "health-b", 1),
        ];
        if reverse {
            harnesses.reverse();
        }
        assert!(SealedRuntimeCandidateRegistryV1::seal(
            HostCandidateRegistryProof::test_only(3),
            harnesses,
            vec![context_candidate()],
            vec![provider_candidate(
                "route-primary",
                ProviderRouteClassV1::Primary,
                "health-provider",
                0,
                AuthSelectionModeV1::HostPolicy
            )]
        )
        .is_err());
    }
}

#[test]
fn duplicate_health_ids_across_component_families_fail() {
    assert!(SealedRuntimeCandidateRegistryV1::seal(
        HostCandidateRegistryProof::test_only(3),
        vec![harness_candidate("harness", false, "shared-health", 0)],
        vec![{
            let descriptor = ContextEngineDescriptor {
                engine_id: "context".to_owned(),
                label: "Context".to_owned(),
                version: "1.0.0".to_owned(),
                lifecycle_hooks: Vec::new(),
            };
            ContextEngineRegistryCandidateV1::new(
                ContextEngineBindingV1::from_registry_descriptor(
                    &descriptor,
                    1,
                    RuntimeAuthorityClass::ScopedMutation,
                )
                .expect("binding"),
                instance("shared-health"),
                Vec::new(),
                0,
            )
            .expect("candidate")
        }],
        vec![provider_candidate(
            "route-primary",
            ProviderRouteClassV1::Primary,
            "health-provider",
            0,
            AuthSelectionModeV1::HostPolicy
        )]
    )
    .is_err());
}

#[test]
fn provider_topology_requires_exactly_one_rank_zero_primary() {
    let result = SealedRuntimeCandidateRegistryV1::seal(
        HostCandidateRegistryProof::test_only(3),
        vec![harness_candidate("harness", false, "health-harness", 0)],
        vec![context_candidate()],
        vec![
            provider_candidate(
                "route-a",
                ProviderRouteClassV1::Primary,
                "health-route-a",
                0,
                AuthSelectionModeV1::HostPolicy,
            ),
            provider_candidate(
                "route-b",
                ProviderRouteClassV1::Primary,
                "health-route-b",
                0,
                AuthSelectionModeV1::HostPolicy,
            ),
        ],
    );
    assert!(result.is_err());
}

#[test]
fn input_order_does_not_change_selection_digest() {
    let left = RuntimeSelectionService::select(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
    )
    .expect("left selection");
    let right = RuntimeSelectionService::select(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
    )
    .expect("right selection");
    assert_eq!(left.projection().selection_digest(), right.projection().selection_digest());
}

#[test]
fn managed_component_generations_are_independent_from_run_generation() {
    let mut selection_request = request(
        RuntimeHealthState::Healthy,
        RuntimeHealthState::Healthy,
        AuthSelectionModeV1::FixedProfile,
    );
    let component_ids = [
        "health-harness-external",
        "health-harness-embedded",
        "health-context-main",
        "health-provider-primary",
        "health-provider-fallback",
    ];
    let records = component_ids
        .into_iter()
        .enumerate()
        .map(|(index, component_id)| {
            let mut record = health(component_id, RuntimeHealthState::Healthy);
            record.generation =
                RuntimeGeneration::new(100 + u64::try_from(index).expect("bounded index"))
                    .expect("component generation");
            record
        })
        .collect();
    selection_request.health =
        ImmutableHealthSnapshotV1::capture(HostHealthSnapshotProof::test_only(5), 30, records)
            .expect("health snapshot");

    RuntimeSelectionService::select(proof(), &selection_request)
        .expect("independent component generations remain valid");
}

#[test]
fn managed_and_host_resident_readiness_authority_cannot_be_exchanged() {
    let managed_id = instance("health-provider-primary");
    let resident_id = instance("host-embedded-harness");
    let snapshot = ImmutableHealthSnapshotV1::capture_with_host_resident(
        HostHealthSnapshotProof::test_only(5),
        HostResidentReadinessProof::test_only(9),
        30,
        vec![health(managed_id.as_str(), RuntimeHealthState::Healthy)],
        vec![HostResidentReadinessV1::test_only(
            resident_id.clone(),
            9,
            digest("embedded-descriptor"),
            RuntimeAuthorityClass::ScopedMutation,
            true,
            label("host.resident.ready"),
            20,
        )],
    )
    .expect("combined health snapshot");

    assert!(snapshot.is_available(RuntimeHealthAuthoritySourceV1::Managed, &managed_id));
    assert!(!snapshot.is_available(RuntimeHealthAuthoritySourceV1::HostResident, &managed_id));
    assert!(snapshot.is_available(RuntimeHealthAuthoritySourceV1::HostResident, &resident_id));
    assert!(!snapshot.is_available(RuntimeHealthAuthoritySourceV1::Managed, &resident_id));
}

#[test]
fn external_recovery_changes_only_harness_and_records_transition() {
    let prior_request = request(
        RuntimeHealthState::Healthy,
        RuntimeHealthState::Healthy,
        AuthSelectionModeV1::FixedProfile,
    );
    let prior = RuntimeSelectionService::select(proof(), &prior_request).expect("selection");
    let next = RuntimeSelectionService::select_fallback(
        proof(),
        &request(
            RuntimeHealthState::Quarantined,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
        prior,
        RuntimeFallbackTriggerV1::HarnessUnavailable { reason_code: label("harness.quarantined") },
        RuntimeSelectionProgressV1::pristine(),
    )
    .expect("harness fallback");
    assert_eq!(
        next.projection().harness().reason,
        CandidateSelectedReasonV1::ExternalHarnessRecovery
    );
    assert_eq!(
        next.projection().context_engine().reason,
        CandidateSelectedReasonV1::PreferredAvailable
    );
    assert_eq!(
        next.projection().provider_route().reason,
        CandidateSelectedReasonV1::PreferredAvailable
    );
}

#[test]
fn provider_recovery_is_distinct_and_cannot_widen_auth_policy() {
    let prior = RuntimeSelectionService::select(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
    )
    .expect("selection");
    let prior_digest = prior.projection().selection_digest().clone();
    let failure = RuntimeSelectionService::select_fallback(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Quarantined,
            AuthSelectionModeV1::PerRequestDelegated,
        ),
        prior,
        RuntimeFallbackTriggerV1::ProviderRouteUnavailable {
            reason_code: label("provider.quarantined"),
        },
        RuntimeSelectionProgressV1::pristine(),
    )
    .expect_err("auth widening must fail");
    assert_eq!(failure.into_prior().projection().selection_digest(), &prior_digest);
}

#[test]
fn partial_output_failure_returns_exact_prior_authority() {
    let prior = RuntimeSelectionService::select(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
    )
    .expect("selection");
    let prior_digest = prior.projection().selection_digest().clone();
    let failure = RuntimeSelectionService::select_fallback(
        proof(),
        &request(
            RuntimeHealthState::Quarantined,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
        prior,
        RuntimeFallbackTriggerV1::HarnessUnavailable { reason_code: label("harness.failed") },
        RuntimeSelectionProgressV1::new(OutputProgressV1::PartialOutput, None),
    )
    .expect_err("progress fence");
    let recovered = failure.into_prior();
    assert_eq!(recovered.grant().selection_digest(), &prior_digest);
    assert_eq!(recovered.projection().selection_digest(), &prior_digest);
}

#[test]
fn projection_digest_tampering_is_named_digest_mismatch() {
    let resolved = RuntimeSelectionService::select(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
    )
    .expect("selection");
    let mut value = serde_json::to_value(resolved.projection()).expect("projection json");
    value["selection_digest"] = serde_json::Value::String(digest("tampered").as_str().to_owned());
    let error = serde_json::from_value::<RuntimeSelectionV1>(value)
        .expect_err("digest mismatch")
        .to_string();
    assert!(error.contains("canonical digest mismatch"), "{error}");
}

#[test]
fn provider_recovery_changes_only_route_with_same_or_narrower_auth() {
    let prior = RuntimeSelectionService::select(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
    )
    .expect("selection");
    let next = RuntimeSelectionService::select_fallback(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Quarantined,
            AuthSelectionModeV1::FixedProfile,
        ),
        prior,
        RuntimeFallbackTriggerV1::ProviderRouteUnavailable {
            reason_code: label("provider.route_unavailable"),
        },
        RuntimeSelectionProgressV1::pristine(),
    )
    .expect("provider fallback");
    assert_eq!(
        next.projection().provider_route().reason,
        CandidateSelectedReasonV1::ProviderRouteRecovery
    );
    assert_eq!(next.projection().harness().reason, CandidateSelectedReasonV1::PreferredAvailable);
    assert_eq!(
        next.projection().context_engine().reason,
        CandidateSelectedReasonV1::PreferredAvailable
    );
}

#[test]
fn dedicated_policy_and_epoch_digests_are_not_interchangeable() {
    let request = request(
        RuntimeHealthState::Healthy,
        RuntimeHealthState::Healthy,
        AuthSelectionModeV1::FixedProfile,
    );
    let digests = [
        request.fallback_policy.digest(),
        request.override_policy.digest(),
        request.capability_requirements.digest(),
        request.epochs.digest(),
    ];
    for left in 0..digests.len() {
        for right in (left + 1)..digests.len() {
            assert_ne!(digests[left], digests[right]);
        }
    }
}

#[test]
fn fallback_policy_drift_returns_the_unchanged_prior_grant() {
    let prior = RuntimeSelectionService::select(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
    )
    .expect("selection");
    let prior_digest = prior.grant().selection_digest().clone();
    let mut changed = request(
        RuntimeHealthState::Quarantined,
        RuntimeHealthState::Healthy,
        AuthSelectionModeV1::FixedProfile,
    );
    changed.fallback_policy = RuntimeFallbackPolicyV1::new(
        FallbackPermissionV1::Forbidden,
        FallbackPermissionV1::Forbidden,
    )
    .expect("changed fallback policy");
    let failure = RuntimeSelectionService::select_fallback(
        proof(),
        &changed,
        prior,
        RuntimeFallbackTriggerV1::HarnessUnavailable { reason_code: label("harness.failed") },
        RuntimeSelectionProgressV1::pristine(),
    )
    .expect_err("policy drift");
    assert_eq!(failure.into_prior().grant().selection_digest(), &prior_digest);
}

#[test]
fn candidate_report_vector_is_streaming_bounded_on_projection_load() {
    let resolved = RuntimeSelectionService::select(
        proof(),
        &request(
            RuntimeHealthState::Healthy,
            RuntimeHealthState::Healthy,
            AuthSelectionModeV1::FixedProfile,
        ),
    )
    .expect("selection");
    let mut value = serde_json::to_value(resolved.projection()).expect("projection json");
    let report = value["candidate_reports"][0].clone();
    value["candidate_reports"] =
        serde_json::Value::Array((0..97).map(|_| report.clone()).collect());
    assert!(serde_json::from_value::<RuntimeSelectionV1>(value).is_err());
}
