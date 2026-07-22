use palyra_common::runtime_contracts::{RuntimeErrorPhase, RuntimeGeneration};
use serde_json::{json, Value};

use super::build_console_runtime_diagnostics_payload;
use crate::{
    application::runtime_kernel_v2::{
        selection::RuntimeAuthorityDecisionV1,
        shadow::{
            ShadowAuthorityToken, ShadowCandidatePlanInputsV1, ShadowCandidatePlannerV1,
            ShadowContextSegmentSemanticV1, ShadowDifferentialObserver, ShadowForbiddenService,
            ShadowPlanSemanticInputsV1, ShadowPlanSnapshotV1, ShadowPolicySemanticV1,
            ShadowSamplingPolicyV1, ShadowSelectionSemanticV1, ShadowToolCatalogSemanticV1,
        },
    },
    gateway::runtime::tests::test_runtime_state,
    runtime_diagnostics::shadow_differential::build_shadow_differential_diagnostics,
};

fn generation() -> RuntimeGeneration {
    RuntimeGeneration::new(23).expect("test generation is non-zero")
}

fn plan(token_budget: u32) -> ShadowPlanSnapshotV1 {
    semantics(token_budget)
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

fn semantics(token_budget: u32) -> ShadowPlanSemanticInputsV1 {
    ShadowPlanSemanticInputsV1::new(
        ShadowSelectionSemanticV1::new(
            "provider-a".to_owned(),
            "model-a".to_owned(),
            "credential-a".to_owned(),
            "healthy".to_owned(),
        )
        .expect("test selection should validate"),
        vec![ShadowContextSegmentSemanticV1::new(
            "current_turn".to_owned(),
            "b".repeat(64),
            42,
            "trusted".to_owned(),
            "volatile".to_owned(),
            None,
        )
        .expect("test segment should validate")],
        Some("c".repeat(64)),
        None,
        u64::from(token_budget),
        ShadowToolCatalogSemanticV1::new("d".repeat(64), "direct".to_owned(), 0)
            .expect("test catalog should validate"),
        ShadowPolicySemanticV1::new(false, false, 4, None, Some(1_024))
            .expect("test policy should validate"),
    )
    .expect("test semantic inputs should validate")
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
fn console_runtime_diagnostics_projects_identity_free_shadow_counters() {
    let runtime = test_runtime_state();
    let observer = ShadowDifferentialObserver::new(
        ShadowSamplingPolicyV1::new(0, [7; 32]).expect("zero rate is valid"),
    );
    let observation = observer
        .observe(
            b"private-session-identity",
            true,
            &authority_decision(),
            &plan(8_192),
            ShadowCandidatePlannerV1::new(ShadowCandidatePlanInputsV1::new(
                generation(),
                semantics(8_448),
            )),
        )
        .expect("explicit observation should succeed");
    runtime.record_runtime_shadow_observation(&observation);

    let authority = ShadowAuthorityToken::new(generation());
    runtime.record_runtime_shadow_authority_denial(
        authority
            .request_side_effect(ShadowForbiddenService::Tool)
            .expect_err("shadow tool authority must be denied"),
    );
    let shadow_payload =
        build_shadow_differential_diagnostics(runtime.runtime_shadow_diagnostics_snapshot());
    let endpoint_payload = build_console_runtime_diagnostics_payload(
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        shadow_payload,
    );

    assert_eq!(
        endpoint_payload
            .pointer("/runtime_kernel_v2_shadow/enrollment/selected_observations_total"),
        Some(&json!(1))
    );
    assert_eq!(
        endpoint_payload.pointer("/runtime_kernel_v2_shadow/classifications/benign_total"),
        Some(&json!(1))
    );
    assert_eq!(
        endpoint_payload.pointer("/runtime_kernel_v2_shadow/authority_denials/tool_total"),
        Some(&json!(1))
    );
    assert_eq!(
        endpoint_payload.pointer("/runtime_kernel_v2_shadow/promotion_blocked"),
        Some(&json!(true))
    );
    let encoded = endpoint_payload.to_string();
    assert!(!encoded.contains("private-session-identity"));
    assert!(!encoded.contains("admin:alice"));
    assert!(!encoded.contains(&"a".repeat(64)));
}
