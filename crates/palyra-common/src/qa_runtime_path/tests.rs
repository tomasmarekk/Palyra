//! Regression coverage for QA runtime-path evidence and provider-binding contracts.
//! Kept separate so the production contract module stays within its source budget.

use super::*;

fn component(id: &str, event: &str) -> RuntimePathComponentEvidence {
    RuntimePathComponentEvidence {
        id: id.to_owned(),
        source_event: event.to_owned(),
        reason_code: "runtime_path.selected".to_owned(),
    }
}

fn evidence() -> RuntimePathEvidence {
    RuntimePathEvidence {
        schema_version: QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION,
        runtime_version:
            "palyrad-sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
        runtime_contract_version: "runtime-contracts.v8".to_owned(),
        runner_version: "qa-runner.v4/0.1.0-git-test".to_owned(),
        provider_lane: "fixture".to_owned(),
        attempt_owner: "embedded_run_stream".to_owned(),
        harness: component("embedded_run_stream", "run.runtime_path_summary"),
        context_engine: component("legacy_provider_input", "run.runtime_path_summary"),
        mcp_transport_mode: None,
        complete: true,
        source_events: vec!["run.runtime_path_summary".to_owned()],
        reason_codes: vec!["runtime_path.complete".to_owned()],
        fallbacks: Vec::new(),
        fallback_count: 0,
    }
}

fn expectation() -> NoHiddenFallbackExpectation {
    NoHiddenFallbackExpectation {
        runtime_contract_version: "runtime-contracts.v8".to_owned(),
        provider_lane: "fixture".to_owned(),
        attempt_owner: "embedded_run_stream".to_owned(),
        harness_id: "embedded_run_stream".to_owned(),
        context_engine_id: "legacy_provider_input".to_owned(),
        mcp_transport_mode: None,
        max_fallback_count: 0,
        allowed_fallback_reason_codes: Vec::new(),
    }
}

#[test]
fn mcp_transport_invocation_event_round_trips_with_exact_identity() {
    let event = McpTransportInvocationEvent {
        schema_version: MCP_TRANSPORT_INVOCATION_EVENT_SCHEMA_VERSION,
        event_name: MCP_TRANSPORT_INVOCATION_EVENT.to_owned(),
        attestation_id: "mcpatt_0123456789abcdef".to_owned(),
        transport_id: "mcp.transport.0123456789abcdef".to_owned(),
        namespaced_tool_id: "mcp.docs:workspace.search:code".to_owned(),
        transport_mode: McpTransportInvocationMode::PerCall,
    };

    event.validate_shape().expect("canonical invocation evidence should validate");
    let encoded = serde_json::to_vec(&event).expect("invocation evidence should encode");
    let decoded: McpTransportInvocationEvent =
        serde_json::from_slice(encoded.as_slice()).expect("invocation evidence should decode");

    assert_eq!(decoded, event);
    assert_eq!(decoded.transport_mode.as_str(), "per_call");
}

#[test]
fn context_engine_binding_requires_nonzero_bounded_identity() {
    let event = ContextEngineBindingEvent {
        schema_version: CONTEXT_ENGINE_BINDING_EVENT_SCHEMA_VERSION,
        event_name: CONTEXT_ENGINE_BINDING_EVENT.to_owned(),
        engine_id: crate::runtime_contracts::RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID
            .to_owned(),
        engine_version:
            crate::runtime_contracts::RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_VERSION
                .to_owned(),
        projection_epoch: 7,
    };

    event.validate_shape().expect("canonical context binding should validate");
    let mut invalid = event;
    invalid.projection_epoch = 0;
    let error = invalid.validate_shape().expect_err("zero projection epoch must fail closed");
    assert_eq!(error.code, "context_engine_binding_projection_epoch_invalid");
}

#[test]
fn provider_lane_attestation_binds_exact_fixture_bytes() {
    let materialized_input_sha256 = "a".repeat(64);
    let provider_binding_sha256 = qa_provider_binding_sha256(
        "record_replay",
        QA_PROVIDER_RECORD_REPLAY_MATERIALIZATION,
        materialized_input_sha256.as_str(),
    )
    .expect("record/replay fixture binding should hash");
    let event = ProviderLaneAttestationEvent {
        schema_version: PROVIDER_LANE_ATTESTATION_EVENT_SCHEMA_VERSION,
        event_name: PROVIDER_LANE_ATTESTATION_EVENT.to_owned(),
        execution_key_digest: "b".repeat(64),
        provider_binding_sha256,
        provider_lane: "record_replay".to_owned(),
        materialization_kind: QA_PROVIDER_RECORD_REPLAY_MATERIALIZATION.to_owned(),
        materialized_input_sha256: Some(materialized_input_sha256),
        live_binding: None,
        provider_id: "deterministic-primary".to_owned(),
        model_id: "deterministic".to_owned(),
    };

    event.validate_shape().expect("canonical provider attestation should validate");
    let encoded = serde_json::to_vec(&event).expect("provider attestation should encode");
    let decoded: ProviderLaneAttestationEvent =
        serde_json::from_slice(encoded.as_slice()).expect("provider attestation should decode");

    assert_eq!(decoded, event);
}

#[test]
fn provider_lane_attestation_rejects_echoed_wrong_binding() {
    let event = ProviderLaneAttestationEvent {
        schema_version: PROVIDER_LANE_ATTESTATION_EVENT_SCHEMA_VERSION,
        event_name: PROVIDER_LANE_ATTESTATION_EVENT.to_owned(),
        execution_key_digest: "b".repeat(64),
        provider_binding_sha256: "c".repeat(64),
        provider_lane: "fixture".to_owned(),
        materialization_kind: QA_PROVIDER_FIXTURE_MATERIALIZATION.to_owned(),
        materialized_input_sha256: Some("a".repeat(64)),
        live_binding: None,
        provider_id: "deterministic-primary".to_owned(),
        model_id: "deterministic".to_owned(),
    };

    let error = event.validate_shape().expect_err("wrong fixture binding must fail closed");
    assert_eq!(error.code, "provider_lane_attestation_binding_mismatch");
}

#[test]
fn live_provider_attestation_rejects_materialized_model_drift() {
    let live_binding = ProviderLiveBindingMetadata {
        provider_kind: "openai_compatible".to_owned(),
        auth_profile_id: "qa-live-selected".to_owned(),
        auth_provider_kind: "openai".to_owned(),
        base_url_sha256: qa_live_provider_base_url_sha256("https://api.openai.com/v1/")
            .expect("base URL should hash"),
        raw_payload_storage: false,
    };
    let provider_binding_sha256 = qa_live_provider_binding_sha256("openai", "gpt-5", &live_binding)
        .expect("parent binding should hash");
    let event = ProviderLaneAttestationEvent {
        schema_version: PROVIDER_LANE_ATTESTATION_EVENT_SCHEMA_VERSION,
        event_name: PROVIDER_LANE_ATTESTATION_EVENT.to_owned(),
        execution_key_digest: "b".repeat(64),
        provider_binding_sha256,
        provider_lane: "live".to_owned(),
        materialization_kind: QA_PROVIDER_LIVE_MATERIALIZATION.to_owned(),
        materialized_input_sha256: None,
        live_binding: Some(live_binding),
        provider_id: "openai".to_owned(),
        model_id: "gpt-5-mini".to_owned(),
    };

    let error = event
        .validate_shape()
        .expect_err("actual model drift must not validate against the parent binding");

    assert_eq!(error.code, "provider_lane_attestation_binding_mismatch");
}

#[test]
fn provider_route_change_requires_distinct_bounded_identities() {
    let event = ProviderRouteChangeEvent {
        schema_version: PROVIDER_ROUTE_CHANGE_EVENT_SCHEMA_VERSION,
        event_name: PROVIDER_ROUTE_CHANGE_EVENT.to_owned(),
        transition_index: 0,
        from_provider_id: "provider-a".to_owned(),
        from_model_id: "model-a".to_owned(),
        to_provider_id: "provider-b".to_owned(),
        to_model_id: "model-b".to_owned(),
        reason_code: "runtime_path.provider.route_changed".to_owned(),
    };

    event.validate_shape().expect("canonical route change should validate");
    let mut unchanged = event;
    unchanged.to_provider_id = unchanged.from_provider_id.clone();
    unchanged.to_model_id = unchanged.from_model_id.clone();
    let error = unchanged.validate_shape().expect_err("unchanged route identity must fail closed");
    assert_eq!(error.code, "provider_route_change_identity_unchanged");
}

#[test]
fn exact_runtime_path_evidence_passes_without_hidden_fallbacks() {
    let evidence = evidence();
    evidence.validate_shape().expect("baseline evidence should validate");

    let mismatches = evaluate_no_hidden_fallback(&expectation(), &evidence)
        .expect("valid contracts should evaluate");

    assert!(mismatches.is_empty());
}

#[test]
fn unobserved_provider_lane_is_valid_only_for_partial_evidence() {
    let mut evidence = evidence();
    evidence.provider_lane = "unobserved".to_owned();
    evidence.complete = false;
    evidence.reason_codes = vec!["qa.runner.runtime_path_provider_attestation_missing".to_owned()];
    evidence.validate_shape().expect("partial evidence must remain persistable");

    evidence.complete = true;
    let error = evidence
        .validate_shape()
        .expect_err("complete evidence cannot claim an unobserved provider lane");
    assert_eq!(error.code, "runtime_path_provider_lane_unobserved_complete");
}

#[test]
fn embedded_legacy_and_per_call_paths_fail_stricter_expectations() {
    let mut evidence = evidence();
    evidence.mcp_transport_mode = Some(RuntimePathComponentEvidence {
        id: "per_call".to_owned(),
        source_event: MCP_TRANSPORT_INVOCATION_EVENT.to_owned(),
        reason_code: "runtime_path.mcp.per_call_attested".to_owned(),
    });
    evidence.source_events.push(MCP_TRANSPORT_INVOCATION_EVENT.to_owned());
    evidence.source_events.push("mcp.transport.selected".to_owned());
    let mut expectation = expectation();
    expectation.attempt_owner = "external_harness".to_owned();
    expectation.harness_id = "external_harness".to_owned();
    expectation.context_engine_id = "default_context_engine".to_owned();
    expectation.mcp_transport_mode = Some(McpTransportInvocationMode::Persistent);

    let mismatches = evaluate_no_hidden_fallback(&expectation, &evidence)
        .expect("valid contracts should evaluate");
    let codes = mismatches.iter().map(|mismatch| mismatch.code.as_str()).collect::<Vec<_>>();

    assert!(codes.contains(&"runtime_path_attempt_owner_mismatch"));
    assert!(codes.contains(&"runtime_path_harness_mismatch"));
    assert!(codes.contains(&"runtime_path_context_engine_mismatch"));
    assert!(codes.contains(&"runtime_path_mcp_transport_mismatch"));
}

#[test]
fn allowed_fallback_remains_in_evidence_without_failing_the_gate() {
    let mut evidence = evidence();
    evidence.fallbacks.push(RuntimeFallbackEvidence {
        component: "provider".to_owned(),
        from: Some("primary".to_owned()),
        to: "secondary".to_owned(),
        reason_code: "provider.recovery.failover_provider".to_owned(),
        source_event: "provider.recovery.decision".to_owned(),
    });
    evidence.source_events.push("provider.recovery.decision".to_owned());
    evidence.fallback_count = 1;
    let mut expectation = expectation();
    expectation.max_fallback_count = 1;
    expectation.allowed_fallback_reason_codes =
        vec!["provider.recovery.failover_provider".to_owned()];

    let mismatches = evaluate_no_hidden_fallback(&expectation, &evidence)
        .expect("valid contracts should evaluate");

    assert!(mismatches.is_empty());
    assert_eq!(evidence.fallbacks.len(), 1);
}

#[test]
fn evidence_rejects_count_that_could_hide_fallbacks() {
    let mut evidence = evidence();
    evidence.fallbacks.push(RuntimeFallbackEvidence {
        component: "harness".to_owned(),
        from: Some("external".to_owned()),
        to: "embedded".to_owned(),
        reason_code: "harness.fallback".to_owned(),
        source_event: "harness.selection".to_owned(),
    });

    let count_error = evidence
        .validate_shape()
        .expect_err("a retained fallback cannot be hidden by fallback_count");
    assert_eq!(count_error.code(), "runtime_path_fallback_count_mismatch");
}

#[test]
fn contracts_reject_free_form_or_unbounded_metadata() {
    for leaked_path in [
        "https://user:secret@example.invalid/?token=secret",
        "C:/Users/Palo/private",
        "/home/qa-user/state",
        "\\\\server\\share\\private",
    ] {
        let mut evidence = evidence();
        evidence.attempt_owner = leaked_path.to_owned();

        let error =
            evidence.validate_shape().expect_err("free-form or path metadata must be rejected");

        assert_eq!(error.code(), "runtime_path_metadata_invalid");
        assert_eq!(error.path(), "$.attempt_owner");
    }

    for leaked_version in ["C:/Users/Palo/palyrad.exe", "/home/qa-user/palyrad"] {
        let mut evidence = evidence();
        evidence.runtime_version = leaked_version.to_owned();

        let error = evidence.validate_shape().expect_err("absolute version paths must be rejected");

        assert_eq!(error.code(), "runtime_path_metadata_invalid");
        assert_eq!(error.path(), "$.runtime_version");
    }
}

#[test]
fn complete_evidence_requires_bound_sources_and_reason_codes() {
    let mut missing_sources = evidence();
    missing_sources.source_events.clear();
    let source_error =
        missing_sources.validate_shape().expect_err("complete evidence must retain source events");
    assert_eq!(source_error.code(), "runtime_path_source_events_required");

    let mut missing_reasons = evidence();
    missing_reasons.reason_codes.clear();
    let reason_error =
        missing_reasons.validate_shape().expect_err("complete evidence must retain reason codes");
    assert_eq!(reason_error.code(), "runtime_path_reason_codes_required");

    let mut unbound_component = evidence();
    unbound_component.harness.source_event = "harness.selection".to_owned();
    let binding_error = unbound_component
        .validate_shape()
        .expect_err("component sources must bind to retained events");
    assert_eq!(binding_error.code(), "runtime_path_source_event_unbound");
    assert_eq!(binding_error.path(), "$.harness.source_event");
}

#[test]
fn durable_contract_round_trips_without_unknown_fields() {
    let evidence = evidence();
    let encoded = serde_json::to_vec(&evidence).expect("evidence should encode");
    let decoded: RuntimePathEvidence =
        serde_json::from_slice(encoded.as_slice()).expect("evidence should decode");

    assert_eq!(decoded, evidence);
}
