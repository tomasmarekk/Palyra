//! Security and contract tests for the metadata-trace projection allowlist.
//!
//! Hostile fixtures deliberately place secrets under plausible benign keys so
//! additions to the source tape cannot silently widen the persisted trace.

use std::collections::BTreeSet;

use palyra_common::metadata_trace::{
    MetadataTraceCapacityLimitV1, MetadataTraceEventDataV1, MetadataTraceProviderAttemptOutcomeV1,
    MetadataTraceTerminalOutcomeV1, MetadataTraceToolGateDecisionV1, MetadataTraceToolOutcomeV1,
    METADATA_TRACE_MAX_EVENTS, METADATA_TRACE_MAX_STAGE_DURATION_MS,
};
use serde_json::{json, Value};

use crate::journal::OrchestratorTapeRecord;

use super::{
    hash_metadata_trace_approval_id, hash_metadata_trace_delivery_id,
    hash_metadata_trace_identifier, hash_metadata_trace_model_id, hash_metadata_trace_profile_id,
    hash_metadata_trace_provider_id, hash_metadata_trace_run_id, hash_metadata_trace_tool_id,
    metadata_trace_capacity_reached_event, project_orchestrator_tape_record,
    projected_event_id_sha256, MetadataTraceIdentifierDomain, MetadataTraceProjectionContext,
};

const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const SECRET_SENTINEL: &str = "DUMMY_SECRET_SHOULD_NOT_APPEAR";

#[test]
fn identifier_hashes_are_stable_and_domain_separated() {
    let raw = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let hashes = [
        hash_metadata_trace_run_id(raw),
        hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Session, raw),
        hash_metadata_trace_tool_id(raw),
        hash_metadata_trace_approval_id(raw),
        hash_metadata_trace_delivery_id(raw),
        hash_metadata_trace_profile_id(raw),
        hash_metadata_trace_provider_id(raw),
        hash_metadata_trace_model_id(raw),
        hash_metadata_trace_identifier(MetadataTraceIdentifierDomain::Custom, raw),
    ]
    .into_iter()
    .map(|hash| hash.expect("bounded identifier should hash"))
    .collect::<BTreeSet<_>>();

    assert_eq!(hashes.len(), 9, "the same raw ID must not correlate across domains");
    assert!(hashes.iter().all(|hash| hash.len() == 64));
    assert_eq!(hash_metadata_trace_run_id(raw), hash_metadata_trace_run_id(raw));
}

#[test]
fn identifier_hashes_reject_empty_or_oversized_values() {
    assert!(hash_metadata_trace_run_id("").is_none());
    assert!(hash_metadata_trace_run_id("x".repeat(4_097).as_str()).is_none());
}

#[test]
fn event_identity_changes_across_generation_source_sequence_and_event_type() {
    let record = OrchestratorTapeRecord {
        seq: 7,
        event_type: "context.engine.plan".to_owned(),
        payload_json: "{}".to_owned(),
    };
    let context = MetadataTraceProjectionContext {
        run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        sequence: 4,
        generation: 1,
        recorded_at_unix_ms: 1_000,
        causal_parent_event_id_sha256: None,
    };
    let baseline = projected_event_id_sha256(context, &record)
        .expect("non-negative source sequence should produce an event hash");

    assert_ne!(
        baseline,
        projected_event_id_sha256(
            MetadataTraceProjectionContext { generation: 2, ..context },
            &record,
        )
        .expect("alternate generation should hash")
    );
    assert_eq!(
        baseline,
        projected_event_id_sha256(
            MetadataTraceProjectionContext { sequence: 5, ..context },
            &record,
        )
        .expect("trace-local sequence must not change source identity")
    );
    let alternate_sequence_record = OrchestratorTapeRecord { seq: 8, ..record.clone() };
    assert_ne!(
        baseline,
        projected_event_id_sha256(context, &alternate_sequence_record)
            .expect("alternate source sequence should hash")
    );
    let alternate_record =
        OrchestratorTapeRecord { event_type: "tool.before_decision".to_owned(), ..record };
    assert_ne!(
        baseline,
        projected_event_id_sha256(context, &alternate_record)
            .expect("alternate source event should hash")
    );
}

#[test]
fn negative_tape_sequence_cannot_form_an_event_identity() {
    let record = OrchestratorTapeRecord {
        seq: -1,
        event_type: "status".to_owned(),
        payload_json: "{}".to_owned(),
    };
    let context = MetadataTraceProjectionContext {
        run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        sequence: 0,
        generation: 1,
        recorded_at_unix_ms: 1_000,
        causal_parent_event_id_sha256: None,
    };

    assert!(projected_event_id_sha256(context, &record).is_none());
}

#[test]
fn preferred_runtime_context_and_provider_events_copy_only_allowlisted_metadata() {
    let auth_profile_hash =
        hash_metadata_trace_profile_id("profile-primary").expect("profile identifier should hash");
    let cases = [
        (
            "harness.selection",
            json!({
                "harness_id": "run_stream",
                "harness_version": "1.0.0",
                "runtime_id": "gateway_runtime",
                "runtime_version": "2026.7.12",
                "route_class": "primary",
                "auth_profile_id_sha256": auth_profile_hash,
                "schema_hashes": [{
                    "schema_id": "agent_harness_descriptor",
                    "sha256": "a".repeat(64),
                }],
                "stage_duration_ms": 7,
                "prompt": SECRET_SENTINEL,
                "benign_label": {
                    "url": format!("https://example.test/?token={SECRET_SENTINEL}"),
                    "path": format!("C:\\\\private\\\\{SECRET_SENTINEL}"),
                },
            }),
        ),
        (
            "metadata.runtime_selected",
            json!({
                "harness_id": "embedded_run_stream",
                "harness_version": "1.0.0",
                "runtime_id": "run_stream_host_owned",
                "runtime_version": "1.0.0",
                "route_class": "primary",
                "schema_hashes": [{
                    "schema_id": "metadata_trace.runtime_selected.v1",
                    "sha256": "d".repeat(64),
                }],
            }),
        ),
        (
            "context.assembled",
            json!({
                "context_engine_id": "context_engine",
                "context_engine_version": "1.0.0",
                "context_schema_sha256": "b".repeat(64),
                "input_item_count": 9,
                "retained_item_count": 6,
                "stage_duration_ms": 11,
                "messages": [SECRET_SENTINEL],
                "provider_text": SECRET_SENTINEL,
            }),
        ),
        (
            "provider.attempt.completed",
            json!({
                "provider_id": "openai_compatible",
                "model_id": "gpt_5",
                "route_class": "primary",
                "auth_profile_id_sha256": hash_metadata_trace_profile_id("profile-primary")
                    .expect("profile identifier should hash"),
                "attempt": 1,
                "outcome": "succeeded",
                "reason_code": "provider.attempt.succeeded",
                "stage_duration_ms": 23,
                "response_body": SECRET_SENTINEL,
                "stdout": SECRET_SENTINEL,
                "stderr": SECRET_SENTINEL,
            }),
        ),
    ];

    for (event_type, payload) in cases {
        let event = project(event_type, payload).expect("preferred metadata event should project");
        assert_projection_excludes_hostile_content(&event);
    }
}

#[test]
fn shadow_differential_projection_is_closed_and_rejects_forged_posture() {
    let payload = json!({
        "schema_version": 1,
        "event_name": "runtime.shadow.differential",
        "redaction_level": "metadata_only",
        "authoritative_runtime": "legacy",
        "shadow_side_effect_free": true,
        "enrollment": "deterministic_sample",
        "enrollment_reason_code": "runtime.shadow.enrollment.deterministic_sample",
        "classification": "benign",
        "reason_code": "runtime.shadow.differential_benign",
        "runtime_selection": "match",
        "context_segments": "match",
        "context_safety": "match",
        "token_budget": "benign_difference",
        "tool_catalog": "match",
        "policy_input": "match",
        "phase_plan": "match",
        "promotion_blocked": false,
        "sampling_identity": SECRET_SENTINEL,
        "raw_prompt_diff": SECRET_SENTINEL,
    });
    let event = project("runtime.shadow.differential", payload.clone())
        .expect("valid shadow metadata should project");
    let MetadataTraceEventDataV1::RuntimeShadowDifferential(metadata) = &event.event else {
        panic!("shadow tape event must project to the typed differential variant");
    };
    assert_eq!(
        metadata.classification,
        palyra_common::metadata_trace::MetadataTraceShadowClassificationV1::Benign
    );
    assert!(!metadata.promotion_blocked);
    assert_projection_excludes_hostile_content(&event);

    let mut forged = payload;
    forged["promotion_blocked"] = json!(true);
    assert!(project("runtime.shadow.differential", forged).is_none());
}

#[test]
fn preferred_provider_event_hashes_route_ids_and_requires_prehashed_auth_profile() {
    let payload = json!({
        "provider_id": "openai_compatible",
        "model_id": "gpt_5",
        "route_class": "live",
        "auth_profile_id_sha256": hash_metadata_trace_profile_id("profile-primary")
            .expect("profile identifier should hash"),
        "attempt": 2,
        "outcome": "retryable_failure",
        "reason_code": "provider.timeout",
        "stage_duration_ms": 100,
    });
    let event = project("provider.attempt.completed", payload)
        .expect("strict provider attempt should project");
    let MetadataTraceEventDataV1::ProviderAttempt(metadata) = event.event else {
        panic!("provider event should retain its typed variant");
    };
    assert_eq!(
        metadata.provider_id_sha256,
        hash_metadata_trace_provider_id("openai_compatible")
            .expect("provider identifier should hash")
    );
    assert_eq!(
        metadata.model_id_sha256,
        hash_metadata_trace_model_id("gpt_5").expect("model identifier should hash")
    );
    assert_eq!(metadata.attempt, 2);
    assert_eq!(metadata.outcome, MetadataTraceProviderAttemptOutcomeV1::RetryableFailure);

    let raw_auth = json!({
        "provider_id": "openai_compatible",
        "model_id": "gpt_5",
        "route_class": "primary",
        "auth_profile_id": "profile-primary",
        "auth_profile_id_sha256": hash_metadata_trace_profile_id("profile-primary")
            .expect("profile identifier should hash"),
        "attempt": 1,
        "outcome": "succeeded",
        "reason_code": "provider.attempt.succeeded",
        "stage_duration_ms": 1,
    });
    assert!(project("provider.attempt.completed", raw_auth).is_none());
}

#[test]
fn legacy_provider_evidence_hashes_identities_without_copying_binding_details() {
    let lane = project(
        "provider.lane.attested",
        json!({
            "provider_lane": "live",
            "provider_id": "openai_compatible",
            "model_id": "gpt_5",
            "live_binding": {
                "auth_profile_id": "profile-primary",
                "auth_provider_kind": "api_key",
                "base_url_sha256": "e".repeat(64),
                "credential": {
                    "safe_name": SECRET_SENTINEL,
                    "header": format!("Bearer {SECRET_SENTINEL}"),
                },
            },
            "provider_text": SECRET_SENTINEL,
            "endpoint": format!("https://example.test/{SECRET_SENTINEL}"),
        }),
    )
    .expect("live lane evidence should project");
    let MetadataTraceEventDataV1::ProviderAttempt(metadata) = &lane.event else {
        panic!("provider lane should become a provider-attempt event");
    };
    let expected_profile_hash =
        hash_metadata_trace_profile_id("profile-primary").expect("profile identifier should hash");
    assert_eq!(metadata.auth_profile_id_sha256.as_deref(), Some(expected_profile_hash.as_str()));
    let serialized = serde_json::to_string(&lane).expect("lane event should serialize");
    assert!(!serialized.contains("profile-primary"));
    assert_projection_excludes_hostile_content(&lane);

    let route_change = project(
        "provider.route.changed",
        json!({
            "transition_index": 0,
            "from_provider_id": "primary_provider",
            "from_model_id": "primary_model",
            "to_provider_id": "fallback_provider",
            "to_model_id": "fallback_model",
            "reason_code": "runtime_path.provider.route_changed",
            "diagnostic": SECRET_SENTINEL,
        }),
    )
    .expect("route transition should project");
    let MetadataTraceEventDataV1::ProviderAttempt(metadata) = route_change.event else {
        panic!("route transition should become a provider-attempt event");
    };
    assert_eq!(metadata.attempt, 2);
    assert_eq!(metadata.outcome, MetadataTraceProviderAttemptOutcomeV1::Started);
}

#[test]
fn tool_gate_approval_and_outcome_never_copy_arguments_or_results() {
    let before_decision = project(
        "tool.before_decision",
        json!({
            "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FB0",
            "report": {
                "final_decision": "require_approval",
                "final_reason_code": "tool.policy.approval_required",
                "signature": {
                    "tool_name": "palyra.process.run",
                    "normalized_args_hash": "c".repeat(64),
                    "derived_path_scope": format!("C:\\\\private\\\\{SECRET_SENTINEL}"),
                    "network_targets": [format!("https://example.test/{SECRET_SENTINEL}")],
                },
                "steps": [{"summary": SECRET_SENTINEL}],
            },
            "arguments": {"command": SECRET_SENTINEL},
        }),
    )
    .expect("before-tool decision should project");
    let MetadataTraceEventDataV1::ToolGate(gate) = &before_decision.event else {
        panic!("tool decision should become a gate event");
    };
    assert_eq!(gate.decision, MetadataTraceToolGateDecisionV1::ApprovalRequired);
    assert_projection_excludes_hostile_content(&before_decision);

    let approval = project(
        "tool_approval_request",
        json!({
            "approval_id": "01ARZ3NDEKTSV4RRFFQ69G5FB1",
            "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FB0",
            "input_json": {"token": SECRET_SENTINEL},
            "request_summary": SECRET_SENTINEL,
            "prompt": {"details_json": {"password": SECRET_SENTINEL}},
        }),
    )
    .expect("approval request should project");
    assert_projection_excludes_hostile_content(&approval);

    let outcome = project(
        "tool_result",
        json!({
            "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FB0",
            "success": false,
            "output_json": {
                "stdout": SECRET_SENTINEL,
                "path": format!("/private/{SECRET_SENTINEL}"),
            },
            "error": format!("stderr={SECRET_SENTINEL}"),
            "diagnostic": {
                "error_kind": "timeout",
                "message": SECRET_SENTINEL,
            },
        }),
    )
    .expect("tool result should project");
    let MetadataTraceEventDataV1::ToolOutcome(tool_outcome) = &outcome.event else {
        panic!("tool result should become an outcome event");
    };
    assert_eq!(tool_outcome.outcome, MetadataTraceToolOutcomeV1::Failed);
    assert_eq!(tool_outcome.reason_code, "tool.result.timeout");
    assert_projection_excludes_hostile_content(&outcome);
}

#[test]
fn recovery_delivery_and_terminal_events_ignore_free_form_diagnostics() {
    let recovery = project(
        "provider.recovery.decision",
        json!({
            "decision": "failover_provider",
            "reason_code": "provider.recovery.failover_provider",
            "message": format!("Bearer {SECRET_SENTINEL} https://example.test/private"),
            "prompt_mutation": SECRET_SENTINEL,
        }),
    )
    .expect("recovery decision should project");
    assert_projection_excludes_hostile_content(&recovery);

    let delivery = project(
        "message.replied",
        json!({
            "reply_text": SECRET_SENTINEL,
            "harmless": format!("https://example.test/{SECRET_SENTINEL}"),
        }),
    )
    .expect("reply persistence should become delivery intent");
    assert_projection_excludes_hostile_content(&delivery);

    let terminal = project(
        "status",
        json!({
            "kind": "cancelled",
            "reason_code": "cancelled_by_request",
            "message": format!("cancel requested with token={SECRET_SENTINEL}"),
        }),
    )
    .expect("terminal status should project");
    let MetadataTraceEventDataV1::Terminalization(metadata) = &terminal.event else {
        panic!("terminal status should remain typed");
    };
    assert_eq!(metadata.outcome, MetadataTraceTerminalOutcomeV1::Cancelled);
    assert!(!metadata.output_emitted);
    assert!(metadata.side_effect_may_have_occurred);
    assert_projection_excludes_hostile_content(&terminal);
}

#[test]
fn unsupported_malformed_oversized_and_out_of_range_rows_fail_closed() {
    assert!(project("model_token", json!({"token": SECRET_SENTINEL})).is_none());
    assert!(project_record(OrchestratorTapeRecord {
        seq: 1,
        event_type: "status".to_owned(),
        payload_json: "not-json".to_owned(),
    })
    .is_none());
    assert!(project_record(OrchestratorTapeRecord {
        seq: 1,
        event_type: "status".to_owned(),
        payload_json: format!("{{\"kind\":\"failed\",\"padding\":\"{}\"}}", "x".repeat(70_000)),
    })
    .is_none());
    assert!(project(
        "context.assembled",
        json!({
            "context_engine_id": "context_engine",
            "context_engine_version": "1.0.0",
            "context_schema_sha256": "d".repeat(64),
            "input_item_count": 1,
            "retained_item_count": 2,
            "stage_duration_ms": 1,
        }),
    )
    .is_none());
    assert!(project(
        "provider.attempt.completed",
        json!({
            "provider_id": "provider",
            "model_id": "model",
            "route_class": "primary",
            "attempt": 1,
            "outcome": "succeeded",
            "reason_code": "provider.succeeded",
            "stage_duration_ms": METADATA_TRACE_MAX_STAGE_DURATION_MS + 1,
        }),
    )
    .is_none());
}

#[test]
fn capacity_event_uses_closed_payload_and_valid_domain_hash() {
    let context = projection_context();
    let capacity = metadata_trace_capacity_reached_event(
        context,
        MetadataTraceCapacityLimitV1::EventCount,
        u32::try_from(METADATA_TRACE_MAX_EVENTS).expect("event cap should fit u32"),
        u32::try_from(METADATA_TRACE_MAX_EVENTS).expect("event cap should fit u32"),
        "metadata_trace.event_count_reached",
    )
    .expect("capacity event should validate");
    assert!(matches!(capacity.event, MetadataTraceEventDataV1::CapacityReached(_)));
}

fn project(
    event_type: &str,
    payload: Value,
) -> Option<palyra_common::metadata_trace::MetadataTraceEventV1> {
    project_record(OrchestratorTapeRecord {
        seq: 1,
        event_type: event_type.to_owned(),
        payload_json: payload.to_string(),
    })
}

fn project_record(
    record: OrchestratorTapeRecord,
) -> Option<palyra_common::metadata_trace::MetadataTraceEventV1> {
    project_orchestrator_tape_record(&record, projection_context())
}

fn projection_context() -> MetadataTraceProjectionContext<'static> {
    MetadataTraceProjectionContext {
        run_id: RUN_ID,
        sequence: 0,
        generation: 1,
        recorded_at_unix_ms: 1_000,
        causal_parent_event_id_sha256: None,
    }
}

fn assert_projection_excludes_hostile_content(
    event: &palyra_common::metadata_trace::MetadataTraceEventV1,
) {
    let serialized = serde_json::to_string(event).expect("metadata event should serialize");
    for forbidden in [
        SECRET_SENTINEL,
        "https://",
        "C:\\\\private",
        "/private/",
        "reply_text",
        "input_json",
        "response_body",
        "stdout",
        "stderr",
        "prompt_mutation",
    ] {
        assert!(
            !serialized.contains(forbidden),
            "metadata trace leaked forbidden source content '{forbidden}': {serialized}"
        );
    }
}
