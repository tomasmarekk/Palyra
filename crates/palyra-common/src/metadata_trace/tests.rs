//! Contract tests for strict metadata traces.
//!
//! These tests pin causal ordering, hard caps, deterministic serialization, and
//! the absence of rich or secret-bearing fields at the serde boundary.

use std::collections::BTreeSet;

use serde_json::{json, Value};

use super::*;

fn digest(domain: MetadataTraceIdDomainV1, source: &str) -> String {
    metadata_trace_id_sha256(domain, source).expect("test identifiers are bounded")
}

fn trace_event(
    sequence: u32,
    generation: u32,
    event: MetadataTraceEventDataV1,
    causal_parent_event_id_sha256: Option<String>,
) -> MetadataTraceEventV1 {
    MetadataTraceEventV1 {
        sequence,
        generation,
        recorded_at_unix_ms: 1_750_000_000_000 + u64::from(sequence),
        event_id_sha256: digest(
            MetadataTraceIdDomainV1::Event,
            format!("event-{sequence}").as_str(),
        ),
        causal_parent_event_id_sha256,
        stage_duration_ms: Some(5),
        event,
    }
}

fn root_event() -> MetadataTraceEventV1 {
    let mut event = trace_event(
        0,
        1,
        MetadataTraceEventDataV1::RunStarted(RunStartedMetadataV1 {
            entrypoint: MetadataTraceEntrypointV1::NewRun,
        }),
        None,
    );
    event.stage_duration_ms = None;
    event
}

fn runtime_selected_event(sequence: u32, generation: u32, parent: String) -> MetadataTraceEventV1 {
    trace_event(
        sequence,
        generation,
        MetadataTraceEventDataV1::RuntimeSelected(RuntimeSelectedMetadataV1 {
            harness_id: "embedded_run_stream".to_owned(),
            harness_version: "1.0.0".to_owned(),
            runtime_id: "palyrad".to_owned(),
            runtime_version: "0.1.0+test".to_owned(),
            route_class: MetadataTraceRouteClassV1::Primary,
            auth_profile_id_sha256: Some(digest(
                MetadataTraceIdDomainV1::AuthProfile,
                "profile-primary",
            )),
            schema_hashes: vec![MetadataTraceSchemaHashV1 {
                schema_id: "runtime.contract.v1".to_owned(),
                sha256: digest(MetadataTraceIdDomainV1::Custom, "runtime-contract"),
            }],
        }),
        Some(parent),
    )
}

fn terminal_event(
    sequence: u32,
    generation: u32,
    parent: String,
    outcome: MetadataTraceTerminalOutcomeV1,
) -> MetadataTraceEventV1 {
    trace_event(
        sequence,
        generation,
        MetadataTraceEventDataV1::Terminalization(TerminalizationMetadataV1 {
            outcome,
            reason_code: match outcome {
                MetadataTraceTerminalOutcomeV1::Done => "run.done",
                MetadataTraceTerminalOutcomeV1::Failed => "run.failed",
                MetadataTraceTerminalOutcomeV1::Cancelled => "run.cancelled",
                MetadataTraceTerminalOutcomeV1::ForcedAbort => "run.forced_abort",
            }
            .to_owned(),
            output_emitted: outcome == MetadataTraceTerminalOutcomeV1::Done,
            side_effect_may_have_occurred: false,
        }),
        Some(parent),
    )
}

fn valid_trace(outcome: MetadataTraceTerminalOutcomeV1) -> MetadataTraceV1 {
    let root = root_event();
    let runtime = runtime_selected_event(1, 1, root.event_id_sha256.clone());
    let terminal = terminal_event(2, 1, runtime.event_id_sha256.clone(), outcome);
    MetadataTraceV1 {
        schema_version: METADATA_TRACE_SCHEMA_VERSION,
        run_id_sha256: digest(MetadataTraceIdDomainV1::Run, "run-1"),
        session_id_sha256: digest(MetadataTraceIdDomainV1::Session, "session-1"),
        segments: vec![MetadataTraceSegmentV1 {
            segment_id_sha256: digest(MetadataTraceIdDomainV1::Segment, "segment-1"),
            segment_index: 0,
            generation: 1,
            status: MetadataTraceSegmentStatusV1::Complete,
            events: vec![root, runtime, terminal],
        }],
    }
}

#[test]
fn accepts_every_terminal_outcome_and_round_trips() {
    for outcome in [
        MetadataTraceTerminalOutcomeV1::Done,
        MetadataTraceTerminalOutcomeV1::Failed,
        MetadataTraceTerminalOutcomeV1::Cancelled,
        MetadataTraceTerminalOutcomeV1::ForcedAbort,
    ] {
        let trace = valid_trace(outcome);
        trace.validate_shape().expect("terminal trace must validate");
        let encoded = serde_json::to_vec(&trace).expect("trace must serialize");
        let decoded: MetadataTraceV1 =
            serde_json::from_slice(encoded.as_slice()).expect("trace must deserialize");
        assert_eq!(decoded, trace);
    }
}

#[test]
fn accepts_interrupted_prefix_and_recovery_continuation() {
    let root = root_event();
    let runtime = runtime_selected_event(1, 1, root.event_id_sha256.clone());
    let first_segment_id = digest(MetadataTraceIdDomainV1::Segment, "segment-1");
    let continuation = trace_event(
        2,
        2,
        MetadataTraceEventDataV1::RecoveryContinuation(RecoveryContinuationMetadataV1 {
            previous_segment_id_sha256: first_segment_id.clone(),
            reason_code: "recovery.process_restart".to_owned(),
        }),
        Some(runtime.event_id_sha256.clone()),
    );
    let terminal = terminal_event(
        3,
        2,
        continuation.event_id_sha256.clone(),
        MetadataTraceTerminalOutcomeV1::ForcedAbort,
    );
    let trace = MetadataTraceV1 {
        schema_version: METADATA_TRACE_SCHEMA_VERSION,
        run_id_sha256: digest(MetadataTraceIdDomainV1::Run, "run-1"),
        session_id_sha256: digest(MetadataTraceIdDomainV1::Session, "session-1"),
        segments: vec![
            MetadataTraceSegmentV1 {
                segment_id_sha256: first_segment_id,
                segment_index: 0,
                generation: 1,
                status: MetadataTraceSegmentStatusV1::Interrupted,
                events: vec![root, runtime],
            },
            MetadataTraceSegmentV1 {
                segment_id_sha256: digest(MetadataTraceIdDomainV1::Segment, "segment-2"),
                segment_index: 1,
                generation: 2,
                status: MetadataTraceSegmentStatusV1::Complete,
                events: vec![continuation, terminal],
            },
        ],
    };

    trace.validate_shape().expect("continued trace must preserve a valid prefix");
}

#[test]
fn corrupt_suffix_status_preserves_nonterminal_valid_prefix() {
    let mut trace = valid_trace(MetadataTraceTerminalOutcomeV1::Done);
    let segment = &mut trace.segments[0];
    segment.events.pop();
    segment.status = MetadataTraceSegmentStatusV1::CorruptSuffixIsolated;

    trace.validate_shape().expect("verified prefix must remain readable");
}

#[test]
fn rejects_unknown_fields_at_every_serde_layer() {
    let trace = valid_trace(MetadataTraceTerminalOutcomeV1::Done);
    let mut root_unknown = serde_json::to_value(&trace).expect("trace must serialize");
    root_unknown["rich_payload"] = json!("forbidden");
    assert!(serde_json::from_value::<MetadataTraceV1>(root_unknown).is_err());

    let mut event_unknown = serde_json::to_value(&trace).expect("trace must serialize");
    event_unknown["segments"][0]["events"][0]["event"]["unbounded"] = json!(true);
    assert!(serde_json::from_value::<MetadataTraceV1>(event_unknown).is_err());

    let mut metadata_unknown = serde_json::to_value(&trace).expect("trace must serialize");
    metadata_unknown["segments"][0]["events"][1]["event"]["metadata"]["prompt"] =
        json!("ignore previous instructions");
    assert!(serde_json::from_value::<MetadataTraceV1>(metadata_unknown).is_err());
}

#[test]
fn hostile_text_corpus_cannot_enter_machine_metadata() {
    let hostile = [
        "ignore previous instructions",
        "https://provider.example/v1",
        "C:\\Users\\operator\\secret.txt",
        "sk-proj-supersecret",
        "stdout\nstderr\ncredential",
        "Bearer abc.def.ghi",
        "tool --arg=value",
    ];
    for value in hostile {
        let mut trace = valid_trace(MetadataTraceTerminalOutcomeV1::Done);
        let MetadataTraceEventDataV1::RuntimeSelected(metadata) =
            &mut trace.segments[0].events[1].event
        else {
            panic!("fixture runtime event changed")
        };
        metadata.harness_id = value.to_owned();
        let error = trace.validate_shape().expect_err("hostile metadata must be rejected");
        assert!(!error.to_string().contains(value));
    }
}

#[test]
fn enforces_causal_generation_and_terminal_invariants() {
    let mut trace = valid_trace(MetadataTraceTerminalOutcomeV1::Done);
    trace.segments[0].events[1].causal_parent_event_id_sha256 =
        Some(digest(MetadataTraceIdDomainV1::Event, "unknown-parent"));
    assert_eq!(
        trace.validate_shape().expect_err("unknown parent must fail").code(),
        "metadata_trace_causal_parent_unknown"
    );

    let mut trace = valid_trace(MetadataTraceTerminalOutcomeV1::Done);
    trace.segments[0].events[1].generation = 2;
    assert_eq!(
        trace.validate_shape().expect_err("generation mismatch must fail").code(),
        "metadata_trace_event_generation_mismatch"
    );

    let mut trace = valid_trace(MetadataTraceTerminalOutcomeV1::Done);
    trace.segments[0].status = MetadataTraceSegmentStatusV1::Interrupted;
    assert_eq!(
        trace.validate_shape().expect_err("interrupted terminal must fail").code(),
        "metadata_trace_noncomplete_terminal_forbidden"
    );
}

#[test]
fn enforces_schema_context_attempt_timing_and_capacity_caps() {
    let mut trace = valid_trace(MetadataTraceTerminalOutcomeV1::Done);
    let MetadataTraceEventDataV1::RuntimeSelected(metadata) =
        &mut trace.segments[0].events[1].event
    else {
        panic!("fixture runtime event changed")
    };
    metadata.schema_hashes = (0..=METADATA_TRACE_MAX_SCHEMA_HASHES)
        .map(|index| MetadataTraceSchemaHashV1 {
            schema_id: format!("schema.{index:02}"),
            sha256: digest(MetadataTraceIdDomainV1::Custom, format!("schema-{index}").as_str()),
        })
        .collect();
    assert_eq!(
        trace.validate_shape().expect_err("schema cap must fail").code(),
        "metadata_trace_schema_hash_count_invalid"
    );

    let event = trace_event(
        0,
        1,
        MetadataTraceEventDataV1::ContextAssembled(ContextAssembledMetadataV1 {
            context_engine_id: "default_context_engine".to_owned(),
            context_engine_version: "1.0.0".to_owned(),
            context_schema_sha256: digest(MetadataTraceIdDomainV1::Custom, "context-schema"),
            input_item_count: 1,
            retained_item_count: 2,
        }),
        None,
    );
    assert_eq!(
        event.validate_shape().expect_err("retained count must be bounded").code(),
        "metadata_trace_context_count_invalid"
    );

    let mut event = root_event();
    event.stage_duration_ms = Some(METADATA_TRACE_MAX_STAGE_DURATION_MS + 1);
    assert_eq!(
        event.validate_shape().expect_err("timing cap must fail").code(),
        "metadata_trace_stage_duration_exceeded"
    );

    let event = trace_event(
        0,
        1,
        MetadataTraceEventDataV1::CapacityReached(CapacityReachedMetadataV1 {
            limit_kind: MetadataTraceCapacityLimitV1::EventCount,
            observed: u32::try_from(METADATA_TRACE_MAX_EVENTS).expect("cap fits u32"),
            limit: 1,
            reason_code: "trace.event_cap".to_owned(),
        }),
        None,
    );
    assert_eq!(
        event.validate_shape().expect_err("capacity limit must be canonical").code(),
        "metadata_trace_capacity_value_invalid"
    );
}

#[test]
fn serializes_deterministically() {
    let event = root_event();
    let event_id = event.event_id_sha256.clone();
    let first = serde_json::to_string(&event).expect("event must serialize");
    let second = serde_json::to_string(&event).expect("event must serialize repeatedly");
    assert_eq!(first, second);
    assert_eq!(
        first,
        format!(
            "{{\"sequence\":0,\"generation\":1,\"recorded_at_unix_ms\":1750000000000,\"event_id_sha256\":\"{event_id}\",\"event\":{{\"kind\":\"run_started\",\"metadata\":{{\"entrypoint\":\"new_run\"}}}}}}"
        )
    );
}

#[test]
fn identity_hashes_are_domain_separated_and_errors_do_not_echo_sources() {
    let source = "same-identifier";
    let run = metadata_trace_id_sha256(MetadataTraceIdDomainV1::Run, source)
        .expect("bounded source must hash");
    let session = metadata_trace_id_sha256(MetadataTraceIdDomainV1::Session, source)
        .expect("bounded source must hash");
    let tool = metadata_trace_id_sha256(MetadataTraceIdDomainV1::Tool, source)
        .expect("bounded source must hash");
    assert_ne!(run, session);
    assert_ne!(run, tool);
    assert_ne!(session, tool);
    assert_eq!(run.len(), 64);

    let sensitive = "sensitive-source".repeat(METADATA_TRACE_MAX_ID_SOURCE_BYTES);
    let error = metadata_trace_id_sha256(MetadataTraceIdDomainV1::Custom, sensitive.as_str())
        .expect_err("oversized source must fail");
    assert!(!error.to_string().contains(sensitive.as_str()));
}

#[test]
fn all_event_kinds_have_stable_unique_wire_names() {
    let events = [
        MetadataTraceEventDataV1::RunStarted(RunStartedMetadataV1 {
            entrypoint: MetadataTraceEntrypointV1::NewRun,
        }),
        MetadataTraceEventDataV1::RuntimeSelected(RuntimeSelectedMetadataV1 {
            harness_id: "harness".to_owned(),
            harness_version: "1.0.0".to_owned(),
            runtime_id: "runtime".to_owned(),
            runtime_version: "1.0.0".to_owned(),
            route_class: MetadataTraceRouteClassV1::Primary,
            auth_profile_id_sha256: None,
            schema_hashes: vec![MetadataTraceSchemaHashV1 {
                schema_id: "runtime.v1".to_owned(),
                sha256: digest(MetadataTraceIdDomainV1::Custom, "schema"),
            }],
        }),
        MetadataTraceEventDataV1::RuntimeShadowDifferential(RuntimeShadowDifferentialMetadataV1 {
            enrollment: MetadataTraceShadowEnrollmentV1::DeterministicSample,
            classification: MetadataTraceShadowClassificationV1::Expected,
            reason_code: "runtime.shadow.differential_expected".to_owned(),
            runtime_selection: MetadataTraceDifferentialOutcomeV1::Match,
            context_segments: MetadataTraceDifferentialOutcomeV1::Match,
            context_safety: MetadataTraceDifferentialOutcomeV1::Match,
            token_budget: MetadataTraceDifferentialOutcomeV1::Match,
            tool_catalog: MetadataTraceDifferentialOutcomeV1::Match,
            policy_input: MetadataTraceDifferentialOutcomeV1::Match,
            phase_plan: MetadataTraceDifferentialOutcomeV1::Match,
            promotion_blocked: false,
            shadow_side_effect_free: true,
        }),
        MetadataTraceEventDataV1::ContextAssembled(ContextAssembledMetadataV1 {
            context_engine_id: "context".to_owned(),
            context_engine_version: "1.0.0".to_owned(),
            context_schema_sha256: digest(MetadataTraceIdDomainV1::Custom, "context"),
            input_item_count: 1,
            retained_item_count: 1,
        }),
        MetadataTraceEventDataV1::ProviderAttempt(ProviderAttemptMetadataV1 {
            provider_id_sha256: digest(MetadataTraceIdDomainV1::Provider, "provider"),
            model_id_sha256: digest(MetadataTraceIdDomainV1::Model, "model"),
            route_class: MetadataTraceRouteClassV1::Primary,
            auth_profile_id_sha256: None,
            attempt: 1,
            outcome: MetadataTraceProviderAttemptOutcomeV1::Started,
            reason_code: "provider.started".to_owned(),
        }),
        MetadataTraceEventDataV1::ToolGate(ToolGateMetadataV1 {
            tool_id_sha256: digest(MetadataTraceIdDomainV1::Tool, "tool"),
            decision: MetadataTraceToolGateDecisionV1::Allowed,
            reason_code: "tool.allowed".to_owned(),
        }),
        MetadataTraceEventDataV1::Approval(ApprovalMetadataV1 {
            approval_id_sha256: digest(MetadataTraceIdDomainV1::Approval, "approval"),
            decision: MetadataTraceApprovalDecisionV1::Requested,
            reason_code: "approval.requested".to_owned(),
        }),
        MetadataTraceEventDataV1::ToolOutcome(ToolOutcomeMetadataV1 {
            tool_id_sha256: digest(MetadataTraceIdDomainV1::Tool, "tool"),
            attempt: 1,
            outcome: MetadataTraceToolOutcomeV1::Succeeded,
            reason_code: "tool.succeeded".to_owned(),
        }),
        MetadataTraceEventDataV1::Recovery(RecoveryMetadataV1 {
            strategy: MetadataTraceRecoveryStrategyV1::RetrySameRoute,
            attempt: 1,
            reason_code: "recovery.retry".to_owned(),
        }),
        MetadataTraceEventDataV1::DeliveryIntent(DeliveryIntentMetadataV1 {
            delivery_id_sha256: digest(MetadataTraceIdDomainV1::Delivery, "delivery"),
            route: MetadataTraceDeliveryRouteV1::Direct,
            state: MetadataTraceDeliveryStateV1::Planned,
            reason_code: "delivery.planned".to_owned(),
        }),
        MetadataTraceEventDataV1::Terminalization(TerminalizationMetadataV1 {
            outcome: MetadataTraceTerminalOutcomeV1::Done,
            reason_code: "run.done".to_owned(),
            output_emitted: true,
            side_effect_may_have_occurred: false,
        }),
        MetadataTraceEventDataV1::RecoveryContinuation(RecoveryContinuationMetadataV1 {
            previous_segment_id_sha256: digest(MetadataTraceIdDomainV1::Segment, "previous"),
            reason_code: "recovery.continue".to_owned(),
        }),
        MetadataTraceEventDataV1::CapacityReached(CapacityReachedMetadataV1 {
            limit_kind: MetadataTraceCapacityLimitV1::EventCount,
            observed: u32::try_from(METADATA_TRACE_MAX_EVENTS).expect("cap fits u32"),
            limit: u32::try_from(METADATA_TRACE_MAX_EVENTS).expect("cap fits u32"),
            reason_code: "trace.event_cap".to_owned(),
        }),
    ];
    let kinds = events.iter().map(MetadataTraceEventDataV1::kind).collect::<BTreeSet<_>>();
    assert_eq!(kinds.len(), events.len());
}

#[test]
fn json_schema_tracks_contract_and_closes_every_object() {
    const SCHEMA: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../schemas/json/common/metadata-trace.v1.json"
    ));
    let schema: Value = serde_json::from_str(SCHEMA).expect("metadata trace schema must be JSON");
    assert_eq!(schema["properties"]["schema_version"]["const"], METADATA_TRACE_SCHEMA_VERSION);
    assert_eq!(schema["properties"]["segments"]["maxItems"], METADATA_TRACE_MAX_SEGMENTS);
    assert_eq!(schema["$defs"]["event"]["properties"]["sequence"]["maximum"], 511);

    let schema_kinds = schema["$defs"]["event_data"]["oneOf"]
        .as_array()
        .expect("event oneOf must be an array")
        .iter()
        .map(|variant| {
            variant["properties"]["kind"]["const"].as_str().expect("event kind must be a string")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        schema_kinds,
        BTreeSet::from([
            "approval",
            "capacity_reached",
            "context_assembled",
            "delivery_intent",
            "provider_attempt",
            "recovery",
            "recovery_continuation",
            "run_started",
            "runtime_selected",
            "runtime_shadow_differential",
            "terminalization",
            "tool_gate",
            "tool_outcome",
        ])
    );

    assert_closed_schema_objects(&schema);
    assert_forbidden_property_names_absent(&schema);
}

#[test]
fn shadow_differential_requires_consistent_closed_metadata() {
    let valid = trace_event(
        1,
        1,
        MetadataTraceEventDataV1::RuntimeShadowDifferential(RuntimeShadowDifferentialMetadataV1 {
            enrollment: MetadataTraceShadowEnrollmentV1::ExplicitSession,
            classification: MetadataTraceShadowClassificationV1::InvariantViolation,
            reason_code: "runtime.shadow.differential_invariant_violation".to_owned(),
            runtime_selection: MetadataTraceDifferentialOutcomeV1::Match,
            context_segments: MetadataTraceDifferentialOutcomeV1::Match,
            context_safety: MetadataTraceDifferentialOutcomeV1::InvariantViolation,
            token_budget: MetadataTraceDifferentialOutcomeV1::Match,
            tool_catalog: MetadataTraceDifferentialOutcomeV1::Match,
            policy_input: MetadataTraceDifferentialOutcomeV1::Match,
            phase_plan: MetadataTraceDifferentialOutcomeV1::Match,
            promotion_blocked: true,
            shadow_side_effect_free: true,
        }),
        Some(root_event().event_id_sha256),
    );
    valid.validate_shape().expect("consistent shadow metadata should validate");

    let mut forged = valid;
    let MetadataTraceEventDataV1::RuntimeShadowDifferential(metadata) = &mut forged.event else {
        panic!("test event must remain a shadow differential");
    };
    metadata.shadow_side_effect_free = false;
    assert_eq!(
        forged.validate_shape().expect_err("forged shadow authority must fail").code(),
        "metadata_trace_shadow_differential_invalid"
    );
}

fn assert_closed_schema_objects(value: &Value) {
    match value {
        Value::Object(object) => {
            if object.get("type") == Some(&Value::String("object".to_owned())) {
                assert_eq!(
                    object.get("additionalProperties"),
                    Some(&Value::Bool(false)),
                    "every metadata-trace object schema must be closed"
                );
            }
            for nested in object.values() {
                assert_closed_schema_objects(nested);
            }
        }
        Value::Array(values) => {
            for nested in values {
                assert_closed_schema_objects(nested);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn assert_forbidden_property_names_absent(value: &Value) {
    const FORBIDDEN: &[&str] = &[
        "prompt",
        "messages",
        "secret",
        "credential",
        "token",
        "tool_args",
        "command",
        "cwd",
        "stdin",
        "stdout",
        "stderr",
        "provider_payload",
        "url",
        "path",
        "headers",
        "cookies",
    ];
    match value {
        Value::Object(object) => {
            if let Some(properties) = object.get("properties").and_then(Value::as_object) {
                for forbidden in FORBIDDEN {
                    assert!(!properties.contains_key(*forbidden), "forbidden field: {forbidden}");
                }
            }
            for nested in object.values() {
                assert_forbidden_property_names_absent(nested);
            }
        }
        Value::Array(values) => {
            for nested in values {
                assert_forbidden_property_names_absent(nested);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}
