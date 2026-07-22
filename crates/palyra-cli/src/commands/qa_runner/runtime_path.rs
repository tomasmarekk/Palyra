//! Projection of observed QA tape events into bounded runtime-path evidence.

use std::collections::{BTreeMap, BTreeSet};

use palyra_common::{
    qa_evidence::{QaRunTapeEvent, QaToolCallEvidence},
    qa_runtime_path::{
        ContextEngineBindingEvent, McpTransportInvocationEvent, McpTransportInvocationMode,
        ProviderLaneAttestationEvent, ProviderRouteChangeEvent, RuntimeFallbackEvidence,
        RuntimePathComponentEvidence, RuntimePathEvidence, CONTEXT_ENGINE_BINDING_EVENT,
        MCP_TRANSPORT_INVOCATION_EVENT, PROVIDER_LANE_ATTESTATION_EVENT,
        PROVIDER_ROUTE_CHANGE_EVENT, PROVIDER_ROUTE_CHANGE_EVIDENCE_TRUNCATED_EVENT,
        QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION,
    },
    runtime_contracts::{
        RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID,
        RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_VERSION,
    },
};
use serde_json::{Map, Value};

const RUNTIME_PATH_SUMMARY_EVENT: &str = "run.runtime_path_summary";
const RUNTIME_AUTHORITY_EVENT: &str = "runtime.authority.selected";
const HARNESS_SELECTION_EVENT: &str = "harness.selection";
const CONTEXT_ENGINE_PLAN_EVENT: &str = "context.engine.plan";
const RUNNER_EXECUTION_KEY_SOURCE: &str = "runner.execution_key";
const RUNNER_TOOL_OBSERVATIONS_SOURCE: &str = "runner.tool_observations";

pub(super) struct RuntimePathExtractionInput<'a> {
    pub(super) tape_events: &'a [QaRunTapeEvent],
    pub(super) tool_calls: &'a [QaToolCallEvidence],
    pub(super) runtime_version: &'a str,
    pub(super) runtime_contract_version: &'a str,
    pub(super) runner_version: &'a str,
    pub(super) expected_provider_lane: &'a str,
    pub(super) execution_key_digest: &'a str,
    pub(super) provider_binding_sha256: &'a str,
}

/// Builds metadata-only evidence even when the tape is incomplete.
///
/// Partial evidence lets legacy failure descriptors remain durable, while a
/// schema-v5 no-hidden-fallback expectation still fails closed on `complete=false`.
pub(super) fn extract_runtime_path_evidence(
    input: RuntimePathExtractionInput<'_>,
) -> RuntimePathEvidence {
    let mut tape_events = input.tape_events.to_vec();
    tape_events.sort_by_key(|event| event.seq);
    let tape_events = tape_events.as_slice();
    let mut source_events = vec![RUNNER_EXECUTION_KEY_SOURCE.to_owned()];
    let mut reason_codes = Vec::new();
    let duplicate_tape_sequence =
        tape_events.windows(2).any(|events| events[0].seq == events[1].seq);
    let mut complete = !duplicate_tape_sequence;
    if duplicate_tape_sequence {
        reason_codes.push("qa.runner.runtime_path_tape_sequence_duplicate".to_owned());
    }
    let summaries = matching_events(tape_events, RUNTIME_PATH_SUMMARY_EVENT);
    let summary = match summaries.as_slice() {
        [summary] if valid_runtime_summary(&summary.payload) => {
            push_unique(&mut source_events, RUNTIME_PATH_SUMMARY_EVENT);
            Some(*summary)
        }
        [] => {
            complete = false;
            reason_codes.push("qa.runner.runtime_path_summary_missing".to_owned());
            None
        }
        [_] => {
            complete = false;
            reason_codes.push("qa.runner.runtime_path_summary_invalid".to_owned());
            None
        }
        _ => {
            complete = false;
            reason_codes.push("qa.runner.runtime_path_summary_duplicate".to_owned());
            None
        }
    };

    let (attempt_owner, harness_state, context_state) = summary.map_or_else(
        || ("unobserved".to_owned(), None, None),
        |event| {
            (
                string_field(&event.payload, "attempt_owner").unwrap_or("unobserved").to_owned(),
                subsystem_state(&event.payload, "harness"),
                subsystem_state(&event.payload, "context_engine"),
            )
        },
    );
    if attempt_owner == "unobserved" {
        complete = false;
        push_unique(&mut reason_codes, "qa.runner.runtime_path_attempt_owner_missing");
    }
    let authoritative_v2 = authoritative_v2_selected(
        tape_events,
        &mut complete,
        &mut source_events,
        &mut reason_codes,
    );

    let harness = extract_harness(
        tape_events,
        attempt_owner.as_str(),
        harness_state,
        authoritative_v2,
        &mut complete,
        &mut source_events,
        &mut reason_codes,
    );
    let context_engine = extract_context_engine(
        tape_events,
        context_state,
        authoritative_v2,
        &mut complete,
        &mut source_events,
        &mut reason_codes,
    );
    let provider_lane = extract_provider_lane(
        tape_events,
        input.expected_provider_lane,
        input.execution_key_digest,
        input.provider_binding_sha256,
        &mut complete,
        &mut source_events,
        &mut reason_codes,
    );
    let mcp_transport_mode = extract_mcp_transport(
        tape_events,
        input.tool_calls,
        &mut complete,
        &mut source_events,
        &mut reason_codes,
    );
    let fallbacks =
        extract_fallbacks(tape_events, &mut complete, &mut source_events, &mut reason_codes);
    for fallback in &fallbacks {
        push_unique(&mut source_events, fallback.source_event.as_str());
    }
    let fallback_count = u32::try_from(fallbacks.len()).unwrap_or(u32::MAX);
    if complete {
        reason_codes.push("qa.runner.runtime_path_complete".to_owned());
    }
    reason_codes.sort();
    reason_codes.dedup();

    RuntimePathEvidence {
        schema_version: QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION,
        runtime_version: input.runtime_version.to_owned(),
        runtime_contract_version: input.runtime_contract_version.to_owned(),
        runner_version: input.runner_version.to_owned(),
        provider_lane,
        attempt_owner,
        harness,
        context_engine,
        mcp_transport_mode,
        complete,
        source_events,
        reason_codes,
        fallbacks,
        fallback_count,
    }
}

fn extract_harness(
    tape_events: &[QaRunTapeEvent],
    attempt_owner: &str,
    rollout_state: Option<&str>,
    authoritative_v2: bool,
    complete: &mut bool,
    source_events: &mut Vec<String>,
    reason_codes: &mut Vec<String>,
) -> RuntimePathComponentEvidence {
    let selections = matching_events(tape_events, HARNESS_SELECTION_EVENT);
    if authoritative_v2 {
        if attempt_owner != "runtime_kernel_v2.embedded" || !selections.is_empty() {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_v2_authority_mismatch");
        }
        return RuntimePathComponentEvidence {
            id: "runtime_kernel_v2.embedded".to_owned(),
            source_event: RUNTIME_AUTHORITY_EVENT.to_owned(),
            reason_code: "runtime_path.harness.runtime_kernel_v2_selected".to_owned(),
        };
    }
    match (rollout_state, selections.as_slice()) {
        (Some("disabled"), []) if attempt_owner == "embedded_run_stream" => {
            RuntimePathComponentEvidence {
                id: "embedded_run_stream".to_owned(),
                source_event: RUNTIME_PATH_SUMMARY_EVENT.to_owned(),
                reason_code: "runtime_path.harness.embedded_selected".to_owned(),
            }
        }
        (Some("enabled"), [selection]) => {
            push_unique(source_events, HARNESS_SELECTION_EVENT);
            let harness_id = string_field(&selection.payload, "harness_id")
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("unobserved");
            if harness_id == "unobserved" || harness_id != attempt_owner {
                *complete = false;
                push_unique(reason_codes, "qa.runner.runtime_path_harness_mismatch");
            }
            RuntimePathComponentEvidence {
                id: harness_id.to_owned(),
                source_event: HARNESS_SELECTION_EVENT.to_owned(),
                reason_code: string_field(&selection.payload, "selection_reason_code")
                    .unwrap_or("runtime_path.harness.selection_observed")
                    .to_owned(),
            }
        }
        (Some("disabled"), [_]) => {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_harness_unexpected_selection");
            RuntimePathComponentEvidence {
                id: attempt_owner.to_owned(),
                source_event: RUNTIME_PATH_SUMMARY_EVENT.to_owned(),
                reason_code: "runtime_path.harness.selection_conflict".to_owned(),
            }
        }
        (_, selections) => {
            *complete = false;
            push_unique(
                reason_codes,
                if selections.len() > 1 {
                    "qa.runner.runtime_path_harness_selection_duplicate"
                } else {
                    "qa.runner.runtime_path_harness_unproven"
                },
            );
            RuntimePathComponentEvidence {
                id: attempt_owner.to_owned(),
                source_event: RUNNER_EXECUTION_KEY_SOURCE.to_owned(),
                reason_code: "runtime_path.harness.unproven".to_owned(),
            }
        }
    }
}

fn authoritative_v2_selected(
    tape_events: &[QaRunTapeEvent],
    complete: &mut bool,
    source_events: &mut Vec<String>,
    reason_codes: &mut Vec<String>,
) -> bool {
    let events = matching_events(tape_events, RUNTIME_AUTHORITY_EVENT);
    match events.as_slice() {
        [event] if valid_v2_authority_event(&event.payload) => {
            push_unique(source_events, RUNTIME_AUTHORITY_EVENT);
            true
        }
        [] | [_] => false,
        _ => {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_authority_selection_duplicate");
            false
        }
    }
}

fn valid_v2_authority_event(payload: &Value) -> bool {
    payload.get("schema_version").and_then(Value::as_u64) == Some(1)
        && string_field(payload, "route") == Some("v2")
        && payload
            .pointer("/authority/profile")
            .and_then(Value::as_str)
            .is_some_and(|profile| profile == "v2")
        && payload
            .pointer("/authority/selected_runtime")
            .and_then(Value::as_str)
            .is_some_and(|authority| authority == "v2")
}

fn extract_context_engine(
    tape_events: &[QaRunTapeEvent],
    rollout_state: Option<&str>,
    authoritative_v2: bool,
    complete: &mut bool,
    source_events: &mut Vec<String>,
    reason_codes: &mut Vec<String>,
) -> RuntimePathComponentEvidence {
    if authoritative_v2 {
        return extract_authoritative_v2_context_engine(
            tape_events,
            complete,
            source_events,
            reason_codes,
        );
    }
    let plans = matching_events(tape_events, CONTEXT_ENGINE_PLAN_EVENT);
    match (rollout_state, plans.as_slice()) {
        (Some("disabled"), []) => RuntimePathComponentEvidence {
            id: "legacy_provider_input".to_owned(),
            source_event: RUNTIME_PATH_SUMMARY_EVENT.to_owned(),
            reason_code: "runtime_path.context.legacy_selected".to_owned(),
        },
        (Some("enabled"), plans) if !plans.is_empty() => {
            push_unique(source_events, CONTEXT_ENGINE_PLAN_EVENT);
            let every_has_id = plans.iter().all(|plan| context_engine_id(&plan.payload).is_some());
            let ids = plans
                .iter()
                .filter_map(|plan| context_engine_id(&plan.payload))
                .collect::<BTreeSet<_>>();
            let every_enabled = plans.iter().all(|plan| {
                plan.payload.get("rollout_enabled").and_then(Value::as_bool) == Some(true)
            });
            if ids.len() != 1 || !every_has_id || !every_enabled {
                *complete = false;
                push_unique(reason_codes, "qa.runner.runtime_path_context_engine_mismatch");
            }
            RuntimePathComponentEvidence {
                id: ids.into_iter().next().unwrap_or("unobserved").to_owned(),
                source_event: CONTEXT_ENGINE_PLAN_EVENT.to_owned(),
                reason_code: "runtime_path.context.engine_selected".to_owned(),
            }
        }
        (Some("disabled"), _) => {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_context_plan_unexpected");
            RuntimePathComponentEvidence {
                id: "legacy_provider_input".to_owned(),
                source_event: RUNTIME_PATH_SUMMARY_EVENT.to_owned(),
                reason_code: "runtime_path.context.plan_conflict".to_owned(),
            }
        }
        _ => {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_context_engine_unproven");
            RuntimePathComponentEvidence {
                id: "unobserved".to_owned(),
                source_event: RUNNER_EXECUTION_KEY_SOURCE.to_owned(),
                reason_code: "runtime_path.context.unproven".to_owned(),
            }
        }
    }
}

fn extract_authoritative_v2_context_engine(
    tape_events: &[QaRunTapeEvent],
    complete: &mut bool,
    source_events: &mut Vec<String>,
    reason_codes: &mut Vec<String>,
) -> RuntimePathComponentEvidence {
    let events = matching_events(tape_events, CONTEXT_ENGINE_BINDING_EVENT);
    let [event] = events.as_slice() else {
        *complete = false;
        push_unique(
            reason_codes,
            if events.is_empty() {
                "qa.runner.runtime_path_v2_context_binding_missing"
            } else {
                "qa.runner.runtime_path_v2_context_binding_duplicate"
            },
        );
        return RuntimePathComponentEvidence {
            id: "unobserved".to_owned(),
            source_event: RUNNER_EXECUTION_KEY_SOURCE.to_owned(),
            reason_code: "runtime_path.context.v2_binding_unproven".to_owned(),
        };
    };
    let Ok(binding) = serde_json::from_value::<ContextEngineBindingEvent>(event.payload.clone())
    else {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_v2_context_binding_invalid");
        return RuntimePathComponentEvidence {
            id: "unobserved".to_owned(),
            source_event: CONTEXT_ENGINE_BINDING_EVENT.to_owned(),
            reason_code: "runtime_path.context.v2_binding_invalid".to_owned(),
        };
    };
    let valid_shape = binding.validate_shape().is_ok();
    let exact_adapter = binding.engine_id == RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID
        && binding.engine_version == RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_VERSION;
    if !valid_shape || !exact_adapter {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_v2_context_binding_mismatch");
    }
    push_unique(source_events, CONTEXT_ENGINE_BINDING_EVENT);
    RuntimePathComponentEvidence {
        id: binding.engine_id,
        source_event: CONTEXT_ENGINE_BINDING_EVENT.to_owned(),
        reason_code: "runtime_path.context.v2_preassembled_bound".to_owned(),
    }
}

fn extract_provider_lane(
    tape_events: &[QaRunTapeEvent],
    expected_provider_lane: &str,
    execution_key_digest: &str,
    provider_binding_sha256: &str,
    complete: &mut bool,
    source_events: &mut Vec<String>,
    reason_codes: &mut Vec<String>,
) -> String {
    let events = matching_events(tape_events, PROVIDER_LANE_ATTESTATION_EVENT);
    if events.is_empty() {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_provider_attestation_missing");
        return "unobserved".to_owned();
    }
    push_unique(source_events, PROVIDER_LANE_ATTESTATION_EVENT);

    let mut decoded = Vec::with_capacity(events.len());
    for event in events {
        let Ok(attestation) =
            serde_json::from_value::<ProviderLaneAttestationEvent>(event.payload.clone())
        else {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_provider_attestation_invalid");
            return "unobserved".to_owned();
        };
        if attestation.validate_shape().is_err() {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_provider_attestation_invalid");
            return "unobserved".to_owned();
        }
        decoded.push(attestation);
    }

    let lanes = decoded
        .iter()
        .map(|attestation| attestation.provider_lane.as_str())
        .collect::<BTreeSet<_>>();
    let provider_lane = if lanes.len() == 1 {
        lanes.iter().next().copied().unwrap_or("unobserved").to_owned()
    } else {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_provider_attestation_inconsistent");
        "unobserved".to_owned()
    };
    let identities = decoded
        .iter()
        .map(|attestation| {
            (
                attestation.execution_key_digest.as_str(),
                attestation.provider_binding_sha256.as_str(),
                attestation.provider_lane.as_str(),
                attestation.materialization_kind.as_str(),
                attestation.materialized_input_sha256.as_deref(),
                attestation.live_binding.as_ref(),
                attestation.provider_id.as_str(),
                attestation.model_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    if identities.len() != 1 {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_provider_attestation_inconsistent");
    }
    if decoded.iter().any(|attestation| attestation.execution_key_digest != execution_key_digest) {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_provider_execution_key_mismatch");
    }
    if decoded
        .iter()
        .any(|attestation| attestation.provider_binding_sha256 != provider_binding_sha256)
    {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_provider_binding_mismatch");
    }
    if provider_lane != "unobserved" && provider_lane != expected_provider_lane {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_provider_lane_mismatch");
    }
    provider_lane
}

fn extract_mcp_transport(
    tape_events: &[QaRunTapeEvent],
    tool_calls: &[QaToolCallEvidence],
    complete: &mut bool,
    source_events: &mut Vec<String>,
    reason_codes: &mut Vec<String>,
) -> Option<RuntimePathComponentEvidence> {
    let observed_tools = tool_calls.iter().filter(|tool| tool.name.starts_with("mcp.")).fold(
        BTreeMap::<&str, usize>::new(),
        |mut counts, tool| {
            let count = counts.entry(tool.name.as_str()).or_default();
            *count = count.saturating_add(1);
            counts
        },
    );
    let invocation_events = matching_events(tape_events, MCP_TRANSPORT_INVOCATION_EVENT);
    if invocation_events.is_empty() {
        if !observed_tools.is_empty() {
            *complete = false;
            push_unique(source_events, RUNNER_TOOL_OBSERVATIONS_SOURCE);
            push_unique(reason_codes, "qa.runner.runtime_path_mcp_attestation_missing");
        }
        return None;
    }

    push_unique(source_events, MCP_TRANSPORT_INVOCATION_EVENT);
    let mut decoded = Vec::with_capacity(invocation_events.len());
    let mut attestation_ids = BTreeSet::new();
    for event in invocation_events {
        let Ok(attestation) =
            serde_json::from_value::<McpTransportInvocationEvent>(event.payload.clone())
        else {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_mcp_attestation_invalid");
            return None;
        };
        if attestation.validate_shape().is_err() {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_mcp_attestation_invalid");
            return None;
        }
        if !attestation_ids.insert(attestation.attestation_id.clone()) {
            *complete = false;
            push_unique(reason_codes, "qa.runner.runtime_path_mcp_attestation_duplicate");
            return None;
        }
        decoded.push(attestation);
    }

    let mut modes = decoded
        .iter()
        .map(|event| event.transport_mode)
        .collect::<BTreeSet<McpTransportInvocationMode>>()
        .into_iter();
    let Some(transport_mode) = modes.next() else {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_mcp_transport_conflict");
        return None;
    };
    if modes.next().is_some() {
        *complete = false;
        push_unique(reason_codes, "qa.runner.runtime_path_mcp_transport_conflict");
        return None;
    }
    let attested_tools =
        decoded.iter().fold(BTreeMap::<&str, usize>::new(), |mut counts, event| {
            let count = counts.entry(event.namespaced_tool_id.as_str()).or_default();
            *count = count.saturating_add(1);
            counts
        });
    if attested_tools != observed_tools {
        *complete = false;
        push_unique(source_events, RUNNER_TOOL_OBSERVATIONS_SOURCE);
        push_unique(reason_codes, "qa.runner.runtime_path_mcp_attestation_uncorrelated");
        return None;
    }

    Some(RuntimePathComponentEvidence {
        id: transport_mode.as_str().to_owned(),
        source_event: MCP_TRANSPORT_INVOCATION_EVENT.to_owned(),
        reason_code: match transport_mode {
            McpTransportInvocationMode::PerCall => "runtime_path.mcp.per_call_attested",
            McpTransportInvocationMode::Persistent => "runtime_path.mcp.persistent_attested",
        }
        .to_owned(),
    })
}

fn extract_fallbacks(
    tape_events: &[QaRunTapeEvent],
    complete: &mut bool,
    source_events: &mut Vec<String>,
    reason_codes: &mut Vec<String>,
) -> Vec<RuntimeFallbackEvidence> {
    let mut fallbacks = Vec::new();
    for event in tape_events {
        if event.event_type == PROVIDER_ROUTE_CHANGE_EVENT {
            push_unique(source_events, PROVIDER_ROUTE_CHANGE_EVENT);
            let Ok(route_change) =
                serde_json::from_value::<ProviderRouteChangeEvent>(event.payload.clone())
            else {
                *complete = false;
                push_unique(reason_codes, "qa.runner.runtime_path_provider_route_change_invalid");
                continue;
            };
            if route_change.validate_shape().is_err() {
                *complete = false;
                push_unique(reason_codes, "qa.runner.runtime_path_provider_route_change_invalid");
                continue;
            }
            fallbacks.push(RuntimeFallbackEvidence {
                component: "provider".to_owned(),
                from: Some(format!(
                    "{}/{}",
                    route_change.from_provider_id, route_change.from_model_id
                )),
                to: format!("{}/{}", route_change.to_provider_id, route_change.to_model_id),
                reason_code: route_change.reason_code,
                source_event: PROVIDER_ROUTE_CHANGE_EVENT.to_owned(),
            });
            continue;
        }
        if event.event_type == PROVIDER_ROUTE_CHANGE_EVIDENCE_TRUNCATED_EVENT {
            *complete = false;
            push_unique(source_events, PROVIDER_ROUTE_CHANGE_EVIDENCE_TRUNCATED_EVENT);
            push_unique(reason_codes, "qa.runner.runtime_path_provider_route_change_truncated");
            continue;
        }
        let fallback_count_before_event = fallbacks.len();
        collect_payload_fallbacks(
            event.event_type.as_str(),
            &event.payload,
            runtime_component(event.event_type.as_str()),
            &mut fallbacks,
        );
        if event.event_type.ends_with("fallback_used")
            && fallbacks.len() == fallback_count_before_event
        {
            fallbacks.push(RuntimeFallbackEvidence {
                component: runtime_component(event.event_type.as_str()).to_owned(),
                from: None,
                to: "fallback".to_owned(),
                reason_code: event_reason_code(&event.payload)
                    .unwrap_or(event.event_type.as_str())
                    .to_owned(),
                source_event: event.event_type.clone(),
            });
        }
    }
    fallbacks
}

fn collect_payload_fallbacks(
    event_type: &str,
    value: &Value,
    component: &str,
    output: &mut Vec<RuntimeFallbackEvidence>,
) {
    match value {
        Value::Object(object) => {
            if object.get("fallback_used").and_then(Value::as_bool) == Some(true) {
                output.push(fallback_from_object(event_type, component, object));
            }
            for child in object.values() {
                collect_payload_fallbacks(event_type, child, component, output);
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_payload_fallbacks(event_type, child, component, output);
            }
        }
        _ => {}
    }
}

fn fallback_from_object(
    event_type: &str,
    component: &str,
    object: &Map<String, Value>,
) -> RuntimeFallbackEvidence {
    RuntimeFallbackEvidence {
        component: component.to_owned(),
        from: first_string(object, &["requested_backend", "requested", "from"])
            .map(ToOwned::to_owned),
        to: first_string(
            object,
            &[
                "resolved_backend",
                "harness_id",
                "selected",
                "compressor_mode",
                "mode",
                "state",
                "to",
            ],
        )
        .unwrap_or("fallback")
        .to_owned(),
        reason_code: first_string(
            object,
            &[
                "fallback_reason_code",
                "reason_code",
                "selection_reason_code",
                "backend_reason_code",
                "fallback_reason",
                "degraded_reason",
            ],
        )
        .unwrap_or("runtime_path.fallback.observed_without_reason")
        .to_owned(),
        source_event: event_type.to_owned(),
    }
}

fn matching_events<'a>(
    tape_events: &'a [QaRunTapeEvent],
    event_type: &str,
) -> Vec<&'a QaRunTapeEvent> {
    tape_events.iter().filter(|event| event.event_type == event_type).collect()
}

fn valid_runtime_summary(payload: &Value) -> bool {
    payload.get("schema_version").and_then(Value::as_u64) == Some(1)
        && string_field(payload, "event_name") == Some(RUNTIME_PATH_SUMMARY_EVENT)
        && matches!(string_field(payload, "terminal_state"), Some("done" | "failed" | "cancelled"))
        && string_field(payload, "terminal_reason").is_some_and(|reason| !reason.trim().is_empty())
        && string_field(payload, "attempt_owner").is_some_and(|owner| !owner.trim().is_empty())
        && payload.get("subsystems").and_then(Value::as_object).is_some()
}

fn subsystem_state<'a>(payload: &'a Value, subsystem: &str) -> Option<&'a str> {
    payload
        .get("subsystems")
        .and_then(Value::as_object)
        .and_then(|subsystems| subsystems.get(subsystem))
        .and_then(|value| value.get("state"))
        .and_then(Value::as_str)
}

fn context_engine_id(payload: &Value) -> Option<&str> {
    payload
        .pointer("/engine_registry/selected_engine_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn event_reason_code(payload: &Value) -> Option<&str> {
    payload
        .get("reason_code")
        .or_else(|| payload.get("fallback_reason_code"))
        .and_then(Value::as_str)
}

fn string_field<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload.get(key).and_then(Value::as_str)
}

fn first_string<'a>(object: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| object.get(*key).and_then(Value::as_str))
}

fn runtime_component(event_type: &str) -> &str {
    event_type.split_once('.').map_or(event_type, |(component, _)| component)
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use palyra_common::{
        qa_evidence::{QaRunTapeEvent, QaToolCallEvidence},
        qa_runtime_path::{
            evaluate_no_hidden_fallback, qa_provider_binding_sha256, ContextEngineBindingEvent,
            McpTransportInvocationEvent, McpTransportInvocationMode, ProviderLaneAttestationEvent,
            ProviderRouteChangeEvent, CONTEXT_ENGINE_BINDING_EVENT, MCP_TRANSPORT_INVOCATION_EVENT,
            MCP_TRANSPORT_INVOCATION_EVENT_SCHEMA_VERSION, PROVIDER_LANE_ATTESTATION_EVENT,
            PROVIDER_LANE_ATTESTATION_EVENT_SCHEMA_VERSION, PROVIDER_ROUTE_CHANGE_EVENT,
            PROVIDER_ROUTE_CHANGE_EVENT_SCHEMA_VERSION, QA_PROVIDER_FIXTURE_MATERIALIZATION,
            QA_PROVIDER_RECORD_REPLAY_MATERIALIZATION,
        },
        qa_scenarios::parse_qa_scenario_manifest_yaml,
        runtime_contracts::{
            RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID,
            RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_VERSION,
        },
    };
    use serde_json::json;

    use super::{extract_runtime_path_evidence, RuntimePathExtractionInput};

    fn summary(harness: &str, context: &str, attempt_owner: &str) -> QaRunTapeEvent {
        QaRunTapeEvent {
            seq: 10,
            event_type: "run.runtime_path_summary".to_owned(),
            payload: json!({
                "schema_version": 1,
                "event_name": "run.runtime_path_summary",
                "terminal_state": "done",
                "terminal_reason": "runtime.terminal.completed",
                "attempt_owner": attempt_owner,
                "subsystems": {
                    "harness": { "state": harness },
                    "context_engine": { "state": context }
                }
            }),
        }
    }

    fn mcp_transport_event(
        seq: i64,
        attestation_suffix: &str,
        tool_name: &str,
        transport_mode: McpTransportInvocationMode,
    ) -> QaRunTapeEvent {
        let payload = McpTransportInvocationEvent {
            schema_version: MCP_TRANSPORT_INVOCATION_EVENT_SCHEMA_VERSION,
            event_name: MCP_TRANSPORT_INVOCATION_EVENT.to_owned(),
            attestation_id: format!("mcpatt_{attestation_suffix}"),
            transport_id: "mcp.transport.0123456789abcdef".to_owned(),
            namespaced_tool_id: tool_name.to_owned(),
            transport_mode,
        };
        QaRunTapeEvent {
            seq,
            event_type: MCP_TRANSPORT_INVOCATION_EVENT.to_owned(),
            payload: serde_json::to_value(payload).expect("MCP transport evidence should encode"),
        }
    }

    fn provider_lane_event(
        seq: i64,
        provider_lane: &str,
        materialization_kind: &str,
        materialized_input_sha256: &str,
    ) -> QaRunTapeEvent {
        let provider_binding_sha256 = qa_provider_binding_sha256(
            provider_lane,
            materialization_kind,
            materialized_input_sha256,
        )
        .expect("fixture binding should hash");
        let payload = ProviderLaneAttestationEvent {
            schema_version: PROVIDER_LANE_ATTESTATION_EVENT_SCHEMA_VERSION,
            event_name: PROVIDER_LANE_ATTESTATION_EVENT.to_owned(),
            execution_key_digest: "e".repeat(64),
            provider_binding_sha256,
            provider_lane: provider_lane.to_owned(),
            materialization_kind: materialization_kind.to_owned(),
            materialized_input_sha256: Some(materialized_input_sha256.to_owned()),
            live_binding: None,
            provider_id: "deterministic-primary".to_owned(),
            model_id: "deterministic".to_owned(),
        };
        QaRunTapeEvent {
            seq,
            event_type: PROVIDER_LANE_ATTESTATION_EVENT.to_owned(),
            payload: serde_json::to_value(payload).expect("provider lane evidence should encode"),
        }
    }

    fn provider_route_change_event(seq: i64) -> QaRunTapeEvent {
        let payload = ProviderRouteChangeEvent {
            schema_version: PROVIDER_ROUTE_CHANGE_EVENT_SCHEMA_VERSION,
            event_name: PROVIDER_ROUTE_CHANGE_EVENT.to_owned(),
            transition_index: 0,
            from_provider_id: "provider-a".to_owned(),
            from_model_id: "model-a".to_owned(),
            to_provider_id: "provider-b".to_owned(),
            to_model_id: "model-b".to_owned(),
            reason_code: "runtime_path.provider.route_changed".to_owned(),
        };
        QaRunTapeEvent {
            seq,
            event_type: PROVIDER_ROUTE_CHANGE_EVENT.to_owned(),
            payload: serde_json::to_value(payload).expect("provider route evidence should encode"),
        }
    }

    fn authoritative_v2_event(seq: i64) -> QaRunTapeEvent {
        QaRunTapeEvent {
            seq,
            event_type: "runtime.authority.selected".to_owned(),
            payload: json!({
                "schema_version": 1,
                "route": "v2",
                "authority": {
                    "schema_version": 1,
                    "profile": "v2",
                    "generation": 1,
                    "disposition": "selected",
                    "selected_runtime": "v2",
                    "shadow_evaluation_enabled": false,
                    "reason": "v2_profile_selected",
                    "reason_code": "runtime.selection.v2_profile_selected"
                }
            }),
        }
    }

    fn authoritative_v2_context_binding_event(seq: i64) -> QaRunTapeEvent {
        QaRunTapeEvent {
            seq,
            event_type: CONTEXT_ENGINE_BINDING_EVENT.to_owned(),
            payload: serde_json::to_value(ContextEngineBindingEvent {
                schema_version:
                    palyra_common::qa_runtime_path::CONTEXT_ENGINE_BINDING_EVENT_SCHEMA_VERSION,
                event_name: CONTEXT_ENGINE_BINDING_EVENT.to_owned(),
                engine_id: RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID.to_owned(),
                engine_version: RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_VERSION.to_owned(),
                projection_epoch: 7,
            })
            .expect("context binding evidence should encode"),
        }
    }

    fn extract(
        events: &[QaRunTapeEvent],
        tools: &[QaToolCallEvidence],
    ) -> palyra_common::qa_runtime_path::RuntimePathEvidence {
        let mut events = events.to_vec();
        events.push(provider_lane_event(
            8,
            "fixture",
            QA_PROVIDER_FIXTURE_MATERIALIZATION,
            &"a".repeat(64),
        ));
        let provider_binding_sha256 = qa_provider_binding_sha256(
            "fixture",
            QA_PROVIDER_FIXTURE_MATERIALIZATION,
            &"a".repeat(64),
        )
        .expect("fixture binding should hash");
        extract_runtime_path_evidence(RuntimePathExtractionInput {
            tape_events: events.as_slice(),
            tool_calls: tools,
            runtime_version: "palyrad-test",
            runtime_contract_version: "runtime-contracts.test",
            runner_version: "qa-runner.test",
            expected_provider_lane: "fixture",
            execution_key_digest: &"e".repeat(64),
            provider_binding_sha256: provider_binding_sha256.as_str(),
        })
    }

    fn extract_without_provider_attestation(
        events: &[QaRunTapeEvent],
    ) -> palyra_common::qa_runtime_path::RuntimePathEvidence {
        let provider_binding_sha256 = qa_provider_binding_sha256(
            "fixture",
            QA_PROVIDER_FIXTURE_MATERIALIZATION,
            &"a".repeat(64),
        )
        .expect("fixture binding should hash");
        extract_runtime_path_evidence(RuntimePathExtractionInput {
            tape_events: events,
            tool_calls: &[],
            runtime_version: "palyrad-test",
            runtime_contract_version: "runtime-contracts.test",
            runner_version: "qa-runner.test",
            expected_provider_lane: "fixture",
            execution_key_digest: &"e".repeat(64),
            provider_binding_sha256: provider_binding_sha256.as_str(),
        })
    }

    #[test]
    fn provider_attestation_missing_or_mismatched_is_durable_partial_evidence() {
        let missing = extract_without_provider_attestation(&[summary(
            "disabled",
            "disabled",
            "embedded_run_stream",
        )]);
        assert!(!missing.complete);
        assert_eq!(missing.provider_lane, "unobserved");
        assert!(missing
            .reason_codes
            .contains(&"qa.runner.runtime_path_provider_attestation_missing".to_owned()));
        missing.validate_shape().expect("missing attestation evidence should remain durable");

        let replay_digest = "c".repeat(64);
        let mismatched = extract_without_provider_attestation(&[
            provider_lane_event(
                8,
                "record_replay",
                QA_PROVIDER_RECORD_REPLAY_MATERIALIZATION,
                replay_digest.as_str(),
            ),
            summary("disabled", "disabled", "embedded_run_stream"),
        ]);
        assert!(!mismatched.complete);
        assert_eq!(mismatched.provider_lane, "record_replay");
        assert!(mismatched
            .reason_codes
            .contains(&"qa.runner.runtime_path_provider_binding_mismatch".to_owned()));
        assert!(mismatched
            .reason_codes
            .contains(&"qa.runner.runtime_path_provider_lane_mismatch".to_owned()));
        mismatched.validate_shape().expect("mismatched attestation evidence should remain durable");
    }

    #[test]
    fn provider_attestations_require_consistent_provider_and_model_identity() {
        let first =
            provider_lane_event(8, "fixture", QA_PROVIDER_FIXTURE_MATERIALIZATION, &"a".repeat(64));
        let mut second =
            provider_lane_event(9, "fixture", QA_PROVIDER_FIXTURE_MATERIALIZATION, &"a".repeat(64));
        second.payload["provider_id"] = json!("deterministic-secondary");
        second.payload["model_id"] = json!("deterministic-v2");

        let evidence = extract_without_provider_attestation(&[
            first,
            second,
            summary("disabled", "disabled", "embedded_run_stream"),
        ]);

        assert!(!evidence.complete);
        assert!(evidence
            .reason_codes
            .contains(&"qa.runner.runtime_path_provider_attestation_inconsistent".to_owned()));
    }

    #[test]
    fn embedded_legacy_path_is_complete_without_fallbacks() {
        let mut evidence = extract(&[summary("disabled", "disabled", "embedded_run_stream")], &[]);

        assert!(evidence.complete, "{evidence:#?}");
        assert_eq!(evidence.attempt_owner, "embedded_run_stream");
        assert_eq!(evidence.harness.id, "embedded_run_stream");
        assert_eq!(evidence.context_engine.id, "legacy_provider_input");
        assert_eq!(evidence.mcp_transport_mode, None);
        assert_eq!(evidence.fallback_count, 0);

        let manifest = parse_qa_scenario_manifest_yaml(include_str!(
            "../../../../../qa/scenarios/real_runtime/text_exact.yaml"
        ))
        .expect("baseline manifest should parse");
        let expectation = manifest.expect.runtime_path.expect("schema-v5 path should be required");
        assert_eq!(expectation.mcp_transport_mode, None);
        evidence.runtime_contract_version = expectation.runtime_contract_version.clone();
        let mismatches = evaluate_no_hidden_fallback(&expectation, &evidence)
            .expect("manifest and extracted evidence should evaluate");
        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    #[test]
    fn authoritative_v2_path_requires_exact_persisted_authority() {
        let evidence = extract(
            &[
                authoritative_v2_event(1),
                authoritative_v2_context_binding_event(2),
                summary("disabled", "disabled", "runtime_kernel_v2.embedded"),
            ],
            &[],
        );

        assert!(evidence.complete, "{evidence:#?}");
        assert_eq!(evidence.attempt_owner, "runtime_kernel_v2.embedded");
        assert_eq!(evidence.harness.id, "runtime_kernel_v2.embedded");
        assert_eq!(evidence.harness.source_event, "runtime.authority.selected");
        assert_eq!(evidence.context_engine.id, RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID);
        assert_eq!(evidence.context_engine.source_event, CONTEXT_ENGINE_BINDING_EVENT);
        assert_eq!(evidence.fallback_count, 0);

        let missing_binding = extract(
            &[
                authoritative_v2_event(1),
                summary("disabled", "disabled", "runtime_kernel_v2.embedded"),
            ],
            &[],
        );
        assert!(!missing_binding.complete);
        assert!(missing_binding
            .reason_codes
            .contains(&"qa.runner.runtime_path_v2_context_binding_missing".to_owned()));

        let mut mismatched_binding = authoritative_v2_context_binding_event(2);
        mismatched_binding.payload["engine_id"] = json!("default_context_engine");
        let mismatched_evidence = extract(
            &[
                authoritative_v2_event(1),
                mismatched_binding,
                summary("disabled", "disabled", "runtime_kernel_v2.embedded"),
            ],
            &[],
        );
        assert!(!mismatched_evidence.complete);
        assert!(mismatched_evidence
            .reason_codes
            .contains(&"qa.runner.runtime_path_v2_context_binding_mismatch".to_owned()));

        let mut invalid = authoritative_v2_event(1);
        invalid.payload["authority"]["selected_runtime"] = json!("legacy");
        let invalid_evidence =
            extract(&[invalid, summary("disabled", "disabled", "runtime_kernel_v2.embedded")], &[]);
        assert!(!invalid_evidence.complete);
        assert!(invalid_evidence
            .reason_codes
            .contains(&"qa.runner.runtime_path_harness_unproven".to_owned()));
    }

    #[test]
    fn external_harness_and_context_plan_must_correlate_with_summary() {
        let events = [
            QaRunTapeEvent {
                seq: 1,
                event_type: "harness.selection".to_owned(),
                payload: json!({
                    "harness_id": "external.harness",
                    "selection_reason_code": "harness.explicit_plugin",
                    "fallback_used": false
                }),
            },
            QaRunTapeEvent {
                seq: 2,
                event_type: "context.engine.plan".to_owned(),
                payload: json!({
                    "rollout_enabled": true,
                    "engine_registry": { "selected_engine_id": "default_context_engine" }
                }),
            },
            summary("enabled", "enabled", "external.harness"),
        ];
        let evidence = extract(&events, &[]);

        assert!(evidence.complete, "{evidence:#?}");
        assert_eq!(evidence.harness.id, "external.harness");
        assert_eq!(evidence.context_engine.id, "default_context_engine");
    }

    #[test]
    fn missing_invalid_or_duplicate_terminal_summary_is_partial() {
        let missing = extract(&[], &[]);
        let mut invalid_summary = summary("disabled", "disabled", "embedded_run_stream");
        invalid_summary
            .payload
            .as_object_mut()
            .expect("summary payload should be an object")
            .remove("terminal_reason");
        let invalid = extract(&[invalid_summary], &[]);
        let duplicate = extract(
            &[
                summary("disabled", "disabled", "embedded_run_stream"),
                summary("disabled", "disabled", "embedded_run_stream"),
            ],
            &[],
        );

        assert!(!missing.complete);
        assert!(missing
            .reason_codes
            .contains(&"qa.runner.runtime_path_summary_missing".to_owned()));
        assert!(!invalid.complete);
        assert!(invalid
            .reason_codes
            .contains(&"qa.runner.runtime_path_summary_invalid".to_owned()));
        assert!(!duplicate.complete);
        assert!(duplicate
            .reason_codes
            .contains(&"qa.runner.runtime_path_summary_duplicate".to_owned()));
    }

    #[test]
    fn explicit_fallback_and_per_call_mcp_remain_visible() {
        let events = [
            QaRunTapeEvent {
                seq: 3,
                event_type: "tool.decision".to_owned(),
                payload: json!({
                    "backend": {
                        "requested_backend": "networked_worker",
                        "resolved_backend": "local",
                        "fallback_used": true,
                        "backend_reason_code": "execution_backend.worker_unavailable"
                    }
                }),
            },
            mcp_transport_event(
                4,
                "0123456789abcdef",
                "mcp.docs.search",
                McpTransportInvocationMode::PerCall,
            ),
            summary("disabled", "disabled", "embedded_run_stream"),
        ];
        let tools = [QaToolCallEvidence {
            name: "mcp.docs.search".to_owned(),
            proposal_id: Some("call-1".to_owned()),
            success: Some(true),
        }];
        let evidence = extract(&events, &tools);

        assert_eq!(evidence.fallback_count, 1);
        assert_eq!(evidence.fallbacks[0].reason_code, "execution_backend.worker_unavailable");
        assert_eq!(
            evidence.mcp_transport_mode.as_ref().map(|mode| mode.id.as_str()),
            Some("per_call")
        );
        assert!(evidence.source_events.iter().any(|event| event == MCP_TRANSPORT_INVOCATION_EVENT));
        assert!(evidence.complete, "{evidence:#?}");
    }

    #[test]
    fn one_provider_route_change_projects_one_fallback() {
        let evidence = extract(
            &[
                provider_route_change_event(9),
                summary("disabled", "disabled", "embedded_run_stream"),
            ],
            &[],
        );

        assert!(evidence.complete, "{evidence:#?}");
        assert_eq!(evidence.fallback_count, 1);
        assert_eq!(evidence.fallbacks[0].component, "provider");
        assert_eq!(evidence.fallbacks[0].from.as_deref(), Some("provider-a/model-a"));
        assert_eq!(evidence.fallbacks[0].to, "provider-b/model-b");
        assert_eq!(evidence.fallbacks[0].reason_code, "runtime_path.provider.route_changed");
    }

    #[test]
    fn mcp_tool_without_canonical_attestation_fails_closed() {
        let events = [
            QaRunTapeEvent {
                seq: 2,
                event_type: "mcp.runtime.started".to_owned(),
                payload: json!({"transport_mode": "persistent"}),
            },
            summary("disabled", "disabled", "embedded_run_stream"),
        ];
        let tools = [QaToolCallEvidence {
            name: "mcp.docs.search".to_owned(),
            proposal_id: Some("call-1".to_owned()),
            success: Some(true),
        }];

        let mut evidence = extract(&events, &tools);

        assert!(!evidence.complete);
        assert_eq!(evidence.mcp_transport_mode, None);
        assert!(evidence
            .reason_codes
            .contains(&"qa.runner.runtime_path_mcp_attestation_missing".to_owned()));
        assert!(!evidence.source_events.iter().any(|event| event == "mcp.runtime.started"));

        let scenario = include_str!("../../../../../qa/scenarios/real_runtime/text_exact.yaml")
            .replace(
            "    context_engine_id: legacy_provider_input\n",
            "    context_engine_id: legacy_provider_input\n    mcp_transport_mode: persistent\n",
        );
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("schema-v5 MCP expectation should parse");
        let expectation = manifest.expect.runtime_path.expect("schema-v5 path should be required");
        evidence.runtime_contract_version = expectation.runtime_contract_version.clone();
        let mismatch_codes = evaluate_no_hidden_fallback(&expectation, &evidence)
            .expect("valid contracts should evaluate")
            .into_iter()
            .map(|mismatch| mismatch.code)
            .collect::<Vec<_>>();
        assert!(mismatch_codes.contains(&"runtime_path_evidence_incomplete".to_owned()));
        assert!(mismatch_codes.contains(&"runtime_path_mcp_transport_missing".to_owned()));
    }

    #[test]
    fn uncorrelated_mcp_attestation_cannot_select_transport_mode() {
        let events = [
            mcp_transport_event(
                2,
                "0123456789abcdef",
                "mcp.docs.search",
                McpTransportInvocationMode::Persistent,
            ),
            summary("disabled", "disabled", "embedded_run_stream"),
        ];
        let tools = [QaToolCallEvidence {
            name: "mcp.docs.lookup".to_owned(),
            proposal_id: Some("call-1".to_owned()),
            success: Some(true),
        }];

        let evidence = extract(&events, &tools);

        assert!(!evidence.complete);
        assert_eq!(evidence.mcp_transport_mode, None);
        assert!(evidence
            .reason_codes
            .contains(&"qa.runner.runtime_path_mcp_attestation_uncorrelated".to_owned()));
    }

    #[test]
    fn duplicate_mcp_attestation_cannot_prove_multiple_calls() {
        let events = [
            mcp_transport_event(
                2,
                "0123456789abcdef",
                "mcp.docs.search",
                McpTransportInvocationMode::PerCall,
            ),
            mcp_transport_event(
                3,
                "0123456789abcdef",
                "mcp.docs.search",
                McpTransportInvocationMode::PerCall,
            ),
            summary("disabled", "disabled", "embedded_run_stream"),
        ];
        let tools = [
            QaToolCallEvidence {
                name: "mcp.docs.search".to_owned(),
                proposal_id: Some("call-1".to_owned()),
                success: Some(true),
            },
            QaToolCallEvidence {
                name: "mcp.docs.search".to_owned(),
                proposal_id: Some("call-2".to_owned()),
                success: Some(true),
            },
        ];

        let evidence = extract(&events, &tools);

        assert!(!evidence.complete);
        assert_eq!(evidence.mcp_transport_mode, None);
        assert!(evidence
            .reason_codes
            .contains(&"qa.runner.runtime_path_mcp_attestation_duplicate".to_owned()));
    }

    #[test]
    fn repeated_fallback_events_each_contribute_to_the_exact_count() {
        let events = [
            QaRunTapeEvent {
                seq: 1,
                event_type: "provider.fallback_used".to_owned(),
                payload: json!({"reason_code": "provider.primary_unavailable"}),
            },
            QaRunTapeEvent {
                seq: 2,
                event_type: "provider.fallback_used".to_owned(),
                payload: json!({"reason_code": "provider.retry_exhausted"}),
            },
            summary("disabled", "disabled", "embedded_run_stream"),
        ];
        let evidence = extract(&events, &[]);

        assert_eq!(evidence.fallback_count, 2);
        assert_eq!(
            evidence
                .fallbacks
                .iter()
                .map(|fallback| fallback.reason_code.as_str())
                .collect::<Vec<_>>(),
            ["provider.primary_unavailable", "provider.retry_exhausted"]
        );
    }
}
