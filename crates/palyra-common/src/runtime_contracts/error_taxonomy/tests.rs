//! Tests for the strict runtime-error and invariant contracts.

use std::collections::BTreeSet;

use super::*;

const RUNTIME_ERROR_SCHEMA_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/json/common/runtime-error-envelope.v1.json"
));
const RUNTIME_ERROR_REDACTION_CORPUS_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/golden/runtime_error_metadata_redaction_cases.json"
));

fn base_input() -> RuntimeErrorEnvelopeV1Input {
    RuntimeErrorEnvelopeV1Input {
        class: RuntimeErrorClass::ProviderTerminal,
        reason_code: "provider.recovery.fail_closed".to_owned(),
        subsystem: RuntimeSubsystem::Provider,
        phase: RuntimeErrorPhase::ProviderFinalization,
        retryability: RuntimeRetryability::NotRetryable,
        security_class: RuntimeErrorSecurityClass::Sensitive,
        user_visibility: RuntimeErrorUserVisibility::SafeMessage,
        output_emitted: false,
        side_effect_may_have_occurred: false,
        safe_message: "provider request failed".to_owned(),
        recovery_hint: "inspect provider diagnostics".to_owned(),
    }
}

fn valid_invariant_evidence() -> RuntimeInvariantEvidence {
    RuntimeInvariantEvidence {
        closed_generation: true,
        terminal_event_count: 1,
        active_generation_count: 0,
        automatic_unresolved_side_effect_attempts: 1,
        mutating_or_external_effect_started: true,
        durable_effect_intent: true,
        delivery_send_started: true,
        durable_delivery_intent: true,
        outcome_unknown: false,
        unknown_outcome_marked_terminal: false,
    }
}

fn invariant_error(
    mut evidence: RuntimeInvariantEvidence,
    mutate: impl FnOnce(&mut RuntimeInvariantEvidence),
) -> RuntimeInvariantValidationError {
    mutate(&mut evidence);
    validate_runtime_invariant_evidence(evidence)
        .expect_err("mutated evidence should violate an invariant")
}

#[test]
fn runtime_error_envelope_serializes_stable_v1_shape() {
    let envelope = RuntimeErrorEnvelopeV1::try_new(base_input())
        .expect("valid runtime error should construct");
    let value = serde_json::to_value(&envelope).expect("runtime error should serialize");

    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["class"], "provider_terminal");
    assert_eq!(value["reason_code"], "provider.recovery.fail_closed");
    assert_eq!(value["subsystem"], "provider");
    assert_eq!(value["phase"], "provider_finalization");
    assert_eq!(value["retryability"], "not_retryable");
    assert_eq!(value["security_class"], "sensitive");
    assert_eq!(value["user_visibility"], "safe_message");
    assert_eq!(value["output_emitted"], false);
    assert_eq!(value["side_effect_may_have_occurred"], false);
    assert_eq!(value["safe_message"], "provider request failed");
    assert_eq!(value["recovery_hint"], "inspect provider diagnostics");
    assert_eq!(value.as_object().map(serde_json::Map::len), Some(12));
}

#[test]
fn runtime_error_deserialization_rejects_unknown_fields_and_schema_versions() {
    let envelope = RuntimeErrorEnvelopeV1::try_new(base_input())
        .expect("valid runtime error should construct");
    let mut value = serde_json::to_value(envelope).expect("runtime error should serialize");
    value["unexpected"] = json!(true);
    assert!(serde_json::from_value::<RuntimeErrorEnvelopeV1>(value).is_err());

    let mut version = serde_json::to_value(
        RuntimeErrorEnvelopeV1::try_new(base_input())
            .expect("valid runtime error should construct"),
    )
    .expect("runtime error should serialize");
    version["schema_version"] = json!(2);
    assert!(serde_json::from_value::<RuntimeErrorEnvelopeV1>(version).is_err());
}

#[test]
fn runtime_error_text_is_redacted_and_utf8_byte_bounded() {
    let mut input = base_input();
    input.safe_message = format!(
        "raw provider body token=secret-value stderr: C:\\Users\\demo\\secret.txt {}",
        "\u{017E}".repeat(MAX_RUNTIME_ERROR_SAFE_MESSAGE_BYTES)
    );
    input.recovery_hint =
        "Authorization: Bearer sk-secret-token\nstack backtrace:\n at C:\\work\\main.rs".to_owned();

    let envelope =
        RuntimeErrorEnvelopeV1::try_new(input).expect("unsafe source text should be sanitized");
    let encoded = serde_json::to_string(&envelope).expect("runtime error should serialize");

    assert!(envelope.safe_message().len() <= MAX_RUNTIME_ERROR_SAFE_MESSAGE_BYTES);
    assert!(envelope.recovery_hint().len() <= MAX_RUNTIME_ERROR_RECOVERY_HINT_BYTES);
    assert!(!encoded.contains("secret-value"));
    assert!(!encoded.contains("sk-secret-token"));
    assert!(!encoded.contains("C:\\\\Users"));
    assert!(!encoded.contains("stack backtrace"));

    let mut long_benign_input = base_input();
    long_benign_input.safe_message = "\u{017E}".repeat(MAX_RUNTIME_ERROR_SAFE_MESSAGE_BYTES);
    long_benign_input.recovery_hint = "inspect diagnostics ".repeat(64);
    let bounded = RuntimeErrorEnvelopeV1::try_new(long_benign_input)
        .expect("long benign text should truncate on a UTF-8 boundary");
    assert!(bounded.safe_message().len() <= MAX_RUNTIME_ERROR_SAFE_MESSAGE_BYTES);
    assert!(bounded.recovery_hint().len() <= MAX_RUNTIME_ERROR_RECOVERY_HINT_BYTES);
}

#[test]
fn runtime_error_metadata_redaction_corpus_never_leaks_source_detail() {
    let corpus: Value = serde_json::from_str(RUNTIME_ERROR_REDACTION_CORPUS_JSON)
        .expect("runtime error redaction corpus should parse");
    assert_eq!(corpus["schema_version"], 1);
    for case in corpus["cases"].as_array().expect("cases should be an array") {
        let mut input = base_input();
        input.safe_message =
            case["safe_message"].as_str().expect("safe_message should be a string").to_owned();
        input.recovery_hint =
            case["recovery_hint"].as_str().expect("recovery_hint should be a string").to_owned();
        let encoded = serde_json::to_string(
            &RuntimeErrorEnvelopeV1::try_new(input)
                .expect("corpus case should sanitize into a valid envelope"),
        )
        .expect("runtime error should serialize");
        for forbidden in case["forbidden"].as_array().expect("forbidden should be an array") {
            let forbidden = forbidden.as_str().expect("forbidden entry should be a string");
            assert!(
                !encoded.contains(forbidden),
                "case {} leaked {forbidden}: {encoded}",
                case["id"].as_str().unwrap_or("unknown")
            );
        }
    }
}

#[test]
fn runtime_error_schema_matches_rust_vocabulary() {
    let schema: Value = serde_json::from_str(RUNTIME_ERROR_SCHEMA_JSON)
        .expect("runtime error JSON Schema should parse");
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["allOf"].as_array().map(Vec::len), Some(7));
    assert_eq!(schema["properties"]["schema_version"]["const"], 1);
    assert_eq!(
        schema["properties"]["reason_code"]["pattern"],
        "^(?!.*[./]{2})[a-z0-9][a-z0-9._/-]*[a-z0-9]$"
    );
    assert_eq!(schema["properties"]["reason_code"]["minLength"], 3);
    assert_eq!(
        schema["properties"]["reason_code"]["maxLength"],
        MAX_RUNTIME_ERROR_REASON_CODE_BYTES
    );
    assert_eq!(
        schema["properties"]["safe_message"]["maxLength"],
        MAX_RUNTIME_ERROR_SAFE_MESSAGE_BYTES
    );
    assert_eq!(
        schema["properties"]["recovery_hint"]["maxLength"],
        MAX_RUNTIME_ERROR_RECOVERY_HINT_BYTES
    );
    assert_eq!(schema["properties"]["recovery_hint"]["pattern"], "\\S");
    assert_eq!(schema["allOf"][6]["then"]["properties"]["safe_message"]["pattern"], "\\S");
    assert_eq!(schema["required"], runtime_error_contract_snapshot()["required_fields"]);
    for (field, expected) in [
        ("class", RuntimeErrorClass::ALL.iter().map(|value| value.as_str()).collect::<Vec<_>>()),
        ("subsystem", RuntimeSubsystem::ALL.iter().map(|value| value.as_str()).collect::<Vec<_>>()),
        ("phase", RuntimeErrorPhase::ALL.iter().map(|value| value.as_str()).collect::<Vec<_>>()),
        (
            "retryability",
            RuntimeRetryability::ALL.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
        ),
        (
            "security_class",
            RuntimeErrorSecurityClass::ALL.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
        ),
        (
            "user_visibility",
            RuntimeErrorUserVisibility::ALL.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
        ),
    ] {
        let actual = schema["properties"][field]["enum"]
            .as_array()
            .expect("enum should be an array")
            .iter()
            .map(|value| value.as_str().expect("enum entry should be a string"))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "schema enum drift for {field}");
    }
}

#[test]
fn runtime_error_reason_codes_are_strict_machine_identifiers() {
    for valid in [
        "provider.recovery.retry_after",
        "provider/auth_failed",
        "platform_outcome_unknown",
        "tool-repair.failed",
    ] {
        assert!(validate_runtime_reason_code(valid).is_ok(), "{valid}");
    }
    for invalid in [
        "UPPER.case",
        "space separated",
        "provider..failed",
        "provider//failed",
        ".leading",
        "trailing/",
    ] {
        assert!(validate_runtime_reason_code(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn uncertain_side_effects_never_allow_automatic_retry() {
    for class in [RuntimeErrorClass::ToolExecutionUnknown, RuntimeErrorClass::DeliveryUnknown] {
        for retryability in RuntimeRetryability::ALL {
            let mut input = base_input();
            input.class = class;
            input.reason_code = "runtime.uncertain.outcome".to_owned();
            input.retryability = *retryability;
            input.side_effect_may_have_occurred = true;
            let result = RuntimeErrorEnvelopeV1::try_new(input);
            assert_eq!(
                result.is_err(),
                retryability.allows_automatic_retry(),
                "unexpected uncertainty posture for {class}/{retryability}"
            );
        }
    }
}

#[test]
fn exact_legacy_mapping_covers_provider_tool_approval_delivery_and_plugin_errors() {
    let cases = [
        ("provider.recovery.retry_after", RuntimeErrorClass::ProviderRetryable, false),
        (
            "tool_replay.mutating_timeout_requires_guard",
            RuntimeErrorClass::ToolExecutionUnknown,
            true,
        ),
        ("approval.required", RuntimeErrorClass::ApprovalRequired, false),
        ("approval.denied", RuntimeErrorClass::PolicyDenied, false),
        ("platform_outcome_unknown", RuntimeErrorClass::DeliveryUnknown, true),
        (
            "agent_harness_plugin.contract_rejected",
            RuntimeErrorClass::PluginContractViolation,
            false,
        ),
        ("plugin.host_call.denied.service_grant_missing", RuntimeErrorClass::PolicyDenied, false),
    ];
    for (reason_code, class, side_effect_may_have_occurred) in cases {
        let envelope = project_legacy_runtime_error(
            reason_code,
            RuntimeErrorObservation { output_emitted: false, side_effect_may_have_occurred },
            "safe error",
            "inspect diagnostics",
        )
        .expect("mapped legacy error should project");
        assert_eq!(envelope.class(), class, "{reason_code}");
        assert_eq!(envelope.reason_code(), reason_code);
        assert!(
            !envelope.retryability().allows_automatic_retry() || !side_effect_may_have_occurred
        );
    }
    assert!(matches!(
        project_legacy_runtime_error(
            "unknown.error",
            RuntimeErrorObservation { output_emitted: false, side_effect_may_have_occurred: false },
            "safe error",
            "inspect diagnostics",
        ),
        Err(RuntimeErrorValidationError::UnmappedLegacyReasonCode)
    ));
}

#[test]
fn exact_legacy_mapping_registry_is_unique_valid_and_projection_complete() {
    let mut reason_codes = BTreeSet::new();
    for mapping in LEGACY_RUNTIME_ERROR_MAPPINGS {
        assert!(reason_codes.insert(mapping.legacy_reason_code));
        assert!(validate_runtime_reason_code(mapping.legacy_reason_code).is_ok());
        let side_effect_may_have_occurred = matches!(
            mapping.class,
            RuntimeErrorClass::ToolExecutionUnknown | RuntimeErrorClass::DeliveryUnknown
        );
        let projected = project_legacy_runtime_error(
            mapping.legacy_reason_code,
            RuntimeErrorObservation { output_emitted: false, side_effect_may_have_occurred },
            "safe error",
            "inspect diagnostics",
        )
        .expect("registered legacy mapping should project");

        assert_eq!(projected.class(), mapping.class);
        assert_eq!(projected.reason_code(), mapping.legacy_reason_code);
        assert_eq!(projected.subsystem(), mapping.subsystem);
        assert_eq!(projected.phase(), mapping.phase);
        assert_eq!(projected.retryability(), mapping.retryability);
        assert_eq!(projected.security_class(), mapping.security_class);
        assert_eq!(projected.user_visibility(), mapping.user_visibility);
    }
}

#[test]
fn unmapped_legacy_reason_error_never_echoes_untrusted_input() {
    let untrusted = "Authorization: Bearer sk-secret-token from C:\\private\\stderr.log";
    let error = project_legacy_runtime_error(
        untrusted,
        RuntimeErrorObservation { output_emitted: false, side_effect_may_have_occurred: false },
        "safe error",
        "inspect diagnostics",
    )
    .expect_err("unmapped legacy input should fail closed");
    let rendered = format!("{error:?} {error}");

    assert!(!rendered.contains("sk-secret-token"));
    assert!(!rendered.contains("private"));
    assert_eq!(error, RuntimeErrorValidationError::UnmappedLegacyReasonCode);
}

#[test]
fn automatic_retry_requires_pristine_output_and_retryable_class() {
    let mut emitted = base_input();
    emitted.class = RuntimeErrorClass::ProviderRetryable;
    emitted.reason_code = "provider.recovery.retry_same_provider".to_owned();
    emitted.retryability = RuntimeRetryability::SafeSameRequest;
    emitted.output_emitted = true;
    assert!(matches!(
        RuntimeErrorEnvelopeV1::try_new(emitted),
        Err(RuntimeErrorValidationError::UnsafeRetryAfterOutput { .. })
    ));

    for class in [
        RuntimeErrorClass::InvalidRequest,
        RuntimeErrorClass::PolicyDenied,
        RuntimeErrorClass::AuthUnavailable,
        RuntimeErrorClass::ProviderTerminal,
        RuntimeErrorClass::PluginContractViolation,
        RuntimeErrorClass::InternalInvariantViolation,
    ] {
        let mut input = base_input();
        input.class = class;
        input.reason_code = "runtime.retry.invalid_class".to_owned();
        input.retryability = RuntimeRetryability::SafeAfterBackoff;
        assert!(matches!(
            RuntimeErrorEnvelopeV1::try_new(input),
            Err(RuntimeErrorValidationError::ClassRetryabilityMismatch { .. })
        ));
    }

    let mut cancelled = base_input();
    cancelled.class = RuntimeErrorClass::Cancelled;
    cancelled.reason_code = "runtime.cancelled".to_owned();
    cancelled.retryability = RuntimeRetryability::RequiresOperatorReview;
    assert!(matches!(
        RuntimeErrorEnvelopeV1::try_new(cancelled),
        Err(RuntimeErrorValidationError::ClassRetryabilityMismatch { .. })
    ));
}

#[test]
fn strict_error_projects_totally_to_frozen_public_envelope() {
    let strict = RuntimeErrorEnvelopeV1::try_new(base_input())
        .expect("valid runtime error should construct");
    let legacy = strict.to_palyra_error_envelope();

    assert_eq!(legacy.schema_version, 1);
    assert_eq!(legacy.category, PalyraErrorCategory::Provider);
    assert_eq!(legacy.code, "provider.recovery.fail_closed");
    assert!(!legacy.retryable);
    assert!(legacy.redacted);
}

#[test]
fn legacy_public_projection_uses_typed_fields_not_message_wording() {
    let context = LegacyPublicErrorProjectionContext {
        subsystem: RuntimeSubsystem::Provider,
        phase: RuntimeErrorPhase::ProviderCall,
        security_class: RuntimeErrorSecurityClass::Sensitive,
        user_visibility: RuntimeErrorUserVisibility::StatusOnly,
        observation: RuntimeErrorObservation {
            output_emitted: false,
            side_effect_may_have_occurred: false,
        },
    };
    let first = PalyraErrorEnvelope::new(
        PalyraErrorCategory::Provider,
        "provider/request_unavailable",
        "temporary failure",
        "retry later",
        true,
        true,
    );
    let second = PalyraErrorEnvelope::new(
        PalyraErrorCategory::Provider,
        "provider/request_unavailable",
        "completely different safe wording",
        "retry later",
        true,
        true,
    );

    let first = project_palyra_error_envelope(&first, context)
        .expect("typed provider error should project");
    let second = project_palyra_error_envelope(&second, context)
        .expect("typed provider error should project");
    assert_eq!(first.class(), second.class());
    assert_eq!(first.retryability(), second.retryability());
    assert_eq!(first.phase(), second.phase());
}

#[test]
fn unmapped_legacy_retryable_errors_require_conservative_backoff() {
    for (category, code) in [
        (PalyraErrorCategory::RateLimit, "provider.rate_limit.unmapped"),
        (PalyraErrorCategory::Provider, "provider.retryable.unmapped"),
        (PalyraErrorCategory::Availability, "runtime.availability.unmapped"),
    ] {
        let legacy = PalyraErrorEnvelope::new(
            category,
            code,
            "temporary failure",
            "retry after a bounded delay",
            true,
            true,
        );
        let projected = project_palyra_error_envelope(
            &legacy,
            LegacyPublicErrorProjectionContext {
                subsystem: RuntimeSubsystem::Provider,
                phase: RuntimeErrorPhase::ProviderRecovery,
                security_class: RuntimeErrorSecurityClass::Internal,
                user_visibility: RuntimeErrorUserVisibility::StatusOnly,
                observation: RuntimeErrorObservation {
                    output_emitted: false,
                    side_effect_may_have_occurred: false,
                },
            },
        )
        .expect("typed legacy retry should project");

        assert_eq!(projected.retryability(), RuntimeRetryability::SafeAfterBackoff, "{code}");
    }
}

#[test]
fn terminal_outcomes_have_stable_reason_codes_and_phases() {
    for outcome in RuntimeTerminalOutcome::ALL {
        assert!(validate_runtime_reason_code(outcome.reason_code()).is_ok());
        assert!(!outcome.phase().as_str().is_empty());
    }
}

#[test]
fn invariant_registry_is_total_unique_and_snapshot_safe() {
    assert_eq!(RUNTIME_INVARIANT_DESCRIPTORS.len(), RuntimeInvariant::ALL.len());
    let mut invariants = BTreeSet::new();
    let mut reason_codes = BTreeSet::new();
    for descriptor in RUNTIME_INVARIANT_DESCRIPTORS {
        assert!(invariants.insert(descriptor.invariant));
        assert!(reason_codes.insert(descriptor.violation_reason_code));
        assert!(validate_runtime_reason_code(descriptor.violation_reason_code).is_ok());
        assert!(!descriptor.phase.as_str().is_empty());
        assert!(!descriptor.evidence_test.is_empty());
    }
}

#[test]
fn closed_generation_requires_exactly_one_terminal_event() {
    for count in 0..=3 {
        let mut evidence = valid_invariant_evidence();
        evidence.terminal_event_count = count;
        assert_eq!(
            validate_runtime_invariant_evidence(evidence).is_ok(),
            count == 1,
            "closed generation count {count}"
        );
    }
    let mut open = valid_invariant_evidence();
    open.closed_generation = false;
    open.terminal_event_count = 0;
    assert!(validate_runtime_invariant_evidence(open).is_ok());
}

#[test]
fn active_generation_is_single_owner() {
    let error = invariant_error(valid_invariant_evidence(), |evidence| {
        evidence.active_generation_count = 2;
    });
    assert_eq!(error.violations()[0].invariant, RuntimeInvariant::OneActiveGeneration);
}

#[test]
fn unresolved_side_effect_is_not_automatically_replayed() {
    let error = invariant_error(valid_invariant_evidence(), |evidence| {
        evidence.automatic_unresolved_side_effect_attempts = 2;
    });
    assert_eq!(error.violations()[0].invariant, RuntimeInvariant::NoAutomaticDuplicateSideEffect);
}

#[test]
fn effect_requires_prior_durable_intent() {
    let error = invariant_error(valid_invariant_evidence(), |evidence| {
        evidence.durable_effect_intent = false;
    });
    assert_eq!(error.violations()[0].invariant, RuntimeInvariant::DurableIntentBeforeEffect);
}

#[test]
fn delivery_send_requires_prior_durable_intent() {
    let error = invariant_error(valid_invariant_evidence(), |evidence| {
        evidence.durable_delivery_intent = false;
    });
    assert_eq!(error.violations()[0].invariant, RuntimeInvariant::DurableDeliveryIntentBeforeSend);
}

#[test]
fn unknown_outcome_is_not_success_or_failure() {
    let error = invariant_error(valid_invariant_evidence(), |evidence| {
        evidence.outcome_unknown = true;
        evidence.unknown_outcome_marked_terminal = true;
    });
    assert_eq!(error.violations()[0].invariant, RuntimeInvariant::UnknownOutcomeDistinct);
}
