//! Regression coverage for the replay-fixture matrix and semantic comparator.

use serde_json::{json, Value};

use super::*;

#[test]
fn replay_fixture_matrix_validates_exact_baseline_and_allowances() {
    let fixture = replay_fixture_matrix();
    let report = validate_replay_fixture_matrix(&fixture).expect("replay fixture should validate");

    assert_eq!(report.schema_version, REPLAY_FIXTURE_MATRIX_SCHEMA_VERSION);
    assert_eq!(report.case_count, 8);
    assert_eq!(report.failure_case_count, 1);
    assert_eq!(report.categories.len(), 8);
    assert_eq!(report.compatibility_allowance_count, 2);
    assert_eq!(report.semantic_payload_count, 8);
    assert_eq!(report.semantic_hash_count, 8);
    assert_eq!(report.tape_event_count, 28);
    assert_eq!(report.protected_invariant_count, 136);
    assert!(report.expected_terminal_states.contains(&"cancelled".to_owned()));
    assert!(report.redaction_snapshot_present);
    assert_eq!(report.artifact_digest_count, 2);
}

#[test]
fn replay_fixture_matrix_rejects_missing_duplicate_and_extra_categories() {
    let mut missing = replay_fixture_matrix();
    let mut missing_outputs = replay_fixture_captured_outputs();
    missing["cases"].as_array_mut().expect("cases should be an array").pop();
    missing_outputs["cases"].as_array_mut().expect("cases should be an array").pop();
    assert!(validate_replay_fixture_matrix_with_captured_outputs(&missing, &missing_outputs)
        .expect_err("missing category must fail")
        .to_string()
        .contains("category mismatch"));

    let mut duplicate = replay_fixture_matrix();
    let mut duplicate_outputs = replay_fixture_captured_outputs();
    duplicate["cases"][7] = duplicate["cases"][0].clone();
    duplicate["cases"][7]["id"] = json!("duplicate_text_category");
    duplicate_outputs["cases"][7] = duplicate_outputs["cases"][0].clone();
    duplicate_outputs["cases"][7]["id"] = json!("duplicate_text_category");
    assert!(validate_replay_fixture_matrix_with_captured_outputs(&duplicate, &duplicate_outputs)
        .expect_err("duplicate category must fail")
        .to_string()
        .contains("duplicate category"));

    let mut extra = replay_fixture_matrix();
    extra["cases"][7]["category"] = json!("unversioned_extra");
    assert!(validate_replay_fixture_matrix(&extra)
        .expect_err("extra category must fail")
        .to_string()
        .contains("category is unknown"));
}

#[test]
fn replay_fixture_matrix_rejects_allowances_over_protected_invariants() {
    for forbidden_path in [
        "$.tape_events[*].seq",
        "$.qa_attempt.generation",
        "$.run.reason_code",
        "$.run.terminal_state",
        "$.idempotency_records[*].key",
        "$.tool_exchanges[*].side_effect_identity",
        "$.runtime_path.attempt_owner",
        "$.fallback_count",
    ] {
        let mut fixture = replay_fixture_matrix();
        fixture["compatibility_allowances"][0]["paths"][0] = json!(forbidden_path);
        let error = validate_replay_fixture_matrix(&fixture)
            .expect_err("protected compatibility allowance path must fail");
        assert!(
            error.to_string().contains("protected invariant"),
            "unexpected error for {forbidden_path}: {error:#}"
        );
    }
}

#[test]
fn replay_fixture_matrix_requires_the_versioned_allowance_set() {
    let mut missing = replay_fixture_matrix();
    missing["compatibility_allowances"]
        .as_array_mut()
        .expect("allowances should be an array")
        .pop();
    assert!(validate_replay_fixture_matrix(&missing)
        .expect_err("missing allowance must fail")
        .to_string()
        .contains("allowance set is incomplete"));

    let mut duplicate = replay_fixture_matrix();
    let first = duplicate["compatibility_allowances"][0].clone();
    duplicate["compatibility_allowances"]
        .as_array_mut()
        .expect("allowances should be an array")
        .push(first);
    assert!(validate_replay_fixture_matrix(&duplicate)
        .expect_err("duplicate allowance must fail")
        .to_string()
        .contains("duplicate compatibility allowance id"));

    let mut unversioned_reason = replay_fixture_matrix();
    unversioned_reason["compatibility_allowances"][0]["reason_code"] =
        json!("replay.compatibility.unversioned");
    assert!(validate_replay_fixture_matrix(&unversioned_reason)
        .expect_err("unversioned allowance reason must fail")
        .to_string()
        .contains("not part of the versioned contract"));
}

#[test]
fn replay_fixture_comparator_rejects_protected_semantic_mutations() {
    let matrix = typed_replay_fixture_matrix();
    let restart = matrix
        .cases
        .iter()
        .find(|case| case.category == "restart_terminalization")
        .expect("restart fixture should exist");
    let delivery = matrix
        .cases
        .iter()
        .find(|case| case.category == "delivery")
        .expect("delivery fixture should exist");
    let mut mutations = Vec::new();

    let mut sequence = restart.golden_payload.clone();
    sequence.tape_events[0].seq = 9;
    mutations.push((restart, sequence, "$.tape_events[0].seq"));

    let mut generation = restart.golden_payload.clone();
    generation.tape_events[1].payload["generation"] = json!(3);
    mutations.push((restart, generation, "$.tape_events[1].payload.generation"));

    let mut reason = restart.golden_payload.clone();
    reason.tape_events[1].payload["reason_code"] = json!("recovery.unversioned");
    mutations.push((restart, reason, "$.tape_events[1].payload.reason_code"));

    let mut terminal_count = restart.golden_payload.clone();
    terminal_count.tape_events[3].payload["terminal_count"] = json!(2);
    mutations.push((restart, terminal_count, "$.tape_events[3].payload.terminal_count"));

    let mut side_effect = delivery.golden_payload.clone();
    side_effect.tape_events[0].payload["side_effect_identity"] = json!("delivery:message:other");
    mutations.push((delivery, side_effect, "$.tape_events[0].payload.side_effect_identity"));

    let mut idempotency = delivery.golden_payload.clone();
    idempotency.idempotency_records[0].key = "delivery:message:other".to_owned();
    mutations.push((delivery, idempotency, "$.idempotency_records[0].key"));

    for (baseline, mutation, expected_path) in mutations {
        let error = compare_replay_fixture_payloads(
            &baseline.golden_payload,
            &mutation,
            matrix.compatibility_allowances.as_slice(),
        )
        .expect_err("protected semantic drift must fail comparison");
        assert!(
            error.to_string().contains(expected_path),
            "unexpected mismatch for {expected_path}: {error:#}"
        );
    }
}

#[test]
fn replay_fixture_comparator_applies_only_versioned_compatibility_allowances() {
    let matrix = typed_replay_fixture_matrix();
    let baseline = matrix
        .cases
        .iter()
        .find(|case| case.category == "text_run")
        .expect("text fixture should exist");
    let mut compatible = baseline.golden_payload.clone();
    compatible.generated_at_unix_ms += 10_000;
    compatible.capture.captured_at_unix_ms += 10_000;
    for event in &mut compatible.tape_events {
        if let Some(timestamp) = event.payload.get_mut("timestamp") {
            *timestamp = json!(9_999_999_999_i64);
        }
        if let Some(timestamp) = event.payload.get_mut("occurred_at_unix_ms") {
            *timestamp = json!(9_999_999_999_i64);
        }
    }
    compatible.run.prompt_tokens += 1;
    compatible.run.total_tokens += 1;

    compare_replay_fixture_payloads(
        &baseline.golden_payload,
        &compatible,
        matrix.compatibility_allowances.as_slice(),
    )
    .expect("timestamp and one-unit usage drift should be compatible");

    compatible.run.prompt_tokens += 1;
    compatible.run.total_tokens += 1;
    let error = compare_replay_fixture_payloads(
        &baseline.golden_payload,
        &compatible,
        matrix.compatibility_allowances.as_slice(),
    )
    .expect_err("usage drift beyond the versioned tolerance must fail");
    assert!(error.to_string().contains("$.run.prompt_tokens"));
}

#[test]
fn replay_fixture_gate_rejects_semantic_drift_in_production_output() {
    let fixture = replay_fixture_matrix();
    let mut captured_outputs = replay_fixture_captured_outputs();
    captured_outputs["cases"][0]["output"]["tape_events"][1]["payload"]["reason_code"] =
        json!("model.response.changed");

    let error = validate_replay_fixture_matrix_with_captured_outputs(&fixture, &captured_outputs)
        .expect_err("production semantic drift must fail the golden comparison");

    assert!(
        format!("{error:#}").contains("$.tape_events[1].payload.reason_code"),
        "unexpected semantic drift error: {error:#}"
    );
}

#[test]
fn replay_fixture_gate_allows_only_versioned_volatile_output_drift() {
    let fixture = replay_fixture_matrix();
    let mut captured_outputs = replay_fixture_captured_outputs();
    {
        let output = &mut captured_outputs["cases"][0]["output"];
        output["generated_at_unix_ms"] = json!(9_999_999_000_i64);
        output["captured_at_unix_ms"] = json!(9_999_999_001_i64);
        output["tape_events"][0]["payload"]["timestamp"] = json!(9_999_999_010_i64);
        output["tape_events"][1]["payload"]["occurred_at_unix_ms"] = json!(9_999_999_020_i64);
        output["tape_events"][2]["payload"]["timestamp"] = json!(9_999_999_030_i64);
        output["run"]["prompt_tokens"] = json!(25);
        output["run"]["total_tokens"] = json!(32);
    }

    validate_replay_fixture_matrix_with_captured_outputs(&fixture, &captured_outputs)
        .expect("versioned timestamp and one-token drift should remain compatible");

    captured_outputs["cases"][0]["output"]["run"]["prompt_tokens"] = json!(26);
    captured_outputs["cases"][0]["output"]["run"]["total_tokens"] = json!(33);
    let error = validate_replay_fixture_matrix_with_captured_outputs(&fixture, &captured_outputs)
        .expect_err("usage drift beyond the versioned tolerance must fail");
    assert!(format!("{error:#}").contains("$.run.prompt_tokens"));
}

#[test]
fn replay_fixture_matrix_recomputes_hashes_and_rejects_placeholder_digests() {
    let mut semantic_drift = replay_fixture_matrix();
    semantic_drift["cases"][0]["golden_payload"]["tape_events"][1]["payload"]["reason_code"] =
        json!("model.response.changed");
    assert!(validate_replay_fixture_matrix(&semantic_drift)
        .expect_err("semantic payload drift without a new digest must fail")
        .to_string()
        .contains("semantic sha256 mismatch"));

    let mut placeholder = replay_fixture_matrix();
    placeholder["cases"][1]["golden_payload"]["artifact_refs"][0]["sha256"] = json!("a".repeat(64));
    assert!(validate_replay_fixture_matrix(&placeholder)
        .expect_err("placeholder artifact digest must fail")
        .to_string()
        .contains("placeholder"));
}

fn replay_fixture_matrix() -> Value {
    serde_json::from_str(include_str!(
        "../../../../../fixtures/golden/replay_capture_stable_fixtures.json"
    ))
    .expect("fixture should parse")
}

fn replay_fixture_captured_outputs() -> Value {
    serde_json::from_str(include_str!(
        "../../../../../fixtures/golden/replay_capture_production_outputs.json"
    ))
    .expect("production output fixture should parse")
}

fn typed_replay_fixture_matrix() -> ReplayFixtureMatrix {
    serde_json::from_value(replay_fixture_matrix()).expect("typed fixture should parse")
}
