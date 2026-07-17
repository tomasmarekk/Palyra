//! Catalog building, schema sanitization, and tool-call intake tests for the
//! model-visible tool registry.

use super::builtin::{registry_entries, registry_entry};
use super::catalog::clear_availability_probe_cache_for_tests;
use super::reconciliation::builtin_has_explicit_execution_semantics;
use super::schema::sanitize_schema_for_provider_with_audit;
use super::types::{
    ModelVisibleToolCatalogSnapshot, ToolApprovalPosture, ToolCallRejectionKind,
    ToolCatalogFilterReasonCode, ToolParallelismPolicy, ToolRegistryEntry, ToolReplaySafetyClass,
};
use super::{
    build_model_visible_tool_catalog_snapshot, describe_catalog_tool,
    effective_tool_surface_report, projection_policy_for_tool,
    provider_tools_from_catalog_snapshot, resolve_catalog_invoke_target,
    resolve_tool_execution_semantics, search_tool_catalog_index,
    snapshot_to_provider_request_value, stable_hash_value, tool_execution_semantics,
    validate_tool_call_against_catalog_snapshot, ToolCatalogBuildRequest,
    ToolCatalogPolicySnapshot, ToolExposureSurface, ToolResultProjectionPolicy, ToolSchemaDialect,
    TOOL_CATALOG_DESCRIBE_TOOL_NAME, TOOL_CATALOG_INVOKE_TOOL_NAME, TOOL_CATALOG_SEARCH_TOOL_NAME,
};
use crate::{
    sandbox_runner::{EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier},
    tool_protocol::{ToolCallConfig, ToolRequestContext},
    wasm_plugin_runner::WasmPluginRunnerPolicy,
};
use palyra_common::{
    runtime_contracts::{
        ReconciliationStrategy, RuntimeGeneration, RuntimeIdempotencyClass, RuntimeOperationId,
        RuntimeToolExecutionId, SideEffectFenceState, SideEffectFenceV1, SideEffectRestartPolicy,
        SideEffectRetryDecision,
    },
    tool_catalog::ToolCatalogExposureMode,
};
use std::sync::{Mutex, OnceLock};

static AVAILABILITY_PROBE_CACHE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
const SCHEMA_TRANSFORM_FIXTURE: &str =
    include_str!("../../../../../fixtures/provider_compat/schema_transform_cases.json");

fn config(allowed_tools: &[&str]) -> ToolCallConfig {
    ToolCallConfig {
        allowed_tools: allowed_tools.iter().map(|tool| (*tool).to_owned()).collect(),
        max_calls_per_run: 4,
        execution_timeout_ms: 1_000,
        process_runner: SandboxProcessRunnerPolicy {
            enabled: false,
            tier: SandboxProcessRunnerTier::B,
            workspace_root: ".".into(),
            path_access_mode: crate::sandbox_runner::PathAccessMode::WorkspaceOnly,
            allowed_executables: Vec::new(),
            allow_interpreters: false,
            egress_enforcement_mode: EgressEnforcementMode::Strict,
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
            cpu_time_limit_ms: 1_000,
            memory_limit_bytes: 128 * 1024 * 1024,
            max_output_bytes: 64 * 1024,
        },
        wasm_runtime: WasmPluginRunnerPolicy {
            enabled: false,
            allow_inline_modules: false,
            max_module_size_bytes: 256 * 1024,
            fuel_budget: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_table_elements: 100_000,
            max_instances: 256,
            allowed_http_hosts: Vec::new(),
            allowed_secrets: Vec::new(),
            allowed_storage_prefixes: Vec::new(),
            allowed_channels: Vec::new(),
        },
    }
}

fn request_context() -> ToolRequestContext {
    ToolRequestContext {
        principal: "user:test".to_owned(),
        device_id: Some("device:test".to_owned()),
        channel: Some("console".to_owned()),
        session_id: Some("session".to_owned()),
        run_id: Some("run".to_owned()),
        skill_id: None,
    }
}

fn catalog_policy(config: &ToolCallConfig) -> ToolCatalogPolicySnapshot {
    ToolCatalogPolicySnapshot::direct_from_allowed_tools(config.allowed_tools.as_slice())
}

fn availability_probe_cache_test_guard() -> std::sync::MutexGuard<'static, ()> {
    AVAILABILITY_PROBE_CACHE_TEST_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("availability probe cache test lock poisoned")
}

#[test]
fn catalog_snapshot_exposes_allowlisted_tools_with_schema_hashes() {
    let config = config(&["palyra.echo", "palyra.sleep"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: Some("gpt-test"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    assert_eq!(snapshot.tools.len(), 2);
    assert!(snapshot.tools.iter().all(|tool| !tool.internal_schema_hash.is_empty()));
    assert!(snapshot.filtered_tools.iter().any(|tool| tool.name == "palyra.process.run"));
    assert!(snapshot.snapshot_id.starts_with("toolcat_"));
}

#[test]
fn registry_entries_classify_replay_safety() {
    let echo = registry_entry("palyra.echo").expect("echo entry exists");
    let sleep = registry_entry("palyra.sleep").expect("sleep entry exists");
    let process_run = registry_entry("palyra.process.run").expect("process run entry exists");
    let browser_type = registry_entry("palyra.browser.type").expect("browser type entry exists");

    let apply_patch = registry_entry("palyra.fs.apply_patch").expect("patch entry exists");
    let http_fetch = registry_entry("palyra.http.fetch").expect("http entry exists");

    assert_eq!(echo.replay_safety_class, ToolReplaySafetyClass::ReadOnly);
    assert_eq!(sleep.replay_safety_class, ToolReplaySafetyClass::IdempotentWrite);
    assert_eq!(process_run.replay_safety_class, ToolReplaySafetyClass::ExternalSideEffect);
    assert_eq!(browser_type.replay_safety_class, ToolReplaySafetyClass::ExternalSideEffect);
    assert_eq!(apply_patch.replay_safety_class, ToolReplaySafetyClass::ExternalSideEffect);
    assert_eq!(http_fetch.replay_safety_class, ToolReplaySafetyClass::ExternalSideEffect);
    assert_eq!(process_run.approval_posture, ToolApprovalPosture::ApprovalRequired);
    assert_eq!(apply_patch.approval_posture, ToolApprovalPosture::ApprovalRequired);
}

#[test]
fn replay_safety_projects_to_shared_side_effect_semantics() {
    let read_only = tool_execution_semantics("palyra.echo", ToolReplaySafetyClass::ReadOnly);
    assert_eq!(read_only.idempotency_class, RuntimeIdempotencyClass::ReadOnly);
    assert_eq!(read_only.restart_policy, SideEffectRestartPolicy::SafeRetry);
    assert_eq!(read_only.reconciliation_strategy, ReconciliationStrategy::None);

    let workspace = tool_execution_semantics(
        "palyra.fs.apply_patch",
        ToolReplaySafetyClass::ExternalSideEffect,
    );
    assert_eq!(workspace.reconciliation_strategy, ReconciliationStrategy::WorkspaceDigest);

    let process =
        tool_execution_semantics("palyra.process.run", ToolReplaySafetyClass::ExternalSideEffect);
    assert_eq!(process.reconciliation_strategy, ReconciliationStrategy::ProcessProvenance);

    let confirmation =
        tool_execution_semantics("palyra.browser.type", ToolReplaySafetyClass::ExternalSideEffect);
    assert_eq!(confirmation.idempotency_class, RuntimeIdempotencyClass::NonIdempotent);
    assert_eq!(confirmation.restart_policy, SideEffectRestartPolicy::RequireConfirmation);
    assert_eq!(confirmation.reconciliation_strategy, ReconciliationStrategy::None);

    let plugin_confirmation = tool_execution_semantics(
        "palyra.plugin.run",
        ToolReplaySafetyClass::RequiresHumanConfirmation,
    );
    assert_eq!(plugin_confirmation.idempotency_class, RuntimeIdempotencyClass::NonIdempotent);
    assert_eq!(plugin_confirmation.restart_policy, SideEffectRestartPolicy::RequireConfirmation);
}

#[test]
fn every_builtin_has_an_explicit_execution_semantics_audit_entry() {
    let missing = registry_entries()
        .into_iter()
        .filter(|entry| !builtin_has_explicit_execution_semantics(entry.name.as_str()))
        .map(|entry| entry.name)
        .collect::<Vec<_>>();

    assert!(missing.is_empty(), "builtins missing execution semantics: {missing:?}");
}

#[test]
fn input_aware_reconciler_registry_covers_major_mutation_classes() {
    let cases = [
        (
            "palyra.fs.apply_patch",
            br#"{"patch":"*** Begin Patch\n*** End Patch\n"}"#.as_slice(),
            ToolReplaySafetyClass::ExternalSideEffect,
            RuntimeIdempotencyClass::ReconciliableMutation,
            ReconciliationStrategy::WorkspaceDigest,
            false,
        ),
        (
            "palyra.process.run",
            br#"{"command":"echo"}"#.as_slice(),
            ToolReplaySafetyClass::ExternalSideEffect,
            RuntimeIdempotencyClass::ReconciliableMutation,
            ReconciliationStrategy::ProcessProvenance,
            false,
        ),
        (
            "palyra.http.fetch",
            br#"{"url":"https://example.test","method":"POST","headers":{"Idempotency-Key":"request-1"}}"#
                .as_slice(),
            ToolReplaySafetyClass::ExternalSideEffect,
            RuntimeIdempotencyClass::ExternalIdempotencyKey,
            ReconciliationStrategy::ExternalIdempotencyReceipt,
            true,
        ),
        (
            "palyra.http.fetch",
            br#"{"url":"https://example.test","method":"POST"}"#.as_slice(),
            ToolReplaySafetyClass::ExternalSideEffect,
            RuntimeIdempotencyClass::NonIdempotent,
            ReconciliationStrategy::None,
            false,
        ),
        (
            "palyra.clarify.ask",
            br#"{"question":"Continue?"}"#.as_slice(),
            ToolReplaySafetyClass::NonIdempotentWrite,
            RuntimeIdempotencyClass::ReconciliableMutation,
            ReconciliationStrategy::DeliveryAcknowledgement,
            false,
        ),
        (
            "sessions_spawn",
            br#"{"task":"inspect the workspace"}"#.as_slice(),
            ToolReplaySafetyClass::NonIdempotentWrite,
            RuntimeIdempotencyClass::ReconciliableMutation,
            ReconciliationStrategy::WorkerLeaseReceipt,
            false,
        ),
    ];

    for (tool_name, input, replay_class, idempotency, strategy, key_required) in cases {
        let resolved = resolve_tool_execution_semantics(tool_name, replay_class, input);
        resolved.semantics.validate().expect("registered semantics should validate");
        assert_eq!(resolved.semantics.idempotency_class, idempotency, "{tool_name}");
        assert_eq!(
            resolved.semantics.restart_policy,
            if idempotency == RuntimeIdempotencyClass::NonIdempotent {
                SideEffectRestartPolicy::RequireConfirmation
            } else {
                SideEffectRestartPolicy::ReconcileBeforeRetry
            },
            "{tool_name}"
        );
        assert_eq!(resolved.semantics.reconciliation_strategy, strategy, "{tool_name}");
        assert_eq!(
            resolved.semantics.external_idempotency_key_required, key_required,
            "{tool_name}"
        );
        assert_eq!(resolved.external_idempotency_key_sha256.is_some(), key_required, "{tool_name}");
    }
}

#[test]
fn mixed_operation_builtins_fence_only_mutating_inputs() {
    let read_cases = [
        ("palyra.http.fetch", br#"{"url":"https://example.test","method":"HEAD"}"#.as_slice()),
        ("palyra.fs.apply_patch", br#"{"patch":"x","dry_run":true}"#.as_slice()),
        ("palyra.fs.os_file", br#"{"operation":"read","path":"C:\\tmp\\a"}"#.as_slice()),
        (
            "palyra.fs.os_file",
            br#"{"operation":"delete_file","path":"C:\\tmp\\a","dry_run":true}"#.as_slice(),
        ),
        ("palyra.plan.manage", br#"{"operation":"read"}"#.as_slice()),
        ("palyra.browser.dialog", br#"{"session_id":"s","action":"inspect"}"#.as_slice()),
        ("palyra.browser.screenshot", br#"{"session_id":"s"}"#.as_slice()),
    ];
    for (tool_name, input) in read_cases {
        let resolved = resolve_tool_execution_semantics(
            tool_name,
            ToolReplaySafetyClass::ExternalSideEffect,
            input,
        );
        assert_eq!(
            resolved.semantics.idempotency_class,
            RuntimeIdempotencyClass::ReadOnly,
            "{tool_name}"
        );
        assert!(resolved.external_idempotency_key_sha256.is_none(), "{tool_name}");
    }

    let mutation_cases = [
        ("palyra.fs.os_file", br#"{"operation":"delete_file","path":"C:\\tmp\\a"}"#.as_slice()),
        ("palyra.plan.manage", br#"{"operation":"complete","item_id":"i"}"#.as_slice()),
        ("palyra.browser.dialog", br#"{"session_id":"s","action":"accept"}"#.as_slice()),
        (
            "palyra.browser.screenshot",
            br#"{"session_id":"s","output_path":"capture.png"}"#.as_slice(),
        ),
    ];
    for (tool_name, input) in mutation_cases {
        let resolved = resolve_tool_execution_semantics(
            tool_name,
            ToolReplaySafetyClass::ExternalSideEffect,
            input,
        );
        assert_eq!(
            resolved.semantics.idempotency_class,
            RuntimeIdempotencyClass::NonIdempotent,
            "{tool_name}"
        );
        assert_eq!(
            resolved.semantics.restart_policy,
            SideEffectRestartPolicy::RequireConfirmation,
            "{tool_name}"
        );
    }
}

#[test]
fn ambiguous_http_idempotency_keys_fail_closed_to_confirmation() {
    let input = br#"{
        "url":"https://example.test",
        "method":"POST",
        "headers":{"Idempotency-Key":"first","idempotency-key":"second"}
    }"#;

    let resolved = resolve_tool_execution_semantics(
        "palyra.http.fetch",
        ToolReplaySafetyClass::ExternalSideEffect,
        input,
    );

    assert_eq!(resolved.semantics.idempotency_class, RuntimeIdempotencyClass::NonIdempotent);
    assert_eq!(resolved.semantics.restart_policy, SideEffectRestartPolicy::RequireConfirmation);
    assert!(resolved.external_idempotency_key_sha256.is_none());
}

#[test]
fn effect_unknown_never_blindly_retries_a_registered_mutation() {
    let cases = [
        (
            "palyra.fs.apply_patch",
            br#"{"patch":"*** Begin Patch\n*** End Patch\n"}"#.as_slice(),
            SideEffectRetryDecision::ReconciliationRequired,
        ),
        (
            "palyra.process.run",
            br#"{"command":"echo"}"#.as_slice(),
            SideEffectRetryDecision::ReconciliationRequired,
        ),
        (
            "palyra.http.fetch",
            br#"{"url":"https://example.test","method":"POST","headers":{"idempotency-key":"request-1"}}"#
                .as_slice(),
            SideEffectRetryDecision::ReconciliationRequired,
        ),
        (
            "palyra.http.fetch",
            br#"{"url":"https://example.test","method":"POST"}"#.as_slice(),
            SideEffectRetryDecision::ConfirmationRequired,
        ),
        (
            "palyra.clarify.ask",
            br#"{"question":"Continue?"}"#.as_slice(),
            SideEffectRetryDecision::ReconciliationRequired,
        ),
        (
            "sessions_spawn",
            br#"{"task":"inspect"}"#.as_slice(),
            SideEffectRetryDecision::ReconciliationRequired,
        ),
        (
            "palyra.browser.click",
            br#"{"session_id":"s","selector":"button"}"#.as_slice(),
            SideEffectRetryDecision::ConfirmationRequired,
        ),
    ];

    for (tool_name, input, expected) in cases {
        let resolved = resolve_tool_execution_semantics(
            tool_name,
            ToolReplaySafetyClass::ExternalSideEffect,
            input,
        );
        let fence = SideEffectFenceV1 {
            schema_version: 1,
            operation_id: RuntimeOperationId::parse("operation_01").expect("operation id"),
            tool_execution_id: RuntimeToolExecutionId::parse("execution_01").expect("execution id"),
            intent_generation: RuntimeGeneration::new(1).expect("generation"),
            observed_generation: RuntimeGeneration::new(1).expect("generation"),
            intent_sha256: "a".repeat(64),
            state: SideEffectFenceState::EffectUnknown,
            semantics: resolved.semantics,
            external_idempotency_key_sha256: resolved.external_idempotency_key_sha256,
            evidence_sha256: None,
            reason_code: "tool.effect.ack_unknown".to_owned(),
            updated_at_unix_ms: 1,
        };
        fence.validate().expect("uncertain fence should validate");
        assert_eq!(fence.retry_decision(), expected, "{tool_name}");
    }
}

proptest::proptest! {
    #[test]
    fn keyed_http_semantics_persist_only_a_deterministic_key_digest(
        key in "[A-Za-z0-9_-]{1,64}"
    ) {
        let input = serde_json::json!({
            "url": "https://example.test",
            "method": "POST",
            "headers": {"IDEMPOTENCY-KEY": key.clone()}
        });
        let input = serde_json::to_vec(&input).expect("input should serialize");

        let resolved = resolve_tool_execution_semantics(
            "palyra.http.fetch",
            ToolReplaySafetyClass::ExternalSideEffect,
            input.as_slice(),
        );

        let digest = resolved
            .external_idempotency_key_sha256
            .expect("keyed mutation should bind a digest");
        let expected_digest = crate::sha256_hex(key.as_bytes());
        proptest::prop_assert_eq!(digest.as_str(), expected_digest.as_str());
        proptest::prop_assert_ne!(digest.as_str(), key.as_str());
        proptest::prop_assert!(resolved.semantics.external_idempotency_key_required);
        proptest::prop_assert_eq!(
            resolved.semantics.reconciliation_strategy,
            ReconciliationStrategy::ExternalIdempotencyReceipt
        );
    }
}

#[test]
fn catalog_snapshot_projects_replay_safety_to_tools_and_index() {
    let config = config(&["palyra.echo", "palyra.sleep"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: Some("gpt-test"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let sleep_tool = snapshot
        .tools
        .iter()
        .find(|tool| tool.name == "palyra.sleep")
        .expect("sleep should be visible");
    let sleep_index = snapshot
        .index
        .entries
        .iter()
        .find(|entry| entry.name == "palyra.sleep")
        .expect("sleep should be indexed");

    assert_eq!(sleep_tool.replay_safety_class, ToolReplaySafetyClass::IdempotentWrite);
    assert_eq!(sleep_index.replay_safety_class, ToolReplaySafetyClass::IdempotentWrite);
    assert!(serde_json::to_value(&snapshot)
        .expect("snapshot should serialize")
        .to_string()
        .contains("idempotent_write"));
}

#[test]
fn availability_probe_cache_uses_ttl_and_last_good_browser_grace() {
    let _guard = availability_probe_cache_test_guard();
    clear_availability_probe_cache_for_tests();
    let config = config(&["palyra.browser.navigate", "palyra.test.browser_probe_cache"]);

    let available = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: true,
        browser_service_configured: true,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 1_000,
    });
    let available_probe = availability_probe(&available, "browser_service");
    assert_eq!(available_probe.status, "available");
    assert_eq!(available_probe.cache_status, "refreshed");
    assert_eq!(available_probe.last_good_unix_ms, Some(1_000));
    assert!(available.tools.iter().any(|tool| tool.name == "palyra.browser.navigate"));

    let cached = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: true,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 2_000,
    });
    let cached_probe = availability_probe(&cached, "browser_service");
    assert_eq!(cached_probe.status, "available");
    assert_eq!(cached_probe.cache_status, "cached");
    assert!(cached.tools.iter().any(|tool| tool.name == "palyra.browser.navigate"));

    let grace = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: true,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 31_001,
    });
    let grace_probe = availability_probe(&grace, "browser_service");
    assert_eq!(grace_probe.status, "last_good_grace");
    assert_eq!(grace_probe.last_good_unix_ms, Some(1_000));
    assert_eq!(grace_probe.last_good_grace_until_unix_ms, Some(121_000));
    assert!(grace.tools.iter().any(|tool| tool.name == "palyra.browser.navigate"));

    let expired = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: true,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 121_001,
    });
    let expired_probe = availability_probe(&expired, "browser_service");
    assert_eq!(expired_probe.status, "unavailable");
    assert!(expired.filtered_tools.iter().any(|tool| {
        tool.name == "palyra.browser.navigate"
            && tool.reason_code == ToolCatalogFilterReasonCode::RuntimeUnavailable
    }));
}

#[test]
fn availability_probe_cache_invalidates_high_risk_process_on_config_change() {
    let _guard = availability_probe_cache_test_guard();
    clear_availability_probe_cache_for_tests();
    let mut config = config(&["palyra.process.run", "palyra.test.process_probe_cache"]);
    config.process_runner.enabled = true;
    let enabled_snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 10_000,
    });
    assert_eq!(availability_probe(&enabled_snapshot, "process_runner").status, "available");
    assert!(enabled_snapshot.tools.iter().any(|tool| tool.name == "palyra.process.run"));

    config.process_runner.enabled = false;
    let disabled_snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 10_001,
    });
    let disabled_probe = availability_probe(&disabled_snapshot, "process_runner");
    assert_eq!(disabled_probe.status, "unavailable");
    assert!(!disabled_probe.grace_allowed);
    assert_eq!(disabled_probe.last_good_unix_ms, None);
    assert!(disabled_snapshot.filtered_tools.iter().any(|tool| {
        tool.name == "palyra.process.run"
            && tool.reason_code == ToolCatalogFilterReasonCode::RuntimeUnavailable
    }));
}

fn availability_probe<'a>(
    snapshot: &'a ModelVisibleToolCatalogSnapshot,
    runtime: &str,
) -> &'a super::types::AvailabilityProbeResult {
    snapshot
        .availability_probes
        .iter()
        .find(|probe| probe.runtime == runtime)
        .expect("runtime probe should be present")
}

#[test]
fn provider_payload_projects_native_openai_tools() {
    let config = config(&["palyra.echo"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });
    let payload = snapshot_to_provider_request_value(&snapshot);
    let tools = provider_tools_from_catalog_snapshot(&payload, ToolSchemaDialect::OpenAiCompatible);

    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["type"], "function");
    assert_eq!(tools[0]["function"]["name"], "palyra.echo");
    assert_eq!(tools[0]["function"]["parameters"]["type"], "object");
}

#[test]
fn compact_catalog_exposes_bridge_tools_and_indexes_authorized_targets() {
    let config = config(&["palyra.echo", "palyra.sleep"]);
    let mut policy = catalog_policy(&config);
    policy.exposure_mode = ToolCatalogExposureMode::Compact;
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &policy,
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });
    let exposed = snapshot.tools.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>();

    assert_eq!(
        exposed,
        vec![
            TOOL_CATALOG_DESCRIBE_TOOL_NAME,
            TOOL_CATALOG_INVOKE_TOOL_NAME,
            TOOL_CATALOG_SEARCH_TOOL_NAME,
        ]
    );
    assert!(snapshot.index.entries.iter().any(|entry| entry.name == "palyra.echo"));
    assert!(snapshot.indexed_tools.iter().any(|tool| tool.name == "palyra.sleep"));
    assert!(snapshot.estimated_direct_tool_bytes > 0);
    assert!(snapshot.estimated_exposed_tool_bytes > 0);
    assert!(snapshot.filtered_tools.iter().any(|tool| {
        tool.name == "palyra.echo" && tool.reason_code.as_str() == "policy_invisible"
    }));
}

#[test]
fn catalog_bridge_search_describe_and_invoke_use_current_index_digest() {
    let config = config(&["palyra.echo"]);
    let mut policy = catalog_policy(&config);
    policy.exposure_mode = ToolCatalogExposureMode::Compact;
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &policy,
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let search = search_tool_catalog_index(&snapshot, br#"{"query":"echo text","limit":3}"#)
        .expect("search should return indexed echo");
    assert_eq!(search["index_digest"], snapshot.index.index_digest);
    assert_eq!(search["results"][0]["id"], "palyra.echo");
    assert_eq!(search["results"][0]["exposure_reason"], "allowlisted_policy_visible");
    assert_eq!(search["results"][0]["projection_policy"], "inline_unless_large");
    assert!(search["results"][0]["repair_hint"]
        .as_str()
        .is_some_and(|hint| hint.contains("provider_schema")));
    let schema_digest = search["results"][0]["schema_digest"].as_str().expect("schema digest");

    let describe = describe_catalog_tool(
        &snapshot,
        format!(r#"{{"tool_id":"palyra.echo","schema_digest":"{schema_digest}"}}"#).as_bytes(),
    )
    .expect("describe should return provider schema");
    assert_eq!(describe["tool_id"], "palyra.echo");
    assert_eq!(describe["provider_schema"]["type"], "object");
    assert_eq!(describe["exposure_reason"], "allowlisted_policy_visible");
    assert!(describe["repair_hint"].as_str().is_some_and(|hint| hint.contains("provider_schema")));

    let invoke = resolve_catalog_invoke_target(
        &snapshot,
        format!(
            r#"{{"tool_id":"palyra.echo","schema_digest":"{schema_digest}","arguments":{{"text":"hello"}}}}"#
        )
        .as_bytes(),
    )
    .expect("invoke target should resolve");
    assert_eq!(invoke.tool_name, "palyra.echo");
    assert_eq!(invoke.schema_digest, schema_digest);
}

#[test]
fn effective_tool_surface_report_explains_visible_compact_and_denied_tools() {
    let config = config(&["palyra.echo", "palyra.unknown"]);
    let mut policy = catalog_policy(&config);
    policy.exposure_mode = ToolCatalogExposureMode::Compact;
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &policy,
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let report = effective_tool_surface_report(&snapshot);
    assert_eq!(report["catalog_hash"].as_str(), Some(snapshot.catalog_hash.as_str()));
    let entries = report["entries"].as_array().expect("entries should be present");
    let echo = entries
        .iter()
        .find(|entry| entry["name"] == "palyra.echo")
        .expect("echo surface entry should exist");
    assert_eq!(echo["status"], "compact_only");
    assert_eq!(echo["reason_code"], "policy_invisible");
    assert!(echo["provider_schema_hash"].as_str().is_some_and(|hash| hash.len() == 64));

    let unknown = entries
        .iter()
        .find(|entry| entry["name"] == "palyra.unknown")
        .expect("unknown allowlist typo should be diagnosed");
    assert_eq!(unknown["status"], "denied");
    assert_eq!(unknown["reason_code"], "unknown_tool");
    assert!(unknown["provider_schema_hash"].is_null());
}

#[test]
fn intake_strict_preview_repairs_standalone_json_object_wrapper() {
    let config = config(&["palyra.echo"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.echo",
        b"```json\n{\"text\":\"hello\"}\n```",
    )
    .expect("strict preview repair should accept standalone fenced object");

    assert_eq!(normalized.audit.steps[0].reason_code, "tool_call.arguments.strict_preview_repair");
    assert_eq!(normalized.input_json, br#"{"text":"hello"}"#);
}

fn echo_snapshot() -> ModelVisibleToolCatalogSnapshot {
    let config = config(&["palyra.echo"]);
    build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    })
}

#[test]
fn intake_repairs_stringified_json_arguments() {
    let snapshot = echo_snapshot();

    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.echo",
        b"\"{\\\"text\\\":\\\"hello\\\"}\"",
    )
    .expect("stringified JSON object should repair");

    assert_eq!(
        normalized.audit.steps[0].reason_code,
        "tool_call.arguments.stringified_json_object_repair"
    );
    assert_eq!(normalized.input_json, br#"{"text":"hello"}"#);
}

#[test]
fn intake_repairs_single_object_array_arguments() {
    let snapshot = echo_snapshot();

    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.echo",
        br#"[{"text":"hello"}]"#,
    )
    .expect("single object array should repair");

    assert_eq!(
        normalized.audit.steps[0].reason_code,
        "tool_call.arguments.single_object_array_repair"
    );
    assert_eq!(normalized.input_json, br#"{"text":"hello"}"#);
}

#[test]
fn intake_repairs_unambiguous_truncated_json_arguments() {
    let snapshot = echo_snapshot();

    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.echo",
        b"{\"text\":\"hello\"",
    )
    .expect("missing final object closer should repair");

    assert_eq!(normalized.audit.steps[0].reason_code, "tool_call.arguments.truncated_json_repair");
    assert_eq!(normalized.input_json, br#"{"text":"hello"}"#);
}

#[test]
fn intake_repairs_trailing_comma_and_smart_quote_json() {
    let snapshot = echo_snapshot();

    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.echo",
        "{\"text\":\"hello\",}".as_bytes(),
    )
    .expect("trailing comma should repair after schema validation");
    assert_eq!(normalized.audit.steps[0].reason_code, "tool_call.arguments.trailing_comma_repair");
    assert_eq!(normalized.input_json, br#"{"text":"hello"}"#);

    let smart_quote_input = "\u{201c}text\u{201d}:\u{201c}hello\u{201d}";
    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.echo",
        format!("{{{smart_quote_input},}}").as_bytes(),
    )
    .expect("smart quotes plus trailing comma should stay within repair budget");
    let reason_codes =
        normalized.audit.steps.iter().map(|step| step.reason_code.as_str()).collect::<Vec<_>>();
    assert_eq!(
        reason_codes,
        vec!["tool_call.arguments.smart_quote_repair", "tool_call.arguments.trailing_comma_repair"]
    );
    assert_eq!(normalized.input_json, br#"{"text":"hello"}"#);
}

#[test]
fn intake_repair_still_requires_schema_validation() {
    let snapshot = echo_snapshot();

    let rejection = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.echo",
        "{\"unknown\":\"hello\",}".as_bytes(),
    )
    .expect_err("repaired JSON must still match the visible tool schema");

    assert_eq!(rejection.kind, ToolCallRejectionKind::MalformedArguments);
    assert_eq!(rejection.reason_code, "tool_call.arguments.schema_mismatch");
}

#[test]
fn unknown_tool_arguments_are_not_syntax_repaired() {
    let snapshot = echo_snapshot();

    let rejection = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.unknown",
        "{\"text\":\"hello\",}".as_bytes(),
    )
    .expect_err("unknown tool passthrough must not repair malformed JSON");

    assert_eq!(rejection.kind, ToolCallRejectionKind::MalformedArguments);
    assert_eq!(rejection.reason_code, "tool_call.arguments.invalid_json");
}

#[test]
fn intake_rejects_ambiguous_argument_repairs() {
    let snapshot = echo_snapshot();

    let array_rejection = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.echo",
        br#"[{"text":"one"},{"text":"two"}]"#,
    )
    .expect_err("multi-object array should be ambiguous");
    assert_eq!(array_rejection.reason_code, "tool_call.arguments.not_object");

    let bad_json_rejection =
        validate_tool_call_against_catalog_snapshot(&snapshot, "palyra.echo", b"{\"text\":")
            .expect_err("missing value cannot repair safely");
    assert_eq!(bad_json_rejection.kind, ToolCallRejectionKind::MalformedArguments);
    assert_eq!(bad_json_rejection.reason_code, "tool_call.arguments.invalid_json");
}

#[test]
fn process_run_allowlist_exposes_lifecycle_controls() {
    let mut config = config(&["palyra.process.run"]);
    config.process_runner.enabled = true;
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });
    let tool_names = snapshot.tools.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>();

    assert!(
        tool_names.contains(&"palyra.process.run"),
        "process tools should be visible, filtered={:?}",
        snapshot
            .filtered_tools
            .iter()
            .filter(|tool| tool.name.starts_with("palyra.process."))
            .collect::<Vec<_>>()
    );
    assert!(tool_names.contains(&"palyra.process.stop"));
    assert!(tool_names.contains(&"palyra.process.status"));
    assert!(tool_names.contains(&"palyra.process.list"));
    assert!(tool_names.contains(&"palyra.process.input"));
    assert!(tool_names.contains(&"palyra.process.send_keys"));
}

#[test]
fn process_run_schema_hides_requested_egress_hosts_when_profile_cannot_enforce_them() {
    let mut config = config(&["palyra.process.run"]);
    config.process_runner.enabled = true;
    config.process_runner.egress_enforcement_mode = EgressEnforcementMode::None;
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });
    let process_run =
        snapshot.tools.iter().find(|tool| tool.name == "palyra.process.run").expect("tool");

    assert!(process_run.schema["properties"].get("requested_egress_hosts").is_none());
    assert!(process_run.provider_schema["properties"].get("requested_egress_hosts").is_none());
    assert!(
        process_run.description.contains("egress profile is 'none'")
            && process_run.description.contains("palyra.http.fetch")
            && process_run.description.contains("egress_enforcement_mode='preflight'"),
        "{}",
        process_run.description
    );
}

#[test]
fn process_run_schema_keeps_requested_egress_hosts_in_preflight_profile() {
    let mut config = config(&["palyra.process.run"]);
    config.process_runner.enabled = true;
    config.process_runner.egress_enforcement_mode = EgressEnforcementMode::Preflight;
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });
    let process_run =
        snapshot.tools.iter().find(|tool| tool.name == "palyra.process.run").expect("tool");

    assert!(process_run.schema["properties"].get("requested_egress_hosts").is_some());
    assert!(process_run.provider_schema["properties"].get("requested_egress_hosts").is_some());
}

#[test]
fn plugin_run_visibility_tracks_wasm_runtime_policy() {
    let mut config = config(&["palyra.plugin.run"]);
    let disabled_snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    assert!(
        disabled_snapshot.tools.iter().all(|tool| tool.name != "palyra.plugin.run"),
        "plugin runner must not be model-visible when the WASM runtime is disabled"
    );
    assert!(
        disabled_snapshot.filtered_tools.iter().any(|tool| {
            tool.name == "palyra.plugin.run" && tool.reason_code.as_str() == "runtime_unavailable"
        }),
        "disabled WASM runtime should explain why plugin execution is hidden"
    );

    config.wasm_runtime.enabled = true;
    let enabled_snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 43,
    });

    assert!(
        enabled_snapshot.tools.iter().any(|tool| tool.name == "palyra.plugin.run"),
        "plugin runner should be model-visible when allowlisted and enabled"
    );
}

#[test]
fn anthropic_catalog_exposes_http_fetch_with_boolean_additional_properties() {
    let config = config(&["palyra.http.fetch"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let http_fetch = snapshot
        .tools
        .iter()
        .find(|tool| tool.name == "palyra.http.fetch")
        .expect("http fetch should stay visible for Anthropic-compatible providers");
    assert!(
        !snapshot.filtered_tools.iter().any(|tool| {
            tool.name == "palyra.http.fetch"
                && tool.reason_code.as_str() == "provider_schema_incompatible"
        }),
        "http fetch must not be filtered for schema dialect incompatibility"
    );
    assert_eq!(
        http_fetch.provider_schema["properties"]["headers"]["additionalProperties"],
        serde_json::Value::Bool(true)
    );
    assert_eq!(
        http_fetch.provider_schema["properties"]["credential_bindings"]["items"]["properties"]
            ["secret_ref"]["properties"]["kind"]["enum"][0],
        serde_json::Value::String("vault".to_owned())
    );
    assert_eq!(
        http_fetch.provider_schema["properties"]["credential_bindings"]["items"]
            ["additionalProperties"],
        serde_json::Value::Bool(false)
    );

    let payload = snapshot_to_provider_request_value(&snapshot);
    let tools = provider_tools_from_catalog_snapshot(&payload, ToolSchemaDialect::Anthropic);
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "palyra.http.fetch");
    assert_eq!(
        tools[0]["input_schema"]["properties"]["headers"]["additionalProperties"],
        serde_json::Value::Bool(true)
    );
}

#[test]
fn anthropic_catalog_exposes_browser_observe_without_default_keywords() {
    let config = config(&["palyra.browser.observe"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: true,
        browser_service_configured: true,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let observe = snapshot
        .tools
        .iter()
        .find(|tool| tool.name == "palyra.browser.observe")
        .expect("browser observe should stay visible for Anthropic-compatible providers");
    assert!(
        !snapshot.filtered_tools.iter().any(|tool| {
            tool.name == "palyra.browser.observe"
                && tool.reason_code.as_str() == "provider_schema_incompatible"
        }),
        "browser observe must not be filtered for schema dialect incompatibility"
    );
    assert_eq!(
        observe.provider_schema["properties"]["include_visible_text"]["type"],
        serde_json::Value::String("boolean".to_owned())
    );
    assert!(observe.provider_schema["properties"]["include_visible_text"].get("default").is_none());

    let payload = snapshot_to_provider_request_value(&snapshot);
    let tools = provider_tools_from_catalog_snapshot(&payload, ToolSchemaDialect::Anthropic);
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "palyra.browser.observe");
}

#[test]
fn anthropic_catalog_exposes_browser_viewport_without_exclusive_bounds() {
    let config = config(&["palyra.browser.viewport"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: true,
        browser_service_configured: true,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let viewport = snapshot
        .tools
        .iter()
        .find(|tool| tool.name == "palyra.browser.viewport")
        .expect("browser viewport should stay visible for Anthropic-compatible providers");
    assert!(
        !snapshot.filtered_tools.iter().any(|tool| {
            tool.name == "palyra.browser.viewport"
                && tool.reason_code.as_str() == "provider_schema_incompatible"
        }),
        "browser viewport must not be filtered for schema dialect incompatibility"
    );
    let device_scale_factor = &viewport.provider_schema["properties"]["device_scale_factor"];
    assert!(device_scale_factor.get("exclusiveMinimum").is_none());
    assert_eq!(device_scale_factor.get("minimum").and_then(serde_json::Value::as_i64), Some(0));
    assert!(device_scale_factor.get("default").is_none());

    let payload = snapshot_to_provider_request_value(&snapshot);
    let tools = provider_tools_from_catalog_snapshot(&payload, ToolSchemaDialect::Anthropic);
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "palyra.browser.viewport");
}

#[test]
fn anthropic_catalog_exposes_routines_control_trigger_payload() {
    let config = config(&["palyra.routines.control"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let control = snapshot
        .tools
        .iter()
        .find(|tool| tool.name == "palyra.routines.control")
        .expect("routines control should stay visible for Anthropic-compatible providers");
    assert!(
        !snapshot.filtered_tools.iter().any(|tool| {
            tool.name == "palyra.routines.control"
                && tool.reason_code.as_str() == "provider_schema_incompatible"
        }),
        "routines control must not be filtered for schema dialect incompatibility"
    );
    assert_eq!(
        control.provider_schema["properties"]["trigger_payload"]["additionalProperties"],
        serde_json::Value::Bool(true)
    );

    let payload = snapshot_to_provider_request_value(&snapshot);
    let tools = provider_tools_from_catalog_snapshot(&payload, ToolSchemaDialect::Anthropic);
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "palyra.routines.control");
}

#[test]
fn schema_transform_fixture_cases_match_expected_provider_schema() {
    let fixture: serde_json::Value =
        serde_json::from_str(SCHEMA_TRANSFORM_FIXTURE).expect("schema transform fixture parses");
    let cases = fixture["cases"].as_array().expect("fixture cases should be an array");

    for case in cases {
        let dialect = match case["dialect"].as_str().expect("case dialect should be a string") {
            "anthropic" => ToolSchemaDialect::Anthropic,
            "deterministic" => ToolSchemaDialect::Deterministic,
            _ => ToolSchemaDialect::OpenAiCompatible,
        };
        let input = &case["input"];
        let (provider_schema, audit) = sanitize_schema_for_provider_with_audit(input, dialect)
            .unwrap_or_else(|error| {
                panic!("case {} should transform: {}", case["id"], error.message)
            });
        assert_eq!(
            provider_schema, case["expected_provider_schema"],
            "case {} should match expected provider schema",
            case["id"]
        );
        let reasons = audit.steps.iter().map(|step| step.reason_code.as_str()).collect::<Vec<_>>();
        let expected_reasons = case["expected_step_reason_codes"]
            .as_array()
            .expect("case expected reasons should be an array")
            .iter()
            .map(|value| value.as_str().expect("reason should be a string"))
            .collect::<Vec<_>>();
        assert_eq!(reasons, expected_reasons, "case {} should match audit steps", case["id"]);
        assert_eq!(audit.input_schema_hash, stable_hash_value(input));
        assert_eq!(audit.output_schema_hash, stable_hash_value(&provider_schema));
    }
}

#[test]
fn schema_transform_rejects_ambiguous_composition() {
    let schema = serde_json::json!({
        "oneOf": [
            {"type":"object","properties":{"text":{"type":"string"}},"required":["text"]},
            {"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}
        ]
    });

    let error =
        sanitize_schema_for_provider_with_audit(&schema, ToolSchemaDialect::OpenAiCompatible)
            .expect_err("ambiguous oneOf should fail closed");

    assert_eq!(error.reason_code, "schema.one_of_ambiguous");
}

#[test]
fn provider_request_hash_tracks_transformed_schema_payload() {
    let input_schema = serde_json::json!({
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "text": {"type": ["string", "null"]}
                },
                "required": ["text"],
                "additionalProperties": false
            },
            {"type":"null"}
        ]
    });
    let tool_name = "palyra.test.schema_transform";
    let config = config(&[tool_name]);
    let external_tool = schema_transform_test_tool(tool_name, input_schema);
    let snapshot = super::build_model_visible_tool_catalog_snapshot_with_external_tools(
        ToolCatalogBuildRequest {
            config: &config,
            catalog_policy: &catalog_policy(&config),
            browser_service_enabled: false,
            browser_service_configured: false,
            request_context: &request_context(),
            provider_kind: "openai_compatible",
            provider_model_id: Some("gpt-test"),
            surface: ToolExposureSurface::RunStream,
            remaining_tool_budget: None,
            created_at_unix_ms: 42,
        },
        &[external_tool],
    );
    let tool = snapshot
        .tools
        .iter()
        .find(|tool| tool.name == tool_name)
        .expect("schema transform tool should be visible");

    assert_eq!(tool.provider_schema_transform.input_schema_hash, tool.internal_schema_hash);
    assert_eq!(tool.provider_schema_transform.output_schema_hash, tool.provider_schema_hash);

    let payload = snapshot_to_provider_request_value(&snapshot);
    let request_hash = stable_hash_value(&payload);
    let mut mutated_payload = payload.clone();
    mutated_payload["tools"][0]["provider_schema"]["properties"]["text"]["maxLength"] =
        serde_json::json!(12);
    assert_ne!(request_hash, stable_hash_value(&mutated_payload));
}

fn schema_transform_test_tool(
    tool_name: &str,
    input_schema: serde_json::Value,
) -> ToolRegistryEntry {
    ToolRegistryEntry {
        name: tool_name.to_owned(),
        description: "Schema transform fixture tool".to_owned(),
        version: 1,
        provenance: "test".to_owned(),
        schema_hash: stable_hash_value(&input_schema),
        input_schema,
        capabilities: vec!["test".to_owned()],
        approval_posture: ToolApprovalPosture::Safe,
        projection_policy: ToolResultProjectionPolicy::InlineUnlessLarge,
        parallelism_policy: ToolParallelismPolicy::ReadOnly,
        replay_safety_class: ToolReplaySafetyClass::ReadOnly,
        target_surfaces: vec![ToolExposureSurface::RunStream],
    }
}

#[test]
fn browser_session_lifecycle_returns_model_visible_handles() {
    assert_eq!(
        projection_policy_for_tool("palyra.browser.session.create"),
        ToolResultProjectionPolicy::InlineUnlessLarge
    );
    assert_eq!(
        projection_policy_for_tool("palyra.browser.session.close"),
        ToolResultProjectionPolicy::InlineUnlessLarge
    );
    assert_eq!(
        projection_policy_for_tool("palyra.fs.read_file"),
        ToolResultProjectionPolicy::InlineUnlessLarge
    );
    assert_eq!(
        projection_policy_for_tool("palyra.fs.list_dir"),
        ToolResultProjectionPolicy::InlineUnlessLarge
    );
    assert_eq!(
        projection_policy_for_tool("palyra.fs.search"),
        ToolResultProjectionPolicy::InlineUnlessLarge
    );
    assert_eq!(
        projection_policy_for_tool("palyra.browser.observe"),
        ToolResultProjectionPolicy::RedactedPreviewAndArtifact
    );
}

#[test]
fn browser_session_create_schema_discourages_invented_profile_ids() {
    let entry = registry_entry("palyra.browser.session.create").expect("browser create tool entry");

    assert!(entry.input_schema["properties"]["profile_id"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("do not invent labels"));
    assert!(
        entry.input_schema["properties"].get("persistence_id").is_none(),
        "browser persistence ids are runtime-scoped and must not be model-callable"
    );
}

#[test]
fn workspace_file_schemas_accept_workspace_root_override() {
    let read_file = registry_entry("palyra.fs.read_file").expect("read file entry exists");
    let list_dir = registry_entry("palyra.fs.list_dir").expect("list dir entry exists");
    let search = registry_entry("palyra.fs.search").expect("search entry exists");

    assert_eq!(read_file.input_schema["properties"]["workspace_root"]["type"], "string");
    assert_eq!(list_dir.input_schema["properties"]["workspace_root"]["type"], "string");
    assert_eq!(search.input_schema["properties"]["workspace_root"]["type"], "string");
    assert_eq!(search.input_schema["properties"]["query"]["maxLength"], 512);
    assert!(read_file.input_schema["properties"]["workspace_root"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("prior apply_patch"));
    assert!(list_dir.input_schema["properties"]["workspace_root"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("nested project"));
    assert!(search.description.contains("public API renames"));
    assert!(search.input_schema["properties"]["query"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("not a regular expression"));
}

#[test]
fn browser_observe_schema_exposes_visible_text_default() {
    let entry = registry_entry("palyra.browser.observe").expect("browser observe tool entry");

    assert_eq!(entry.input_schema["properties"]["include_visible_text"]["type"], "boolean");
    assert_eq!(entry.input_schema["properties"]["include_visible_text"]["default"], true);
    assert!(
        entry.description.contains("redacted form/storage metadata"),
        "observe tool description should advertise redacted form/storage evidence"
    );
    assert!(
        entry.input_schema["properties"]["include_visible_text"]["description"]
            .as_str()
            .unwrap_or_default()
            .contains("local/session storage key names"),
        "visible text schema should mention redacted storage key summaries"
    );
    assert_eq!(entry.input_schema["properties"]["max_visible_text_bytes"]["minimum"], 0);
}

#[test]
fn browser_binary_artifact_tools_expose_output_path() {
    for tool_name in
        ["palyra.browser.screenshot", "palyra.browser.pdf", "palyra.browser.downloads.get"]
    {
        let entry = registry_entry(tool_name).expect("browser binary artifact tool should exist");
        let description = entry.input_schema["properties"]["output_path"]["description"]
            .as_str()
            .unwrap_or_default();
        assert!(
            description.contains("workspace-relative") && description.contains("absolute OS path"),
            "{tool_name} should expose a first-class binary save path"
        );
    }
}

#[test]
fn routines_control_schema_discourages_slug_ids_and_short_intervals() {
    let entry = registry_entry("palyra.routines.control").expect("routines control tool entry");

    assert!(
        entry.description.contains("omit routine_id"),
        "description should tell models not to invent human routine ids"
    );
    assert!(entry.input_schema["properties"]["routine_id"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("do not put human slugs here"));
    assert!(
        entry.description.contains("operation=delete"),
        "description should expose cleanup deletion to agents"
    );
    assert!(
        entry.input_schema["properties"]["operation"]["enum"]
            .as_array()
            .expect("operation enum should be an array")
            .iter()
            .any(|value| value.as_str() == Some("delete")),
        "operation enum should include delete"
    );
    assert_eq!(entry.input_schema["properties"]["every_interval_ms"]["minimum"], 30_000);
    assert!(entry.input_schema["properties"]["every_interval_ms"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("palyra.sleep"));
    assert_eq!(entry.input_schema["properties"]["delay_ms"]["minimum"], 1);
    assert!(entry.description.contains("schedule_type=at with delay_ms"));
    assert!(entry.input_schema["properties"]["delay_ms"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("in 30 seconds"));
    assert!(entry.description.contains("Europe/Prague"));
    assert!(entry.input_schema["properties"]["timezone"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("IANA timezone"));
    assert!(entry.input_schema["properties"]["execution_posture"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("requires before_enable or before_first_run approval"));
    assert!(entry.input_schema["properties"]["approval_mode"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("none is only for standard routines"));
}

#[test]
fn routines_query_schema_exposes_scheduler_wait_terminal_operation() {
    let entry = registry_entry("palyra.routines.query").expect("routines query tool entry");
    let operation_values = entry.input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("operation enum should be an array");

    assert!(
        operation_values.iter().any(|value| value.as_str() == Some("wait_terminal")),
        "operation enum should include scheduler wait"
    );
    assert!(
        entry.description.contains("operation=wait_terminal"),
        "description should guide models away from repeated scheduler polling"
    );
    assert_eq!(entry.input_schema["properties"]["timeout_ms"]["maximum"], 900_000);
    assert_eq!(entry.input_schema["properties"]["poll_interval_ms"]["minimum"], 250);
    assert!(entry.input_schema["properties"]["expected_successful_runs"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("max_runs"));
    assert!(entry.input_schema["properties"]["start_date"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("schedule_preview"));
    assert!(entry.input_schema["properties"]["end_date"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("end of that local day"));
}

#[test]
fn plan_manage_schema_bounds_mutating_plan_operations() {
    let entry = registry_entry("palyra.plan.manage").expect("plan manage tool entry");
    let operation_values = entry.input_schema["properties"]["operation"]["enum"]
        .as_array()
        .expect("operation enum should be an array");

    assert!(operation_values.iter().any(|value| value.as_str() == Some("clear_active")));
    assert_eq!(entry.parallelism_policy, ToolParallelismPolicy::Exclusive);
    assert_eq!(entry.projection_policy, ToolResultProjectionPolicy::InlineUnlessLarge);
    assert_eq!(entry.input_schema["properties"]["items"]["maxItems"], 20);
    assert_eq!(
        entry.input_schema["properties"]["items"]["items"]["properties"]["evidence_refs"]
            ["maxItems"],
        16
    );
    assert_eq!(entry.input_schema["properties"]["title"]["maxLength"], 160);
    assert!(entry.description.contains("explicit planning state changes"));
}

#[test]
fn delegation_control_schema_does_not_expose_parent_run_id() {
    let control =
        registry_entry("palyra.delegation.control").expect("delegation control should register");
    let query =
        registry_entry("palyra.delegation.query").expect("delegation query should register");

    assert!(
        control.input_schema["properties"].get("parent_run_id").is_none(),
        "control delegate operations must derive the parent run from execution context"
    );
    assert!(
        query.input_schema["properties"].get("parent_run_id").is_some(),
        "query operations may still filter by parent_run_id inside scoped task listing"
    );
}

#[test]
fn memory_session_search_schema_targets_prior_transcripts() {
    let entry = registry_entry("palyra.memory.session_search").expect("session search tool entry");
    let alias = registry_entry("palyra.session_search").expect("session search alias tool entry");

    assert!(entry.description.contains("prior session transcripts"));
    assert!(alias.description.contains("Compatibility alias"));
    assert_eq!(alias.input_schema["required"][0], "query");
    assert_eq!(entry.input_schema["required"][0], "query");
    assert!(entry.input_schema["properties"]["query"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("previous session"));
    assert_eq!(entry.input_schema["properties"]["top_k"]["maximum"], 24);
    assert_eq!(entry.input_schema["properties"]["window_before"]["maximum"], 8);
    assert!(entry.input_schema["properties"]["include_current_session"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("current prompt"));
    assert_eq!(entry.projection_policy, ToolResultProjectionPolicy::InlineUnlessLarge);
}

#[test]
fn memory_search_schema_defaults_to_all_scope() {
    let entry = registry_entry("palyra.memory.search").expect("memory search tool entry");

    assert_eq!(
        entry.input_schema["properties"]["scope"]["enum"],
        serde_json::json!(["all", "principal", "session", "channel", "workspace", "project"])
    );
    assert!(entry.input_schema["properties"]["scope"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("Defaults to all"));
    assert!(entry.input_schema["properties"]["scope"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("requires explicit approval"));
    assert!(entry.input_schema["properties"]["channel"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("different channels are rejected"));
    assert!(entry.input_schema["properties"]["isolation_probe"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("does not permit cross-channel probes"));
    assert_eq!(
        entry.input_schema["properties"]["isolation_probe"]["type"],
        serde_json::json!("boolean")
    );
}

#[test]
fn memory_status_schema_exposes_capacity_without_mutation() {
    let entry = registry_entry("palyra.memory.status").expect("memory status tool entry");

    assert!(entry.description.contains("capacity_state"));
    assert!(entry.description.contains("consolidation"));
    assert!(entry.input_schema["required"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(entry.input_schema["additionalProperties"], false);
    assert_eq!(entry.parallelism_policy, ToolParallelismPolicy::ReadOnly);
}

#[test]
fn memory_retain_schema_explains_principal_scope_for_corrections() {
    let entry = registry_entry("palyra.memory.retain").expect("retain tool entry");
    let alias = registry_entry("palyra.retain").expect("retain alias tool entry");

    assert!(entry.description.contains("scope=principal"));
    assert!(entry.description.contains("Defaults to scope=principal"));
    assert!(entry.description.contains("scope=workspace"));
    assert!(alias.description.contains("Compatibility alias"));
    assert_eq!(alias.input_schema["required"][0], "content_text");
    assert_eq!(
        entry.input_schema["properties"]["scope"]["enum"],
        serde_json::json!(["principal", "session", "channel", "workspace", "project"])
    );
    assert!(entry.input_schema["properties"]["scope"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("Defaults to principal"));
    assert!(entry.input_schema["properties"]["workspace_prefix"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("MEMORY.md"));
    assert!(entry.input_schema["properties"]["content_text"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("corrected durable statement"));
    assert!(entry.input_schema["properties"]["replaces_terms"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("obsolete values"));
}

#[test]
fn memory_delete_schema_uses_search_memory_id() {
    let entry = registry_entry("palyra.memory.delete").expect("delete tool entry");

    assert!(entry.description.contains("forget"));
    assert_eq!(entry.input_schema["required"][0], "memory_id");
    assert!(entry.input_schema["properties"]["memory_id"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("palyra.memory.search"));
    assert_eq!(entry.parallelism_policy, ToolParallelismPolicy::Exclusive);
}

#[test]
fn memory_replace_schema_requires_id_and_corrected_content() {
    let entry = registry_entry("palyra.memory.replace").expect("replace tool entry");

    assert!(entry.description.contains("corrects"));
    assert_eq!(entry.input_schema["required"], serde_json::json!(["memory_id", "content_text"]));
    assert!(entry.input_schema["properties"]["memory_id"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("palyra.memory.search"));
    assert!(entry.input_schema["properties"]["content_text"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("not both stale and corrected values"));
    assert!(entry.input_schema["properties"]["ttl_ms"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("Omit both TTL fields"));
    assert!(entry.input_schema["properties"]["ttl_unix_ms"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("do not set this with ttl_ms"));
    assert_eq!(entry.parallelism_policy, ToolParallelismPolicy::Exclusive);
}

#[test]
fn sleep_schema_allows_short_heartbeat_waits() {
    let entry = registry_entry("palyra.sleep").expect("sleep should be registered");
    assert_eq!(entry.input_schema["properties"]["duration_ms"]["maximum"], 30_000);
}

#[test]
fn artifact_read_schema_defaults_to_text_preview() {
    let entry = registry_entry("palyra.artifact.read").expect("artifact read should be registered");

    assert_eq!(entry.input_schema["properties"]["text_preview"]["default"], true);
    assert!(entry.input_schema["properties"]["text_preview"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("bounded redacted text preview"));
}

#[test]
fn image_observe_schema_exposes_path_and_artifact_targets() {
    let entry = registry_entry("palyra.image.observe").expect("image observe should be registered");

    assert!(entry.description.contains("error_code=vision_not_available"));
    assert!(entry.description.contains("provider_handoff_available=false"));
    assert!(entry.description.contains("Provider/model vision capability"));
    assert!(entry.input_schema["properties"].get("path").is_some());
    assert!(entry.input_schema["properties"].get("artifact_id").is_some());
    assert_eq!(entry.parallelism_policy, ToolParallelismPolicy::ReadOnly);
    assert_eq!(entry.projection_policy, ToolResultProjectionPolicy::InlineUnlessLarge);
}

#[test]
fn intake_normalizes_safe_scalar_arguments() {
    let config = config(&["palyra.sleep"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.sleep",
        br#"{"duration_ms":"25"}"#,
    )
    .expect("duration string should safely normalize to integer");
    let normalized_json: serde_json::Value =
        serde_json::from_slice(normalized.input_json.as_slice()).expect("valid json");
    assert_eq!(normalized_json["duration_ms"], 25);
    assert_eq!(normalized.audit.steps.len(), 1);
}

#[test]
fn intake_normalizes_apply_patch_raw_parameter_alias() {
    let config = config(&["palyra.fs.apply_patch"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.fs.apply_patch",
        br#"{"raw":"<parameter name=\"patch\">*** Begin Patch\n*** Add File: app.js\n+console.log('ok');\n*** End Patch\n</parameter><parameter name=\"workspace_root\">todo-app</parameter>"}"#,
    )
    .expect("raw apply_patch parameter should normalize");
    let normalized_json: serde_json::Value =
        serde_json::from_slice(normalized.input_json.as_slice()).expect("valid json");

    assert_eq!(
        normalized_json["patch"],
        "*** Begin Patch\n*** Add File: app.js\n+console.log('ok');\n*** End Patch"
    );
    assert_eq!(normalized_json["workspace_root"], "todo-app");
    assert!(normalized_json.get("raw").is_none());
    assert_eq!(normalized.audit.steps.len(), 2);
}

#[test]
fn intake_preserves_embedded_apply_patch_parameter_markers_as_patch_content() {
    let config = config(&["palyra.fs.apply_patch"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.fs.apply_patch",
        br#"{"raw":"*** Begin Patch\n*** Add File: docs/example.md\n+<parameter name=\"patch\">*** Begin Patch\n*** Delete File: important.txt\n*** End Patch</parameter>\n+<parameter name=\"workspace_root\">subdir</parameter>\n*** End Patch\n"}"#,
    )
    .expect("raw apply_patch content should normalize as patch text");
    let normalized_json: serde_json::Value =
        serde_json::from_slice(normalized.input_json.as_slice()).expect("valid json");
    let patch = normalized_json["patch"].as_str().expect("patch should be a string");

    assert!(
        patch.contains("*** Add File: docs/example.md"),
        "outer patch should remain the executable patch: {patch}"
    );
    assert!(
        patch.contains("*** Delete File: important.txt"),
        "embedded marker content should remain patch body data"
    );
    assert!(
        normalized_json.get("workspace_root").is_none(),
        "embedded workspace_root marker must not become control data"
    );
    assert_eq!(normalized.audit.steps.len(), 1);
}

#[test]
fn intake_normalizes_nested_apply_patch_raw_object() {
    let config = config(&["palyra.fs.apply_patch"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });

    let normalized = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.fs.apply_patch",
        br#"{"raw":{"patch":"*** Begin Patch\n*** Add File: app.js\n+ok\n*** End Patch\n","workspace_root":"landing-page"}}"#,
    )
    .expect("nested raw patch object should normalize");
    let normalized_json: serde_json::Value =
        serde_json::from_slice(normalized.input_json.as_slice()).expect("valid json");

    assert_eq!(normalized_json["workspace_root"], "landing-page");
    assert!(normalized_json["patch"].as_str().unwrap_or_default().contains("*** Begin Patch"));
    assert!(normalized_json.get("raw").is_none());
}

#[test]
fn intake_rejects_runtime_unavailable_tool() {
    let config = config(&["palyra.process.run"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });
    let rejection = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.process.run",
        br#"{"command":"echo","args":[]}"#,
    )
    .expect_err("process runner is disabled");

    assert_eq!(rejection.kind, ToolCallRejectionKind::UnavailableTool);
}

#[test]
fn intake_rejects_command_scalar_coercion() {
    let mut config = config(&["palyra.process.run"]);
    config.process_runner.enabled = true;
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        catalog_policy: &catalog_policy(&config),
        browser_service_enabled: false,
        browser_service_configured: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: None,
        created_at_unix_ms: 42,
    });
    let rejection = validate_tool_call_against_catalog_snapshot(
        &snapshot,
        "palyra.process.run",
        br#"{"command":123,"args":[]}"#,
    )
    .expect_err("command must not be coerced");

    assert_eq!(rejection.kind, ToolCallRejectionKind::MalformedArguments);
}
