use super::builtin::registry_entry;
use super::types::{ToolCallRejectionKind, ToolParallelismPolicy};
use super::{
    build_model_visible_tool_catalog_snapshot, projection_policy_for_tool,
    provider_tools_from_catalog_snapshot, snapshot_to_provider_request_value,
    validate_tool_call_against_catalog_snapshot, ToolCatalogBuildRequest, ToolExposureSurface,
    ToolResultProjectionPolicy, ToolSchemaDialect,
};
use crate::{
    sandbox_runner::{EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier},
    tool_protocol::{ToolCallConfig, ToolRequestContext},
    wasm_plugin_runner::WasmPluginRunnerPolicy,
};

fn config(allowed_tools: &[&str]) -> ToolCallConfig {
    ToolCallConfig {
        allowed_tools: allowed_tools.iter().map(|tool| (*tool).to_owned()).collect(),
        max_calls_per_run: 4,
        execution_timeout_ms: 1_000,
        process_runner: SandboxProcessRunnerPolicy {
            enabled: false,
            tier: SandboxProcessRunnerTier::B,
            workspace_root: ".".into(),
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

#[test]
fn catalog_snapshot_exposes_allowlisted_tools_with_schema_hashes() {
    let config = config(&["palyra.echo", "palyra.sleep"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: Some("gpt-test"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 2,
        created_at_unix_ms: 42,
    });

    assert_eq!(snapshot.tools.len(), 2);
    assert!(snapshot.tools.iter().all(|tool| !tool.internal_schema_hash.is_empty()));
    assert!(snapshot.filtered_tools.iter().any(|tool| tool.name == "palyra.process.run"));
    assert!(snapshot.snapshot_id.starts_with("toolcat_"));
}

#[test]
fn provider_payload_projects_native_openai_tools() {
    let config = config(&["palyra.echo"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
fn process_run_allowlist_exposes_lifecycle_controls() {
    let mut config = config(&["palyra.process.run"]);
    config.process_runner.enabled = true;
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 4,
        created_at_unix_ms: 42,
    });
    let tool_names = snapshot.tools.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>();

    assert!(tool_names.contains(&"palyra.process.run"));
    assert!(tool_names.contains(&"palyra.process.stop"));
    assert!(tool_names.contains(&"palyra.process.status"));
    assert!(tool_names.contains(&"palyra.process.list"));
}

#[test]
fn plugin_run_visibility_tracks_wasm_runtime_policy() {
    let mut config = config(&["palyra.plugin.run"]);
    let disabled_snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: true,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: true,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        entry.description.contains("form/storage"),
        "observe tool description should advertise form/storage evidence"
    );
    assert!(
        entry.input_schema["properties"]["include_visible_text"]["description"]
            .as_str()
            .unwrap_or_default()
            .contains("local/session storage"),
        "visible text schema should mention storage summaries"
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
    assert!(entry.description.contains("Europe/Prague"));
    assert!(entry.input_schema["properties"]["timezone"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("IANA timezone"));
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
fn memory_search_schema_defaults_to_principal_scope() {
    let entry = registry_entry("palyra.memory.search").expect("memory search tool entry");

    assert_eq!(
        entry.input_schema["properties"]["scope"]["enum"],
        serde_json::json!(["principal", "session", "channel", "workspace", "project"])
    );
    assert!(entry.input_schema["properties"]["scope"]["description"]
        .as_str()
        .unwrap_or_default()
        .contains("Defaults to principal"));
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
fn intake_normalizes_safe_scalar_arguments() {
    let config = config(&["palyra.sleep"]);
    let snapshot = build_model_visible_tool_catalog_snapshot(ToolCatalogBuildRequest {
        config: &config,
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "anthropic",
        provider_model_id: Some("minimax-m2.7"),
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
        browser_service_enabled: false,
        request_context: &request_context(),
        provider_kind: "openai_compatible",
        provider_model_id: None,
        surface: ToolExposureSurface::RunStream,
        remaining_tool_budget: 1,
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
