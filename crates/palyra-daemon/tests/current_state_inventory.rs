//! Golden-snapshot inventory of the daemon's current externally observable
//! state: capabilities, CLI command families, and compat route surface,
//! probed against a live `DaemonHarness` and pinned via JSON goldens.

mod support;

use std::{collections::BTreeMap, fs, path::PathBuf};

use anyhow::{bail, Context, Result};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{json, Value};

use support::{assert_json_golden, assert_text_golden, DaemonHarness};

#[derive(Debug, Deserialize)]
struct CliParityMatrix {
    entries: Vec<CliParityEntry>,
}

#[derive(Debug, Deserialize)]
struct CliParityEntry {
    path: String,
    category: String,
}

#[test]
fn current_state_inventory_snapshot_covers_capabilities_cli_and_compat_surface() -> Result<()> {
    let harness = DaemonHarness::spawn(&[])?;
    let session = harness.login_as_admin()?;
    let snapshot = build_current_state_inventory_snapshot(&harness, &session)?;

    let compat_routes = snapshot
        .get("compat_routes")
        .and_then(Value::as_array)
        .context("compat_routes should be an array")?;
    assert!(
        compat_routes
            .iter()
            .all(|entry| entry.get("registered").and_then(Value::as_bool) == Some(true)),
        "compat routes should stay registered in the current runtime snapshot"
    );

    let execution_backend_preferences = snapshot
        .get("execution_backend_preferences")
        .and_then(Value::as_array)
        .context("execution_backend_preferences should be an array")?
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        execution_backend_preferences,
        vec!["automatic", "local_sandbox", "desktop_node", "networked_worker", "ssh_tunnel",],
        "inventory should continue to advertise the current execution backend preference set"
    );

    let execution_backends = snapshot
        .get("execution_backends")
        .and_then(Value::as_array)
        .context("execution_backends should be an array")?;
    let backend_ids = execution_backends
        .iter()
        .filter_map(|entry| entry.get("backend_id").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(
        backend_ids,
        vec!["desktop_node", "docker", "local_sandbox", "networked_worker", "ssh_tunnel"],
        "inventory should expose the current runtime execution backend inventory"
    );

    let report = render_runtime_audit_report(&snapshot)?;
    assert!(
        report.contains("## Runtime Controls"),
        "runtime audit report should expose runtime-control state"
    );
    assert_text_golden("current_state_inventory_report.md", report.as_str())?;
    assert_json_golden("current_state_inventory.json", &snapshot)?;
    Ok(())
}

#[test]
fn runtime_audit_baseline_rejects_unknown_runtime_control_state() {
    let error = parse_runtime_control_state("pilot")
        .expect_err("unknown runtime control state should be rejected");

    assert!(
        error.to_string().contains("unknown runtime control state pilot"),
        "error should name the unknown runtime control state: {error}"
    );
}

#[test]
fn runtime_audit_baseline_rejects_unknown_feature_rollout_maturity() {
    let error = parse_feature_rollout_maturity("pilot")
        .expect_err("unknown feature rollout maturity should be rejected");

    assert!(
        error.to_string().contains("unknown feature rollout maturity pilot"),
        "error should name the unknown feature rollout maturity: {error}"
    );
}

fn build_current_state_inventory_snapshot(
    harness: &DaemonHarness,
    session: &support::ConsoleSession,
) -> Result<Value> {
    let capability_catalog =
        harness.console_json("/console/v1/control-plane/capabilities", session)?;
    let diagnostics = harness.console_json("/console/v1/diagnostics", session)?;
    let method_registry = harness.admin_json("/admin/v1/methods")?;

    let mut diagnostics_sections = diagnostics
        .as_object()
        .context("diagnostics payload should be a JSON object")?
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    diagnostics_sections.sort();

    let mut capabilities = capability_catalog
        .get("capabilities")
        .and_then(Value::as_array)
        .context("capability catalog should expose capabilities array")?
        .iter()
        .map(|entry| {
            json!({
                "id": entry.get("id").cloned().unwrap_or(Value::Null),
                "domain": entry.get("domain").cloned().unwrap_or(Value::Null),
                "dashboard_section": entry.get("dashboard_section").cloned().unwrap_or(Value::Null),
                "surfaces": entry.get("surfaces").cloned().unwrap_or(Value::Null),
                "execution_mode": entry.get("execution_mode").cloned().unwrap_or(Value::Null),
                "mutation_classes": entry.get("mutation_classes").cloned().unwrap_or(Value::Null),
                "contract_paths": entry.get("contract_paths").cloned().unwrap_or(Value::Null),
                "cli_handoff_commands": entry.get("cli_handoff_commands").cloned().unwrap_or(Value::Null),
            })
        })
        .collect::<Vec<_>>();
    capabilities.sort_by(|left, right| {
        left.get("id").and_then(Value::as_str).cmp(&right.get("id").and_then(Value::as_str))
    });

    let mut execution_backends = diagnostics
        .get("execution_backends")
        .and_then(Value::as_array)
        .context("diagnostics should expose execution_backends array")?
        .iter()
        .map(|entry| {
            json!({
                "backend_id": entry.get("backend_id").cloned().unwrap_or(Value::Null),
                "label": entry.get("label").cloned().unwrap_or(Value::Null),
                "description": entry.get("description").cloned().unwrap_or(Value::Null),
                "executor_label": entry.get("executor_label").cloned().unwrap_or(Value::Null),
                "rollout_flag": entry.get("rollout_flag").cloned().unwrap_or(Value::Null),
                "rollout_enabled": entry.get("rollout_enabled").cloned().unwrap_or(Value::Null),
                "tradeoffs": entry.get("tradeoffs").cloned().unwrap_or(Value::Null),
                "requires_attestation": entry.get("requires_attestation").cloned().unwrap_or(Value::Null),
                "requires_egress_proxy": entry.get("requires_egress_proxy").cloned().unwrap_or(Value::Null),
                "workspace_scope_mode": entry.get("workspace_scope_mode").cloned().unwrap_or(Value::Null),
                "artifact_transport": entry.get("artifact_transport").cloned().unwrap_or(Value::Null),
                "cleanup_strategy": entry.get("cleanup_strategy").cloned().unwrap_or(Value::Null),
            })
        })
        .collect::<Vec<_>>();
    execution_backends.sort_by(|left, right| {
        left.get("backend_id")
            .and_then(Value::as_str)
            .cmp(&right.get("backend_id").and_then(Value::as_str))
    });

    let cli_families = load_cli_families()?;
    let compat_routes = vec![
        compat_route_probe(harness, Method::GET, "/v1/capabilities")?,
        compat_route_probe(harness, Method::GET, "/v1/models")?,
        compat_route_probe(harness, Method::GET, "/v1/models/compat-probe")?,
        compat_route_probe(harness, Method::POST, "/v1/embeddings")?,
        compat_route_probe(harness, Method::POST, "/v1/chat/completions")?,
        compat_route_probe(harness, Method::POST, "/v1/responses")?,
        compat_route_probe(harness, Method::GET, "/v1/responses/resp_probe")?,
        compat_route_probe(harness, Method::DELETE, "/v1/responses/resp_probe")?,
        compat_route_probe(harness, Method::POST, "/v1/runs")?,
        compat_route_probe(harness, Method::GET, "/v1/runs/01ARZ3NDEKTSV4RRFFQ69G5FAV")?,
        compat_route_probe(harness, Method::GET, "/v1/runs/01ARZ3NDEKTSV4RRFFQ69G5FAV/events")?,
        compat_route_probe(harness, Method::POST, "/v1/runs/01ARZ3NDEKTSV4RRFFQ69G5FAV/wait")?,
        compat_route_probe(harness, Method::POST, "/v1/runs/01ARZ3NDEKTSV4RRFFQ69G5FAV/stop")?,
        compat_route_probe(harness, Method::POST, "/v1/runs/01ARZ3NDEKTSV4RRFFQ69G5FAV/detach")?,
        compat_route_probe(harness, Method::POST, "/v1/runs/01ARZ3NDEKTSV4RRFFQ69G5FAV/approval")?,
        compat_route_probe(harness, Method::POST, "/v1/tools/invoke")?,
    ];
    let mut runtime_controls = diagnostics
        .get("runtime_controls")
        .and_then(Value::as_object)
        .context("diagnostics should expose runtime_controls object")?
        .clone();
    let runtime_control_capabilities = runtime_controls
        .remove("capabilities")
        .and_then(|value| value.as_array().cloned())
        .context("runtime_controls should expose capabilities array")?;
    let mut runtime_control_capabilities = runtime_control_capabilities
        .into_iter()
        .map(|entry| {
            json!({
                "capability": entry.get("capability").cloned().unwrap_or(Value::Null),
                "mode": entry.get("mode").cloned().unwrap_or(Value::Null),
                "effective_state": entry.get("effective_state").cloned().unwrap_or(Value::Null),
                "rollout_enabled": entry.get("rollout_enabled").cloned().unwrap_or(Value::Null),
                "rollout_source": entry.get("rollout_source").cloned().unwrap_or(Value::Null),
                "activation_blockers": entry.get("activation_blockers").cloned().unwrap_or(Value::Null),
            })
        })
        .collect::<Vec<_>>();
    runtime_control_capabilities.sort_by(|left, right| {
        left.get("capability")
            .and_then(Value::as_str)
            .cmp(&right.get("capability").and_then(Value::as_str))
    });

    let mut snapshot = json!({
        "contract": capability_catalog.get("contract").cloned().unwrap_or(Value::Null),
        "catalog_version": capability_catalog.get("version").cloned().unwrap_or(Value::Null),
        "diagnostics_sections": diagnostics_sections,
        "capabilities": capabilities,
        "migration_notes": capability_catalog.get("migration_notes").cloned().unwrap_or(Value::Null),
        "feature_rollouts": diagnostics.get("feature_rollouts").cloned().unwrap_or(Value::Null),
        "feature_rollout_maturity": diagnostics.get("feature_rollout_maturity").cloned().unwrap_or(Value::Null),
        "method_registry": method_registry,
        "runtime_roadmap": diagnostics.get("runtime_roadmap").cloned().unwrap_or(Value::Null),
        "runtime_controls": {
            "schema_version": runtime_controls.remove("schema_version").unwrap_or(Value::Null),
            "state": runtime_controls.remove("state").unwrap_or(Value::Null),
            "preview_capabilities": runtime_controls
                .remove("preview_capabilities")
                .unwrap_or(Value::Null),
            "enabled_capabilities": runtime_controls
                .remove("enabled_capabilities")
                .unwrap_or(Value::Null),
            "blocked_capabilities": runtime_controls
                .remove("blocked_capabilities")
                .unwrap_or(Value::Null),
            "disabled_capabilities": runtime_controls
                .remove("disabled_capabilities")
                .unwrap_or(Value::Null),
            "capabilities": runtime_control_capabilities,
        },
        "execution_backend_preferences": ["automatic", "local_sandbox", "desktop_node", "networked_worker", "ssh_tunnel"],
        "execution_backends": execution_backends,
        "compat_routes": compat_routes,
        "cli_families": cli_families,
    });
    let runtime_audit_baseline = build_runtime_audit_baseline(&snapshot)?;
    let Some(snapshot_object) = snapshot.as_object_mut() else {
        bail!("current state inventory snapshot should be a JSON object");
    };
    snapshot_object.insert("runtime_audit_baseline".to_owned(), runtime_audit_baseline);
    Ok(snapshot)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeControlState {
    Enabled,
    PreviewOnly,
    Disabled,
    Blocked,
}

impl RuntimeControlState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::PreviewOnly => "preview_only",
            Self::Disabled => "disabled",
            Self::Blocked => "blocked",
        }
    }
}

fn parse_runtime_control_state(raw: &str) -> Result<RuntimeControlState> {
    match raw {
        "enabled" => Ok(RuntimeControlState::Enabled),
        "preview_only" => Ok(RuntimeControlState::PreviewOnly),
        "disabled" => Ok(RuntimeControlState::Disabled),
        "blocked" => Ok(RuntimeControlState::Blocked),
        other => bail!("unknown runtime control state {other}"),
    }
}

fn build_runtime_audit_baseline(snapshot: &Value) -> Result<Value> {
    let capabilities = required_array(snapshot, "/capabilities")?;
    let cli_families = required_array(snapshot, "/cli_families")?;
    let compat_routes = required_array(snapshot, "/compat_routes")?;
    let execution_backends = required_array(snapshot, "/execution_backends")?;
    let feature_rollouts = required_object(snapshot, "/feature_rollouts")?;
    let feature_rollout_maturity = required_object(snapshot, "/feature_rollout_maturity")?;
    let method_registry = required_object(snapshot, "/method_registry")?;
    let method_registry_methods = required_array(snapshot, "/method_registry/methods")?;
    let method_registry_scopes = required_array(snapshot, "/method_registry/scopes")?;
    let runtime_controls = required_object(snapshot, "/runtime_controls")?;
    let runtime_control_capabilities = required_array(snapshot, "/runtime_controls/capabilities")?;
    let roadmap_area_map = roadmap_area_map();
    let status_counts = count_roadmap_area_statuses(roadmap_area_map.as_slice())?;

    Ok(json!({
        "schema_version": 1,
        "generation_commands": [
            "pwsh -NoLogo -File scripts/dev/generate-runtime-audit-baseline.ps1",
            "bash scripts/dev/generate-runtime-audit-baseline.sh"
        ],
        "golden_snapshot": "crates/palyra-daemon/tests/golden/current_state_inventory.json",
        "human_report": "crates/palyra-daemon/tests/golden/current_state_inventory_report.md",
        "source_of_truth": runtime_audit_source_map(),
        "roadmap_area_map": roadmap_area_map,
        "surface_counts": {
            "capability_catalog_entries": capabilities.len(),
            "cli_families": cli_families.len(),
            "compat_routes": compat_routes.len(),
            "registered_compat_routes": compat_routes
                .iter()
                .filter(|entry| entry.get("registered").and_then(Value::as_bool) == Some(true))
                .count(),
            "diagnostics_sections": required_array(snapshot, "/diagnostics_sections")?.len(),
            "execution_backends": execution_backends.len(),
            "feature_rollout_flags": feature_rollouts.len(),
            "method_registry_methods": method_registry_methods.len(),
            "method_registry_scopes": method_registry_scopes.len(),
            "runtime_control_capabilities": runtime_control_capabilities.len(),
        },
        "roadmap_area_status_counts": status_counts,
        "feature_rollout_counts": count_feature_rollouts(feature_rollouts)?,
        "feature_rollout_maturity": {
            "schema_version": feature_rollout_maturity.get("schema_version").cloned().unwrap_or(Value::Null),
            "maturity_counts": count_feature_rollout_maturity(feature_rollouts)?,
            "migration_note": feature_rollout_maturity.get("migration_note").cloned().unwrap_or(Value::Null),
        },
        "method_registry": {
            "schema_version": method_registry.get("schema_version").cloned().unwrap_or(Value::Null),
            "registry_version": method_registry.get("registry_version").cloned().unwrap_or(Value::Null),
            "surface_counts": count_method_registry_surfaces(method_registry_methods)?,
        },
        "runtime_control_state_counts": count_runtime_control_states(
            runtime_control_capabilities
        )?,
        "runtime_controls_summary": {
            "schema_version": runtime_controls.get("schema_version").cloned().unwrap_or(Value::Null),
            "state": runtime_controls.get("state").cloned().unwrap_or(Value::Null),
            "preview_capabilities": runtime_controls.get("preview_capabilities").cloned().unwrap_or(Value::Null),
            "enabled_capabilities": runtime_controls.get("enabled_capabilities").cloned().unwrap_or(Value::Null),
            "blocked_capabilities": runtime_controls.get("blocked_capabilities").cloned().unwrap_or(Value::Null),
            "disabled_capabilities": runtime_controls.get("disabled_capabilities").cloned().unwrap_or(Value::Null),
        }
    }))
}

fn runtime_audit_source_map() -> Vec<Value> {
    vec![
        json!({
            "surface": "capability_catalog",
            "source_paths": [
                "crates/palyra-daemon/src/transport/http/handlers/console/auth.rs",
                "crates/palyra-daemon/src/transport/http/handlers/console/diagnostics.rs",
                "crates/palyra-control-plane/src/models.rs"
            ],
            "reason": "public capability ids, surfaces, mutation classes, and contract paths"
        }),
        json!({
            "surface": "runtime_diagnostics",
            "source_paths": [
                "crates/palyra-daemon/src/transport/http/handlers/console/diagnostics.rs",
                "crates/palyra-daemon/src/runtime_diagnostics.rs"
            ],
            "reason": "runtime sections, health, metrics, roadmap, observability, and feature rollout payloads"
        }),
        json!({
            "surface": "runtime_preview_controls",
            "source_paths": [
                "crates/palyra-daemon/src/runtime_preview_controls.rs",
                "crates/palyra-common/src/runtime_preview.rs",
                "crates/palyra-daemon/src/config/schema.rs"
            ],
            "reason": "preview capability modes, rollout gates, activation blockers, and shared wire names"
        }),
        json!({
            "surface": "feature_rollout_maturity",
            "source_paths": [
                "crates/palyra-daemon/src/feature_rollout_maturity.rs",
                "crates/palyra-daemon/src/config/schema.rs",
                "crates/palyra-daemon/tests/current_state_inventory.rs"
            ],
            "reason": "rollout maturity states, owners, required tests, public exposure, and promotion blockers"
        }),
        json!({
            "surface": "method_registry",
            "source_paths": [
                "crates/palyra-daemon/src/method_registry.rs",
                "crates/palyra-daemon/src/transport/http/router.rs",
                "crates/palyra-daemon/src/access_control.rs"
            ],
            "reason": "public method descriptors, route scopes, schema hashes, streaming flags, and idempotency support"
        }),
        json!({
            "surface": "compat_routes",
            "source_paths": [
                "crates/palyra-daemon/src/transport/http/router.rs",
                "crates/palyra-daemon/tests/current_state_inventory.rs"
            ],
            "reason": "registered OpenAI-compatible route surface probed by the live daemon harness"
        }),
        json!({
            "surface": "cli_families",
            "source_paths": [
                "crates/palyra-cli/tests/cli_parity_matrix.toml",
                "crates/palyra-cli/tests/cli_parity_report.md"
            ],
            "reason": "top-level CLI families and parity status used by operator handoff surfaces"
        }),
        json!({
            "surface": "execution_backends",
            "source_paths": [
                "crates/palyra-daemon/src/execution_backends.rs",
                "crates/palyra-daemon/src/application/tool_runtime"
            ],
            "reason": "local, desktop, Docker, networked worker, and SSH backend posture"
        }),
    ]
}

fn roadmap_area_map() -> Vec<Value> {
    vec![
        json!({
            "area": "api",
            "status": "production",
            "evidence": ["/console/v1/control-plane/capabilities", "/v1/models", "/v1/chat/completions", "/v1/responses"],
            "reason": "console and compat routes are registered in the live daemon harness"
        }),
        json!({
            "area": "mcp",
            "status": "scaffold",
            "evidence": ["cli family: mcp", "roadmap phase 5"],
            "reason": "MCP serve is discoverable, while external MCP import/supervision remains roadmap work"
        }),
        json!({
            "area": "subagents",
            "status": "preview",
            "evidence": ["runtime_controls.auxiliary_executor", "cli: sessions subagents", "session snapshot subagent_records"],
            "reason": "delegated work and durable subagent record projections exist behind preview controls"
        }),
        json!({
            "area": "execution_backends",
            "status": "preview",
            "evidence": ["execution_backends", "runtime_controls.networked_workers"],
            "reason": "local sandbox is available, while remote backends and workers remain gated or disabled"
        }),
        json!({
            "area": "qa_lab",
            "status": "scaffold",
            "evidence": ["runtime_roadmap.phase0_harness", "fixtures/golden/release_eval_inventory.json"],
            "reason": "regression fixtures exist before the dedicated QA Lab manifest and runner"
        }),
        json!({
            "area": "hooks",
            "status": "preview",
            "evidence": ["capability: hooks", "cli family: hooks"],
            "reason": "basic hook operability is exposed before the full agent hook taxonomy"
        }),
        json!({
            "area": "observability",
            "status": "production",
            "evidence": ["/console/v1/diagnostics", "runtime_health", "agent_runtime_metrics", "opentelemetry"],
            "reason": "diagnostics and metrics sections are emitted by the live daemon harness"
        }),
        json!({
            "area": "provider_recovery",
            "status": "scaffold",
            "evidence": ["feature_rollouts.provider_stream_normalizer", "feature_rollouts.tool_repair"],
            "reason": "recovery flags are visible but default-off before classifier and stream-normalizer work"
        }),
    ]
}

fn count_feature_rollouts(
    feature_rollouts: &serde_json::Map<String, Value>,
) -> Result<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::new();
    for (name, entry) in feature_rollouts {
        let enabled = entry
            .get("enabled")
            .and_then(Value::as_bool)
            .with_context(|| format!("feature rollout {name} should expose enabled"))?;
        let key = if enabled { "enabled" } else { "disabled" };
        *counts.entry(key.to_owned()).or_default() += 1;
    }
    Ok(counts)
}

fn count_feature_rollout_maturity(
    feature_rollouts: &serde_json::Map<String, Value>,
) -> Result<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::new();
    for (name, entry) in feature_rollouts {
        let maturity = entry
            .get("maturity")
            .and_then(Value::as_str)
            .with_context(|| format!("feature rollout {name} should expose maturity"))?;
        let maturity = parse_feature_rollout_maturity(maturity)?;
        *counts.entry(maturity.to_owned()).or_default() += 1;
    }
    Ok(counts)
}

fn parse_feature_rollout_maturity(raw: &str) -> Result<&str> {
    match raw {
        "scaffold" | "preview_only" | "gated_production" | "stable" | "deprecated" | "blocked" => {
            Ok(raw)
        }
        other => bail!("unknown feature rollout maturity {other}"),
    }
}

fn count_method_registry_surfaces(methods: &[Value]) -> Result<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::new();
    for method in methods {
        let surface = method
            .get("surface")
            .and_then(Value::as_str)
            .context("method registry entry should expose surface")?;
        let required_scope = method
            .get("required_scope")
            .and_then(Value::as_str)
            .context("method registry entry should expose required_scope")?;
        let request_schema_hash = method
            .get("request_schema_hash")
            .and_then(Value::as_str)
            .context("method registry entry should expose request_schema_hash")?;
        let response_schema_hash = method
            .get("response_schema_hash")
            .and_then(Value::as_str)
            .context("method registry entry should expose response_schema_hash")?;
        if required_scope.trim().is_empty() {
            bail!("method registry entry should not use an empty required_scope");
        }
        if request_schema_hash.len() != 64 || response_schema_hash.len() != 64 {
            bail!("method registry schema hashes should be 64-character SHA-256 hex strings");
        }
        *counts.entry(surface.to_owned()).or_default() += 1;
    }
    Ok(counts)
}

fn count_runtime_control_states(capabilities: &[Value]) -> Result<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::new();
    for entry in capabilities {
        let state = entry
            .get("effective_state")
            .and_then(Value::as_str)
            .context("runtime control capability should expose effective_state")?;
        let state = parse_runtime_control_state(state)?;
        *counts.entry(state.as_str().to_owned()).or_default() += 1;
    }
    Ok(counts)
}

fn count_roadmap_area_statuses(areas: &[Value]) -> Result<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::new();
    for area in areas {
        let status = area
            .get("status")
            .and_then(Value::as_str)
            .context("roadmap area should expose status")?;
        match status {
            "production" | "preview" | "disabled" | "scaffold" => {
                *counts.entry(status.to_owned()).or_default() += 1;
            }
            other => bail!("unknown roadmap area status {other}"),
        }
    }
    Ok(counts)
}

fn render_runtime_audit_report(snapshot: &Value) -> Result<String> {
    let baseline = snapshot
        .get("runtime_audit_baseline")
        .context("runtime inventory should include runtime_audit_baseline")?;
    let capabilities = required_array(snapshot, "/capabilities")?;
    let cli_families = required_array(snapshot, "/cli_families")?;
    let compat_routes = required_array(snapshot, "/compat_routes")?;
    let feature_rollouts = required_object(snapshot, "/feature_rollouts")?;
    let method_registry_methods = required_array(snapshot, "/method_registry/methods")?;
    let runtime_control_capabilities = required_array(snapshot, "/runtime_controls/capabilities")?;
    let source_of_truth = required_array(baseline, "/source_of_truth")?;
    let roadmap_areas = required_array(baseline, "/roadmap_area_map")?;

    let registered_compat_routes = compat_routes
        .iter()
        .filter(|entry| entry.get("registered").and_then(Value::as_bool) == Some(true))
        .count();
    let runtime_state_counts = count_runtime_control_states(runtime_control_capabilities)?;
    let rollout_counts = count_feature_rollouts(feature_rollouts)?;
    let maturity_counts = count_feature_rollout_maturity(feature_rollouts)?;

    let mut report = String::new();
    report.push_str("# Runtime Audit Baseline\n\n");
    report.push_str("Generated from the live daemon harness and committed CLI parity matrix.\n\n");
    report.push_str("Regenerate with one command:\n\n");
    report.push_str("```powershell\n");
    report.push_str("pwsh -NoLogo -File scripts/dev/generate-runtime-audit-baseline.ps1\n");
    report.push_str("```\n\n");
    report.push_str("Linux/macOS equivalent:\n\n");
    report.push_str("```bash\n");
    report.push_str("bash scripts/dev/generate-runtime-audit-baseline.sh\n");
    report.push_str("```\n\n");

    report.push_str("## Summary\n\n");
    report.push_str(format!("- Capability catalog entries: `{}`\n", capabilities.len()).as_str());
    report.push_str(format!("- CLI families: `{}`\n", cli_families.len()).as_str());
    report.push_str(
        format!("- Method registry entries: `{}`\n", method_registry_methods.len()).as_str(),
    );
    report.push_str(
        format!(
            "- Compat routes registered: `{registered_compat_routes}/{}`\n",
            compat_routes.len()
        )
        .as_str(),
    );
    report.push_str(format!("- Feature rollout flags: `{}`\n", feature_rollouts.len()).as_str());
    report.push_str(
        format!(
            "- Runtime preview controls: `{}` capabilities\n",
            runtime_control_capabilities.len()
        )
        .as_str(),
    );
    report.push_str(
        format!(
            "- Feature rollout maturity: `scaffold={}`, `preview_only={}`, `gated_production={}`, `stable={}`, `deprecated={}`, `blocked={}`\n",
            maturity_counts.get("scaffold").copied().unwrap_or_default(),
            maturity_counts.get("preview_only").copied().unwrap_or_default(),
            maturity_counts.get("gated_production").copied().unwrap_or_default(),
            maturity_counts.get("stable").copied().unwrap_or_default(),
            maturity_counts.get("deprecated").copied().unwrap_or_default(),
            maturity_counts.get("blocked").copied().unwrap_or_default(),
        )
        .as_str(),
    );
    report.push('\n');

    report.push_str("## State Buckets\n\n");
    report.push_str("| Bucket | Count | Source |\n");
    report.push_str("| --- | ---: | --- |\n");
    push_count_row(
        &mut report,
        "production",
        required_usize(baseline, "/roadmap_area_status_counts/production")?,
        "roadmap area source map",
    );
    push_count_row(
        &mut report,
        "preview",
        required_usize(baseline, "/roadmap_area_status_counts/preview")?,
        "roadmap area source map",
    );
    push_count_row(
        &mut report,
        "disabled",
        runtime_state_counts.get("disabled").copied().unwrap_or_default()
            + runtime_state_counts.get("blocked").copied().unwrap_or_default(),
        "runtime_controls effective_state",
    );
    push_count_row(
        &mut report,
        "scaffold",
        required_usize(baseline, "/roadmap_area_status_counts/scaffold")?,
        "roadmap area source map",
    );
    report.push('\n');

    report.push_str("## Source Of Truth\n\n");
    report.push_str("| Surface | Source paths | Why Palyra tracks it |\n");
    report.push_str("| --- | --- | --- |\n");
    for entry in source_of_truth {
        report.push_str(
            format!(
                "| `{}` | {} | {} |\n",
                required_str_field(entry, "surface")?,
                format_string_array(entry, "source_paths")?,
                required_str_field(entry, "reason")?
            )
            .as_str(),
        );
    }
    report.push('\n');

    report.push_str("## Roadmap Area Map\n\n");
    report.push_str("| Area | Status | Evidence | Reason |\n");
    report.push_str("| --- | --- | --- | --- |\n");
    for area in roadmap_areas {
        report.push_str(
            format!(
                "| `{}` | `{}` | {} | {} |\n",
                required_str_field(area, "area")?,
                required_str_field(area, "status")?,
                format_string_array(area, "evidence")?,
                required_str_field(area, "reason")?
            )
            .as_str(),
        );
    }
    report.push('\n');

    report.push_str("## Runtime Controls\n\n");
    report.push_str("| Capability | Mode | Effective state | Rollout | Blockers |\n");
    report.push_str("| --- | --- | --- | --- | --- |\n");
    for entry in runtime_control_capabilities {
        let state = required_str_field(entry, "effective_state")?;
        parse_runtime_control_state(state)?;
        report.push_str(
            format!(
                "| `{}` | `{}` | `{}` | `{}` from `{}` | {} |\n",
                required_str_field(entry, "capability")?,
                required_str_field(entry, "mode")?,
                state,
                required_bool_field(entry, "rollout_enabled")?,
                required_str_field(entry, "rollout_source")?,
                format_activation_blockers(entry)
            )
            .as_str(),
        );
    }
    report.push('\n');

    report.push_str("## Feature Rollouts\n\n");
    report.push_str(
        format!(
            "- Enabled: `{}`\n- Disabled/default-off: `{}`\n\n",
            rollout_counts.get("enabled").copied().unwrap_or_default(),
            rollout_counts.get("disabled").copied().unwrap_or_default()
        )
        .as_str(),
    );
    report.push_str("| Flag | Enabled | Source | Maturity | Owner | Public API exposure | Config path | Env var | Blockers |\n");
    report.push_str("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
    let mut rollout_names = feature_rollouts.keys().collect::<Vec<_>>();
    rollout_names.sort();
    for name in rollout_names {
        let entry = feature_rollouts
            .get(name)
            .with_context(|| format!("feature rollout {name} should be present"))?;
        report.push_str(
            format!(
                "| `{}` | `{}` | `{}` | `{}` | `{}` | {} | `{}` | `{}` | {} |\n",
                name,
                required_bool_field(entry, "enabled")?,
                required_str_field(entry, "source")?,
                parse_feature_rollout_maturity(required_str_field(entry, "maturity")?)?,
                required_str_field(entry, "owner_component")?,
                required_str_field(entry, "public_api_exposure")?,
                required_str_field(entry, "config_path")?,
                required_str_field(entry, "env_var")?,
                format_activation_blockers(entry)
            )
            .as_str(),
        );
    }
    report.push('\n');

    report.push_str("## Compat Routes\n\n");
    report.push_str("| Method | Path | Registered |\n");
    report.push_str("| --- | --- | --- |\n");
    for route in compat_routes {
        report.push_str(
            format!(
                "| `{}` | `{}` | `{}` |\n",
                required_str_field(route, "method")?,
                required_str_field(route, "path")?,
                required_bool_field(route, "registered")?
            )
            .as_str(),
        );
    }

    Ok(report)
}

fn push_count_row(report: &mut String, bucket: &str, count: usize, source: &str) {
    report.push_str(format!("| `{bucket}` | `{count}` | {source} |\n").as_str());
}

fn required_array<'a>(value: &'a Value, pointer: &str) -> Result<&'a Vec<Value>> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .with_context(|| format!("runtime inventory should expose array at {pointer}"))
}

fn required_object<'a>(
    value: &'a Value,
    pointer: &str,
) -> Result<&'a serde_json::Map<String, Value>> {
    value
        .pointer(pointer)
        .and_then(Value::as_object)
        .with_context(|| format!("runtime inventory should expose object at {pointer}"))
}

fn required_str_field<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("runtime inventory entry should expose string field {field}"))
}

fn required_bool_field(value: &Value, field: &str) -> Result<bool> {
    value
        .get(field)
        .and_then(Value::as_bool)
        .with_context(|| format!("runtime inventory entry should expose boolean field {field}"))
}

fn required_usize(value: &Value, pointer: &str) -> Result<usize> {
    let raw = value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .with_context(|| format!("runtime audit baseline should expose number at {pointer}"))?;
    usize::try_from(raw).with_context(|| format!("runtime audit count at {pointer} exceeds usize"))
}

fn format_string_array(value: &Value, field: &str) -> Result<String> {
    let entries = value
        .get(field)
        .and_then(Value::as_array)
        .with_context(|| format!("runtime inventory entry should expose array field {field}"))?
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(|raw| format!("`{raw}`"))
                .with_context(|| format!("runtime inventory array {field} should contain strings"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(entries.join(", "))
}

fn format_activation_blockers(entry: &Value) -> String {
    match entry.get("activation_blockers").and_then(Value::as_array) {
        Some(blockers) if !blockers.is_empty() => blockers
            .iter()
            .filter_map(Value::as_str)
            .map(|blocker| format!("`{blocker}`"))
            .collect::<Vec<_>>()
            .join("<br>"),
        _ => "-".to_owned(),
    }
}

fn compat_route_probe(harness: &DaemonHarness, method: Method, path: &str) -> Result<Value> {
    Ok(json!({
        "path": path,
        "method": method.as_str(),
        "registered": harness.route_registered(method, path)?,
    }))
}

fn load_cli_families() -> Result<Vec<String>> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("palyra-cli")
        .join("tests")
        .join("cli_parity_matrix.toml");
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read cli parity matrix {}", path.display()))?;
    let matrix: CliParityMatrix = toml::from_str(raw.as_str())
        .with_context(|| format!("failed to parse cli parity matrix {}", path.display()))?;
    let mut families = matrix
        .entries
        .into_iter()
        .filter(|entry| {
            matches!(entry.category.as_str(), "root" | "top_level" | "canonical_family")
        })
        .map(|entry| entry.path)
        .collect::<Vec<_>>();
    families.sort();
    families.dedup();
    Ok(families)
}
