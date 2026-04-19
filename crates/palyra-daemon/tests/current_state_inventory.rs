mod support;

use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{json, Value};

use support::{assert_json_golden, DaemonHarness};

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
        vec!["desktop_node", "local_sandbox", "networked_worker", "ssh_tunnel"],
        "inventory should expose the current runtime execution backend inventory"
    );

    assert_json_golden("current_state_inventory.json", &snapshot)?;
    Ok(())
}

fn build_current_state_inventory_snapshot(
    harness: &DaemonHarness,
    session: &support::ConsoleSession,
) -> Result<Value> {
    let capability_catalog =
        harness.console_json("/console/v1/control-plane/capabilities", session)?;
    let diagnostics = harness.console_json("/console/v1/diagnostics", session)?;

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
        compat_route_probe(harness, Method::GET, "/v1/models")?,
        compat_route_probe(harness, Method::GET, "/v1/models/compat-probe")?,
        compat_route_probe(harness, Method::POST, "/v1/embeddings")?,
        compat_route_probe(harness, Method::POST, "/v1/chat/completions")?,
        compat_route_probe(harness, Method::POST, "/v1/responses")?,
        compat_route_probe(harness, Method::POST, "/v1/tools/invoke")?,
    ];

    Ok(json!({
        "contract": capability_catalog.get("contract").cloned().unwrap_or(Value::Null),
        "catalog_version": capability_catalog.get("version").cloned().unwrap_or(Value::Null),
        "diagnostics_sections": diagnostics_sections,
        "capabilities": capabilities,
        "migration_notes": capability_catalog.get("migration_notes").cloned().unwrap_or(Value::Null),
        "feature_rollouts": diagnostics.get("feature_rollouts").cloned().unwrap_or(Value::Null),
        "execution_backend_preferences": ["automatic", "local_sandbox", "desktop_node", "networked_worker", "ssh_tunnel"],
        "execution_backends": execution_backends,
        "compat_routes": compat_routes,
        "cli_families": cli_families,
    }))
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
