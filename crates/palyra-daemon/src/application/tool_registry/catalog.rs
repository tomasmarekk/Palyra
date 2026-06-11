//! Builds the per-provider-turn model-visible tool catalog snapshot.
//!
//! Builtin registry entries pass through allowlist, surface, runtime
//! availability, and provider-schema gates; both survivors and filtered
//! entries are sorted deterministically and content-hashed so identical
//! inputs always yield the same `snapshot_id`/`catalog_hash`.

use std::collections::BTreeSet;

use serde_json::{json, Value};

use crate::tool_protocol::ToolCallConfig;

use super::builtin::{registry_entries, registry_entry};
use super::hashing::{catalog_hash_payload, stable_hash_bytes, stable_hash_value};
use super::schema::{
    exposure_reason, filtered, provider_tool_payload, sanitize_schema_for_provider,
};
use super::types::{
    ModelVisibleTool, ModelVisibleToolCatalogSnapshot, ToolCatalogBuildRequest,
    ToolCatalogFilterReasonCode, ToolResultProjectionPolicy, ToolSchemaDialect,
    TOOL_CATALOG_SCHEMA_VERSION,
};

/// Builds the catalog snapshot describing exactly which tools one provider
/// turn may see, including filter records for every excluded tool.
///
/// Principal and channel identifiers are hashed before they enter the
/// snapshot so the catalog never carries raw identity strings.
pub(crate) fn build_model_visible_tool_catalog_snapshot(
    request: ToolCatalogBuildRequest<'_>,
) -> ModelVisibleToolCatalogSnapshot {
    let dialect = ToolSchemaDialect::from_provider_kind(request.provider_kind);
    let mut filtered_tools = Vec::new();
    let allowed_tools = normalized_allowlist(request.config.allowed_tools.as_slice());
    let mut tools = Vec::new();

    // An exhausted budget collapses the whole catalog: every known or
    // allowlisted name is reported as filtered instead of evaluating gates.
    if request.remaining_tool_budget == 0 {
        for name in all_registry_and_allowlist_names(allowed_tools.iter().map(String::as_str)) {
            filtered_tools.push(filtered(
                &name,
                ToolCatalogFilterReasonCode::BudgetExhausted,
                "start a new run or increase tool_call.max_calls_per_run",
            ));
        }
    } else {
        for entry in registry_entries() {
            if !allowed_tools.contains(entry.name.as_str()) {
                filtered_tools.push(filtered(
                    entry.name.as_str(),
                    ToolCatalogFilterReasonCode::NotAllowlisted,
                    "add the tool to tool_call.allowed_tools for this runtime",
                ));
                continue;
            }
            if !entry.target_surfaces.contains(&request.surface) {
                filtered_tools.push(filtered(
                    entry.name.as_str(),
                    ToolCatalogFilterReasonCode::SurfaceUnsupported,
                    "call the tool from a supported surface",
                ));
                continue;
            }
            if !runtime_available(
                request.config,
                request.browser_service_enabled,
                entry.name.as_str(),
            ) {
                filtered_tools.push(filtered(
                    entry.name.as_str(),
                    ToolCatalogFilterReasonCode::RuntimeUnavailable,
                    "enable the required runtime dependency before exposing this tool",
                ));
                continue;
            }
            let provider_schema = match sanitize_schema_for_provider(&entry.input_schema, dialect) {
                Ok(schema) => schema,
                Err(error) => {
                    filtered_tools.push(filtered(
                        entry.name.as_str(),
                        ToolCatalogFilterReasonCode::ProviderSchemaIncompatible,
                        error.message.as_str(),
                    ));
                    continue;
                }
            };
            tools.push(ModelVisibleTool {
                name: entry.name,
                description_hash: stable_hash_bytes(entry.description.as_bytes()),
                description: entry.description,
                version: entry.version,
                provenance: entry.provenance,
                provider_schema_hash: stable_hash_value(&provider_schema),
                internal_schema_hash: entry.schema_hash,
                schema: entry.input_schema,
                provider_schema,
                capabilities: entry.capabilities,
                approval_posture: entry.approval_posture,
                projection_policy: entry.projection_policy,
                parallelism_policy: entry.parallelism_policy,
                exposure_reason: exposure_reason(entry.approval_posture).to_owned(),
            });
        }

        // Surface allowlisted names with no registry entry so operators see
        // typos instead of tools silently never appearing.
        for allowed in &allowed_tools {
            if registry_entry(allowed.as_str()).is_none() {
                filtered_tools.push(filtered(
                    allowed.as_str(),
                    ToolCatalogFilterReasonCode::UnknownTool,
                    "remove the unknown tool from tool_call.allowed_tools or register metadata",
                ));
            }
        }
    }

    // Deterministic ordering and dedup: the catalog hash must not depend on
    // evaluation order, and a name can be filtered for one reason at most once.
    filtered_tools.sort_by(|left, right| {
        left.name.cmp(&right.name).then(left.reason_code.as_str().cmp(right.reason_code.as_str()))
    });
    filtered_tools
        .dedup_by(|left, right| left.name == right.name && left.reason_code == right.reason_code);
    tools.sort_by(|left, right| left.name.cmp(&right.name));

    let mut snapshot = ModelVisibleToolCatalogSnapshot {
        schema_version: TOOL_CATALOG_SCHEMA_VERSION,
        snapshot_id: String::new(),
        catalog_hash: String::new(),
        provider_dialect: dialect,
        provider_kind: request.provider_kind.to_owned(),
        provider_model_id: request.provider_model_id.map(ToOwned::to_owned),
        surface: request.surface,
        principal_hash: stable_hash_bytes(request.request_context.principal.as_bytes()),
        channel_hash: request
            .request_context
            .channel
            .as_deref()
            .map(|channel| stable_hash_bytes(channel.as_bytes())),
        remaining_tool_budget: request.remaining_tool_budget,
        created_at_unix_ms: request.created_at_unix_ms,
        tools,
        filtered_tools,
    };
    // The hash payload excludes the still-empty snapshot_id/catalog_hash
    // fields, so hashing before filling them in is well-defined; the id is a
    // stable prefix of the hash.
    let catalog_hash = stable_hash_value(&catalog_hash_payload(&snapshot));
    snapshot.snapshot_id = format!("toolcat_{}", &catalog_hash[..16]);
    snapshot.catalog_hash = catalog_hash;
    snapshot
}

/// Serializes the snapshot for embedding in a provider request record,
/// degrading to a minimal tool-less envelope if serialization ever fails.
pub(crate) fn snapshot_to_provider_request_value(
    snapshot: &ModelVisibleToolCatalogSnapshot,
) -> Value {
    serde_json::to_value(snapshot).unwrap_or_else(|_| {
        json!({
            "schema_version": TOOL_CATALOG_SCHEMA_VERSION,
            "snapshot_id": snapshot.snapshot_id,
            "catalog_hash": snapshot.catalog_hash,
            "tools": [],
            "filtered_tools": [],
        })
    })
}

/// Converts a serialized catalog snapshot back into dialect-specific provider
/// tool payloads; an unparsable snapshot yields no tools (fail closed).
pub(crate) fn provider_tools_from_catalog_snapshot(
    snapshot: &Value,
    dialect: ToolSchemaDialect,
) -> Vec<Value> {
    let Ok(snapshot) = serde_json::from_value::<ModelVisibleToolCatalogSnapshot>(snapshot.clone())
    else {
        return Vec::new();
    };
    snapshot.tools.iter().map(|tool| provider_tool_payload(tool, dialect)).collect()
}

/// Renders the hashes-only tape line for a catalog snapshot: descriptions and
/// schemas are reduced to their hashes so the tape stays bounded and free of
/// model-facing prose.
pub(crate) fn tool_catalog_tape_payload(snapshot: &ModelVisibleToolCatalogSnapshot) -> String {
    let payload = json!({
        "schema_version": snapshot.schema_version,
        "snapshot_id": snapshot.snapshot_id,
        "catalog_hash": snapshot.catalog_hash,
        "provider_dialect": snapshot.provider_dialect.as_str(),
        "provider_kind": snapshot.provider_kind,
        "provider_model_id": snapshot.provider_model_id,
        "surface": snapshot.surface.as_str(),
        "remaining_tool_budget": snapshot.remaining_tool_budget,
        "principal_hash": snapshot.principal_hash,
        "channel_hash": snapshot.channel_hash,
        "tools": snapshot.tools.iter().map(|tool| {
            json!({
                "name": tool.name,
                "description_hash": tool.description_hash,
                "internal_schema_hash": tool.internal_schema_hash,
                "provider_schema_hash": tool.provider_schema_hash,
                "exposure_reason": tool.exposure_reason,
                "approval_posture": tool.approval_posture.as_str(),
                "projection_policy": tool.projection_policy.as_str(),
                "parallelism_policy": tool.parallelism_policy.as_str(),
            })
        }).collect::<Vec<_>>(),
        "filtered_tools": snapshot.filtered_tools.iter().map(|tool| {
            json!({
                "name": tool.name,
                "reason_code": tool.reason_code.as_str(),
                "repair_hint": tool.repair_hint,
            })
        }).collect::<Vec<_>>(),
    });
    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_owned())
}

/// Returns the registry projection policy for `tool_name`, defaulting unknown
/// tools to the most restrictive policy (redacted preview plus artifact).
pub(crate) fn projection_policy_for_tool(tool_name: &str) -> ToolResultProjectionPolicy {
    registry_entry(tool_name)
        .map(|entry| entry.projection_policy)
        .unwrap_or(ToolResultProjectionPolicy::RedactedPreviewAndArtifact)
}

/// Reports whether the runtime dependency backing `tool_name` is enabled;
/// tools without a dedicated runtime are always available.
fn runtime_available(
    config: &ToolCallConfig,
    browser_service_enabled: bool,
    tool_name: &str,
) -> bool {
    match tool_name {
        "palyra.process.run"
        | "palyra.process.stop"
        | "palyra.process.status"
        | "palyra.process.list" => config.process_runner.enabled,
        "palyra.plugin.run" => config.wasm_runtime.enabled,
        tool if tool.starts_with("palyra.browser.") => browser_service_enabled,
        _ => true,
    }
}

/// Trims, lowercases, and dedups the configured allowlist.
fn normalized_allowlist(allowed_tools: &[String]) -> BTreeSet<String> {
    let mut tools = BTreeSet::new();
    for tool in allowed_tools {
        let tool = tool.trim().to_ascii_lowercase();
        if tool.is_empty() {
            continue;
        }
        // Allowing process.run implies its lifecycle companions so that
        // background processes can always be stopped and inspected.
        if tool == "palyra.process.run" {
            tools.insert("palyra.process.stop".to_owned());
            tools.insert("palyra.process.status".to_owned());
            tools.insert("palyra.process.list".to_owned());
        }
        tools.insert(tool);
    }
    tools
}

/// Union of every builtin registry name and every allowlisted name, used to
/// report a complete filtered set when the tool budget is exhausted.
fn all_registry_and_allowlist_names<'a>(
    allowlist: impl Iterator<Item = &'a str>,
) -> BTreeSet<String> {
    let mut names = registry_entries().into_iter().map(|entry| entry.name).collect::<BTreeSet<_>>();
    names.extend(allowlist.map(ToOwned::to_owned));
    names
}
