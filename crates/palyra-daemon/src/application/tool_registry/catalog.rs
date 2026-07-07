//! Builds the per-provider-turn model-visible tool catalog snapshot.
//!
//! Builtin registry entries pass through allowlist, surface, runtime
//! availability, and provider-schema gates; both survivors and filtered
//! entries are sorted deterministically and content-hashed so identical
//! inputs always yield the same `snapshot_id`/`catalog_hash`.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Mutex, OnceLock},
};

use palyra_common::tool_catalog::ToolCatalogExposureMode;
use serde_json::{json, Value};

use crate::sandbox_runner::EgressEnforcementMode;
use crate::tool_protocol::ToolCallConfig;

use super::builtin::{registry_entries, registry_entry};
use super::hashing::{
    canonical_json_bytes, catalog_hash_payload, stable_hash_bytes, stable_hash_value,
};
use super::schema::{
    exposure_reason, filtered, provider_tool_payload, sanitize_schema_for_provider_with_audit,
};
use super::types::{
    AvailabilityProbeResult, FilteredToolCatalogEntry, ModelVisibleTool,
    ModelVisibleToolCatalogSnapshot, ToolCatalogBridgeError, ToolCatalogBuildRequest,
    ToolCatalogFilterReasonCode, ToolCatalogIndex, ToolCatalogIndexEntry, ToolCatalogInvokeTarget,
    ToolRegistryEntry, ToolResultProjectionPolicy, ToolSchemaDialect, ToolSchemaTransformAudit,
    TOOL_CATALOG_DESCRIBE_TOOL_NAME, TOOL_CATALOG_INVOKE_TOOL_NAME, TOOL_CATALOG_SCHEMA_VERSION,
    TOOL_CATALOG_SEARCH_TOOL_NAME,
};

const AVAILABILITY_PROBE_TTL_MS: u64 = 30_000;
const LAST_GOOD_GRACE_MS: u64 = 120_000;

/// Builds the catalog snapshot describing exactly which tools one provider
/// turn may see, including filter records for every excluded tool.
///
/// Principal and channel identifiers are hashed before they enter the
/// snapshot so the catalog never carries raw identity strings.
pub(crate) fn build_model_visible_tool_catalog_snapshot(
    request: ToolCatalogBuildRequest<'_>,
) -> ModelVisibleToolCatalogSnapshot {
    build_model_visible_tool_catalog_snapshot_with_external_tools(request, &[])
}

/// Builds a catalog snapshot with additional externally-discovered tools.
///
/// External entries are filtered through the same allowlist, surface, runtime,
/// provider-schema, deterministic ordering, and catalog hashing gates as
/// builtins. Callers are responsible for namespacing external tools before
/// import so provider-visible names cannot collide with builtins.
pub(crate) fn build_model_visible_tool_catalog_snapshot_with_external_tools(
    request: ToolCatalogBuildRequest<'_>,
    external_tools: &[ToolRegistryEntry],
) -> ModelVisibleToolCatalogSnapshot {
    build_model_visible_tool_catalog_snapshot_with_external_records(
        request,
        external_tools,
        &[],
        &[],
    )
}

/// Builds a catalog snapshot with external tools plus external availability evidence.
///
/// `external_registered_names` prevents allowlisted but currently unavailable
/// external tools from also being reported as unknown names.
pub(crate) fn build_model_visible_tool_catalog_snapshot_with_external_records(
    request: ToolCatalogBuildRequest<'_>,
    external_tools: &[ToolRegistryEntry],
    external_filtered_tools: &[FilteredToolCatalogEntry],
    external_registered_names: &[String],
) -> ModelVisibleToolCatalogSnapshot {
    let dialect = ToolSchemaDialect::from_provider_kind(request.provider_kind);
    let mut filtered_tools = external_filtered_tools.to_vec();
    let allowed_tools = normalized_configured_tools(
        request.catalog_policy.profile_expansion.effective_allowed_tools.as_slice(),
    );
    let availability_probes = runtime_availability_probes(
        request.config,
        request.browser_service_enabled,
        request.browser_service_configured,
        request.created_at_unix_ms,
    );
    let mut authorized_target_tools = Vec::new();
    let mut registry = registry_entries();
    registry.extend(external_tools.iter().cloned());
    let mut registered_names =
        registry.iter().map(|entry| entry.name.clone()).collect::<BTreeSet<_>>();
    registered_names.extend(external_registered_names.iter().cloned());

    for entry in registry {
        let entry = runtime_adjusted_registry_entry(entry, request.config);
        if is_catalog_bridge_tool(entry.name.as_str()) {
            continue;
        }
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
        if !runtime_available(availability_probes.as_slice(), entry.name.as_str()) {
            filtered_tools.push(filtered(
                entry.name.as_str(),
                ToolCatalogFilterReasonCode::RuntimeUnavailable,
                "enable the required runtime dependency before exposing this tool",
            ));
            continue;
        }
        let (provider_schema, provider_schema_transform) =
            match sanitize_schema_for_provider_with_audit(&entry.input_schema, dialect) {
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
        authorized_target_tools.push(visible_tool_from_entry(
            entry,
            provider_schema,
            provider_schema_transform,
        ));
    }

    // Surface allowlisted names with no registry entry so operators see
    // typos instead of tools silently never appearing.
    for allowed in &allowed_tools {
        if !registered_names.contains(allowed.as_str()) {
            filtered_tools.push(filtered(
                allowed.as_str(),
                ToolCatalogFilterReasonCode::UnknownTool,
                "remove the unknown tool from tool_call.allowed_tools or register metadata",
            ));
        }
    }

    for bridge_tool in catalog_bridge_tool_names() {
        if request.catalog_policy.exposure_mode == ToolCatalogExposureMode::Direct {
            filtered_tools.push(filtered(
                bridge_tool,
                ToolCatalogFilterReasonCode::SurfaceUnsupported,
                "set tool_call.catalog_exposure_mode to compact or hybrid before exposing catalog bridge tools",
            ));
        }
    }

    // Deterministic ordering and dedup: the catalog hash must not depend on
    // evaluation order, and a name can be filtered for one reason at most once.
    filtered_tools.sort_by(|left, right| {
        left.name.cmp(&right.name).then(left.reason_code.as_str().cmp(right.reason_code.as_str()))
    });
    filtered_tools
        .dedup_by(|left, right| left.name == right.name && left.reason_code == right.reason_code);
    authorized_target_tools.sort_by(|left, right| left.name.cmp(&right.name));

    let index = build_tool_catalog_index(authorized_target_tools.as_slice());
    let direct_tool_count = authorized_target_tools.len();
    let estimated_direct_tool_bytes =
        estimate_provider_tool_bytes(authorized_target_tools.as_slice(), dialect);
    let mut tools = expose_tools_for_policy(
        authorized_target_tools.as_slice(),
        request.catalog_policy.exposure_mode,
        request.catalog_policy.compact_tool_threshold,
        dialect,
    );
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    let exposed_names = tools.iter().map(|tool| tool.name.as_str()).collect::<BTreeSet<_>>();
    if request.catalog_policy.exposure_mode != ToolCatalogExposureMode::Direct {
        for tool in &authorized_target_tools {
            if !exposed_names.contains(tool.name.as_str()) {
                filtered_tools.push(filtered(
                    tool.name.as_str(),
                    ToolCatalogFilterReasonCode::PolicyInvisible,
                    "use palyra.tools.search, palyra.tools.describe, and palyra.tools.invoke for compact-catalog access",
                ));
            }
        }
        filtered_tools.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then(left.reason_code.as_str().cmp(right.reason_code.as_str()))
        });
        filtered_tools.dedup_by(|left, right| {
            left.name == right.name && left.reason_code == right.reason_code
        });
    }
    let estimated_exposed_tool_bytes = estimate_provider_tool_bytes(tools.as_slice(), dialect);
    let estimated_saved_bytes =
        estimated_direct_tool_bytes.saturating_sub(estimated_exposed_tool_bytes);

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
        profile_expansion: request.catalog_policy.profile_expansion.clone(),
        exposure_mode: request.catalog_policy.exposure_mode,
        compact_tool_threshold: request.catalog_policy.compact_tool_threshold,
        direct_tool_count,
        exposed_tool_count: tools.len(),
        estimated_direct_tool_bytes,
        estimated_exposed_tool_bytes,
        estimated_saved_bytes,
        availability_probes,
        index,
        indexed_tools: authorized_target_tools,
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

/// Executes a deterministic search over the compact catalog index.
pub(crate) fn search_tool_catalog_index(
    snapshot: &ModelVisibleToolCatalogSnapshot,
    input_json: &[u8],
) -> Result<Value, ToolCatalogBridgeError> {
    let input = parse_bridge_input(input_json)?;
    let query = required_string(&input, "query")?;
    let limit = input
        .get("limit")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(5)
        .clamp(1, 12);
    let hints = input
        .get("capability_hints")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .flat_map(search_tokens)
        .collect::<Vec<_>>();
    let query_tokens = search_tokens(query.as_str()).collect::<Vec<_>>();
    let mut ranked = snapshot
        .index
        .entries
        .iter()
        .map(|entry| {
            (entry, catalog_search_relevance(entry, query_tokens.as_slice(), hints.as_slice()))
        })
        .filter(|(_, relevance)| *relevance > 0)
        .collect::<Vec<_>>();
    if ranked.is_empty() && !query_tokens.is_empty() {
        ranked = snapshot.index.entries.iter().map(|entry| (entry, 0)).collect();
    }
    ranked.sort_by(|(left, left_score), (right, right_score)| {
        right_score.cmp(left_score).then(left.name.cmp(&right.name))
    });
    let results = ranked
        .into_iter()
        .take(limit)
        .map(|(entry, relevance)| {
            json!({
                "id": entry.name,
                "short_description": entry.short_description,
                "capability_class": entry.capability_class,
                "risk_tier": entry.risk_tier,
                "approval_summary": entry.approval_summary,
                "exposure_reason": entry.exposure_reason,
                "repair_hint": entry.repair_hint,
                "projection_policy": entry.projection_policy.as_str(),
                "schema_digest": entry.provider_schema_hash,
                "relevance": relevance,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "schema_version": 1,
        "query": query,
        "index_digest": snapshot.index.index_digest,
        "filtered_count": snapshot.index.entries.len().saturating_sub(results.len()),
        "results": results,
    }))
}

/// Describes one authorized indexed tool with its provider-compatible schema.
pub(crate) fn describe_catalog_tool(
    snapshot: &ModelVisibleToolCatalogSnapshot,
    input_json: &[u8],
) -> Result<Value, ToolCatalogBridgeError> {
    let input = parse_bridge_input(input_json)?;
    let tool_id = required_string(&input, "tool_id")?;
    let tool = indexed_tool(snapshot, tool_id.as_str())?;
    if let Some(expected_digest) = input.get("schema_digest").and_then(Value::as_str) {
        if !expected_digest.trim().is_empty() && expected_digest != tool.provider_schema_hash {
            return Err(bridge_error(
                "tool_catalog.schema_digest_mismatch",
                "requested schema_digest does not match the current catalog snapshot",
            ));
        }
    }
    Ok(json!({
        "schema_version": 1,
        "tool_id": tool.name,
        "description": tool.description,
        "provider_schema": tool.provider_schema,
        "schema_digest": tool.provider_schema_hash,
        "internal_schema_digest": tool.internal_schema_hash,
        "capabilities": tool.capabilities,
        "approval_required": matches!(
            tool.approval_posture,
            super::types::ToolApprovalPosture::ApprovalRequired
        ),
        "approval_summary": approval_summary(tool),
        "exposure_reason": tool.exposure_reason,
        "repair_hint": repair_hint_for_tool(tool),
        "projection_policy": tool.projection_policy.as_str(),
        "parallelism_policy": tool.parallelism_policy.as_str(),
        "safety_notes": safety_notes_for_tool(tool),
        "index_digest": snapshot.index.index_digest,
    }))
}

/// Resolves a compact invoke wrapper into the target tool and canonical arguments.
pub(crate) fn resolve_catalog_invoke_target(
    snapshot: &ModelVisibleToolCatalogSnapshot,
    input_json: &[u8],
) -> Result<ToolCatalogInvokeTarget, ToolCatalogBridgeError> {
    let input = parse_bridge_input(input_json)?;
    let tool_id = required_string(&input, "tool_id")?;
    let schema_digest = required_string(&input, "schema_digest")?;
    let tool = indexed_tool(snapshot, tool_id.as_str())?;
    if schema_digest != tool.provider_schema_hash {
        return Err(bridge_error(
            "tool_catalog.schema_digest_mismatch",
            "schema_digest must match the current catalog snapshot before invoking a compact tool",
        ));
    }
    if is_catalog_bridge_tool(tool.name.as_str()) {
        return Err(bridge_error(
            "tool_catalog.recursive_bridge_invoke",
            "catalog bridge tools cannot be invoked through palyra.tools.invoke",
        ));
    }
    let arguments = input.get("arguments").ok_or_else(|| {
        bridge_error("tool_catalog.arguments_missing", "arguments must be present")
    })?;
    if !arguments.is_object() {
        return Err(bridge_error(
            "tool_catalog.arguments_not_object",
            "arguments must be a JSON object",
        ));
    }
    Ok(ToolCatalogInvokeTarget {
        tool_name: tool.name.clone(),
        schema_digest,
        input_json: canonical_json_bytes(arguments),
    })
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
        "profile_expansion": {
            "profiles": snapshot.profile_expansion.profiles,
            "explicit_allowed_tools": snapshot.profile_expansion.explicit_allowed_tools,
            "extra_tools": snapshot.profile_expansion.extra_tools,
            "disabled_tools": snapshot.profile_expansion.disabled_tools,
            "effective_allowed_tools": snapshot.profile_expansion.effective_allowed_tools,
            "profile_expansions": snapshot.profile_expansion.profile_expansions.iter().map(|entry| {
                json!({
                    "profile": entry.profile,
                    "tools": entry.tools,
                })
            }).collect::<Vec<_>>(),
        },
        "exposure_mode": snapshot.exposure_mode.as_str(),
        "compact_tool_threshold": snapshot.compact_tool_threshold,
        "direct_tool_count": snapshot.direct_tool_count,
        "exposed_tool_count": snapshot.exposed_tool_count,
        "index_digest": snapshot.index.index_digest,
        "index_entry_count": snapshot.index.entries.len(),
        "estimated_direct_tool_bytes": snapshot.estimated_direct_tool_bytes,
        "estimated_exposed_tool_bytes": snapshot.estimated_exposed_tool_bytes,
        "estimated_saved_bytes": snapshot.estimated_saved_bytes,
        "availability_probes": snapshot.availability_probes.iter().map(|probe| {
            json!({
                "runtime": probe.runtime,
                "status": probe.status,
                "cache_status": probe.cache_status,
                "ttl_ms": probe.ttl_ms,
                "checked_at_unix_ms": probe.checked_at_unix_ms,
                "ttl_expires_unix_ms": probe.ttl_expires_unix_ms,
                "last_good_unix_ms": probe.last_good_unix_ms,
                "last_good_grace_until_unix_ms": probe.last_good_grace_until_unix_ms,
                "reason_code": probe.reason_code,
                "repair_hint": probe.repair_hint,
                "cache_key_hash": probe.cache_key_hash,
                "config_hash": probe.config_hash,
                "grace_allowed": probe.grace_allowed,
            })
        }).collect::<Vec<_>>(),
        "tools": snapshot.tools.iter().map(|tool| {
            json!({
                "name": tool.name,
                "description_hash": tool.description_hash,
                "internal_schema_hash": tool.internal_schema_hash,
                "provider_schema_hash": tool.provider_schema_hash,
                "exposure_reason": tool.exposure_reason,
                "repair_hint": repair_hint_for_tool(tool),
                "approval_posture": tool.approval_posture.as_str(),
                "projection_policy": tool.projection_policy.as_str(),
                "parallelism_policy": tool.parallelism_policy.as_str(),
            })
        }).collect::<Vec<_>>(),
        "filtered_tools": snapshot.filtered_tools.iter().map(|tool| {
            json!({
                "name": tool.name,
                "reason_code": tool.reason_code.as_str(),
                "external_reason_code": tool.external_reason_code,
                "repair_hint": tool.repair_hint,
            })
        }).collect::<Vec<_>>(),
        "effective_tool_surface": effective_tool_surface_report(snapshot),
    });
    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_owned())
}

/// Redacted per-run explanation for the effective tool surface.
///
/// The report is intentionally hash-heavy: it explains why each tool is
/// visible, compact-only, hidden, unavailable, or denied without copying
/// provider schemas or sensitive runtime inputs into support payloads.
pub(crate) fn effective_tool_surface_report(snapshot: &ModelVisibleToolCatalogSnapshot) -> Value {
    let exposed_names =
        snapshot.tools.iter().map(|tool| tool.name.as_str()).collect::<BTreeSet<_>>();
    let filtered_by_name = snapshot
        .filtered_tools
        .iter()
        .map(|tool| (tool.name.as_str(), tool))
        .collect::<BTreeMap<_, _>>();
    let indexed_names =
        snapshot.indexed_tools.iter().map(|tool| tool.name.as_str()).collect::<BTreeSet<_>>();
    let mut entries = Vec::new();

    for tool in &snapshot.indexed_tools {
        let filtered = filtered_by_name.get(tool.name.as_str()).copied();
        let compact_only = !exposed_names.contains(tool.name.as_str())
            && filtered.is_some_and(|entry| {
                entry.reason_code == ToolCatalogFilterReasonCode::PolicyInvisible
            });
        let status = if exposed_names.contains(tool.name.as_str()) {
            "visible"
        } else if compact_only {
            "compact_only"
        } else {
            "hidden"
        };
        entries.push(json!({
            "name": tool.name.as_str(),
            "status": status,
            "reason_code": filtered
                .map(|entry| entry.reason_code.as_str())
                .unwrap_or("tool_surface.visible"),
            "reason": filtered
                .map(|entry| entry.repair_hint.as_str())
                .unwrap_or(tool.exposure_reason.as_str()),
            "policy_source": "tool_call.allowed_tools",
            "runtime_status": "available",
            "provider_dialect": snapshot.provider_dialect.as_str(),
            "catalog_hash": snapshot.catalog_hash.as_str(),
            "index_digest": snapshot.index.index_digest.as_str(),
            "provider_schema_hash": tool.provider_schema_hash.as_str(),
            "internal_schema_hash": tool.internal_schema_hash.as_str(),
            "projection_policy": tool.projection_policy.as_str(),
            "approval_posture": tool.approval_posture.as_str(),
        }));
    }

    for filtered in &snapshot.filtered_tools {
        if indexed_names.contains(filtered.name.as_str()) {
            continue;
        }
        entries.push(json!({
            "name": filtered.name.as_str(),
            "status": surface_status_for_filter(filtered.reason_code),
            "reason_code": filtered.reason_code.as_str(),
            "reason": filtered.repair_hint.as_str(),
            "policy_source": "tool_call.allowed_tools",
            "runtime_status": runtime_status_for_filter(filtered.reason_code),
            "provider_dialect": snapshot.provider_dialect.as_str(),
            "catalog_hash": snapshot.catalog_hash.as_str(),
            "index_digest": snapshot.index.index_digest.as_str(),
            "provider_schema_hash": Value::Null,
            "internal_schema_hash": Value::Null,
        }));
    }

    entries.sort_by(|left, right| {
        left.get("name")
            .and_then(Value::as_str)
            .cmp(&right.get("name").and_then(Value::as_str))
            .then(
                left.get("status")
                    .and_then(Value::as_str)
                    .cmp(&right.get("status").and_then(Value::as_str)),
            )
    });
    json!({
        "schema_version": 1,
        "snapshot_id": snapshot.snapshot_id.as_str(),
        "catalog_hash": snapshot.catalog_hash.as_str(),
        "index_digest": snapshot.index.index_digest.as_str(),
        "exposure_mode": snapshot.exposure_mode.as_str(),
        "provider_dialect": snapshot.provider_dialect.as_str(),
        "entries": entries,
    })
}

fn surface_status_for_filter(reason_code: ToolCatalogFilterReasonCode) -> &'static str {
    match reason_code {
        ToolCatalogFilterReasonCode::RuntimeUnavailable
        | ToolCatalogFilterReasonCode::ProviderSchemaIncompatible => "unavailable",
        ToolCatalogFilterReasonCode::NotAllowlisted
        | ToolCatalogFilterReasonCode::UnknownTool
        | ToolCatalogFilterReasonCode::BudgetExhausted => "denied",
        ToolCatalogFilterReasonCode::SurfaceUnsupported
        | ToolCatalogFilterReasonCode::PolicyInvisible => "hidden",
    }
}

fn runtime_status_for_filter(reason_code: ToolCatalogFilterReasonCode) -> &'static str {
    match reason_code {
        ToolCatalogFilterReasonCode::RuntimeUnavailable => "unavailable",
        _ => "not_applicable",
    }
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
fn runtime_available(probes: &[AvailabilityProbeResult], tool_name: &str) -> bool {
    match tool_name {
        "palyra.process.run"
        | "palyra.process.input"
        | "palyra.process.send_keys"
        | "palyra.process.stop"
        | "palyra.process.status"
        | "palyra.process.list" => runtime_probe_available(probes, "process_runner"),
        "palyra.plugin.run" => runtime_probe_available(probes, "wasm_runtime"),
        tool if tool.starts_with("palyra.browser.") => {
            runtime_probe_available(probes, "browser_service")
                || runtime_probe_grace_available(probes, "browser_service")
        }
        _ => true,
    }
}

fn runtime_probe_available(probes: &[AvailabilityProbeResult], runtime: &str) -> bool {
    probes.iter().any(|probe| probe.runtime == runtime && probe.status == "available")
}

fn runtime_probe_grace_available(probes: &[AvailabilityProbeResult], runtime: &str) -> bool {
    probes.iter().any(|probe| {
        probe.runtime == runtime && probe.status == "last_good_grace" && probe.grace_allowed
    })
}

fn parse_bridge_input(
    input_json: &[u8],
) -> Result<serde_json::Map<String, Value>, ToolCatalogBridgeError> {
    let value = serde_json::from_slice::<Value>(input_json).map_err(|_| {
        bridge_error("tool_catalog.invalid_json", "bridge input must be valid JSON")
    })?;
    match value {
        Value::Object(map) => Ok(map),
        _ => {
            Err(bridge_error("tool_catalog.input_not_object", "bridge input must be a JSON object"))
        }
    }
}

fn required_string(
    input: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<String, ToolCatalogBridgeError> {
    let value = input.get(field).and_then(Value::as_str).map(str::trim).unwrap_or_default();
    if value.is_empty() {
        return Err(bridge_error(
            "tool_catalog.required_field_missing",
            format!("{field} must be a non-empty string").as_str(),
        ));
    }
    Ok(value.to_owned())
}

fn indexed_tool<'a>(
    snapshot: &'a ModelVisibleToolCatalogSnapshot,
    tool_id: &str,
) -> Result<&'a ModelVisibleTool, ToolCatalogBridgeError> {
    snapshot.indexed_tools.iter().find(|tool| tool.name == tool_id).ok_or_else(|| {
        bridge_error(
            "tool_catalog.tool_not_indexed",
            "tool_id is unknown or hidden in the current catalog snapshot",
        )
    })
}

fn bridge_error(reason_code: &str, message: &str) -> ToolCatalogBridgeError {
    ToolCatalogBridgeError { reason_code: reason_code.to_owned(), message: message.to_owned() }
}

fn visible_tool_from_entry(
    entry: ToolRegistryEntry,
    provider_schema: Value,
    provider_schema_transform: ToolSchemaTransformAudit,
) -> ModelVisibleTool {
    ModelVisibleTool {
        name: entry.name,
        description_hash: stable_hash_bytes(entry.description.as_bytes()),
        description: entry.description,
        version: entry.version,
        provenance: entry.provenance,
        provider_schema_hash: stable_hash_value(&provider_schema),
        provider_schema_transform,
        internal_schema_hash: entry.schema_hash,
        schema: entry.input_schema,
        provider_schema,
        capabilities: entry.capabilities,
        approval_posture: entry.approval_posture,
        projection_policy: entry.projection_policy,
        parallelism_policy: entry.parallelism_policy,
        replay_safety_class: entry.replay_safety_class,
        exposure_reason: exposure_reason(entry.approval_posture).to_owned(),
    }
}

fn expose_tools_for_policy(
    authorized_target_tools: &[ModelVisibleTool],
    exposure_mode: ToolCatalogExposureMode,
    compact_tool_threshold: usize,
    dialect: ToolSchemaDialect,
) -> Vec<ModelVisibleTool> {
    match exposure_mode {
        ToolCatalogExposureMode::Direct => authorized_target_tools.to_vec(),
        ToolCatalogExposureMode::Compact => catalog_bridge_tools(dialect),
        ToolCatalogExposureMode::Hybrid => {
            let mut exposed = authorized_target_tools
                .iter()
                .take(compact_tool_threshold)
                .cloned()
                .collect::<Vec<_>>();
            exposed.extend(catalog_bridge_tools(dialect));
            exposed
        }
    }
}

fn catalog_bridge_tools(dialect: ToolSchemaDialect) -> Vec<ModelVisibleTool> {
    catalog_bridge_tool_names()
        .iter()
        .filter_map(|tool_name| registry_entry(tool_name))
        .filter_map(|entry| {
            sanitize_schema_for_provider_with_audit(&entry.input_schema, dialect).ok().map(
                |(provider_schema, audit)| visible_tool_from_entry(entry, provider_schema, audit),
            )
        })
        .collect()
}

fn catalog_bridge_tool_names() -> &'static [&'static str] {
    &[TOOL_CATALOG_SEARCH_TOOL_NAME, TOOL_CATALOG_DESCRIBE_TOOL_NAME, TOOL_CATALOG_INVOKE_TOOL_NAME]
}

fn is_catalog_bridge_tool(tool_name: &str) -> bool {
    catalog_bridge_tool_names().contains(&tool_name)
}

fn build_tool_catalog_index(tools: &[ModelVisibleTool]) -> ToolCatalogIndex {
    let mut entries = tools.iter().map(index_entry_for_tool).collect::<Vec<_>>();
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    let mut index = ToolCatalogIndex { schema_version: 1, index_digest: String::new(), entries };
    index.index_digest = stable_hash_value(&json!({
        "schema_version": index.schema_version,
        "entries": index.entries,
    }));
    index
}

fn index_entry_for_tool(tool: &ModelVisibleTool) -> ToolCatalogIndexEntry {
    ToolCatalogIndexEntry {
        name: tool.name.clone(),
        short_description: short_description(tool.description.as_str()),
        keywords: tool_keywords(tool),
        capability_class: capability_class(tool),
        risk_tier: tool.approval_posture.as_str().to_owned(),
        approval_summary: approval_summary(tool),
        exposure_reason: tool.exposure_reason.clone(),
        repair_hint: repair_hint_for_tool(tool),
        projection_policy: tool.projection_policy,
        replay_safety_class: tool.replay_safety_class,
        provider_schema_hash: tool.provider_schema_hash.clone(),
        internal_schema_hash: tool.internal_schema_hash.clone(),
    }
}

fn short_description(description: &str) -> String {
    let first_sentence = description.split('.').next().unwrap_or(description).trim();
    if first_sentence.len() <= 180 {
        return first_sentence.to_owned();
    }
    first_sentence.chars().take(177).collect::<String>() + "..."
}

fn capability_class(tool: &ModelVisibleTool) -> String {
    if tool.capabilities.is_empty() {
        "general".to_owned()
    } else {
        tool.capabilities.join("+")
    }
}

fn approval_summary(tool: &ModelVisibleTool) -> String {
    match tool.approval_posture {
        super::types::ToolApprovalPosture::Safe => "no approval required".to_owned(),
        super::types::ToolApprovalPosture::ApprovalRequired => {
            "approval required before execution".to_owned()
        }
    }
}

fn repair_hint_for_tool(tool: &ModelVisibleTool) -> String {
    if is_catalog_bridge_tool(tool.name.as_str()) {
        return "use palyra.tools.search to find a tool, palyra.tools.describe to fetch its schema_digest, then palyra.tools.invoke with that digest"
            .to_owned();
    }
    if matches!(tool.approval_posture, super::types::ToolApprovalPosture::ApprovalRequired) {
        return "request approval before execution, or choose a read-only lower-risk tool when approval is unavailable"
            .to_owned();
    }
    if matches!(
        tool.projection_policy,
        ToolResultProjectionPolicy::SummarizeAndArtifact
            | ToolResultProjectionPolicy::RedactedPreviewAndArtifact
    ) {
        return "large or sensitive results return a summary plus artifact metadata; use palyra.artifact.read for bounded local preview reads"
            .to_owned();
    }
    "call with arguments matching provider_schema; if hidden by compact catalog, use palyra.tools.describe before invoking".to_owned()
}

fn safety_notes_for_tool(tool: &ModelVisibleTool) -> Vec<String> {
    let mut notes = Vec::new();
    if tool.capabilities.is_empty() {
        notes.push("general safe tool; still subject to runtime policy".to_owned());
    } else {
        notes.push(format!("capabilities: {}", tool.capabilities.join(", ")));
    }
    if matches!(tool.approval_posture, super::types::ToolApprovalPosture::ApprovalRequired) {
        notes.push("approval gate applies before execution".to_owned());
    }
    notes.push(format!("result projection: {}", tool.projection_policy.as_str()));
    notes
}

fn catalog_search_relevance(
    entry: &ToolCatalogIndexEntry,
    query_tokens: &[String],
    hints: &[String],
) -> i32 {
    let mut score = 0_i32;
    let haystack = format!(
        "{} {} {} {} {}",
        entry.name,
        entry.short_description,
        entry.keywords.join(" "),
        entry.capability_class,
        entry.risk_tier
    )
    .to_ascii_lowercase();
    for token in query_tokens {
        if entry.name.contains(token) {
            score += 8;
        }
        if entry.keywords.iter().any(|keyword| keyword == token) {
            score += 4;
        }
        if haystack.contains(token) {
            score += 2;
        }
    }
    for hint in hints {
        if entry.capability_class.contains(hint)
            || entry.keywords.iter().any(|keyword| keyword == hint)
        {
            score += 3;
        }
    }
    score
}

fn search_tokens(input: &str) -> impl Iterator<Item = String> + '_ {
    input
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.')
        .map(str::trim)
        .filter(|token| token.len() >= 2)
        .map(str::to_ascii_lowercase)
}

fn tool_keywords(tool: &ModelVisibleTool) -> Vec<String> {
    let mut keywords = Vec::new();
    let mut seen = BTreeSet::new();
    for source in std::iter::once(tool.name.as_str())
        .chain(tool.description.split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_'))
        .chain(tool.capabilities.iter().map(String::as_str))
    {
        let keyword = source.trim().to_ascii_lowercase();
        if keyword.len() < 3 || !seen.insert(keyword.clone()) {
            continue;
        }
        keywords.push(keyword);
        if keywords.len() == 24 {
            break;
        }
    }
    keywords
}

fn runtime_adjusted_registry_entry(
    mut entry: ToolRegistryEntry,
    config: &ToolCallConfig,
) -> ToolRegistryEntry {
    if entry.name == "palyra.process.run"
        && !matches!(
            config.process_runner.egress_enforcement_mode,
            EgressEnforcementMode::Preflight
        )
    {
        remove_object_schema_property(&mut entry.input_schema, "requested_egress_hosts");
        entry.schema_hash = stable_hash_value(&entry.input_schema);
        entry.description = format!(
            "{} Current process-runner egress profile is '{}', so requested_egress_hosts is hidden for this run; omit it for ordinary local commands, use palyra.http.fetch or browser tools for network retrieval, or enable tool_call.process_runner.egress_enforcement_mode='preflight' for host preflight checks.",
            entry.description,
            config.process_runner.egress_enforcement_mode.as_str()
        );
    }
    entry
}

fn remove_object_schema_property(schema: &mut Value, property: &str) {
    if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) {
        properties.remove(property);
    }
}

fn estimate_provider_tool_bytes(tools: &[ModelVisibleTool], dialect: ToolSchemaDialect) -> usize {
    tools
        .iter()
        .map(|tool| {
            serde_json::to_vec(&provider_tool_payload(tool, dialect)).map_or(0, |v| v.len())
        })
        .sum()
}

fn runtime_availability_probes(
    config: &ToolCallConfig,
    browser_service_enabled: bool,
    browser_service_configured: bool,
    created_at_unix_ms: i64,
) -> Vec<AvailabilityProbeResult> {
    vec![
        runtime_probe(
            "process_runner",
            config.process_runner.enabled,
            runtime_config_hash(
                "process_runner",
                json!({
                    "enabled": config.process_runner.enabled,
                    "allowed_tools": config.allowed_tools.clone(),
                    "tier": format!("{:?}", config.process_runner.tier),
                    "path_access_mode": format!("{:?}", config.process_runner.path_access_mode),
                    "allow_interpreters": config.process_runner.allow_interpreters,
                    "egress_enforcement_mode": format!("{:?}", config.process_runner.egress_enforcement_mode),
                    "allowed_executables": config.process_runner.allowed_executables.clone(),
                    "allowed_egress_hosts": config.process_runner.allowed_egress_hosts.clone(),
                    "allowed_dns_suffixes": config.process_runner.allowed_dns_suffixes.clone(),
                }),
            ),
            "tool_call.process_runner.enabled=false",
            "enable tool_call.process_runner before exposing process tools",
            false,
            created_at_unix_ms,
        ),
        runtime_probe(
            "wasm_runtime",
            config.wasm_runtime.enabled,
            runtime_config_hash(
                "wasm_runtime",
                json!({
                    "enabled": config.wasm_runtime.enabled,
                    "allowed_tools": config.allowed_tools.clone(),
                    "allow_inline_modules": config.wasm_runtime.allow_inline_modules,
                    "max_module_size_bytes": config.wasm_runtime.max_module_size_bytes,
                    "fuel_budget": config.wasm_runtime.fuel_budget,
                    "max_memory_bytes": config.wasm_runtime.max_memory_bytes,
                    "max_table_elements": config.wasm_runtime.max_table_elements,
                    "max_instances": config.wasm_runtime.max_instances,
                    "allowed_http_hosts": config.wasm_runtime.allowed_http_hosts.clone(),
                    "allowed_storage_prefixes": config.wasm_runtime.allowed_storage_prefixes.clone(),
                    "allowed_channels": config.wasm_runtime.allowed_channels.clone(),
                }),
            ),
            "tool_call.wasm_runtime.enabled=false",
            "enable tool_call.wasm_runtime before exposing plugin tools",
            false,
            created_at_unix_ms,
        ),
        runtime_probe(
            "browser_service",
            browser_service_enabled,
            runtime_config_hash(
                "browser_service",
                json!({
                    "configured": browser_service_configured,
                    "allowed_tools": config.allowed_tools.clone(),
                }),
            ),
            "tool_call.browser_service.enabled=false",
            "enable and start browserd before exposing browser tools",
            browser_service_configured,
            created_at_unix_ms,
        ),
    ]
}

fn runtime_probe(
    runtime: &str,
    available: bool,
    config_hash: String,
    unavailable_reason: &str,
    repair_hint: &str,
    grace_allowed: bool,
    created_at_unix_ms: i64,
) -> AvailabilityProbeResult {
    let cache_key = format!("{runtime}:{config_hash}");
    let mut cache = availability_probe_cache().lock().expect("availability probe cache poisoned");
    if let Some(cached) = cache.get(cache_key.as_str()) {
        if created_at_unix_ms <= cached.result.ttl_expires_unix_ms {
            let mut result = cached.result.clone();
            result.cache_status = "cached".to_owned();
            return result;
        }
    }

    let last_good_unix_ms =
        cache.get(cache_key.as_str()).and_then(|cached| cached.result.last_good_unix_ms);
    let grace_until = last_good_unix_ms.map(|last_good| {
        last_good.saturating_add(i64::try_from(LAST_GOOD_GRACE_MS).unwrap_or(i64::MAX))
    });
    let use_last_good_grace =
        !available && grace_allowed && grace_until.is_some_and(|until| created_at_unix_ms <= until);
    let (status, reason_code, repair_hint) = if available {
        ("available", "runtime.available", "runtime is available".to_owned())
    } else if use_last_good_grace {
        (
            "last_good_grace",
            "runtime.last_good_grace",
            format!(
                "runtime is currently unavailable; using last good probe until {}",
                grace_until.unwrap_or(created_at_unix_ms)
            ),
        )
    } else {
        ("unavailable", unavailable_reason, repair_hint.to_owned())
    };
    let ttl_expires_unix_ms = created_at_unix_ms
        .saturating_add(i64::try_from(AVAILABILITY_PROBE_TTL_MS).unwrap_or(i64::MAX));
    let result = AvailabilityProbeResult {
        runtime: runtime.to_owned(),
        status: status.to_owned(),
        cache_status: "refreshed".to_owned(),
        ttl_ms: AVAILABILITY_PROBE_TTL_MS,
        checked_at_unix_ms: created_at_unix_ms,
        ttl_expires_unix_ms,
        last_good_unix_ms: if available { Some(created_at_unix_ms) } else { last_good_unix_ms },
        last_good_grace_until_unix_ms: grace_until,
        reason_code: reason_code.to_owned(),
        repair_hint,
        cache_key_hash: stable_hash_bytes(cache_key.as_bytes()),
        config_hash,
        grace_allowed,
    };
    cache.insert(cache_key, AvailabilityProbeCacheEntry { result: result.clone() });
    result
}

fn runtime_config_hash(runtime: &str, value: Value) -> String {
    stable_hash_bytes(format!("{runtime}:{}", stable_hash_value(&value)).as_bytes())
}

#[derive(Clone)]
struct AvailabilityProbeCacheEntry {
    result: AvailabilityProbeResult,
}

fn availability_probe_cache() -> &'static Mutex<BTreeMap<String, AvailabilityProbeCacheEntry>> {
    static CACHE: OnceLock<Mutex<BTreeMap<String, AvailabilityProbeCacheEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn clear_availability_probe_cache_for_tests() {
    availability_probe_cache().lock().expect("availability probe cache poisoned").clear();
}

fn normalized_configured_tools(allowed_tools: &[String]) -> BTreeSet<String> {
    allowed_tools
        .iter()
        .map(|tool| tool.trim().to_ascii_lowercase())
        .filter(|tool| !tool.is_empty())
        .collect()
}
