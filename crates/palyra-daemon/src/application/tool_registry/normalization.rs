use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Number, Value};

use crate::tool_protocol::ToolExecutionOutcome;

use super::hashing::{canonical_json_bytes, stable_hash_bytes};
use super::types::{
    ModelVisibleToolCatalogSnapshot, NormalizedToolCall, ToolArgumentNormalizationAudit,
    ToolArgumentNormalizationStep, ToolCallRejection, ToolCallRejectionKind,
    ToolCatalogFilterReasonCode, ToolParallelismPolicy, TOOL_REJECTION_SCHEMA_VERSION,
};

#[allow(clippy::result_large_err)]
pub(crate) fn validate_tool_call_against_catalog_snapshot(
    snapshot: &ModelVisibleToolCatalogSnapshot,
    tool_name: &str,
    input_json: &[u8],
) -> Result<NormalizedToolCall, ToolCallRejection> {
    let raw_json_hash = stable_hash_bytes(input_json);
    let Some(tool) = snapshot.tools.iter().find(|tool| tool.name == tool_name) else {
        match snapshot.filtered_tools.iter().find(|tool| tool.name == tool_name) {
            Some(filtered)
                if matches!(
                    filtered.reason_code,
                    ToolCatalogFilterReasonCode::NotAllowlisted
                        | ToolCatalogFilterReasonCode::UnknownTool
                ) =>
            {
                return normalize_legacy_policy_passthrough(snapshot, tool_name, input_json);
            }
            None => return normalize_legacy_policy_passthrough(snapshot, tool_name, input_json),
            Some(_) => {}
        }
        return Err(rejection_for_missing_snapshot_tool(snapshot, tool_name, raw_json_hash));
    };
    if tool.parallelism_policy != ToolParallelismPolicy::ReadOnly
        && snapshot.remaining_tool_budget == 0
    {
        return Err(ToolCallRejection {
            schema_version: TOOL_REJECTION_SCHEMA_VERSION,
            kind: ToolCallRejectionKind::UnsupportedParallelism,
            tool_name: tool_name.to_owned(),
            reason_code: "tool_call.parallelism_budget_exhausted".to_owned(),
            message: "tool call was not executable under the provider-turn budget".to_owned(),
            raw_json_hash,
            snapshot_id: Some(snapshot.snapshot_id.clone()),
            catalog_hash: Some(snapshot.catalog_hash.clone()),
        });
    }

    let raw_value =
        serde_json::from_slice::<Value>(input_json).map_err(|error| ToolCallRejection {
            schema_version: TOOL_REJECTION_SCHEMA_VERSION,
            kind: ToolCallRejectionKind::MalformedArguments,
            tool_name: tool_name.to_owned(),
            reason_code: "tool_call.arguments.invalid_json".to_owned(),
            message: format!("tool arguments must be valid JSON object: {error}"),
            raw_json_hash: raw_json_hash.clone(),
            snapshot_id: Some(snapshot.snapshot_id.clone()),
            catalog_hash: Some(snapshot.catalog_hash.clone()),
        })?;
    if !raw_value.is_object() {
        return Err(ToolCallRejection {
            schema_version: TOOL_REJECTION_SCHEMA_VERSION,
            kind: ToolCallRejectionKind::MalformedArguments,
            tool_name: tool_name.to_owned(),
            reason_code: "tool_call.arguments.not_object".to_owned(),
            message: "tool arguments must be a JSON object".to_owned(),
            raw_json_hash,
            snapshot_id: Some(snapshot.snapshot_id.clone()),
            catalog_hash: Some(snapshot.catalog_hash.clone()),
        });
    }

    let mut steps = Vec::new();
    let raw_value = normalize_tool_specific_argument_aliases(tool_name, raw_value, &mut steps);
    let normalized_value =
        normalize_value_against_schema(raw_value, &tool.schema, "", None, &mut steps).map_err(
            |message| ToolCallRejection {
                schema_version: TOOL_REJECTION_SCHEMA_VERSION,
                kind: ToolCallRejectionKind::MalformedArguments,
                tool_name: tool_name.to_owned(),
                reason_code: "tool_call.arguments.schema_mismatch".to_owned(),
                message,
                raw_json_hash: raw_json_hash.clone(),
                snapshot_id: Some(snapshot.snapshot_id.clone()),
                catalog_hash: Some(snapshot.catalog_hash.clone()),
            },
        )?;
    let input_json = canonical_json_bytes(&normalized_value);
    let normalized_json_hash = stable_hash_bytes(input_json.as_slice());
    Ok(NormalizedToolCall {
        input_json,
        audit: ToolArgumentNormalizationAudit { raw_json_hash, normalized_json_hash, steps },
    })
}

#[allow(clippy::result_large_err)]
fn normalize_legacy_policy_passthrough(
    snapshot: &ModelVisibleToolCatalogSnapshot,
    tool_name: &str,
    input_json: &[u8],
) -> Result<NormalizedToolCall, ToolCallRejection> {
    let raw_json_hash = stable_hash_bytes(input_json);
    let raw_value =
        serde_json::from_slice::<Value>(input_json).map_err(|error| ToolCallRejection {
            schema_version: TOOL_REJECTION_SCHEMA_VERSION,
            kind: ToolCallRejectionKind::MalformedArguments,
            tool_name: tool_name.to_owned(),
            reason_code: "tool_call.arguments.invalid_json".to_owned(),
            message: format!("tool arguments must be valid JSON object: {error}"),
            raw_json_hash: raw_json_hash.clone(),
            snapshot_id: Some(snapshot.snapshot_id.clone()),
            catalog_hash: Some(snapshot.catalog_hash.clone()),
        })?;
    if !raw_value.is_object() {
        return Err(ToolCallRejection {
            schema_version: TOOL_REJECTION_SCHEMA_VERSION,
            kind: ToolCallRejectionKind::MalformedArguments,
            tool_name: tool_name.to_owned(),
            reason_code: "tool_call.arguments.not_object".to_owned(),
            message: "tool arguments must be a JSON object".to_owned(),
            raw_json_hash,
            snapshot_id: Some(snapshot.snapshot_id.clone()),
            catalog_hash: Some(snapshot.catalog_hash.clone()),
        });
    }
    let input_json = canonical_json_bytes(&raw_value);
    let normalized_json_hash = stable_hash_bytes(input_json.as_slice());
    Ok(NormalizedToolCall {
        input_json,
        audit: ToolArgumentNormalizationAudit {
            raw_json_hash,
            normalized_json_hash,
            steps: Vec::new(),
        },
    })
}

fn normalize_tool_specific_argument_aliases(
    tool_name: &str,
    value: Value,
    steps: &mut Vec<ToolArgumentNormalizationStep>,
) -> Value {
    if tool_name != "palyra.fs.apply_patch" {
        return value;
    }
    normalize_apply_patch_raw_alias(value, steps)
}

fn normalize_apply_patch_raw_alias(
    value: Value,
    steps: &mut Vec<ToolArgumentNormalizationStep>,
) -> Value {
    let mut input = match value {
        Value::Object(input) => input,
        other => return other,
    };
    if input.contains_key("patch") {
        return Value::Object(input);
    }

    let Some(raw) = input.remove("raw") else {
        return Value::Object(input);
    };
    match raw {
        Value::String(raw_text) => {
            let raw_parameters = parse_top_level_xmlish_parameters(raw_text.as_str());
            let patch = raw_parameters
                .as_ref()
                .and_then(|parameters| parameters.get("patch"))
                .cloned()
                .unwrap_or_else(|| raw_text.trim().to_owned());
            if !patch.is_empty() {
                input.insert("patch".to_owned(), Value::String(patch));
                steps.push(ToolArgumentNormalizationStep {
                    json_pointer: "/raw".to_owned(),
                    from_type: "string".to_owned(),
                    to_type: "object.patch".to_owned(),
                    reason_code: "tool_call.arguments.apply_patch_raw_alias".to_owned(),
                });
            }
            if !input.contains_key("workspace_root") {
                if let Some(workspace_root) =
                    raw_parameters.as_ref().and_then(|parameters| parameters.get("workspace_root"))
                {
                    if !workspace_root.trim().is_empty() {
                        input.insert(
                            "workspace_root".to_owned(),
                            Value::String(workspace_root.clone()),
                        );
                        steps.push(ToolArgumentNormalizationStep {
                            json_pointer: "/raw".to_owned(),
                            from_type: "string".to_owned(),
                            to_type: "object.workspace_root".to_owned(),
                            reason_code: "tool_call.arguments.apply_patch_raw_workspace_root_alias"
                                .to_owned(),
                        });
                    }
                }
            }
        }
        Value::Object(mut raw_object) => {
            if let Some(patch) = raw_object.remove("patch").filter(Value::is_string) {
                input.insert("patch".to_owned(), patch);
                steps.push(ToolArgumentNormalizationStep {
                    json_pointer: "/raw/patch".to_owned(),
                    from_type: "string".to_owned(),
                    to_type: "object.patch".to_owned(),
                    reason_code: "tool_call.arguments.apply_patch_nested_patch_alias".to_owned(),
                });
            }
            if !input.contains_key("workspace_root") {
                if let Some(workspace_root) =
                    raw_object.remove("workspace_root").filter(Value::is_string)
                {
                    input.insert("workspace_root".to_owned(), workspace_root);
                    steps.push(ToolArgumentNormalizationStep {
                        json_pointer: "/raw/workspace_root".to_owned(),
                        from_type: "string".to_owned(),
                        to_type: "object.workspace_root".to_owned(),
                        reason_code: "tool_call.arguments.apply_patch_nested_workspace_root_alias"
                            .to_owned(),
                    });
                }
            }
        }
        other => {
            input.insert("raw".to_owned(), other);
        }
    }
    Value::Object(input)
}

fn parse_top_level_xmlish_parameters(input: &str) -> Option<BTreeMap<String, String>> {
    let mut remaining = input.trim();
    if remaining.is_empty() {
        return None;
    }

    let mut parameters = BTreeMap::new();
    while !remaining.is_empty() {
        let (name, value_start) = parse_xmlish_parameter_opening(remaining)?;
        if parameters.contains_key(name.as_str()) {
            return None;
        }
        let value_body = &remaining[value_start..];
        let value_end = value_body.find("</parameter>")?;
        parameters.insert(name, value_body[..value_end].trim().to_owned());
        remaining = value_body[value_end + "</parameter>".len()..].trim_start();
    }

    (!parameters.is_empty()).then_some(parameters)
}

fn parse_xmlish_parameter_opening(input: &str) -> Option<(String, usize)> {
    let prefix = "<parameter name=";
    let after_prefix = input.strip_prefix(prefix)?;
    let quote = after_prefix.as_bytes().first().copied()?;
    if !matches!(quote, b'\'' | b'"') {
        return None;
    }
    let after_open_quote = &after_prefix[1..];
    let name_end = after_open_quote.find(char::from(quote))?;
    let name = after_open_quote[..name_end].to_owned();
    let after_name = &after_open_quote[name_end + 1..];
    if !after_name.starts_with('>') || name.is_empty() {
        return None;
    }

    Some((name, prefix.len() + 1 + name_end + 1 + 1))
}

pub(crate) fn tool_call_rejection_error_payload(rejection: &ToolCallRejection) -> Value {
    json!({
        "schema_version": rejection.schema_version,
        "error": {
            "kind": rejection.kind.as_str(),
            "reason_code": rejection.reason_code,
            "message": rejection.message,
            "tool_name": rejection.tool_name,
            "raw_json_hash": rejection.raw_json_hash,
            "snapshot_id": rejection.snapshot_id,
            "catalog_hash": rejection.catalog_hash,
        }
    })
}

pub(crate) fn tool_call_rejection_outcome(
    proposal_id: &str,
    input_json: &[u8],
    rejection: &ToolCallRejection,
) -> ToolExecutionOutcome {
    let output_json = serde_json::to_vec(&tool_call_rejection_error_payload(rejection))
        .unwrap_or_else(|_| b"{}".to_vec());
    crate::tool_protocol::build_tool_execution_outcome(
        proposal_id,
        rejection.tool_name.as_str(),
        input_json,
        false,
        output_json,
        format!("{}: {}", rejection.kind.as_str(), rejection.message),
        false,
        "tool_intake".to_owned(),
        "schema_snapshot".to_owned(),
    )
}

pub(crate) fn normalization_audit_tape_payload(
    proposal_id: &str,
    tool_name: &str,
    audit: &ToolArgumentNormalizationAudit,
) -> String {
    serde_json::to_string(&json!({
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "raw_json_hash": audit.raw_json_hash,
        "normalized_json_hash": audit.normalized_json_hash,
        "steps": audit.steps,
    }))
    .unwrap_or_else(|_| "{}".to_owned())
}

pub(crate) fn rejection_tape_payload(proposal_id: &str, rejection: &ToolCallRejection) -> String {
    serde_json::to_string(&json!({
        "proposal_id": proposal_id,
        "schema_version": rejection.schema_version,
        "kind": rejection.kind.as_str(),
        "tool_name": rejection.tool_name,
        "reason_code": rejection.reason_code,
        "message": rejection.message,
        "raw_json_hash": rejection.raw_json_hash,
        "snapshot_id": rejection.snapshot_id,
        "catalog_hash": rejection.catalog_hash,
    }))
    .unwrap_or_else(|_| "{}".to_owned())
}

fn rejection_for_missing_snapshot_tool(
    snapshot: &ModelVisibleToolCatalogSnapshot,
    tool_name: &str,
    raw_json_hash: String,
) -> ToolCallRejection {
    let maybe_filtered = snapshot.filtered_tools.iter().find(|tool| tool.name == tool_name);
    let (kind, reason_code, message) = match maybe_filtered.map(|tool| tool.reason_code) {
        Some(ToolCatalogFilterReasonCode::NotAllowlisted) => (
            ToolCallRejectionKind::PolicyInvisible,
            "tool_call.not_exposed.not_allowlisted",
            "tool was not exposed to the model because it is not allowlisted",
        ),
        Some(ToolCatalogFilterReasonCode::RuntimeUnavailable) => (
            ToolCallRejectionKind::UnavailableTool,
            "tool_call.not_exposed.runtime_unavailable",
            "tool was not exposed to the model because its runtime is unavailable",
        ),
        Some(ToolCatalogFilterReasonCode::ProviderSchemaIncompatible) => (
            ToolCallRejectionKind::SchemaInvalid,
            "tool_call.not_exposed.schema_incompatible",
            "tool was not exposed to the model because its schema is incompatible",
        ),
        Some(ToolCatalogFilterReasonCode::SurfaceUnsupported) => (
            ToolCallRejectionKind::UnavailableTool,
            "tool_call.not_exposed.surface_unsupported",
            "tool was not exposed to the model on this surface",
        ),
        Some(ToolCatalogFilterReasonCode::BudgetExhausted) => (
            ToolCallRejectionKind::UnavailableTool,
            "tool_call.not_exposed.budget_exhausted",
            "tool was not exposed to the model because tool budget is exhausted",
        ),
        Some(ToolCatalogFilterReasonCode::PolicyInvisible) => (
            ToolCallRejectionKind::PolicyInvisible,
            "tool_call.not_exposed.policy_invisible",
            "tool was not exposed to the model by policy",
        ),
        Some(ToolCatalogFilterReasonCode::UnknownTool) | None => (
            ToolCallRejectionKind::UnknownTool,
            "tool_call.unknown_tool",
            "tool was not present in the provider-turn tool catalog snapshot",
        ),
    };
    ToolCallRejection {
        schema_version: TOOL_REJECTION_SCHEMA_VERSION,
        kind,
        tool_name: tool_name.to_owned(),
        reason_code: reason_code.to_owned(),
        message: message.to_owned(),
        raw_json_hash,
        snapshot_id: Some(snapshot.snapshot_id.clone()),
        catalog_hash: Some(snapshot.catalog_hash.clone()),
    }
}

fn normalize_value_against_schema(
    value: Value,
    schema: &Value,
    path: &str,
    property_name: Option<&str>,
    steps: &mut Vec<ToolArgumentNormalizationStep>,
) -> Result<Value, String> {
    let schema_object =
        schema.as_object().ok_or_else(|| format!("schema at {path} is not an object"))?;
    let schema_type = schema_object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("schema at {path} is missing type"))?;
    let normalized = match schema_type {
        "object" => normalize_object(value, schema_object, path, steps)?,
        "array" => normalize_array(value, schema_object, path, steps)?,
        "string" => normalize_string(value, path)?,
        "integer" => normalize_integer(value, path, property_name, steps)?,
        "number" => normalize_number(value, path, property_name, steps)?,
        "boolean" => normalize_boolean(value, path, property_name, steps)?,
        other => return Err(format!("schema at {path} has unsupported type {other}")),
    };
    validate_enum(&normalized, schema_object, path)?;
    validate_bounds(&normalized, schema_object, path)?;
    Ok(normalized)
}

fn normalize_object(
    value: Value,
    schema: &Map<String, Value>,
    path: &str,
    steps: &mut Vec<ToolArgumentNormalizationStep>,
) -> Result<Value, String> {
    let path_label = path_or_root(path);
    let Value::Object(input) = value else {
        return Err(format!("{path_label} must be an object"));
    };
    if let Some(max_properties) = schema.get("maxProperties").and_then(Value::as_u64) {
        if input.len() as u64 > max_properties {
            return Err(format!("{path_label} exceeds maxProperties {max_properties}"));
        }
    }
    let allow_additional =
        schema.get("additionalProperties").and_then(Value::as_bool).unwrap_or(false);
    let additional_schema = schema.get("additionalProperties").filter(|value| value.is_object());
    let empty_properties = Map::new();
    let properties = match schema.get("properties").and_then(Value::as_object) {
        Some(properties) => properties,
        None if allow_additional => &empty_properties,
        None if additional_schema.is_some() => &empty_properties,
        None => return Err(format!("object schema at {path_label} is missing properties")),
    };
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|values| {
            values.iter().filter_map(Value::as_str).map(ToOwned::to_owned).collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    for required_field in &required {
        if !input.contains_key(required_field) {
            return Err(format!("{path_label} missing required field '{required_field}'"));
        }
    }
    let mut normalized = Map::new();
    for (key, value) in input {
        let child_path = format!("{}/{}", path, escape_json_pointer(key.as_str()));
        if let Some(property_schema) = properties.get(key.as_str()) {
            normalized.insert(
                key.clone(),
                normalize_value_against_schema(
                    value,
                    property_schema,
                    child_path.as_str(),
                    Some(key.as_str()),
                    steps,
                )?,
            );
        } else if let Some(additional_schema) = additional_schema {
            normalized.insert(
                key.clone(),
                normalize_value_against_schema(
                    value,
                    additional_schema,
                    child_path.as_str(),
                    Some(key.as_str()),
                    steps,
                )?,
            );
        } else if allow_additional {
            normalized.insert(key, value);
        } else {
            return Err(format!("{path_label} contains unsupported field '{key}'"));
        }
    }
    Ok(Value::Object(normalized))
}

fn normalize_array(
    value: Value,
    schema: &Map<String, Value>,
    path: &str,
    steps: &mut Vec<ToolArgumentNormalizationStep>,
) -> Result<Value, String> {
    let path_label = path_or_root(path);
    let Value::Array(values) = value else {
        return Err(format!("{path_label} must be an array"));
    };
    if let Some(max_items) = schema.get("maxItems").and_then(Value::as_u64) {
        if values.len() as u64 > max_items {
            return Err(format!("{path_label} exceeds maxItems {max_items}"));
        }
    }
    let item_schema = schema
        .get("items")
        .ok_or_else(|| format!("array schema at {path_label} is missing items"))?;
    let mut normalized = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        let child_path = format!("{path}/{index}");
        normalized.push(normalize_value_against_schema(
            value,
            item_schema,
            child_path.as_str(),
            None,
            steps,
        )?);
    }
    Ok(Value::Array(normalized))
}

fn normalize_string(value: Value, path: &str) -> Result<Value, String> {
    if value.is_string() {
        Ok(value)
    } else {
        Err(format!("{} must be a string", path_or_root(path)))
    }
}

fn normalize_integer(
    value: Value,
    path: &str,
    property_name: Option<&str>,
    steps: &mut Vec<ToolArgumentNormalizationStep>,
) -> Result<Value, String> {
    let path_label = path_or_root(path);
    match value {
        Value::Number(number) if number.as_i64().is_some() || number.as_u64().is_some() => {
            Ok(Value::Number(number))
        }
        Value::String(text) if safe_scalar_coercion_allowed(path, property_name) => {
            let parsed = text
                .trim()
                .parse::<i64>()
                .map_err(|_| format!("{path_label} must be an integer; string coercion failed"))?;
            steps.push(ToolArgumentNormalizationStep {
                json_pointer: path_label.to_owned(),
                from_type: "string".to_owned(),
                to_type: "integer".to_owned(),
                reason_code: "schema_safe_scalar_coercion".to_owned(),
            });
            Ok(Value::Number(Number::from(parsed)))
        }
        _ => Err(format!("{path_label} must be an integer")),
    }
}

fn normalize_number(
    value: Value,
    path: &str,
    property_name: Option<&str>,
    steps: &mut Vec<ToolArgumentNormalizationStep>,
) -> Result<Value, String> {
    let path_label = path_or_root(path);
    match value {
        Value::Number(number) => Ok(Value::Number(number)),
        Value::String(text) if safe_scalar_coercion_allowed(path, property_name) => {
            let parsed = text
                .trim()
                .parse::<f64>()
                .map_err(|_| format!("{path_label} must be a number; string coercion failed"))?;
            if !parsed.is_finite() {
                return Err(format!("{path_label} must be a finite number"));
            }
            let Some(number) = Number::from_f64(parsed) else {
                return Err(format!("{path_label} must be a finite number"));
            };
            steps.push(ToolArgumentNormalizationStep {
                json_pointer: path_label.to_owned(),
                from_type: "string".to_owned(),
                to_type: "number".to_owned(),
                reason_code: "schema_safe_scalar_coercion".to_owned(),
            });
            Ok(Value::Number(number))
        }
        _ => Err(format!("{path_label} must be a number")),
    }
}

fn normalize_boolean(
    value: Value,
    path: &str,
    property_name: Option<&str>,
    steps: &mut Vec<ToolArgumentNormalizationStep>,
) -> Result<Value, String> {
    let path_label = path_or_root(path);
    match value {
        Value::Bool(_) => Ok(value),
        Value::String(text) if safe_scalar_coercion_allowed(path, property_name) => {
            let normalized = match text.trim().to_ascii_lowercase().as_str() {
                "true" => true,
                "false" => false,
                _ => {
                    return Err(format!("{path_label} must be a boolean; string coercion failed"));
                }
            };
            steps.push(ToolArgumentNormalizationStep {
                json_pointer: path_label.to_owned(),
                from_type: "string".to_owned(),
                to_type: "boolean".to_owned(),
                reason_code: "schema_safe_scalar_coercion".to_owned(),
            });
            Ok(Value::Bool(normalized))
        }
        _ => Err(format!("{path_label} must be a boolean")),
    }
}

fn validate_enum(value: &Value, schema: &Map<String, Value>, path: &str) -> Result<(), String> {
    let Some(enum_values) = schema.get("enum").and_then(Value::as_array) else {
        return Ok(());
    };
    if enum_values.iter().any(|candidate| candidate == value) {
        Ok(())
    } else {
        Err(format!("{} is not an allowed enum value", path_or_root(path)))
    }
}

fn validate_bounds(value: &Value, schema: &Map<String, Value>, path: &str) -> Result<(), String> {
    let Some(number) = value.as_f64() else {
        return Ok(());
    };
    if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64) {
        if number < minimum {
            return Err(format!("{} must be >= {minimum}", path_or_root(path)));
        }
    }
    if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64) {
        if number > maximum {
            return Err(format!("{} must be <= {maximum}", path_or_root(path)));
        }
    }
    Ok(())
}

fn safe_scalar_coercion_allowed(path: &str, property_name: Option<&str>) -> bool {
    let combined = format!(
        "{} {}",
        path.to_ascii_lowercase(),
        property_name.unwrap_or_default().to_ascii_lowercase()
    );
    ![
        "path",
        "secret",
        "token",
        "key",
        "credential",
        "command",
        "arg",
        "body",
        "url",
        "header",
        "module",
        "patch",
        "content",
    ]
    .iter()
    .any(|needle| combined.contains(needle))
}

fn path_or_root(path: &str) -> &str {
    if path.is_empty() {
        "/"
    } else {
        path
    }
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}
