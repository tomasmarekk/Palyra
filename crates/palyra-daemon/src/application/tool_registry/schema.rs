use serde_json::{json, Map, Value};

use super::hashing::sort_json_value;
use super::types::{
    FilteredToolCatalogEntry, ModelVisibleTool, ToolApprovalPosture, ToolCatalogFilterReasonCode,
    ToolSchemaDialect, MAX_SCHEMA_DEPTH, MAX_SCHEMA_PROPERTIES,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SchemaCompatibilityError {
    pub(super) reason_code: String,
    pub(super) message: String,
}

pub(super) fn sanitize_schema_for_provider(
    schema: &Value,
    dialect: ToolSchemaDialect,
) -> Result<Value, SchemaCompatibilityError> {
    let mut sanitized = schema.clone();
    normalize_schema_for_provider(&mut sanitized, dialect, 0)?;
    validate_schema_subset(&sanitized, dialect, 0)?;
    sort_json_value(&mut sanitized);
    Ok(sanitized)
}

fn normalize_schema_for_provider(
    schema: &mut Value,
    dialect: ToolSchemaDialect,
    depth: usize,
) -> Result<(), SchemaCompatibilityError> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(schema_error(
            "schema.depth_exceeded",
            "tool schema nesting exceeds provider gate",
        ));
    }
    let Some(object) = schema.as_object_mut() else {
        return Ok(());
    };

    object.remove("default");
    downlevel_exclusive_numeric_bounds(object);

    if dialect == ToolSchemaDialect::Anthropic
        && object.get("additionalProperties").is_some_and(Value::is_object)
    {
        object.insert("additionalProperties".to_owned(), Value::Bool(true));
    }

    if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
        for property_schema in properties.values_mut() {
            normalize_schema_for_provider(property_schema, dialect, depth.saturating_add(1))?;
        }
    }
    if let Some(items) = object.get_mut("items") {
        normalize_schema_for_provider(items, dialect, depth.saturating_add(1))?;
    }
    if dialect != ToolSchemaDialect::Anthropic {
        if let Some(additional) =
            object.get_mut("additionalProperties").filter(|value| value.is_object())
        {
            normalize_schema_for_provider(additional, dialect, depth.saturating_add(1))?;
        }
    }

    Ok(())
}

fn downlevel_exclusive_numeric_bounds(object: &mut Map<String, Value>) {
    if let Some(bound) = object.remove("exclusiveMinimum") {
        object.entry("minimum".to_owned()).or_insert(bound);
    }
    if let Some(bound) = object.remove("exclusiveMaximum") {
        object.entry("maximum".to_owned()).or_insert(bound);
    }
}

fn validate_schema_subset(
    schema: &Value,
    dialect: ToolSchemaDialect,
    depth: usize,
) -> Result<(), SchemaCompatibilityError> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(schema_error(
            "schema.depth_exceeded",
            "tool schema nesting exceeds provider gate",
        ));
    }
    let object = schema
        .as_object()
        .ok_or_else(|| schema_error("schema.not_object", "tool schema nodes must be objects"))?;
    let allowed_keywords = [
        "type",
        "description",
        "properties",
        "required",
        "items",
        "additionalProperties",
        "enum",
        "minimum",
        "maximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "maxProperties",
    ];
    for keyword in object.keys() {
        if !allowed_keywords.contains(&keyword.as_str()) {
            return Err(schema_error(
                "schema.unsupported_keyword",
                format!("unsupported JSON schema keyword '{keyword}'").as_str(),
            ));
        }
    }
    let schema_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| schema_error("schema.type_missing", "schema nodes must declare type"))?;
    if !matches!(schema_type, "object" | "array" | "string" | "integer" | "number" | "boolean") {
        return Err(schema_error("schema.type_unsupported", "schema type is unsupported"));
    }
    if let Some(required) = object.get("required") {
        let required = required
            .as_array()
            .ok_or_else(|| schema_error("schema.required_invalid", "required must be an array"))?;
        for field in required {
            if field.as_str().is_none_or(str::is_empty) {
                return Err(schema_error(
                    "schema.required_invalid",
                    "required fields must be non-empty strings",
                ));
            }
        }
    }
    if let Some(enum_values) = object.get("enum") {
        let enum_values = enum_values
            .as_array()
            .ok_or_else(|| schema_error("schema.enum_invalid", "enum must be an array"))?;
        if enum_values.is_empty() {
            return Err(schema_error("schema.enum_invalid", "enum must not be empty"));
        }
    }
    if let Some(max_properties) = object.get("maxProperties") {
        if max_properties.as_u64().is_none() {
            return Err(schema_error(
                "schema.max_properties_invalid",
                "maxProperties must be an unsigned integer",
            ));
        }
    }
    if let Some(additional) = object.get("additionalProperties") {
        match additional {
            Value::Bool(_) => {}
            Value::Object(_) if dialect != ToolSchemaDialect::Anthropic => {
                validate_schema_subset(additional, dialect, depth.saturating_add(1))?;
            }
            Value::Object(_) => {
                return Err(schema_error(
                    "schema.additional_properties_schema_unsupported",
                    "anthropic tool schemas require boolean additionalProperties",
                ));
            }
            _ => {
                return Err(schema_error(
                    "schema.additional_properties_invalid",
                    "additionalProperties must be boolean or a schema object",
                ));
            }
        }
    }
    if schema_type == "object" {
        let empty_properties = Map::new();
        let has_additional_property_schema =
            object.get("additionalProperties").is_some_and(Value::is_object);
        let properties = match object.get("properties").and_then(Value::as_object) {
            Some(properties) => properties,
            None if object.get("additionalProperties").and_then(Value::as_bool) == Some(true) => {
                &empty_properties
            }
            None if has_additional_property_schema => &empty_properties,
            None => {
                return Err(schema_error(
                    "schema.properties_missing",
                    "object schema needs properties",
                ));
            }
        };
        if properties.len() > MAX_SCHEMA_PROPERTIES {
            return Err(schema_error(
                "schema.properties_exceeded",
                "tool schema declares too many properties",
            ));
        }
        for property_schema in properties.values() {
            validate_schema_subset(property_schema, dialect, depth.saturating_add(1))?;
        }
    }
    if schema_type == "array" {
        let items = object
            .get("items")
            .ok_or_else(|| schema_error("schema.items_missing", "array schema needs items"))?;
        validate_schema_subset(items, dialect, depth.saturating_add(1))?;
    }
    Ok(())
}

fn schema_error(reason_code: &str, message: &str) -> SchemaCompatibilityError {
    SchemaCompatibilityError { reason_code: reason_code.to_owned(), message: message.to_owned() }
}

pub(super) fn provider_tool_payload(tool: &ModelVisibleTool, dialect: ToolSchemaDialect) -> Value {
    match dialect {
        ToolSchemaDialect::Anthropic => json!({
            "name": tool.name,
            "description": tool.description,
            "input_schema": tool.provider_schema,
        }),
        ToolSchemaDialect::OpenAiCompatible | ToolSchemaDialect::Deterministic => json!({
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.provider_schema,
            }
        }),
    }
}

pub(super) fn filtered(
    name: &str,
    reason_code: ToolCatalogFilterReasonCode,
    repair_hint: &str,
) -> FilteredToolCatalogEntry {
    FilteredToolCatalogEntry {
        name: name.to_owned(),
        reason_code,
        repair_hint: repair_hint.to_owned(),
    }
}

pub(super) fn exposure_reason(approval_posture: ToolApprovalPosture) -> &'static str {
    match approval_posture {
        ToolApprovalPosture::Safe => "allowlisted_policy_visible",
        ToolApprovalPosture::ApprovalRequired => "allowlisted_policy_visible_approval_required",
    }
}
