//! Provider-facing JSON schema sanitization and subset validation.
//!
//! Internal registry schemas are richer than what providers accept, so each
//! schema is first normalized for the target dialect (unsupported keywords
//! stripped or downleveled) and then validated against a closed keyword
//! subset. Incompatible tools are filtered out of the catalog with a reason
//! code rather than shipped with a schema a provider could silently mangle.

use std::collections::BTreeSet;

use serde_json::{json, Map, Value};

use super::hashing::{sort_json_value, stable_hash_value};
use super::types::{
    FilteredToolCatalogEntry, ModelVisibleTool, ToolApprovalPosture, ToolCatalogFilterReasonCode,
    ToolSchemaDialect, ToolSchemaTransformAudit, ToolSchemaTransformStep, MAX_SCHEMA_DEPTH,
    MAX_SCHEMA_PROPERTIES, TOOL_SCHEMA_TRANSFORM_AUDIT_VERSION,
};

/// Why a tool schema cannot be expressed in the target provider dialect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaCompatibilityError {
    pub(crate) reason_code: String,
    pub(crate) message: String,
}

/// Produces the provider-safe form of an internal tool schema.
///
/// Validation runs after normalization so downleveled keywords are judged in
/// their provider form; key sorting runs last to keep `provider_schema_hash`
/// canonical.
///
/// # Errors
/// Returns [`SchemaCompatibilityError`] when the schema exceeds the depth or
/// property gates, or uses constructs outside the supported provider subset.
pub(crate) fn sanitize_schema_for_provider(
    schema: &Value,
    dialect: ToolSchemaDialect,
) -> Result<Value, SchemaCompatibilityError> {
    sanitize_schema_for_provider_with_audit(schema, dialect).map(|(schema, _audit)| schema)
}

/// Produces the provider-safe schema plus a hash-only transform audit.
///
/// # Errors
/// Returns [`SchemaCompatibilityError`] when a schema construct cannot be
/// normalized without changing the privilege or validation boundary.
pub(crate) fn sanitize_schema_for_provider_with_audit(
    schema: &Value,
    dialect: ToolSchemaDialect,
) -> Result<(Value, ToolSchemaTransformAudit), SchemaCompatibilityError> {
    let input_schema_hash = stable_hash_value(schema);
    let mut sanitized = schema.clone();
    let profile = schema_transform_profile_for_dialect(dialect);
    let mut steps = Vec::new();
    normalize_schema_for_provider(&mut sanitized, dialect, profile, 0, "", &mut steps)?;
    validate_schema_subset(&sanitized, dialect, 0)?;
    sort_json_value(&mut sanitized);
    let output_schema_hash = stable_hash_value(&sanitized);
    Ok((
        sanitized,
        ToolSchemaTransformAudit {
            schema_version: TOOL_SCHEMA_TRANSFORM_AUDIT_VERSION,
            dialect,
            input_schema_hash,
            output_schema_hash,
            steps,
        },
    ))
}

#[derive(Debug, Clone, Copy)]
struct ToolSchemaDialectTransformProfile {
    top_level_composition: bool,
    nullable_unions: bool,
    enum_type_inference: bool,
    redundant_not_null: bool,
    anthropic_boolean_additional_properties: bool,
}

fn schema_transform_profile_for_dialect(
    dialect: ToolSchemaDialect,
) -> ToolSchemaDialectTransformProfile {
    ToolSchemaDialectTransformProfile {
        top_level_composition: true,
        nullable_unions: true,
        enum_type_inference: true,
        redundant_not_null: true,
        anthropic_boolean_additional_properties: dialect == ToolSchemaDialect::Anthropic,
    }
}

fn normalize_schema_for_provider(
    schema: &mut Value,
    dialect: ToolSchemaDialect,
    profile: ToolSchemaDialectTransformProfile,
    depth: usize,
    path: &str,
    steps: &mut Vec<ToolSchemaTransformStep>,
) -> Result<(), SchemaCompatibilityError> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(schema_error(
            "schema.depth_exceeded",
            "tool schema nesting exceeds provider gate",
        ));
    }
    normalize_schema_composition(schema, profile, path, steps)?;
    let Some(object) = schema.as_object_mut() else {
        return Ok(());
    };

    normalize_nullable_type_union(object, profile, path, steps)?;
    infer_type_from_enum(object, profile, path, steps)?;
    remove_null_enum_value(object, path, steps)?;

    // `default` is outside the supported provider subset and intake
    // normalization never applies defaults, so dropping it is lossless.
    if object.remove("default").is_some() {
        steps.push(schema_transform_step(path, "schema.default_removed", "default", "omitted"));
    }
    downlevel_exclusive_numeric_bounds(object, path, steps);
    remove_redundant_not_null(object, profile, path, steps);

    // Anthropic tool schemas only accept boolean additionalProperties; widen
    // an object-valued schema to `true` instead of filtering the whole tool.
    if profile.anthropic_boolean_additional_properties
        && object.get("additionalProperties").is_some_and(Value::is_object)
    {
        object.insert("additionalProperties".to_owned(), Value::Bool(true));
        steps.push(schema_transform_step(
            append_json_pointer(path, "additionalProperties").as_str(),
            "schema.anthropic_additional_properties_widened",
            "schema",
            "boolean",
        ));
    }

    if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
        for (property_name, property_schema) in properties {
            let property_path = append_json_pointer(
                append_json_pointer(path, "properties").as_str(),
                property_name,
            );
            normalize_schema_for_provider(
                property_schema,
                dialect,
                profile,
                depth.saturating_add(1),
                property_path.as_str(),
                steps,
            )?;
        }
    }
    if let Some(items) = object.get_mut("items") {
        let items_path = append_json_pointer(path, "items");
        normalize_schema_for_provider(
            items,
            dialect,
            profile,
            depth.saturating_add(1),
            items_path.as_str(),
            steps,
        )?;
    }
    if dialect != ToolSchemaDialect::Anthropic {
        if let Some(additional) =
            object.get_mut("additionalProperties").filter(|value| value.is_object())
        {
            let additional_path = append_json_pointer(path, "additionalProperties");
            normalize_schema_for_provider(
                additional,
                dialect,
                profile,
                depth.saturating_add(1),
                additional_path.as_str(),
                steps,
            )?;
        }
    }

    Ok(())
}

fn normalize_schema_composition(
    schema: &mut Value,
    profile: ToolSchemaDialectTransformProfile,
    path: &str,
    steps: &mut Vec<ToolSchemaTransformStep>,
) -> Result<(), SchemaCompatibilityError> {
    if !profile.top_level_composition || !path.is_empty() {
        return Ok(());
    }
    let Some(object) = schema.as_object().cloned() else {
        return Ok(());
    };
    for keyword in ["oneOf", "anyOf"] {
        if object.contains_key(keyword) {
            *schema = collapse_single_schema_union(&object, keyword)?;
            steps.push(schema_transform_step(
                path,
                composition_reason_code(keyword),
                keyword,
                "single_schema",
            ));
            return Ok(());
        }
    }
    if object.contains_key("allOf") {
        *schema = merge_all_of_schema(&object)?;
        steps.push(schema_transform_step(path, "schema.all_of_merged", "allOf", "schema"));
    }
    Ok(())
}

fn collapse_single_schema_union(
    object: &Map<String, Value>,
    keyword: &str,
) -> Result<Value, SchemaCompatibilityError> {
    let branches = object.get(keyword).and_then(Value::as_array).ok_or_else(|| {
        schema_error("schema.composition_invalid", "composition must be an array")
    })?;
    let concrete = branches.iter().filter(|branch| !is_null_schema(branch)).collect::<Vec<_>>();
    if concrete.len() != 1 {
        return Err(schema_error(
            composition_ambiguous_reason_code(keyword),
            "composition has multiple possible non-null schemas",
        ));
    }
    let mut merged = concrete[0].as_object().cloned().ok_or_else(|| {
        schema_error("schema.composition_invalid", "composition branch must be an object")
    })?;
    for (key, value) in object {
        if key == keyword {
            continue;
        }
        merge_schema_field(&mut merged, key, value)?;
    }
    Ok(Value::Object(merged))
}

fn merge_all_of_schema(object: &Map<String, Value>) -> Result<Value, SchemaCompatibilityError> {
    let branches = object
        .get("allOf")
        .and_then(Value::as_array)
        .ok_or_else(|| schema_error("schema.composition_invalid", "allOf must be an array"))?;
    if branches.is_empty() {
        return Err(schema_error("schema.composition_invalid", "allOf must not be empty"));
    }
    let mut merged = Map::new();
    for (key, value) in object {
        if key != "allOf" {
            merge_schema_field(&mut merged, key, value)?;
        }
    }
    for branch in branches {
        let branch = branch.as_object().ok_or_else(|| {
            schema_error("schema.composition_invalid", "allOf branches must be objects")
        })?;
        for (key, value) in branch {
            merge_schema_field(&mut merged, key, value)?;
        }
    }
    Ok(Value::Object(merged))
}

fn merge_schema_field(
    target: &mut Map<String, Value>,
    key: &str,
    value: &Value,
) -> Result<(), SchemaCompatibilityError> {
    match (key, target.get_mut(key)) {
        ("properties", Some(Value::Object(existing))) => {
            let incoming = value.as_object().ok_or_else(|| {
                schema_error("schema.composition_conflict", "properties must be an object")
            })?;
            for (property_name, property_schema) in incoming {
                match existing.get(property_name) {
                    Some(current) if current != property_schema => {
                        return Err(schema_error(
                            "schema.composition_conflict",
                            "allOf properties contain conflicting schemas",
                        ));
                    }
                    Some(_) => {}
                    None => {
                        existing.insert(property_name.clone(), property_schema.clone());
                    }
                }
            }
        }
        ("required", Some(Value::Array(existing))) => {
            let incoming = value.as_array().ok_or_else(|| {
                schema_error("schema.composition_conflict", "required must be an array")
            })?;
            let mut required = existing
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<BTreeSet<_>>();
            required.extend(incoming.iter().filter_map(Value::as_str).map(ToOwned::to_owned));
            *existing = required.into_iter().map(Value::String).collect();
        }
        ("additionalProperties", Some(existing)) => {
            merge_additional_properties(existing, value)?;
        }
        ("description", Some(_)) => {}
        (_, Some(existing)) if existing == value => {}
        (_, Some(_)) => {
            return Err(schema_error(
                "schema.composition_conflict",
                "composition contains conflicting schema fields",
            ));
        }
        (_, None) => {
            target.insert(key.to_owned(), value.clone());
        }
    }
    Ok(())
}

fn merge_additional_properties(
    existing: &mut Value,
    incoming: &Value,
) -> Result<(), SchemaCompatibilityError> {
    match (&*existing, incoming) {
        (left, right) if left == right => Ok(()),
        (Value::Bool(true), Value::Bool(false)) => {
            *existing = Value::Bool(false);
            Ok(())
        }
        (Value::Bool(false), Value::Bool(true)) => Ok(()),
        _ => Err(schema_error(
            "schema.composition_conflict",
            "additionalProperties schemas conflict",
        )),
    }
}

fn normalize_nullable_type_union(
    object: &mut Map<String, Value>,
    profile: ToolSchemaDialectTransformProfile,
    path: &str,
    steps: &mut Vec<ToolSchemaTransformStep>,
) -> Result<(), SchemaCompatibilityError> {
    if !profile.nullable_unions {
        return Ok(());
    }
    let Some(Value::Array(types)) = object.get("type") else {
        return Ok(());
    };
    let mut non_null = Vec::new();
    let mut has_null = false;
    for value in types {
        match value.as_str() {
            Some("null") => has_null = true,
            Some(schema_type) => non_null.push(schema_type.to_owned()),
            None => {
                return Err(schema_error(
                    "schema.type_union_invalid",
                    "type unions must contain string type names",
                ));
            }
        }
    }
    if !has_null || non_null.len() != 1 {
        return Err(schema_error(
            "schema.type_union_unsupported",
            "only nullable single-type unions can be normalized",
        ));
    }
    object.insert("type".to_owned(), Value::String(non_null[0].clone()));
    steps.push(schema_transform_step(
        append_json_pointer(path, "type").as_str(),
        "schema.nullable_union_removed",
        "type_union",
        non_null[0].as_str(),
    ));
    Ok(())
}

fn infer_type_from_enum(
    object: &mut Map<String, Value>,
    profile: ToolSchemaDialectTransformProfile,
    path: &str,
    steps: &mut Vec<ToolSchemaTransformStep>,
) -> Result<(), SchemaCompatibilityError> {
    if !profile.enum_type_inference || object.contains_key("type") {
        return Ok(());
    }
    let Some(enum_values) = object.get("enum").and_then(Value::as_array) else {
        return Ok(());
    };
    let inferred_type = infer_enum_type(enum_values)?;
    object.insert("type".to_owned(), Value::String(inferred_type.to_owned()));
    steps.push(schema_transform_step(path, "schema.enum_type_inferred", "enum", inferred_type));
    Ok(())
}

fn infer_enum_type(enum_values: &[Value]) -> Result<&'static str, SchemaCompatibilityError> {
    let mut inferred: Option<&'static str> = None;
    for value in enum_values.iter().filter(|value| !value.is_null()) {
        let kind = enum_value_schema_type(value).ok_or_else(|| {
            schema_error("schema.enum_type_ambiguous", "enum values use unsupported JSON types")
        })?;
        inferred = match (inferred, kind) {
            (None, value) => Some(value),
            (Some("integer"), "number") | (Some("number"), "integer") => Some("number"),
            (Some(current), value) if current == value => Some(current),
            _ => {
                return Err(schema_error(
                    "schema.enum_type_ambiguous",
                    "enum values use multiple primitive types",
                ));
            }
        };
    }
    inferred.ok_or_else(|| {
        schema_error("schema.enum_type_ambiguous", "enum must not contain only null")
    })
}

fn enum_value_schema_type(value: &Value) -> Option<&'static str> {
    match value {
        Value::String(_) => Some("string"),
        Value::Bool(_) => Some("boolean"),
        Value::Number(number) if number.as_i64().is_some() || number.as_u64().is_some() => {
            Some("integer")
        }
        Value::Number(_) => Some("number"),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

fn remove_null_enum_value(
    object: &mut Map<String, Value>,
    path: &str,
    steps: &mut Vec<ToolSchemaTransformStep>,
) -> Result<(), SchemaCompatibilityError> {
    let Some(enum_values) = object.get_mut("enum").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    let before_len = enum_values.len();
    enum_values.retain(|value| !value.is_null());
    if enum_values.len() == before_len {
        return Ok(());
    }
    if enum_values.is_empty() {
        return Err(schema_error("schema.enum_invalid", "enum must not contain only null"));
    }
    steps.push(schema_transform_step(
        append_json_pointer(path, "enum").as_str(),
        "schema.enum_null_removed",
        "null",
        "omitted",
    ));
    Ok(())
}

fn remove_redundant_not_null(
    object: &mut Map<String, Value>,
    profile: ToolSchemaDialectTransformProfile,
    path: &str,
    steps: &mut Vec<ToolSchemaTransformStep>,
) {
    if !profile.redundant_not_null {
        return;
    }
    let has_non_null_type =
        object.get("type").and_then(Value::as_str).is_some_and(|schema_type| schema_type != "null");
    if has_non_null_type && object.get("not").is_some_and(is_null_schema) {
        object.remove("not");
        steps.push(schema_transform_step(
            append_json_pointer(path, "not").as_str(),
            "schema.redundant_not_null_removed",
            "not_null",
            "omitted",
        ));
    }
}

fn is_null_schema(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.get("type").and_then(Value::as_str) == Some("null"))
}

// exclusiveMinimum/exclusiveMaximum are outside the supported subset.
// Downleveling to inclusive bounds deliberately widens the range by one
// boundary value rather than dropping the bound entirely; an explicit
// inclusive bound, when present, wins.
fn downlevel_exclusive_numeric_bounds(
    object: &mut Map<String, Value>,
    path: &str,
    steps: &mut Vec<ToolSchemaTransformStep>,
) {
    if let Some(bound) = object.remove("exclusiveMinimum") {
        object.entry("minimum".to_owned()).or_insert(bound);
        steps.push(schema_transform_step(
            append_json_pointer(path, "exclusiveMinimum").as_str(),
            "schema.exclusive_minimum_downleveled",
            "exclusiveMinimum",
            "minimum",
        ));
    }
    if let Some(bound) = object.remove("exclusiveMaximum") {
        object.entry("maximum".to_owned()).or_insert(bound);
        steps.push(schema_transform_step(
            append_json_pointer(path, "exclusiveMaximum").as_str(),
            "schema.exclusive_maximum_downleveled",
            "exclusiveMaximum",
            "maximum",
        ));
    }
}

/// Recursively checks that a normalized schema stays within the closed
/// keyword/type subset every supported provider dialect understands.
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
    // Closed keyword set: anything outside it filters the tool with a reason
    // code instead of sending a schema a provider may silently ignore.
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
        // An object schema may omit `properties` only when additional
        // properties are accepted (open map); otherwise it would describe an
        // object that can never validate any input.
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

fn composition_reason_code(keyword: &str) -> &'static str {
    match keyword {
        "oneOf" => "schema.one_of_single_branch",
        "anyOf" => "schema.any_of_single_branch",
        _ => "schema.composition_single_branch",
    }
}

fn composition_ambiguous_reason_code(keyword: &str) -> &'static str {
    match keyword {
        "oneOf" => "schema.one_of_ambiguous",
        "anyOf" => "schema.any_of_ambiguous",
        _ => "schema.composition_ambiguous",
    }
}

fn schema_transform_step(
    path: &str,
    reason_code: &str,
    from: &str,
    to: &str,
) -> ToolSchemaTransformStep {
    ToolSchemaTransformStep {
        json_pointer: path_or_root(path).to_owned(),
        reason_code: reason_code.to_owned(),
        from: from.to_owned(),
        to: to.to_owned(),
    }
}

fn path_or_root(path: &str) -> &str {
    if path.is_empty() {
        "/"
    } else {
        path
    }
}

fn append_json_pointer(path: &str, segment: &str) -> String {
    let segment = escape_json_pointer(segment);
    if path.is_empty() {
        format!("/{segment}")
    } else {
        format!("{path}/{segment}")
    }
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

/// Renders one tool in the wire shape the provider dialect expects: bare
/// `input_schema` for Anthropic, the `function` wrapper for OpenAI-compatible
/// and deterministic providers.
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

/// Convenience constructor for a filtered-catalog entry.
pub(super) fn filtered(
    name: &str,
    reason_code: ToolCatalogFilterReasonCode,
    repair_hint: &str,
) -> FilteredToolCatalogEntry {
    FilteredToolCatalogEntry {
        name: name.to_owned(),
        reason_code,
        external_reason_code: None,
        repair_hint: repair_hint.to_owned(),
    }
}

/// Stable exposure-reason label recorded on every model-visible tool.
pub(super) fn exposure_reason(approval_posture: ToolApprovalPosture) -> &'static str {
    match approval_posture {
        ToolApprovalPosture::Safe => "allowlisted_policy_visible",
        ToolApprovalPosture::ApprovalRequired => "allowlisted_policy_visible_approval_required",
    }
}
