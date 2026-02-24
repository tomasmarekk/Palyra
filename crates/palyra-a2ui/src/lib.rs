use serde_json::{Map, Value};
use thiserror::Error;

pub const A2UI_PATCH_PROTOCOL_VERSION: u64 = 1;

const KNOWN_GOOD_JSON: &str =
    include_str!("../../../schemas/json/a2ui-placeholder-known-good.json");

#[derive(Debug, Error, PartialEq, Eq)]
pub enum A2uiValidationError {
    #[error("document must be valid JSON")]
    InvalidJson,
    #[error("document must be a JSON object")]
    NotAnObject,
    #[error("field '{0}' is required")]
    MissingField(&'static str),
    #[error("field '{0}' has an invalid type")]
    InvalidType(&'static str),
    #[error("field '{0}' cannot be empty")]
    EmptyField(&'static str),
    #[error("unsupported A2UI version")]
    UnsupportedVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchOperationKind {
    Add,
    Replace,
    Remove,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatchOperation {
    pub kind: PatchOperationKind,
    pub path: String,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PatchDocument {
    pub version: u64,
    pub operations: Vec<PatchOperation>,
}

#[derive(Debug, Error)]
pub enum A2uiPatchError {
    #[error("patch payload must be valid JSON")]
    InvalidJson,
    #[error("patch document must be a JSON object")]
    NotAnObject,
    #[error("patch field '{0}' is required")]
    MissingField(&'static str),
    #[error("patch field '{0}' has an invalid type")]
    InvalidType(&'static str),
    #[error("patch protocol version {0} is unsupported")]
    UnsupportedVersion(u64),
    #[error("patch operations cannot be empty")]
    EmptyOperations,
    #[error("patch operation at index {index} must be an object")]
    OperationNotObject { index: usize },
    #[error("patch operation at index {index} missing required field '{field}'")]
    MissingOperationField { index: usize, field: &'static str },
    #[error("patch operation at index {index} field '{field}' has an invalid type")]
    InvalidOperationFieldType { index: usize, field: &'static str },
    #[error("patch operation at index {index} has unsupported op '{op}'")]
    UnsupportedOperation { index: usize, op: String },
    #[error("patch operation at index {index} has invalid pointer '{path}': {reason}")]
    InvalidPointer { index: usize, path: String, reason: String },
    #[error("patch operation at index {index} conflicts at path '{path}': {reason}")]
    Conflict { index: usize, path: String, reason: String },
    #[error("failed to serialize patch document: {0}")]
    SerializePatch(#[from] serde_json::Error),
}

#[must_use]
pub fn known_good_document() -> Value {
    serde_json::from_str(KNOWN_GOOD_JSON).expect("known-good A2UI fixture must stay valid JSON")
}

pub fn parse_and_validate_document(input: &[u8]) -> Result<Value, A2uiValidationError> {
    let document: Value =
        serde_json::from_slice(input).map_err(|_| A2uiValidationError::InvalidJson)?;
    validate_document(&document)?;
    Ok(document)
}

pub fn validate_document(document: &Value) -> Result<(), A2uiValidationError> {
    let object = document.as_object().ok_or(A2uiValidationError::NotAnObject)?;

    validate_required_version(object)?;
    validate_required_string(object, "surface")?;
    validate_required_array(object, "components")?;
    Ok(())
}

pub fn parse_patch_document(input: &[u8]) -> Result<PatchDocument, A2uiPatchError> {
    let value: Value = serde_json::from_slice(input).map_err(|_| A2uiPatchError::InvalidJson)?;
    parse_patch_value(&value)
}

pub fn parse_patch_value(value: &Value) -> Result<PatchDocument, A2uiPatchError> {
    let object = value.as_object().ok_or(A2uiPatchError::NotAnObject)?;
    let version = object
        .get("v")
        .ok_or(A2uiPatchError::MissingField("v"))?
        .as_u64()
        .ok_or(A2uiPatchError::InvalidType("v"))?;
    if version != A2UI_PATCH_PROTOCOL_VERSION {
        return Err(A2uiPatchError::UnsupportedVersion(version));
    }
    let operations_raw = object
        .get("ops")
        .ok_or(A2uiPatchError::MissingField("ops"))?
        .as_array()
        .ok_or(A2uiPatchError::InvalidType("ops"))?;
    if operations_raw.is_empty() {
        return Err(A2uiPatchError::EmptyOperations);
    }

    let mut operations = Vec::with_capacity(operations_raw.len());
    for (index, entry) in operations_raw.iter().enumerate() {
        let operation_object =
            entry.as_object().ok_or(A2uiPatchError::OperationNotObject { index })?;
        let kind = parse_operation_kind(operation_object, index)?;
        let path = operation_object
            .get("path")
            .ok_or(A2uiPatchError::MissingOperationField { index, field: "path" })?
            .as_str()
            .ok_or(A2uiPatchError::InvalidOperationFieldType { index, field: "path" })?
            .to_owned();
        let _ = parse_pointer_tokens(path.as_str(), index)?;
        let value = operation_object.get("value").cloned();

        match kind {
            PatchOperationKind::Add | PatchOperationKind::Replace => {
                if value.is_none() {
                    return Err(A2uiPatchError::MissingOperationField { index, field: "value" });
                }
            }
            PatchOperationKind::Remove => {
                if value.is_some() {
                    return Err(A2uiPatchError::Conflict {
                        index,
                        path,
                        reason: "remove operation must not include a value".to_owned(),
                    });
                }
            }
        }

        operations.push(PatchOperation { kind, path, value });
    }

    Ok(PatchDocument { version, operations })
}

pub fn build_replace_root_patch(state: &Value) -> PatchDocument {
    PatchDocument {
        version: A2UI_PATCH_PROTOCOL_VERSION,
        operations: vec![PatchOperation {
            kind: PatchOperationKind::Replace,
            path: String::new(),
            value: Some(state.clone()),
        }],
    }
}

pub fn patch_document_to_value(document: &PatchDocument) -> Value {
    let operations: Vec<Value> = document
        .operations
        .iter()
        .map(|operation| {
            let op_name = match operation.kind {
                PatchOperationKind::Add => "add",
                PatchOperationKind::Replace => "replace",
                PatchOperationKind::Remove => "remove",
            };
            let mut object = Map::new();
            object.insert("op".to_owned(), Value::String(op_name.to_owned()));
            object.insert("path".to_owned(), Value::String(operation.path.clone()));
            if let Some(value) = &operation.value {
                object.insert("value".to_owned(), value.clone());
            }
            Value::Object(object)
        })
        .collect();

    let mut patch = Map::new();
    patch.insert("v".to_owned(), Value::from(document.version));
    patch.insert("ops".to_owned(), Value::Array(operations));
    Value::Object(patch)
}

pub fn patch_document_to_bytes(document: &PatchDocument) -> Result<Vec<u8>, A2uiPatchError> {
    Ok(serde_json::to_vec(&patch_document_to_value(document))?)
}

pub fn apply_patch_document(state: &Value, patch: &PatchDocument) -> Result<Value, A2uiPatchError> {
    if patch.version != A2UI_PATCH_PROTOCOL_VERSION {
        return Err(A2uiPatchError::UnsupportedVersion(patch.version));
    }
    if patch.operations.is_empty() {
        return Err(A2uiPatchError::EmptyOperations);
    }

    let mut current = state.clone();
    for (index, operation) in patch.operations.iter().enumerate() {
        apply_single_operation(&mut current, operation, index)?;
    }
    Ok(current)
}

fn apply_single_operation(
    current: &mut Value,
    operation: &PatchOperation,
    index: usize,
) -> Result<(), A2uiPatchError> {
    let tokens = parse_pointer_tokens(operation.path.as_str(), index)?;
    if tokens.is_empty() {
        return match operation.kind {
            PatchOperationKind::Add | PatchOperationKind::Replace => {
                let value = operation
                    .value
                    .clone()
                    .ok_or(A2uiPatchError::MissingOperationField { index, field: "value" })?;
                *current = value;
                Ok(())
            }
            PatchOperationKind::Remove => Err(A2uiPatchError::Conflict {
                index,
                path: operation.path.clone(),
                reason: "cannot remove the document root".to_owned(),
            }),
        };
    }

    let (parent_tokens, leaf_tokens) = tokens.split_at(tokens.len() - 1);
    let parent =
        resolve_pointer_parent_mut(current, parent_tokens, index, operation.path.as_str())?;
    let leaf = leaf_tokens[0].as_str();
    match parent {
        Value::Object(map) => apply_object_operation(map, leaf, operation, index),
        Value::Array(values) => apply_array_operation(values, leaf, operation, index),
        _ => Err(A2uiPatchError::Conflict {
            index,
            path: operation.path.clone(),
            reason: "target parent is neither object nor array".to_owned(),
        }),
    }
}

fn apply_object_operation(
    map: &mut Map<String, Value>,
    leaf: &str,
    operation: &PatchOperation,
    index: usize,
) -> Result<(), A2uiPatchError> {
    match operation.kind {
        PatchOperationKind::Add => {
            let value = operation
                .value
                .clone()
                .ok_or(A2uiPatchError::MissingOperationField { index, field: "value" })?;
            map.insert(leaf.to_owned(), value);
            Ok(())
        }
        PatchOperationKind::Replace => {
            let value = operation
                .value
                .clone()
                .ok_or(A2uiPatchError::MissingOperationField { index, field: "value" })?;
            if !map.contains_key(leaf) {
                return Err(A2uiPatchError::Conflict {
                    index,
                    path: operation.path.clone(),
                    reason: "replace target does not exist".to_owned(),
                });
            }
            map.insert(leaf.to_owned(), value);
            Ok(())
        }
        PatchOperationKind::Remove => {
            if map.remove(leaf).is_none() {
                return Err(A2uiPatchError::Conflict {
                    index,
                    path: operation.path.clone(),
                    reason: "remove target does not exist".to_owned(),
                });
            }
            Ok(())
        }
    }
}

fn apply_array_operation(
    values: &mut Vec<Value>,
    leaf: &str,
    operation: &PatchOperation,
    index: usize,
) -> Result<(), A2uiPatchError> {
    match operation.kind {
        PatchOperationKind::Add => {
            let insert_index =
                parse_array_index(leaf, values.len(), true, index, operation.path.as_str())?;
            let value = operation
                .value
                .clone()
                .ok_or(A2uiPatchError::MissingOperationField { index, field: "value" })?;
            if insert_index == values.len() {
                values.push(value);
            } else {
                values.insert(insert_index, value);
            }
            Ok(())
        }
        PatchOperationKind::Replace => {
            let replace_index =
                parse_array_index(leaf, values.len(), false, index, operation.path.as_str())?;
            let value = operation
                .value
                .clone()
                .ok_or(A2uiPatchError::MissingOperationField { index, field: "value" })?;
            values[replace_index] = value;
            Ok(())
        }
        PatchOperationKind::Remove => {
            let remove_index =
                parse_array_index(leaf, values.len(), false, index, operation.path.as_str())?;
            values.remove(remove_index);
            Ok(())
        }
    }
}

fn resolve_pointer_parent_mut<'a>(
    root: &'a mut Value,
    tokens: &[String],
    index: usize,
    path: &str,
) -> Result<&'a mut Value, A2uiPatchError> {
    let mut current = root;
    for token in tokens {
        current = match current {
            Value::Object(map) => map.get_mut(token).ok_or_else(|| A2uiPatchError::Conflict {
                index,
                path: path.to_owned(),
                reason: format!("object key '{token}' does not exist"),
            })?,
            Value::Array(values) => {
                let value_index = parse_array_index(token, values.len(), false, index, path)?;
                values.get_mut(value_index).ok_or_else(|| A2uiPatchError::Conflict {
                    index,
                    path: path.to_owned(),
                    reason: "array index is out of bounds".to_owned(),
                })?
            }
            _ => {
                return Err(A2uiPatchError::Conflict {
                    index,
                    path: path.to_owned(),
                    reason: "path traverses through a scalar value".to_owned(),
                })
            }
        };
    }
    Ok(current)
}

fn parse_array_index(
    token: &str,
    len: usize,
    allow_append: bool,
    index: usize,
    path: &str,
) -> Result<usize, A2uiPatchError> {
    if token == "-" {
        if allow_append {
            return Ok(len);
        }
        return Err(A2uiPatchError::Conflict {
            index,
            path: path.to_owned(),
            reason: "array '-' token is only valid for add operations".to_owned(),
        });
    }
    if token.is_empty() {
        return Err(A2uiPatchError::Conflict {
            index,
            path: path.to_owned(),
            reason: "array index token cannot be empty".to_owned(),
        });
    }
    if token.starts_with('+') || (token.starts_with('0') && token.len() > 1) {
        return Err(A2uiPatchError::Conflict {
            index,
            path: path.to_owned(),
            reason: "array index must be canonical base-10 integer".to_owned(),
        });
    }
    let parsed = token.parse::<usize>().map_err(|_| A2uiPatchError::Conflict {
        index,
        path: path.to_owned(),
        reason: "array index is not a valid usize".to_owned(),
    })?;
    if allow_append {
        if parsed > len {
            return Err(A2uiPatchError::Conflict {
                index,
                path: path.to_owned(),
                reason: format!("array index {parsed} exceeds length {len}"),
            });
        }
    } else if parsed >= len {
        return Err(A2uiPatchError::Conflict {
            index,
            path: path.to_owned(),
            reason: format!("array index {parsed} exceeds max index {}", len.saturating_sub(1)),
        });
    }
    Ok(parsed)
}

fn parse_pointer_tokens(path: &str, index: usize) -> Result<Vec<String>, A2uiPatchError> {
    validate_pointer(path, index)?;
    if path.is_empty() {
        return Ok(Vec::new());
    }
    path[1..].split('/').map(|raw| decode_pointer_segment(raw, index, path)).collect()
}

fn decode_pointer_segment(raw: &str, index: usize, path: &str) -> Result<String, A2uiPatchError> {
    let mut decoded = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(character) = chars.next() {
        if character != '~' {
            decoded.push(character);
            continue;
        }
        let escaped = chars.next().ok_or_else(|| A2uiPatchError::InvalidPointer {
            index,
            path: path.to_owned(),
            reason: "dangling '~' escape sequence".to_owned(),
        })?;
        match escaped {
            '0' => decoded.push('~'),
            '1' => decoded.push('/'),
            other => {
                return Err(A2uiPatchError::InvalidPointer {
                    index,
                    path: path.to_owned(),
                    reason: format!("unsupported escape '~{other}'"),
                })
            }
        }
    }
    Ok(decoded)
}

fn validate_pointer(path: &str, index: usize) -> Result<(), A2uiPatchError> {
    if path.is_empty() {
        return Ok(());
    }
    if path.starts_with('/') {
        return Ok(());
    }
    Err(A2uiPatchError::InvalidPointer {
        index,
        path: path.to_owned(),
        reason: "pointer must start with '/' or be empty for root".to_owned(),
    })
}

fn parse_operation_kind(
    operation_object: &Map<String, Value>,
    index: usize,
) -> Result<PatchOperationKind, A2uiPatchError> {
    let op_name = operation_object
        .get("op")
        .ok_or(A2uiPatchError::MissingOperationField { index, field: "op" })?
        .as_str()
        .ok_or(A2uiPatchError::InvalidOperationFieldType { index, field: "op" })?;

    match op_name {
        "add" => Ok(PatchOperationKind::Add),
        "replace" => Ok(PatchOperationKind::Replace),
        "remove" => Ok(PatchOperationKind::Remove),
        other => Err(A2uiPatchError::UnsupportedOperation { index, op: other.to_owned() }),
    }
}

fn validate_required_version(object: &Map<String, Value>) -> Result<(), A2uiValidationError> {
    let version = object
        .get("v")
        .ok_or(A2uiValidationError::MissingField("v"))?
        .as_u64()
        .ok_or(A2uiValidationError::InvalidType("v"))?;

    if version == 1 {
        Ok(())
    } else {
        Err(A2uiValidationError::UnsupportedVersion)
    }
}

fn validate_required_string(
    object: &Map<String, Value>,
    key: &'static str,
) -> Result<(), A2uiValidationError> {
    let value = object
        .get(key)
        .ok_or(A2uiValidationError::MissingField(key))?
        .as_str()
        .ok_or(A2uiValidationError::InvalidType(key))?;

    if value.is_empty() {
        return Err(A2uiValidationError::EmptyField(key));
    }
    Ok(())
}

fn validate_required_array(
    object: &Map<String, Value>,
    key: &'static str,
) -> Result<(), A2uiValidationError> {
    let array = object
        .get(key)
        .ok_or(A2uiValidationError::MissingField(key))?
        .as_array()
        .ok_or(A2uiValidationError::InvalidType(key))?;

    if array.is_empty() {
        return Err(A2uiValidationError::EmptyField(key));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        apply_patch_document, build_replace_root_patch, known_good_document,
        parse_and_validate_document, parse_patch_document, patch_document_to_value,
        validate_document, A2uiPatchError, A2uiValidationError, PatchOperation, PatchOperationKind,
    };

    #[test]
    fn known_good_fixture_passes_validation() {
        let valid_document = known_good_document();
        assert_eq!(validate_document(&valid_document), Ok(()));
    }

    #[test]
    fn missing_required_field_fails_validation() {
        let invalid_document = json!({
            "v": 1,
            "components": [{"id": "card1"}]
        });

        let result = validate_document(&invalid_document);
        assert_eq!(result, Err(A2uiValidationError::MissingField("surface")));
    }

    #[test]
    fn parse_and_validate_rejects_invalid_json() {
        let result = parse_and_validate_document(br#"{"v":1,"surface":"main","components":["#);
        assert_eq!(result, Err(A2uiValidationError::InvalidJson));
    }

    #[test]
    fn parse_and_validate_accepts_known_good_fixture() {
        let known_good = known_good_document().to_string();
        let result = parse_and_validate_document(known_good.as_bytes());
        assert!(result.is_ok());
    }

    #[test]
    fn empty_required_string_returns_empty_field_error() {
        let invalid_document = json!({
            "v": 1,
            "surface": "",
            "components": [{"id": "card1"}]
        });

        let result = validate_document(&invalid_document);
        assert_eq!(result, Err(A2uiValidationError::EmptyField("surface")));
    }

    #[test]
    fn empty_required_array_returns_empty_field_error() {
        let invalid_document = json!({
            "v": 1,
            "surface": "main",
            "components": []
        });

        let result = validate_document(&invalid_document);
        assert_eq!(result, Err(A2uiValidationError::EmptyField("components")));
    }

    #[test]
    fn patch_protocol_parses_and_applies_deterministically() {
        let state = json!({
            "items": [
                {"id": "one"},
                {"id": "two"}
            ],
            "meta": {"status": "draft"}
        });
        let patch = parse_patch_document(
            br#"{"v":1,"ops":[
                {"op":"add","path":"/items/1/name","value":"middle"},
                {"op":"replace","path":"/meta/status","value":"published"},
                {"op":"remove","path":"/items/0/id"}
            ]}"#,
        )
        .expect("patch should parse");

        let once = apply_patch_document(&state, &patch).expect("patch should apply once");
        let twice = apply_patch_document(&state, &patch).expect("patch should apply twice");
        assert_eq!(once, twice, "same input + same patch must be deterministic");
        assert_eq!(
            once,
            json!({
                "items": [
                    {},
                    {"id":"two","name":"middle"}
                ],
                "meta": {"status": "published"}
            })
        );
    }

    #[test]
    fn patch_conflict_returns_precise_error() {
        let state = json!({"items":[{"id":"one"}]});
        let patch = parse_patch_document(
            br#"{"v":1,"ops":[{"op":"replace","path":"/items/99/id","value":"x"}]}"#,
        )
        .expect("patch should parse");
        let error = apply_patch_document(&state, &patch).expect_err("conflict must fail");
        assert!(matches!(error, A2uiPatchError::Conflict { index: 0, .. }));
    }

    #[test]
    fn patch_rejects_invalid_pointer_encoding() {
        let error = parse_patch_document(
            br#"{"v":1,"ops":[{"op":"add","path":"/items/~2/bad","value":1}]}"#,
        )
        .expect_err("invalid pointer escape should fail");
        assert!(matches!(error, A2uiPatchError::InvalidPointer { index: 0, .. }));
    }

    #[test]
    fn build_replace_root_patch_replaces_entire_document() {
        let source = json!({"from":"source"});
        let patch = build_replace_root_patch(&source);
        let patched =
            apply_patch_document(&json!({"stale":true}), &patch).expect("root replace should work");
        assert_eq!(patched, source);

        let serialized = patch_document_to_value(&patch);
        assert_eq!(
            serialized,
            json!({
                "v": 1,
                "ops": [
                    {"op":"replace","path":"","value":{"from":"source"}}
                ]
            })
        );
    }

    #[test]
    fn patch_remove_root_is_rejected() {
        let patch = super::PatchDocument {
            version: 1,
            operations: vec![PatchOperation {
                kind: PatchOperationKind::Remove,
                path: String::new(),
                value: None,
            }],
        };
        let error =
            apply_patch_document(&json!({"value":1}), &patch).expect_err("remove root must fail");
        assert!(matches!(error, A2uiPatchError::Conflict { index: 0, .. }));
    }
}
