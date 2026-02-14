use serde_json::{Map, Value};
use thiserror::Error;

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
    #[error("unsupported A2UI version")]
    UnsupportedVersion,
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
        return Err(A2uiValidationError::InvalidType(key));
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
        return Err(A2uiValidationError::InvalidType(key));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        known_good_document, parse_and_validate_document, validate_document, A2uiValidationError,
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
}
