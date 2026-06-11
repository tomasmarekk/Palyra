//! Bounded JSON shrinking for prompt-context compression.
//!
//! [`shrink_json_value`] recursively clamps string length, array and object
//! fanout, and nesting depth so arbitrarily large tool or journal payloads
//! cannot blow up a provider prompt. Every cut is marked in-band (an `...`
//! suffix on strings, `_palyra_truncated` sentinel entries on collections)
//! so downstream consumers can tell a shrunk value from a complete one.
//! Consumed by `application::context_engine` when assembling provider input.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

const DEFAULT_MAX_STRING_CHARS: usize = 240;
const DEFAULT_MAX_ARRAY_ITEMS: usize = 8;
const DEFAULT_MAX_OBJECT_FIELDS: usize = 16;
const DEFAULT_MAX_DEPTH: usize = 8;

/// Per-dimension limits applied by [`shrink_json_value`].
///
/// All limits are inclusive caps on what is kept; anything beyond a cap is
/// replaced by a truncation marker rather than dropped silently.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct JsonShrinkConfig {
    pub(crate) max_string_chars: usize,
    pub(crate) max_array_items: usize,
    pub(crate) max_object_fields: usize,
    pub(crate) max_depth: usize,
}

impl Default for JsonShrinkConfig {
    fn default() -> Self {
        Self {
            max_string_chars: DEFAULT_MAX_STRING_CHARS,
            max_array_items: DEFAULT_MAX_ARRAY_ITEMS,
            max_object_fields: DEFAULT_MAX_OBJECT_FIELDS,
            max_depth: DEFAULT_MAX_DEPTH,
        }
    }
}

/// Result of shrinking: the bounded value plus truncation diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct JsonShrinkOutcome {
    pub(crate) value: Value,
    /// True when any string, collection, or subtree was cut by a limit.
    pub(crate) truncated: bool,
    pub(crate) original_bytes: usize,
    pub(crate) output_bytes: usize,
}

/// Shrinks `value` to fit `config`, marking every truncation in-band.
///
/// The returned value is always valid JSON; strings are cut on `char`
/// boundaries so multi-byte sequences are never split. The byte counts are
/// serialized sizes before and after shrinking.
pub(crate) fn shrink_json_value(value: &Value, config: JsonShrinkConfig) -> JsonShrinkOutcome {
    // Byte sizes are diagnostics only, so a (practically unreachable)
    // serialization failure degrades to 0 instead of failing the shrink.
    let original_bytes = serde_json::to_vec(value).map(|bytes| bytes.len()).unwrap_or_default();
    let mut truncated = false;
    let value = shrink_value(value, config, 0, &mut truncated);
    let output_bytes = serde_json::to_vec(&value).map(|bytes| bytes.len()).unwrap_or_default();
    JsonShrinkOutcome { value, truncated, original_bytes, output_bytes }
}

fn shrink_value(
    value: &Value,
    config: JsonShrinkConfig,
    depth: usize,
    truncated: &mut bool,
) -> Value {
    if depth >= config.max_depth {
        *truncated = true;
        return Value::String("<truncated:depth>".to_owned());
    }

    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => value.clone(),
        Value::String(raw) => shrink_string(raw, config.max_string_chars, truncated),
        Value::Array(items) => shrink_array(items, config, depth, truncated),
        Value::Object(fields) => shrink_object(fields, config, depth, truncated),
    }
}

fn shrink_string(raw: &str, max_chars: usize, truncated: &mut bool) -> Value {
    if raw.chars().count() <= max_chars {
        return Value::String(raw.to_owned());
    }
    *truncated = true;
    // Cut by chars, not bytes: byte slicing could split a multi-byte
    // sequence and produce invalid UTF-8 / unparseable JSON.
    let mut shortened = raw.chars().take(max_chars).collect::<String>();
    shortened.push_str("...");
    Value::String(shortened)
}

fn shrink_array(
    items: &[Value],
    config: JsonShrinkConfig,
    depth: usize,
    truncated: &mut bool,
) -> Value {
    let mut output = items
        .iter()
        .take(config.max_array_items)
        .map(|item| shrink_value(item, config, depth + 1, truncated))
        .collect::<Vec<_>>();
    if items.len() > config.max_array_items {
        *truncated = true;
        let omitted = items.len() - config.max_array_items;
        let mut marker = Map::new();
        marker.insert("_palyra_truncated".to_owned(), Value::Bool(true));
        marker.insert("omitted_items".to_owned(), Value::Number(omitted.into()));
        output.push(Value::Object(marker));
    }
    Value::Array(output)
}

fn shrink_object(
    fields: &Map<String, Value>,
    config: JsonShrinkConfig,
    depth: usize,
    truncated: &mut bool,
) -> Value {
    let mut output = Map::new();
    for (key, value) in fields.iter().take(config.max_object_fields) {
        output.insert(key.clone(), shrink_value(value, config, depth + 1, truncated));
    }
    if fields.len() > config.max_object_fields {
        *truncated = true;
        output.insert("_palyra_truncated".to_owned(), Value::Bool(true));
        output.insert(
            "omitted_fields".to_owned(),
            Value::Number((fields.len() - config.max_object_fields).into()),
        );
    }
    Value::Object(output)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{shrink_json_value, JsonShrinkConfig};

    #[test]
    fn shrinker_preserves_valid_json_for_nested_values() {
        let value = json!({
            "outer": {
                "items": [
                    {"name": "alpha", "body": "\u{1F642}".repeat(20)},
                    {"name": "beta", "body": "b".repeat(20)},
                    {"name": "gamma", "body": "c".repeat(20)}
                ]
            }
        });
        let outcome = shrink_json_value(
            &value,
            JsonShrinkConfig {
                max_string_chars: 8,
                max_array_items: 2,
                max_object_fields: 8,
                max_depth: 8,
            },
        );

        assert!(outcome.truncated);
        let serialized =
            serde_json::to_string(&outcome.value).expect("shrunk value should serialize");
        serde_json::from_str::<serde_json::Value>(serialized.as_str())
            .expect("shrunk JSON must remain parseable");
        assert!(
            serialized.contains(format!("{}...", "\u{1F642}".repeat(8)).as_str()),
            "Unicode strings should be shortened by char boundary"
        );
        assert!(serialized.contains("_palyra_truncated"));
    }

    #[test]
    fn shrinker_never_slices_binary_like_string_inside_escape_sequence() {
        let value = json!({"blob": "\\u0000\\u0001\\u0002".repeat(32)});
        let outcome = shrink_json_value(
            &value,
            JsonShrinkConfig {
                max_string_chars: 7,
                max_array_items: 4,
                max_object_fields: 4,
                max_depth: 4,
            },
        );

        assert!(outcome.truncated);
        serde_json::to_vec(&outcome.value).expect("shrunk binary-like JSON should serialize");
    }
}
