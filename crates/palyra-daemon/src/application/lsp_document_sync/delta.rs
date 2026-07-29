use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use super::contracts::{
    DiagnosticRangeV2, DiagnosticSeverityV2, NormalizedDiagnosticV2, UnchangedDiagnosticV2,
};

const MAX_CODE_CHARS: usize = 128;
const MAX_SOURCE_CHARS: usize = 128;
const MAX_MESSAGE_CHARS: usize = 2_048;

#[derive(Debug, Clone, Serialize)]
pub(super) struct ClassifiedDiagnostics {
    pub introduced: Vec<NormalizedDiagnosticV2>,
    pub resolved: Vec<NormalizedDiagnosticV2>,
    pub unchanged: Vec<UnchangedDiagnosticV2>,
}

pub(super) fn normalize_diagnostics(
    relative_path: &str,
    raw: &[Value],
) -> Vec<NormalizedDiagnosticV2> {
    raw.iter().map(|diagnostic| normalize_diagnostic(relative_path, diagnostic)).collect()
}

pub(super) fn classify_diagnostics(
    before: &[NormalizedDiagnosticV2],
    after: &[NormalizedDiagnosticV2],
) -> ClassifiedDiagnostics {
    let mut before_by_identity = group_by_identity(before);
    let mut after_by_identity = group_by_identity(after);
    let mut identities =
        before_by_identity.keys().chain(after_by_identity.keys()).cloned().collect::<Vec<_>>();
    identities.sort();
    identities.dedup();

    let mut introduced = Vec::new();
    let mut resolved = Vec::new();
    let mut unchanged = Vec::new();
    for identity in identities {
        let mut baseline = before_by_identity.remove(identity.as_str()).unwrap_or_default();
        let mut current = after_by_identity.remove(identity.as_str()).unwrap_or_default();
        baseline.sort_by_key(position_key);
        current.sort_by_key(position_key);
        while !baseline.is_empty() && !current.is_empty() {
            let (before_index, after_index) = nearest_pair(baseline.as_slice(), current.as_slice());
            let before_item = baseline.remove(before_index);
            let after_item = current.remove(after_index);
            unchanged.push(UnchangedDiagnosticV2 {
                line_shift: i64::from(after_item.range.start_line)
                    - i64::from(before_item.range.start_line),
                character_shift: if after_item.range.start_line == before_item.range.start_line {
                    i64::from(after_item.range.start_character)
                        - i64::from(before_item.range.start_character)
                } else {
                    0
                },
                before: before_item,
                after: after_item,
            });
        }
        resolved.extend(baseline);
        introduced.extend(current);
    }
    introduced.sort_by(compare_diagnostics);
    resolved.sort_by(compare_diagnostics);
    unchanged.sort_by(|left, right| compare_diagnostics(&left.after, &right.after));
    ClassifiedDiagnostics { introduced, resolved, unchanged }
}

fn normalize_diagnostic(relative_path: &str, raw: &Value) -> NormalizedDiagnosticV2 {
    let range = DiagnosticRangeV2 {
        start_line: value_u32(raw.pointer("/range/start/line")),
        start_character: value_u32(raw.pointer("/range/start/character")),
        end_line: value_u32(raw.pointer("/range/end/line")),
        end_character: value_u32(raw.pointer("/range/end/character")),
    };
    let severity = match raw.get("severity").and_then(Value::as_u64) {
        Some(1) => DiagnosticSeverityV2::Error,
        Some(2) => DiagnosticSeverityV2::Warning,
        Some(4) => DiagnosticSeverityV2::Hint,
        _ => DiagnosticSeverityV2::Information,
    };
    let raw_code = raw.get("code").and_then(value_to_string);
    let raw_source = raw.get("source").and_then(Value::as_str).map(str::to_owned);
    let raw_message = raw.get("message").and_then(Value::as_str).unwrap_or("LSP diagnostic");
    let identity_sha256 = diagnostic_identity(
        relative_path,
        severity,
        raw_code.as_deref(),
        raw_source.as_deref(),
        raw_message,
    );
    let (code, code_truncated) = truncate_optional(raw_code, MAX_CODE_CHARS);
    let (source, source_truncated) = truncate_optional(raw_source, MAX_SOURCE_CHARS);
    let (message, message_truncated) = truncate_text(raw_message, MAX_MESSAGE_CHARS);
    NormalizedDiagnosticV2 {
        relative_path: relative_path.to_owned(),
        identity_sha256,
        range,
        severity,
        code,
        source,
        message,
        text_truncated: code_truncated || source_truncated || message_truncated,
    }
}

fn diagnostic_identity(
    relative_path: &str,
    severity: DiagnosticSeverityV2,
    code: Option<&str>,
    source: Option<&str>,
    message: &str,
) -> String {
    let mut hasher = Sha256::new();
    hash_part(&mut hasher, relative_path);
    hash_part(&mut hasher, severity_label(severity));
    hash_part(&mut hasher, code.unwrap_or_default());
    hash_part(&mut hasher, source.unwrap_or_default());
    hash_part(&mut hasher, message);
    hex::encode(hasher.finalize())
}

fn severity_label(severity: DiagnosticSeverityV2) -> &'static str {
    match severity {
        DiagnosticSeverityV2::Error => "error",
        DiagnosticSeverityV2::Warning => "warning",
        DiagnosticSeverityV2::Information => "information",
        DiagnosticSeverityV2::Hint => "hint",
    }
}

fn hash_part(hasher: &mut Sha256, value: &str) {
    hasher.update(value.len().to_le_bytes());
    hasher.update(value.as_bytes());
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_u32(value: Option<&Value>) -> u32 {
    value.and_then(Value::as_u64).and_then(|value| u32::try_from(value).ok()).unwrap_or_default()
}

fn truncate_optional(value: Option<String>, max_chars: usize) -> (Option<String>, bool) {
    match value {
        Some(value) => {
            let (value, truncated) = truncate_text(value.as_str(), max_chars);
            (Some(value), truncated)
        }
        None => (None, false),
    }
}

fn truncate_text(value: &str, max_chars: usize) -> (String, bool) {
    let mut chars = value.chars();
    let bounded = chars.by_ref().take(max_chars).collect::<String>();
    let truncated = chars.next().is_some();
    (bounded, truncated)
}

fn group_by_identity(
    diagnostics: &[NormalizedDiagnosticV2],
) -> BTreeMap<String, Vec<NormalizedDiagnosticV2>> {
    let mut grouped = BTreeMap::<String, Vec<NormalizedDiagnosticV2>>::new();
    for diagnostic in diagnostics {
        grouped.entry(diagnostic.identity_sha256.clone()).or_default().push(diagnostic.clone());
    }
    grouped
}

fn nearest_pair(
    before: &[NormalizedDiagnosticV2],
    after: &[NormalizedDiagnosticV2],
) -> (usize, usize) {
    let mut selected = (0, 0);
    let mut selected_distance = u64::MAX;
    for (before_index, baseline) in before.iter().enumerate() {
        for (after_index, current) in after.iter().enumerate() {
            let distance = position_distance(baseline.range, current.range);
            if distance < selected_distance {
                selected = (before_index, after_index);
                selected_distance = distance;
            }
        }
    }
    selected
}

fn position_distance(before: DiagnosticRangeV2, after: DiagnosticRangeV2) -> u64 {
    u64::from(before.start_line.abs_diff(after.start_line))
        .saturating_mul(1_000_000)
        .saturating_add(u64::from(before.start_character.abs_diff(after.start_character)))
}

fn position_key(diagnostic: &NormalizedDiagnosticV2) -> (u32, u32, u32, u32) {
    (
        diagnostic.range.start_line,
        diagnostic.range.start_character,
        diagnostic.range.end_line,
        diagnostic.range.end_character,
    )
}

fn compare_diagnostics(
    left: &NormalizedDiagnosticV2,
    right: &NormalizedDiagnosticV2,
) -> std::cmp::Ordering {
    left.relative_path
        .cmp(&right.relative_path)
        .then(left.range.start_line.cmp(&right.range.start_line))
        .then(left.range.start_character.cmp(&right.range.start_character))
        .then(left.identity_sha256.cmp(&right.identity_sha256))
}

pub(super) fn cap_classification(
    mut classified: ClassifiedDiagnostics,
    max_visible_items: usize,
) -> (ClassifiedDiagnostics, bool) {
    let total = classified
        .introduced
        .len()
        .saturating_add(classified.resolved.len())
        .saturating_add(classified.unchanged.len());
    let mut remaining = max_visible_items;
    classified.introduced.truncate(remaining);
    remaining = remaining.saturating_sub(classified.introduced.len());
    classified.resolved.truncate(remaining);
    remaining = remaining.saturating_sub(classified.resolved.len());
    classified.unchanged.truncate(remaining);
    (classified, total > max_visible_items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn line_insert_pairs_the_same_diagnostic_identity() {
        let before = normalize_diagnostics(
            "src/lib.rs",
            &[diagnostic(2, "same"), diagnostic(10, "removed")],
        );
        let after =
            normalize_diagnostics("src/lib.rs", &[diagnostic(5, "same"), diagnostic(7, "new")]);
        let delta = classify_diagnostics(before.as_slice(), after.as_slice());
        assert_eq!(delta.unchanged.len(), 1);
        assert_eq!(delta.unchanged[0].line_shift, 3);
        assert_eq!(delta.introduced.len(), 1);
        assert_eq!(delta.resolved.len(), 1);
    }

    #[test]
    fn duplicate_diagnostics_pair_by_nearest_range() {
        let before =
            normalize_diagnostics("src/lib.rs", &[diagnostic(1, "same"), diagnostic(20, "same")]);
        let after =
            normalize_diagnostics("src/lib.rs", &[diagnostic(2, "same"), diagnostic(25, "same")]);
        let delta = classify_diagnostics(before.as_slice(), after.as_slice());
        assert_eq!(delta.unchanged.len(), 2);
        assert_eq!(delta.unchanged[0].line_shift, 1);
        assert_eq!(delta.unchanged[1].line_shift, 5);
    }

    #[test]
    fn visible_cap_prioritizes_introduced_then_resolved() {
        let classified = ClassifiedDiagnostics {
            introduced: normalize_diagnostics(
                "a.rs",
                &[diagnostic(1, "one"), diagnostic(2, "two")],
            ),
            resolved: normalize_diagnostics("a.rs", &[diagnostic(3, "three")]),
            unchanged: Vec::new(),
        };
        let (bounded, truncated) = cap_classification(classified, 2);
        assert!(truncated);
        assert_eq!(bounded.introduced.len(), 2);
        assert!(bounded.resolved.is_empty());
    }

    fn diagnostic(line: u64, message: &str) -> Value {
        json!({
            "range": {
                "start": {"line": line, "character": 0},
                "end": {"line": line, "character": 1}
            },
            "severity": 1,
            "code": "fixture",
            "source": "fixture",
            "message": message
        })
    }
}
