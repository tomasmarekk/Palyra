//! Bounded, redacted JSON projections for model-visible session operations.
//! This layer keeps lineage and command records useful without exposing raw
//! transcript payloads or unrelated session identities.

use std::collections::BTreeMap;

use palyra_common::runtime_contracts::{SessionBudgetV2, SessionProgressV2, SessionSummaryV2};
use serde_json::{json, Value};

use super::{safe_text, truncate_text};
use crate::journal::{
    OrchestratorBackgroundTaskRecord, OrchestratorRunStatusSnapshot, OrchestratorSessionRecord,
    SessionModelCommandRecord,
};

pub(super) fn related_session_map(
    root_session_id: &str,
    sessions: Vec<OrchestratorSessionRecord>,
) -> BTreeMap<String, OrchestratorSessionRecord> {
    let by_id = sessions
        .into_iter()
        .map(|session| (session.session_id.clone(), session))
        .collect::<BTreeMap<_, _>>();
    by_id
        .iter()
        .filter(|(session_id, _)| {
            session_id.as_str() == root_session_id
                || is_descendant_session(root_session_id, session_id.as_str(), &by_id)
        })
        .map(|(session_id, session)| (session_id.clone(), session.clone()))
        .collect()
}

fn is_descendant_session(
    root_session_id: &str,
    candidate_session_id: &str,
    sessions: &BTreeMap<String, OrchestratorSessionRecord>,
) -> bool {
    let mut cursor =
        sessions.get(candidate_session_id).and_then(|session| session.parent_session_id.as_deref());
    for _ in 0..32 {
        let Some(parent_id) = cursor else {
            return false;
        };
        if parent_id == root_session_id {
            return true;
        }
        cursor = sessions.get(parent_id).and_then(|session| session.parent_session_id.as_deref());
    }
    false
}

pub(super) fn session_summary_json(
    root_session_id: &str,
    session: &OrchestratorSessionRecord,
    run: Option<&OrchestratorRunStatusSnapshot>,
    task: Option<&OrchestratorBackgroundTaskRecord>,
    generation: Option<u64>,
) -> Value {
    let relation = if session.session_id == root_session_id {
        "self"
    } else if session.parent_session_id.as_deref() == Some(root_session_id) {
        "child"
    } else {
        "descendant"
    };
    serde_json::to_value(SessionSummaryV2 {
        schema_version: 2,
        session_id: session.session_id.clone(),
        relation: relation.to_owned(),
        parent_session_id: session.parent_session_id.clone(),
        origin_run_id: session.branch_origin_run_id.clone(),
        state: run.map_or_else(
            || session.last_run_state.clone().unwrap_or_else(|| "idle".to_owned()),
            |run| run.state.clone(),
        ),
        generation,
        budget: task.map(|task| SessionBudgetV2 {
            tokens: task.budget_tokens,
            attempts: task.attempt_count,
            max_attempts: task.max_attempts,
        }),
        last_progress: task.map(|task| SessionProgressV2 {
            task_id: task.task_id.clone(),
            state: task.state.clone(),
            revision: task.revision,
            updated_at_unix_ms: task.updated_at_unix_ms,
        }),
        ownership_token: task.map(|task| task.task_id.clone()),
        title: safe_text(session.title.as_str()),
        preview: session.preview.as_deref().map(safe_text),
        last_run_id: session.last_run_id.clone(),
        updated_at_unix_ms: session.updated_at_unix_ms,
    })
    .unwrap_or_else(|_| json!({"schema_version": 2, "session_id": session.session_id}))
}

pub(super) fn run_status_json(run: &OrchestratorRunStatusSnapshot) -> Value {
    json!({
        "run_id": run.run_id,
        "session_id": run.session_id,
        "state": run.state,
        "cancel_requested": run.cancel_requested,
        "usage": {
            "prompt_tokens": run.prompt_tokens,
            "completion_tokens": run.completion_tokens,
            "total_tokens": run.total_tokens,
        },
        "updated_at_unix_ms": run.updated_at_unix_ms,
        "completed_at_unix_ms": run.completed_at_unix_ms,
        "parent_run_id": run.parent_run_id,
        "tape_events": run.tape_events,
    })
}

pub(super) fn command_outcome_json(
    command: &SessionModelCommandRecord,
    generation: Option<u64>,
    superseded_command_id: Option<String>,
) -> Value {
    json!({
        "command_id": command.command_id,
        "request_id": command.request_key,
        "operation": command.command_kind,
        "outcome": command.state,
        "reason_code": command.reason_code,
        "target_session_id": command.target_session_id,
        "target_run_id": command.target_run_id,
        "target_generation": generation,
        "queued_input_id": command.queued_input_id,
        "superseded_command_id": superseded_command_id,
    })
}

pub(super) fn redact_payload_json(payload_json: &str) -> Value {
    let mut value = serde_json::from_str::<Value>(payload_json)
        .unwrap_or_else(|_| Value::String(payload_json.to_owned()));
    redact_value(&mut value, None);
    value
}

fn redact_value(value: &mut Value, key: Option<&str>) {
    if key.is_some_and(sensitive_key) {
        *value = Value::String("[REDACTED]".to_owned());
        return;
    }
    match value {
        Value::Object(object) => {
            for (key, value) in object.iter_mut() {
                redact_value(value, Some(key.as_str()));
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_value(value, None);
            }
        }
        Value::String(text) => {
            *text = truncate_text(safe_text(text.as_str()), 2_048);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn sensitive_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    [
        "authorization",
        "cookie",
        "credential",
        "password",
        "secret",
        "token",
        "api_key",
        "apikey",
        "private_key",
    ]
    .iter()
    .any(|candidate| normalized.contains(candidate))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(session_id: &str, parent_session_id: Option<&str>) -> OrchestratorSessionRecord {
        OrchestratorSessionRecord {
            session_id: session_id.to_owned(),
            session_key: session_id.to_owned(),
            session_label: None,
            principal: "principal".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("console".to_owned()),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            last_run_id: None,
            archived_at_unix_ms: None,
            auto_title: None,
            auto_title_source: None,
            auto_title_generator_version: None,
            auto_title_updated_at_unix_ms: None,
            title_generation_state: "idle".to_owned(),
            manual_title_locked: false,
            manual_title_updated_at_unix_ms: None,
            model_profile_override: None,
            thinking_override: None,
            trace_override: None,
            verbose_override: None,
            title: session_id.to_owned(),
            title_source: "fallback".to_owned(),
            title_generator_version: None,
            preview: None,
            last_intent: None,
            last_summary: None,
            match_snippet: None,
            branch_state: "active".to_owned(),
            parent_session_id: parent_session_id.map(ToOwned::to_owned),
            branch_origin_run_id: None,
            last_run_state: None,
        }
    }

    #[test]
    fn related_session_map_excludes_unrelated_siblings() {
        let related = related_session_map(
            "parent",
            vec![
                session("parent", None),
                session("child", Some("parent")),
                session("grandchild", Some("child")),
                session("unrelated", None),
            ],
        );

        assert!(related.contains_key("parent"));
        assert!(related.contains_key("child"));
        assert!(related.contains_key("grandchild"));
        assert!(!related.contains_key("unrelated"));
    }

    #[test]
    fn history_projection_redacts_nested_secret_fields() {
        let value = redact_payload_json(
            r#"{"message":"safe","nested":{"authorization":"Bearer secret","api_key":"value"}}"#,
        );

        assert_eq!(value["message"], "safe");
        assert_eq!(value["nested"]["authorization"], "[REDACTED]");
        assert_eq!(value["nested"]["api_key"], "[REDACTED]");
        assert!(!value.to_string().contains("Bearer secret"));
    }
}
