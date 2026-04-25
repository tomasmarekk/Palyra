use super::*;
use palyra_common::runtime_contracts::{AcpCapability, AcpScope, AcpTransportKind};
use serde_json::json;

fn context() -> AcpClientContext {
    AcpClientContext {
        protocol_version: 1,
        client_id: "zed-extension".to_owned(),
        transport: AcpTransportKind::Stdio,
        owner_principal: "operator".to_owned(),
        device_id: "desktop".to_owned(),
        channel: None,
        scopes: vec![AcpScope::SessionsRead, AcpScope::SessionsWrite],
        capabilities: vec![AcpCapability::SessionLoad, AcpCapability::PendingPrompts],
    }
}

#[test]
fn session_binding_survives_restart_and_marks_permissions_stale() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let runtime = AcpRuntime::open(tempdir.path().join("acp")).expect("runtime should open");
    let binding = runtime
        .upsert_session_binding(AcpSessionBindingUpsert {
            context: context(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_key: "repo:C:/work/palyra".to_owned(),
            session_label: Some("Palyra".to_owned()),
            mode: AcpSessionMode::Normal,
            config: json!({ "mode": "normal" }),
            cursor: AcpCursor { sequence: 4 },
        })
        .expect("binding should persist");
    assert!(!binding.stale_permissions);

    let reopened = AcpRuntime::open(tempdir.path().join("acp")).expect("runtime should reopen");
    let binding = reopened
        .session_binding_for_acp("zed-extension", "acp-session-a")
        .expect("binding should load");
    assert!(binding.stale_permissions);
    assert_eq!(binding.cursor.sequence, 4);
}

#[test]
fn reconnect_returns_pending_prompt_within_grace_window() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let runtime = AcpRuntime::open(tempdir.path().join("acp")).expect("runtime should open");
    runtime
        .upsert_session_binding(AcpSessionBindingUpsert {
            context: context(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_key: "repo:C:/work/palyra".to_owned(),
            session_label: None,
            mode: AcpSessionMode::Normal,
            config: json!({}),
            cursor: AcpCursor { sequence: 7 },
        })
        .expect("binding should persist");
    runtime
        .remember_pending_prompt(AcpPendingPromptUpsert {
            prompt_id: "prompt-a".to_owned(),
            acp_client_id: "zed-extension".to_owned(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            approval_id: None,
            run_id: None,
            prompt_kind: "permission".to_owned(),
            redacted_summary: "Allow tool execution?".to_owned(),
            ttl_ms: 60_000,
        })
        .expect("pending prompt should persist");

    let outcome = runtime
        .reconnect(&context(), "acp-session-a", AcpCursor { sequence: 8 })
        .expect("reconnect should succeed");
    assert_eq!(outcome.binding.cursor.sequence, 8);
    assert_eq!(outcome.pending_prompts.len(), 1);
    assert!(outcome.expired_prompt_ids.is_empty());
}

#[test]
fn config_rejects_secret_bearing_keys() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let runtime = AcpRuntime::open(tempdir.path().join("acp")).expect("runtime should open");
    let error = runtime
        .upsert_session_binding(AcpSessionBindingUpsert {
            context: context(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_key: "repo:C:/work/palyra".to_owned(),
            session_label: None,
            mode: AcpSessionMode::Normal,
            config: json!({ "api_key": "not allowed" }),
            cursor: AcpCursor::default(),
        })
        .expect_err("secret-bearing config should be rejected");
    assert_eq!(error.stable_code(), "acp/invalid_field");
}

#[test]
fn conversation_binding_repair_detaches_duplicate_external_binding() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let runtime = AcpRuntime::open(tempdir.path().join("acp")).expect("runtime should open");
    let first = runtime
        .upsert_conversation_binding(ConversationBindingUpsert {
            connector_kind: "acp".to_owned(),
            external_identity: "user-a".to_owned(),
            external_conversation_id: "thread-1".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            owner_principal: "operator".to_owned(),
            device_id: "desktop".to_owned(),
            channel: None,
            scopes: vec!["sessions:read".to_owned()],
            sensitivity: ConversationBindingSensitivity::Internal,
            delivery_cursor: AcpCursor { sequence: 2 },
            last_event_id: Some("event-1".to_owned()),
        })
        .expect("first binding should persist");
    let second = runtime
        .upsert_conversation_binding(ConversationBindingUpsert {
            connector_kind: "acp".to_owned(),
            external_identity: "user-a".to_owned(),
            external_conversation_id: "thread-1".to_owned(),
            palyra_session_id: "01BX5ZZKBKACTAV9WEVGEMMVRZ".to_owned(),
            owner_principal: "operator".to_owned(),
            device_id: "desktop".to_owned(),
            channel: None,
            scopes: vec!["sessions:read".to_owned()],
            sensitivity: ConversationBindingSensitivity::Internal,
            delivery_cursor: AcpCursor { sequence: 3 },
            last_event_id: Some("event-2".to_owned()),
        })
        .expect("second binding should persist");
    assert_ne!(first.binding_id, second.binding_id);

    let plan = runtime.plan_conversation_binding_repair().expect("plan should build");
    assert_eq!(plan.actions.len(), 1);
    let applied = runtime.apply_conversation_binding_repair().expect("repair should apply");
    assert_eq!(applied.actions.len(), 1);
    let bindings = runtime
        .list_conversation_bindings(ConversationBindingFilter {
            include_detached: true,
            ..ConversationBindingFilter::default()
        })
        .expect("bindings should list");
    assert!(bindings
        .iter()
        .any(|entry| entry.conflict_state == ConversationBindingConflictState::Detached));
}

#[test]
fn translator_rejects_unknown_event_types_as_compatibility_errors() {
    assert_eq!(translate_palyra_event_type("model_token").unwrap(), "message.delta");
    let error =
        translate_palyra_event_type("provider.raw.unknown").expect_err("unknown event fails");
    assert_eq!(error.stable_code(), "acp/compatibility_error");
}
