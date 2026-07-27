use super::*;
use palyra_common::runtime_contracts::{
    AcpCapability, AcpEventLedgerKind, AcpEventLedgerRecord, AcpScope, AcpTransportKind,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Mutex};

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
fn runtime_rejects_state_root_with_parent_traversal_components() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let unsafe_root = tempdir.path().join("state").join("..").join("escape");

    let error = AcpRuntime::open(unsafe_root)
        .expect_err("runtime state root must reject parent traversal components");

    assert_eq!(error.stable_code(), "acp/invalid_field");
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
fn event_ledger_redacts_payload_and_keeps_only_hashable_replay_metadata() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let root = tempdir.path().join("acp");
    let runtime = AcpRuntime::open(root.clone()).expect("runtime should open");
    runtime
        .upsert_session_binding(AcpSessionBindingUpsert {
            context: context(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_key: "repo:C:/work/palyra".to_owned(),
            session_label: None,
            mode: AcpSessionMode::Normal,
            config: json!({}),
            cursor: AcpCursor::default(),
        })
        .expect("binding should persist");

    let record = runtime
        .record_event(AcpEventLedgerAppend {
            context: context(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            kind: AcpEventLedgerKind::ToolCallUpdate,
            run_id: Some("01BX5ZZKBKACTAV9WEVGEMMVRZ".to_owned()),
            approval_id: None,
            redacted_summary: "Tool call completed".to_owned(),
            redacted_payload: json!({
                "tool": "shell",
                "api_key": "secret-value",
                "nested": { "token": "raw-token", "safe": "visible" },
            }),
        })
        .expect("ledger event should persist");

    let expected_payload = serde_json::to_vec(&json!({
        "tool": "shell",
        "api_key": "[REDACTED]",
        "nested": { "token": "[REDACTED]", "safe": "visible" },
    }))
    .expect("expected payload should serialize");
    assert_eq!(record.kind, AcpEventLedgerKind::ToolCallUpdate);
    assert_eq!(record.payload_sha256, crate::sha256_hex(expected_payload.as_slice()));
    let persisted =
        std::fs::read_to_string(root.join(ACP_BINDINGS_INDEX_FILE_NAME)).expect("state exists");
    assert!(!persisted.contains("secret-value"));
    assert!(!persisted.contains("raw-token"));
    assert!(persisted.contains("Tool call completed"));
}

#[test]
fn presentation_projection_builds_edit_proposal_and_rich_blocks() {
    let payload = json!({
        "approval_id": "approval-123",
        "edit_proposal": {
            "diff_summary": "Update workspace configuration",
            "risk_level": "High",
            "affected_files": ["crates/palyra-daemon/src/lib.rs"],
            "provenance_refs": ["workspace_patch:abc123"],
        },
        "rich_blocks": [
            {
                "kind": "file_uri",
                "uri": "file:///repo/crates/palyra-daemon/src/lib.rs",
                "title": "daemon lib",
                "preview": "module registration changed",
            },
            {
                "kind": "artifact_ref",
                "artifact_id": "artifact-1",
            },
        ],
        "tool_name": "palyra.fs.apply_patch",
    });

    let projection = build_acp_presentation_projection(AcpPresentationProjectionInput {
        event_kind: "approval.request",
        run_id: Some("run-1"),
        session_id: Some("session-1"),
        tape_segment: Some("42..44"),
        compaction_generation: Some(3),
        source_binding: Some("acpbind_123"),
        payload: &payload,
    });

    let proposal = projection.edit_proposal.as_ref().expect("edit proposal should be projected");
    assert_eq!(proposal.proposal_id, "approval-123");
    assert_eq!(proposal.risk_level, "high");
    assert_eq!(proposal.approval_actions, ["approve", "reject", "modify"]);
    assert_eq!(proposal.affected_files, ["crates/palyra-daemon/src/lib.rs"]);
    assert_eq!(projection.rich_blocks.len(), 2);
    assert!(projection
        .rich_blocks
        .iter()
        .any(|block| block.block_kind == AcpRichContentBlockKind::FileUri));
    assert!(projection.renderers.iter().any(|renderer| renderer.renderer == "workspace_patch"));
    assert_eq!(
        projection.metadata,
        AcpPresentationMetadata {
            run_id: Some("run-1".to_owned()),
            session_id: Some("session-1".to_owned()),
            tape_segment: Some("42..44".to_owned()),
            compaction_generation: Some(3),
            source_binding: Some("acpbind_123".to_owned()),
        }
    );
}

#[test]
fn reconnect_returns_event_ledger_records_after_client_cursor() {
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
            cursor: AcpCursor::default(),
        })
        .expect("binding should persist");
    runtime
        .record_event(AcpEventLedgerAppend {
            context: context(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            kind: AcpEventLedgerKind::SessionUpdate,
            run_id: None,
            approval_id: None,
            redacted_summary: "Session opened".to_owned(),
            redacted_payload: json!({ "event": "session.new" }),
        })
        .expect("first event should persist");
    runtime
        .record_event(AcpEventLedgerAppend {
            context: context(),
            acp_session_id: "acp-session-a".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            kind: AcpEventLedgerKind::Cancel,
            run_id: Some("01BX5ZZKBKACTAV9WEVGEMMVRZ".to_owned()),
            approval_id: None,
            redacted_summary: "Cancel requested".to_owned(),
            redacted_payload: json!({ "event": "run.abort" }),
        })
        .expect("second event should persist");

    let outcome = runtime
        .reconnect(&context(), "acp-session-a", AcpCursor { sequence: 1 })
        .expect("reconnect should succeed");

    assert_eq!(outcome.event_ledger.len(), 1);
    assert_eq!(outcome.event_ledger[0].kind, AcpEventLedgerKind::Cancel);
    assert_eq!(outcome.event_ledger[0].sequence, 2);
}

#[test]
fn event_ledger_retention_is_bounded_per_session() {
    let mut index = AcpBindingsIndex {
        schema_version: 1,
        updated_at_unix_ms: 10_000,
        session_bindings: Vec::new(),
        conversation_bindings: Vec::new(),
        pending_prompts: Vec::new(),
        event_ledger: (0..MAX_EVENT_LEDGER_EVENTS_PER_SESSION + 5)
            .map(|offset| {
                event_record(
                    format!("acpevt_{offset}").as_str(),
                    (offset + 1) as u64,
                    offset as i64,
                )
            })
            .collect(),
    };

    prune_event_ledger(&mut index);

    assert_eq!(index.event_ledger.len(), MAX_EVENT_LEDGER_EVENTS_PER_SESSION);
    assert_eq!(index.event_ledger.first().map(|entry| entry.sequence), Some(6));
    assert_eq!(
        index.event_ledger.last().map(|entry| entry.sequence),
        Some((MAX_EVENT_LEDGER_EVENTS_PER_SESSION + 5) as u64)
    );
}

#[test]
fn event_ledger_record_matches_golden_fixture() {
    let actual = serde_json::to_value(event_record("acpevt_fixture", 42, 1_700_000_000_000))
        .expect("event should serialize");
    let expected = read_golden_json("acp_event_ledger_record.json");

    assert_eq!(actual, expected);
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
fn config_rejects_client_owned_runtime_launch_metadata() {
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
            config: json!({
                "runtime_backend": "trusted-id",
                "runtime_executable": "C:/client/owned.exe",
            }),
            cursor: AcpCursor::default(),
        })
        .expect_err("client-owned process launch metadata should be rejected");

    assert_eq!(error.stable_code(), "acp/invalid_field");
    assert!(error.to_string().contains("process launch authority"));
}

#[test]
fn conversation_binding_rejects_excessive_scope_count() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let runtime = AcpRuntime::open(tempdir.path().join("acp")).expect("runtime should open");
    let scopes = (0..=128).map(|index| format!("scope-{index}")).collect::<Vec<_>>();

    let error = runtime
        .upsert_conversation_binding(ConversationBindingUpsert {
            connector_kind: "acp".to_owned(),
            external_identity: "user-a".to_owned(),
            external_conversation_id: "thread-1".to_owned(),
            palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            owner_principal: "operator".to_owned(),
            device_id: "desktop".to_owned(),
            channel: None,
            scopes,
            sensitivity: ConversationBindingSensitivity::Internal,
            delivery_cursor: AcpCursor::default(),
            last_event_id: None,
        })
        .expect_err("oversized scope list must be rejected before normalization");

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
fn conversation_binding_repair_marks_principal_mismatch_without_widening() {
    let index = AcpBindingsIndex {
        schema_version: 1,
        updated_at_unix_ms: 10_000,
        session_bindings: Vec::new(),
        conversation_bindings: vec![
            conversation_record("convbind_a", "operator:a", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1),
            conversation_record("convbind_b", "operator:b", "01BX5ZZKBKACTAV9WEVGEMMVRZ", 2),
        ],
        pending_prompts: Vec::new(),
        event_ledger: Vec::new(),
    };

    let plan = build_repair_plan(&index, true);

    assert_eq!(plan.actions.len(), 2);
    assert!(plan.actions.iter().all(|action| action.action == "mark_stale"));
    assert!(plan.actions.iter().all(|action| !action.automatic_apply));
    assert!(plan.actions.iter().all(|action| action.conflict_kind == "principal_mismatch"));
}

#[test]
fn conversation_binding_repair_apply_skips_principal_mismatch_manual_actions() {
    let tempdir = tempfile::tempdir().expect("tempdir should be created");
    let root = tempdir.path().join("acp");
    let runtime = AcpRuntime {
        root: root.clone(),
        index_path: root.join(ACP_BINDINGS_INDEX_FILE_NAME),
        index: Mutex::new(AcpBindingsIndex {
            schema_version: 1,
            updated_at_unix_ms: 10_000,
            session_bindings: Vec::new(),
            conversation_bindings: vec![
                conversation_record("convbind_a", "operator:a", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1),
                conversation_record("convbind_b", "operator:b", "01BX5ZZKBKACTAV9WEVGEMMVRZ", 2),
            ],
            pending_prompts: Vec::new(),
            event_ledger: Vec::new(),
        }),
        rate_limits: Mutex::new(BTreeMap::new()),
        live_manager: live_runtime_manager::AcpLiveRuntimeManager::open(
            root.as_path(),
            false,
            AcpRuntimeConfig::default(),
        )
        .expect("live manager should initialize"),
    };

    let applied = runtime.apply_conversation_binding_repair().expect("repair should apply");
    let snapshot = runtime.snapshot().expect("snapshot should load");

    assert_eq!(applied.actions.len(), 2);
    assert!(applied.actions.iter().all(|action| !action.automatic_apply));
    assert!(snapshot
        .conversation_bindings
        .iter()
        .any(|entry| { entry.binding_id == "convbind_a" && entry.updated_at_unix_ms == 1 }));
    assert!(snapshot
        .conversation_bindings
        .iter()
        .any(|entry| { entry.binding_id == "convbind_b" && entry.updated_at_unix_ms == 2 }));
}

#[test]
fn conversation_binding_repair_plan_matches_golden_fixture() {
    let index = AcpBindingsIndex {
        schema_version: 1,
        updated_at_unix_ms: 10_000,
        session_bindings: Vec::new(),
        conversation_bindings: vec![
            conversation_record("convbind_a", "operator", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1),
            conversation_record("convbind_b", "operator", "01BX5ZZKBKACTAV9WEVGEMMVRZ", 2),
        ],
        pending_prompts: Vec::new(),
        event_ledger: Vec::new(),
    };

    let actual = serde_json::to_value(build_repair_plan(&index, true))
        .expect("repair plan should serialize");
    let expected = read_golden_json("acp_binding_repair_plan.json");

    assert_eq!(actual, expected);
}

#[test]
fn conversation_binding_repair_can_mark_parent_missing_for_required_parent_scope() {
    let mut record =
        conversation_record("convbind_parent", "operator", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 1);
    record.scopes.push("parent:required".to_owned());
    let index = AcpBindingsIndex {
        schema_version: 1,
        updated_at_unix_ms: 10_000,
        session_bindings: Vec::new(),
        conversation_bindings: vec![record],
        pending_prompts: Vec::new(),
        event_ledger: Vec::new(),
    };

    let plan = build_repair_plan(&index, true);

    assert_eq!(plan.actions.len(), 1);
    assert_eq!(plan.actions[0].action, "mark_stale");
    assert_eq!(plan.actions[0].conflict_kind, "parent_missing");
    assert!(plan.actions[0].automatic_apply);
}

#[test]
fn translator_rejects_unknown_event_types_as_compatibility_errors() {
    assert_eq!(translate_palyra_event_type("model_token").unwrap(), "message.delta");
    let error =
        translate_palyra_event_type("provider.raw.unknown").expect_err("unknown event fails");
    assert_eq!(error.stable_code(), "acp/compatibility_error");
}

fn conversation_record(
    binding_id: &str,
    owner_principal: &str,
    palyra_session_id: &str,
    updated_at_unix_ms: i64,
) -> ConversationBindingRecord {
    ConversationBindingRecord {
        schema_version: 1,
        binding_id: binding_id.to_owned(),
        connector_kind: "acp".to_owned(),
        external_identity: "user-a".to_owned(),
        external_conversation_id: "thread-1".to_owned(),
        palyra_session_id: palyra_session_id.to_owned(),
        owner_principal: owner_principal.to_owned(),
        device_id: "desktop".to_owned(),
        channel: None,
        scopes: vec!["sessions:read".to_owned()],
        sensitivity: ConversationBindingSensitivity::Internal,
        delivery_cursor: AcpCursor::default(),
        last_event_id: Some(format!("event-{updated_at_unix_ms}")),
        conflict_state: ConversationBindingConflictState::None,
        created_at_unix_ms: 0,
        updated_at_unix_ms,
    }
}

fn event_record(event_id: &str, sequence: u64, created_at_unix_ms: i64) -> AcpEventLedgerRecord {
    AcpEventLedgerRecord {
        schema_version: 1,
        event_id: event_id.to_owned(),
        kind: AcpEventLedgerKind::SessionUpdate,
        sequence,
        acp_client_id: "zed-extension".to_owned(),
        acp_session_id: "acp-session-a".to_owned(),
        palyra_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
        run_id: None,
        approval_id: None,
        redacted_summary: "Session updated".to_owned(),
        payload_sha256: "a".repeat(64),
        created_at_unix_ms,
        protocol_version: 1,
    }
}

fn read_golden_json(name: &str) -> Value {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests").join("golden").join(name);
    let payload = std::fs::read_to_string(path).expect("golden fixture should be readable");
    serde_json::from_str(payload.as_str()).expect("golden fixture should parse")
}
