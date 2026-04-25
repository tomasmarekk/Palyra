use super::{
    acp, build_tool_permission_request, map_list_sessions_response, map_permission_outcome,
    AcpSessionDefaults, AgentConnection, BridgeState, ClientBridgeRequest, PalyraAcpAgent,
    SessionBinding, PERMISSION_ALLOW_ALWAYS, PERMISSION_ALLOW_ONCE, PERMISSION_REJECT_ALWAYS,
    PERMISSION_REJECT_ONCE,
};
use crate::proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;

fn test_agent() -> PalyraAcpAgent {
    let (client_request_tx, _client_request_rx) = mpsc::unbounded_channel::<ClientBridgeRequest>();
    PalyraAcpAgent::new(
        AgentConnection {
            grpc_url: "http://127.0.0.1:7443".to_owned(),
            token: None,
            principal: "user:test".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: "cli".to_owned(),
            trace_id: "cli:test".to_owned(),
        },
        None,
        false,
        AcpSessionDefaults::default(),
        Arc::new(Mutex::new(BridgeState::default())),
        client_request_tx,
        PathBuf::from("."),
    )
}

#[test]
fn prompt_text_includes_resource_link_blocks() {
    let prompt = vec![
        acp::ContentBlock::from("Summarize the context"),
        acp::ContentBlock::ResourceLink(
            acp::ResourceLink::new("runbook", "https://example.test/runbook").title("Runbook"),
        ),
    ];

    let prompt = PalyraAcpAgent::prompt_text(&prompt);

    assert_eq!(prompt, "Summarize the context\nlink (Runbook): https://example.test/runbook");
}

#[tokio::test]
async fn initialize_negotiates_requested_protocol_version() {
    let agent = test_agent();
    let requested_version = acp::ProtocolVersion::V0;
    let response = <PalyraAcpAgent as acp::Agent>::initialize(
        &agent,
        acp::InitializeRequest::new(requested_version.clone()),
    )
    .await
    .expect("initialize must succeed");

    assert_eq!(response.protocol_version, requested_version);
    assert!(response.agent_capabilities.load_session);
    assert!(response.agent_capabilities.session_capabilities.list.is_some());
}

#[test]
fn build_tool_permission_request_maps_prompt_payload_and_options() {
    let session_id = acp::SessionId::new("session-alpha");
    let approval = common_v1::ToolApprovalRequest {
        proposal_id: Some(common_v1::CanonicalId { ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned() }),
        tool_name: "palyra.http.fetch".to_owned(),
        input_json: br#"{"url":"https://example.test"}"#.to_vec(),
        approval_required: true,
        approval_id: Some(common_v1::CanonicalId { ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned() }),
        prompt: Some(common_v1::ApprovalPrompt {
            title: "  Approve outbound request  ".to_owned(),
            risk_level: 3,
            subject_id: "network:egress".to_owned(),
            summary: "HTTP call to external API".to_owned(),
            options: Vec::new(),
            timeout_seconds: 30,
            details_json: br#"{"host":"example.test","method":"GET"}"#.to_vec(),
            policy_explanation: "external egress requires operator approval".to_owned(),
        }),
        request_summary: "GET https://example.test".to_owned(),
    };

    let request = build_tool_permission_request(&session_id, &approval)
        .expect("permission request mapping should succeed")
        .expect("proposal id should produce a permission request");

    assert_eq!(request.session_id, session_id);
    assert_eq!(request.tool_call.tool_call_id.0.as_ref(), "01ARZ3NDEKTSV4RRFFQ69G5FAV");
    assert_eq!(request.tool_call.fields.title.as_deref(), Some("Approve outbound request"));
    assert_eq!(request.tool_call.fields.kind, Some(acp::ToolKind::Execute));
    assert_eq!(request.tool_call.fields.status, Some(acp::ToolCallStatus::Pending));
    let raw_input = request
        .tool_call
        .fields
        .raw_input
        .as_ref()
        .expect("prompt payload should include raw input details");
    assert_eq!(raw_input["tool_name"], json!("palyra.http.fetch"));
    assert_eq!(raw_input["request_summary"], json!("GET https://example.test"));
    assert_eq!(raw_input["prompt"]["subject_id"], json!("network:egress"));
    assert_eq!(
        raw_input["prompt"]["policy_explanation"],
        json!("external egress requires operator approval")
    );
    assert_eq!(raw_input["prompt"]["details_json"]["host"], json!("example.test"));
    assert_eq!(raw_input["prompt"]["details_json"]["method"], json!("GET"));

    let option_contract = request
        .options
        .iter()
        .map(|option| (option.option_id.0.as_ref(), option.name.as_str(), option.kind))
        .collect::<Vec<_>>();
    assert_eq!(
        option_contract,
        vec![
            (PERMISSION_ALLOW_ONCE, "Allow once", acp::PermissionOptionKind::AllowOnce,),
            (PERMISSION_ALLOW_ALWAYS, "Allow always", acp::PermissionOptionKind::AllowAlways,),
            (PERMISSION_REJECT_ONCE, "Reject once", acp::PermissionOptionKind::RejectOnce,),
            (PERMISSION_REJECT_ALWAYS, "Reject always", acp::PermissionOptionKind::RejectAlways,),
        ]
    );
}

#[test]
fn build_tool_permission_request_without_proposal_id_returns_none() {
    let request = build_tool_permission_request(
        &acp::SessionId::new("session-alpha"),
        &common_v1::ToolApprovalRequest { proposal_id: None, ..Default::default() },
    )
    .expect("missing proposal id should not fail");

    assert!(request.is_none());
}

#[test]
fn map_permission_outcome_preserves_permission_option_semantics() {
    let once_scope = common_v1::ApprovalDecisionScope::Once as i32;
    let session_scope = common_v1::ApprovalDecisionScope::Session as i32;
    let cases = vec![
        (PERMISSION_ALLOW_ONCE, true, "approved:allow-once", once_scope),
        (PERMISSION_ALLOW_ALWAYS, true, "approved:allow-always", session_scope),
        (PERMISSION_REJECT_ONCE, false, "denied:reject-once", once_scope),
        (PERMISSION_REJECT_ALWAYS, false, "denied:reject-always", session_scope),
        ("unsupported-option", false, "denied:unsupported-option", once_scope),
    ];

    for (option_id, expected_approved, expected_reason, expected_scope) in cases {
        let response = acp::RequestPermissionResponse::new(
            acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(option_id)),
        );
        let mapped = map_permission_outcome(response);
        assert_eq!(mapped.0, expected_approved);
        assert_eq!(mapped.1, expected_reason);
        assert_eq!(mapped.2, expected_scope);
        assert_eq!(mapped.3, None);
    }

    let cancelled = map_permission_outcome(acp::RequestPermissionResponse::new(
        acp::RequestPermissionOutcome::Cancelled,
    ));
    assert_eq!(cancelled, (false, "cancelled_by_client".to_owned(), once_scope, None,));
}

#[test]
fn map_list_sessions_response_uses_session_key_fallback_and_binding_cwd() {
    let default_cwd = PathBuf::from("C:/workspace/default");
    let mut state = BridgeState::default();
    state.remember_binding(
        "session-alpha",
        SessionBinding {
            gateway_session_id_ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
            session_key: "session-alpha".to_owned(),
            session_label: Some("Project Alpha".to_owned()),
            cwd: PathBuf::from("C:/workspace/alpha"),
        },
    );
    let response = gateway_v1::ListSessionsResponse {
        v: 1,
        sessions: vec![
            gateway_v1::SessionSummary {
                session_id: Some(common_v1::CanonicalId {
                    ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAY".to_owned(),
                }),
                session_key: "session-alpha".to_owned(),
                session_label: "  Project Alpha  ".to_owned(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
                last_run_id: None,
                archived_at_unix_ms: 0,
                ..Default::default()
            },
            gateway_v1::SessionSummary {
                session_id: Some(common_v1::CanonicalId {
                    ulid: "01ARZ3NDEKTSV4RRFFQ69G5FAZ".to_owned(),
                }),
                session_key: String::new(),
                session_label: "  ".to_owned(),
                created_at_unix_ms: 0,
                updated_at_unix_ms: 0,
                last_run_id: None,
                archived_at_unix_ms: 0,
                ..Default::default()
            },
        ],
        next_after_session_key: "  cursor-2  ".to_owned(),
    };

    let mapped = map_list_sessions_response(response, &state, &default_cwd);
    assert_eq!(mapped.next_cursor.as_deref(), Some("cursor-2"));
    assert_eq!(mapped.sessions.len(), 2);

    assert_eq!(mapped.sessions[0].session_id.0.as_ref(), "session-alpha");
    assert_eq!(mapped.sessions[0].cwd, PathBuf::from("C:/workspace/alpha"));
    assert_eq!(mapped.sessions[0].title.as_deref(), Some("Project Alpha"));

    assert_eq!(mapped.sessions[1].session_id.0.as_ref(), "01ARZ3NDEKTSV4RRFFQ69G5FAZ");
    assert_eq!(mapped.sessions[1].cwd, default_cwd);
    assert_eq!(mapped.sessions[1].title, None);
}

#[test]
fn bridge_state_lookup_covers_acp_session_key_and_gateway_session_id() {
    let binding = SessionBinding {
        gateway_session_id_ulid: "01ARZ3NDEKTSV4RRFFQ69G5FA1".to_owned(),
        session_key: "ops:triage".to_owned(),
        session_label: Some("Ops triage".to_owned()),
        cwd: PathBuf::from("C:/workspace/triage"),
    };
    let mut state = BridgeState::default();
    state.remember_binding("acp-session-1", binding.clone());

    assert_eq!(
        state.lookup_binding("acp-session-1").expect("binding by ACP session id").cwd,
        binding.cwd
    );
    assert_eq!(
        state.lookup_binding("ops:triage").expect("binding by session key").gateway_session_id_ulid,
        binding.gateway_session_id_ulid
    );
    assert_eq!(
        state
            .lookup_binding("01ARZ3NDEKTSV4RRFFQ69G5FA1")
            .expect("binding by gateway session id")
            .session_key,
        binding.session_key
    );
}
