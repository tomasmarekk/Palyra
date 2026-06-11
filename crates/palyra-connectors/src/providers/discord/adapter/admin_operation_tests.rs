//! Unit tests for Discord admin operations (read, search, edit, react) covering preflight
//! permission verdicts and denial paths against a scripted fake transport.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use reqwest::Url;
use serde_json::{json, Value};

use super::{
    DiscordAdapterConfig, DiscordConnectorAdapter, DiscordCredential, DiscordCredentialResolver,
    DiscordTransport, DiscordTransportResponse,
};
use crate::{
    protocol::{
        ConnectorConversationTarget, ConnectorKind, ConnectorMessageEditRequest,
        ConnectorMessageLocator, ConnectorMessageMutationStatus, ConnectorMessageReactionRequest,
        ConnectorMessageReadRequest, ConnectorMessageSearchRequest,
    },
    storage::ConnectorInstanceRecord,
    supervisor::{ConnectorAdapter, ConnectorAdapterError},
};

#[derive(Debug, Default)]
struct StaticCredentialResolver;

#[async_trait]
impl DiscordCredentialResolver for StaticCredentialResolver {
    async fn resolve_credential(
        &self,
        _instance: &ConnectorInstanceRecord,
    ) -> Result<DiscordCredential, ConnectorAdapterError> {
        Ok(DiscordCredential { token: "discord-token".to_owned(), source: "test".to_owned() })
    }
}

#[derive(Default)]
struct AdminFakeTransport {
    get_responses: Mutex<VecDeque<Result<DiscordTransportResponse, ConnectorAdapterError>>>,
    patch_responses: Mutex<VecDeque<Result<DiscordTransportResponse, ConnectorAdapterError>>>,
    delete_responses: Mutex<VecDeque<Result<DiscordTransportResponse, ConnectorAdapterError>>>,
    put_responses: Mutex<VecDeque<Result<DiscordTransportResponse, ConnectorAdapterError>>>,
}

impl AdminFakeTransport {
    fn push_get(&self, response: DiscordTransportResponse) {
        self.get_responses
            .lock()
            .expect("get responses lock should be healthy")
            .push_back(Ok(response));
    }

    fn push_patch(&self, response: DiscordTransportResponse) {
        self.patch_responses
            .lock()
            .expect("patch responses lock should be healthy")
            .push_back(Ok(response));
    }

    fn push_delete(&self, response: DiscordTransportResponse) {
        self.delete_responses
            .lock()
            .expect("delete responses lock should be healthy")
            .push_back(Ok(response));
    }

    fn push_put(&self, response: DiscordTransportResponse) {
        self.put_responses
            .lock()
            .expect("put responses lock should be healthy")
            .push_back(Ok(response));
    }
}

#[async_trait]
impl DiscordTransport for AdminFakeTransport {
    async fn get(
        &self,
        _url: &Url,
        _token: &str,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        self.get_responses
            .lock()
            .expect("get responses lock should be healthy")
            .pop_front()
            .ok_or_else(|| ConnectorAdapterError::Backend("missing fake GET response".to_owned()))?
    }

    async fn post_json(
        &self,
        _url: &Url,
        _token: &str,
        _payload: &Value,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        Ok(DiscordTransportResponse {
            status: 200,
            headers: Default::default(),
            body: "{\"id\":\"default\"}".to_owned(),
        })
    }

    async fn post_multipart(
        &self,
        _url: &Url,
        _token: &str,
        _payload: &Value,
        _files: &[super::DiscordMultipartAttachment],
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        Ok(DiscordTransportResponse {
            status: 200,
            headers: Default::default(),
            body: "{\"id\":\"default\"}".to_owned(),
        })
    }

    async fn put(
        &self,
        _url: &Url,
        _token: &str,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        self.put_responses
            .lock()
            .expect("put responses lock should be healthy")
            .pop_front()
            .ok_or_else(|| ConnectorAdapterError::Backend("missing fake PUT response".to_owned()))?
    }

    async fn patch_json(
        &self,
        _url: &Url,
        _token: &str,
        _payload: &Value,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        self.patch_responses
            .lock()
            .expect("patch responses lock should be healthy")
            .pop_front()
            .ok_or_else(|| {
                ConnectorAdapterError::Backend("missing fake PATCH response".to_owned())
            })?
    }

    async fn delete(
        &self,
        _url: &Url,
        _token: &str,
        _timeout_ms: u64,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        self.delete_responses
                .lock()
                .expect("delete responses lock should be healthy")
                .pop_front()
                .ok_or_else(|| {
                    ConnectorAdapterError::Backend("missing fake DELETE response".to_owned())
                })?
    }
}

fn make_adapter(transport: Arc<AdminFakeTransport>) -> DiscordConnectorAdapter {
    DiscordConnectorAdapter::with_dependencies(
        DiscordAdapterConfig::default(),
        transport,
        Arc::new(StaticCredentialResolver),
    )
}

fn instance() -> ConnectorInstanceRecord {
    ConnectorInstanceRecord {
        connector_id: "discord:default".to_owned(),
        kind: ConnectorKind::Discord,
        principal: "channel:discord:default".to_owned(),
        auth_profile_ref: Some("discord.default".to_owned()),
        token_vault_ref: Some("global/discord_bot_token".to_owned()),
        egress_allowlist: vec!["discord.com".to_owned(), "*.discord.com".to_owned()],
        enabled: true,
        readiness: crate::protocol::ConnectorReadiness::Ready,
        liveness: crate::protocol::ConnectorLiveness::Running,
        restart_count: 0,
        last_error: None,
        last_inbound_unix_ms: None,
        last_outbound_unix_ms: None,
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
    }
}

fn ok_identity_response() -> DiscordTransportResponse {
    DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: "{\"id\":\"bot-1\",\"username\":\"Palyra Bot\"}".to_owned(),
    }
}

fn ok_channel_response() -> DiscordTransportResponse {
    DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: format!(
            "{{\"id\":\"chan-1\",\"permissions\":\"{}\"}}",
            (super::super::permissions::DISCORD_PERMISSION_VIEW_CHANNEL
                | super::super::permissions::DISCORD_PERMISSION_READ_MESSAGE_HISTORY
                | super::super::permissions::DISCORD_PERMISSION_SEND_MESSAGES
                | super::super::permissions::DISCORD_PERMISSION_MANAGE_MESSAGES
                | super::super::permissions::DISCORD_PERMISSION_ADD_REACTIONS)
        ),
    }
}

fn sample_message(message_id: &str, author_id: &str, body: &str) -> DiscordTransportResponse {
    DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: json!({
            "id": message_id,
            "channel_id": "chan-1",
            "guild_id": "guild-1",
            "content": body,
            "author": {
                "id": author_id,
                "username": "user"
            },
            "attachments": [],
            "reactions": []
        })
        .to_string(),
    }
}

#[test]
fn discord_capabilities_publish_admin_metadata() {
    let capabilities = DiscordConnectorAdapter::default().capabilities();
    assert!(capabilities.message.read.supported);
    assert_eq!(capabilities.message.read.policy_action.as_deref(), Some("channel.message.read"));
    assert_eq!(capabilities.message.delete.audit_event_type.as_deref(), Some("message.delete"));
    assert_eq!(
        capabilities.message.edit.approval_mode,
        Some(crate::protocol::ConnectorApprovalMode::Conditional)
    );
    assert!(capabilities
        .message
        .react_add
        .required_permissions
        .iter()
        .any(|entry| entry == "Add Reactions"));
}

#[tokio::test]
async fn read_messages_fetches_exact_message() {
    let transport = Arc::new(AdminFakeTransport::default());
    transport.push_get(ok_identity_response());
    transport.push_get(ok_channel_response());
    transport.push_get(sample_message("msg-1", "bot-1", "hello admin read"));
    let adapter = make_adapter(transport);

    let result = adapter
        .read_messages(
            &instance(),
            &ConnectorMessageReadRequest {
                target: ConnectorConversationTarget {
                    conversation_id: "chan-1".to_owned(),
                    thread_id: None,
                },
                message_id: Some("msg-1".to_owned()),
                before_message_id: None,
                after_message_id: None,
                around_message_id: None,
                limit: 1,
            },
        )
        .await
        .expect("read request should succeed");

    assert!(result.preflight.allowed);
    assert_eq!(result.messages.len(), 1);
    assert_eq!(result.messages[0].locator.message_id, "msg-1");
    assert!(result.messages[0].is_connector_authored);
}

#[tokio::test]
async fn search_messages_filters_content_and_author() {
    let transport = Arc::new(AdminFakeTransport::default());
    transport.push_get(ok_identity_response());
    transport.push_get(ok_channel_response());
    transport.push_get(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: json!([
            {
                "id": "msg-1",
                "channel_id": "chan-1",
                "guild_id": "guild-1",
                "content": "deploy complete",
                "author": { "id": "bot-1", "username": "bot" },
                "attachments": [],
                "reactions": []
            },
            {
                "id": "msg-2",
                "channel_id": "chan-1",
                "guild_id": "guild-1",
                "content": "deploy pending",
                "author": { "id": "user-2", "username": "other" },
                "attachments": [],
                "reactions": []
            }
        ])
        .to_string(),
    });
    let adapter = make_adapter(transport);

    let result = adapter
        .search_messages(
            &instance(),
            &ConnectorMessageSearchRequest {
                target: ConnectorConversationTarget {
                    conversation_id: "chan-1".to_owned(),
                    thread_id: None,
                },
                query: Some("complete".to_owned()),
                author_id: Some("bot-1".to_owned()),
                has_attachments: Some(false),
                before_message_id: None,
                limit: 25,
            },
        )
        .await
        .expect("search request should succeed");

    assert!(result.preflight.allowed);
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].locator.message_id, "msg-1");
}

#[tokio::test]
async fn edit_message_denies_non_connector_authored_messages() {
    let transport = Arc::new(AdminFakeTransport::default());
    transport.push_get(ok_identity_response());
    transport.push_get(ok_channel_response());
    transport.push_get(sample_message("msg-9", "user-9", "human text"));
    let adapter = make_adapter(transport);

    let result = adapter
        .edit_message(
            &instance(),
            &ConnectorMessageEditRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "chan-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: "msg-9".to_owned(),
                },
                body: "edited".to_owned(),
            },
        )
        .await
        .expect("edit request should resolve");

    assert_eq!(result.status, ConnectorMessageMutationStatus::Denied);
    assert!(result.reason.as_deref().is_some_and(|reason| reason.contains("connector-authored")));
}

#[tokio::test]
async fn edit_message_updates_connector_authored_messages() {
    let transport = Arc::new(AdminFakeTransport::default());
    transport.push_get(ok_identity_response());
    transport.push_get(ok_channel_response());
    transport.push_get(sample_message("msg-10", "bot-1", "before"));
    transport.push_patch(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: json!({
            "id": "msg-10",
            "channel_id": "chan-1",
            "guild_id": "guild-1",
            "content": "after",
            "author": { "id": "bot-1", "username": "bot" },
            "attachments": [],
            "reactions": []
        })
        .to_string(),
    });
    let adapter = make_adapter(transport);

    let result = adapter
        .edit_message(
            &instance(),
            &ConnectorMessageEditRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "chan-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: "msg-10".to_owned(),
                },
                body: "after".to_owned(),
            },
        )
        .await
        .expect("edit request should resolve");

    assert_eq!(result.status, ConnectorMessageMutationStatus::Updated);
    assert_eq!(result.diff.as_ref().and_then(|diff| diff.before_body.as_deref()), Some("before"));
    assert_eq!(result.diff.as_ref().and_then(|diff| diff.after_body.as_deref()), Some("after"));
}

#[tokio::test]
async fn delete_and_reaction_mutations_succeed() {
    let transport = Arc::new(AdminFakeTransport::default());
    transport.push_get(ok_identity_response());
    transport.push_get(ok_channel_response());
    transport.push_get(sample_message("msg-11", "bot-1", "delete me"));
    transport.push_delete(DiscordTransportResponse {
        status: 204,
        headers: Default::default(),
        body: String::new(),
    });
    let adapter = make_adapter(Arc::clone(&transport));

    let deleted = adapter
        .delete_message(
            &instance(),
            &crate::protocol::ConnectorMessageDeleteRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "chan-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: "msg-11".to_owned(),
                },
                reason: Some("cleanup".to_owned()),
            },
        )
        .await
        .expect("delete request should resolve");
    assert_eq!(deleted.status, ConnectorMessageMutationStatus::Deleted);

    let transport = Arc::new(AdminFakeTransport::default());
    transport.push_get(ok_identity_response());
    transport.push_get(ok_channel_response());
    transport.push_get(sample_message("msg-12", "bot-1", "react"));
    transport.push_put(DiscordTransportResponse {
        status: 204,
        headers: Default::default(),
        body: String::new(),
    });
    transport.push_get(DiscordTransportResponse {
        status: 200,
        headers: Default::default(),
        body: json!({
            "id": "msg-12",
            "channel_id": "chan-1",
            "guild_id": "guild-1",
            "content": "react",
            "author": { "id": "bot-1", "username": "bot" },
            "attachments": [],
            "reactions": [{
                "count": 1,
                "me": true,
                "emoji": { "name": "thumbsup" }
            }]
        })
        .to_string(),
    });
    let adapter = make_adapter(transport);

    let reacted = adapter
        .add_reaction(
            &instance(),
            &ConnectorMessageReactionRequest {
                locator: ConnectorMessageLocator {
                    target: ConnectorConversationTarget {
                        conversation_id: "chan-1".to_owned(),
                        thread_id: None,
                    },
                    message_id: "msg-12".to_owned(),
                },
                emoji: "thumbsup".to_owned(),
            },
        )
        .await
        .expect("reaction add should resolve");
    assert_eq!(reacted.status, ConnectorMessageMutationStatus::ReactionAdded);
    assert_eq!(reacted.message.as_ref().map(|value| value.reactions.len()), Some(1));
}
