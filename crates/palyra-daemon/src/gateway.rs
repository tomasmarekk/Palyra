#![cfg_attr(test, allow(dead_code, private_interfaces))]

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use palyra_a2ui::{
    apply_patch_document, build_replace_root_patch, parse_patch_document, patch_document_to_bytes,
};
#[cfg(test)]
use palyra_auth::{AuthCredentialType, AuthProfileRecord};
use palyra_auth::{
    AuthExpiryDistribution, AuthHealthSummary, AuthProfileRegistry, OAuthRefreshAdapter,
    OAuthRefreshOutcome,
};
use palyra_common::{build_metadata, validate_canonical_id, CANONICAL_PROTOCOL_MAJOR};
use palyra_policy::{evaluate_with_config, PolicyDecision, PolicyEvaluationConfig, PolicyRequest};
#[cfg(test)]
use palyra_vault::{
    BackendPreference as VaultBackendPreference, VaultConfig as VaultConfigOptions,
};
use palyra_vault::{SecretMetadata as VaultSecretMetadata, Vault, VaultError, VaultScope};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::{Status, Streaming};
use tracing::warn;
use ulid::Ulid;

#[cfg(test)]
use crate::application::approvals::{build_tool_approval_subject_id, PendingToolApproval};
pub(crate) use crate::transport::grpc::auth::{GatewayAuthConfig, RequestContext};
pub(crate) use crate::transport::grpc::proto;
pub(crate) use crate::transport::grpc::services::{
    auth::AuthServiceImpl, cron::CronServiceImpl, vault::VaultServiceImpl,
};
use crate::{
    agents::{
        AgentCreateOutcome, AgentCreateRequest, AgentRecord, AgentRegistry, AgentRegistryError,
        AgentResolutionSource,
    },
    channel_router::{
        ChannelPairingSnapshot, ChannelRouter, ChannelRouterConfig,
        InboundMessage as ChannelInboundMessage, PairingApprovalOutcome, PairingCodeRecord,
        PairingConsumeOutcome, RoutePreview as ChannelRoutePreview,
    },
    cron::schedule_to_proto,
    execution_backends::ExecutionBackendPreference,
    journal::{
        ApprovalCreateRequest, ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot,
        ApprovalPromptOption, ApprovalPromptRecord, ApprovalRecord, ApprovalResolveRequest,
        ApprovalRiskLevel, ApprovalSubjectType, ApprovalsListFilter, CanvasStatePatchRecord,
        CanvasStateSnapshotRecord, CanvasStateTransitionRequest, CronConcurrencyPolicy,
        CronJobCreateRequest, CronJobRecord, CronJobUpdatePatch, CronJobsListFilter,
        CronRunFinalizeRequest, CronRunRecord, CronRunStartRequest, CronRunStatus,
        CronRunsListFilter, JournalAppendRequest, JournalError, JournalEventRecord, JournalStore,
        MemoryEmbeddingsBackfillOutcome, MemoryItemCreateRequest, MemoryItemsListFilter,
        MemoryMaintenanceRequest, MemoryMaintenanceStatus, MemoryPurgeRequest,
        MemoryRetentionPolicy, MemorySearchHit, MemorySearchRequest, MemorySource,
        OrchestratorCancelRequest, OrchestratorRunStartRequest, OrchestratorRunStatusSnapshot,
        OrchestratorSessionRecord, OrchestratorSessionResolveOutcome,
        OrchestratorSessionResolveRequest, OrchestratorTapeAppendRequest, OrchestratorTapeRecord,
        OrchestratorUsageDelta, SkillStatusRecord, SkillStatusUpsertRequest,
    },
    media::MediaRuntimeConfig,
    model_provider::{
        AudioTranscriptionRequest, AudioTranscriptionResponse, ModelProvider, ProviderError,
        ProviderRequest, ProviderStatusSnapshot,
    },
    orchestrator::{RunLifecycleState, RunStateMachine, RunTransition},
    tool_protocol::{
        execute_tool_call, tool_policy_snapshot, ToolCallConfig, ToolCallPolicySnapshot,
        ToolExecutionOutcome,
    },
};

use proto::palyra::{common::v1 as common_v1, cron::v1 as cron_v1, gateway::v1 as gateway_v1};

pub const HEADER_PRINCIPAL: &str = "x-palyra-principal";
pub const HEADER_DEVICE_ID: &str = "x-palyra-device-id";
pub const HEADER_CHANNEL: &str = "x-palyra-channel";
pub(crate) const MAX_JOURNAL_RECENT_EVENTS: usize = 100;
pub(crate) const MAX_SESSIONS_PAGE_LIMIT: usize = 500;
pub(crate) const MAX_AGENTS_PAGE_LIMIT: usize = 500;
pub(crate) const JOURNAL_WRITE_LATENCY_BUDGET_MS: u128 = 25;
pub(crate) const TOOL_EXECUTION_LATENCY_BUDGET_MS: u128 = 200;
pub(crate) const MIN_TAPE_PAGE_LIMIT: usize = 1;
pub(crate) const SENSITIVE_TOOLS_DENY_REASON: &str =
    "allow_sensitive_tools=true is denied by default and requires explicit approvals";
pub(crate) const CANCELLED_REASON: &str = "cancelled by request";
pub(crate) const APPROVAL_CHANNEL_UNAVAILABLE_REASON: &str =
    "approval required but no interactive approval channel is available for this run";
pub(crate) const APPROVAL_DENIED_REASON: &str =
    "tool execution denied by explicit client approval response";
pub(crate) const APPROVAL_DECISION_CACHE_CAPACITY: usize = 1_024;
pub(crate) const MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN: usize = 1_024;
pub(crate) const MAX_CRON_JOB_NAME_BYTES: usize = 128;
pub(crate) const MAX_CRON_PROMPT_BYTES: usize = 16 * 1024;
pub(crate) const MAX_CRON_JITTER_MS: u64 = 60_000;
pub(crate) const MAX_CRON_PAGE_LIMIT: usize = 500;
pub(crate) const MAX_APPROVAL_PAGE_LIMIT: usize = 500;
pub(crate) const MAX_APPROVAL_EXPORT_LIMIT: usize = 5_000;
pub(crate) const MAX_APPROVAL_EXPORT_CHUNK_BYTES: usize = 64 * 1024;
pub(crate) const APPROVAL_EXPORT_NDJSON_SCHEMA_ID: &str = "palyra.approvals.export.ndjson.v1";
pub(crate) const APPROVAL_EXPORT_NDJSON_RECORD_TYPE_ENTRY: &str = "approval_record";
pub(crate) const APPROVAL_EXPORT_NDJSON_RECORD_TYPE_TRAILER: &str = "export_trailer";
pub(crate) const APPROVAL_EXPORT_CHAIN_SEED_HEX: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
pub(crate) const MAX_MEMORY_PAGE_LIMIT: usize = 500;
pub(crate) const MAX_MEMORY_SEARCH_TOP_K: usize = 64;
pub(crate) const MAX_MEMORY_ITEM_BYTES: usize = 16 * 1024;
pub(crate) const MAX_MEMORY_ITEM_TOKENS: usize = 2_048;
pub(crate) const MAX_MEMORY_TOOL_QUERY_BYTES: usize = 4 * 1024;
pub(crate) const MAX_MEMORY_TOOL_TAGS: usize = 32;
pub(crate) const MAX_PREVIOUS_RUN_CONTEXT_TAPE_EVENTS: usize = 128;
pub(crate) const MAX_PREVIOUS_RUN_CONTEXT_TURNS: usize = 6;
pub(crate) const MAX_PREVIOUS_RUN_CONTEXT_ENTRY_CHARS: usize = 512;
pub(crate) const MAX_WORKSPACE_PATCH_TOOL_INPUT_BYTES: usize = 256 * 1024;
pub(crate) const MAX_HTTP_FETCH_TOOL_INPUT_BYTES: usize = 64 * 1024;
pub(crate) const MAX_HTTP_FETCH_BODY_BYTES: usize = 512 * 1024;
pub(crate) const MAX_HTTP_FETCH_REDIRECTS: usize = 10;
pub(crate) const MAX_HTTP_FETCH_CACHE_KEY_BYTES: usize = 4 * 1024;
pub(crate) const MAX_BROWSER_TOOL_INPUT_BYTES: usize = 128 * 1024;
pub(crate) const MAX_CANVAS_ID_BYTES: usize = 64;
pub(crate) const MAX_CANVAS_BUNDLE_ID_BYTES: usize = 128;
pub(crate) const MAX_CANVAS_ASSET_PATH_BYTES: usize = 256;
pub(crate) const MAX_CANVAS_ASSET_CONTENT_TYPE_BYTES: usize = 128;
pub(crate) const MAX_CANVAS_ALLOWED_PARENT_ORIGINS: usize = 16;
pub(crate) const MAX_CANVAS_ORIGIN_BYTES: usize = 256;
pub(crate) const MAX_CANVAS_TOKEN_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
pub(crate) const MIN_CANVAS_TOKEN_TTL_MS: u64 = 30 * 1_000;
pub(crate) const MAX_CANVAS_RECOVERY_SNAPSHOTS: usize = 10_000;
pub(crate) const MAX_CANVAS_STREAM_PATCH_BATCH: usize = 64;
pub(crate) const CANVAS_STREAM_POLL_INTERVAL: Duration = Duration::from_millis(250);
pub(crate) const MAX_PATCH_TOOL_REDACTION_PATTERNS: usize = 64;
pub(crate) const MAX_PATCH_TOOL_SECRET_FILE_MARKERS: usize = 64;
pub(crate) const MAX_PATCH_TOOL_PATTERN_BYTES: usize = 256;
pub(crate) const MAX_PATCH_TOOL_MARKER_BYTES: usize = 256;
pub(crate) const MAX_AGENT_STATUS_BINDINGS: usize = 128;
pub(crate) const MAX_VAULT_SECRET_BYTES: usize = 64 * 1024;
pub(crate) const MAX_VAULT_LIST_RESULTS: usize = 1_000;
pub(crate) const VAULT_RATE_LIMIT_WINDOW_MS: u64 = 1_000;
pub(crate) const VAULT_RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 30;
pub(crate) const VAULT_RATE_LIMIT_MAX_PRINCIPAL_BUCKETS: usize = 4_096;
pub(crate) const MEMORY_SEARCH_LATENCY_BUDGET_MS: u128 = 75;
pub(crate) const MEMORY_SEARCH_CACHE_CAPACITY: usize = 128;
pub(crate) const MEMORY_AUTO_INJECT_MIN_SCORE: f64 = 0.2;
pub(crate) const APPROVAL_POLICY_ID: &str = "tool_call_policy.v1";
pub(crate) const APPROVAL_PROMPT_TIMEOUT_SECONDS: u32 = 60;
pub(crate) const APPROVAL_REQUEST_SUMMARY_MAX_BYTES: usize = 1024;
pub(crate) const TOOL_APPROVAL_RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const SKILL_EXECUTION_DENY_REASON_PREFIX: &str =
    "skill execution blocked by security gate";
pub(crate) const MEMORY_SEARCH_TOOL_NAME: &str = "palyra.memory.search";
pub(crate) const MEMORY_RECALL_TOOL_NAME: &str = "palyra.memory.recall";
pub(crate) const MEMORY_RETAIN_TOOL_NAME: &str = "palyra.memory.retain";
pub(crate) const MEMORY_REFLECT_TOOL_NAME: &str = "palyra.memory.reflect";
pub(crate) const ROUTINES_QUERY_TOOL_NAME: &str = "palyra.routines.query";
pub(crate) const ROUTINES_CONTROL_TOOL_NAME: &str = "palyra.routines.control";
pub(crate) const ARTIFACT_READ_TOOL_NAME: &str = "palyra.artifact.read";
pub(crate) const WORKSPACE_PATCH_TOOL_NAME: &str = "palyra.fs.apply_patch";
pub(crate) const PROCESS_RUNNER_TOOL_NAME: &str = "palyra.process.run";
pub(crate) const HTTP_FETCH_TOOL_NAME: &str = "palyra.http.fetch";
pub(crate) const TOOL_PROGRAM_RUN_TOOL_NAME: &str = "palyra.tool_program.run";
pub(crate) const BROWSER_SESSION_CREATE_TOOL_NAME: &str = "palyra.browser.session.create";
pub(crate) const BROWSER_SESSION_CLOSE_TOOL_NAME: &str = "palyra.browser.session.close";
pub(crate) const BROWSER_NAVIGATE_TOOL_NAME: &str = "palyra.browser.navigate";
pub(crate) const BROWSER_CLICK_TOOL_NAME: &str = "palyra.browser.click";
pub(crate) const BROWSER_TYPE_TOOL_NAME: &str = "palyra.browser.type";
pub(crate) const BROWSER_PRESS_TOOL_NAME: &str = "palyra.browser.press";
pub(crate) const BROWSER_SELECT_TOOL_NAME: &str = "palyra.browser.select";
pub(crate) const BROWSER_HIGHLIGHT_TOOL_NAME: &str = "palyra.browser.highlight";
pub(crate) const BROWSER_SCROLL_TOOL_NAME: &str = "palyra.browser.scroll";
pub(crate) const BROWSER_WAIT_FOR_TOOL_NAME: &str = "palyra.browser.wait_for";
pub(crate) const BROWSER_TITLE_TOOL_NAME: &str = "palyra.browser.title";
pub(crate) const BROWSER_SCREENSHOT_TOOL_NAME: &str = "palyra.browser.screenshot";
pub(crate) const BROWSER_PDF_TOOL_NAME: &str = "palyra.browser.pdf";
pub(crate) const BROWSER_OBSERVE_TOOL_NAME: &str = "palyra.browser.observe";
pub(crate) const BROWSER_NETWORK_LOG_TOOL_NAME: &str = "palyra.browser.network_log";
pub(crate) const BROWSER_CONSOLE_LOG_TOOL_NAME: &str = "palyra.browser.console_log";
pub(crate) const BROWSER_RESET_STATE_TOOL_NAME: &str = "palyra.browser.reset_state";
pub(crate) const BROWSER_TABS_LIST_TOOL_NAME: &str = "palyra.browser.tabs.list";
pub(crate) const BROWSER_TABS_OPEN_TOOL_NAME: &str = "palyra.browser.tabs.open";
pub(crate) const BROWSER_TABS_SWITCH_TOOL_NAME: &str = "palyra.browser.tabs.switch";
pub(crate) const BROWSER_TABS_CLOSE_TOOL_NAME: &str = "palyra.browser.tabs.close";
pub(crate) const BROWSER_PERMISSIONS_GET_TOOL_NAME: &str = "palyra.browser.permissions.get";
pub(crate) const BROWSER_PERMISSIONS_SET_TOOL_NAME: &str = "palyra.browser.permissions.set";

mod approvals;
mod canvas;
mod common;
mod cron_support;
mod messages;
mod runtime;
mod util;
mod vault;

pub(crate) use approvals::*;
pub(crate) use canvas::*;
pub(crate) use common::*;
pub(crate) use cron_support::*;
pub(crate) use messages::*;
pub(crate) use runtime::*;
pub(crate) use util::*;
pub(crate) use vault::*;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn ingest_memory_best_effort(
    runtime_state: &Arc<GatewayRuntimeState>,
    principal: &str,
    channel: Option<&str>,
    session_id: Option<&str>,
    source: MemorySource,
    content_text: &str,
    tags: Vec<String>,
    confidence: Option<f64>,
    reason: &str,
) {
    if content_text.trim().is_empty() {
        return;
    }
    if let Err(error) = crate::application::service_authorization::authorize_memory_action(
        principal,
        "memory.ingest",
        "memory:item",
    ) {
        runtime_state.record_denied();
        warn!(
            reason,
            principal,
            status_code = ?error.code(),
            status_message = %error.message(),
            "memory ingest best-effort skipped by policy"
        );
        return;
    }
    if let Err(error) = runtime_state
        .ingest_memory_item(MemoryItemCreateRequest {
            memory_id: Ulid::new().to_string(),
            principal: principal.to_owned(),
            channel: channel.map(str::to_owned),
            session_id: session_id.map(str::to_owned),
            source,
            content_text: content_text.to_owned(),
            tags,
            confidence,
            ttl_unix_ms: None,
        })
        .await
    {
        warn!(
            reason,
            status_code = ?error.code(),
            status_message = %error.message(),
            "memory ingest best-effort path rejected candidate"
        );
    }
}

pub(crate) fn tool_cancellation_requires_execution_drain(tool_name: &str) -> bool {
    matches!(tool_name, PROCESS_RUNNER_TOOL_NAME | TOOL_PROGRAM_RUN_TOOL_NAME | "palyra.plugin.run")
}

#[cfg(test)]
pub(crate) fn apply_tool_approval_outcome(
    mut decision: crate::tool_protocol::ToolDecision,
    tool_name: &str,
    approval: Option<&ToolApprovalOutcome>,
) -> crate::tool_protocol::ToolDecision {
    if !(decision.allowed && decision.approval_required) {
        return decision;
    }

    let Some(approval) = approval else {
        decision.allowed = false;
        decision.reason = format!(
            "{APPROVAL_CHANNEL_UNAVAILABLE_REASON}; tool={tool_name}; original_reason={}",
            decision.reason
        );
        return decision;
    };

    if approval.approved {
        decision.reason = format!(
            "explicit approval granted for tool={tool_name}; approval_reason={}; original_reason={}",
            approval.reason, decision.reason
        );
        return decision;
    }

    decision.allowed = false;
    decision.reason = format!(
        "{APPROVAL_DENIED_REASON}; tool={tool_name}; approval_reason={}; original_reason={}",
        approval.reason, decision.reason
    );
    decision
}

#[allow(clippy::result_large_err)]
pub(crate) async fn await_tool_approval_response(
    stream: &mut Streaming<common_v1::RunStreamRequest>,
    expected_session_id: &str,
    expected_run_id: &str,
    proposal_id: &str,
    approval_id: &str,
) -> Result<ToolApprovalOutcome, Status> {
    while let Some(item) = stream.next().await {
        let message = item.map_err(|error| {
            Status::internal(format!("failed to read approval stream item: {error}"))
        })?;
        if message.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }

        let message_session_id = canonical_id(message.session_id, "session_id")?;
        if message_session_id != expected_session_id {
            return Err(Status::invalid_argument(
                "run stream cannot switch session_id while awaiting tool approval response",
            ));
        }
        let message_run_id = canonical_id(message.run_id, "run_id")?;
        if message_run_id != expected_run_id {
            return Err(Status::invalid_argument(
                "run stream cannot switch run_id while awaiting tool approval response",
            ));
        }
        if message.input.is_some() {
            return Err(Status::invalid_argument(
                "received prompt payload while waiting for tool approval response",
            ));
        }

        let Some(response) = message.tool_approval_response else {
            continue;
        };
        let response_proposal_id =
            canonical_id(response.proposal_id, "tool_approval_response.proposal_id")?;
        if response_proposal_id != proposal_id {
            return Err(Status::invalid_argument(
                "tool approval response proposal_id does not match pending tool proposal",
            ));
        }
        let response_approval_id = if let Some(response_approval_id) =
            response.approval_id.and_then(|value| non_empty(value.ulid))
        {
            validate_canonical_id(response_approval_id.as_str()).map_err(|_| {
                Status::invalid_argument(
                    "tool_approval_response.approval_id must be a canonical ULID",
                )
            })?;
            if response_approval_id != approval_id {
                return Err(Status::invalid_argument(
                    "tool approval response approval_id does not match pending approval record",
                ));
            }
            response_approval_id
        } else {
            approval_id.to_owned()
        };

        let reason = non_empty(response.reason).unwrap_or_else(|| {
            if response.approved {
                "approved_by_client".to_owned()
            } else {
                "denied_by_client".to_owned()
            }
        });
        return Ok(ToolApprovalOutcome {
            approval_id: response_approval_id,
            approved: response.approved,
            reason,
            decision: if response.approved {
                ApprovalDecision::Allow
            } else {
                ApprovalDecision::Deny
            },
            decision_scope: approval_scope_from_proto(response.decision_scope),
            decision_scope_ttl_ms: if response.decision_scope_ttl_ms > 0 {
                Some(response.decision_scope_ttl_ms)
            } else {
                None
            },
        });
    }

    Ok(ToolApprovalOutcome {
        approval_id: approval_id.to_owned(),
        approved: false,
        reason: APPROVAL_CHANNEL_UNAVAILABLE_REASON.to_owned(),
        decision: ApprovalDecision::Error,
        decision_scope: ApprovalDecisionScope::Once,
        decision_scope_ttl_ms: None,
    })
}

pub(crate) fn map_provider_error(error: ProviderError) -> Status {
    match error {
        ProviderError::CircuitOpen { retry_after_ms } => Status::unavailable(format!(
            "model provider circuit breaker is open; retry after {retry_after_ms}ms"
        )),
        ProviderError::MissingApiKey => {
            Status::failed_precondition("model provider API key is missing")
        }
        ProviderError::MissingAnthropicApiKey => {
            Status::failed_precondition("anthropic model provider API key is missing")
        }
        ProviderError::MissingEmbeddingsModel => {
            Status::failed_precondition("model provider embeddings model is missing")
        }
        ProviderError::VisionUnsupported { provider } => {
            Status::failed_precondition(format!("provider '{provider}' does not support vision"))
        }
        ProviderError::InvalidEmbeddingsRequest { message } => {
            Status::invalid_argument(format!("embeddings request invalid: {message}"))
        }
        ProviderError::RequestFailed {
            message,
            retryable,
            retry_count,
            classification,
        } => {
            let status_message = format!(
                "model provider request failed after {retry_count} retries (retryable={retryable}, class={}, action={}): {message}",
                classification.class.as_str(),
                classification.recommended_action.as_str(),
            );
            if retryable {
                Status::unavailable(status_message)
            } else if classification.class.as_str() == "auth_invalid"
                || classification.class.as_str() == "auth_expired"
            {
                Status::unauthenticated(status_message)
            } else if classification.class.as_str() == "permission_denied" {
                Status::permission_denied(status_message)
            } else if classification.class.as_str() == "quota_exceeded" {
                Status::resource_exhausted(status_message)
            } else if classification.class.as_str() == "context_window_exceeded" {
                Status::invalid_argument(status_message)
            } else if classification.class.as_str() == "content_policy_blocked" {
                Status::failed_precondition(status_message)
            } else {
                Status::internal(status_message)
            }
        }
        ProviderError::InvalidResponse { message, retry_count, classification } => Status::internal(
            format!(
                "model provider response invalid after {retry_count} retries (class={}, action={}): {message}",
                classification.class.as_str(),
                classification.recommended_action.as_str(),
            ),
        ),
        ProviderError::StatePoisoned => Status::internal("model provider state lock poisoned"),
    }
}

pub(crate) fn security_requests_json_mode(security: Option<&common_v1::SecurityContext>) -> bool {
    security
        .map(|value| value.labels.iter().any(|label| label.eq_ignore_ascii_case("json_mode")))
        .unwrap_or(false)
}

pub(crate) fn build_tool_result_memory_text(
    tool_name: &str,
    success: bool,
    output_json: &[u8],
    error: &str,
) -> String {
    let output_preview = truncate_with_ellipsis(
        String::from_utf8_lossy(output_json).replace(['\r', '\n'], " "),
        512,
    );
    let error_preview = truncate_with_ellipsis(error.replace(['\r', '\n'], " "), 256);
    if success {
        format!("tool={tool_name} success=true output={output_preview}")
    } else {
        format!("tool={tool_name} success=false output={} error={error_preview}", output_preview)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ToolRuntimeExecutionContext<'a> {
    pub(crate) principal: &'a str,
    pub(crate) device_id: &'a str,
    pub(crate) channel: Option<&'a str>,
    pub(crate) session_id: &'a str,
    pub(crate) run_id: &'a str,
    pub(crate) execution_backend: ExecutionBackendPreference,
    pub(crate) backend_reason_code: &'a str,
}

#[derive(Clone, Copy)]
pub(crate) struct ToolExecutionTraceContext<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) proposal_id: &'a str,
    pub(crate) tool_name: &'a str,
    pub(crate) execution_surface: &'a str,
}

pub(crate) async fn execute_tool_with_runtime_dispatch(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    if context.execution_backend == ExecutionBackendPreference::NetworkedWorker {
        crate::application::tool_runtime::networked_worker::execute_networked_worker_tool(
            runtime_state,
            context,
            proposal_id,
            tool_name,
            input_json,
        )
        .await
    } else if tool_name == TOOL_PROGRAM_RUN_TOOL_NAME {
        crate::application::tool_runtime::tool_program::execute_tool_program_run_tool(
            runtime_state,
            context,
            proposal_id,
            input_json,
        )
        .await
    } else if tool_name == MEMORY_SEARCH_TOOL_NAME {
        crate::application::tool_runtime::memory::execute_memory_search_tool(
            runtime_state,
            context.principal,
            context.channel,
            context.session_id,
            proposal_id,
            input_json,
        )
        .await
    } else if tool_name == MEMORY_RECALL_TOOL_NAME {
        crate::application::tool_runtime::memory::execute_memory_recall_tool(
            runtime_state,
            context,
            proposal_id,
            input_json,
        )
        .await
    } else if tool_name == MEMORY_RETAIN_TOOL_NAME {
        crate::application::tool_runtime::memory::execute_memory_retain_tool(
            runtime_state,
            context,
            proposal_id,
            input_json,
        )
        .await
    } else if tool_name == MEMORY_REFLECT_TOOL_NAME {
        crate::application::tool_runtime::memory::execute_memory_reflect_tool(
            context,
            proposal_id,
            input_json,
        )
        .await
    } else if matches!(tool_name, ROUTINES_QUERY_TOOL_NAME | ROUTINES_CONTROL_TOOL_NAME) {
        crate::application::tool_runtime::routines::execute_routines_tool(
            runtime_state,
            context,
            tool_name,
            proposal_id,
            input_json,
        )
        .await
    } else if tool_name == ARTIFACT_READ_TOOL_NAME {
        crate::application::tool_runtime::artifacts::execute_artifact_read_tool(
            runtime_state,
            context,
            proposal_id,
            input_json,
        )
        .await
    } else if tool_name == HTTP_FETCH_TOOL_NAME {
        crate::application::tool_runtime::http_fetch::execute_http_fetch_tool(
            runtime_state,
            proposal_id,
            input_json,
        )
        .await
    } else if tool_name.starts_with("palyra.browser.") {
        crate::application::tool_runtime::browser::execute_browser_tool(
            runtime_state,
            context.principal,
            context.channel,
            tool_name,
            proposal_id,
            input_json,
        )
        .await
    } else if tool_name == WORKSPACE_PATCH_TOOL_NAME {
        crate::application::tool_runtime::workspace_patch::execute_workspace_patch_tool(
            runtime_state,
            crate::application::tool_runtime::workspace_patch::WorkspacePatchToolRequest::from_runtime_context(
                context,
                proposal_id,
                input_json,
            ),
        )
        .await
    } else {
        execute_tool_call(&runtime_state.config.tool_call, proposal_id, tool_name, input_json).await
    }
}

pub(crate) fn record_tool_execution_outcome_metrics(
    runtime_state: &Arc<GatewayRuntimeState>,
    trace: ToolExecutionTraceContext<'_>,
    decision_allowed: bool,
    started_at: Instant,
    outcome: &ToolExecutionOutcome,
) {
    let elapsed_ms = started_at.elapsed().as_millis();
    if elapsed_ms > TOOL_EXECUTION_LATENCY_BUDGET_MS {
        warn!(
            run_id = %trace.run_id,
            proposal_id = %trace.proposal_id,
            tool_name = %trace.tool_name,
            execution_surface = trace.execution_surface,
            execution_duration_ms = elapsed_ms,
            budget_ms = TOOL_EXECUTION_LATENCY_BUDGET_MS,
            "tool execution exceeded latency budget"
        );
    }
    if !outcome.success {
        runtime_state.counters.tool_execution_failures.fetch_add(1, Ordering::Relaxed);
    }
    if outcome.attestation.timed_out {
        runtime_state.counters.tool_execution_timeouts.fetch_add(1, Ordering::Relaxed);
    }
    if trace.tool_name == PROCESS_RUNNER_TOOL_NAME {
        record_process_runner_execution_metrics(&runtime_state.counters, decision_allowed, outcome);
    }
    if trace.tool_name == WORKSPACE_PATCH_TOOL_NAME {
        if outcome.success {
            runtime_state.counters.patches_applied.fetch_add(1, Ordering::Relaxed);
        } else {
            runtime_state.counters.patches_rejected.fetch_add(1, Ordering::Relaxed);
        }
        let (files_touched, rollback_performed) =
            workspace_patch_metrics_from_output(outcome.output_json.as_slice());
        if files_touched > 0 {
            runtime_state
                .counters
                .patch_files_touched
                .fetch_add(files_touched as u64, Ordering::Relaxed);
        }
        if rollback_performed {
            runtime_state.counters.patch_rollbacks.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
pub(crate) fn record_tool_decision_metrics(
    runtime_state: &Arc<GatewayRuntimeState>,
    tool_name: &str,
    decision_allowed: bool,
) {
    if decision_allowed {
        runtime_state.counters.tool_decisions_allowed.fetch_add(1, Ordering::Relaxed);
        return;
    }

    runtime_state.counters.tool_decisions_denied.fetch_add(1, Ordering::Relaxed);
    runtime_state.record_denied();
    if tool_name == PROCESS_RUNNER_TOOL_NAME {
        runtime_state.counters.sandbox_policy_denies.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) async fn build_and_ingest_tool_result_memory_summary(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
    decision_allowed: bool,
    outcome: &ToolExecutionOutcome,
    ingest_operation_name: &str,
) -> String {
    let summary = build_tool_result_memory_text(
        tool_name,
        outcome.success,
        outcome.output_json.as_slice(),
        outcome.error.as_str(),
    );
    if decision_allowed || outcome.success {
        ingest_memory_best_effort(
            runtime_state,
            context.principal,
            context.channel,
            Some(context.session_id),
            MemorySource::TapeToolResult,
            summary.as_str(),
            vec![format!("tool:{tool_name}")],
            Some(if outcome.success { 0.85 } else { 0.55 }),
            ingest_operation_name,
        )
        .await;
    }
    summary
}

#[allow(clippy::result_large_err)]
pub(crate) async fn best_effort_mark_approval_error(
    runtime_state: &Arc<GatewayRuntimeState>,
    approval_id: &str,
    reason: String,
) {
    if let Err(error) = runtime_state
        .resolve_approval_record(ApprovalResolveRequest {
            approval_id: approval_id.to_owned(),
            decision: ApprovalDecision::Error,
            decision_scope: ApprovalDecisionScope::Once,
            decision_reason: reason,
            decision_scope_ttl_ms: None,
        })
        .await
    {
        warn!(approval_id, error = %error, "failed to mark approval record as error");
    }
}

pub(crate) struct RunFailureFinalization<'a> {
    pub(crate) sender: &'a mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    pub(crate) runtime_state: &'a Arc<GatewayRuntimeState>,
    pub(crate) request_context: Option<&'a RequestContext>,
    pub(crate) active_session_id: Option<&'a str>,
    pub(crate) run_state: &'a mut RunStateMachine,
    pub(crate) active_run_id: Option<&'a str>,
    pub(crate) tape_seq: &'a mut i64,
    pub(crate) reason: &'a str,
}

pub(crate) async fn finalize_run_failure(input: RunFailureFinalization<'_>) {
    let Some(run_id) = input.active_run_id else {
        return;
    };
    if input.run_state.state().is_terminal() {
        return;
    }
    if input.run_state.transition(RunTransition::Fail).is_err() {
        return;
    }
    let _ = input
        .runtime_state
        .update_orchestrator_run_state(
            run_id.to_owned(),
            RunLifecycleState::Failed,
            Some(input.reason.to_owned()),
        )
        .await;
    let _ = crate::application::run_stream::tape::send_status_with_tape(
        input.sender,
        input.runtime_state,
        run_id,
        input.tape_seq,
        common_v1::stream_status::StatusKind::Failed,
        input.reason,
    )
    .await;
    record_run_failure_journal_event(
        input.runtime_state,
        input.request_context,
        input.active_session_id,
        run_id,
        input.reason,
    )
    .await;
}

async fn record_run_failure_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: Option<&RequestContext>,
    session_id: Option<&str>,
    run_id: &str,
    reason: &str,
) {
    let (Some(context), Some(session_id)) = (request_context, session_id) else {
        return;
    };
    let diagnostic = run_failure_diagnostic(reason);
    let mut payload = json!({
        "event": "run.failed",
        "success": false,
        "message": truncate_with_ellipsis(reason.to_owned(), 512),
        "error": truncate_with_ellipsis(reason.to_owned(), 512),
        "diagnostic_hint": diagnostic.diagnostic_hint,
    });
    if let Some(payload_map) = payload.as_object_mut() {
        if let Some(error_class) = diagnostic.error_class {
            payload_map.insert("error_class".to_owned(), Value::String(error_class));
        }
        if let Some(recommended_action) = diagnostic.recommended_action {
            payload_map.insert("recommended_action".to_owned(), Value::String(recommended_action));
        }
    }

    if let Err(error) = runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::RunFailed as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: payload.to_string().into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
    {
        warn!(run_id, error = %error, "failed to record run failure journal event");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunFailureDiagnostic {
    error_class: Option<String>,
    recommended_action: Option<String>,
    diagnostic_hint: &'static str,
}

fn run_failure_diagnostic(reason: &str) -> RunFailureDiagnostic {
    let error_class = extract_failure_marker(reason, "class=");
    let recommended_action = extract_failure_marker(reason, "action=");
    let diagnostic_hint = match error_class.as_deref() {
        Some("provider_timeout") => {
            "Provider request timed out; retry or increase model_provider.request_timeout_ms for slow model responses."
        }
        Some("network_unavailable") => {
            "Check DNS, proxy, firewall, and provider reachability. If direct provider calls work, inspect model_provider timeout and base URL settings."
        }
        Some("auth_invalid") | Some("auth_expired") => {
            "Refresh or rotate the model provider credential, then rerun the agent."
        }
        Some("permission_denied") => {
            "Verify the model provider account, API key scopes, and selected model access."
        }
        Some("rate_limited") => {
            "Retry after the provider rate limit clears or switch to a lower-pressure model/provider."
        }
        Some("context_window_exceeded") => {
            "Reduce prompt or attachment size, then rerun the agent."
        }
        Some("content_policy_blocked") => {
            "Revise the prompt or inputs; the provider rejected the request by policy."
        }
        _ => "Inspect the run tape and provider status for the failed run before retrying.",
    };
    RunFailureDiagnostic { error_class, recommended_action, diagnostic_hint }
}

fn extract_failure_marker(reason: &str, marker: &str) -> Option<String> {
    let value = reason.split(marker).nth(1)?;
    let value = value
        .split(|character: char| {
            character == ',' || character == ')' || character == ':' || character.is_whitespace()
        })
        .next()
        .unwrap_or_default()
        .trim();
    (!value.is_empty()).then(|| value.to_owned())
}

#[cfg(test)]
mod run_failure_diagnostic_tests {
    use super::run_failure_diagnostic;

    #[test]
    fn extracts_provider_failure_class_and_action() {
        let diagnostic = run_failure_diagnostic(
            "model provider request failed after 2 retries (retryable=true, class=provider_timeout, action=retry): anthropic request failed",
        );

        assert_eq!(diagnostic.error_class.as_deref(), Some("provider_timeout"));
        assert_eq!(diagnostic.recommended_action.as_deref(), Some("retry"));
        assert!(diagnostic.diagnostic_hint.contains("timed out"));
    }
}

#[allow(clippy::result_large_err)]
#[cfg(test)]
pub(crate) async fn compact_model_token_tape_stub(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &crate::transport::grpc::auth::RequestContext,
    session_id: &str,
    run_id: &str,
    tape_seq: &mut i64,
) -> Result<(), Status> {
    crate::application::run_stream::tape::compact_model_token_tape(
        runtime_state,
        request_context,
        session_id,
        run_id,
        tape_seq,
    )
    .await
}

#[cfg(test)]
pub(crate) fn model_token_compaction_tape_payload(max_model_token_events: usize) -> String {
    json!({
        "kind": "token_cap_reached",
        "max_model_token_tape_events": max_model_token_events,
        "compaction_hook": "stub",
    })
    .to_string()
}

#[cfg(test)]
pub(crate) fn default_approval_prompt_options() -> Vec<ApprovalPromptOption> {
    vec![
        ApprovalPromptOption {
            option_id: "allow_once".to_owned(),
            label: "Allow once".to_owned(),
            description: "Approve this single action".to_owned(),
            default_selected: true,
            decision_scope: ApprovalDecisionScope::Once,
            timebox_ttl_ms: None,
        },
        ApprovalPromptOption {
            option_id: "allow_session".to_owned(),
            label: "Allow for session".to_owned(),
            description: "Remember approval for this session".to_owned(),
            default_selected: false,
            decision_scope: ApprovalDecisionScope::Session,
            timebox_ttl_ms: None,
        },
        ApprovalPromptOption {
            option_id: "deny_once".to_owned(),
            label: "Deny".to_owned(),
            description: "Reject this action".to_owned(),
            default_selected: false,
            decision_scope: ApprovalDecisionScope::Once,
            timebox_ttl_ms: None,
        },
    ]
}

pub(crate) fn truncate_with_ellipsis(input: String, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input;
    }
    let cutoff = max_bytes.saturating_sub(3);
    let mut output = String::new();
    for character in input.chars() {
        if output.len().saturating_add(character.len_utf8()) > cutoff {
            break;
        }
        output.push(character);
    }
    output.push_str("...");
    output
}

#[cfg(test)]
pub(crate) fn build_tool_request_summary(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_json: &[u8],
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    truncate_with_ellipsis(
        json!({
            "tool_name": tool_name,
            "skill_id": skill_context.map(|context| context.skill_id.as_str()),
            "skill_version": skill_context.and_then(|context| context.version.as_deref()),
            "input_json": normalized_input,
        })
        .to_string(),
        APPROVAL_REQUEST_SUMMARY_MAX_BYTES,
    )
}

#[cfg(test)]
pub(crate) fn build_tool_policy_snapshot(
    config: &ToolCallConfig,
    tool_name: &str,
) -> ApprovalPolicySnapshot {
    let snapshot = tool_policy_snapshot(config);
    let policy_snapshot_json = serde_json::to_vec(&snapshot).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(policy_snapshot_json.as_slice());
    let policy_hash = hex::encode(hasher.finalize());
    ApprovalPolicySnapshot {
        policy_id: APPROVAL_POLICY_ID.to_owned(),
        policy_hash,
        evaluation_summary: format!(
            "action=tool.execute resource=tool:{tool_name} approval_required=true deny_by_default=true"
        ),
    }
}

pub(crate) fn request_context_with_resolved_route_channel(
    request_context: &RequestContext,
    route_channel: &str,
) -> RequestContext {
    RequestContext {
        principal: request_context.principal.clone(),
        device_id: request_context.device_id.clone(),
        channel: Some(route_channel.to_owned()),
    }
}

#[cfg(test)]
pub(crate) fn build_pending_tool_approval(
    tool_name: &str,
    skill_context: Option<&ToolSkillContext>,
    input_json: &[u8],
    config: &ToolCallConfig,
) -> PendingToolApproval {
    let subject_id = build_tool_approval_subject_id(tool_name, skill_context);
    let request_summary = build_tool_request_summary(tool_name, skill_context, input_json);
    let policy_snapshot = build_tool_policy_snapshot(config, tool_name);
    let details = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    let prompt = ApprovalPromptRecord {
        title: format!("Approve {}", tool_name),
        risk_level: approval_risk_for_tool(tool_name, input_json, config),
        subject_id: subject_id.clone(),
        summary: format!("Tool `{tool_name}` requested explicit approval"),
        options: default_approval_prompt_options(),
        timeout_seconds: APPROVAL_PROMPT_TIMEOUT_SECONDS,
        details_json: json!({
            "tool_name": tool_name,
            "subject_id": subject_id,
            "skill_id": skill_context.map(|context| context.skill_id.as_str()),
            "skill_version": skill_context.and_then(|context| context.version.as_deref()),
            "input_json": details,
        })
        .to_string(),
        policy_explanation: "Sensitive tool actions are deny-by-default until explicitly approved"
            .to_owned(),
    };
    PendingToolApproval {
        approval_id: Ulid::new().to_string(),
        request_summary,
        policy_snapshot,
        prompt,
    }
}

#[cfg(test)]
pub(crate) fn approval_risk_for_tool(
    tool_name: &str,
    input_json: &[u8],
    config: &ToolCallConfig,
) -> ApprovalRiskLevel {
    if tool_name != PROCESS_RUNNER_TOOL_NAME {
        return ApprovalRiskLevel::High;
    }
    if !matches!(config.process_runner.tier, crate::sandbox_runner::SandboxProcessRunnerTier::C) {
        return ApprovalRiskLevel::High;
    }
    if process_runner_command_is_read_only(input_json) {
        ApprovalRiskLevel::Medium
    } else {
        ApprovalRiskLevel::High
    }
}

#[cfg(test)]
pub(crate) fn process_runner_command_is_read_only(input_json: &[u8]) -> bool {
    const READ_ONLY_COMMANDS: &[&str] = &[
        "cat", "find", "grep", "head", "id", "ls", "pwd", "rg", "stat", "tail", "uname", "wc",
        "whoami",
    ];

    let parsed = match serde_json::from_slice::<Value>(input_json) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let Some(payload) = parsed.as_object() else {
        return false;
    };
    let Some(command) = payload.get("command").and_then(Value::as_str).map(str::trim) else {
        return false;
    };
    READ_ONLY_COMMANDS.iter().any(|candidate| candidate.eq_ignore_ascii_case(command))
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn record_approval_requested_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    subject_id: &str,
    request_summary: &str,
    policy_snapshot: &ApprovalPolicySnapshot,
    prompt: &ApprovalPromptRecord,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolProposed as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: approval_requested_journal_payload(
                proposal_id,
                approval_id,
                tool_name,
                subject_id,
                request_summary,
                policy_snapshot,
                prompt,
            ),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[cfg(test)]
pub(crate) fn approval_requested_journal_payload(
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    subject_id: &str,
    request_summary: &str,
    policy_snapshot: &ApprovalPolicySnapshot,
    prompt: &ApprovalPromptRecord,
) -> Vec<u8> {
    let prompt_details_json = serde_json::from_str::<Value>(prompt.details_json.as_str())
        .unwrap_or_else(|_| json!({ "raw": prompt.details_json }));
    json!({
        "event": "approval.requested",
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "subject_type": "tool",
        "subject_id": subject_id,
        "tool_name": tool_name,
        "request_summary": request_summary,
        "policy_snapshot": policy_snapshot,
        "prompt": {
            "title": prompt.title,
            "risk_level": prompt.risk_level.as_str(),
            "subject_id": prompt.subject_id,
            "summary": prompt.summary,
            "timeout_seconds": prompt.timeout_seconds,
            "policy_explanation": prompt.policy_explanation,
            "options": prompt.options.iter().map(|option| json!({
                "option_id": option.option_id,
                "label": option.label,
                "description": option.description,
                "default_selected": option.default_selected,
                "decision_scope": option.decision_scope.as_str(),
                "timebox_ttl_ms": option.timebox_ttl_ms,
            })).collect::<Vec<_>>(),
            "details_json": prompt_details_json,
        },
    })
    .to_string()
    .into_bytes()
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn record_approval_resolved_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    approval_id: &str,
    decision: ApprovalDecision,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
    reason: &str,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: approval_resolved_journal_payload(
                proposal_id,
                approval_id,
                decision,
                decision_scope,
                decision_scope_ttl_ms,
                reason,
            ),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[cfg(test)]
pub(crate) fn approval_resolved_journal_payload(
    proposal_id: &str,
    approval_id: &str,
    decision: ApprovalDecision,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
    reason: &str,
) -> Vec<u8> {
    json!({
        "event": "approval.resolved",
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "decision": decision.as_str(),
        "decision_scope": decision_scope.as_str(),
        "decision_scope_ttl_ms": decision_scope_ttl_ms,
        "reason": reason,
    })
    .to_string()
    .into_bytes()
}

#[allow(clippy::result_large_err)]
pub(crate) async fn record_message_router_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    event_name: &str,
    actor: i32,
    payload: Value,
) -> Result<(), Status> {
    let mut payload = payload;
    if let Some(map) = payload.as_object_mut() {
        map.entry("event".to_owned()).or_insert(Value::String(event_name.to_owned()));
    }
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::MessageReceived as i32,
            actor,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: payload.to_string().into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
pub(crate) async fn record_vault_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    event: &str,
    action: &str,
    scope: &VaultScope,
    key: Option<&str>,
    value_size: Option<usize>,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": event,
                "action": action,
                "scope": scope.to_string(),
                "key": key.unwrap_or_default(),
                "value_bytes": value_size,
                "vault_backend": runtime_state.vault.backend_kind().as_str(),
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await?;
    runtime_state.counters.vault_access_audit_events.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

#[allow(clippy::result_large_err)]
pub(crate) async fn record_agent_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    payload: Value,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: payload.to_string().into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
#[cfg(test)]
pub(crate) async fn record_auth_profile_saved_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    profile: &AuthProfileRecord,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": "auth.profile.saved",
                "profile_id": profile.profile_id,
                "provider": profile.provider.label(),
                "scope": profile.scope.scope_key(),
                "credential_type": match profile.credential.credential_type() {
                    AuthCredentialType::ApiKey => "api_key",
                    AuthCredentialType::Oauth => "oauth",
                },
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
#[cfg(test)]
pub(crate) async fn record_auth_profile_deleted_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    profile_id: &str,
    profile: Option<&AuthProfileRecord>,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": "auth.profile.deleted",
                "profile_id": profile_id,
                "provider": profile.map(|value| value.provider.label()),
                "scope": profile.map(|value| value.scope.scope_key()),
                "credential_type": profile.map(|value| match value.credential.credential_type() {
                    AuthCredentialType::ApiKey => "api_key",
                    AuthCredentialType::Oauth => "oauth",
                }),
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
#[cfg(test)]
pub(crate) async fn record_auth_refresh_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    outcome: &OAuthRefreshOutcome,
) -> Result<(), Status> {
    if !outcome.kind.attempted() {
        return Ok(());
    }
    let event_name =
        if outcome.kind.success() { "auth.token.refreshed" } else { "auth.refresh.failed" };
    let redacted_reason = crate::model_provider::sanitize_remote_error(outcome.reason.as_str());
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": event_name,
                "profile_id": outcome.profile_id,
                "provider": outcome.provider,
                "reason": redacted_reason,
                "next_allowed_refresh_unix_ms": outcome.next_allowed_refresh_unix_ms,
                "expires_at_unix_ms": outcome.expires_at_unix_ms,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

pub(crate) fn workspace_patch_metrics_from_output(output_json: &[u8]) -> (usize, bool) {
    let parsed = serde_json::from_slice::<Value>(output_json).ok();
    let Some(Value::Object(payload)) = parsed else {
        return (0, false);
    };
    let files_touched =
        payload.get("files_touched").and_then(Value::as_array).map_or(0, std::vec::Vec::len);
    let rollback_performed =
        payload.get("rollback_performed").and_then(Value::as_bool).unwrap_or(false);
    (files_touched, rollback_performed)
}

pub(crate) fn record_process_runner_execution_metrics(
    counters: &RuntimeCounters,
    decision_allowed: bool,
    outcome: &ToolExecutionOutcome,
) {
    if !decision_allowed {
        return;
    }

    counters.sandbox_launches.fetch_add(1, Ordering::Relaxed);
    match outcome.attestation.executor.as_str() {
        "sandbox_tier_b" => {
            counters.sandbox_backend_selected_tier_b.fetch_add(1, Ordering::Relaxed);
        }
        "sandbox_tier_c_linux_bubblewrap" => {
            counters
                .sandbox_backend_selected_tier_c_linux_bubblewrap
                .fetch_add(1, Ordering::Relaxed);
        }
        "sandbox_tier_c_macos_sandbox_exec" => {
            counters
                .sandbox_backend_selected_tier_c_macos_sandbox_exec
                .fetch_add(1, Ordering::Relaxed);
        }
        "sandbox_tier_c_windows_job_object" => {
            counters
                .sandbox_backend_selected_tier_c_windows_job_object
                .fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }

    if !outcome.success {
        if outcome.error.contains("sandbox denied") {
            counters.sandbox_policy_denies.fetch_add(1, Ordering::Relaxed);
        }
        match classify_sandbox_escape_attempt(outcome.error.as_str()) {
            Some(SandboxEscapeAttemptType::Workspace) => {
                counters.sandbox_escape_attempts_blocked_workspace.fetch_add(1, Ordering::Relaxed);
            }
            Some(SandboxEscapeAttemptType::Egress) => {
                counters.sandbox_escape_attempts_blocked_egress.fetch_add(1, Ordering::Relaxed);
            }
            Some(SandboxEscapeAttemptType::Executable) => {
                counters.sandbox_escape_attempts_blocked_executable.fetch_add(1, Ordering::Relaxed);
            }
            None => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SandboxEscapeAttemptType {
    Workspace,
    Egress,
    Executable,
}

pub(crate) fn classify_sandbox_escape_attempt(error: &str) -> Option<SandboxEscapeAttemptType> {
    let normalized = error.to_ascii_lowercase();
    if normalized.contains("path traversal")
        || normalized.contains("workspace scope")
        || normalized.contains("escapes workspace")
        || normalized.contains("absolute path")
    {
        return Some(SandboxEscapeAttemptType::Workspace);
    }
    if normalized.contains("egress")
        || normalized.contains("host-level egress")
        || normalized.contains("network isolation")
    {
        return Some(SandboxEscapeAttemptType::Egress);
    }
    if normalized.contains("not allowlisted")
        || normalized.contains("allow_interpreters")
        || normalized.contains("bare executable")
        || normalized.contains("shell-eval")
    {
        return Some(SandboxEscapeAttemptType::Executable);
    }
    None
}

#[cfg(test)]
pub(crate) fn build_test_vault() -> Arc<Vault> {
    let nonce = Ulid::new();
    let root = std::env::temp_dir().join(format!("palyra-gateway-test-vault-{nonce}"));
    let identity_root =
        std::env::temp_dir().join(format!("palyra-gateway-test-vault-identity-{nonce}"));
    Arc::new(
        Vault::open_with_config(VaultConfigOptions {
            root: Some(root),
            identity_store_root: Some(identity_root),
            backend_preference: VaultBackendPreference::EncryptedFile,
            max_secret_bytes: MAX_VAULT_SECRET_BYTES,
        })
        .expect("test vault should initialize"),
    )
}

#[cfg(test)]
mod tests;
