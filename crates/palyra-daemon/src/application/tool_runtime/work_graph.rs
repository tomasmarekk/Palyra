//! Model-visible WorkGraph requests over host-owned durable transitions.

use std::{collections::BTreeSet, sync::Arc};

use palyra_common::redaction::{redact_auth_error, redact_url_segments_in_text};
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use crate::{
    domain::work_graph::{
        ClaimReadyWorkItemOutcome, ClaimReadyWorkItemRequest, WorkClaimAuthority,
        WorkClaimSettlementOutcome, WorkClaimSettlementRequest, WorkClaimToken,
        WorkGraphCommentCreateRequest, WorkGraphCreateRequest, WorkGraphHostDecisionV1,
        WorkGraphOwnerScopeV1, WorkGraphReviewRequest, WorkGraphToolOperation,
        WorkGraphToolRequest, WorkItemHandoffCreateRequest, WorkItemHeartbeatOutcome,
        WorkItemHeartbeatRequest, WorkItemSideEffectFenceOutcome, WorkItemSideEffectFenceRequest,
        WorkItemState, WorkItemTransitionRequest, WorkVerificationState, MAX_WORK_CLAIM_TTL_MS,
        MIN_WORK_CLAIM_TTL_MS, WORK_GRAPH_SCHEMA_VERSION,
    },
    gateway::{
        GatewayRuntimeState, ToolRuntimeExecutionContext, WORK_GRAPH_ARTIFACT_TOOL_NAME,
        WORK_GRAPH_CONTROL_TOOL_NAME, WORK_GRAPH_QUERY_TOOL_NAME,
    },
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const WORK_GRAPH_TOOL_EXECUTOR: &str = "work_graph_runtime";
const WORK_GRAPH_TOOL_SANDBOX: &str = "work_graph_host_authority";
const DEFAULT_CLAIM_TTL_MS: u64 = 30_000;
const DEFAULT_HEARTBEAT_EXTENSION_MS: u64 = 5_000;

/// Executes a model request while deriving identity and worker authority from the host context.
pub(crate) async fn execute_work_graph_tool(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let result = execute_work_graph_tool_inner(runtime, context, tool_name, input_json).await;
    match result {
        Ok(output) => {
            build_outcome(proposal_id, tool_name, input_json, true, output, String::new())
        }
        Err(error) => {
            let message = safe_text(error.message());
            build_outcome(
                proposal_id,
                tool_name,
                input_json,
                false,
                json!({
                    "schema_version": WORK_GRAPH_SCHEMA_VERSION,
                    "accepted": false,
                    "error": message,
                }),
                message,
            )
        }
    }
}

async fn execute_work_graph_tool_inner(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    tool_name: &str,
    input_json: &[u8],
) -> Result<Value, Status> {
    if !matches!(
        tool_name,
        WORK_GRAPH_QUERY_TOOL_NAME | WORK_GRAPH_CONTROL_TOOL_NAME | WORK_GRAPH_ARTIFACT_TOOL_NAME
    ) {
        return Err(Status::invalid_argument("unsupported WorkGraph tool name"));
    }
    let request = serde_json::from_slice::<WorkGraphToolRequest>(input_json).map_err(|error| {
        Status::invalid_argument(format!("WorkGraph tool input is invalid JSON: {error}"))
    })?;
    match (tool_name, request.operation) {
        (WORK_GRAPH_QUERY_TOOL_NAME, WorkGraphToolOperation::List) => {
            list_graphs(runtime, context, &request).await
        }
        (WORK_GRAPH_QUERY_TOOL_NAME, WorkGraphToolOperation::Diagnostics) => {
            graph_diagnostics(runtime, context, &request).await
        }
        (WORK_GRAPH_ARTIFACT_TOOL_NAME, WorkGraphToolOperation::Retrieve) => {
            retrieve_handoff(runtime, context, &request).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Create) => {
            create_graph(runtime, context, request).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Claim) => {
            claim_item(runtime, context, &request).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Complete) => {
            complete_item(runtime, context, &request).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Block) => {
            transition_claimed_item(runtime, context, &request, WorkItemState::Waiting).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Unblock) => {
            transition_claimed_item(runtime, context, &request, WorkItemState::Running).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Heartbeat) => {
            heartbeat_item(runtime, context, &request).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::SideEffect) => {
            update_side_effect(runtime, context, &request).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Reclaim) => {
            reconcile_stale_item(runtime, context, &request).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Cancel) => {
            cancel_graph(runtime, context, &request).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Comment) => {
            comment_item(runtime, context, &request).await
        }
        (WORK_GRAPH_CONTROL_TOOL_NAME, WorkGraphToolOperation::Review) => {
            review_item(runtime, context, &request).await
        }
        _ => Err(Status::invalid_argument(
            "WorkGraph operation is not available on this tool surface",
        )),
    }
}

async fn list_graphs(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let graphs = runtime
        .list_work_graphs_for_owner(
            context.principal.to_owned(),
            Some(context.session_id.to_owned()),
            request.limit.unwrap_or(32),
        )
        .await?;
    Ok(json!({
        "schema_version": WORK_GRAPH_SCHEMA_VERSION,
        "operation": "list",
        "graphs": graphs,
    }))
}

async fn graph_diagnostics(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let graph_id = required(request.graph_id.as_deref(), "graph_id")?;
    let snapshot = owned_snapshot(runtime, context, graph_id.as_str()).await?;
    let diagnostics = runtime.work_graph_claim_diagnostics(graph_id.clone()).await?;
    let events = runtime.work_graph_events(graph_id.clone(), request.limit.unwrap_or(32)).await?;
    let comments = runtime
        .work_graph_comments(
            context.principal.to_owned(),
            graph_id.clone(),
            request.work_item_id.clone(),
            request.limit.unwrap_or(32),
        )
        .await?;
    let resources = runtime.work_graph_resource_snapshot()?;
    Ok(json!({
        "schema_version": WORK_GRAPH_SCHEMA_VERSION,
        "operation": "diagnostics",
        "graph_id": graph_id,
        "state": snapshot.graph.state,
        "revision": snapshot.graph.revision,
        "reason_code": snapshot.graph.reason_code,
        "claim_diagnostics": diagnostics,
        "resource_governor": resources,
        "events": events,
        "comments": comments,
    }))
}

async fn retrieve_handoff(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let graph_id = required(request.graph_id.as_deref(), "graph_id")?;
    let handoff_id = required(request.handoff_id.as_deref(), "handoff_id")?;
    let handoff = runtime
        .work_item_handoff(context.principal.to_owned(), graph_id.clone(), handoff_id.clone())
        .await?
        .ok_or_else(|| Status::not_found("WorkGraph handoff is unavailable in this owner scope"))?;
    Ok(json!({
        "schema_version": WORK_GRAPH_SCHEMA_VERSION,
        "operation": "retrieve",
        "handoff": handoff,
        "source_refs": {
            "evidence": handoff.evidence_refs,
            "artifacts": handoff.artifact_refs,
        },
    }))
}

async fn create_graph(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: WorkGraphToolRequest,
) -> Result<Value, Status> {
    if request.objective_id.is_some()
        || request.routine_id.is_some()
        || request.flow_id.is_some()
        || request.flow_step_id.is_some()
    {
        return Err(Status::permission_denied(
            "objective, routine, and flow bindings are host-coordinator authority",
        ));
    }
    let graph_id = request
        .graph_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Ulid::new().to_string());
    let snapshot = runtime
        .create_work_graph(WorkGraphCreateRequest {
            graph_id: graph_id.clone(),
            owner: WorkGraphOwnerScopeV1 {
                principal: context.principal.to_owned(),
                device_id: context.device_id.to_owned(),
                channel: context.channel.map(ToOwned::to_owned),
                session_id: Some(context.session_id.to_owned()),
                origin_run_id: Some(context.run_id.to_owned()),
            },
            objective_id: None,
            routine_id: None,
            flow_id: None,
            flow_step_id: None,
            budget: request.budget.unwrap_or_default(),
            concurrency_policy: request.concurrency_policy.unwrap_or_default(),
            items: request.items,
            actor_principal: context.principal.to_owned(),
        })
        .await?;
    Ok(json!({
        "decision": WorkGraphHostDecisionV1 {
            graph_id: Some(graph_id),
            state: Some(snapshot.graph.state.as_str().to_owned()),
            revision: Some(snapshot.graph.revision),
            ..WorkGraphHostDecisionV1::accepted("work_graph.created")
        },
        "item_count": snapshot.items.len(),
    }))
}

async fn claim_item(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let graph_id = required(request.graph_id.as_deref(), "graph_id")?;
    let snapshot = owned_snapshot(runtime, context, graph_id.as_str()).await?;
    let mut capability_profiles = request
        .capability_profiles
        .iter()
        .map(|profile| profile.trim().to_owned())
        .filter(|profile| !profile.is_empty())
        .collect::<BTreeSet<_>>();
    if capability_profiles.is_empty() {
        capability_profiles
            .extend(snapshot.items.iter().map(|item| item.capability_profile.clone()));
    }
    let lease_ttl_ms = request
        .lease_ttl_ms
        .unwrap_or(DEFAULT_CLAIM_TTL_MS)
        .clamp(MIN_WORK_CLAIM_TTL_MS, MAX_WORK_CLAIM_TTL_MS);
    let outcome = runtime
        .claim_ready_work_item(ClaimReadyWorkItemRequest {
            graph_id: graph_id.clone(),
            work_item_id: request.work_item_id.clone(),
            expected_item_revision: request.expected_revision,
            worker_id: context.run_id.to_owned(),
            worker_principal: context.principal.to_owned(),
            authorized_owner_principal: context.principal.to_owned(),
            capability_profiles,
            provider_backpressure_profiles: BTreeSet::new(),
            memory_pressure: false,
            resource_lease_id: None,
            runtime_instance_id: format!("model-tool:{}", context.run_id),
            process_start_token: context.run_id.to_owned(),
            lease_ttl_ms,
        })
        .await?;
    match outcome {
        ClaimReadyWorkItemOutcome::Granted(grant) => Ok(json!({
            "decision": WorkGraphHostDecisionV1 {
                graph_id: Some(graph_id),
                work_item_id: Some(grant.item.work_item_id.clone()),
                state: Some(grant.item.state.as_str().to_owned()),
                revision: Some(grant.item.revision),
                ..WorkGraphHostDecisionV1::accepted("work_graph.claim.granted")
            },
            "claim": {
                "work_item_id": grant.item.work_item_id,
                "generation": grant.claim.generation,
                "claim_capability": grant.token.expose_hex(),
                "expires_at_unix_ms": grant.claim.expires_at_unix_ms,
            },
        })),
        ClaimReadyWorkItemOutcome::NoEligibleItem { reason_code } => Ok(json!({
            "decision": WorkGraphHostDecisionV1 {
                accepted: false,
                graph_id: Some(graph_id),
                ..WorkGraphHostDecisionV1::accepted(reason_code)
            },
        })),
    }
}

async fn complete_item(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let authority = claim_authority(context, request)?;
    let snapshot = owned_snapshot(runtime, context, authority.graph_id.as_str()).await?;
    let item = snapshot
        .items
        .iter()
        .find(|item| item.work_item_id == authority.work_item_id)
        .ok_or_else(|| Status::not_found("WorkGraph item was not found"))?;
    let verification_state = if item.requires_review {
        WorkVerificationState::Pending
    } else {
        WorkVerificationState::Waived
    };
    let commit = runtime
        .record_work_item_handoff(WorkItemHandoffCreateRequest {
            authority: authority.clone(),
            expected_item_revision: request
                .expected_revision
                .ok_or_else(|| Status::invalid_argument("expected_revision is required"))?,
            actor_principal: context.principal.to_owned(),
            summary: required(request.summary.as_deref(), "summary")?,
            structured_result: request.structured_result.clone().unwrap_or_else(|| json!({})),
            evidence_refs: request.evidence_refs.clone(),
            artifact_refs: request.artifact_refs.clone(),
            verification_state,
        })
        .await?;
    let mut settlement_revision = commit.item_revision;
    if item.state != WorkItemState::Running {
        let started = runtime
            .transition_work_graph_item(WorkItemTransitionRequest {
                graph_id: authority.graph_id.clone(),
                work_item_id: authority.work_item_id.clone(),
                expected_revision: settlement_revision,
                target_state: WorkItemState::Running,
                verification_state: Some(verification_state),
                reason_code: "work_graph.complete.host_started".to_owned(),
                actor_principal: context.principal.to_owned(),
            })
            .await?;
        settlement_revision = started.item.revision;
    }
    let target_state =
        if item.requires_review { WorkItemState::Review } else { WorkItemState::Succeeded };
    let settlement = runtime
        .settle_work_item_claim(WorkClaimSettlementRequest {
            authority,
            expected_item_revision: settlement_revision,
            target_state,
            verification_state,
            result_sha256: commit.handoff.provenance_sha256.clone(),
            actor_principal: context.principal.to_owned(),
            reason_code: if item.requires_review {
                "work_graph.complete.review_requested".to_owned()
            } else {
                "work_graph.complete.host_verified".to_owned()
            },
        })
        .await?;
    match settlement {
        WorkClaimSettlementOutcome::Applied { item, graph_revision } => Ok(json!({
            "decision": WorkGraphHostDecisionV1 {
                graph_id: Some(item.graph_id.clone()),
                work_item_id: Some(item.work_item_id.clone()),
                handoff_id: Some(commit.handoff.handoff_id.clone()),
                state: Some(item.state.as_str().to_owned()),
                revision: Some(item.revision),
                ..WorkGraphHostDecisionV1::accepted(item.reason_code.clone())
            },
            "graph_revision": graph_revision,
            "handoff": {
                "handoff_id": commit.handoff.handoff_id,
                "summary": commit.handoff.summary,
                "context_cost_tokens": commit.handoff.context_cost_tokens,
                "evidence_refs": commit.handoff.evidence_refs,
                "artifact_refs": commit.handoff.artifact_refs,
                "verification_state": commit.handoff.verification_state,
                "provenance_sha256": commit.handoff.provenance_sha256,
            },
        })),
        WorkClaimSettlementOutcome::Orphaned { reason_code } => Ok(json!({
            "decision": WorkGraphHostDecisionV1 {
                accepted: false,
                graph_id: Some(commit.handoff.graph_id),
                work_item_id: Some(commit.handoff.work_item_id),
                handoff_id: Some(commit.handoff.handoff_id),
                ..WorkGraphHostDecisionV1::accepted(reason_code)
            },
        })),
    }
}

async fn transition_claimed_item(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
    target_state: WorkItemState,
) -> Result<Value, Status> {
    let authority = claim_authority(context, request)?;
    owned_snapshot(runtime, context, authority.graph_id.as_str()).await?;
    let renewed = runtime
        .heartbeat_work_item(WorkItemHeartbeatRequest {
            authority: authority.clone(),
            extend_by_ms: DEFAULT_HEARTBEAT_EXTENSION_MS,
        })
        .await?;
    let claim = match renewed {
        WorkItemHeartbeatOutcome::Renewed(claim) => claim,
        WorkItemHeartbeatOutcome::StaleAuthority { reason_code }
        | WorkItemHeartbeatOutcome::Expired { reason_code } => {
            return Err(Status::failed_precondition(reason_code));
        }
    };
    let outcome = runtime
        .transition_work_graph_item(WorkItemTransitionRequest {
            graph_id: authority.graph_id.clone(),
            work_item_id: authority.work_item_id.clone(),
            expected_revision: claim.record_revision,
            target_state,
            verification_state: None,
            reason_code: match target_state {
                WorkItemState::Waiting => "work_graph.worker.block_requested",
                WorkItemState::Running => "work_graph.worker.unblock_requested",
                _ => "work_graph.worker.transition_requested",
            }
            .to_owned(),
            actor_principal: context.principal.to_owned(),
        })
        .await?;
    Ok(json!({
        "decision": WorkGraphHostDecisionV1 {
            graph_id: Some(authority.graph_id),
            work_item_id: Some(authority.work_item_id),
            state: Some(outcome.item.state.as_str().to_owned()),
            revision: Some(outcome.item.revision),
            ..WorkGraphHostDecisionV1::accepted(outcome.item.reason_code)
        },
        "graph_revision": outcome.graph_revision,
    }))
}

async fn heartbeat_item(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let authority = claim_authority(context, request)?;
    owned_snapshot(runtime, context, authority.graph_id.as_str()).await?;
    let outcome = runtime
        .heartbeat_work_item(WorkItemHeartbeatRequest {
            authority: authority.clone(),
            extend_by_ms: request
                .extend_by_ms
                .unwrap_or(DEFAULT_HEARTBEAT_EXTENSION_MS)
                .clamp(MIN_WORK_CLAIM_TTL_MS, MAX_WORK_CLAIM_TTL_MS),
        })
        .await?;
    match outcome {
        WorkItemHeartbeatOutcome::Renewed(claim) => Ok(json!({
            "decision": WorkGraphHostDecisionV1 {
                graph_id: Some(authority.graph_id),
                work_item_id: Some(authority.work_item_id),
                state: Some("claimed".to_owned()),
                revision: Some(claim.record_revision),
                ..WorkGraphHostDecisionV1::accepted("work_graph.heartbeat.renewed")
            },
            "expires_at_unix_ms": claim.expires_at_unix_ms,
        })),
        WorkItemHeartbeatOutcome::StaleAuthority { reason_code }
        | WorkItemHeartbeatOutcome::Expired { reason_code } => Ok(json!({
            "decision": WorkGraphHostDecisionV1 {
                accepted: false,
                graph_id: Some(authority.graph_id),
                work_item_id: Some(authority.work_item_id),
                ..WorkGraphHostDecisionV1::accepted(reason_code)
            },
        })),
    }
}

async fn update_side_effect(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let authority = claim_authority(context, request)?;
    owned_snapshot(runtime, context, authority.graph_id.as_str()).await?;
    let outcome = runtime
        .record_work_item_side_effect_fence(WorkItemSideEffectFenceRequest {
            authority: authority.clone(),
            expected_item_revision: request
                .expected_revision
                .ok_or_else(|| Status::invalid_argument("expected_revision is required"))?,
            state: request
                .side_effect_state
                .ok_or_else(|| Status::invalid_argument("side_effect_state is required"))?,
            actor_principal: context.principal.to_owned(),
        })
        .await?;
    match outcome {
        WorkItemSideEffectFenceOutcome::Updated(claim) => Ok(json!({
            "decision": WorkGraphHostDecisionV1 {
                graph_id: Some(authority.graph_id),
                work_item_id: Some(authority.work_item_id),
                state: Some(claim.side_effect_fence.as_str().to_owned()),
                revision: Some(claim.record_revision),
                ..WorkGraphHostDecisionV1::accepted("work_graph.side_effect_fence.updated")
            },
        })),
        WorkItemSideEffectFenceOutcome::StaleAuthority { reason_code } => Ok(json!({
            "decision": WorkGraphHostDecisionV1 {
                accepted: false,
                graph_id: Some(authority.graph_id),
                work_item_id: Some(authority.work_item_id),
                ..WorkGraphHostDecisionV1::accepted(reason_code)
            },
        })),
    }
}

async fn reconcile_stale_item(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let graph_id = required(request.graph_id.as_deref(), "graph_id")?;
    let work_item_id = required(request.work_item_id.as_deref(), "work_item_id")?;
    owned_snapshot(runtime, context, graph_id.as_str()).await?;
    let decision = runtime
        .reconcile_stale_work_item(
            graph_id.clone(),
            work_item_id.clone(),
            context.principal.to_owned(),
        )
        .await?;
    let (state, reason_code) = match &decision {
        crate::domain::work_graph::StaleReclaimDecision::Reclaimed { item, reason_code }
        | crate::domain::work_graph::StaleReclaimDecision::RequiresReview { item, reason_code } => {
            (item.state.as_str(), *reason_code)
        }
        crate::domain::work_graph::StaleReclaimDecision::DeferredLive { reason_code } => {
            ("deferred_live", *reason_code)
        }
        crate::domain::work_graph::StaleReclaimDecision::NotExpired { reason_code } => {
            ("not_expired", *reason_code)
        }
        crate::domain::work_graph::StaleReclaimDecision::LostRace { reason_code } => {
            ("lost_race", *reason_code)
        }
    };
    Ok(json!({
        "decision": WorkGraphHostDecisionV1 {
            graph_id: Some(graph_id),
            work_item_id: Some(work_item_id),
            state: Some(state.to_owned()),
            ..WorkGraphHostDecisionV1::accepted(reason_code)
        },
    }))
}

async fn cancel_graph(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let graph_id = required(request.graph_id.as_deref(), "graph_id")?;
    owned_snapshot(runtime, context, graph_id.as_str()).await?;
    let report = runtime
        .cancel_work_graph(
            graph_id.clone(),
            request
                .expected_revision
                .ok_or_else(|| Status::invalid_argument("expected_revision is required"))?,
            context.principal.to_owned(),
        )
        .await?;
    Ok(json!({
        "decision": WorkGraphHostDecisionV1 {
            graph_id: Some(graph_id),
            state: Some("cancelled".to_owned()),
            revision: Some(report.graph_revision),
            ..WorkGraphHostDecisionV1::accepted(report.reason_code.clone())
        },
        "cancellation": report,
    }))
}

async fn comment_item(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let graph_id = required(request.graph_id.as_deref(), "graph_id")?;
    let work_item_id = required(request.work_item_id.as_deref(), "work_item_id")?;
    let comment = runtime
        .create_work_graph_comment(WorkGraphCommentCreateRequest {
            graph_id: graph_id.clone(),
            work_item_id: work_item_id.clone(),
            actor_principal: context.principal.to_owned(),
            body: required(request.body.as_deref(), "body")?,
        })
        .await?;
    Ok(json!({
        "decision": WorkGraphHostDecisionV1 {
            graph_id: Some(graph_id),
            work_item_id: Some(work_item_id),
            ..WorkGraphHostDecisionV1::accepted("work_graph.comment.committed")
        },
        "comment": comment,
    }))
}

async fn review_item(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<Value, Status> {
    let graph_id = required(request.graph_id.as_deref(), "graph_id")?;
    owned_snapshot(runtime, context, graph_id.as_str()).await?;
    let work_item_id = required(request.work_item_id.as_deref(), "work_item_id")?;
    let handoff_id = required(request.handoff_id.as_deref(), "handoff_id")?;
    let outcome = runtime
        .review_work_item_handoff(WorkGraphReviewRequest {
            graph_id: graph_id.clone(),
            work_item_id: work_item_id.clone(),
            handoff_id: handoff_id.clone(),
            reviewer_principal: context.principal.to_owned(),
            decision: request
                .review_decision
                .ok_or_else(|| Status::invalid_argument("review_decision is required"))?,
            reason_code: request
                .reason_code
                .clone()
                .unwrap_or_else(|| "work_graph.review.requested".to_owned()),
        })
        .await?;
    Ok(json!({
        "decision": WorkGraphHostDecisionV1 {
            graph_id: Some(graph_id),
            work_item_id: Some(work_item_id),
            handoff_id: Some(handoff_id),
            state: Some(outcome.item_state.as_str().to_owned()),
            revision: Some(outcome.item_revision),
            ..WorkGraphHostDecisionV1::accepted(match outcome.review.decision {
                crate::domain::work_graph::WorkGraphReviewDecision::Approve =>
                    "work_graph.review.approved",
                crate::domain::work_graph::WorkGraphReviewDecision::Reject =>
                    "work_graph.review.rejected",
            })
        },
        "review": outcome.review,
        "graph_revision": outcome.graph_revision,
    }))
}

async fn owned_snapshot(
    runtime: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    graph_id: &str,
) -> Result<crate::journal::work_graph::WorkGraphSnapshotV1, Status> {
    let snapshot = runtime
        .work_graph_snapshot(graph_id.to_owned())
        .await?
        .ok_or_else(|| Status::not_found("WorkGraph was not found"))?;
    if snapshot.graph.owner.principal != context.principal
        || snapshot.graph.owner.device_id != context.device_id
        || snapshot.graph.owner.session_id.as_deref() != Some(context.session_id)
    {
        return Err(Status::permission_denied(
            "WorkGraph is outside the current owner/session scope",
        ));
    }
    Ok(snapshot)
}

fn claim_authority(
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkGraphToolRequest,
) -> Result<WorkClaimAuthority, Status> {
    let token_hex = required(request.claim_token.as_deref(), "claim_token")?;
    let token = WorkClaimToken::from_hex(token_hex.as_str())
        .ok_or_else(|| Status::invalid_argument("claim_token is not a valid capability"))?;
    Ok(WorkClaimAuthority {
        graph_id: required(request.graph_id.as_deref(), "graph_id")?,
        work_item_id: required(request.work_item_id.as_deref(), "work_item_id")?,
        worker_id: context.run_id.to_owned(),
        generation: request
            .claim_generation
            .ok_or_else(|| Status::invalid_argument("claim_generation is required"))?,
        token,
    })
}

fn required(value: Option<&str>, field: &str) -> Result<String, Status> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| Status::invalid_argument(format!("{field} is required")))
}

fn safe_text(value: &str) -> String {
    redact_url_segments_in_text(&redact_auth_error(value))
}

fn build_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    success: bool,
    output: Value,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        success,
        serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec()),
        error,
        false,
        WORK_GRAPH_TOOL_EXECUTOR.to_owned(),
        WORK_GRAPH_TOOL_SANDBOX.to_owned(),
    )
}
