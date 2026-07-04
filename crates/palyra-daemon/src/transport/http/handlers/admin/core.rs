//! Core admin HTTP handlers for runtime diagnostics and run control.
//!
//! These handlers are intentionally thin adapters around daemon runtime
//! services so auth, counters, and response shaping stay consistent with the
//! rest of the transport layer.

use crate::*;

/// Returns the aggregate admin status document for the daemon.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// runtime snapshot collection, serialization, or diagnostics assembly fails.
pub(crate) async fn admin_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let snapshot = state
        .runtime
        .status_snapshot_async(context.clone(), state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let auth_snapshot = state
        .auth_runtime
        .admin_status_snapshot(Arc::clone(&state.runtime))
        .await
        .map_err(runtime_status_response)?;
    let mut payload = serde_json::to_value(&snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize admin status snapshot: {error}"
        )))
    })?;
    let auth_payload = serde_json::to_value(auth_snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize auth status snapshot: {error}"
        )))
    })?;
    let media_payload = state.channels.media_snapshot().map_err(channel_platform_error_response)?;
    let observability_payload = build_observability_payload(
        &state,
        &context,
        &snapshot.model_provider,
        &auth_payload,
        &media_payload,
    )
    .await?;
    let tool_jobs = state
        .runtime
        .list_tool_jobs(crate::journal::ToolJobsListFilter {
            owner_principal: Some(context.principal.clone()),
            session_id: None,
            run_id: None,
            include_terminal: true,
            limit: 256,
        })
        .await
        .map_err(runtime_status_response)?;
    let generated_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let memory_payload = json!({
        "usage": {
            "entries": snapshot.counters.memory_items_ingested,
            "bytes": 0,
        },
        "providers": [],
    });
    let skills_payload = collect_console_skills_diagnostics(&state).await;
    let plugins_payload = collect_console_plugins_diagnostics();
    let networked_workers_payload = collect_console_networked_worker_diagnostics(&state);
    let null_payload = Value::Null;
    let runtime_preview_payload =
        observability_payload.pointer("/runtime_preview").unwrap_or(&null_payload);
    let support_bundle_payload =
        observability_payload.pointer("/support_bundle").unwrap_or(&null_payload);
    let mcp_payload = collect_console_mcp_diagnostics(&state, generated_at_unix_ms);
    let runtime_health = crate::runtime_diagnostics::build_runtime_health_snapshot(
        generated_at_unix_ms,
        &snapshot,
        &auth_payload,
        &memory_payload,
        &skills_payload,
        &plugins_payload,
        &networked_workers_payload,
        support_bundle_payload,
        runtime_preview_payload,
        &mcp_payload,
        &tool_jobs,
    );
    let runtime_metrics = crate::runtime_diagnostics::build_agent_runtime_metrics_snapshot(
        &snapshot,
        runtime_preview_payload,
        &memory_payload,
        &tool_jobs,
    );
    let lifecycle = crate::runtime_diagnostics::build_daemon_lifecycle_snapshot_from_status(
        &snapshot,
        runtime_preview_payload,
    );
    let metrics_catalog = crate::runtime_diagnostics::build_metrics_catalog_snapshot();
    let trace_export = crate::runtime_diagnostics::build_trace_exporter_contract();
    let diagnostics_timeline =
        crate::runtime_diagnostics::build_diagnostics_timeline_contract(generated_at_unix_ms);
    let active_tool_jobs = json!({
        "total": tool_jobs.len(),
        "active": tool_jobs.iter().filter(|job| job.state.is_active()).count(),
        "states": tool_jobs.iter().map(|job| job.state.as_str()).collect::<Vec<_>>(),
    });
    let shutdown_forensics =
        crate::support::build_shutdown_forensic_snapshot(crate::support::ShutdownForensicInput {
            generated_at_unix_ms,
            active_sessions: 0,
            active_runs: lifecycle.active_runs,
            queue_depth: lifecycle.queue_depth,
            pending_approvals: lifecycle.pending_approvals,
            provider_lease_state: json!({"state": "not_reported"}),
            active_tool_jobs,
            child_process_tree: json!({"state": "not_collected"}),
            mcp_state: mcp_payload.clone(),
            worker_leases: networked_workers_payload.clone(),
            recent_runtime_errors: Vec::new(),
        });
    let support_runtime = crate::support::build_support_runtime_snapshot(
        generated_at_unix_ms,
        None,
        vec!["daemon".to_owned(), "runtime_diagnostics".to_owned(), "support_bundle".to_owned()],
        json!({
            "lifecycle": lifecycle.clone(),
            "shutdown_forensics": shutdown_forensics.clone(),
            "metrics_catalog": metrics_catalog.clone(),
            "timeline": diagnostics_timeline.clone(),
            "trace_export": trace_export.clone(),
        }),
    );
    if let Value::Object(ref mut map) = payload {
        map.insert("auth".to_owned(), auth_payload);
        map.insert("mcp".to_owned(), mcp_payload);
        map.insert("media".to_owned(), media_payload);
        map.insert("observability".to_owned(), observability_payload);
        map.insert(
            "feature_rollouts".to_owned(),
            crate::feature_rollout_maturity::build_feature_rollout_diagnostics(
                &state.runtime.config.feature_rollouts,
            ),
        );
        map.insert(
            "feature_rollout_maturity".to_owned(),
            crate::feature_rollout_maturity::build_feature_rollout_maturity_summary(
                &state.runtime.config.feature_rollouts,
            ),
        );
        map.insert(
            "runtime_health".to_owned(),
            serde_json::to_value(runtime_health).map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to serialize runtime health snapshot: {error}"
                )))
            })?,
        );
        map.insert(
            "lifecycle".to_owned(),
            serde_json::to_value(lifecycle).map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to serialize lifecycle snapshot: {error}"
                )))
            })?,
        );
        map.insert("agent_runtime_metrics".to_owned(), runtime_metrics);
        map.insert("metrics_catalog".to_owned(), metrics_catalog);
        map.insert("trace_export".to_owned(), trace_export.clone());
        map.insert(
            "runtime_diagnostics".to_owned(),
            json!({
                "timeline": diagnostics_timeline,
                "trace_export": trace_export,
                "shutdown_forensics": shutdown_forensics,
                "support_runtime": support_runtime,
            }),
        );
    }
    Ok(Json(payload))
}

/// Returns the machine-readable method and scope registry for public surfaces.
///
/// # Errors
/// Returns an error response when admin authorization or request-context
/// validation fails.
pub(crate) async fn admin_methods_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<crate::method_registry::MethodRegistrySnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    Ok(Json(crate::method_registry::build_method_registry_snapshot()))
}

/// Renders daemon runtime metrics in Prometheus text format.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// runtime metric snapshot collection fails.
pub(crate) async fn admin_metrics_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let snapshot = state
        .runtime
        .status_snapshot_async(context.clone(), state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let tool_jobs = state
        .runtime
        .list_tool_jobs(crate::journal::ToolJobsListFilter {
            owner_principal: Some(context.principal),
            session_id: None,
            run_id: None,
            include_terminal: true,
            limit: 256,
        })
        .await
        .map_err(runtime_status_response)?;
    let body = crate::runtime_diagnostics::render_prometheus_metrics(&snapshot, &tool_jobs);
    Ok(([(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], body).into_response())
}

/// Returns the most recent gateway journal records.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// journal snapshot collection fails.
pub(crate) async fn admin_journal_recent_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<JournalRecentQuery>,
) -> Result<Json<gateway::JournalRecentSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let limit = query.limit.unwrap_or(20);
    let snapshot =
        state.runtime.recent_journal_snapshot(limit).await.map_err(runtime_status_response)?;
    Ok(Json(snapshot))
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdminStateDoctorQuery {
    #[serde(default)]
    fast_window: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdminStateHashChainQuery {
    #[serde(default)]
    full: Option<bool>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdminStateRepairRequest {
    #[serde(default)]
    dry_run: Option<bool>,
    #[serde(default)]
    fts_only: Option<bool>,
    #[serde(default)]
    actor_principal: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdminStateCheckpointRequest {
    #[serde(default)]
    mode: Option<crate::journal::state_health::JournalWalCheckpointMode>,
}

/// Returns the SQLite journal state doctor report.
///
/// # Errors
/// Returns an error response when admin authorization, request context
/// extraction, or state probes fail.
pub(crate) async fn admin_state_doctor_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AdminStateDoctorQuery>,
) -> Result<Json<crate::journal::state_health::JournalHealthReport>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let report = state
        .runtime
        .journal_state_health_report(query.fast_window)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(report))
}

/// Previews or applies targeted SQLite journal repair.
///
/// # Errors
/// Returns an error response when admin authorization, request context
/// extraction, or the repair operation fails.
pub(crate) async fn admin_state_repair_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<AdminStateRepairRequest>,
) -> Result<Json<crate::journal::state_health::JournalStateRepairReport>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let actor_principal = payload.actor_principal.unwrap_or(context.principal);
    let request = crate::journal::state_health::JournalStateRepairRequest {
        dry_run: payload.dry_run.unwrap_or(true),
        fts_only: payload.fts_only.unwrap_or(true),
        actor_principal,
    };
    let report =
        state.runtime.repair_journal_state(request).await.map_err(runtime_status_response)?;
    Ok(Json(report))
}

/// Runs a WAL checkpoint against the SQLite journal.
///
/// # Errors
/// Returns an error response when admin authorization, request context
/// extraction, or SQLite checkpoint execution fails.
pub(crate) async fn admin_state_checkpoint_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<AdminStateCheckpointRequest>,
) -> Result<Json<crate::journal::state_health::JournalWalCheckpointReport>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let mode =
        payload.mode.unwrap_or(crate::journal::state_health::JournalWalCheckpointMode::Passive);
    let report =
        state.runtime.checkpoint_journal_wal(mode).await.map_err(runtime_status_response)?;
    Ok(Json(report))
}

/// Verifies the SQLite journal hash chain.
///
/// # Errors
/// Returns an error response when admin authorization, request context
/// extraction, or hash-chain verification fails.
pub(crate) async fn admin_state_hash_chain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AdminStateHashChainQuery>,
) -> Result<Json<crate::journal::state_health::JournalHashChainVerificationReport>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let scope = if query.full.unwrap_or(false) {
        crate::journal::state_health::JournalHashVerificationScope::Full
    } else {
        crate::journal::state_health::JournalHashVerificationScope::FastWindow {
            limit: query.limit.unwrap_or(256).max(1),
        }
    };
    let report =
        state.runtime.verify_journal_hash_chain(scope).await.map_err(runtime_status_response)?;
    Ok(Json(report))
}

/// Creates rebuildable SQLite journal sidecar directories.
///
/// # Errors
/// Returns an error response when admin authorization, request context
/// extraction, directory creation, or permission hardening fails.
pub(crate) async fn admin_state_sidecars_prepare_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<crate::journal::state_health::SidecarIndexDescriptor>>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let descriptors =
        state.runtime.prepare_journal_sidecar_storage().await.map_err(runtime_status_response)?;
    Ok(Json(descriptors))
}

/// Explains the policy decision for an operator-supplied principal/action pair.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction, or
/// policy evaluation fails.
pub(crate) async fn admin_policy_explain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<PolicyExplainQuery>,
) -> Result<Json<PolicyExplainResponse>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();

    let request = PolicyRequest {
        principal: query.principal,
        action: query.action,
        resource: query.resource,
    };
    let requested_tool = requested_tool_for_admin_policy_explain(&request);
    let request_context = palyra_policy::PolicyRequestContext {
        device_id: query.device_id,
        channel: query.channel,
        session_id: query.session_id,
        run_id: query.run_id,
        tool_name: requested_tool.clone(),
        skill_id: None,
        capabilities: requested_tool
            .as_deref()
            .map(crate::tool_protocol::tool_policy_capability_names)
            .unwrap_or_default(),
    };
    let evaluation_config = PolicyEvaluationConfig {
        allowlisted_tools: state.runtime.config.tool_call.allowed_tools.clone(),
        sensitive_tool_names: palyra_common::tool_catalog::sensitive_allowlisted_tool_names(
            state.runtime.config.tool_call.allowed_tools.as_slice(),
        ),
        sensitive_capability_names: palyra_common::tool_catalog::SENSITIVE_CAPABILITY_POLICY_NAMES
            .iter()
            .map(|value| (*value).to_owned())
            .collect(),
        ..PolicyEvaluationConfig::default()
    };
    let evaluation =
        palyra_policy::evaluate_with_context(&request, &request_context, &evaluation_config)
            .map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to evaluate policy with Cedar engine: {error}"
                )))
            })?;
    let diagnostics = palyra_policy::policy_explain_diagnostics_value(&request, &evaluation);
    let (decision, approval_required, reason) = match &evaluation.decision {
        PolicyDecision::Allow => ("allow".to_owned(), false, evaluation.explanation.reason.clone()),
        PolicyDecision::DenyByDefault { reason } => {
            ("deny_by_default".to_owned(), true, reason.clone())
        }
    };
    let runtime_approval_tool = requested_tool;
    let runtime_approval_required = runtime_approval_tool
        .as_deref()
        .map(crate::tool_protocol::tool_requires_approval)
        .unwrap_or(false);

    Ok(Json(PolicyExplainResponse {
        principal: request.principal,
        action: request.action,
        resource: request.resource,
        decision,
        approval_required,
        runtime_approval_required,
        runtime_approval_tool,
        reason,
        matched_policies: evaluation.explanation.matched_policy_ids,
        diagnostics,
    }))
}

fn requested_tool_for_admin_policy_explain(request: &PolicyRequest) -> Option<String> {
    if !request.action.eq_ignore_ascii_case("tool.execute") {
        return None;
    }
    let trimmed = request.resource.trim();
    if trimmed.is_empty() {
        return None;
    }
    let tool_name = trimmed.strip_prefix("tool:").unwrap_or(trimmed).trim();
    if tool_name.is_empty() {
        None
    } else {
        Some(tool_name.to_ascii_lowercase())
    }
}

/// Returns an orchestrator run status snapshot for admin diagnostics.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// run-id validation, run-id resolution, or status snapshot lookup fails.
pub(crate) async fn admin_run_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let diagnostics_run_id = resolve_admin_diagnostics_run_id(&state, run_id.as_str()).await?;
    let snapshot = state
        .runtime
        .orchestrator_run_status_snapshot(diagnostics_run_id.clone())
        .await
        .map_err(runtime_status_response)?;
    let Some(snapshot) = snapshot else {
        return Err(runtime_status_response(tonic::Status::not_found(format!(
            "orchestrator run not found after resolving {run_id} to {diagnostics_run_id}"
        ))));
    };
    let verification_summary = collect_run_verification_summary(&state, &snapshot).await;
    let mut payload = run_status_payload(&snapshot)
        .map_err(|error| runtime_status_response(tonic::Status::internal(error)))?;
    payload["verification_summary"] = verification_summary;
    Ok(Json(payload))
}

fn run_status_payload(snapshot: &OrchestratorRunStatusSnapshot) -> Result<Value, String> {
    let mut payload = serde_json::to_value(snapshot)
        .map_err(|error| format!("failed to serialize run status snapshot: {error}"))?;
    enrich_run_status_lifecycle(&mut payload, snapshot);
    Ok(payload)
}

async fn collect_run_verification_summary(
    state: &AppState,
    snapshot: &OrchestratorRunStatusSnapshot,
) -> Value {
    let journal = match state.runtime.journal_snapshot_for_run(snapshot.run_id.clone(), 256).await {
        Ok(journal) => journal,
        Err(error) => {
            return verification_summary_unavailable(
                state.runtime.config.feature_rollouts.verification_runtime.enabled,
                "verification.status.journal_unavailable",
                format!("failed to load run verification journal: {error}").as_str(),
            );
        }
    };
    let (projections, diagnostics) = verification_summary_inputs_from_journal(&journal.events);
    let finalizer = collect_run_verification_finalizer(state, snapshot).await;
    let summary = crate::application::verification::verification_summary_for_public_artifact(
        crate::application::verification::VerificationSummaryRequest {
            rollout_enabled: state.runtime.config.feature_rollouts.verification_runtime.enabled,
            journal_total_events: journal.total_events,
            journal_window_events: u64::try_from(journal.events.len()).unwrap_or(u64::MAX),
            projections: projections.as_slice(),
            diagnostics: diagnostics.as_slice(),
            finalizer: finalizer.as_ref(),
        },
    );
    serde_json::to_value(summary).unwrap_or_else(|error| {
        verification_summary_unavailable(
            state.runtime.config.feature_rollouts.verification_runtime.enabled,
            "verification.status.serialization_failed",
            format!("failed to serialize run verification summary: {error}").as_str(),
        )
    })
}

fn verification_summary_inputs_from_journal(
    events: &[crate::journal::JournalEventRecord],
) -> (
    Vec<crate::application::verification::VerificationJournalProjection>,
    Vec<crate::application::verification::VerificationSummaryDiagnostic>,
) {
    let mut projections = Vec::new();
    let mut diagnostics = Vec::new();
    for event in events {
        let Ok(payload) = serde_json::from_str::<Value>(event.payload_json.as_str()) else {
            continue;
        };
        if let Some(projection) =
            crate::application::verification::verification_projection_from_payload(&payload)
        {
            projections.push(projection);
            continue;
        }
        if let Some(diagnostic) =
            crate::application::verification::verification_diagnostic_from_payload(&payload)
        {
            diagnostics.push(diagnostic);
        }
    }
    (projections, diagnostics)
}

async fn collect_run_verification_finalizer(
    state: &AppState,
    snapshot: &OrchestratorRunStatusSnapshot,
) -> Option<Value> {
    let after_seq = tail_tape_after_seq(snapshot.tape_events, 256);
    let tape = state
        .runtime
        .orchestrator_tape_snapshot(snapshot.run_id.clone(), after_seq, Some(256))
        .await
        .ok()?;
    tape.events.iter().rev().find_map(|event| {
        let payload = serde_json::from_str::<Value>(event.payload_json.as_str()).ok()?;
        payload.pointer("/finalization/verification_finalizer").cloned()
    })
}

fn tail_tape_after_seq(tape_events: u64, limit: usize) -> Option<i64> {
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    (tape_events > limit).then(|| {
        i64::try_from(tape_events.saturating_sub(limit).saturating_sub(1)).unwrap_or(i64::MAX)
    })
}

fn verification_summary_unavailable(
    rollout_enabled: bool,
    reason_code: &str,
    error: &str,
) -> Value {
    json!({
        "schema_version": crate::application::verification::VERIFICATION_SCHEMA_VERSION,
        "state": if rollout_enabled { "unavailable" } else { "disabled" },
        "rollout_enabled": rollout_enabled,
        "changed_files": [],
        "commands_executed": [],
        "command_classification": [],
        "latest_verification_status": {
            "schema_version": crate::application::verification::VERIFICATION_SCHEMA_VERSION,
            "decision": if rollout_enabled { "unknown" } else { "disabled" },
            "rollout_enabled": rollout_enabled,
            "journal_total_events": Value::Null,
            "journal_window_events": 0,
            "verification_projection_events": 0,
            "classified_commands": 0,
            "recorded_events": 0,
            "passing_events": 0,
            "failed_events": 0,
            "stale_requirements": 0,
            "fresh_requirements": 0,
            "unknown_requirements": 0,
            "latest_event_type": Value::Null,
            "latest_status": Value::Null,
            "latest_created_at_unix_ms": Value::Null,
            "reason_codes": [reason_code],
            "journal_events": [
                crate::application::verification::VERIFICATION_COMMAND_CLASSIFIED,
                crate::application::verification::VERIFICATION_EVENT_RECORDED,
                crate::application::verification::VERIFICATION_STATE_STALE,
                crate::application::verification::VERIFICATION_FRESHNESS_CHECKED,
            ],
            "redaction_level": crate::application::verification::VERIFICATION_REDACTION_LEVEL,
        },
        "unverified_mutations": [],
        "stale_evidence_reasons": [reason_code],
        "diagnostics": [],
        "final_answer": {
            "observed": false,
            "status": Value::Null,
            "reason_code": Value::Null,
            "allowed": false,
            "allowed_because": "verification.finalizer.not_observed",
            "pending_requirement_count": Value::Null,
            "satisfied_requirement_count": Value::Null,
            "evidence_refs": [],
            "nudge": Value::Null,
            "unverified_reason": Value::Null,
        },
        "final_answer_allowed": false,
        "final_answer_allowed_because": "verification.finalizer.not_observed",
        "evidence_refs": [],
        "reason_codes": [reason_code],
        "error": sanitize_http_error_message(error),
        "redaction_level": crate::application::verification::VERIFICATION_REDACTION_LEVEL,
    })
}

fn enrich_run_status_lifecycle(payload: &mut Value, snapshot: &OrchestratorRunStatusSnapshot) {
    if snapshot.state != "failed" {
        return;
    }
    let Some(message) = snapshot.last_error.as_deref() else {
        return;
    };
    if !is_needs_continuation_message(message) {
        return;
    }
    payload["wire_state"] = Value::String(snapshot.state.clone());
    payload["lifecycle_state"] = Value::String("needs_continuation".to_owned());
    payload["partial"] = Value::Bool(true);
    payload["continuation_required"] = Value::Bool(true);
    payload["continuation_available"] = Value::Bool(true);
    if let Some(reason_code) = continuation_reason_code(message) {
        payload["reason_code"] = Value::String(reason_code.to_owned());
    }
}

fn is_needs_continuation_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("needs_continuation=true") || lower.contains("needs continuation")
}

fn continuation_reason_code(message: &str) -> Option<&str> {
    let marker = "reason_code=";
    let start = message.find(marker)? + marker.len();
    let rest = &message[start..];
    let end = rest
        .find(|ch: char| ch.is_whitespace() || matches!(ch, ';' | ',' | ')'))
        .unwrap_or(rest.len());
    let reason = rest[..end].trim();
    (!reason.is_empty()).then_some(reason)
}

/// Waits for an orchestrator run to finish through the admin diagnostics API.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// run-id validation, diagnostics run-id resolution, or status snapshot lookup
/// fails. A wait deadline is returned as a successful payload containing the
/// current run status.
pub(crate) async fn admin_run_wait_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    payload: Option<Json<RunWaitRequest>>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let diagnostics_run_id = resolve_admin_diagnostics_run_id(&state, run_id.as_str()).await?;
    let request = payload
        .map(|Json(payload)| payload)
        .unwrap_or(RunWaitRequest { timeout_ms: None, return_on_waiting: None });
    let timeout_ms = request.timeout_ms.unwrap_or(30_000).clamp(1, 120_000);
    let outcome = state
        .runtime
        .wait_for_orchestrator_run(crate::gateway::OrchestratorRunWaitRequest {
            run_id: diagnostics_run_id.clone(),
            timeout: std::time::Duration::from_millis(timeout_ms),
            poll_interval: std::time::Duration::from_millis(250),
            return_on_waiting: request.return_on_waiting.unwrap_or(false),
        })
        .await;
    let payload = match outcome {
        Ok(outcome) => admin_run_wait_payload(
            outcome.snapshot.run_id.as_str(),
            timeout_ms,
            false,
            Some(outcome.canonical_state.as_str()),
            {
                let verification_summary =
                    collect_run_verification_summary(&state, &outcome.snapshot).await;
                let mut run = run_status_payload(&outcome.snapshot)
                    .map_err(|error| runtime_status_response(tonic::Status::internal(error)))?;
                run["verification_summary"] = verification_summary;
                run
            },
        ),
        Err(error) if error.code() == tonic::Code::DeadlineExceeded => {
            let snapshot = state
                .runtime
                .orchestrator_run_status_snapshot(diagnostics_run_id.clone())
                .await
                .map_err(runtime_status_response)?
                .ok_or_else(|| {
                    runtime_status_response(tonic::Status::not_found(format!(
                        "orchestrator run not found after resolving {run_id} to {diagnostics_run_id}"
                    )))
                })?;
            admin_run_wait_payload(snapshot.run_id.as_str(), timeout_ms, true, None, {
                let verification_summary =
                    collect_run_verification_summary(&state, &snapshot).await;
                let mut run = run_status_payload(&snapshot)
                    .map_err(|error| runtime_status_response(tonic::Status::internal(error)))?;
                run["verification_summary"] = verification_summary;
                run
            })
        }
        Err(error) => return Err(runtime_status_response(error)),
    };
    Ok(Json(payload))
}

fn admin_run_wait_payload(
    run_id: &str,
    timeout_ms: u64,
    timed_out: bool,
    canonical_state: Option<&str>,
    run: Value,
) -> Value {
    let status = if timed_out {
        "timeout"
    } else {
        run.get("state").and_then(Value::as_str).unwrap_or("unknown")
    };
    json!({
        "run_id": run_id,
        "object": "run.wait",
        "status": status,
        "timed_out": timed_out,
        "timeout_ms": timeout_ms,
        "canonical_state": canonical_state,
        "run": run,
    })
}

/// Returns a paginated orchestrator run tape for admin diagnostics.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// run-id validation, run-id resolution, or tape snapshot lookup fails.
pub(crate) async fn admin_run_tape_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Query(query): Query<RunTapeQuery>,
) -> Result<Json<gateway::RunTapeSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let diagnostics_run_id = resolve_admin_diagnostics_run_id(&state, run_id.as_str()).await?;
    let snapshot = state
        .runtime
        .orchestrator_tape_snapshot(diagnostics_run_id, query.after_seq, query.limit)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(snapshot))
}

async fn resolve_admin_diagnostics_run_id(
    state: &AppState,
    requested_run_id: &str,
) -> Result<String, Response> {
    state
        .runtime
        .resolve_orchestrator_diagnostics_run_id(requested_run_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {requested_run_id}; gateway diagnostics accept \
                 orchestrator_run_id and linked routine/cron run_id values. If this id came from \
                 objective or routine output, use orchestrator_run_id when available or retry after \
                 the run links one."
            )))
    })
}

/// Applies a unified run control command through the admin API.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// run-id validation, run lookup, or the selected control effect fails.
/// Authorization is completed before any runtime mutation is attempted.
pub(crate) async fn admin_run_control_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    Json(payload): Json<RunControlRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let snapshot = state
        .runtime
        .orchestrator_run_status_snapshot(run_id.clone())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "orchestrator run not found: {run_id}"
            )))
        })?;
    let requested_session_id = payload.session_id.and_then(trim_to_option);
    if let Some(session_id) = requested_session_id.as_deref() {
        if session_id != snapshot.session_id {
            return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
                "session_id {session_id} does not match run {run_id} session {}",
                snapshot.session_id
            ))));
        }
    }
    let command = payload.command;
    let outcome = state
        .runtime
        .apply_turn_control(crate::application::turn_control::TurnControlRequest {
            operation: command.operation(),
            actor_principal: context.principal,
            active_phase: payload.active_phase,
            session_id: requested_session_id.or_else(|| Some(snapshot.session_id.clone())),
            run_id: Some(snapshot.run_id.clone()),
            queued_input_id: payload.queued_input_id.and_then(trim_to_option),
            priority_lane: payload.priority_lane.and_then(trim_to_option),
            instruction: payload.instruction.and_then(trim_to_option),
            reason: payload
                .reason
                .and_then(trim_to_option)
                .or_else(|| Some(format!("admin_run_control:{}", command.as_str()))),
            dry_run: payload.dry_run.unwrap_or(false),
        })
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "run_id": snapshot.run_id,
        "object": "run.control",
        "command": command.as_str(),
        "turn_control": outcome.decision,
        "effect": outcome.effect,
    })))
}

/// Requests cancellation of an orchestrator run from the admin API.
///
/// # Errors
/// Returns an error response when admin authorization, context extraction,
/// run-id validation, cancellation request recording, or cleanup signaling
/// fails.
pub(crate) async fn admin_run_cancel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(run_id): Path<String>,
    payload: Option<Json<RunCancelRequest>>,
) -> Result<Json<gateway::RunCancelSnapshot>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    validate_canonical_id(run_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument("run_id must be a canonical ULID"))
    })?;
    state.runtime.record_admin_status_request();
    let reason = payload
        .and_then(|body| body.0.reason)
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        })
        .unwrap_or_else(|| "admin_cancel_requested".to_owned());
    let mut response = state
        .runtime
        .request_orchestrator_cancel(OrchestratorCancelRequest { run_id, reason })
        .await
        .map_err(runtime_status_response)?;
    let cleanup_summary = gateway::cleanup_run_resources(
        &state.runtime,
        response.run_id.as_str(),
        response.reason.as_str(),
    )
    .await;
    response.cleanup_warning = cleanup_summary.cleanup_warning;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_status_payload_marks_resumable_failed_runs_as_needs_continuation() {
        let snapshot = OrchestratorRunStatusSnapshot {
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAS".to_owned(),
            state: "failed".to_owned(),
            cancel_requested: false,
            cancel_reason: None,
            principal: "user:ops".to_owned(),
            device_id: "device:test".to_owned(),
            channel: None,
            prompt_tokens: 10,
            completion_tokens: 2,
            total_tokens: 12,
            created_at_unix_ms: 1,
            started_at_unix_ms: 2,
            completed_at_unix_ms: Some(3),
            updated_at_unix_ms: 3,
            last_error: Some(
                "agent loop wall-clock budget exhausted; needs_continuation=true reason_code=wall_clock"
                    .to_owned(),
            ),
            origin_kind: "agent_run".to_owned(),
            origin_run_id: None,
            parent_run_id: None,
            triggered_by_principal: None,
            parameter_delta_json: None,
            delegation: None,
            merge_result: None,
            tape_events: 42,
        };

        let payload = run_status_payload(&snapshot).expect("snapshot should serialize");

        assert_eq!(payload["state"], "failed");
        assert_eq!(payload["wire_state"], "failed");
        assert_eq!(payload["lifecycle_state"], "needs_continuation");
        assert_eq!(payload["continuation_required"], true);
        assert_eq!(payload["continuation_available"], true);
        assert_eq!(payload["partial"], true);
        assert_eq!(payload["reason_code"], "wall_clock");
    }
}
