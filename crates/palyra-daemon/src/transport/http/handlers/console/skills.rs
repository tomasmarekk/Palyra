//! Console skill inventory, install, verification, and builder handlers.
//!
//! These routes manage local skill artifacts and generated builder candidates.
//! Artifact verification and security audit outcomes are part of the console
//! trust-chain surface, so JSON shapes must stay stable with `apps/web`.

use crate::gateway::current_unix_ms;
use crate::journal::{
    LearningCandidateListFilter, LearningCandidateRecord, LearningCandidateReviewRequest,
};
use crate::*;
use palyra_common::feature_rollouts::{
    FeatureRolloutSetting, DYNAMIC_TOOL_BUILDER_ROLLOUT_CONFIG_PATH,
    DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
};
use palyra_common::versioned_json::{migrate_updated_at_metadata_v0_to_v1, parse_versioned_json};

/// Lists installed skills with runtime status snapshots.
///
/// # Errors
/// Returns an error response when session authorization, skills-root
/// resolution, index loading, or runtime status lookup fails.
pub(crate) async fn console_skills_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSkillsListQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let skills_root = resolve_skills_root()?;
    let mut index = {
        let _guard = INSTALLED_SKILLS_INDEX_LOCK.lock().map_err(|_| {
            runtime_status_response(tonic::Status::internal(
                "installed skills index lock is poisoned",
            ))
        })?;
        let mut index = load_installed_skills_index(skills_root.as_path())?;
        if refresh_installed_skill_staleness(&mut index, unix_ms_now().unwrap_or_default()) {
            save_installed_skills_index(skills_root.as_path(), &index)?;
        }
        index
    };
    if let Some(skill_id) =
        query.skill_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        let skill_id = skill_id.to_ascii_lowercase();
        index.entries.retain(|entry| entry.skill_id == skill_id);
    }

    let mut entries = Vec::with_capacity(index.entries.len());
    for entry in index.entries {
        let status = state
            .runtime
            .skill_status(entry.skill_id.clone(), entry.version.clone())
            .await
            .map_err(runtime_status_response)?;
        entries.push(json!({
            "record": entry,
            "status": status,
        }));
    }
    Ok(Json(json!({
        "skills_root": skills_root,
        "count": entries.len(),
        "entries": entries,
        "page": build_page_info(entries.len().max(1), entries.len(), None),
    })))
}

/// Lists generated skill-builder candidates.
///
/// # Errors
/// Returns an error response when session authorization, skills-root
/// resolution, or candidate-index loading fails.
pub(crate) async fn console_skill_builder_candidates_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSkillBuilderCandidatesQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let skills_root = resolve_skills_root()?;
    let mut index = load_skill_builder_candidate_index(skills_root.as_path())?;
    let rollout = dynamic_tool_builder_rollout(&state);
    if let Some(source_kind) =
        query.source_kind.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        index.entries.retain(|entry| entry.source_kind == source_kind);
    }

    Ok(Json(json!({
        "rollout_flag": DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
        "rollout_source": rollout.source,
        "rollout_enabled": rollout.enabled,
        "count": index.entries.len(),
        "entries": index.entries,
        "skills_root": skills_root,
    })))
}

/// Creates a generated skill-builder scaffold from a prompt or procedure.
///
/// # Errors
/// Returns an error response when session authorization, rollout checks,
/// candidate loading, input normalization, scaffold writing, status upsert,
/// event recording, or candidate-index persistence fails.
pub(crate) async fn console_skill_builder_candidate_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleSkillBuilderCreateRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let rollout = dynamic_tool_builder_rollout(&state);
    if !rollout.enabled {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "dynamic tool builder is disabled; enable {DYNAMIC_TOOL_BUILDER_ROLLOUT_CONFIG_PATH} or set {DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV}=true to opt in"
        ))));
    }

    let source = if let Some(candidate_id) =
        payload.learning_candidate_id.as_deref().and_then(|value| trim_to_option(value.to_owned()))
    {
        let candidate =
            load_console_procedure_candidate(&state, &session.context, candidate_id.as_str())
                .await?;
        if candidate.candidate_kind != "procedure" {
            return Err(runtime_status_response(tonic::Status::failed_precondition(
                "dynamic builder only accepts procedure learning candidates or explicit prompts",
            )));
        }
        BuilderSource::Procedure(Box::new(candidate))
    } else {
        let prompt = payload.prompt.clone().and_then(trim_to_option).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "prompt or learning_candidate_id is required",
            ))
        })?;
        BuilderSource::Prompt {
            prompt,
            source_ref: format!("prompt:{}", Ulid::generate().to_string().to_ascii_lowercase()),
        }
    };

    let default_skill_id = match &source {
        BuilderSource::Procedure(candidate) => {
            default_generated_skill_id(candidate.candidate_id.as_str())
        }
        BuilderSource::Prompt { source_ref, .. } => {
            format!("palyra.generated.builder.{}", source_ref.replace(':', "."))
        }
    };
    let skill_id = normalize_generated_skill_identifier(
        payload.skill_id.as_deref().unwrap_or(default_skill_id.as_str()),
        "skill_id",
    )?;
    let version = normalize_generated_skill_version(payload.version.as_deref().unwrap_or("0.1.0"))?;
    let publisher = normalize_generated_skill_identifier(
        payload.publisher.as_deref().unwrap_or("palyra.generated"),
        "publisher",
    )?;
    let fallback_name = match &source {
        BuilderSource::Procedure(candidate) => candidate.title.clone(),
        BuilderSource::Prompt { prompt, .. } => prompt.clone(),
    };
    let name = payload
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or(fallback_name);
    let tool_name = payload
        .tool_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "Run builder candidate".to_owned());
    let tool_description = payload
        .tool_description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| match &source {
            BuilderSource::Procedure(candidate) => candidate.summary.clone(),
            BuilderSource::Prompt { prompt, .. } => prompt.clone(),
        });

    let scaffold = write_skill_builder_scaffold(
        &source,
        SkillBuilderScaffoldRequest {
            skill_id: skill_id.clone(),
            version: version.clone(),
            publisher: publisher.clone(),
            name: name.clone(),
            tool_id: payload.tool_id.clone(),
            tool_name,
            tool_description,
            review_notes: payload.review_notes.clone(),
            capabilities: payload.capabilities.clone(),
        },
    )?;

    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id: skill_id.clone(),
            version: version.clone(),
            status: SkillExecutionStatus::Quarantined,
            reason: Some(format!("dynamic_builder_candidate:{}", scaffold.builder_candidate_id)),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: session.context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&session.context, "skill.builder_candidate_created", &record)
        .await
        .map_err(runtime_status_response)?;

    let skills_root = resolve_skills_root()?;
    let mut index = load_skill_builder_candidate_index(skills_root.as_path())?;
    index.entries.retain(|entry| entry.candidate_id != scaffold.builder_candidate_id);
    index.entries.push(SkillBuilderCandidateRecord {
        candidate_id: scaffold.builder_candidate_id.clone(),
        skill_id: scaffold.skill_id.clone(),
        version: scaffold.version.clone(),
        publisher: scaffold.publisher.clone(),
        name: scaffold.name.clone(),
        source_kind: scaffold.source_kind.clone(),
        source_ref: scaffold.source_ref.clone(),
        summary: scaffold.summary.clone(),
        status: "quarantined".to_owned(),
        rollout_flag: DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV.to_owned(),
        rollout_enabled: rollout.enabled,
        scaffold_root: scaffold.scaffold_root.clone(),
        manifest_path: scaffold.manifest_path.clone(),
        capability_declaration_path: scaffold.capability_declaration_path.clone(),
        provenance_path: scaffold.provenance_path.clone(),
        test_harness_path: scaffold.test_harness_path.clone(),
        artifact_plan_path: Some(scaffold.artifact_plan_path.clone()),
        eval_outcome_path: Some(scaffold.eval_outcome_path.clone()),
        artifact_status: scaffold.artifact_status.clone(),
        eval_status: scaffold.eval_status.clone(),
        quarantine_reason: scaffold.quarantine_reason.clone(),
        reproducibility_key: Some(scaffold.reproducibility_key.clone()),
        capability_profile: scaffold.capability_profile.clone(),
        generated_at_unix_ms: scaffold.generated_at_unix_ms,
        updated_at_unix_ms: scaffold.generated_at_unix_ms,
    });
    save_skill_builder_candidate_index(skills_root.as_path(), &index)?;

    Ok(Json(json!({
        "rollout_flag": DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV,
        "rollout_source": rollout.source,
        "rollout_enabled": rollout.enabled,
        "candidate": index.entries.iter().find(|entry| entry.candidate_id == scaffold.builder_candidate_id).cloned(),
        "skill": {
            "skill_id": scaffold.skill_id,
            "version": scaffold.version,
            "publisher": scaffold.publisher,
            "name": scaffold.name,
            "scaffold_root": scaffold.scaffold_root,
            "files": scaffold.files,
            "quarantine_status": skill_status_response(record),
        },
    })))
}

/// Installs a skill artifact after verification and security audit.
///
/// # Errors
/// Returns an error response when session authorization, artifact IO,
/// inspection, trust-store handling, security audit, managed-artifact
/// persistence, or installed-index persistence fails.
pub(crate) async fn console_skills_install_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleSkillInstallRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let artifact_path_raw = payload.artifact_path.trim();
    if artifact_path_raw.is_empty() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "artifact_path cannot be empty",
        )));
    }
    let artifact_path = PathBuf::from(artifact_path_raw);
    let artifact_bytes = fs::read(artifact_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to read artifact {}: {error}",
            artifact_path.display()
        )))
    })?;
    let inspection = inspect_skill_artifact(artifact_bytes.as_slice()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill artifact inspection failed: {error}"
        )))
    })?;

    let skills_root = resolve_skills_root()?;
    fs::create_dir_all(skills_root.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to create skills root {}: {error}",
            skills_root.display()
        )))
    })?;
    let trust_store_path = resolve_skills_trust_store_path(skills_root.as_path());
    let mut trust_store = load_trust_store(trust_store_path.as_path())?;
    let allow_tofu = payload.allow_tofu.unwrap_or(true);
    if payload.allow_untrusted.unwrap_or(false) {
        tracing::warn!(
            artifact_path = %artifact_path.display(),
            "console skill install allow_untrusted override does not bypass security audit"
        );
    }
    let audit_report = audit_skill_artifact_security(
        artifact_bytes.as_slice(),
        &mut trust_store,
        allow_tofu,
        &SkillSecurityAuditPolicy::default(),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill artifact security audit failed: {error}"
        )))
    })?;
    if audit_report.should_quarantine {
        let reason = if audit_report.quarantine_reasons.is_empty() {
            "skill artifact security audit requested quarantine".to_owned()
        } else {
            audit_report.quarantine_reasons.join(" | ")
        };
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill artifact security audit failed: {reason}"
        ))));
    }
    save_trust_store(trust_store_path.as_path(), &trust_store)?;

    let skill_id = inspection.manifest.skill_id.clone();
    let version = inspection.manifest.version.clone();
    let managed_artifact_path =
        managed_skill_artifact_path(skills_root.as_path(), skill_id.as_str(), version.as_str());
    if let Some(parent) = managed_artifact_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to create managed skill directory {}: {error}",
                parent.display()
            )))
        })?;
    }
    let _index_guard = INSTALLED_SKILLS_INDEX_LOCK.lock().map_err(|_| {
        runtime_status_response(tonic::Status::internal("installed skills index lock is poisoned"))
    })?;
    let artifact_sha256 = sha256_hex(artifact_bytes.as_slice());
    if managed_artifact_path.exists() {
        let existing_bytes = fs::read(managed_artifact_path.as_path()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to read existing immutable artifact {}: {error}",
                managed_artifact_path.display()
            )))
        })?;
        if sha256_hex(existing_bytes.as_slice()) != artifact_sha256 {
            return Err(runtime_status_response(tonic::Status::already_exists(format!(
                "immutable skill version {}@{} already has a different artifact",
                skill_id, version
            ))));
        }
    } else {
        write_new_skill_artifact_atomically(
            managed_artifact_path.as_path(),
            artifact_bytes.as_slice(),
        )
        .map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to persist managed artifact {}: {error}",
                managed_artifact_path.display()
            )))
        })?;
    }

    let mut index = load_installed_skills_index(skills_root.as_path())?;
    if index.entries.iter().any(|entry| entry.skill_id == skill_id && entry.version == version) {
        return Err(runtime_status_response(tonic::Status::already_exists(format!(
            "immutable skill version already installed: {}@{}",
            skill_id, version
        ))));
    }
    let previous_current = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == skill_id && entry.current && entry.version != version)
        .cloned();
    let installed_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    for entry in &mut index.entries {
        if entry.skill_id == skill_id && entry.current {
            entry.current = false;
            palyra_skills::mark_skill_version_stale(&mut entry.lifecycle, installed_at_unix_ms);
        }
    }
    let payload_sha256 = audit_report.payload_sha256.clone();
    let trust_decision = trust_decision_label(audit_report.trust_decision);
    let eval_pack_sha256 = sha256_hex(
        serde_json::to_vec(&audit_report)
            .map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to encode skill evaluation evidence: {error}"
                )))
            })?
            .as_slice(),
    );
    let lifecycle = palyra_skills::activate_signed_skill_version(
        artifact_sha256.clone(),
        eval_pack_sha256,
        audit_report.should_quarantine || !audit_report.passed,
        palyra_skills::SkillActivationGate { operator_approved: true, policy_approved: false },
        installed_at_unix_ms,
    )
    .map_err(|decision| {
        runtime_status_response(tonic::Status::failed_precondition(format!(
            "skill activation gate denied: {}",
            decision.reason_codes.join(",")
        )))
    })?;
    let record = InstalledSkillRecord {
        skill_id: skill_id.clone(),
        version: version.clone(),
        publisher: inspection.manifest.publisher.clone(),
        current: true,
        installed_at_unix_ms,
        artifact_sha256,
        payload_sha256: payload_sha256.clone(),
        signature_key_id: inspection.signature.key_id.clone(),
        trust_decision: trust_decision.clone(),
        source: InstalledSkillSource {
            kind: "managed_artifact".to_owned(),
            reference: artifact_path.to_string_lossy().into_owned(),
        },
        missing_secrets: Vec::new(),
        security_scan: InstalledSkillSecuritySnapshot {
            schema_version: 1,
            accepted: audit_report.accepted,
            passed: audit_report.passed,
            should_quarantine: audit_report.should_quarantine,
            generated_at_unix_ms: audit_report.generated_at_unix_ms,
            payload_sha256: payload_sha256.clone(),
            trust_decision,
            check_count: audit_report.checks.len(),
            failed_checks: audit_report
                .checks
                .iter()
                .filter(|check| check.status == SkillAuditCheckStatus::Fail)
                .map(|check| check.check_id.clone())
                .collect(),
            warning_checks: audit_report
                .checks
                .iter()
                .filter(|check| check.status == SkillAuditCheckStatus::Warn)
                .map(|check| check.check_id.clone())
                .collect(),
            quarantine_reasons: audit_report.quarantine_reasons.clone(),
            policy: audit_report.policy.clone(),
        },
        rollback_snapshot: previous_current.as_ref().map(|entry| InstalledSkillRollbackSnapshot {
            schema_version: 1,
            previous_version: entry.version.clone(),
            previous_artifact_sha256: entry.artifact_sha256.clone(),
            previous_payload_sha256: entry.payload_sha256.clone(),
            captured_at_unix_ms: installed_at_unix_ms,
        }),
        lifecycle,
        usage: SkillUsageTelemetry::default(),
    };
    index.entries.push(record.clone());
    save_installed_skills_index(skills_root.as_path(), &index)?;
    Ok(Json(json!({
        "installed": true,
        "record": record,
        "skills_root": skills_root,
        "trust_store": trust_store_path,
    })))
}

/// Verifies a managed skill artifact and updates its trust metadata.
///
/// # Errors
/// Returns an error response when session authorization, skill lookup, artifact
/// IO, trust-store handling, verification, or index persistence fails.
pub(crate) async fn console_skills_verify_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<ConsoleSkillActionRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let skills_root = resolve_skills_root()?;
    let _index_guard = INSTALLED_SKILLS_INDEX_LOCK.lock().map_err(|_| {
        runtime_status_response(tonic::Status::internal("installed skills index lock is poisoned"))
    })?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    let version = resolve_installed_skill_version_any_state(
        &index,
        skill_id.as_str(),
        payload.version.as_deref(),
    )?
    .version
    .clone();
    let artifact_path =
        managed_skill_artifact_path(skills_root.as_path(), skill_id.as_str(), version.as_str());
    let artifact_bytes = fs::read(artifact_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to read managed artifact {}: {error}",
            artifact_path.display()
        )))
    })?;
    let artifact_sha256 = sha256_hex(artifact_bytes.as_slice());
    let expected_artifact_sha256 = index
        .entries
        .iter()
        .find(|entry| entry.skill_id == skill_id && entry.version == version)
        .map(|entry| entry.artifact_sha256.as_str())
        .unwrap_or_default();
    if artifact_sha256 != expected_artifact_sha256 {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "skill.lifecycle.immutable_artifact_mismatch",
        )));
    }

    let trust_store_path = resolve_skills_trust_store_path(skills_root.as_path());
    let mut trust_store = load_trust_store(trust_store_path.as_path())?;
    let report = verify_skill_artifact(
        artifact_bytes.as_slice(),
        &mut trust_store,
        payload.allow_tofu.unwrap_or(false),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill verification failed: {error}"
        )))
    })?;
    save_trust_store(trust_store_path.as_path(), &trust_store)?;
    if let Some(entry) = index
        .entries
        .iter_mut()
        .find(|entry| entry.skill_id == skill_id && entry.version == version)
    {
        entry.payload_sha256 = report.payload_sha256.clone();
        entry.publisher = report.manifest.publisher.clone();
        entry.trust_decision = trust_decision_label(report.trust_decision);
        entry.lifecycle.artifact_signed = true;
        entry.lifecycle.artifact_sha256 = artifact_sha256;
    }
    save_installed_skills_index(skills_root.as_path(), &index)?;
    Ok(Json(json!({ "report": report })))
}

/// Runs a security audit for a managed skill artifact.
///
/// # Errors
/// Returns an error response when session authorization, skill lookup, artifact
/// IO, trust-store handling, security audit, quarantine status update, or event
/// recording fails.
pub(crate) async fn console_skills_audit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<ConsoleSkillActionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let skills_root = resolve_skills_root()?;
    let (version, expected_artifact_sha256) = {
        let _index_guard = INSTALLED_SKILLS_INDEX_LOCK.lock().map_err(|_| {
            runtime_status_response(tonic::Status::internal(
                "installed skills index lock is poisoned",
            ))
        })?;
        let index = load_installed_skills_index(skills_root.as_path())?;
        let record = resolve_installed_skill_version_any_state(
            &index,
            skill_id.as_str(),
            payload.version.as_deref(),
        )?;
        (record.version.clone(), record.artifact_sha256.clone())
    };
    let artifact_path =
        managed_skill_artifact_path(skills_root.as_path(), skill_id.as_str(), version.as_str());
    let artifact_bytes = fs::read(artifact_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to read managed artifact {}: {error}",
            artifact_path.display()
        )))
    })?;
    if sha256_hex(artifact_bytes.as_slice()) != expected_artifact_sha256 {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "skill.lifecycle.immutable_artifact_mismatch",
        )));
    }

    let trust_store_path = resolve_skills_trust_store_path(skills_root.as_path());
    let mut trust_store = load_trust_store(trust_store_path.as_path())?;
    let report = audit_skill_artifact_security(
        artifact_bytes.as_slice(),
        &mut trust_store,
        payload.allow_tofu.unwrap_or(false),
        &SkillSecurityAuditPolicy::default(),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "skill security audit failed: {error}"
        )))
    })?;
    save_trust_store(trust_store_path.as_path(), &trust_store)?;
    let eval_pack_sha256 = sha256_hex(
        serde_json::to_vec(&report)
            .map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to encode skill evaluation evidence: {error}"
                )))
            })?
            .as_slice(),
    );
    {
        let _index_guard = INSTALLED_SKILLS_INDEX_LOCK.lock().map_err(|_| {
            runtime_status_response(tonic::Status::internal(
                "installed skills index lock is poisoned",
            ))
        })?;
        let mut index = load_installed_skills_index(skills_root.as_path())?;
        let entry = index
            .entries
            .iter_mut()
            .find(|entry| entry.skill_id == skill_id && entry.version == version)
            .ok_or_else(|| {
                runtime_status_response(tonic::Status::not_found(format!(
                    "installed skill not found: {skill_id}@{version}"
                )))
            })?;
        entry.security_scan = installed_skill_security_snapshot(&report);
        palyra_skills::record_skill_evaluation(
            &mut entry.lifecycle,
            report.passed && report.accepted,
            Some(eval_pack_sha256),
            report.should_quarantine || !report.passed,
            report.generated_at_unix_ms,
        );
        if !entry.lifecycle.state.is_executable() {
            entry.current = false;
        }
        save_installed_skills_index(skills_root.as_path(), &index)?;
    }

    let quarantined = if report.should_quarantine && payload.quarantine_on_fail.unwrap_or(true) {
        let record = state
            .runtime
            .upsert_skill_status(SkillStatusUpsertRequest {
                skill_id: report.skill_id.clone(),
                version: report.version.clone(),
                status: SkillExecutionStatus::Quarantined,
                reason: Some(format!("console_audit: {}", report.quarantine_reasons.join(" | "))),
                detected_at_ms: unix_ms_now().map_err(|error| {
                    runtime_status_response(tonic::Status::internal(format!(
                        "failed to read system clock: {error}"
                    )))
                })?,
                operator_principal: session.context.principal.clone(),
            })
            .await
            .map_err(runtime_status_response)?;
        state
            .runtime
            .record_skill_status_event(&session.context, "skill.quarantined", &record)
            .await
            .map_err(runtime_status_response)?;
        true
    } else {
        false
    };
    Ok(Json(json!({
        "report": report,
        "quarantined": quarantined,
    })))
}

fn installed_skill_security_snapshot(
    report: &palyra_skills::SkillSecurityAuditReport,
) -> InstalledSkillSecuritySnapshot {
    InstalledSkillSecuritySnapshot {
        schema_version: 1,
        accepted: report.accepted,
        passed: report.passed,
        should_quarantine: report.should_quarantine,
        generated_at_unix_ms: report.generated_at_unix_ms,
        payload_sha256: report.payload_sha256.clone(),
        trust_decision: trust_decision_label(report.trust_decision),
        check_count: report.checks.len(),
        failed_checks: report
            .checks
            .iter()
            .filter(|check| check.status == SkillAuditCheckStatus::Fail)
            .map(|check| check.check_id.clone())
            .collect(),
        warning_checks: report
            .checks
            .iter()
            .filter(|check| check.status == SkillAuditCheckStatus::Warn)
            .map(|check| check.check_id.clone())
            .collect(),
        quarantine_reasons: report.quarantine_reasons.clone(),
        policy: report.policy.clone(),
    }
}

/// Marks a skill version as quarantined.
///
/// # Errors
/// Returns an error response when session authorization, input normalization,
/// status upsert, or event recording fails.
pub(crate) async fn console_skill_quarantine_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<SkillStatusRequest>,
) -> Result<Json<SkillStatusResponse>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let version = normalize_non_empty_field(payload.version, "version")?;
    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id,
            version,
            status: SkillExecutionStatus::Quarantined,
            reason: payload.reason.and_then(trim_to_option),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: session.context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&session.context, "skill.quarantined", &record)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(skill_status_response(record)))
}

/// Enables a quarantined skill version after explicit override.
///
/// # Errors
/// Returns an error response when session authorization, override validation,
/// input normalization, status upsert, or event recording fails.
pub(crate) async fn console_skill_enable_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<SkillStatusRequest>,
) -> Result<Json<SkillStatusResponse>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    if !payload.override_enabled.unwrap_or(false) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "enable requires explicit override=true acknowledgment",
        )));
    }
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let version = normalize_non_empty_field(payload.version, "version")?;
    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id,
            version,
            status: SkillExecutionStatus::Active,
            reason: payload.reason.and_then(trim_to_option),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: session.context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&session.context, "skill.enabled", &record)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(skill_status_response(record)))
}

/// Applies an operator-reviewed lifecycle action to one immutable skill version.
///
/// # Errors
/// Returns an error response when authorization, lifecycle invariants, active
/// pointer validation, rollback evidence, or atomic index persistence fails.
pub(crate) async fn console_skill_lifecycle_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<ConsoleSkillLifecycleRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let version = normalize_non_empty_field(payload.version, "version")?;
    let action = normalize_non_empty_field(payload.action, "action")?.to_ascii_lowercase();
    let reason = payload.reason.and_then(trim_to_option);
    let now_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    let skills_root = resolve_skills_root()?;
    let record = {
        let _guard = INSTALLED_SKILLS_INDEX_LOCK.lock().map_err(|_| {
            runtime_status_response(tonic::Status::internal(
                "installed skills index lock is poisoned",
            ))
        })?;
        let mut index = load_installed_skills_index(skills_root.as_path())?;
        let record = apply_installed_skill_lifecycle_action(
            &mut index,
            skill_id.as_str(),
            version.as_str(),
            action.as_str(),
            payload.operator_approved.unwrap_or(false),
            now_unix_ms,
        )?;
        save_installed_skills_index(skills_root.as_path(), &index)?;
        record
    };
    let lifecycle_reason_code = format!("skill.lifecycle.{action}_completed");
    let trace_recorded = match state
        .runtime
        .record_console_event(
            &session.context,
            "skill.lifecycle.transition",
            json!({
                "skill_id": record.skill_id,
                "version": record.version,
                "action": action,
                "state": record.lifecycle.state.as_str(),
                "reason_code": lifecycle_reason_code,
                "operator_reason_present": reason.is_some(),
                "operator_reason_sha256": reason
                    .as_deref()
                    .map(|value| sha256_hex(value.as_bytes())),
                "artifact_sha256": record.artifact_sha256,
                "payload_sha256": record.payload_sha256,
                "rollback_count": record.lifecycle.rollback_count,
            }),
        )
        .await
    {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                error = %error,
                skill_id = %skill_id,
                version = %version,
                action = %action,
                "failed to record skill lifecycle console event"
            );
            false
        }
    };

    Ok(Json(json!({
        "record": record,
        "action": action,
        "reason": reason,
        "operator_principal_hash": sha256_hex(session.context.principal.as_bytes()),
        "diagnostics": {
            "reason_code": lifecycle_reason_code,
            "artifact_immutable": true,
            "activation_authority": "operator_or_host_policy_only",
            "model_activation_allowed": false,
            "trace_recorded": trace_recorded,
        }
    })))
}

#[allow(clippy::result_large_err)]
fn apply_installed_skill_lifecycle_action(
    index: &mut InstalledSkillsIndex,
    skill_id: &str,
    version: &str,
    action: &str,
    operator_approved: bool,
    now_unix_ms: i64,
) -> Result<InstalledSkillRecord, Response> {
    let target_index = index
        .entries
        .iter()
        .position(|entry| entry.skill_id == skill_id && entry.version == version)
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found(format!(
                "installed skill not found: {skill_id}@{version}"
            )))
        })?;
    match action {
        "pin" => palyra_skills::set_skill_version_pinned(
            &mut index.entries[target_index].lifecycle,
            true,
            now_unix_ms,
        ),
        "unpin" => palyra_skills::set_skill_version_pinned(
            &mut index.entries[target_index].lifecycle,
            false,
            now_unix_ms,
        ),
        "mark_stale" => {
            let entry = &mut index.entries[target_index];
            if !matches!(
                entry.lifecycle.state,
                SkillLifecycleState::Active | SkillLifecycleState::Stale
            ) {
                return Err(lifecycle_conflict("skill.lifecycle.stale_transition_denied"));
            }
            if entry.lifecycle.pinned {
                return Err(lifecycle_conflict("skill.lifecycle.pinned_stale_denied"));
            }
            if !entry.usage.dependent_routine_refs.is_empty() {
                return Err(lifecycle_conflict("skill.lifecycle.referenced_stale_denied"));
            }
            palyra_skills::mark_skill_version_stale(&mut entry.lifecycle, now_unix_ms);
        }
        "archive" => {
            let entry = &mut index.entries[target_index];
            palyra_skills::archive_skill_version(
                &mut entry.lifecycle,
                entry.current,
                entry.usage.dependent_routine_refs.len(),
                now_unix_ms,
            )
            .map_err(lifecycle_conflict)?;
            entry.current = false;
        }
        "restore" => {
            palyra_skills::restore_skill_version(
                &mut index.entries[target_index].lifecycle,
                now_unix_ms,
            )
            .map_err(lifecycle_conflict)?;
            index.entries[target_index].current = false;
        }
        "activate" => {
            if !operator_approved {
                return Err(lifecycle_conflict("skill.lifecycle.operator_ack_required"));
            }
            if index.entries[target_index].current {
                return Err(lifecycle_conflict("skill.lifecycle.already_active"));
            }
            let previous_index =
                index.entries.iter().position(|entry| entry.skill_id == skill_id && entry.current);
            let rollback_snapshot =
                previous_index.map(|previous_entry_index| InstalledSkillRollbackSnapshot {
                    schema_version: 1,
                    previous_version: index.entries[previous_entry_index].version.clone(),
                    previous_artifact_sha256: index.entries[previous_entry_index]
                        .artifact_sha256
                        .clone(),
                    previous_payload_sha256: index.entries[previous_entry_index]
                        .payload_sha256
                        .clone(),
                    captured_at_unix_ms: now_unix_ms,
                });
            palyra_skills::activate_existing_skill_version(
                &mut index.entries[target_index].lifecycle,
                palyra_skills::SkillActivationGate {
                    operator_approved: true,
                    policy_approved: false,
                },
                now_unix_ms,
            )
            .map_err(|decision| lifecycle_conflict(decision.reason_codes.join(",").as_str()))?;
            if let Some(previous_index) = previous_index {
                index.entries[previous_index].current = false;
                palyra_skills::mark_skill_version_stale(
                    &mut index.entries[previous_index].lifecycle,
                    now_unix_ms,
                );
            }
            index.entries[target_index].current = true;
            index.entries[target_index].rollback_snapshot = rollback_snapshot;
        }
        "rollback" => {
            if !operator_approved {
                return Err(lifecycle_conflict("skill.lifecycle.operator_ack_required"));
            }
            if !index.entries[target_index].current {
                return Err(lifecycle_conflict("skill.lifecycle.rollback_requires_current"));
            }
            let snapshot = index.entries[target_index]
                .rollback_snapshot
                .clone()
                .ok_or_else(|| lifecycle_conflict("skill.lifecycle.rollback_snapshot_missing"))?;
            let previous_index = index
                .entries
                .iter()
                .position(|entry| {
                    entry.skill_id == skill_id && entry.version == snapshot.previous_version
                })
                .ok_or_else(|| {
                    lifecycle_conflict("skill.lifecycle.rollback_previous_version_missing")
                })?;
            let previous = &index.entries[previous_index];
            if previous.artifact_sha256 != snapshot.previous_artifact_sha256
                || previous.payload_sha256 != snapshot.previous_payload_sha256
            {
                return Err(lifecycle_conflict("skill.lifecycle.rollback_evidence_mismatch"));
            }
            palyra_skills::activate_existing_skill_version(
                &mut index.entries[previous_index].lifecycle,
                palyra_skills::SkillActivationGate {
                    operator_approved: true,
                    policy_approved: false,
                },
                now_unix_ms,
            )
            .map_err(|decision| lifecycle_conflict(decision.reason_codes.join(",").as_str()))?;
            index.entries[previous_index].current = true;
            index.entries[previous_index].rollback_snapshot = None;
            index.entries[target_index].current = false;
            index.entries[target_index].rollback_snapshot = None;
            palyra_skills::mark_skill_version_rolled_back(
                &mut index.entries[target_index].lifecycle,
                now_unix_ms,
            );
        }
        _ => {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "action must be pin, unpin, mark_stale, archive, restore, activate, or rollback",
            )));
        }
    }
    normalize_installed_skills_index(index);
    index
        .entries
        .iter()
        .find(|entry| entry.skill_id == skill_id && entry.version == version)
        .cloned()
        .ok_or_else(|| lifecycle_conflict("skill.lifecycle.target_lost_after_normalization"))
}

fn lifecycle_conflict(reason_code: &str) -> Response {
    runtime_status_response(tonic::Status::failed_precondition(reason_code.to_owned()))
}

/// Promotes a reviewed procedure learning candidate to a skill scaffold.
///
/// # Errors
/// Returns an error response when session authorization, candidate loading,
/// promotability checks, scaffold writing, status upsert, event recording,
/// candidate-index persistence, or review update fails.
pub(crate) async fn console_procedure_skill_promote_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(candidate_id): Path<String>,
    Json(payload): Json<ConsoleProcedureSkillPromotionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let candidate =
        load_console_procedure_candidate(&state, &session.context, candidate_id.as_str()).await?;
    if candidate.candidate_kind != "procedure" {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "only procedure learning candidates can be promoted to skill scaffolds",
        )));
    }
    if !procedure_candidate_status_is_promotable(candidate.status.as_str()) {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "candidate is not promotable in its current review state",
        )));
    }

    let default_skill_id = default_generated_skill_id(candidate.candidate_id.as_str());
    let skill_id = normalize_generated_skill_identifier(
        payload.skill_id.as_deref().unwrap_or(default_skill_id.as_str()),
        "skill_id",
    )?;
    let version = normalize_generated_skill_version(payload.version.as_deref().unwrap_or("0.1.0"))?;
    let publisher = normalize_generated_skill_identifier(
        payload.publisher.as_deref().unwrap_or("palyra.generated"),
        "publisher",
    )?;
    let name = payload
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| candidate.title.clone());

    let scaffold = write_skill_builder_scaffold(
        &BuilderSource::Procedure(Box::new(candidate.clone())),
        SkillBuilderScaffoldRequest {
            skill_id: skill_id.clone(),
            version: version.clone(),
            publisher: publisher.clone(),
            name: name.clone(),
            tool_id: None,
            tool_name: "Run promoted procedure".to_owned(),
            tool_description: candidate.summary.clone(),
            review_notes: Some("Promoted from reusable procedure candidate.".to_owned()),
            capabilities: None,
        },
    )?;
    let record = state
        .runtime
        .upsert_skill_status(SkillStatusUpsertRequest {
            skill_id: skill_id.clone(),
            version: version.clone(),
            status: SkillExecutionStatus::Quarantined,
            reason: Some(format!("generated_from_learning_candidate:{}", candidate.candidate_id)),
            detected_at_ms: unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to read system clock: {error}"
                )))
            })?,
            operator_principal: session.context.principal.clone(),
        })
        .await
        .map_err(runtime_status_response)?;
    state
        .runtime
        .record_skill_status_event(&session.context, "skill.scaffolded", &record)
        .await
        .map_err(runtime_status_response)?;

    let skills_root = resolve_skills_root()?;
    let mut index = load_skill_builder_candidate_index(skills_root.as_path())?;
    index.entries.retain(|entry| entry.candidate_id != scaffold.builder_candidate_id);
    index.entries.push(SkillBuilderCandidateRecord {
        candidate_id: scaffold.builder_candidate_id.clone(),
        skill_id: scaffold.skill_id.clone(),
        version: scaffold.version.clone(),
        publisher: scaffold.publisher.clone(),
        name: scaffold.name.clone(),
        source_kind: scaffold.source_kind.clone(),
        source_ref: scaffold.source_ref.clone(),
        summary: scaffold.summary.clone(),
        status: "quarantined".to_owned(),
        rollout_flag: DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV.to_owned(),
        rollout_enabled: dynamic_tool_builder_rollout(&state).enabled,
        scaffold_root: scaffold.scaffold_root.clone(),
        manifest_path: scaffold.manifest_path.clone(),
        capability_declaration_path: scaffold.capability_declaration_path.clone(),
        provenance_path: scaffold.provenance_path.clone(),
        test_harness_path: scaffold.test_harness_path.clone(),
        artifact_plan_path: Some(scaffold.artifact_plan_path.clone()),
        eval_outcome_path: Some(scaffold.eval_outcome_path.clone()),
        artifact_status: scaffold.artifact_status.clone(),
        eval_status: scaffold.eval_status.clone(),
        quarantine_reason: scaffold.quarantine_reason.clone(),
        reproducibility_key: Some(scaffold.reproducibility_key.clone()),
        capability_profile: scaffold.capability_profile.clone(),
        generated_at_unix_ms: scaffold.generated_at_unix_ms,
        updated_at_unix_ms: scaffold.generated_at_unix_ms,
    });
    save_skill_builder_candidate_index(skills_root.as_path(), &index)?;

    if payload.accept_candidate.unwrap_or(true) {
        state
            .runtime
            .review_learning_candidate(LearningCandidateReviewRequest {
                candidate_id: candidate.candidate_id.clone(),
                status: "accepted".to_owned(),
                reviewed_by_principal: session.context.principal.clone(),
                action_summary: Some(format!("promoted to scaffold {}", scaffold.skill_id)),
                action_payload_json: Some(
                    json!({
                        "action": "promote_to_skill_scaffold",
                        "skill_id": scaffold.skill_id,
                        "version": scaffold.version,
                        "scaffold_root": scaffold.scaffold_root,
                        "builder_candidate_id": scaffold.builder_candidate_id,
                    })
                    .to_string(),
                ),
            })
            .await
            .map_err(runtime_status_response)?;
    }

    Ok(Json(json!({
        "candidate": candidate,
        "skill": {
            "skill_id": scaffold.skill_id,
            "version": scaffold.version,
            "publisher": scaffold.publisher,
            "name": scaffold.name,
            "scaffold_root": scaffold.scaffold_root,
            "files": scaffold.files,
            "quarantine_status": skill_status_response(record),
        },
        "builder_candidate": index.entries.iter().find(|entry| entry.candidate_id == scaffold.builder_candidate_id).cloned(),
    })))
}

#[derive(Debug, Clone)]
enum BuilderSource {
    Procedure(Box<LearningCandidateRecord>),
    Prompt { prompt: String, source_ref: String },
}

#[derive(Debug, Clone)]
struct SkillBuilderScaffoldRequest {
    skill_id: String,
    version: String,
    publisher: String,
    name: String,
    tool_id: Option<String>,
    tool_name: String,
    tool_description: String,
    review_notes: Option<String>,
    capabilities: Option<ConsoleSkillBuilderCapabilityRequest>,
}

#[derive(Debug)]
struct GeneratedSkillScaffold {
    builder_candidate_id: String,
    skill_id: String,
    version: String,
    publisher: String,
    name: String,
    source_kind: String,
    source_ref: String,
    summary: String,
    scaffold_root: String,
    manifest_path: String,
    capability_declaration_path: String,
    provenance_path: String,
    test_harness_path: String,
    artifact_plan_path: String,
    eval_outcome_path: String,
    artifact_status: String,
    eval_status: String,
    quarantine_reason: String,
    reproducibility_key: String,
    capability_profile: crate::plugins::PluginCapabilityProfile,
    generated_at_unix_ms: i64,
    files: Vec<String>,
}

async fn load_console_procedure_candidate(
    state: &AppState,
    context: &RequestContext,
    candidate_id: &str,
) -> Result<LearningCandidateRecord, Response> {
    state
        .runtime
        .list_learning_candidates(LearningCandidateListFilter {
            candidate_id: Some(candidate_id.to_owned()),
            owner_principal: Some(context.principal.clone()),
            device_id: None,
            channel: context.channel.clone(),
            session_id: None,
            scope_kind: None,
            scope_id: None,
            candidate_kind: None,
            status: None,
            risk_level: None,
            source_task_id: None,
            min_confidence: None,
            max_confidence: None,
            limit: 1,
        })
        .await
        .map_err(runtime_status_response)?
        .into_iter()
        .next()
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::not_found("learning candidate not found"))
        })
}

fn default_generated_skill_id(candidate_id: &str) -> String {
    format!("palyra.generated.procedure.{}", candidate_id.to_ascii_lowercase())
}

fn procedure_candidate_status_is_promotable(status: &str) -> bool {
    !matches!(status.trim(), "denied" | "rejected" | "suppressed")
}

fn dynamic_tool_builder_rollout(state: &AppState) -> FeatureRolloutSetting {
    state.runtime.config.feature_rollouts.dynamic_tool_builder
}

#[allow(clippy::result_large_err)]
fn normalize_generated_skill_identifier(raw: &str, field: &str) -> Result<String, Response> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.contains("..")
        || !normalized.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')
        })
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "{field} must use non-empty lowercase [a-z0-9._-] segments"
        ))));
    }
    Ok(normalized)
}

// HTTP handler helpers return `Response` directly so callers preserve the
// structured status body through `?`.
#[allow(clippy::result_large_err)]
fn normalize_generated_skill_version(raw: &str) -> Result<String, Response> {
    let normalized = raw.trim();
    let mut parts = normalized.split('.');
    let valid = (0..3)
        .all(|_| parts.next().is_some_and(|part| !part.is_empty() && part.parse::<u32>().is_ok()))
        && parts.next().is_none();
    if !valid {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "version must use numeric major.minor.patch form",
        )));
    }
    Ok(normalized.to_owned())
}

/// Loads the generated skill-builder candidate index.
///
/// # Errors
/// Returns an error response when the index file cannot be read or parsed.
#[allow(clippy::result_large_err)]
pub(crate) fn load_skill_builder_candidate_index(
    skills_root: &FsPath,
) -> Result<SkillBuilderCandidateIndex, Response> {
    let path = skill_builder_candidates_index_path(skills_root);
    if !path.exists() {
        return Ok(SkillBuilderCandidateIndex::default());
    }
    let bytes = fs::read(path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read skill builder candidate index {}: {error}",
            path.display()
        )))
    })?;
    let mut index = parse_versioned_json::<SkillBuilderCandidateIndex>(
        bytes.as_slice(),
        SKILL_BUILDER_CANDIDATE_INDEX_FORMAT,
        &[(0, migrate_updated_at_metadata_v0_to_v1)],
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to parse skill builder candidate index {}: {error}",
            path.display()
        )))
    })?;
    index.entries.sort_by_key(|left| left.generated_at_unix_ms);
    Ok(index)
}

#[allow(clippy::result_large_err)]
fn save_skill_builder_candidate_index(
    skills_root: &FsPath,
    index: &SkillBuilderCandidateIndex,
) -> Result<(), Response> {
    let root = skill_builder_candidates_root(skills_root);
    fs::create_dir_all(root.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to create skill builder candidates root {}: {error}",
            root.display()
        )))
    })?;
    let mut normalized = index.clone();
    normalized.schema_version = SKILL_BUILDER_CANDIDATE_LAYOUT_VERSION;
    normalized.updated_at_unix_ms = unix_ms_now().map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to read system clock: {error}"
        )))
    })?;
    normalized.entries.sort_by_key(|left| left.generated_at_unix_ms);
    let path = skill_builder_candidates_index_path(skills_root);
    let payload = serde_json::to_vec_pretty(&normalized).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize skill builder candidate index: {error}"
        )))
    })?;
    fs::write(path.as_path(), payload).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to write skill builder candidate index {}: {error}",
            path.display()
        )))
    })
}

fn skill_builder_candidates_root(skills_root: &FsPath) -> PathBuf {
    skills_root.join("builder-candidates")
}

fn skill_builder_candidates_index_path(skills_root: &FsPath) -> PathBuf {
    skill_builder_candidates_root(skills_root).join("index.json")
}

#[allow(clippy::result_large_err)]
fn write_skill_builder_scaffold(
    source: &BuilderSource,
    mut request: SkillBuilderScaffoldRequest,
) -> Result<GeneratedSkillScaffold, Response> {
    request.skill_id = normalize_generated_skill_identifier(request.skill_id.as_str(), "skill_id")?;
    request.publisher =
        normalize_generated_skill_identifier(request.publisher.as_str(), "publisher")?;
    request.version = normalize_generated_skill_version(request.version.as_str())?;
    let skills_root = resolve_skills_root()?;
    let scaffold_root = skill_builder_candidates_root(skills_root.as_path())
        .join(request.skill_id.as_str())
        .join(request.version.as_str());
    let root_existed = scaffold_root.exists();
    fs::create_dir_all(scaffold_root.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to create scaffold root {}: {error}",
            scaffold_root.display()
        )))
    })?;
    let generated_at_unix_ms = current_unix_ms();

    let manifest = build_builder_skill_manifest(source, &request);
    let manifest_toml = toml::to_string_pretty(&manifest).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize scaffold manifest: {error}"
        )))
    })?;
    palyra_skills::parse_manifest_toml(manifest_toml.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::failed_precondition(format!(
            "generated scaffold manifest failed validation: {error}"
        )))
    })?;

    let readme =
        build_builder_skill_readme(source, request.skill_id.as_str(), request.version.as_str());
    let request_payload = build_builder_request_payload(source, &request, generated_at_unix_ms);
    let request_json_bytes = serde_json::to_vec_pretty(&request_payload).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode builder request JSON: {error}"
        )))
    })?;
    let capability_profile = crate::plugins::plugin_capability_profile_from_manifest(&manifest);
    let capability_json_bytes = serde_json::to_vec_pretty(&json!({
        "declared_from_manifest": true,
        "profile": capability_profile,
        "requires_review": palyra_skills::builder_manifest_requires_review(&manifest),
    }))
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode builder capability declaration: {error}"
        )))
    })?;
    let tool = manifest.entrypoints.tools.first().cloned().ok_or_else(|| {
        runtime_status_response(tonic::Status::failed_precondition(
            "generated builder manifest must define at least one tool",
        ))
    })?;
    let test_harness = crate::wasm_plugin_runner::build_manifest_test_harness(&manifest, &tool);
    let test_harness_bytes = serde_json::to_vec_pretty(&test_harness).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode builder test harness: {error}"
        )))
    })?;
    let sbom_bytes = serde_json::to_vec_pretty(&json!({
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "components": [
            {
                "type": "application",
                "name": request.skill_id,
                "version": request.version,
                "publisher": request.publisher,
            }
        ],
    }))
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode scaffold SBOM: {error}"
        )))
    })?;
    let provenance_bytes = serde_json::to_vec_pretty(&json!({
        "builder": {
            "id": "palyra.console.learning_skill_promotion",
            "version": build_metadata().version,
        },
        "buildType": "palyra.skill_scaffold/v1",
        "subject": [
            {
                "name": "skill.toml",
                "digest": { "sha256": sha256_hex(manifest_toml.as_bytes()) },
            },
            {
                "name": "README.md",
                "digest": { "sha256": sha256_hex(readme.as_bytes()) },
            },
            {
                "name": "builder-request.json",
                "digest": { "sha256": sha256_hex(request_json_bytes.as_slice()) },
            },
            {
                "name": "builder-capabilities.json",
                "digest": { "sha256": sha256_hex(capability_json_bytes.as_slice()) },
            },
            {
                "name": "tests/smoke.test.json",
                "digest": { "sha256": sha256_hex(test_harness_bytes.as_slice()) },
            },
            {
                "name": "sbom.cdx.json",
                "digest": { "sha256": sha256_hex(sbom_bytes.as_slice()) },
            }
        ],
        "metadata": {
            "source_kind": builder_source_kind(source),
            "source_ref": builder_source_ref(source),
            "summary": builder_source_summary(source),
        }
    }))
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode scaffold provenance: {error}"
        )))
    })?;
    let package_id = palyra_skills::skill_extension_package_id(
        request.skill_id.as_str(),
        request.version.as_str(),
    );
    let reproducibility_key = sha256_hex(
        format!(
            "{}:{}:{}:{}:{}",
            sha256_hex(manifest_toml.as_bytes()),
            sha256_hex(capability_json_bytes.as_slice()),
            sha256_hex(test_harness_bytes.as_slice()),
            sha256_hex(sbom_bytes.as_slice()),
            builder_source_ref(source),
        )
        .as_bytes(),
    );
    let artifact_plan = palyra_skills::skill_scaffold_artifact_plan(
        package_id.clone(),
        "skill.toml",
        "builder-capabilities.json",
        "provenance.json",
        "tests/smoke.test.json",
        reproducibility_key.clone(),
    );
    let artifact_plan_bytes = serde_json::to_vec_pretty(&artifact_plan).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode scaffold artifact plan: {error}"
        )))
    })?;
    let eval_record = palyra_skills::skill_eval_run_record(
        format!("pending:{package_id}"),
        package_id,
        Vec::new(),
        vec!["generated_skill_smoke".to_owned()],
        generated_at_unix_ms,
    );
    let eval_outcome_bytes = serde_json::to_vec_pretty(&eval_record).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode scaffold eval outcome: {error}"
        )))
    })?;

    let files = [
        ("skill.toml", manifest_toml.into_bytes()),
        ("README.md", readme.into_bytes()),
        ("builder-request.json", request_json_bytes),
        ("builder-capabilities.json", capability_json_bytes),
        ("tests/smoke.test.json", test_harness_bytes),
        ("artifact-plan.json", artifact_plan_bytes),
        ("tests/eval-outcome.json", eval_outcome_bytes),
        ("sbom.cdx.json", sbom_bytes),
        ("provenance.json", provenance_bytes),
    ];
    let mut written_files = Vec::new();
    for (relative_path, bytes) in files {
        let target = scaffold_root.join(relative_path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to create scaffold parent {}: {error}",
                    parent.display()
                )))
            })?;
        }
        if let Err(error) = fs::write(target.as_path(), bytes) {
            if !root_existed {
                let _ = fs::remove_dir_all(scaffold_root.as_path());
            }
            return Err(runtime_status_response(tonic::Status::internal(format!(
                "failed to write scaffold file {}: {error}",
                target.display()
            ))));
        }
        written_files.push(target.to_string_lossy().into_owned());
    }

    let builder_candidate_id = Ulid::generate().to_string().to_ascii_lowercase();
    Ok(GeneratedSkillScaffold {
        builder_candidate_id,
        skill_id: request.skill_id,
        version: request.version,
        publisher: request.publisher,
        name: request.name,
        source_kind: builder_source_kind(source).to_owned(),
        source_ref: builder_source_ref(source),
        summary: builder_source_summary(source),
        scaffold_root: scaffold_root.to_string_lossy().into_owned(),
        manifest_path: scaffold_root.join("skill.toml").to_string_lossy().into_owned(),
        capability_declaration_path: scaffold_root
            .join("builder-capabilities.json")
            .to_string_lossy()
            .into_owned(),
        provenance_path: scaffold_root.join("provenance.json").to_string_lossy().into_owned(),
        test_harness_path: scaffold_root
            .join("tests/smoke.test.json")
            .to_string_lossy()
            .into_owned(),
        artifact_plan_path: scaffold_root.join("artifact-plan.json").to_string_lossy().into_owned(),
        eval_outcome_path: scaffold_root
            .join("tests/eval-outcome.json")
            .to_string_lossy()
            .into_owned(),
        artifact_status: artifact_plan.artifact_status,
        eval_status: eval_record.status,
        quarantine_reason:
            "generated skill remains quarantined until signed artifact, eval, and review pass"
                .to_owned(),
        reproducibility_key,
        capability_profile,
        generated_at_unix_ms,
        files: written_files,
    })
}

fn build_builder_skill_manifest(
    source: &BuilderSource,
    request: &SkillBuilderScaffoldRequest,
) -> SkillManifest {
    let tool_id = request
        .tool_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{}.run", request.publisher));
    let capability_request =
        request.capabilities.clone().unwrap_or(ConsoleSkillBuilderCapabilityRequest {
            http_hosts: Vec::new(),
            secrets: Vec::new(),
            storage_prefixes: Vec::new(),
            channels: Vec::new(),
        });
    SkillManifest {
        manifest_version: SKILL_MANIFEST_VERSION,
        skill_id: request.skill_id.clone(),
        name: request.name.clone(),
        version: request.version.clone(),
        publisher: request.publisher.clone(),
        entrypoints: SkillEntrypoints {
            tools: vec![SkillToolEntrypoint {
                id: tool_id,
                name: request.tool_name.clone(),
                description: request.tool_description.clone(),
                input_schema: json!({
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "args": {
                            "type": "object"
                        }
                    }
                }),
                output_schema: json!({
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "result": { "type": "string" }
                    }
                }),
                risk: SkillToolRisk { default_sensitive: false, requires_approval: true },
            }],
        },
        capabilities: SkillCapabilities {
            filesystem: SkillFilesystemCapabilities {
                read_roots: Vec::new(),
                write_roots: capability_request.storage_prefixes.clone(),
            },
            http_egress_allowlist: capability_request.http_hosts.clone(),
            secrets: capability_request
                .secrets
                .iter()
                .map(|key| palyra_skills::SkillSecretScope {
                    scope: format!("skill:{}", request.skill_id),
                    key_names: vec![key.clone()],
                })
                .collect(),
            device_capabilities: Vec::new(),
            node_capabilities: capability_request.channels.clone(),
            quotas: SkillQuotaConfig::default(),
            wildcard_opt_in: Default::default(),
        },
        compat: SkillCompat {
            required_protocol_major: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            min_palyra_version: build_metadata().version.to_owned(),
            max_palyra_version: None,
        },
        integrity: SkillIntegrity::default(),
        builder: Some(palyra_skills::SkillBuilderMetadata {
            experimental: true,
            source_kind: builder_source_kind(source).to_owned(),
            source_ref: builder_source_ref(source),
            rollout_flag: DYNAMIC_TOOL_BUILDER_ROLLOUT_ENV.to_owned(),
            review_status: "quarantined".to_owned(),
            checklist: palyra_skills::SkillBuilderChecklist {
                capability_declaration_path: "builder-capabilities.json".to_owned(),
                provenance_path: "provenance.json".to_owned(),
                test_harness_path: "tests/smoke.test.json".to_owned(),
                review_notes: request.review_notes.clone().unwrap_or_default(),
            },
        }),
        operator: palyra_skills::SkillOperatorMetadata {
            display_name: Some(request.name.clone()),
            summary: Some(request.tool_description.clone()),
            description: request.review_notes.clone(),
            categories: vec!["builder".to_owned()],
            tags: vec!["generated".to_owned(), "quarantined".to_owned()],
            docs_url: None,
            plugin: palyra_skills::SkillPluginMetadata {
                default_tool_id: Some(
                    request
                        .tool_id
                        .as_deref()
                        .filter(|value| !value.trim().is_empty())
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| format!("{}.run", request.publisher)),
                ),
                default_module_path: Some("modules/module.wasm".to_owned()),
                default_entrypoint: Some("run".to_owned()),
                contracts: Vec::new(),
                ..palyra_skills::SkillPluginMetadata::default()
            },
            config: None,
        },
    }
}

fn build_builder_skill_readme(source: &BuilderSource, skill_id: &str, version: &str) -> String {
    format!(
        "# {skill_id}\n\n\
Version: {version}\n\
Source: {source_kind} ({source_ref})\n\n\
## Summary\n\n\
{summary}\n\n\
## Builder posture\n\n\
- Generated by the experimental dynamic tool builder.\n\
- Candidate remains quarantined until an operator packages, signs, verifies, and explicitly enables it.\n\
- Review `builder-request.json`, `builder-capabilities.json`, `tests/smoke.test.json`, and `provenance.json` before turning this scaffold into a signed artifact.\n",
        source_kind = builder_source_kind(source),
        source_ref = builder_source_ref(source),
        summary = builder_source_summary(source),
    )
}

fn build_builder_request_payload(
    source: &BuilderSource,
    request: &SkillBuilderScaffoldRequest,
    generated_at_unix_ms: i64,
) -> Value {
    json!({
        "source_kind": builder_source_kind(source),
        "source_ref": builder_source_ref(source),
        "summary": builder_source_summary(source),
        "prompt": match source {
            BuilderSource::Procedure(candidate) => serde_json::from_str::<Value>(candidate.content_json.as_str()).unwrap_or_else(|_| json!({ "raw": candidate.content_json })),
            BuilderSource::Prompt { prompt, .. } => json!({ "prompt": prompt }),
        },
        "review_notes": request.review_notes,
        "requested_capabilities": request.capabilities,
        "generated_at_unix_ms": generated_at_unix_ms,
    })
}

fn builder_source_kind(source: &BuilderSource) -> &'static str {
    match source {
        BuilderSource::Procedure(_) => "procedure",
        BuilderSource::Prompt { .. } => "prompt",
    }
}

fn builder_source_ref(source: &BuilderSource) -> String {
    match source {
        BuilderSource::Procedure(candidate) => candidate.candidate_id.clone(),
        BuilderSource::Prompt { source_ref, .. } => source_ref.clone(),
    }
}

fn builder_source_summary(source: &BuilderSource) -> String {
    match source {
        BuilderSource::Procedure(candidate) => candidate.summary.clone(),
        BuilderSource::Prompt { prompt, .. } => prompt.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use palyra_skills::{
        activate_signed_skill_version, mark_skill_version_stale, SkillActivationGate,
        SkillLifecycleState, SkillUsageTelemetry,
    };
    use tempfile::tempdir;

    use super::{
        apply_installed_skill_lifecycle_action, load_skill_builder_candidate_index,
        normalize_generated_skill_version, procedure_candidate_status_is_promotable,
        skill_builder_candidates_index_path,
    };
    use crate::{
        InstalledSkillRecord, InstalledSkillRollbackSnapshot, InstalledSkillSecuritySnapshot,
        InstalledSkillSource, InstalledSkillsIndex, SkillSecurityAuditPolicy,
        SKILLS_LAYOUT_VERSION, SKILL_BUILDER_CANDIDATE_LAYOUT_VERSION,
    };

    fn lifecycle_record(version: &str, current: bool) -> InstalledSkillRecord {
        let mut lifecycle = activate_signed_skill_version(
            format!("artifact-{version}"),
            format!("eval-{version}"),
            false,
            SkillActivationGate { operator_approved: true, policy_approved: false },
            10,
        )
        .expect("fixture lifecycle should activate");
        if !current {
            mark_skill_version_stale(&mut lifecycle, 20);
        }
        InstalledSkillRecord {
            skill_id: "acme.lifecycle".to_owned(),
            version: version.to_owned(),
            publisher: "acme".to_owned(),
            current,
            installed_at_unix_ms: 10,
            artifact_sha256: format!("artifact-{version}"),
            payload_sha256: format!("payload-{version}"),
            signature_key_id: "key-1".to_owned(),
            trust_decision: "allowlisted".to_owned(),
            source: InstalledSkillSource {
                kind: "managed_artifact".to_owned(),
                reference: format!("{version}.palyra-skill"),
            },
            missing_secrets: Vec::new(),
            security_scan: InstalledSkillSecuritySnapshot {
                schema_version: 1,
                accepted: true,
                passed: true,
                should_quarantine: false,
                generated_at_unix_ms: 10,
                payload_sha256: format!("payload-{version}"),
                trust_decision: "allowlisted".to_owned(),
                check_count: 1,
                failed_checks: Vec::new(),
                warning_checks: Vec::new(),
                quarantine_reasons: Vec::new(),
                policy: SkillSecurityAuditPolicy::default(),
            },
            rollback_snapshot: None,
            lifecycle,
            usage: SkillUsageTelemetry::default(),
        }
    }

    #[test]
    fn load_skill_builder_candidate_index_migrates_legacy_metadata() {
        let tempdir = tempdir().expect("temporary directory should be created");
        let index_path = skill_builder_candidates_index_path(tempdir.path());
        let parent = index_path.parent().expect("candidate index path should have a parent");
        fs::create_dir_all(parent).expect("candidate index parent should be created");
        fs::write(index_path, br#"{"entries":[]}"#)
            .expect("legacy skill builder candidate index should be written");
        let index = load_skill_builder_candidate_index(tempdir.path())
            .expect("legacy skill builder candidate index should load");
        assert_eq!(index.schema_version, SKILL_BUILDER_CANDIDATE_LAYOUT_VERSION);
        assert_eq!(index.updated_at_unix_ms, 0);
        assert!(index.entries.is_empty());
    }

    #[test]
    fn denied_learning_procedure_status_is_not_promotable() {
        assert!(!procedure_candidate_status_is_promotable("denied"));
        assert!(!procedure_candidate_status_is_promotable(" rejected "));
        assert!(!procedure_candidate_status_is_promotable("suppressed"));
        assert!(procedure_candidate_status_is_promotable("proposed"));
        assert!(procedure_candidate_status_is_promotable("accepted"));
    }

    #[test]
    fn generated_skill_version_rejects_path_syntax_before_scaffolding() {
        for version in ["../escape", "1.0/../../escape", r"C:\escape", ".", "1.2"] {
            assert!(
                normalize_generated_skill_version(version).is_err(),
                "{version:?} must fail closed"
            );
        }
        assert_eq!(
            normalize_generated_skill_version(" 1.2.3 ").expect("semver should normalize"),
            "1.2.3"
        );
    }

    #[test]
    fn lifecycle_action_archives_and_restores_without_activation() {
        let mut index = InstalledSkillsIndex {
            schema_version: SKILLS_LAYOUT_VERSION,
            updated_at_unix_ms: 0,
            entries: vec![lifecycle_record("1.0.0", false)],
        };

        let archived = apply_installed_skill_lifecycle_action(
            &mut index,
            "acme.lifecycle",
            "1.0.0",
            "archive",
            false,
            30,
        )
        .expect("inactive version should archive");
        assert_eq!(archived.lifecycle.state, SkillLifecycleState::Archived);
        assert!(!archived.current);

        let restored = apply_installed_skill_lifecycle_action(
            &mut index,
            "acme.lifecycle",
            "1.0.0",
            "restore",
            false,
            40,
        )
        .expect("archived version should restore");
        assert_eq!(restored.lifecycle.state, SkillLifecycleState::Evaluated);
        assert!(!restored.current, "restore must not silently reactivate code");
    }

    #[test]
    fn lifecycle_rollback_consumes_exact_previous_pointer_once() {
        let previous = lifecycle_record("1.0.0", false);
        let mut current = lifecycle_record("2.0.0", true);
        current.rollback_snapshot = Some(InstalledSkillRollbackSnapshot {
            schema_version: 1,
            previous_version: previous.version.clone(),
            previous_artifact_sha256: previous.artifact_sha256.clone(),
            previous_payload_sha256: previous.payload_sha256.clone(),
            captured_at_unix_ms: 20,
        });
        let mut index = InstalledSkillsIndex {
            schema_version: SKILLS_LAYOUT_VERSION,
            updated_at_unix_ms: 0,
            entries: vec![previous, current],
        };

        let rolled_back = apply_installed_skill_lifecycle_action(
            &mut index,
            "acme.lifecycle",
            "2.0.0",
            "rollback",
            true,
            30,
        )
        .expect("matching immutable rollback evidence should restore the previous pointer");
        assert_eq!(rolled_back.lifecycle.state, SkillLifecycleState::RolledBack);
        assert!(!rolled_back.current);
        assert!(rolled_back.rollback_snapshot.is_none());
        assert!(index.entries.iter().any(|entry| entry.version == "1.0.0" && entry.current));

        let error = apply_installed_skill_lifecycle_action(
            &mut index,
            "acme.lifecycle",
            "2.0.0",
            "rollback",
            true,
            40,
        )
        .expect_err("consumed rollback pointer must not be reusable");
        assert_eq!(error.status(), axum::http::StatusCode::PRECONDITION_FAILED);
    }
}
