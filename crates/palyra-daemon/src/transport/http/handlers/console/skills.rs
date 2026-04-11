use crate::gateway::current_unix_ms;
use crate::journal::{
    LearningCandidateListFilter, LearningCandidateRecord, LearningCandidateReviewRequest,
};
use crate::*;

const DYNAMIC_TOOL_BUILDER_ROLLOUT_FLAG: &str = "PALYRA_EXPERIMENTAL_DYNAMIC_TOOL_BUILDER";

pub(crate) async fn console_skills_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSkillsListQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let skills_root = resolve_skills_root()?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
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

pub(crate) async fn console_skill_builder_candidates_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleSkillBuilderCandidatesQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let skills_root = resolve_skills_root()?;
    let mut index = load_skill_builder_candidate_index(skills_root.as_path())?;
    if let Some(source_kind) =
        query.source_kind.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        index.entries.retain(|entry| entry.source_kind == source_kind);
    }

    Ok(Json(json!({
        "rollout_flag": DYNAMIC_TOOL_BUILDER_ROLLOUT_FLAG,
        "rollout_enabled": dynamic_tool_builder_rollout_enabled(),
        "count": index.entries.len(),
        "entries": index.entries,
        "skills_root": skills_root,
    })))
}

pub(crate) async fn console_skill_builder_candidate_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleSkillBuilderCreateRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    if !dynamic_tool_builder_rollout_enabled() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "dynamic tool builder is disabled; set {DYNAMIC_TOOL_BUILDER_ROLLOUT_FLAG}=true to opt in"
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
            source_ref: format!("prompt:{}", Ulid::new().to_string().to_ascii_lowercase()),
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
    let version = payload.version.unwrap_or_else(|| "0.1.0".to_owned());
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
        rollout_flag: DYNAMIC_TOOL_BUILDER_ROLLOUT_FLAG.to_owned(),
        rollout_enabled: true,
        scaffold_root: scaffold.scaffold_root.clone(),
        manifest_path: scaffold.manifest_path.clone(),
        capability_declaration_path: scaffold.capability_declaration_path.clone(),
        provenance_path: scaffold.provenance_path.clone(),
        test_harness_path: scaffold.test_harness_path.clone(),
        capability_profile: scaffold.capability_profile.clone(),
        generated_at_unix_ms: scaffold.generated_at_unix_ms,
        updated_at_unix_ms: scaffold.generated_at_unix_ms,
    });
    save_skill_builder_candidate_index(skills_root.as_path(), &index)?;

    Ok(Json(json!({
        "rollout_flag": DYNAMIC_TOOL_BUILDER_ROLLOUT_FLAG,
        "rollout_enabled": true,
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
    let verification =
        match verify_skill_artifact(artifact_bytes.as_slice(), &mut trust_store, allow_tofu) {
            Ok(report) => Some(report),
            Err(error) if payload.allow_untrusted.unwrap_or(false) => {
                tracing::warn!(
                    error = %error,
                    artifact_path = %artifact_path.display(),
                    "console skill install proceeding with allow_untrusted override"
                );
                None
            }
            Err(error) => {
                return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
                    "skill artifact verification failed: {error}"
                ))));
            }
        };
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
    fs::write(managed_artifact_path.as_path(), artifact_bytes.as_slice()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to persist managed artifact {}: {error}",
            managed_artifact_path.display()
        )))
    })?;

    let mut index = load_installed_skills_index(skills_root.as_path())?;
    index.entries.retain(|entry| !(entry.skill_id == skill_id && entry.version == version));
    for entry in &mut index.entries {
        if entry.skill_id == skill_id {
            entry.current = false;
        }
    }
    let record = InstalledSkillRecord {
        skill_id: skill_id.clone(),
        version: version.clone(),
        publisher: inspection.manifest.publisher.clone(),
        current: true,
        installed_at_unix_ms: unix_ms_now().map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to read system clock: {error}"
            )))
        })?,
        artifact_sha256: sha256_hex(artifact_bytes.as_slice()),
        payload_sha256: verification
            .as_ref()
            .map(|report| report.payload_sha256.clone())
            .unwrap_or_else(|| inspection.payload_sha256.clone()),
        signature_key_id: inspection.signature.key_id.clone(),
        trust_decision: verification
            .as_ref()
            .map(|report| trust_decision_label(report.trust_decision))
            .unwrap_or_else(|| "untrusted_override".to_owned()),
        source: InstalledSkillSource {
            kind: "managed_artifact".to_owned(),
            reference: artifact_path.to_string_lossy().into_owned(),
        },
        missing_secrets: Vec::new(),
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

pub(crate) async fn console_skills_verify_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<ConsoleSkillActionRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let skills_root = resolve_skills_root()?;
    let mut index = load_installed_skills_index(skills_root.as_path())?;
    let version = resolve_skill_version(&index, skill_id.as_str(), payload.version.as_deref())?;
    let artifact_path =
        managed_skill_artifact_path(skills_root.as_path(), skill_id.as_str(), version.as_str());
    let artifact_bytes = fs::read(artifact_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to read managed artifact {}: {error}",
            artifact_path.display()
        )))
    })?;

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
    }
    save_installed_skills_index(skills_root.as_path(), &index)?;
    Ok(Json(json!({ "report": report })))
}

pub(crate) async fn console_skills_audit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(skill_id): Path<String>,
    Json(payload): Json<ConsoleSkillActionRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let skill_id = normalize_non_empty_field(skill_id, "skill_id")?;
    let skills_root = resolve_skills_root()?;
    let index = load_installed_skills_index(skills_root.as_path())?;
    let version = resolve_skill_version(&index, skill_id.as_str(), payload.version.as_deref())?;
    let artifact_path =
        managed_skill_artifact_path(skills_root.as_path(), skill_id.as_str(), version.as_str());
    let artifact_bytes = fs::read(artifact_path.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(format!(
            "failed to read managed artifact {}: {error}",
            artifact_path.display()
        )))
    })?;

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
    if matches!(candidate.status.as_str(), "rejected" | "suppressed") {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "candidate is not promotable in its current review state",
        )));
    }

    let default_skill_id = default_generated_skill_id(candidate.candidate_id.as_str());
    let skill_id = normalize_generated_skill_identifier(
        payload.skill_id.as_deref().unwrap_or(default_skill_id.as_str()),
        "skill_id",
    )?;
    let version = payload.version.unwrap_or_else(|| "0.1.0".to_owned());
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
            review_notes: Some("Promoted from Phase 6 reusable procedure candidate.".to_owned()),
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
        rollout_flag: DYNAMIC_TOOL_BUILDER_ROLLOUT_FLAG.to_owned(),
        rollout_enabled: dynamic_tool_builder_rollout_enabled(),
        scaffold_root: scaffold.scaffold_root.clone(),
        manifest_path: scaffold.manifest_path.clone(),
        capability_declaration_path: scaffold.capability_declaration_path.clone(),
        provenance_path: scaffold.provenance_path.clone(),
        test_harness_path: scaffold.test_harness_path.clone(),
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

fn dynamic_tool_builder_rollout_enabled() -> bool {
    std::env::var(DYNAMIC_TOOL_BUILDER_ROLLOUT_FLAG)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(false)
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
    let mut index = serde_json::from_slice::<SkillBuilderCandidateIndex>(bytes.as_slice())
        .map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to parse skill builder candidate index {}: {error}",
                path.display()
            )))
        })?;
    if index.schema_version != SKILL_BUILDER_CANDIDATE_LAYOUT_VERSION {
        return Err(runtime_status_response(tonic::Status::failed_precondition(format!(
            "unsupported skill builder candidate index schema version {}",
            index.schema_version
        ))));
    }
    index.entries.sort_by(|left, right| left.generated_at_unix_ms.cmp(&right.generated_at_unix_ms));
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
    normalized
        .entries
        .sort_by(|left, right| left.generated_at_unix_ms.cmp(&right.generated_at_unix_ms));
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
    request: SkillBuilderScaffoldRequest,
) -> Result<GeneratedSkillScaffold, Response> {
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
    let request_payload = build_builder_request_payload(source, &request);
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

    let files = [
        ("skill.toml", manifest_toml.into_bytes()),
        ("README.md", readme.into_bytes()),
        ("builder-request.json", request_json_bytes),
        ("builder-capabilities.json", capability_json_bytes),
        ("tests/smoke.test.json", test_harness_bytes),
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

    let builder_candidate_id = Ulid::new().to_string().to_ascii_lowercase();
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
        capability_profile,
        generated_at_unix_ms: current_unix_ms(),
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
        },
        integrity: SkillIntegrity::default(),
        builder: Some(palyra_skills::SkillBuilderMetadata {
            experimental: true,
            source_kind: builder_source_kind(source).to_owned(),
            source_ref: builder_source_ref(source),
            rollout_flag: DYNAMIC_TOOL_BUILDER_ROLLOUT_FLAG.to_owned(),
            review_status: "quarantined".to_owned(),
            checklist: palyra_skills::SkillBuilderChecklist {
                capability_declaration_path: "builder-capabilities.json".to_owned(),
                provenance_path: "provenance.json".to_owned(),
                test_harness_path: "tests/smoke.test.json".to_owned(),
                review_notes: request.review_notes.clone().unwrap_or_default(),
            },
        }),
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
        "generated_at_unix_ms": current_unix_ms(),
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
