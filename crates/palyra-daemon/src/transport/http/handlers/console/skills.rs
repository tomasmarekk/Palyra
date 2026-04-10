use crate::gateway::current_unix_ms;
use crate::journal::{
    LearningCandidateListFilter, LearningCandidateRecord, LearningCandidateReviewRequest,
};
use crate::*;

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

    let scaffold = write_procedure_skill_scaffold(
        &candidate,
        skill_id.as_str(),
        version.as_str(),
        publisher.as_str(),
        name.as_str(),
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
    })))
}

#[derive(Debug)]
struct ProcedureSkillScaffold {
    skill_id: String,
    version: String,
    publisher: String,
    name: String,
    scaffold_root: String,
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
fn write_procedure_skill_scaffold(
    candidate: &LearningCandidateRecord,
    skill_id: &str,
    version: &str,
    publisher: &str,
    name: &str,
) -> Result<ProcedureSkillScaffold, Response> {
    let skills_root = resolve_skills_root()?;
    let scaffold_root = skills_root.join("candidate-scaffolds").join(skill_id).join(version);
    let root_existed = scaffold_root.exists();
    fs::create_dir_all(scaffold_root.as_path()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to create scaffold root {}: {error}",
            scaffold_root.display()
        )))
    })?;

    let manifest = build_procedure_skill_manifest(candidate, skill_id, version, publisher, name);
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

    let readme = build_procedure_skill_readme(candidate, skill_id, version);
    let procedure_json = json!({
        "candidate_id": candidate.candidate_id,
        "candidate_kind": candidate.candidate_kind,
        "summary": candidate.summary,
        "content": serde_json::from_str::<Value>(candidate.content_json.as_str()).unwrap_or_else(|_| json!({ "raw": candidate.content_json })),
        "provenance": serde_json::from_str::<Value>(candidate.provenance_json.as_str()).unwrap_or_else(|_| json!([])),
        "generated_at_unix_ms": current_unix_ms(),
    });
    let procedure_json_bytes = serde_json::to_vec_pretty(&procedure_json).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to encode scaffold procedure JSON: {error}"
        )))
    })?;
    let sbom_bytes = serde_json::to_vec_pretty(&json!({
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "components": [
            {
                "type": "application",
                "name": skill_id,
                "version": version,
                "publisher": publisher,
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
                "name": "procedure.json",
                "digest": { "sha256": sha256_hex(procedure_json_bytes.as_slice()) },
            },
            {
                "name": "sbom.cdx.json",
                "digest": { "sha256": sha256_hex(sbom_bytes.as_slice()) },
            }
        ],
        "metadata": {
            "candidate_id": candidate.candidate_id,
            "summary": candidate.summary,
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
        ("procedure.json", procedure_json_bytes),
        ("sbom.cdx.json", sbom_bytes),
        ("provenance.json", provenance_bytes),
    ];
    let mut written_files = Vec::new();
    for (relative_path, bytes) in files {
        let target = scaffold_root.join(relative_path);
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

    Ok(ProcedureSkillScaffold {
        skill_id: skill_id.to_owned(),
        version: version.to_owned(),
        publisher: publisher.to_owned(),
        name: name.to_owned(),
        scaffold_root: scaffold_root.to_string_lossy().into_owned(),
        files: written_files,
    })
}

fn build_procedure_skill_manifest(
    candidate: &LearningCandidateRecord,
    skill_id: &str,
    version: &str,
    publisher: &str,
    name: &str,
) -> SkillManifest {
    SkillManifest {
        manifest_version: SKILL_MANIFEST_VERSION,
        skill_id: skill_id.to_owned(),
        name: name.to_owned(),
        version: version.to_owned(),
        publisher: publisher.to_owned(),
        entrypoints: SkillEntrypoints {
            tools: vec![SkillToolEntrypoint {
                id: format!("{publisher}.run"),
                name: "Run promoted procedure".to_owned(),
                description: candidate.summary.clone(),
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
            filesystem: SkillFilesystemCapabilities::default(),
            http_egress_allowlist: Vec::new(),
            secrets: Vec::new(),
            device_capabilities: Vec::new(),
            node_capabilities: Vec::new(),
            quotas: SkillQuotaConfig::default(),
            wildcard_opt_in: Default::default(),
        },
        compat: SkillCompat {
            required_protocol_major: palyra_common::CANONICAL_PROTOCOL_MAJOR,
            min_palyra_version: build_metadata().version.to_owned(),
        },
        integrity: SkillIntegrity::default(),
    }
}

fn build_procedure_skill_readme(
    candidate: &LearningCandidateRecord,
    skill_id: &str,
    version: &str,
) -> String {
    format!(
        "# {skill_id}\n\n\
Version: {version}\n\
Source candidate: {candidate_id}\n\
Risk level: {risk_level}\n\n\
## Summary\n\n\
{summary}\n\n\
## Promotion posture\n\n\
- Generated from a Phase 6 procedure learning candidate.\n\
- Scaffold remains quarantined until an operator packages, signs, verifies, and explicitly enables it.\n\
- Review `procedure.json` and `provenance.json` before turning this scaffold into a signed artifact.\n",
        candidate_id = candidate.candidate_id,
        risk_level = candidate.risk_level,
        summary = candidate.summary,
    )
}
