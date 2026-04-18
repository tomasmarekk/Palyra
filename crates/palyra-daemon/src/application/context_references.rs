use std::{
    collections::VecDeque,
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use palyra_common::context_references::{
    parse_context_references, ContextReferenceKind, ContextReferenceParseError,
    ContextReferenceParseResult, ParsedContextReference,
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::process::Command;
use tonic::Status;

use crate::{
    agents::AgentResolveRequest,
    application::tool_runtime::http_fetch::execute_http_fetch_tool,
    gateway::{truncate_with_ellipsis, GatewayRuntimeState, MEMORY_AUTO_INJECT_MIN_SCORE},
    journal::MemorySearchRequest,
    tool_protocol::{decide_tool_call, ToolRequestContext},
    transport::grpc::auth::RequestContext,
};

const MAX_REFERENCE_COUNT: usize = 8;
const MAX_TOTAL_REFERENCE_CHARS: usize = 24_000;
const MAX_FILE_REFERENCE_CHARS: usize = 8_000;
const MAX_FOLDER_REFERENCE_FILES: usize = 6;
const MAX_FOLDER_REFERENCE_CHARS: usize = 12_000;
const MAX_URL_REFERENCE_CHARS: usize = 8_000;
const MAX_GIT_REFERENCE_CHARS: usize = 10_000;
const MAX_MEMORY_REFERENCE_ITEMS: usize = 4;
const MAX_PREVIEW_TEXT_CHARS: usize = 320;
const ALLOWED_URL_CONTENT_TYPES: &[&str] =
    &["text/plain", "text/markdown", "text/html", "application/json", "application/xml"];
const BLOCKED_PATH_COMPONENTS: &[&str] = &[".git", ".ssh", ".aws"];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ContextReferenceProvenance {
    pub(crate) kind: String,
    pub(crate) location: String,
    pub(crate) note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ResolvedContextReference {
    pub(crate) reference_id: String,
    pub(crate) kind: ContextReferenceKind,
    pub(crate) raw_text: String,
    pub(crate) target: Option<String>,
    pub(crate) display_target: String,
    pub(crate) start_offset: usize,
    pub(crate) end_offset: usize,
    pub(crate) estimated_tokens: usize,
    pub(crate) warnings: Vec<String>,
    pub(crate) provenance: Vec<ContextReferenceProvenance>,
    pub(crate) preview_text: String,
    pub(crate) resolved_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ContextReferencePreviewEnvelope {
    pub(crate) clean_prompt: String,
    pub(crate) references: Vec<ResolvedContextReference>,
    pub(crate) total_estimated_tokens: usize,
    pub(crate) warnings: Vec<String>,
    pub(crate) errors: Vec<ContextReferenceParseError>,
}

pub(crate) async fn preview_context_references(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    input_text: &str,
) -> Result<ContextReferencePreviewEnvelope, Status> {
    let ContextReferenceParseResult { references: parsed_references, errors, clean_text } =
        parse_context_references(input_text);

    let mut warnings = Vec::new();
    if parsed_references.len() > MAX_REFERENCE_COUNT {
        warnings.push(format!(
            "Only the first {MAX_REFERENCE_COUNT} context references are resolved per prompt."
        ));
    }

    let needs_workspace = parsed_references.iter().any(|reference| {
        matches!(
            reference.kind,
            ContextReferenceKind::File
                | ContextReferenceKind::Folder
                | ContextReferenceKind::Diff
                | ContextReferenceKind::Staged
        )
    });
    let workspace_roots = if needs_workspace {
        resolve_workspace_roots(runtime_state, context, session_id).await?
    } else {
        Vec::new()
    };

    let mut total_chars = 0usize;
    let mut resolved_references = Vec::new();
    for (index, reference) in parsed_references.into_iter().take(MAX_REFERENCE_COUNT).enumerate() {
        let resolved = resolve_context_reference(
            runtime_state,
            context,
            session_id,
            &workspace_roots,
            reference,
            index,
            &mut total_chars,
        )
        .await?;
        resolved_references.push(resolved);
    }

    Ok(ContextReferencePreviewEnvelope {
        clean_prompt: clean_text,
        total_estimated_tokens: resolved_references
            .iter()
            .map(|reference| reference.estimated_tokens)
            .sum(),
        references: resolved_references,
        warnings,
        errors,
    })
}

pub(crate) fn render_context_reference_block(
    preview: &ContextReferencePreviewEnvelope,
) -> Option<String> {
    if preview.references.is_empty() {
        return None;
    }
    let mut block = String::from("<context_references>\n");
    for (index, reference) in preview.references.iter().enumerate() {
        block.push_str(
            format!(
                "{}. kind={} target={} estimated_tokens={}\n",
                index + 1,
                reference.kind.as_str(),
                reference.display_target,
                reference.estimated_tokens
            )
            .as_str(),
        );
        if !reference.warnings.is_empty() {
            block.push_str("warnings=");
            block.push_str(reference.warnings.join(" | ").as_str());
            block.push('\n');
        }
        block.push_str(reference.resolved_text.trim());
        block.push('\n');
        if index + 1 < preview.references.len() {
            block.push('\n');
        }
    }
    block.push_str("</context_references>");
    Some(block)
}

pub(crate) fn render_context_reference_prompt(
    preview: &ContextReferencePreviewEnvelope,
    prompt_input_text: &str,
) -> Option<String> {
    let block = render_context_reference_block(preview)?;
    let trimmed_prompt = prompt_input_text.trim();
    let final_prompt = if trimmed_prompt.is_empty() {
        let clean_prompt = preview.clean_prompt.trim();
        if clean_prompt.is_empty() {
            "Use the attached context references to answer the request.".to_owned()
        } else {
            clean_prompt.to_owned()
        }
    } else {
        prompt_input_text.to_owned()
    };

    Some(format!("{block}\n\n{final_prompt}"))
}

async fn resolve_context_reference(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    workspace_roots: &[PathBuf],
    reference: ParsedContextReference,
    index: usize,
    total_chars: &mut usize,
) -> Result<ResolvedContextReference, Status> {
    match reference.kind {
        ContextReferenceKind::File => {
            resolve_file_reference(workspace_roots, reference, index, total_chars)
        }
        ContextReferenceKind::Folder => {
            resolve_folder_reference(workspace_roots, reference, index, total_chars)
        }
        ContextReferenceKind::Diff => {
            resolve_git_reference(workspace_roots, reference, index, total_chars, false).await
        }
        ContextReferenceKind::Staged => {
            resolve_git_reference(workspace_roots, reference, index, total_chars, true).await
        }
        ContextReferenceKind::Url => {
            resolve_url_reference(runtime_state, context, session_id, reference, index, total_chars)
                .await
        }
        ContextReferenceKind::Memory => {
            resolve_memory_reference(
                runtime_state,
                context,
                session_id,
                reference,
                index,
                total_chars,
            )
            .await
        }
    }
}

async fn resolve_workspace_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
) -> Result<Vec<PathBuf>, Status> {
    let resolved = runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.clone(),
            channel: context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await?;
    let roots = resolved.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    if roots.is_empty() {
        return Err(Status::failed_precondition(
            "no workspace roots are configured for the resolved agent",
        ));
    }
    Ok(roots)
}

fn resolve_file_reference(
    workspace_roots: &[PathBuf],
    reference: ParsedContextReference,
    index: usize,
    total_chars: &mut usize,
) -> Result<ResolvedContextReference, Status> {
    let raw_target = reference.target.as_deref().unwrap_or_default();
    let resolved = resolve_existing_workspace_path(workspace_roots, raw_target, false)?;
    let mut warnings = Vec::new();
    let body = read_text_file_limited(
        resolved.canonical_path.as_path(),
        MAX_FILE_REFERENCE_CHARS.min(MAX_TOTAL_REFERENCE_CHARS.saturating_sub(*total_chars)),
        &mut warnings,
    )?;
    *total_chars = total_chars.saturating_add(body.len());
    build_resolved_reference(
        reference,
        index,
        resolved.display_target,
        body,
        warnings,
        vec![ContextReferenceProvenance {
            kind: "workspace_file".to_owned(),
            location: resolved.canonical_path.to_string_lossy().into_owned(),
            note: "Resolved inside the active agent workspace roots.".to_owned(),
        }],
    )
}

fn resolve_folder_reference(
    workspace_roots: &[PathBuf],
    reference: ParsedContextReference,
    index: usize,
    total_chars: &mut usize,
) -> Result<ResolvedContextReference, Status> {
    let raw_target = reference.target.as_deref().unwrap_or_default();
    let resolved = resolve_existing_workspace_path(workspace_roots, raw_target, true)?;
    let mut warnings = Vec::new();
    let mut provenance = Vec::new();
    let body = collect_folder_reference_text(
        resolved.canonical_path.as_path(),
        resolved.matched_root.as_path(),
        total_chars,
        &mut warnings,
        &mut provenance,
    )?;
    build_resolved_reference(reference, index, resolved.display_target, body, warnings, provenance)
}

async fn resolve_git_reference(
    workspace_roots: &[PathBuf],
    reference: ParsedContextReference,
    index: usize,
    total_chars: &mut usize,
    staged: bool,
) -> Result<ResolvedContextReference, Status> {
    let git_root = workspace_roots
        .iter()
        .find(|candidate| candidate.join(".git").exists())
        .cloned()
        .or_else(|| workspace_roots.first().cloned())
        .ok_or_else(|| Status::failed_precondition("no workspace roots are available"))?;
    let target =
        reference.target.as_deref().map(validate_workspace_relative_git_target).transpose()?;

    let mut command = Command::new("git");
    command.arg("-C").arg(git_root.as_path()).arg("diff");
    if staged {
        command.arg("--staged");
    }
    if let Some(target) = target.as_deref() {
        command.arg("--").arg(target.as_os_str());
    }
    let output = command.output().await.map_err(|error| {
        Status::internal(format!("failed to launch git for context reference: {error}"))
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(output.stderr.as_slice()).trim().to_owned();
        return Err(Status::invalid_argument(if stderr.is_empty() {
            "git diff returned a non-success status for the requested reference".to_owned()
        } else {
            format!("git diff failed: {stderr}")
        }));
    }
    let mut warnings = Vec::new();
    let mut diff_text = String::from_utf8_lossy(output.stdout.as_slice()).into_owned();
    if diff_text.trim().is_empty() {
        warnings.push(if staged {
            "No staged diff matched this reference.".to_owned()
        } else {
            "No diff matched this reference.".to_owned()
        });
    }
    diff_text = truncate_reference_text(diff_text, MAX_GIT_REFERENCE_CHARS, &mut warnings);
    diff_text = consume_reference_budget(diff_text, total_chars, &mut warnings);
    let display_target = reference
        .target
        .clone()
        .unwrap_or_else(|| if staged { "staged" } else { "working tree diff" }.to_owned());
    build_resolved_reference(
        reference,
        index,
        display_target,
        diff_text,
        warnings,
        vec![ContextReferenceProvenance {
            kind: if staged { "git_staged" } else { "git_diff" }.to_owned(),
            location: git_root.to_string_lossy().into_owned(),
            note: "Captured from the active workspace git checkout.".to_owned(),
        }],
    )
}

async fn resolve_url_reference(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    reference: ParsedContextReference,
    index: usize,
    total_chars: &mut usize,
) -> Result<ResolvedContextReference, Status> {
    let target = reference.target.clone().unwrap_or_default();
    let parsed_url = Url::parse(target.as_str())
        .map_err(|error| Status::invalid_argument(format!("invalid @url target: {error}")))?;
    if !matches!(parsed_url.scheme(), "http" | "https") {
        return Err(Status::invalid_argument("@url only supports http and https targets"));
    }
    authorize_url_reference_fetch(&runtime_state.config.tool_call, context, session_id)?;
    let payload = serde_json::to_vec(&json!({
        "url": parsed_url.as_str(),
        "allow_redirects": true,
        "max_redirects": 3,
        "max_response_bytes": MAX_URL_REFERENCE_CHARS,
        "allowed_content_types": ALLOWED_URL_CONTENT_TYPES,
        "cache": true,
    }))
    .map_err(|error| {
        Status::internal(format!("failed to serialize @url resolver input: {error}"))
    })?;
    let outcome = execute_http_fetch_tool(
        runtime_state,
        format!("context-url-{index}").as_str(),
        payload.as_slice(),
    )
    .await;
    if !outcome.success {
        return Err(Status::invalid_argument(outcome.error));
    }
    let response_json: Value =
        serde_json::from_slice(outcome.output_json.as_slice()).map_err(|error| {
            Status::internal(format!("failed to decode palyra.http.fetch output: {error}"))
        })?;
    let mut warnings = Vec::new();
    let content_type =
        response_json.get("content_type").and_then(Value::as_str).unwrap_or("unknown");
    let body_text =
        response_json.get("body_text").and_then(Value::as_str).unwrap_or_default().to_owned();
    let body_text = consume_reference_budget(body_text, total_chars, &mut warnings);
    build_resolved_reference(
        reference,
        index,
        target.clone(),
        body_text,
        warnings,
        vec![ContextReferenceProvenance {
            kind: "url".to_owned(),
            location: parsed_url.to_string(),
            note: format!("Fetched via palyra.http.fetch ({content_type})."),
        }],
    )
}

fn authorize_url_reference_fetch(
    tool_call_config: &crate::tool_protocol::ToolCallConfig,
    context: &RequestContext,
    session_id: &str,
) -> Result<(), Status> {
    let mut remaining_budget = 1;
    let decision = decide_tool_call(
        tool_call_config,
        &mut remaining_budget,
        &ToolRequestContext {
            principal: context.principal.clone(),
            device_id: Some(context.device_id.clone()),
            channel: context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            run_id: None,
            skill_id: None,
        },
        "palyra.http.fetch",
        false,
    );
    if !decision.allowed || decision.approval_required {
        return Err(Status::permission_denied(format!(
            "@url references cannot execute palyra.http.fetch: {}",
            decision.reason
        )));
    }
    Ok(())
}

async fn resolve_memory_reference(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    reference: ParsedContextReference,
    index: usize,
    total_chars: &mut usize,
) -> Result<ResolvedContextReference, Status> {
    let query = reference.target.as_deref().unwrap_or_default().trim().to_owned();
    let hits = runtime_state
        .search_memory(MemorySearchRequest {
            principal: context.principal.clone(),
            channel: context.channel.clone(),
            session_id: Some(session_id.to_owned()),
            query: query.clone(),
            top_k: MAX_MEMORY_REFERENCE_ITEMS,
            min_score: MEMORY_AUTO_INJECT_MIN_SCORE,
            tags: Vec::new(),
            sources: Vec::new(),
        })
        .await?;
    let mut warnings = Vec::new();
    if hits.is_empty() {
        warnings.push("No memory entries matched this reference.".to_owned());
    }
    let mut provenance = Vec::new();
    let mut rendered = String::from("<memory_reference>\n");
    for (position, hit) in hits.iter().enumerate() {
        provenance.push(ContextReferenceProvenance {
            kind: "memory_item".to_owned(),
            location: hit.item.memory_id.clone(),
            note: format!("score={:.4}", hit.score),
        });
        rendered.push_str(
            format!(
                "{}. memory_id={} source={} score={:.4} snippet={}\n",
                position + 1,
                hit.item.memory_id,
                hit.item.source.as_str(),
                hit.score,
                truncate_with_ellipsis(hit.snippet.replace(['\r', '\n'], " "), 256),
            )
            .as_str(),
        );
    }
    rendered.push_str("</memory_reference>");
    let rendered = consume_reference_budget(rendered, total_chars, &mut warnings);
    build_resolved_reference(reference, index, query, rendered, warnings, provenance)
}

fn build_resolved_reference(
    reference: ParsedContextReference,
    index: usize,
    display_target: String,
    resolved_text: String,
    warnings: Vec<String>,
    provenance: Vec<ContextReferenceProvenance>,
) -> Result<ResolvedContextReference, Status> {
    Ok(ResolvedContextReference {
        reference_id: format!("ref-{}", index + 1),
        kind: reference.kind,
        raw_text: reference.raw_text,
        target: reference.target,
        display_target: display_target.clone(),
        start_offset: reference.start_offset,
        end_offset: reference.end_offset,
        estimated_tokens: estimate_text_tokens(resolved_text.as_str()),
        preview_text: truncate_with_ellipsis(
            resolved_text.replace(['\r', '\n'], " "),
            MAX_PREVIEW_TEXT_CHARS,
        ),
        resolved_text,
        warnings,
        provenance,
    })
}

fn consume_reference_budget(
    text: String,
    total_chars: &mut usize,
    warnings: &mut Vec<String>,
) -> String {
    let remaining = MAX_TOTAL_REFERENCE_CHARS.saturating_sub(*total_chars);
    let next = truncate_reference_text(text, remaining, warnings);
    *total_chars = total_chars.saturating_add(next.len());
    next
}

fn truncate_reference_text(text: String, max_chars: usize, warnings: &mut Vec<String>) -> String {
    if max_chars == 0 {
        warnings
            .push("Context reference budget is exhausted; this reference was omitted.".to_owned());
        return String::new();
    }
    let char_count = text.chars().count();
    if char_count <= max_chars {
        return text;
    }
    warnings.push(format!("Reference content was truncated to {max_chars} characters."));
    text.chars().take(max_chars).collect::<String>()
}

fn estimate_text_tokens(text: &str) -> usize {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        0
    } else {
        trimmed.chars().count().div_ceil(4)
    }
}

struct ResolvedWorkspacePath {
    canonical_path: PathBuf,
    matched_root: PathBuf,
    display_target: String,
}

fn resolve_existing_workspace_path(
    workspace_roots: &[PathBuf],
    raw_target: &str,
    expect_directory: bool,
) -> Result<ResolvedWorkspacePath, Status> {
    let target = validate_workspace_relative_git_target(raw_target)?;
    for root in workspace_roots {
        let candidate = root.join(target.as_path());
        if !candidate.exists() {
            continue;
        }
        let canonical = fs::canonicalize(candidate.as_path()).map_err(|error| {
            Status::internal(format!("failed to canonicalize context reference target: {error}"))
        })?;
        if !canonical.starts_with(root.as_path()) {
            return Err(Status::invalid_argument(
                "context reference escapes the active workspace root",
            ));
        }
        if contains_blocked_path_component(canonical.as_path()) {
            return Err(Status::permission_denied(
                "context reference targets a blocked or sensitive workspace path",
            ));
        }
        let metadata = fs::metadata(canonical.as_path()).map_err(|error| {
            Status::internal(format!("failed to read context reference metadata: {error}"))
        })?;
        if expect_directory && !metadata.is_dir() {
            return Err(Status::invalid_argument("expected a directory reference"));
        }
        if !expect_directory && !metadata.is_file() {
            return Err(Status::invalid_argument("expected a file reference"));
        }
        return Ok(ResolvedWorkspacePath {
            display_target: target.to_string_lossy().replace('\\', "/"),
            canonical_path: canonical,
            matched_root: root.clone(),
        });
    }
    Err(Status::not_found(format!(
        "workspace target '{}' does not exist inside the active workspace roots",
        raw_target.trim()
    )))
}

fn validate_workspace_relative_git_target(raw_target: &str) -> Result<PathBuf, Status> {
    let trimmed = raw_target.trim();
    if trimmed.is_empty() {
        return Err(Status::invalid_argument("context reference target cannot be empty"));
    }
    let path = PathBuf::from(trimmed);
    if path.is_absolute() {
        return Err(Status::invalid_argument(
            "workspace context references must stay relative to the active workspace roots",
        ));
    }
    for component in path.components() {
        match component {
            Component::ParentDir => {
                return Err(Status::invalid_argument(
                    "context reference cannot use parent-directory traversal",
                ))
            }
            Component::Normal(value) => {
                let normalized = value.to_string_lossy();
                if BLOCKED_PATH_COMPONENTS.iter().any(|blocked| *blocked == normalized) {
                    return Err(Status::permission_denied(
                        "context reference targets a blocked or sensitive workspace path",
                    ));
                }
                if normalized.starts_with(".env") {
                    return Err(Status::permission_denied(
                        "context reference targets a blocked or sensitive workspace path",
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(path)
}

fn contains_blocked_path_component(path: &Path) -> bool {
    path.components().any(|component| match component {
        Component::Normal(value) => {
            let normalized = value.to_string_lossy();
            BLOCKED_PATH_COMPONENTS.iter().any(|blocked| *blocked == normalized)
                || normalized.starts_with(".env")
        }
        _ => false,
    })
}

fn read_text_file_limited(
    path: &Path,
    max_chars: usize,
    warnings: &mut Vec<String>,
) -> Result<String, Status> {
    let bytes = fs::read(path).map_err(|error| {
        Status::internal(format!("failed to read context reference file: {error}"))
    })?;
    let raw_text = String::from_utf8(bytes)
        .map_err(|_| Status::invalid_argument("context reference file is not valid UTF-8 text"))?;
    Ok(truncate_reference_text(raw_text, max_chars, warnings))
}

fn collect_folder_reference_text(
    folder: &Path,
    matched_root: &Path,
    total_chars: &mut usize,
    warnings: &mut Vec<String>,
    provenance: &mut Vec<ContextReferenceProvenance>,
) -> Result<String, Status> {
    let mut queue = VecDeque::from([folder.to_path_buf()]);
    let mut rendered = String::new();
    let mut file_count = 0usize;

    while let Some(current) = queue.pop_front() {
        let entries = fs::read_dir(current.as_path()).map_err(|error| {
            Status::internal(format!("failed to read context reference folder: {error}"))
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                Status::internal(format!("failed to traverse folder reference: {error}"))
            })?;
            let file_type = entry.file_type().map_err(|error| {
                Status::internal(format!("failed to inspect folder entry: {error}"))
            })?;
            if file_type.is_symlink() {
                warnings
                    .push(format!("Skipped symlinked entry {}.", entry.path().to_string_lossy()));
                continue;
            }
            let entry_path = entry.path();
            if contains_blocked_path_component(entry_path.as_path()) {
                warnings.push(format!(
                    "Skipped blocked workspace path {}.",
                    entry_path.to_string_lossy()
                ));
                continue;
            }
            if file_type.is_dir() {
                queue.push_back(entry_path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            if file_count >= MAX_FOLDER_REFERENCE_FILES {
                warnings.push(format!(
                    "Folder reference was capped at {MAX_FOLDER_REFERENCE_FILES} files."
                ));
                return Ok(consume_reference_budget(rendered, total_chars, warnings));
            }

            let relative = entry_path
                .strip_prefix(matched_root)
                .unwrap_or(entry_path.as_path())
                .to_string_lossy()
                .replace('\\', "/");
            let mut file_warnings = Vec::new();
            let content = match read_text_file_limited(
                entry_path.as_path(),
                MAX_FOLDER_REFERENCE_CHARS,
                &mut file_warnings,
            ) {
                Ok(content) => content,
                Err(_) => {
                    warnings.push(format!("Skipped non-text file {relative}."));
                    continue;
                }
            };
            warnings.append(&mut file_warnings);
            provenance.push(ContextReferenceProvenance {
                kind: "workspace_file".to_owned(),
                location: entry_path.to_string_lossy().into_owned(),
                note: "Included from a folder reference.".to_owned(),
            });
            rendered.push_str(format!("<folder_file path=\"{relative}\">\n").as_str());
            rendered.push_str(content.as_str());
            rendered.push_str("\n</folder_file>\n");
            file_count += 1;
            if rendered.chars().count() >= MAX_FOLDER_REFERENCE_CHARS {
                warnings.push(format!(
                    "Folder reference output was capped at {MAX_FOLDER_REFERENCE_CHARS} characters."
                ));
                break;
            }
        }
    }

    Ok(consume_reference_budget(rendered, total_chars, warnings))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::{
        authorize_url_reference_fetch, contains_blocked_path_component, estimate_text_tokens,
        read_text_file_limited, render_context_reference_prompt,
        validate_workspace_relative_git_target, ContextReferencePreviewEnvelope,
        ContextReferenceProvenance, ResolvedContextReference,
    };
    use crate::{
        sandbox_runner::{
            EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier,
        },
        tool_protocol::ToolCallConfig,
        transport::grpc::auth::RequestContext,
        wasm_plugin_runner::WasmPluginRunnerPolicy,
    };
    use palyra_common::context_references::ContextReferenceKind;

    #[test]
    fn validate_workspace_relative_target_rejects_parent_traversal() {
        let error = validate_workspace_relative_git_target("../secret.txt")
            .expect_err("parent traversal must be rejected");
        assert!(error.message().contains("parent-directory traversal"));
    }

    #[test]
    fn blocked_component_detection_matches_sensitive_paths() {
        assert!(contains_blocked_path_component(Path::new("workspace/.git/config")));
        assert!(contains_blocked_path_component(Path::new("workspace/.env")));
        assert!(!contains_blocked_path_component(Path::new("workspace/src/lib.rs")));
    }

    #[test]
    fn file_reader_truncates_large_files_and_tracks_warnings() {
        let temp = tempfile::tempdir().expect("temp dir should exist");
        let file_path = temp.path().join("notes.txt");
        fs::write(&file_path, "A".repeat(128)).expect("fixture file should be written");
        let mut warnings = Vec::new();
        let output = read_text_file_limited(file_path.as_path(), 32, &mut warnings)
            .expect("read should work");
        assert_eq!(output.len(), 32);
        assert!(!warnings.is_empty(), "truncate should surface a warning");
    }

    #[test]
    fn render_context_reference_prompt_uses_clean_prompt_when_available() {
        let preview = ContextReferencePreviewEnvelope {
            clean_prompt: "Summarize the referenced files.".to_owned(),
            total_estimated_tokens: 64,
            warnings: Vec::new(),
            errors: Vec::new(),
            references: vec![ResolvedContextReference {
                reference_id: "ref-1".to_owned(),
                kind: ContextReferenceKind::File,
                raw_text: "@file:README.md".to_owned(),
                target: Some("README.md".to_owned()),
                display_target: "README.md".to_owned(),
                start_offset: 0,
                end_offset: 15,
                estimated_tokens: 32,
                warnings: Vec::new(),
                provenance: vec![ContextReferenceProvenance {
                    kind: "workspace_file".to_owned(),
                    location: "/tmp/README.md".to_owned(),
                    note: "fixture".to_owned(),
                }],
                preview_text: "README preview".to_owned(),
                resolved_text: "<reference_file path=\"README.md\">README</reference_file>"
                    .to_owned(),
            }],
        };
        let rendered = render_context_reference_prompt(&preview, "fallback")
            .expect("non-empty preview should render");
        assert!(rendered.contains("<context_references>"));
        assert!(rendered.contains("fallback"));
    }

    #[test]
    fn token_estimate_is_zero_for_empty_text() {
        assert_eq!(estimate_text_tokens("   "), 0);
        assert_eq!(estimate_text_tokens("12345678"), 2);
    }

    fn context_reference_tool_config(allowed_tools: Vec<String>) -> ToolCallConfig {
        ToolCallConfig {
            allowed_tools,
            max_calls_per_run: 8,
            execution_timeout_ms: 5_000,
            process_runner: SandboxProcessRunnerPolicy {
                enabled: false,
                tier: SandboxProcessRunnerTier::B,
                workspace_root: Path::new(".").to_path_buf(),
                allowed_executables: Vec::new(),
                allow_interpreters: false,
                egress_enforcement_mode: EgressEnforcementMode::None,
                allowed_egress_hosts: Vec::new(),
                allowed_dns_suffixes: Vec::new(),
                cpu_time_limit_ms: 1_000,
                memory_limit_bytes: 1_024 * 1_024,
                max_output_bytes: 4_096,
            },
            wasm_runtime: WasmPluginRunnerPolicy {
                enabled: false,
                allow_inline_modules: false,
                max_module_size_bytes: 64 * 1024,
                fuel_budget: 10_000,
                max_memory_bytes: 64 * 1024,
                max_table_elements: 128,
                max_instances: 1,
                allowed_http_hosts: Vec::new(),
                allowed_secrets: Vec::new(),
                allowed_storage_prefixes: Vec::new(),
                allowed_channels: Vec::new(),
            },
        }
    }

    #[test]
    fn url_references_reject_approval_gated_http_fetch() {
        let error = authorize_url_reference_fetch(
            &context_reference_tool_config(vec!["palyra.http.fetch".to_owned()]),
            &RequestContext {
                principal: "user:test".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("console".to_owned()),
            },
            "01ARZ3NDEKTSV4RRFFQ69G5FAA",
        )
        .expect_err("@url references must fail closed for approval-gated fetches");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert!(
            error.message().contains("palyra.http.fetch"),
            "denial should explain that @url cannot bypass the fetch tool gate: {}",
            error.message()
        );
    }
}
