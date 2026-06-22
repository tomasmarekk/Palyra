//! Image observation tool backend.
//!
//! The current daemon does not ship an OCR or multimodal vision engine. This
//! tool still gives agents a first-class path for image artifacts: it resolves
//! workspace image paths, returns bounded metadata without base64, and fails
//! immediately with a structured capability error instead of encouraging
//! ad-hoc OCR install loops.

use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    agents::AgentResolveRequest,
    application::tool_runtime::workspace_scope::workspace_roots_with_run_launch_context_for_agent_source,
    gateway::{GatewayRuntimeState, ToolRuntimeExecutionContext, IMAGE_OBSERVE_TOOL_NAME},
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const IMAGE_OBSERVE_MAX_BYTES: u64 = 32 * 1024 * 1024;
const IMAGE_OBSERVE_UNSUPPORTED_MESSAGE: &str =
    "Current Palyra runtime has no OCR or vision capability configured for image artifacts.";
const IMAGE_OBSERVE_UNSUPPORTED_NEXT_ACTION: &str = "Stop this image-dependent workflow and report that OCR/vision is unsupported in the current runtime; do not infer visual content from verifier tests, golden files, expected-output hashes, or companion files.";

#[derive(Debug, Deserialize)]
struct ImageObserveInput {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    artifact_id: Option<String>,
    #[serde(default)]
    expected_digest_sha256: Option<String>,
}

#[derive(Debug)]
struct ImageFileMetadata {
    path: String,
    workspace_root_index: usize,
    mime_type: String,
    size_bytes: u64,
    sha256: String,
    width: Option<u32>,
    height: Option<u32>,
}

/// Executes `palyra.image.observe`.
///
/// The tool is deliberately read-only and non-approval-gated. It never returns
/// image bytes to the model; when no OCR or provider vision bridge is available
/// it returns `vision_not_available` with target metadata in one tool call.
pub(crate) async fn execute_image_observe_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let input = match serde_json::from_slice::<ImageObserveInput>(input_json) {
        Ok(input) => input,
        Err(error) => {
            return image_observe_outcome(
                proposal_id,
                input_json,
                false,
                image_observe_error_json("invalid_input", format!("invalid input JSON: {error}")),
                format!(
                    "{IMAGE_OBSERVE_TOOL_NAME} input must match image observation schema: {error}"
                ),
            );
        }
    };

    let has_path = input.path.as_deref().is_some_and(|path| !path.trim().is_empty());
    let has_artifact =
        input.artifact_id.as_deref().is_some_and(|artifact_id| !artifact_id.trim().is_empty());
    if has_path == has_artifact {
        return image_observe_outcome(
            proposal_id,
            input_json,
            false,
            image_observe_error_json(
                "invalid_target",
                "provide exactly one of path or artifact_id".to_owned(),
            ),
            format!("{IMAGE_OBSERVE_TOOL_NAME} requires exactly one of path or artifact_id"),
        );
    }

    let output = if has_path {
        match observe_workspace_image_path(runtime_state, context, input.path.as_deref().unwrap())
            .await
        {
            Ok(metadata) => capability_error_for_file(metadata),
            Err(error) => image_observe_error_json("image_target_unavailable", error.clone()),
        }
    } else {
        capability_error_for_artifact(
            input.artifact_id.as_deref().unwrap().trim(),
            input.expected_digest_sha256.as_deref(),
        )
    };
    let error =
        output.get("error").and_then(Value::as_str).unwrap_or("vision_not_available").to_owned();
    image_observe_outcome(proposal_id, input_json, false, output, error)
}

async fn observe_workspace_image_path(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    raw_path: &str,
) -> Result<ImageFileMetadata, String> {
    let agent_outcome = runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
        .map_err(|status| {
            format!(
                "{IMAGE_OBSERVE_TOOL_NAME} failed to resolve agent workspace: {}",
                status.message()
            )
        })?;
    let agent_workspace_roots =
        agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>();
    let workspace_roots = workspace_roots_with_run_launch_context_for_agent_source(
        runtime_state,
        context.run_id,
        agent_workspace_roots.as_slice(),
        agent_outcome.source,
    )
    .await;
    read_image_metadata_from_roots(workspace_roots.as_slice(), raw_path)
}

fn read_image_metadata_from_roots(
    workspace_roots: &[PathBuf],
    raw_path: &str,
) -> Result<ImageFileMetadata, String> {
    let requested = raw_path.trim();
    if requested.is_empty() || requested.chars().any(char::is_control) {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} requires a non-empty image path"));
    }
    let canonical_roots = canonical_workspace_roots(workspace_roots)?;
    if canonical_roots.is_empty() {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} agent has no accessible workspace roots"));
    }

    let requested_path = Path::new(requested);
    if requested_path.is_absolute() {
        for (workspace_root_index, canonical_root) in &canonical_roots {
            if !path_stays_inside_root(requested_path, canonical_root.as_path()) {
                continue;
            }
            let canonical_target = canonical_image_file(requested_path, canonical_root)?;
            let display_path = display_path(canonical_target.as_path(), canonical_root);
            return read_image_metadata(*workspace_root_index, display_path, canonical_target);
        }
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} path escapes agent workspace roots"));
    }

    let relative = strip_workspace_alias(requested);
    if relative.split(['/', '\\']).any(|segment| segment == "..") {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} path escapes agent workspace roots"));
    }
    for (workspace_root_index, canonical_root) in &canonical_roots {
        let candidate = canonical_root.join(relative);
        let canonical_target = match canonical_image_file(candidate.as_path(), canonical_root) {
            Ok(path) => path,
            Err(error) if error.contains("file not found") => continue,
            Err(error) => return Err(error),
        };
        let display_path = display_path(canonical_target.as_path(), canonical_root);
        return read_image_metadata(*workspace_root_index, display_path, canonical_target);
    }
    Err(format!("{IMAGE_OBSERVE_TOOL_NAME} file not found in agent workspace roots: {requested}"))
}

fn canonical_workspace_roots(workspace_roots: &[PathBuf]) -> Result<Vec<(usize, PathBuf)>, String> {
    let mut canonical_roots = Vec::with_capacity(workspace_roots.len());
    for (index, root) in workspace_roots.iter().enumerate() {
        match fs::canonicalize(root) {
            Ok(path) if path.is_dir() => canonical_roots.push((index, path)),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "{IMAGE_OBSERVE_TOOL_NAME} failed to resolve workspace root {index}: {error}"
                ));
            }
        }
    }
    Ok(canonical_roots)
}

fn canonical_image_file(candidate: &Path, canonical_root: &Path) -> Result<PathBuf, String> {
    let canonical_target = fs::canonicalize(candidate).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            format!("{IMAGE_OBSERVE_TOOL_NAME} file not found in agent workspace roots")
        } else {
            format!("{IMAGE_OBSERVE_TOOL_NAME} failed to resolve image path: {error}")
        }
    })?;
    if !path_stays_inside_root(canonical_target.as_path(), canonical_root) {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} path escapes agent workspace roots"));
    }
    if !canonical_target.is_file() {
        return Err(format!("{IMAGE_OBSERVE_TOOL_NAME} target is not a regular file"));
    }
    Ok(canonical_target)
}

fn read_image_metadata(
    workspace_root_index: usize,
    path: String,
    canonical_target: PathBuf,
) -> Result<ImageFileMetadata, String> {
    let mut file = fs::File::open(canonical_target.as_path()).map_err(|error| {
        format!("{IMAGE_OBSERVE_TOOL_NAME} failed to open image file {path}: {error}")
    })?;
    let metadata = file.metadata().map_err(|error| {
        format!("{IMAGE_OBSERVE_TOOL_NAME} failed to inspect image file {path}: {error}")
    })?;
    let size_bytes = metadata.len();
    if size_bytes > IMAGE_OBSERVE_MAX_BYTES {
        return Err(format!(
            "{IMAGE_OBSERVE_TOOL_NAME} image file exceeds {IMAGE_OBSERVE_MAX_BYTES} bytes"
        ));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(size_bytes).unwrap_or(0).min(8192));
    file.read_to_end(&mut bytes).map_err(|error| {
        format!("{IMAGE_OBSERVE_TOOL_NAME} failed to read image file {path}: {error}")
    })?;
    let sha256 = hex::encode(Sha256::digest(bytes.as_slice()));
    let dimensions = image_dimensions(bytes.as_slice());
    Ok(ImageFileMetadata {
        path,
        workspace_root_index,
        mime_type: image_mime_type(bytes.as_slice()).to_owned(),
        size_bytes,
        sha256,
        width: dimensions.map(|(width, _)| width),
        height: dimensions.map(|(_, height)| height),
    })
}

fn capability_error_for_file(metadata: ImageFileMetadata) -> Value {
    let mut output = image_observe_unsupported_base();
    output["target"] = json!({
            "kind": "file",
            "path": metadata.path,
            "workspace_root_index": metadata.workspace_root_index,
    });
    output["mime_type"] = json!(metadata.mime_type);
    output["size_bytes"] = json!(metadata.size_bytes);
    output["sha256"] = json!(metadata.sha256);
    output["width"] = json!(metadata.width);
    output["height"] = json!(metadata.height);
    output
}

fn capability_error_for_artifact(artifact_id: &str, expected_digest_sha256: Option<&str>) -> Value {
    let mut output = image_observe_unsupported_base();
    output["target"] = json!({
            "kind": "artifact",
            "artifact_id": artifact_id,
            "expected_digest_sha256": expected_digest_sha256,
    });
    output
}

fn image_observe_unsupported_base() -> Value {
    json!({
        "success": false,
        "error": "vision_not_available",
        "error_code": "vision_not_available",
        "message": IMAGE_OBSERVE_UNSUPPORTED_MESSAGE,
        "capability_status": "unsupported",
        "blocked_capability": "ocr_or_vision",
        "should_continue_image_task": false,
        "oracle_workaround_allowed": false,
        "next_action": IMAGE_OBSERVE_UNSUPPORTED_NEXT_ACTION,
        "claim_boundary": "image content is unknown; do not claim image-derived facts unless a later successful OCR/vision capability provides them",
        "ocr": {
            "available": false,
            "text": "",
            "confidence": null,
            "blocks": [],
        },
        "vision_summary": null,
        "capabilities": image_observe_capabilities(),
    })
}

fn image_observe_error_json(error_code: &str, message: String) -> Value {
    json!({
        "success": false,
        "error": error_code,
        "error_code": error_code,
        "message": message,
        "capabilities": image_observe_capabilities(),
    })
}

fn image_observe_capabilities() -> Value {
    json!({
        "ocr_available": false,
        "vision_available": false,
        "provider_handoff_available": false,
        "fallback": "unsupported_capability",
        "unsupported_capability": {
            "name": "ocr_or_vision",
            "should_continue_image_task": false,
            "oracle_workaround_allowed": false,
        },
        "actionable_fallbacks": [
            {
                "kind": "runtime_configuration",
                "description": "enable an OCR/vision backend before running image-dependent workflows",
            },
            {
                "kind": "user_attachment",
                "description": "submit the image as a normal user attachment to a vision-capable provider instead of relying on workspace-path observation",
            },
        ],
    })
}

fn image_observe_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output: Value,
    error: String,
) -> ToolExecutionOutcome {
    let output_json = serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec());
    build_tool_execution_outcome(
        proposal_id,
        IMAGE_OBSERVE_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        false,
        "image_observe".to_owned(),
        "workspace_roots".to_owned(),
    )
}

fn strip_workspace_alias(path: &str) -> &str {
    path.strip_prefix("/workspace/")
        .or_else(|| path.strip_prefix("workspace/"))
        .or_else(|| path.strip_prefix(r"workspace\"))
        .unwrap_or(path)
}

fn path_stays_inside_root(path: &Path, root: &Path) -> bool {
    if path == root {
        return true;
    }
    path.starts_with(root)
}

fn display_path(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| path.to_string_lossy().into_owned())
}

fn image_mime_type(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(b"\x89PNG\r\n\x1A\n") {
        "image/png"
    } else if bytes.starts_with(b"\xFF\xD8\xFF") {
        "image/jpeg"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        "image/webp"
    } else {
        "application/octet-stream"
    }
}

fn image_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    png_dimensions(bytes).or_else(|| gif_dimensions(bytes)).or_else(|| jpeg_dimensions(bytes))
}

fn png_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 24 || !bytes.starts_with(b"\x89PNG\r\n\x1A\n") {
        return None;
    }
    let width = u32::from_be_bytes(bytes.get(16..20)?.try_into().ok()?);
    let height = u32::from_be_bytes(bytes.get(20..24)?.try_into().ok()?);
    Some((width, height))
}

fn gif_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 10 || !(bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a")) {
        return None;
    }
    let width = u16::from_le_bytes(bytes.get(6..8)?.try_into().ok()?);
    let height = u16::from_le_bytes(bytes.get(8..10)?.try_into().ok()?);
    Some((u32::from(width), u32::from(height)))
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 4 || !bytes.starts_with(b"\xFF\xD8") {
        return None;
    }
    let mut index = 2usize;
    while index + 9 < bytes.len() {
        if bytes[index] != 0xFF {
            index = index.saturating_add(1);
            continue;
        }
        while index < bytes.len() && bytes[index] == 0xFF {
            index = index.saturating_add(1);
        }
        let marker = *bytes.get(index)?;
        index = index.saturating_add(1);
        if marker == 0xD9 || marker == 0xDA {
            return None;
        }
        let length = u16::from_be_bytes(bytes.get(index..index + 2)?.try_into().ok()?) as usize;
        if length < 2 || index + length > bytes.len() {
            return None;
        }
        if matches!(
            marker,
            0xC0 | 0xC1
                | 0xC2
                | 0xC3
                | 0xC5
                | 0xC6
                | 0xC7
                | 0xC9
                | 0xCA
                | 0xCB
                | 0xCD
                | 0xCE
                | 0xCF
        ) {
            let height = u16::from_be_bytes(bytes.get(index + 3..index + 5)?.try_into().ok()?);
            let width = u16::from_be_bytes(bytes.get(index + 5..index + 7)?.try_into().ok()?);
            return Some((u32::from(width), u32::from(height)));
        }
        index = index.saturating_add(length);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        capability_error_for_file, image_dimensions, image_mime_type,
        read_image_metadata_from_roots, strip_workspace_alias, ImageFileMetadata,
    };

    #[test]
    fn image_observe_reads_png_metadata_without_base64_payload() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let png = [
            0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1A, b'\n', 0, 0, 0, 13, b'I', b'H', b'D', b'R',
            0, 0, 0, 2, 0, 0, 0, 3, 8, 2, 0, 0, 0,
        ];
        std::fs::write(tempdir.path().join("code.png"), png).expect("png should be written");

        let metadata =
            read_image_metadata_from_roots(&[tempdir.path().to_path_buf()], "workspace/code.png")
                .expect("image metadata should load");

        assert_eq!(metadata.path, "code.png");
        assert_eq!(metadata.mime_type, "image/png");
        assert_eq!(metadata.width, Some(2));
        assert_eq!(metadata.height, Some(3));
        assert!(!metadata.sha256.is_empty());
    }

    #[test]
    fn image_observe_rejects_workspace_escape() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let outside = tempfile::tempdir().expect("outside tempdir should be created");
        let outside_path = outside.path().join("secret.png");
        std::fs::write(outside_path.as_path(), b"not a real image").expect("file should exist");

        let error = read_image_metadata_from_roots(
            &[tempdir.path().to_path_buf()],
            outside_path.to_string_lossy().as_ref(),
        )
        .expect_err("absolute path outside root should be rejected");

        assert!(error.contains("path escapes agent workspace roots"));
    }

    #[test]
    fn image_observe_detects_common_image_metadata() {
        let png = [
            0x89, b'P', b'N', b'G', b'\r', b'\n', 0x1A, b'\n', 0, 0, 0, 13, b'I', b'H', b'D', b'R',
            0, 0, 0, 8, 0, 0, 0, 9,
        ];
        let gif = [b'G', b'I', b'F', b'8', b'9', b'a', 4, 0, 5, 0];

        assert_eq!(image_mime_type(&png), "image/png");
        assert_eq!(image_dimensions(&png), Some((8, 9)));
        assert_eq!(image_mime_type(&gif), "image/gif");
        assert_eq!(image_dimensions(&gif), Some((4, 5)));
        assert_eq!(strip_workspace_alias("/workspace/code.png"), "code.png");
    }

    #[test]
    fn image_observe_unsupported_capability_blocks_oracle_workarounds() {
        let output = capability_error_for_file(ImageFileMetadata {
            path: "chess_board.png".to_owned(),
            workspace_root_index: 0,
            mime_type: "image/png".to_owned(),
            size_bytes: 128,
            sha256: "abc123".to_owned(),
            width: Some(8),
            height: Some(8),
        });

        assert_eq!(output["error_code"], "vision_not_available");
        assert_eq!(output["capability_status"], "unsupported");
        assert_eq!(output["blocked_capability"], "ocr_or_vision");
        assert_eq!(output["should_continue_image_task"], false);
        assert_eq!(output["oracle_workaround_allowed"], false);
        assert!(output["next_action"]
            .as_str()
            .expect("next action should be model-visible")
            .contains("do not infer visual content from verifier tests"));
        assert_eq!(
            output["capabilities"]["unsupported_capability"]["oracle_workaround_allowed"],
            false
        );
    }
}
