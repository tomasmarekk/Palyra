//! Per-run file view registry for workspace read-before-write guards.
//!
//! The registry stores metadata only: resolved path, size, mtime, chunk hash,
//! and read range. It never stores file contents, so stale-edit preflight can
//! block unsafe writes without creating a second copy of workspace data.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const FILE_VIEW_REGISTRY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspaceFileViewRecord {
    pub(crate) schema_version: u32,
    pub(crate) run_id: String,
    pub(crate) proposal_id: String,
    pub(crate) display_path: String,
    pub(crate) workspace_root_index: usize,
    #[serde(skip)]
    pub(crate) resolved_path: PathBuf,
    pub(crate) resolved_path_hash: String,
    pub(crate) size_bytes: u64,
    pub(crate) mtime_unix_ms: Option<i64>,
    pub(crate) offset_bytes: u64,
    pub(crate) returned_bytes: u64,
    pub(crate) chunk_sha256: String,
    pub(crate) observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspacePatchFileViewDecision {
    Allow,
    Warn,
    Block,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchFileViewDiagnostic {
    pub(crate) path: String,
    pub(crate) decision: WorkspacePatchFileViewDecision,
    pub(crate) reason_code: String,
    pub(crate) remediation: String,
    pub(crate) previous_read_timestamp_unix_ms: Option<i64>,
    pub(crate) previous_read_proposal_id: Option<String>,
    pub(crate) previous_read_chunk_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkspacePatchFileViewReport {
    pub(crate) schema_version: u32,
    pub(crate) run_id: String,
    pub(crate) hard_block: bool,
    pub(crate) diagnostics: Vec<WorkspacePatchFileViewDiagnostic>,
}

#[derive(Debug, Default)]
pub(crate) struct FileViewRegistry {
    views: BTreeMap<String, WorkspaceFileViewRecord>,
}

impl FileViewRegistry {
    pub(crate) fn record_read(&mut self, record: WorkspaceFileViewRecord) {
        let key = view_key(record.run_id.as_str(), record.display_path.as_str());
        self.views.insert(key, record);
    }

    pub(crate) fn evaluate_patch(
        &self,
        run_id: &str,
        patch_text: &str,
    ) -> WorkspacePatchFileViewReport {
        let mut diagnostics = Vec::new();
        for touched in extract_patch_touched_paths(patch_text) {
            if !touched.requires_prior_read {
                continue;
            }
            let key = view_key(run_id, touched.path.as_str());
            let Some(record) = self.views.get(key.as_str()) else {
                diagnostics.push(WorkspacePatchFileViewDiagnostic {
                    path: touched.path,
                    decision: WorkspacePatchFileViewDecision::Warn,
                    reason_code: "workspace_file_view.never_read".to_owned(),
                    remediation: "Read the current file before applying a mutating patch to it."
                        .to_owned(),
                    previous_read_timestamp_unix_ms: None,
                    previous_read_proposal_id: None,
                    previous_read_chunk_sha256: None,
                });
                continue;
            };
            let current = current_file_metadata(record.resolved_path.as_path());
            match current {
                Ok(current)
                    if current.size_bytes == record.size_bytes
                        && current.mtime_unix_ms == record.mtime_unix_ms =>
                {
                    diagnostics.push(WorkspacePatchFileViewDiagnostic {
                        path: touched.path,
                        decision: WorkspacePatchFileViewDecision::Allow,
                        reason_code: "workspace_file_view.fresh".to_owned(),
                        remediation: "Previously read file metadata still matches.".to_owned(),
                        previous_read_timestamp_unix_ms: Some(record.observed_at_unix_ms),
                        previous_read_proposal_id: Some(record.proposal_id.clone()),
                        previous_read_chunk_sha256: Some(record.chunk_sha256.clone()),
                    });
                }
                Ok(_) => {
                    diagnostics.push(WorkspacePatchFileViewDiagnostic {
                        path: touched.path,
                        decision: WorkspacePatchFileViewDecision::Block,
                        reason_code: "workspace_file_view.stale_metadata".to_owned(),
                        remediation:
                            "Re-read the file because size or mtime changed since the last read."
                                .to_owned(),
                        previous_read_timestamp_unix_ms: Some(record.observed_at_unix_ms),
                        previous_read_proposal_id: Some(record.proposal_id.clone()),
                        previous_read_chunk_sha256: Some(record.chunk_sha256.clone()),
                    });
                }
                Err(error) => {
                    diagnostics.push(WorkspacePatchFileViewDiagnostic {
                        path: touched.path,
                        decision: WorkspacePatchFileViewDecision::Block,
                        reason_code: "workspace_file_view.stat_failed".to_owned(),
                        remediation: format!(
                            "Re-read the file before patching; current metadata could not be inspected: {error}"
                        ),
                        previous_read_timestamp_unix_ms: Some(record.observed_at_unix_ms),
                        previous_read_proposal_id: Some(record.proposal_id.clone()),
                        previous_read_chunk_sha256: Some(record.chunk_sha256.clone()),
                    });
                }
            }
        }
        let hard_block = diagnostics
            .iter()
            .any(|diagnostic| diagnostic.decision == WorkspacePatchFileViewDecision::Block);
        WorkspacePatchFileViewReport {
            schema_version: FILE_VIEW_REGISTRY_SCHEMA_VERSION,
            run_id: run_id.to_owned(),
            hard_block,
            diagnostics,
        }
    }
}

pub(crate) fn build_workspace_file_view_record(
    run_id: &str,
    proposal_id: &str,
    resolved_path: &Path,
    output_json: &[u8],
    observed_at_unix_ms: i64,
) -> Option<WorkspaceFileViewRecord> {
    let output = serde_json::from_slice::<Value>(output_json).ok()?;
    let display_path = output.get("path")?.as_str()?.to_owned();
    let workspace_root_index =
        usize::try_from(output.get("workspace_root_index")?.as_u64()?).ok()?;
    let size_bytes = output.get("size_bytes")?.as_u64()?;
    let offset_bytes = output.get("offset_bytes")?.as_u64()?;
    let returned_bytes = output.get("returned_bytes")?.as_u64()?;
    let chunk_sha256 = output.get("chunk_sha256")?.as_str()?.to_owned();
    let metadata = current_file_metadata(resolved_path).ok()?;
    Some(WorkspaceFileViewRecord {
        schema_version: FILE_VIEW_REGISTRY_SCHEMA_VERSION,
        run_id: run_id.to_owned(),
        proposal_id: proposal_id.to_owned(),
        display_path,
        workspace_root_index,
        resolved_path: resolved_path.to_path_buf(),
        resolved_path_hash: crate::sha256_hex(resolved_path.to_string_lossy().as_bytes()),
        size_bytes,
        mtime_unix_ms: metadata.mtime_unix_ms,
        offset_bytes,
        returned_bytes,
        chunk_sha256,
        observed_at_unix_ms,
    })
}

pub(crate) fn file_view_report_output(report: &WorkspacePatchFileViewReport) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "success": false,
        "tool": "palyra.fs.apply_patch",
        "error": "stale file view guard blocked workspace patch",
        "file_view_guard": report,
        "recovery_hint": "Read the listed file paths again, then retry with fresh patch context.",
    }))
    .unwrap_or_else(|_| b"{}".to_vec())
}

pub(crate) fn attach_file_view_report_to_output(
    output_json: Vec<u8>,
    report: &WorkspacePatchFileViewReport,
) -> Vec<u8> {
    let Ok(mut output) = serde_json::from_slice::<Value>(output_json.as_slice()) else {
        return output_json;
    };
    let Value::Object(output) = &mut output else {
        return output_json;
    };
    output.insert("file_view_guard".to_owned(), json!(report));
    serde_json::to_vec(&output).unwrap_or(output_json)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TouchedPatchPath {
    path: String,
    requires_prior_read: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileMetadata {
    size_bytes: u64,
    mtime_unix_ms: Option<i64>,
}

fn current_file_metadata(path: &Path) -> Result<FileMetadata, String> {
    let metadata = fs::metadata(path)
        .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
    let mtime_unix_ms = metadata.modified().ok().and_then(system_time_to_unix_ms);
    Ok(FileMetadata { size_bytes: metadata.len(), mtime_unix_ms })
}

fn system_time_to_unix_ms(value: SystemTime) -> Option<i64> {
    let duration = value.duration_since(SystemTime::UNIX_EPOCH).ok()?;
    i64::try_from(duration.as_millis()).ok()
}

fn view_key(run_id: &str, display_path: &str) -> String {
    format!("{}:{}", run_id.trim(), normalize_patch_path(display_path))
}

fn extract_patch_touched_paths(patch_text: &str) -> Vec<TouchedPatchPath> {
    let mut paths = Vec::new();
    for line in patch_text.lines() {
        let Some(path) = path_from_patch_control_line(line) else {
            continue;
        };
        paths.push(path);
    }
    paths.sort_by(|left, right| left.path.cmp(&right.path));
    paths.dedup_by(|left, right| {
        if left.path == right.path {
            left.requires_prior_read |= right.requires_prior_read;
            true
        } else {
            false
        }
    });
    paths
}

fn path_from_patch_control_line(line: &str) -> Option<TouchedPatchPath> {
    let trimmed = line.trim();
    for (prefix, requires_prior_read) in [
        ("*** Update File:", true),
        ("*** Replace File:", true),
        ("*** Replace Line:", true),
        ("*** Delete File:", true),
        ("*** Add File:", false),
        ("--- a/", true),
        ("+++ b/", false),
    ] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let path = normalize_patch_path(rest);
            if !path.is_empty() && path != "/dev/null" {
                return Some(TouchedPatchPath { path, requires_prior_read });
            }
        }
    }
    None
}

fn normalize_patch_path(path: &str) -> String {
    let mut normalized = path.trim().replace('\\', "/");
    if let Some(stripped) = normalized.strip_prefix("a/").or_else(|| normalized.strip_prefix("b/"))
    {
        normalized = stripped.to_owned();
    }
    normalized
        .split('/')
        .filter(|component| !component.is_empty() && *component != ".")
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use std::{fs, time::SystemTime};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn registry_warns_for_patch_to_never_read_file() {
        let registry = FileViewRegistry::default();
        let report = registry.evaluate_patch(
            "run-1",
            "*** Begin Patch\n*** Update File: src/lib.rs\n@@\n-old\n+new\n*** End Patch\n",
        );

        assert!(!report.hard_block);
        assert_eq!(report.diagnostics[0].decision, WorkspacePatchFileViewDecision::Warn);
        assert_eq!(report.diagnostics[0].reason_code, "workspace_file_view.never_read");
    }

    #[test]
    fn registry_allows_patch_when_read_metadata_matches() {
        let tempdir = tempdir().expect("tempdir should be created");
        let file = tempdir.path().join("src.txt");
        fs::write(&file, "hello").expect("file should be written");
        let output = json!({
            "path": "src.txt",
            "workspace_root_index": 0,
            "offset_bytes": 0,
            "returned_bytes": 5,
            "size_bytes": 5,
            "chunk_sha256": crate::sha256_hex(b"hello"),
        });
        let record = build_workspace_file_view_record(
            "run-1",
            "proposal-read",
            &file,
            serde_json::to_vec(&output).expect("output should serialize").as_slice(),
            10,
        )
        .expect("view record should build");
        let mut registry = FileViewRegistry::default();
        registry.record_read(record);

        let report = registry.evaluate_patch(
            "run-1",
            "*** Begin Patch\n*** Update File: src.txt\n@@\n-hello\n+hello world\n*** End Patch\n",
        );

        assert!(!report.hard_block);
        assert_eq!(report.diagnostics[0].reason_code, "workspace_file_view.fresh");
        assert_eq!(
            report.diagnostics[0].previous_read_proposal_id.as_deref(),
            Some("proposal-read")
        );
    }

    #[test]
    fn registry_blocks_patch_when_file_metadata_changes_after_read() {
        let tempdir = tempdir().expect("tempdir should be created");
        let file = tempdir.path().join("src.txt");
        fs::write(&file, "hello").expect("file should be written");
        let output = json!({
            "path": "src.txt",
            "workspace_root_index": 0,
            "offset_bytes": 0,
            "returned_bytes": 5,
            "size_bytes": 5,
            "chunk_sha256": crate::sha256_hex(b"hello"),
        });
        let record = build_workspace_file_view_record(
            "run-1",
            "proposal-read",
            &file,
            serde_json::to_vec(&output).expect("output should serialize").as_slice(),
            10,
        )
        .expect("view record should build");
        fs::write(&file, "hello changed").expect("file should be changed");
        let mut registry = FileViewRegistry::default();
        registry.record_read(record);

        let report = registry.evaluate_patch(
            "run-1",
            "*** Begin Patch\n*** Update File: src.txt\n@@\n-hello\n+hello world\n*** End Patch\n",
        );

        assert!(report.hard_block);
        assert_eq!(report.diagnostics[0].reason_code, "workspace_file_view.stale_metadata");
    }

    #[test]
    fn patch_path_extractor_does_not_require_prior_read_for_add_file() {
        let paths = extract_patch_touched_paths(
            "*** Begin Patch\n*** Add File: src/new.rs\n+fn main() {}\n*** End Patch\n",
        );

        assert_eq!(
            paths,
            vec![TouchedPatchPath { path: "src/new.rs".to_owned(), requires_prior_read: false }]
        );
    }

    #[test]
    fn report_attachment_preserves_success_output_and_adds_guidance() {
        let registry = FileViewRegistry::default();
        let report = registry.evaluate_patch(
            "run-1",
            "*** Begin Patch\n*** Update File: src/lib.rs\n@@\n-old\n+new\n*** End Patch\n",
        );
        let output = attach_file_view_report_to_output(
            br#"{"patch_sha256":"abc","files_touched":[]}"#.to_vec(),
            &report,
        );
        let output: Value = serde_json::from_slice(output.as_slice())
            .expect("attached output should remain valid JSON");

        assert_eq!(output.get("patch_sha256").and_then(Value::as_str), Some("abc"));
        assert_eq!(
            output.pointer("/file_view_guard/diagnostics/0/reason_code").and_then(Value::as_str),
            Some("workspace_file_view.never_read")
        );
    }

    #[test]
    fn system_time_conversion_handles_epoch() {
        assert_eq!(system_time_to_unix_ms(SystemTime::UNIX_EPOCH), Some(0));
    }
}
