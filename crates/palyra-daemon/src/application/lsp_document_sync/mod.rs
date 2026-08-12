//! Generation-aware LSP document synchronization and diagnostics verification.
//!
//! The process-backed workspace supervisor owns server lifecycles. This module
//! owns document versions and immutable owner-scoped diagnostic evidence.

mod artifacts;
pub mod contracts;
mod delta;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use artifacts::DiagnosticsArtifactStore;
pub use contracts::{
    fallback_tool_for_language, DiagnosticDocumentGenerationV2, DiagnosticRangeV2,
    DiagnosticSeverityV2, DiagnosticsArtifactRefV2, DiagnosticsBaselineDescriptorV2,
    DiagnosticsDeltaStatusV2, DiagnosticsDeltaV2, DiagnosticsFallbackPlanV2,
    DiagnosticsFallbackToolV2, LspDocumentStateV2, LspRollbackOutcomeV2, NormalizedDiagnosticV2,
    UnchangedDiagnosticV2, LSP_DOCUMENT_SYNC_SCHEMA_VERSION,
};
use delta::{
    cap_classification, classify_diagnostics, normalize_diagnostics, ClassifiedDiagnostics,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::lsp_workspace_supervisor::{
    path_to_file_uri, LspLanguageV2, LspPublishedDiagnosticsV2, LspServerHandleV2,
    LspServerLifecycleV2, LspWorkspaceSupervisor, LspWorkspaceSupervisorError,
};

const MAX_LANGUAGE_ID_BYTES: usize = 64;

/// Bounded host policy for document state and diagnostic evidence.
#[derive(Clone)]
pub struct LspDocumentSyncConfig {
    /// Owner-only artifact root.
    pub artifact_root: PathBuf,
    /// Fixed owner scope for every artifact issued by this coordinator.
    pub artifact_owner_id: String,
    /// Maximum simultaneously open documents.
    pub max_documents: usize,
    /// Maximum UTF-8 bytes per full document synchronization.
    pub max_document_bytes: usize,
    /// Maximum diagnostics accepted in one publish notification.
    pub max_diagnostics_per_document: usize,
    /// Maximum diagnostic entries exposed inline across all classifications.
    pub max_visible_delta_items: usize,
    /// Maximum bytes in one immutable artifact.
    pub max_artifact_bytes: usize,
    /// Maximum artifacts retained for this owner.
    pub max_artifacts: usize,
    /// Deadline for an exact-version publishDiagnostics notification.
    pub diagnostics_timeout: Duration,
}

/// Request to open one workspace-relative document.
#[derive(Debug, Clone)]
pub struct LspDocumentOpenRequestV2 {
    /// Active server handle.
    pub handle: LspServerHandleV2,
    /// Canonicalizable workspace root corresponding to the handle.
    pub workspace_root: PathBuf,
    /// Safe workspace-relative document path.
    pub relative_path: PathBuf,
    /// LSP language identifier selected by host policy.
    pub language_id: String,
    /// Full initial document text.
    pub text: String,
}

/// Full-content document change used by patch verification.
#[derive(Debug, Clone)]
pub struct LspDocumentChangeV2 {
    /// Safe workspace-relative document path.
    pub relative_path: PathBuf,
    /// Full post-edit text.
    pub text: String,
}

/// LSP synchronization or artifact failure.
#[derive(Debug, Error)]
pub enum LspDocumentSyncError {
    /// Bounds or owner policy is invalid.
    #[error("LSP document sync configuration is invalid")]
    InvalidConfiguration,
    /// Workspace, document, version, or language state is invalid.
    #[error("LSP document request is invalid: {0}")]
    InvalidDocument(String),
    /// Requested document is not open on the exact server handle.
    #[error("LSP document is not open")]
    DocumentNotOpen,
    /// A document is already open on the exact server handle.
    #[error("LSP document is already open")]
    DocumentAlreadyOpen,
    /// Document capacity is exhausted.
    #[error("LSP document capacity is exhausted")]
    DocumentCapacityExhausted,
    /// The provider exceeded the configured diagnostic bound.
    #[error("LSP diagnostics exceed configured bounds")]
    DiagnosticsLimitExceeded,
    /// No exact-version diagnostics are available for a baseline.
    #[error("exact-version LSP diagnostics are unavailable")]
    DiagnosticsUnavailable,
    /// The server did not publish exact-version diagnostics before the deadline.
    #[error("exact-version LSP diagnostics timed out")]
    DiagnosticsTimedOut,
    /// The process-backed server stopped or its handle became unavailable.
    #[error("LSP server became unavailable")]
    ServerUnavailable,
    /// Immutable artifact capacity is exhausted.
    #[error("LSP diagnostics artifact capacity is exhausted")]
    ArtifactCapacityExhausted,
    /// Artifact exceeds its configured bound.
    #[error("LSP diagnostics artifact exceeds configured bounds")]
    ArtifactTooLarge,
    /// Artifact identity, size, hash, or payload is invalid.
    #[error("LSP diagnostics artifact integrity check failed")]
    ArtifactIntegrity,
    /// The process-backed LSP supervisor rejected an operation.
    #[error("LSP supervisor operation failed: {0}")]
    Supervisor(String),
    /// Owner-only persistence failed.
    #[error("LSP diagnostics persistence failed: {0}")]
    Persistence(String),
    /// In-memory document state is unavailable.
    #[error("LSP document state is unavailable")]
    StateUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DocumentKey {
    handle_id: String,
    relative_path: String,
}

#[derive(Debug, Clone)]
struct OpenDocument {
    handle_id: String,
    server_generation: u64,
    language: LspLanguageV2,
    relative_path: String,
    uri: String,
    document_version: i64,
    diagnostics_version: Option<i64>,
    text: String,
    diagnostics: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactDocumentV2 {
    relative_path: String,
    uri_sha256: String,
    document_version: i64,
    raw_diagnostics: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BaselineArtifactV2 {
    schema_version: u32,
    baseline_id: String,
    handle_id: String,
    server_generation: u64,
    documents: Vec<ArtifactDocumentV2>,
    created_at_unix_ms: i64,
}

#[derive(Debug, Serialize)]
struct DeltaArtifactV2<'a> {
    schema_version: u32,
    baseline: &'a DiagnosticsArtifactRefV2,
    handle_id: &'a str,
    server_generation: u64,
    documents: &'a [ArtifactDocumentV2],
    introduced: &'a [NormalizedDiagnosticV2],
    resolved: &'a [NormalizedDiagnosticV2],
    unchanged: &'a [UnchangedDiagnosticV2],
    created_at_unix_ms: i64,
}

enum ChangeSyncOutcome {
    Synchronized(OpenDocument),
    TimedOut(OpenDocument),
}

/// Live document authority layered over a persistent workspace supervisor.
pub struct LspDocumentCoordinator {
    config: LspDocumentSyncConfig,
    supervisor: Arc<LspWorkspaceSupervisor>,
    artifacts: DiagnosticsArtifactStore,
    documents: Mutex<BTreeMap<DocumentKey, OpenDocument>>,
}

impl LspDocumentCoordinator {
    /// Opens a bounded owner-scoped coordinator.
    ///
    /// # Errors
    /// Returns an error for unsafe artifact policy or unbounded configuration.
    pub fn open(
        config: LspDocumentSyncConfig,
        supervisor: Arc<LspWorkspaceSupervisor>,
    ) -> Result<Self, LspDocumentSyncError> {
        validate_config(&config)?;
        let artifacts = DiagnosticsArtifactStore::open(
            config.artifact_root.as_path(),
            config.artifact_owner_id.as_str(),
            config.max_artifacts,
            config.max_artifact_bytes,
        )?;
        Ok(Self { config, supervisor, artifacts, documents: Mutex::new(BTreeMap::new()) })
    }

    /// Sends didOpen and waits for diagnostics carrying the exact first version.
    ///
    /// # Errors
    /// Returns an explicit supervisor timeout or validation failure. A timed-out
    /// document remains tracked so callers can close or synchronize rollback.
    pub fn open_document(
        &self,
        request: LspDocumentOpenRequestV2,
    ) -> Result<LspDocumentStateV2, LspDocumentSyncError> {
        self.assert_active_handle(&request.handle)?;
        validate_language_id(request.language_id.as_str())?;
        validate_text(request.text.as_str(), self.config.max_document_bytes)?;
        let (relative_path, uri) = resolve_document(
            request.workspace_root.as_path(),
            request.relative_path.as_path(),
            request.handle.workspace_root_sha256.as_str(),
        )?;
        let key = DocumentKey {
            handle_id: request.handle.handle_id.clone(),
            relative_path: relative_path.clone(),
        };
        {
            let documents = self.lock_documents()?;
            if documents.contains_key(&key) {
                return Err(LspDocumentSyncError::DocumentAlreadyOpen);
            }
            if documents.len() >= self.config.max_documents {
                return Err(LspDocumentSyncError::DocumentCapacityExhausted);
            }
        }
        self.supervisor
            .notify(
                request.handle.handle_id.as_str(),
                "textDocument/didOpen",
                json!({
                    "textDocument": {
                        "uri": uri,
                        "languageId": request.language_id,
                        "version": 1,
                        "text": request.text
                    }
                }),
            )
            .map_err(map_supervisor_error)?;
        let mut document = OpenDocument {
            handle_id: request.handle.handle_id,
            server_generation: request.handle.generation,
            language: request.handle.language,
            relative_path,
            uri,
            document_version: 1,
            diagnostics_version: None,
            text: request.text,
            diagnostics: Vec::new(),
        };
        self.lock_documents()?.insert(key.clone(), document.clone());
        let published = self.wait_for_exact_diagnostics(&document)?;
        apply_published(&mut document, published, self.config.max_diagnostics_per_document)?;
        self.lock_documents()?.insert(key, document.clone());
        Ok(project_state(&document))
    }

    /// Captures an immutable full-diagnostics baseline for open documents.
    ///
    /// # Errors
    /// Returns an error when a document lacks exact-version diagnostics or
    /// does not belong to the supplied active server generation.
    pub fn capture_baseline(
        &self,
        handle: &LspServerHandleV2,
        relative_paths: &[PathBuf],
    ) -> Result<DiagnosticsBaselineDescriptorV2, LspDocumentSyncError> {
        self.assert_active_handle(handle)?;
        if relative_paths.is_empty() {
            return Err(LspDocumentSyncError::InvalidDocument(
                "baseline requires at least one document".to_owned(),
            ));
        }
        let requested = normalize_unique_paths(relative_paths)?;
        let mut artifact_documents = {
            let documents = self.lock_documents()?;
            requested
                .iter()
                .map(|relative_path| {
                    let key = DocumentKey {
                        handle_id: handle.handle_id.clone(),
                        relative_path: relative_path.clone(),
                    };
                    let document =
                        documents.get(&key).ok_or(LspDocumentSyncError::DocumentNotOpen)?;
                    if document.server_generation != handle.generation
                        || document.diagnostics_version != Some(document.document_version)
                    {
                        return Err(LspDocumentSyncError::DiagnosticsUnavailable);
                    }
                    Ok(artifact_document(document))
                })
                .collect::<Result<Vec<_>, LspDocumentSyncError>>()?
        };
        artifact_documents.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        let baseline_id = format!("baseline_{}", ulid::Ulid::generate());
        let created_at_unix_ms = unix_time_ms();
        let payload = BaselineArtifactV2 {
            schema_version: LSP_DOCUMENT_SYNC_SCHEMA_VERSION,
            baseline_id: baseline_id.clone(),
            handle_id: handle.handle_id.clone(),
            server_generation: handle.generation,
            documents: artifact_documents.clone(),
            created_at_unix_ms,
        };
        let artifact = self.artifacts.write("baseline", &payload)?;
        Ok(DiagnosticsBaselineDescriptorV2 {
            schema_version: LSP_DOCUMENT_SYNC_SCHEMA_VERSION,
            baseline_id,
            handle_id: handle.handle_id.clone(),
            server_generation: handle.generation,
            documents: artifact_documents.iter().map(document_generation).collect(),
            artifact,
            created_at_unix_ms,
            reason_code: "lsp.diagnostics_baseline_captured".to_owned(),
        })
    }

    /// Synchronizes full-content changes and compares exact-generation diagnostics.
    ///
    /// The caller applies filesystem mutations. This method is the LSP half of
    /// the patch transaction and never claims verification on timeout or restart.
    ///
    /// # Errors
    /// Returns an error for invalid local state or corrupt evidence. Expected
    /// LSP timeout, crash, and generation changes are represented in the delta.
    pub fn verify_changes(
        &self,
        handle: &LspServerHandleV2,
        baseline: &DiagnosticsBaselineDescriptorV2,
        changes: &[LspDocumentChangeV2],
    ) -> Result<DiagnosticsDeltaV2, LspDocumentSyncError> {
        let baseline_payload: BaselineArtifactV2 = self.artifacts.read(&baseline.artifact)?;
        validate_baseline(baseline, &baseline_payload)?;
        if handle.handle_id != baseline.handle_id || handle.generation != baseline.server_generation
        {
            return Ok(self.unavailable_delta(
                handle,
                baseline,
                DiagnosticsDeltaStatusV2::ServerGenerationChanged,
                "lsp.diagnostics_generation_changed",
            ));
        }
        if self.assert_active_handle(handle).is_err() {
            return Ok(self.unavailable_delta(
                handle,
                baseline,
                DiagnosticsDeltaStatusV2::ServerGenerationChanged,
                "lsp.diagnostics_generation_changed",
            ));
        }
        let normalized_changes = validate_changes(changes, self.config.max_document_bytes)?;
        let baseline_paths = baseline_payload
            .documents
            .iter()
            .map(|document| document.relative_path.as_str())
            .collect::<BTreeSet<_>>();
        if normalized_changes.keys().map(String::as_str).collect::<BTreeSet<_>>() != baseline_paths
        {
            return Err(LspDocumentSyncError::InvalidDocument(
                "changes must match the captured baseline document set".to_owned(),
            ));
        }

        let mut synchronized = Vec::new();
        let mut timed_out = false;
        for (relative_path, text) in normalized_changes {
            match self.change_document(handle, relative_path.as_str(), text) {
                Ok(ChangeSyncOutcome::Synchronized(document)) => synchronized.push(document),
                Ok(ChangeSyncOutcome::TimedOut(document)) => {
                    synchronized.push(document);
                    timed_out = true;
                }
                Err(LspDocumentSyncError::ServerUnavailable) => {
                    return Ok(self.unavailable_delta_with_documents(
                        handle,
                        baseline,
                        DiagnosticsDeltaStatusV2::FallbackRequired,
                        "lsp.diagnostics_server_unavailable",
                        synchronized.as_slice(),
                    ));
                }
                Err(error) => return Err(error),
            }
        }
        synchronized.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        if timed_out {
            return Ok(self.unavailable_delta_with_documents(
                handle,
                baseline,
                DiagnosticsDeltaStatusV2::DiagnosticsTimedOut,
                "lsp.diagnostics_timeout",
                synchronized.as_slice(),
            ));
        }
        if synchronized
            .iter()
            .any(|document| document.server_generation != baseline.server_generation)
        {
            return Ok(self.unavailable_delta_with_documents(
                handle,
                baseline,
                DiagnosticsDeltaStatusV2::ServerGenerationChanged,
                "lsp.diagnostics_generation_changed",
                synchronized.as_slice(),
            ));
        }

        let after_documents = synchronized.iter().map(artifact_document).collect::<Vec<_>>();
        let full_classified =
            classify_all(baseline_payload.documents.as_slice(), after_documents.as_slice());
        let introduced_count = full_classified.introduced.len();
        let resolved_count = full_classified.resolved.len();
        let unchanged_count = full_classified.unchanged.len();
        let blocking_introduced_count = full_classified
            .introduced
            .iter()
            .filter(|diagnostic| diagnostic.severity == DiagnosticSeverityV2::Error)
            .count();
        let artifact = self.artifacts.write(
            "delta",
            &DeltaArtifactV2 {
                schema_version: LSP_DOCUMENT_SYNC_SCHEMA_VERSION,
                baseline: &baseline.artifact,
                handle_id: handle.handle_id.as_str(),
                server_generation: handle.generation,
                documents: after_documents.as_slice(),
                introduced: full_classified.introduced.as_slice(),
                resolved: full_classified.resolved.as_slice(),
                unchanged: full_classified.unchanged.as_slice(),
                created_at_unix_ms: unix_time_ms(),
            },
        )?;
        let (visible, truncated) =
            cap_classification(full_classified, self.config.max_visible_delta_items);
        let status = if blocking_introduced_count == 0 {
            DiagnosticsDeltaStatusV2::Verified
        } else {
            DiagnosticsDeltaStatusV2::BlockingDiagnostics
        };
        Ok(DiagnosticsDeltaV2 {
            schema_version: LSP_DOCUMENT_SYNC_SCHEMA_VERSION,
            baseline_id: baseline.baseline_id.clone(),
            handle_id: handle.handle_id.clone(),
            baseline_server_generation: baseline.server_generation,
            result_server_generation: Some(handle.generation),
            status,
            documents: after_documents.iter().map(document_generation).collect(),
            introduced: visible.introduced,
            resolved: visible.resolved,
            unchanged: visible.unchanged,
            introduced_count,
            resolved_count,
            unchanged_count,
            blocking_introduced_count,
            truncated,
            full_diagnostics_artifact: Some(artifact),
            fallback: None,
            reason_codes: vec![if status == DiagnosticsDeltaStatusV2::Verified {
                "lsp.diagnostics_delta_verified".to_owned()
            } else {
                "lsp.diagnostics_delta_blocking".to_owned()
            }],
        })
    }

    /// Synchronizes restored full content after a filesystem rollback.
    ///
    /// # Errors
    /// Returns an error for invalid document state or non-timeout supervisor failure.
    pub fn synchronize_rollback(
        &self,
        handle: &LspServerHandleV2,
        restored: &[LspDocumentChangeV2],
    ) -> Result<LspRollbackOutcomeV2, LspDocumentSyncError> {
        self.assert_active_handle(handle)?;
        let restored = validate_changes(restored, self.config.max_document_bytes)?;
        let mut documents = Vec::new();
        let mut synchronized = true;
        for (relative_path, text) in restored {
            match self.change_document(handle, relative_path.as_str(), text)? {
                ChangeSyncOutcome::Synchronized(document) => {
                    documents.push(project_state(&document));
                }
                ChangeSyncOutcome::TimedOut(document) => {
                    synchronized = false;
                    documents.push(project_state(&document));
                }
            }
        }
        documents.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        Ok(LspRollbackOutcomeV2 {
            server_generation: handle.generation,
            documents,
            synchronized,
            reason_code: if synchronized {
                "lsp.rollback_synchronized".to_owned()
            } else {
                "lsp.rollback_diagnostics_timeout".to_owned()
            },
        })
    }

    /// Sends didClose and removes the local version authority.
    ///
    /// # Errors
    /// Returns an error when the exact document is not open or notification fails.
    pub fn close_document(
        &self,
        handle_id: &str,
        relative_path: &Path,
    ) -> Result<(), LspDocumentSyncError> {
        let relative_path = normalize_relative_path(relative_path)?;
        let key =
            DocumentKey { handle_id: handle_id.to_owned(), relative_path: relative_path.clone() };
        let document = self
            .lock_documents()?
            .get(&key)
            .cloned()
            .ok_or(LspDocumentSyncError::DocumentNotOpen)?;
        self.supervisor
            .notify(
                handle_id,
                "textDocument/didClose",
                json!({"textDocument": {"uri": document.uri}}),
            )
            .map_err(map_supervisor_error)?;
        self.lock_documents()?.remove(&key);
        Ok(())
    }

    /// Returns current redacted state for one open document.
    ///
    /// # Errors
    /// Returns an error when the document is unknown or state is unavailable.
    pub fn document_state(
        &self,
        handle_id: &str,
        relative_path: &Path,
    ) -> Result<LspDocumentStateV2, LspDocumentSyncError> {
        let relative_path = normalize_relative_path(relative_path)?;
        let key = DocumentKey { handle_id: handle_id.to_owned(), relative_path };
        self.lock_documents()?
            .get(&key)
            .map(project_state)
            .ok_or(LspDocumentSyncError::DocumentNotOpen)
    }

    /// Deletes owner artifacts not present in the explicit retention set.
    ///
    /// # Errors
    /// Returns an error for unsafe artifact state or filesystem failure.
    pub fn gc_artifacts(&self, retained_ids: &[String]) -> Result<usize, LspDocumentSyncError> {
        self.artifacts.remove_unreferenced(retained_ids)
    }

    fn change_document(
        &self,
        handle: &LspServerHandleV2,
        relative_path: &str,
        text: String,
    ) -> Result<ChangeSyncOutcome, LspDocumentSyncError> {
        let key = DocumentKey {
            handle_id: handle.handle_id.clone(),
            relative_path: relative_path.to_owned(),
        };
        let mut document = self
            .lock_documents()?
            .get(&key)
            .cloned()
            .ok_or(LspDocumentSyncError::DocumentNotOpen)?;
        if document.server_generation != handle.generation {
            return Err(LspDocumentSyncError::DiagnosticsUnavailable);
        }
        let next_version = document.document_version.checked_add(1).ok_or_else(|| {
            LspDocumentSyncError::InvalidDocument("document version overflow".to_owned())
        })?;
        self.supervisor
            .notify(
                handle.handle_id.as_str(),
                "textDocument/didChange",
                json!({
                    "textDocument": {
                        "uri": document.uri,
                        "version": next_version
                    },
                    "contentChanges": [{"text": text}]
                }),
            )
            .map_err(map_supervisor_error)?;
        document.document_version = next_version;
        document.text = text;
        document.diagnostics_version = None;
        document.diagnostics.clear();
        self.lock_documents()?.insert(key.clone(), document.clone());
        match self.wait_for_exact_diagnostics(&document) {
            Ok(published) => {
                apply_published(
                    &mut document,
                    published,
                    self.config.max_diagnostics_per_document,
                )?;
                self.lock_documents()?.insert(key, document.clone());
                Ok(ChangeSyncOutcome::Synchronized(document))
            }
            Err(LspDocumentSyncError::DiagnosticsTimedOut) => {
                Ok(ChangeSyncOutcome::TimedOut(document))
            }
            Err(error) => Err(error),
        }
    }

    fn wait_for_exact_diagnostics(
        &self,
        document: &OpenDocument,
    ) -> Result<LspPublishedDiagnosticsV2, LspDocumentSyncError> {
        let published = self
            .supervisor
            .wait_for_diagnostics(
                document.handle_id.as_str(),
                document.uri.as_str(),
                document.document_version,
                self.config.diagnostics_timeout,
            )
            .map_err(map_supervisor_error)?;
        if published.server_generation != document.server_generation
            || published.document_version != document.document_version
        {
            return Err(LspDocumentSyncError::DiagnosticsUnavailable);
        }
        Ok(published)
    }

    fn assert_active_handle(&self, handle: &LspServerHandleV2) -> Result<(), LspDocumentSyncError> {
        if handle.lifecycle != LspServerLifecycleV2::Ready {
            return Err(LspDocumentSyncError::DiagnosticsUnavailable);
        }
        let health = self.supervisor.health().map_err(map_supervisor_error)?;
        if health.handles.iter().any(|current| {
            current.handle_id == handle.handle_id
                && current.generation == handle.generation
                && current.lifecycle == LspServerLifecycleV2::Ready
        }) {
            Ok(())
        } else {
            Err(LspDocumentSyncError::DiagnosticsUnavailable)
        }
    }

    fn unavailable_delta(
        &self,
        handle: &LspServerHandleV2,
        baseline: &DiagnosticsBaselineDescriptorV2,
        status: DiagnosticsDeltaStatusV2,
        reason_code: &str,
    ) -> DiagnosticsDeltaV2 {
        self.unavailable_delta_with_documents(handle, baseline, status, reason_code, &[])
    }

    fn unavailable_delta_with_documents(
        &self,
        handle: &LspServerHandleV2,
        baseline: &DiagnosticsBaselineDescriptorV2,
        status: DiagnosticsDeltaStatusV2,
        reason_code: &str,
        documents: &[OpenDocument],
    ) -> DiagnosticsDeltaV2 {
        DiagnosticsDeltaV2 {
            schema_version: LSP_DOCUMENT_SYNC_SCHEMA_VERSION,
            baseline_id: baseline.baseline_id.clone(),
            handle_id: handle.handle_id.clone(),
            baseline_server_generation: baseline.server_generation,
            result_server_generation: Some(handle.generation),
            status,
            documents: documents
                .iter()
                .map(artifact_document)
                .map(|document| document_generation(&document))
                .collect(),
            introduced: Vec::new(),
            resolved: Vec::new(),
            unchanged: Vec::new(),
            introduced_count: 0,
            resolved_count: 0,
            unchanged_count: 0,
            blocking_introduced_count: 0,
            truncated: false,
            full_diagnostics_artifact: None,
            fallback: Some(DiagnosticsFallbackPlanV2 {
                tool: fallback_tool_for_language(handle.language),
                command_label: fallback_tool_for_language(handle.language)
                    .command_label()
                    .to_owned(),
                reason_code: reason_code.to_owned(),
            }),
            reason_codes: vec![reason_code.to_owned()],
        }
    }

    fn lock_documents(
        &self,
    ) -> Result<MutexGuard<'_, BTreeMap<DocumentKey, OpenDocument>>, LspDocumentSyncError> {
        self.documents.lock().map_err(|_| LspDocumentSyncError::StateUnavailable)
    }
}

fn apply_published(
    document: &mut OpenDocument,
    published: LspPublishedDiagnosticsV2,
    max_diagnostics: usize,
) -> Result<(), LspDocumentSyncError> {
    if published.diagnostics.len() > max_diagnostics {
        return Err(LspDocumentSyncError::DiagnosticsLimitExceeded);
    }
    document.diagnostics_version = Some(published.document_version);
    document.diagnostics = published.diagnostics;
    Ok(())
}

fn artifact_document(document: &OpenDocument) -> ArtifactDocumentV2 {
    ArtifactDocumentV2 {
        relative_path: document.relative_path.clone(),
        uri_sha256: sha256(document.uri.as_bytes()),
        document_version: document.document_version,
        raw_diagnostics: document.diagnostics.clone(),
    }
}

fn document_generation(document: &ArtifactDocumentV2) -> DiagnosticDocumentGenerationV2 {
    DiagnosticDocumentGenerationV2 {
        relative_path: document.relative_path.clone(),
        uri_sha256: document.uri_sha256.clone(),
        document_version: document.document_version,
        diagnostic_count: document.raw_diagnostics.len(),
        diagnostics_sha256: diagnostics_sha256(document.raw_diagnostics.as_slice()),
    }
}

fn project_state(document: &OpenDocument) -> LspDocumentStateV2 {
    LspDocumentStateV2 {
        handle_id: document.handle_id.clone(),
        server_generation: document.server_generation,
        language: document.language,
        relative_path: document.relative_path.clone(),
        uri_sha256: sha256(document.uri.as_bytes()),
        document_version: document.document_version,
        diagnostics_version: document.diagnostics_version,
        diagnostic_count: document.diagnostics.len(),
        reason_code: if document.diagnostics_version == Some(document.document_version) {
            "lsp.document_synchronized".to_owned()
        } else {
            "lsp.document_diagnostics_pending".to_owned()
        },
    }
}

fn classify_all(
    before: &[ArtifactDocumentV2],
    after: &[ArtifactDocumentV2],
) -> ClassifiedDiagnostics {
    let before_by_path = before
        .iter()
        .map(|document| (document.relative_path.as_str(), document))
        .collect::<BTreeMap<_, _>>();
    let mut aggregate = ClassifiedDiagnostics {
        introduced: Vec::new(),
        resolved: Vec::new(),
        unchanged: Vec::new(),
    };
    for current in after {
        let Some(baseline) = before_by_path.get(current.relative_path.as_str()) else {
            continue;
        };
        let baseline = normalize_diagnostics(
            baseline.relative_path.as_str(),
            baseline.raw_diagnostics.as_slice(),
        );
        let current = normalize_diagnostics(
            current.relative_path.as_str(),
            current.raw_diagnostics.as_slice(),
        );
        let mut classified = classify_diagnostics(baseline.as_slice(), current.as_slice());
        aggregate.introduced.append(&mut classified.introduced);
        aggregate.resolved.append(&mut classified.resolved);
        aggregate.unchanged.append(&mut classified.unchanged);
    }
    aggregate
}

fn validate_baseline(
    descriptor: &DiagnosticsBaselineDescriptorV2,
    payload: &BaselineArtifactV2,
) -> Result<(), LspDocumentSyncError> {
    if descriptor.schema_version != LSP_DOCUMENT_SYNC_SCHEMA_VERSION
        || payload.schema_version != LSP_DOCUMENT_SYNC_SCHEMA_VERSION
        || descriptor.baseline_id != payload.baseline_id
        || descriptor.handle_id != payload.handle_id
        || descriptor.server_generation != payload.server_generation
    {
        return Err(LspDocumentSyncError::ArtifactIntegrity);
    }
    let payload_documents = payload.documents.iter().map(document_generation).collect::<Vec<_>>();
    if payload_documents != descriptor.documents {
        return Err(LspDocumentSyncError::ArtifactIntegrity);
    }
    Ok(())
}

fn validate_config(config: &LspDocumentSyncConfig) -> Result<(), LspDocumentSyncError> {
    if !config.artifact_root.is_absolute()
        || config.artifact_owner_id.trim().is_empty()
        || config.artifact_owner_id.len() > 256
        || config.max_documents == 0
        || config.max_document_bytes == 0
        || config.max_diagnostics_per_document == 0
        || config.max_visible_delta_items == 0
        || config.max_artifact_bytes == 0
        || config.max_artifacts == 0
        || config.diagnostics_timeout.is_zero()
    {
        return Err(LspDocumentSyncError::InvalidConfiguration);
    }
    Ok(())
}

fn validate_language_id(language_id: &str) -> Result<(), LspDocumentSyncError> {
    if language_id.trim().is_empty()
        || language_id.len() > MAX_LANGUAGE_ID_BYTES
        || language_id.chars().any(char::is_control)
    {
        return Err(LspDocumentSyncError::InvalidDocument(
            "language id must be non-empty, bounded, and free of control characters".to_owned(),
        ));
    }
    Ok(())
}

fn validate_text(text: &str, max_document_bytes: usize) -> Result<(), LspDocumentSyncError> {
    if text.len() > max_document_bytes {
        return Err(LspDocumentSyncError::InvalidDocument(
            "document text exceeds configured bounds".to_owned(),
        ));
    }
    Ok(())
}

fn validate_changes(
    changes: &[LspDocumentChangeV2],
    max_document_bytes: usize,
) -> Result<BTreeMap<String, String>, LspDocumentSyncError> {
    if changes.is_empty() {
        return Err(LspDocumentSyncError::InvalidDocument(
            "at least one document change is required".to_owned(),
        ));
    }
    let mut normalized = BTreeMap::new();
    for change in changes {
        let relative_path = normalize_relative_path(change.relative_path.as_path())?;
        validate_text(change.text.as_str(), max_document_bytes)?;
        if normalized.insert(relative_path, change.text.clone()).is_some() {
            return Err(LspDocumentSyncError::InvalidDocument(
                "duplicate document change".to_owned(),
            ));
        }
    }
    Ok(normalized)
}

fn normalize_unique_paths(paths: &[PathBuf]) -> Result<Vec<String>, LspDocumentSyncError> {
    let mut normalized = paths
        .iter()
        .map(|path| normalize_relative_path(path.as_path()))
        .collect::<Result<Vec<_>, _>>()?;
    normalized.sort();
    if normalized.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(LspDocumentSyncError::InvalidDocument(
            "duplicate baseline document".to_owned(),
        ));
    }
    Ok(normalized)
}

fn resolve_document(
    workspace_root: &Path,
    relative_path: &Path,
    expected_workspace_sha256: &str,
) -> Result<(String, String), LspDocumentSyncError> {
    if !workspace_root.is_absolute() || !workspace_root.is_dir() {
        return Err(LspDocumentSyncError::InvalidDocument(
            "workspace root must be an existing absolute directory".to_owned(),
        ));
    }
    reject_link(workspace_root)?;
    let workspace_root = workspace_root.canonicalize().map_err(|error| {
        LspDocumentSyncError::InvalidDocument(format!("workspace canonicalization failed: {error}"))
    })?;
    if sha256_path(workspace_root.as_path()) != expected_workspace_sha256 {
        return Err(LspDocumentSyncError::InvalidDocument(
            "workspace root does not match the server handle".to_owned(),
        ));
    }
    let relative_path = normalize_relative_path(relative_path)?;
    let target = workspace_root.join(relative_path.replace('/', std::path::MAIN_SEPARATOR_STR));
    reject_existing_links_between(workspace_root.as_path(), target.as_path())?;
    if target.exists() {
        let canonical = target.canonicalize().map_err(|error| {
            LspDocumentSyncError::InvalidDocument(format!(
                "document canonicalization failed: {error}"
            ))
        })?;
        if !canonical.starts_with(workspace_root.as_path()) {
            return Err(LspDocumentSyncError::InvalidDocument(
                "document escapes the workspace root".to_owned(),
            ));
        }
    }
    let uri = path_to_file_uri(target.as_path());
    Ok((relative_path, uri))
}

fn normalize_relative_path(path: &Path) -> Result<String, LspDocumentSyncError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(LspDocumentSyncError::InvalidDocument(
            "document path must be a safe non-empty relative path".to_owned(),
        ));
    }
    let normalized = path.to_string_lossy().replace('\\', "/");
    if normalized.len() > 4_096 || normalized.chars().any(char::is_control) {
        return Err(LspDocumentSyncError::InvalidDocument(
            "document path exceeds configured safety bounds".to_owned(),
        ));
    }
    Ok(normalized)
}

fn reject_existing_links_between(root: &Path, target: &Path) -> Result<(), LspDocumentSyncError> {
    let relative = target.strip_prefix(root).map_err(|_| {
        LspDocumentSyncError::InvalidDocument("document escapes the workspace root".to_owned())
    })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        if !current.exists() {
            continue;
        }
        reject_link(current.as_path())?;
    }
    Ok(())
}

fn reject_link(path: &Path) -> Result<(), LspDocumentSyncError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        LspDocumentSyncError::InvalidDocument(format!("document metadata failed: {error}"))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(LspDocumentSyncError::InvalidDocument(
            "workspace documents cannot traverse symbolic links".to_owned(),
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(LspDocumentSyncError::InvalidDocument(
                "workspace documents cannot traverse reparse points".to_owned(),
            ));
        }
    }
    Ok(())
}

fn map_supervisor_error(error: LspWorkspaceSupervisorError) -> LspDocumentSyncError {
    match error {
        LspWorkspaceSupervisorError::RequestTimeout => LspDocumentSyncError::DiagnosticsTimedOut,
        LspWorkspaceSupervisorError::ServerCrashed
        | LspWorkspaceSupervisorError::ServerUnavailable
        | LspWorkspaceSupervisorError::HandleNotFound
        | LspWorkspaceSupervisorError::CircuitOpen(_) => LspDocumentSyncError::ServerUnavailable,
        other => LspDocumentSyncError::Supervisor(other.to_string()),
    }
}

fn diagnostics_sha256(diagnostics: &[Value]) -> String {
    serde_json::to_vec(diagnostics)
        .map(|bytes| sha256(bytes.as_slice()))
        .unwrap_or_else(|_| sha256(b"invalid-diagnostics"))
}

fn sha256_path(path: &Path) -> String {
    sha256(path.as_os_str().to_string_lossy().as_bytes())
}

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn unix_time_ms() -> i64 {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis())
        .unwrap_or(i64::MAX)
}
