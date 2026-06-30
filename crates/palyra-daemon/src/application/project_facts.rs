//! Read-only project facts used by coding verification and diagnostics.
//!
//! The service intentionally records metadata only: it looks for known
//! manifests, touched-file languages, and suggested verification commands
//! without reading arbitrary source files or exposing absolute workspace
//! paths. Later verification milestones can consume this stable contract
//! without turning repository metadata into instructions.

use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path},
};

use palyra_common::workspace_patch::WorkspacePatchFileAttestation;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(crate) const PROJECT_FACTS_SCHEMA_VERSION: u32 = 1;
pub(crate) const PROJECT_FACTS_STARTED_EVENT: &str =
    "projectfactsservice_pro_coding_posture.started";
pub(crate) const PROJECT_FACTS_COMPLETED_EVENT: &str =
    "projectfactsservice_pro_coding_posture.completed";
pub(crate) const PROJECT_FACTS_FAILED_EVENT: &str = "projectfactsservice_pro_coding_posture.failed";

pub(crate) const PROJECT_FACTS_REDACTION_LEVEL: &str = "metadata_only";
const MAX_PACKAGE_JSON_BYTES: u64 = 128 * 1024;
const MAX_COMMAND_HINTS: usize = 16;

/// Stable reason codes emitted by the project facts service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ProjectFactsReasonCode {
    Captured,
    GeneratedPathTouched,
    HighRiskPathTouched,
    ManifestDetected,
    NoTouchedFiles,
    PackageJsonParseFailed,
    RolloutDisabled,
    SourceFilesTouched,
    VerificationRecommended,
    WorkspaceRootMissing,
}

impl ProjectFactsReasonCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Captured => "project_facts.captured",
            Self::GeneratedPathTouched => "project_facts.generated_path_touched",
            Self::HighRiskPathTouched => "project_facts.high_risk_path_touched",
            Self::ManifestDetected => "project_facts.manifest_detected",
            Self::NoTouchedFiles => "project_facts.no_touched_files",
            Self::PackageJsonParseFailed => "project_facts.package_json_parse_failed",
            Self::RolloutDisabled => "project_facts.rollout_disabled",
            Self::SourceFilesTouched => "project_facts.source_files_touched",
            Self::VerificationRecommended => "project_facts.verification_recommended",
            Self::WorkspaceRootMissing => "project_facts.workspace_root_missing",
        }
    }
}

/// Service decision for one capture attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectFactsDecision {
    Ready,
    Skipped,
    Degraded,
    Failed,
}

/// Metadata-only reference to a workspace root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectWorkspaceRootRef {
    pub(crate) index: usize,
    pub(crate) root_id_sha256: String,
    pub(crate) display_name: String,
    pub(crate) exists: bool,
}

/// Language family inferred from touched files and manifests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectLanguageFamily {
    Rust,
    TypeScript,
    JavaScript,
    Python,
    Swift,
    Kotlin,
    Markdown,
    Json,
    Toml,
    Shell,
    Powershell,
    Other,
}

/// Known manifest/config file that influences coding posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectManifestKind {
    AgentsContext,
    CargoLock,
    CargoManifest,
    Justfile,
    Makefile,
    NodeVersion,
    PackageJson,
    PackageLock,
    PalyraContext,
    Pyproject,
    Requirements,
    RustToolchain,
    TauriConfig,
    Tsconfig,
    ViteConfig,
}

/// One discovered manifest/config fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectManifestFact {
    pub(crate) kind: ProjectManifestKind,
    pub(crate) path: String,
}

/// Suggested operator command derived from manifests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectCommandKind {
    Format,
    Lint,
    Test,
    Build,
    Check,
}

/// Command hint for verification planning. It is advisory metadata, not an
/// instruction to execute anything automatically.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectCommandHint {
    pub(crate) kind: ProjectCommandKind,
    pub(crate) command: String,
    pub(crate) source: String,
    pub(crate) reason_code: String,
}

/// Touched path classification used by verification freshness checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectTouchedPathFact {
    pub(crate) path: String,
    pub(crate) operation: String,
    pub(crate) language: ProjectLanguageFamily,
    pub(crate) high_risk: bool,
    pub(crate) generated: bool,
}

/// Compact coding posture summary derived from the facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectCodingPosture {
    pub(crate) requires_verification: bool,
    pub(crate) high_risk_change: bool,
    pub(crate) generated_path_change: bool,
    pub(crate) suggested_commands: Vec<ProjectCommandHint>,
}

/// Stable project facts snapshot safe for journal payloads and tool output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectFactsSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) rollout_enabled: bool,
    pub(crate) decision: ProjectFactsDecision,
    pub(crate) workspace_root: ProjectWorkspaceRootRef,
    pub(crate) manifests: Vec<ProjectManifestFact>,
    pub(crate) languages: Vec<ProjectLanguageFamily>,
    pub(crate) touched_paths: Vec<ProjectTouchedPathFact>,
    pub(crate) coding_posture: ProjectCodingPosture,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) redaction_level: String,
}

/// Journal projection for started/completed/failed capture events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProjectFactsJournalProjection {
    pub(crate) schema_version: u32,
    pub(crate) event_type: String,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) workspace_root: ProjectWorkspaceRootRef,
    pub(crate) decision: ProjectFactsDecision,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) evidence_refs: Vec<String>,
    pub(crate) redaction_level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) snapshot: Option<ProjectFactsSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

/// Inputs for one metadata-only project facts capture.
pub(crate) struct ProjectFactsCaptureRequest<'a> {
    pub(crate) workspace_root_index: usize,
    pub(crate) workspace_root: &'a Path,
    pub(crate) files_touched: &'a [WorkspacePatchFileAttestation],
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) rollout_enabled: bool,
}

/// Read-only service for deriving project facts from bounded workspace metadata.
pub(crate) struct ProjectFactsService;

impl ProjectFactsService {
    #[must_use]
    pub(crate) fn capture(request: ProjectFactsCaptureRequest<'_>) -> ProjectFactsSnapshot {
        let root_ref = workspace_root_ref(
            request.workspace_root_index,
            request.workspace_root,
            request.workspace_root.exists(),
        );
        let mut reason_codes = BTreeSet::new();
        if !request.rollout_enabled {
            reason_codes.insert(ProjectFactsReasonCode::RolloutDisabled);
            return ProjectFactsSnapshot {
                schema_version: PROJECT_FACTS_SCHEMA_VERSION,
                generated_at_unix_ms: request.generated_at_unix_ms,
                rollout_enabled: false,
                decision: ProjectFactsDecision::Skipped,
                workspace_root: root_ref,
                manifests: Vec::new(),
                languages: Vec::new(),
                touched_paths: Vec::new(),
                coding_posture: ProjectCodingPosture {
                    requires_verification: false,
                    high_risk_change: false,
                    generated_path_change: false,
                    suggested_commands: Vec::new(),
                },
                reason_codes: render_reason_codes(reason_codes),
                redaction_level: PROJECT_FACTS_REDACTION_LEVEL.to_owned(),
            };
        }
        if !request.workspace_root.is_dir() {
            reason_codes.insert(ProjectFactsReasonCode::WorkspaceRootMissing);
            return ProjectFactsSnapshot {
                schema_version: PROJECT_FACTS_SCHEMA_VERSION,
                generated_at_unix_ms: request.generated_at_unix_ms,
                rollout_enabled: true,
                decision: ProjectFactsDecision::Failed,
                workspace_root: root_ref,
                manifests: Vec::new(),
                languages: Vec::new(),
                touched_paths: Vec::new(),
                coding_posture: ProjectCodingPosture {
                    requires_verification: false,
                    high_risk_change: false,
                    generated_path_change: false,
                    suggested_commands: Vec::new(),
                },
                reason_codes: render_reason_codes(reason_codes),
                redaction_level: PROJECT_FACTS_REDACTION_LEVEL.to_owned(),
            };
        }

        let manifests = discover_manifests(request.workspace_root);
        if !manifests.is_empty() {
            reason_codes.insert(ProjectFactsReasonCode::ManifestDetected);
        }
        let mut touched_paths = touched_path_facts(request.files_touched);
        if touched_paths.is_empty() {
            reason_codes.insert(ProjectFactsReasonCode::NoTouchedFiles);
        }
        touched_paths.sort_by(|left, right| left.path.cmp(&right.path));
        let source_files_touched =
            touched_paths.iter().any(|path| source_language_requires_verification(path.language));
        if source_files_touched {
            reason_codes.insert(ProjectFactsReasonCode::SourceFilesTouched);
        }
        let high_risk_change = touched_paths.iter().any(|path| path.high_risk);
        if high_risk_change {
            reason_codes.insert(ProjectFactsReasonCode::HighRiskPathTouched);
        }
        let generated_path_change = touched_paths.iter().any(|path| path.generated);
        if generated_path_change {
            reason_codes.insert(ProjectFactsReasonCode::GeneratedPathTouched);
        }

        let (suggested_commands, command_degraded) =
            suggested_commands(request.workspace_root, manifests.as_slice());
        if command_degraded {
            reason_codes.insert(ProjectFactsReasonCode::PackageJsonParseFailed);
        }
        let requires_verification =
            source_files_touched || high_risk_change || !manifests.is_empty();
        if requires_verification {
            reason_codes.insert(ProjectFactsReasonCode::VerificationRecommended);
        }
        reason_codes.insert(ProjectFactsReasonCode::Captured);

        let languages = language_families(manifests.as_slice(), touched_paths.as_slice());
        ProjectFactsSnapshot {
            schema_version: PROJECT_FACTS_SCHEMA_VERSION,
            generated_at_unix_ms: request.generated_at_unix_ms,
            rollout_enabled: true,
            decision: if command_degraded {
                ProjectFactsDecision::Degraded
            } else {
                ProjectFactsDecision::Ready
            },
            workspace_root: root_ref,
            manifests,
            languages,
            touched_paths,
            coding_posture: ProjectCodingPosture {
                requires_verification,
                high_risk_change,
                generated_path_change,
                suggested_commands,
            },
            reason_codes: render_reason_codes(reason_codes),
            redaction_level: PROJECT_FACTS_REDACTION_LEVEL.to_owned(),
        }
    }
}

/// Builds the metadata-only journal projection for one project facts event.
#[must_use]
pub(crate) fn project_facts_journal_projection(
    event_type: &str,
    session_id: &str,
    run_id: &str,
    snapshot: Option<ProjectFactsSnapshot>,
    workspace_root: ProjectWorkspaceRootRef,
    created_at_unix_ms: i64,
    error: Option<String>,
) -> ProjectFactsJournalProjection {
    let (decision, reason_codes, evidence_refs) = if let Some(snapshot) = snapshot.as_ref() {
        (
            snapshot.decision,
            snapshot.reason_codes.clone(),
            snapshot
                .manifests
                .iter()
                .map(|manifest| format!("manifest:{}", manifest.path))
                .chain(
                    snapshot.touched_paths.iter().map(|path| format!("touched_path:{}", path.path)),
                )
                .collect::<Vec<_>>(),
        )
    } else {
        (
            if error.is_some() {
                ProjectFactsDecision::Failed
            } else {
                ProjectFactsDecision::Ready
            },
            Vec::new(),
            Vec::new(),
        )
    };
    ProjectFactsJournalProjection {
        schema_version: PROJECT_FACTS_SCHEMA_VERSION,
        event_type: event_type.to_owned(),
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        workspace_root,
        decision,
        reason_codes,
        created_at_unix_ms,
        evidence_refs,
        redaction_level: PROJECT_FACTS_REDACTION_LEVEL.to_owned(),
        snapshot,
        error: error.map(|value| truncate_error(value.as_str())),
    }
}

/// Inserts the project facts snapshot into a successful tool output.
pub(crate) fn append_project_facts_output(
    output_value: &mut Value,
    snapshot: ProjectFactsSnapshot,
) {
    let Some(payload) = output_value.as_object_mut() else {
        return;
    };
    payload.insert(
        "coding_posture".to_owned(),
        json!({
            "schema_version": PROJECT_FACTS_SCHEMA_VERSION,
            "instruction_authority": "none",
            "project_facts": snapshot,
            "redaction_level": PROJECT_FACTS_REDACTION_LEVEL,
        }),
    );
}

#[must_use]
pub(crate) fn workspace_root_ref(
    index: usize,
    workspace_root: &Path,
    exists: bool,
) -> ProjectWorkspaceRootRef {
    ProjectWorkspaceRootRef {
        index,
        root_id_sha256: hash_workspace_root(workspace_root),
        display_name: workspace_root
            .file_name()
            .and_then(|value| value.to_str())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("workspace")
            .to_owned(),
        exists,
    }
}

fn discover_manifests(workspace_root: &Path) -> Vec<ProjectManifestFact> {
    let mut manifests = Vec::new();
    for (kind, relative_path) in known_manifest_paths() {
        if workspace_root.join(relative_path).is_file() {
            manifests.push(ProjectManifestFact { kind, path: relative_path.to_owned() });
        }
    }
    manifests
}

fn known_manifest_paths() -> Vec<(ProjectManifestKind, &'static str)> {
    vec![
        (ProjectManifestKind::CargoManifest, "Cargo.toml"),
        (ProjectManifestKind::CargoLock, "Cargo.lock"),
        (ProjectManifestKind::RustToolchain, "rust-toolchain.toml"),
        (ProjectManifestKind::PackageJson, "package.json"),
        (ProjectManifestKind::PackageJson, "apps/web/package.json"),
        (ProjectManifestKind::PackageJson, "apps/desktop/ui/package.json"),
        (ProjectManifestKind::PackageJson, "apps/browser-extension/package.json"),
        (ProjectManifestKind::PackageLock, "package-lock.json"),
        (ProjectManifestKind::NodeVersion, ".node-version"),
        (ProjectManifestKind::Tsconfig, "tsconfig.json"),
        (ProjectManifestKind::ViteConfig, "vite.config.ts"),
        (ProjectManifestKind::TauriConfig, "apps/desktop/src-tauri/tauri.conf.json"),
        (ProjectManifestKind::Pyproject, "pyproject.toml"),
        (ProjectManifestKind::Requirements, "requirements.txt"),
        (ProjectManifestKind::Justfile, "justfile"),
        (ProjectManifestKind::Makefile, "Makefile"),
        (ProjectManifestKind::AgentsContext, "AGENTS.md"),
        (ProjectManifestKind::PalyraContext, "PALYRA.md"),
    ]
}

fn touched_path_facts(
    files_touched: &[WorkspacePatchFileAttestation],
) -> Vec<ProjectTouchedPathFact> {
    files_touched
        .iter()
        .map(|attestation| {
            let normalized = normalize_relative_path(attestation.path.as_str());
            ProjectTouchedPathFact {
                language: language_from_path(normalized.as_str()),
                high_risk: high_risk_path(normalized.as_str()),
                generated: generated_path(normalized.as_str()),
                path: normalized,
                operation: attestation.operation.clone(),
            }
        })
        .collect()
}

fn language_families(
    manifests: &[ProjectManifestFact],
    touched_paths: &[ProjectTouchedPathFact],
) -> Vec<ProjectLanguageFamily> {
    let mut languages = touched_paths.iter().map(|path| path.language).collect::<BTreeSet<_>>();
    for manifest in manifests {
        match manifest.kind {
            ProjectManifestKind::CargoManifest
            | ProjectManifestKind::CargoLock
            | ProjectManifestKind::RustToolchain => {
                languages.insert(ProjectLanguageFamily::Rust);
            }
            ProjectManifestKind::PackageJson
            | ProjectManifestKind::PackageLock
            | ProjectManifestKind::NodeVersion
            | ProjectManifestKind::Tsconfig
            | ProjectManifestKind::ViteConfig
            | ProjectManifestKind::TauriConfig => {
                languages.insert(ProjectLanguageFamily::TypeScript);
            }
            ProjectManifestKind::Pyproject | ProjectManifestKind::Requirements => {
                languages.insert(ProjectLanguageFamily::Python);
            }
            ProjectManifestKind::AgentsContext
            | ProjectManifestKind::Justfile
            | ProjectManifestKind::Makefile
            | ProjectManifestKind::PalyraContext => {}
        }
    }
    languages.into_iter().collect()
}

fn suggested_commands(
    workspace_root: &Path,
    manifests: &[ProjectManifestFact],
) -> (Vec<ProjectCommandHint>, bool) {
    let mut commands = BTreeSet::<(ProjectCommandKind, String, String, String)>::new();
    let mut degraded = false;
    if manifests.iter().any(|manifest| manifest.kind == ProjectManifestKind::CargoManifest) {
        commands.insert(command_hint_tuple(
            ProjectCommandKind::Format,
            "cargo fmt --all --check",
            "Cargo.toml",
            "project_facts.rust_format_command",
        ));
        commands.insert(command_hint_tuple(
            ProjectCommandKind::Lint,
            "cargo clippy --workspace --all-targets -- -D warnings",
            "Cargo.toml",
            "project_facts.rust_lint_command",
        ));
        commands.insert(command_hint_tuple(
            ProjectCommandKind::Test,
            "cargo test --workspace --locked",
            "Cargo.toml",
            "project_facts.rust_test_command",
        ));
    }
    for manifest in
        manifests.iter().filter(|manifest| manifest.kind == ProjectManifestKind::PackageJson)
    {
        match package_json_command_hints(workspace_root, manifest.path.as_str()) {
            Ok(hints) => {
                commands.extend(
                    hints
                        .into_iter()
                        .map(|hint| (hint.kind, hint.command, hint.source, hint.reason_code)),
                );
            }
            Err(()) => {
                degraded = true;
            }
        }
    }
    if manifests.iter().any(|manifest| manifest.kind == ProjectManifestKind::Justfile) {
        commands.insert(command_hint_tuple(
            ProjectCommandKind::Check,
            "just doctor",
            "justfile",
            "project_facts.just_doctor_command",
        ));
    }
    let hints = commands
        .into_iter()
        .take(MAX_COMMAND_HINTS)
        .map(|(kind, command, source, reason_code)| ProjectCommandHint {
            kind,
            command,
            source,
            reason_code,
        })
        .collect();
    (hints, degraded)
}

fn package_json_command_hints(
    workspace_root: &Path,
    relative_path: &str,
) -> Result<Vec<ProjectCommandHint>, ()> {
    let path = workspace_root.join(relative_path);
    let metadata = fs::metadata(path.as_path()).map_err(|_| ())?;
    if metadata.len() > MAX_PACKAGE_JSON_BYTES {
        return Err(());
    }
    let raw = fs::read_to_string(path.as_path()).map_err(|_| ())?;
    let value = serde_json::from_str::<Value>(raw.as_str()).map_err(|_| ())?;
    let Some(scripts) = value.get("scripts").and_then(Value::as_object) else {
        return Ok(Vec::new());
    };
    let package_prefix = relative_path
        .strip_suffix("/package.json")
        .filter(|prefix| !prefix.is_empty())
        .map(|prefix| format!("npm --prefix {prefix} run "))
        .unwrap_or_else(|| "npm run ".to_owned());
    let mut hints = Vec::new();
    for (script, kind, reason_code) in [
        ("js:check", ProjectCommandKind::Check, "project_facts.npm_check_command"),
        ("check", ProjectCommandKind::Check, "project_facts.npm_check_command"),
        ("js:lint", ProjectCommandKind::Lint, "project_facts.npm_lint_command"),
        ("lint", ProjectCommandKind::Lint, "project_facts.npm_lint_command"),
        ("test:run", ProjectCommandKind::Test, "project_facts.npm_test_command"),
        ("test", ProjectCommandKind::Test, "project_facts.npm_test_command"),
        ("build", ProjectCommandKind::Build, "project_facts.npm_build_command"),
        ("web:ci", ProjectCommandKind::Check, "project_facts.npm_ci_command"),
        ("desktop-ui:ci", ProjectCommandKind::Check, "project_facts.npm_ci_command"),
    ] {
        if scripts.contains_key(script) {
            hints.push(ProjectCommandHint {
                kind,
                command: format!("{package_prefix}{script}"),
                source: relative_path.to_owned(),
                reason_code: reason_code.to_owned(),
            });
        }
    }
    Ok(hints)
}

fn command_hint_tuple(
    kind: ProjectCommandKind,
    command: &str,
    source: &str,
    reason_code: &str,
) -> (ProjectCommandKind, String, String, String) {
    (kind, command.to_owned(), source.to_owned(), reason_code.to_owned())
}

fn language_from_path(path: &str) -> ProjectLanguageFamily {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".rs") {
        ProjectLanguageFamily::Rust
    } else if lower.ends_with(".ts") || lower.ends_with(".tsx") {
        ProjectLanguageFamily::TypeScript
    } else if lower.ends_with(".js")
        || lower.ends_with(".jsx")
        || lower.ends_with(".mjs")
        || lower.ends_with(".cjs")
    {
        ProjectLanguageFamily::JavaScript
    } else if lower.ends_with(".py") || lower.ends_with(".pyi") {
        ProjectLanguageFamily::Python
    } else if lower.ends_with(".swift") {
        ProjectLanguageFamily::Swift
    } else if lower.ends_with(".kt") || lower.ends_with(".kts") {
        ProjectLanguageFamily::Kotlin
    } else if lower.ends_with(".md") || lower.ends_with(".mdx") {
        ProjectLanguageFamily::Markdown
    } else if lower.ends_with(".json") || lower.ends_with(".jsonl") {
        ProjectLanguageFamily::Json
    } else if lower.ends_with(".toml") {
        ProjectLanguageFamily::Toml
    } else if lower.ends_with(".sh") || lower.ends_with(".bash") {
        ProjectLanguageFamily::Shell
    } else if lower.ends_with(".ps1") || lower.ends_with(".psm1") {
        ProjectLanguageFamily::Powershell
    } else {
        ProjectLanguageFamily::Other
    }
}

fn source_language_requires_verification(language: ProjectLanguageFamily) -> bool {
    matches!(
        language,
        ProjectLanguageFamily::Rust
            | ProjectLanguageFamily::TypeScript
            | ProjectLanguageFamily::JavaScript
            | ProjectLanguageFamily::Python
            | ProjectLanguageFamily::Swift
            | ProjectLanguageFamily::Kotlin
            | ProjectLanguageFamily::Shell
            | ProjectLanguageFamily::Powershell
            | ProjectLanguageFamily::Toml
            | ProjectLanguageFamily::Json
    )
}

fn high_risk_path(path: &str) -> bool {
    path == "Cargo.lock"
        || path == "package-lock.json"
        || path == "deny.toml"
        || path == "osv-scanner.toml"
        || path == "SECURITY.md"
        || path == "rust-toolchain.toml"
        || path == "clippy.toml"
        || path == "rustfmt.toml"
        || path.starts_with(".github/")
        || path.starts_with("scripts/")
        || path.starts_with("crates/palyra-daemon/src/application/tool_runtime/")
        || path.starts_with("crates/palyra-policy/")
        || path.starts_with("crates/palyra-vault/")
        || path.starts_with("crates/palyra-safety/")
        || path.starts_with("crates/palyra-sandbox/")
}

fn generated_path(path: &str) -> bool {
    path.starts_with("schemas/generated/")
        || path.starts_with("target/")
        || path.starts_with("node_modules/")
        || path.contains("/dist/")
        || path.contains("/build/")
        || path.ends_with(".snap")
}

fn normalize_relative_path(path: &str) -> String {
    let mut parts = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(value) => {
                if let Some(part) = value.to_str().filter(|value| !value.is_empty()) {
                    parts.push(part.to_owned());
                }
            }
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                parts.push("_outside_workspace".to_owned());
            }
        }
    }
    if parts.is_empty() {
        ".".to_owned()
    } else {
        parts.join("/")
    }
}

fn hash_workspace_root(workspace_root: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(workspace_root.to_string_lossy().as_bytes());
    hex::encode(hasher.finalize())
}

fn render_reason_codes(reason_codes: BTreeSet<ProjectFactsReasonCode>) -> Vec<String> {
    reason_codes.into_iter().map(ProjectFactsReasonCode::as_str).map(str::to_owned).collect()
}

fn truncate_error(error: &str) -> String {
    const MAX_ERROR_CHARS: usize = 240;
    let trimmed = error.trim();
    if trimmed.chars().count() <= MAX_ERROR_CHARS {
        return trimmed.to_owned();
    }
    trimmed.chars().take(MAX_ERROR_CHARS).collect::<String>()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use palyra_common::workspace_patch::WorkspacePatchFileAttestation;

    use super::{
        append_project_facts_output, project_facts_journal_projection, workspace_root_ref,
        ProjectCommandKind, ProjectFactsCaptureRequest, ProjectFactsDecision, ProjectFactsService,
        ProjectLanguageFamily, ProjectManifestKind, PROJECT_FACTS_COMPLETED_EVENT,
        PROJECT_FACTS_REDACTION_LEVEL, PROJECT_FACTS_SCHEMA_VERSION,
    };

    fn touched(path: &str, operation: &str) -> WorkspacePatchFileAttestation {
        WorkspacePatchFileAttestation {
            path: path.to_owned(),
            workspace_root_index: 0,
            operation: operation.to_owned(),
            moved_from: None,
            before_sha256: None,
            before_size_bytes: None,
            after_sha256: None,
            after_size_bytes: None,
        }
    }

    #[test]
    fn captures_rust_node_posture_without_absolute_paths() {
        let temp = tempfile::tempdir().expect("tempdir should exist");
        fs::write(temp.path().join("Cargo.toml"), "[workspace]\nmembers = [\"crates/app\"]\n")
            .expect("cargo manifest should be written");
        fs::write(
            temp.path().join("package.json"),
            r#"{"scripts":{"js:check":"vite --version","test:run":"vitest run"}}"#,
        )
        .expect("package manifest should be written");

        let snapshot = ProjectFactsService::capture(ProjectFactsCaptureRequest {
            workspace_root_index: 0,
            workspace_root: temp.path(),
            files_touched: &[
                touched("crates/app/src/lib.rs", "update"),
                touched(".github/workflows/ci.yml", "update"),
            ],
            generated_at_unix_ms: 42,
            rollout_enabled: true,
        });

        assert_eq!(snapshot.schema_version, PROJECT_FACTS_SCHEMA_VERSION);
        assert_eq!(snapshot.decision, ProjectFactsDecision::Ready);
        assert!(snapshot
            .manifests
            .iter()
            .any(|manifest| manifest.kind == ProjectManifestKind::CargoManifest));
        assert!(snapshot.languages.contains(&ProjectLanguageFamily::Rust));
        assert!(snapshot.coding_posture.requires_verification);
        assert!(snapshot.coding_posture.high_risk_change);
        assert!(snapshot
            .coding_posture
            .suggested_commands
            .iter()
            .any(|hint| hint.kind == ProjectCommandKind::Lint
                && hint.command == "cargo clippy --workspace --all-targets -- -D warnings"));
        let serialized = serde_json::to_string(&snapshot).expect("snapshot should serialize");
        assert!(
            !serialized.contains(temp.path().to_string_lossy().as_ref()),
            "absolute workspace roots must be redacted from project facts"
        );
        let roundtrip = serde_json::from_str::<super::ProjectFactsSnapshot>(serialized.as_str())
            .expect("snapshot should deserialize");
        assert_eq!(roundtrip, snapshot);
    }

    #[test]
    fn missing_workspace_root_fails_closed_for_capture_contract() {
        let temp = tempfile::tempdir().expect("tempdir should exist");
        let missing = temp.path().join("missing");
        let snapshot = ProjectFactsService::capture(ProjectFactsCaptureRequest {
            workspace_root_index: 1,
            workspace_root: missing.as_path(),
            files_touched: &[touched("src/lib.rs", "update")],
            generated_at_unix_ms: 7,
            rollout_enabled: true,
        });

        assert_eq!(snapshot.decision, ProjectFactsDecision::Failed);
        assert!(snapshot
            .reason_codes
            .iter()
            .any(|code| code == "project_facts.workspace_root_missing"));
        assert!(!snapshot.coding_posture.requires_verification);
    }

    #[test]
    fn package_json_parse_failure_degrades_without_leaking_contents() {
        let temp = tempfile::tempdir().expect("tempdir should exist");
        fs::write(temp.path().join("package.json"), "{not json")
            .expect("package should be written");

        let snapshot = ProjectFactsService::capture(ProjectFactsCaptureRequest {
            workspace_root_index: 0,
            workspace_root: temp.path(),
            files_touched: &[touched("src/index.ts", "update")],
            generated_at_unix_ms: 11,
            rollout_enabled: true,
        });

        assert_eq!(snapshot.decision, ProjectFactsDecision::Degraded);
        assert!(snapshot
            .reason_codes
            .iter()
            .any(|code| code == "project_facts.package_json_parse_failed"));
        let serialized = serde_json::to_string(&snapshot).expect("snapshot should serialize");
        assert!(!serialized.contains("not json"));
    }

    #[test]
    fn journal_projection_uses_metadata_only_boundary() {
        let temp = tempfile::tempdir().expect("tempdir should exist");
        fs::write(temp.path().join("Cargo.toml"), "[workspace]\n")
            .expect("cargo manifest should be written");
        let snapshot = ProjectFactsService::capture(ProjectFactsCaptureRequest {
            workspace_root_index: 0,
            workspace_root: temp.path(),
            files_touched: &[touched("src/lib.rs", "update")],
            generated_at_unix_ms: 100,
            rollout_enabled: true,
        });
        let projection = project_facts_journal_projection(
            PROJECT_FACTS_COMPLETED_EVENT,
            "session-1",
            "run-1",
            Some(snapshot),
            workspace_root_ref(0, temp.path(), true),
            101,
            None,
        );

        let value = serde_json::to_value(&projection).expect("projection should serialize");
        assert_eq!(value["event_type"], PROJECT_FACTS_COMPLETED_EVENT);
        assert_eq!(value["redaction_level"], PROJECT_FACTS_REDACTION_LEVEL);
        assert!(value["evidence_refs"]
            .as_array()
            .expect("evidence refs should be an array")
            .iter()
            .any(|entry| entry.as_str() == Some("manifest:Cargo.toml")));
        assert!(
            !value.to_string().contains(temp.path().to_string_lossy().as_ref()),
            "journal projection must not include absolute roots"
        );
    }

    #[test]
    fn output_projection_marks_project_facts_as_non_instructional() {
        let temp = tempfile::tempdir().expect("tempdir should exist");
        fs::write(temp.path().join("Cargo.toml"), "[workspace]\n")
            .expect("cargo manifest should be written");
        let snapshot = ProjectFactsService::capture(ProjectFactsCaptureRequest {
            workspace_root_index: 0,
            workspace_root: temp.path(),
            files_touched: &[touched("src/lib.rs", "update")],
            generated_at_unix_ms: 100,
            rollout_enabled: true,
        });
        let mut output = serde_json::json!({"ok": true});
        append_project_facts_output(&mut output, snapshot);

        assert_eq!(output["coding_posture"]["instruction_authority"], "none");
        assert_eq!(output["coding_posture"]["redaction_level"], PROJECT_FACTS_REDACTION_LEVEL);
        assert_eq!(
            output["coding_posture"]["project_facts"]["schema_version"],
            PROJECT_FACTS_SCHEMA_VERSION
        );
    }
}
