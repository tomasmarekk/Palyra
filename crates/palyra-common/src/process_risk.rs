//! Advisory risk classification for `palyra.process.run` command payloads.
//!
//! This module does not enforce sandbox or path policy. It describes host-wide side effects
//! before/after execution so tool outputs and diagnostics can surface blast radius.

use std::{collections::BTreeSet, fs, path::Path};

use serde::Serialize;

use crate::process_runner_input::ProcessRunnerToolInput;

/// Finding target used when a Windows host command cannot preserve Linux/Docker POSIX file-mode
/// semantics for generated artifacts.
pub const TARGET_HOST_WINDOWS_VS_DOCKER_POSIX_PERMISSIONS: &str =
    "host_windows_vs_docker_posix_permissions";

/// Risk category detected in a process-run command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessRiskClass {
    /// Command may install or mutate packages in a host-global package namespace.
    HostPackageManagerMutation,
    /// Package manager command does not show a workspace-local or target-runtime isolation boundary.
    PackageManagerWithoutIsolation,
    /// Docker command mutates host-global images, volumes, registry state, or system cache.
    DockerGlobalMutation,
    /// Docker command publishes container ports onto the host network namespace.
    DockerHostPortPublish,
    /// Command reads or writes the user's SSH or credential namespace.
    CredentialNamespaceMutation,
    /// Command appears to move private credential material across a container or remote boundary.
    CredentialMaterialExport,
    /// Command mutates host service-manager state.
    SystemServiceMutation,
    /// Command performs recursive or forceful filesystem deletion.
    DestructiveFilesystemOperation,
    /// Host command may not affect the inferred verifier or target runtime.
    TargetRuntimeMismatch,
}

impl ProcessRiskClass {
    /// Stable JSON spelling used in advisory metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HostPackageManagerMutation => "host_package_manager_mutation",
            Self::PackageManagerWithoutIsolation => "package_manager_without_isolation",
            Self::DockerGlobalMutation => "docker_global_mutation",
            Self::DockerHostPortPublish => "docker_host_port_publish",
            Self::CredentialNamespaceMutation => "credential_namespace_mutation",
            Self::CredentialMaterialExport => "credential_material_export",
            Self::SystemServiceMutation => "system_service_mutation",
            Self::DestructiveFilesystemOperation => "destructive_filesystem_operation",
            Self::TargetRuntimeMismatch => "target_runtime_mismatch",
        }
    }
}

/// Operator-facing severity for a process risk finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessRiskSeverity {
    /// Informational finding with no host-wide blast radius detected.
    Low,
    /// Risk is visible but usually recoverable without host-wide mutation.
    Medium,
    /// Risk can affect host-global state, credentials, services, images, or ports.
    High,
}

impl ProcessRiskSeverity {
    /// Stable JSON spelling used in advisory metadata.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

/// One detected command-risk finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProcessRiskFinding {
    /// Stable category for this finding.
    pub risk_class: ProcessRiskClass,
    /// Highest expected blast radius for this finding.
    pub severity: ProcessRiskSeverity,
    /// Short operator-facing explanation of the detected pattern.
    pub message: String,
    /// Package manager, runtime, or tool detected from command and args.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_manager: Option<String>,
    /// Host namespace, runtime, or resource likely affected by the command.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Safer default workflow the agent can choose without user approval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safer_default: Option<String>,
    /// Cleanup guidance that should be surfaced without deleting host resources automatically.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_hint: Option<String>,
    /// Paths that may be read or mutated by the command.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub affected_paths: Vec<String>,
    /// Paths the command appears likely to create.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub created_paths: Vec<String>,
    /// Best-effort cleanup commands or placeholders for resources created by the command.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cleanup_commands: Vec<String>,
    /// Host ports published by a Docker command.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub host_ports: Vec<u16>,
    /// True when command text or path patterns imply private credential export.
    #[serde(skip_serializing_if = "is_false")]
    pub exported_material: bool,
}

/// Inferred task runtime boundary used to detect host/runtime mismatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TargetRuntimeBoundary {
    /// Runtime kind inferred for the task, such as `docker`.
    pub kind: String,
    /// File or metadata source that provided the boundary signal.
    pub source: String,
    /// Human-readable evidence captured from the source.
    pub evidence: String,
}

/// Advisory report attached to process-run outputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProcessRiskReport {
    /// Report schema version for downstream clients.
    pub schema_version: u8,
    /// Enforcement posture; currently always `advisory_only`.
    pub policy: String,
    /// True because the classifier must not block execution.
    pub execution_allowed: bool,
    /// False because the classifier is visibility-only, not a deny gate.
    pub blocks_execution: bool,
    /// False because Task 05 must work out of the box without user approval.
    pub requires_user_approval: bool,
    /// Highest severity across all findings, or `Low` when none were detected.
    pub highest_severity: ProcessRiskSeverity,
    /// Inferred verifier or task runtime, when a boundary is visible from workspace files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_runtime: Option<TargetRuntimeBoundary>,
    /// Ordered list of command-risk findings.
    pub findings: Vec<ProcessRiskFinding>,
}

impl ProcessRiskReport {
    /// True when no elevated process risks were detected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.findings.is_empty()
    }
}

/// Filesystem context for classifying a process-run command.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessRiskContext<'a> {
    /// Workspace root used for target-runtime inference and workspace-venv detection.
    pub workspace_root: Option<&'a Path>,
    /// Resolved working directory selected for the process invocation.
    pub resolved_cwd: Option<&'a Path>,
}

/// Classify advisory process risks without denying execution.
#[must_use]
pub fn classify_process_run(
    input: &ProcessRunnerToolInput,
    context: ProcessRiskContext<'_>,
) -> ProcessRiskReport {
    let signature = CommandSignature::new(input);
    let target_runtime = infer_target_runtime(context.workspace_root);
    let mut findings = Vec::new();

    classify_package_manager(input, context, &signature, &mut findings);
    classify_docker(&signature, &mut findings);
    classify_credentials(input, &signature, &mut findings);
    classify_system_service_mutation(&signature, &mut findings);
    classify_destructive_filesystem_operation(&signature, &mut findings);
    classify_target_runtime_mismatch(&signature, target_runtime.as_ref(), &mut findings);

    let highest_severity =
        findings.iter().map(|finding| finding.severity).max().unwrap_or(ProcessRiskSeverity::Low);
    ProcessRiskReport {
        schema_version: 1,
        policy: "advisory_only".to_owned(),
        execution_allowed: true,
        blocks_execution: false,
        requires_user_approval: false,
        highest_severity,
        target_runtime,
        findings,
    }
}

fn classify_package_manager(
    input: &ProcessRunnerToolInput,
    context: ProcessRiskContext<'_>,
    signature: &CommandSignature,
    findings: &mut Vec<ProcessRiskFinding>,
) {
    if let Some(manager) = detected_pip_install_manager(signature) {
        if uses_workspace_python_isolation(input, context) {
            return;
        }
        findings.push(ProcessRiskFinding {
            risk_class: ProcessRiskClass::HostPackageManagerMutation,
            severity: ProcessRiskSeverity::High,
            message: "Python package install targets the host interpreter instead of a workspace virtual environment or target container".to_owned(),
            detected_manager: Some(manager),
            target: Some("global_host_python".to_owned()),
            safer_default: Some("create a workspace .venv or run the install inside the target container".to_owned()),
            cleanup_hint: Some("pip uninstall may affect the user's host environment; show exact package names before cleanup and avoid automatic host cleanup".to_owned()),
            affected_paths: Vec::new(),
            created_paths: Vec::new(),
            cleanup_commands: Vec::new(),
            host_ports: Vec::new(),
            exported_material: false,
        });
    }

    if let Some(manager) = detected_system_package_manager(signature) {
        findings.push(ProcessRiskFinding {
            risk_class: ProcessRiskClass::HostPackageManagerMutation,
            severity: ProcessRiskSeverity::High,
            message: "System package manager command can mutate the host or WSL runtime".to_owned(),
            detected_manager: Some(manager),
            target: Some("host_system_package_database".to_owned()),
            safer_default: Some("install inside the task container or a workspace-local runtime image".to_owned()),
            cleanup_hint: Some("package-manager cleanup may remove user-installed software; show exact package names before cleanup".to_owned()),
            affected_paths: Vec::new(),
            created_paths: Vec::new(),
            cleanup_commands: Vec::new(),
            host_ports: Vec::new(),
            exported_material: false,
        });
    }

    if let Some((manager, target)) = detected_global_language_package_install(signature) {
        findings.push(ProcessRiskFinding {
            risk_class: ProcessRiskClass::HostPackageManagerMutation,
            severity: ProcessRiskSeverity::High,
            message: "Language package manager command installs into a global host namespace".to_owned(),
            detected_manager: Some(manager),
            target: Some(target),
            safer_default: Some("prefer a workspace-local dependency directory, virtual environment, or target container".to_owned()),
            cleanup_hint: Some("global package removal can affect other projects; capture exact package names before cleanup".to_owned()),
            affected_paths: Vec::new(),
            created_paths: Vec::new(),
            cleanup_commands: Vec::new(),
            host_ports: Vec::new(),
            exported_material: false,
        });
    }
}

fn classify_docker(signature: &CommandSignature, findings: &mut Vec<ProcessRiskFinding>) {
    let Some(docker) = DockerInvocation::new(signature) else {
        return;
    };

    if let Some(target) = docker.global_mutation_target() {
        findings.push(ProcessRiskFinding {
            risk_class: ProcessRiskClass::DockerGlobalMutation,
            severity: ProcessRiskSeverity::High,
            message: "Docker command mutates host-global images, volumes, registry state, or Docker system cache".to_owned(),
            detected_manager: Some("docker".to_owned()),
            target: Some(target),
            safer_default: Some("use disposable task containers and unique local tags scoped to the scenario".to_owned()),
            cleanup_hint: Some("record changed image tags, volumes, and containers before cleanup; avoid rewriting official-looking tags".to_owned()),
            affected_paths: Vec::new(),
            created_paths: Vec::new(),
            cleanup_commands: Vec::new(),
            host_ports: Vec::new(),
            exported_material: false,
        });
    }

    let host_ports = docker.host_published_ports();
    if !host_ports.is_empty() {
        findings.push(ProcessRiskFinding {
            risk_class: ProcessRiskClass::DockerHostPortPublish,
            severity: ProcessRiskSeverity::High,
            message: "Docker run publishes container ports onto the host network namespace".to_owned(),
            detected_manager: Some("docker".to_owned()),
            target: Some("host_network_ports".to_owned()),
            safer_default: Some("bind only to loopback, use random high host ports, or run verifier probes inside the container network".to_owned()),
            cleanup_hint: Some("stop and remove the publishing container before handing off the run".to_owned()),
            affected_paths: Vec::new(),
            created_paths: Vec::new(),
            cleanup_commands: Vec::new(),
            host_ports,
            exported_material: false,
        });
    }
}

fn classify_credentials(
    input: &ProcessRunnerToolInput,
    signature: &CommandSignature,
    findings: &mut Vec<ProcessRiskFinding>,
) {
    let credential_paths = collect_credential_paths(input, signature);
    let contains_private_material = signature.contains_private_key_material();
    let exports_private_key = contains_private_material
        || (signature.is_copy_to_external_boundary() && signature.references_private_key_path());

    if exports_private_key {
        findings.push(ProcessRiskFinding {
            risk_class: ProcessRiskClass::CredentialMaterialExport,
            severity: ProcessRiskSeverity::High,
            message: "Command appears to copy or embed private credential material outside its current namespace".to_owned(),
            detected_manager: None,
            target: Some("credential_material_export".to_owned()),
            safer_default: Some("use short-lived test credentials generated inside the target runtime and never copy private keys into containers or remotes".to_owned()),
            cleanup_hint: Some("rotate or delete any exported test key material and remove copied files from the target runtime".to_owned()),
            affected_paths: credential_paths.clone(),
            created_paths: Vec::new(),
            cleanup_commands: credential_cleanup_commands(credential_paths.as_slice()),
            host_ports: Vec::new(),
            exported_material: true,
        });
    }

    if signature.mutates_credential_namespace() || !credential_paths.is_empty() {
        findings.push(ProcessRiskFinding {
            risk_class: ProcessRiskClass::CredentialNamespaceMutation,
            severity: ProcessRiskSeverity::High,
            message: "Command touches the user's SSH or credential namespace".to_owned(),
            detected_manager: None,
            target: Some("user_credential_namespace".to_owned()),
            safer_default: Some("place test keys under the workspace or generate credentials inside the target container".to_owned()),
            cleanup_hint: Some("show generated test keys and SSH config entries before cleanup; do not delete user credentials automatically".to_owned()),
            affected_paths: credential_paths.clone(),
            created_paths: credential_paths,
            cleanup_commands: Vec::new(),
            host_ports: Vec::new(),
            exported_material: false,
        });
    }
}

fn classify_system_service_mutation(
    signature: &CommandSignature,
    findings: &mut Vec<ProcessRiskFinding>,
) {
    if !signature.mutates_system_service() {
        return;
    }
    findings.push(ProcessRiskFinding {
        risk_class: ProcessRiskClass::SystemServiceMutation,
        severity: ProcessRiskSeverity::High,
        message: "Command can mutate host system service state".to_owned(),
        detected_manager: Some(signature.command_name.clone()),
        target: Some("host_service_manager".to_owned()),
        safer_default: Some("use a workspace-owned foreground process or task container service instead".to_owned()),
        cleanup_hint: Some("restore previous service state explicitly; do not assume restart/enable changes are disposable".to_owned()),
        affected_paths: Vec::new(),
        created_paths: Vec::new(),
        cleanup_commands: Vec::new(),
        host_ports: Vec::new(),
        exported_material: false,
    });
}

fn classify_destructive_filesystem_operation(
    signature: &CommandSignature,
    findings: &mut Vec<ProcessRiskFinding>,
) {
    let Some(target) = signature.destructive_filesystem_target() else {
        return;
    };
    findings.push(ProcessRiskFinding {
        risk_class: ProcessRiskClass::DestructiveFilesystemOperation,
        severity: ProcessRiskSeverity::High,
        message: "Command performs a recursive or forceful filesystem deletion".to_owned(),
        detected_manager: None,
        target: Some(target),
        safer_default: Some(
            "delete only explicit workspace-local paths after listing them first".to_owned(),
        ),
        cleanup_hint: Some(
            "capture the exact deletion target and operator intent before cleanup".to_owned(),
        ),
        affected_paths: Vec::new(),
        created_paths: Vec::new(),
        cleanup_commands: Vec::new(),
        host_ports: Vec::new(),
        exported_material: false,
    });
}

fn classify_target_runtime_mismatch(
    signature: &CommandSignature,
    target_runtime: Option<&TargetRuntimeBoundary>,
    findings: &mut Vec<ProcessRiskFinding>,
) {
    if target_runtime.map(|runtime| runtime.kind.as_str()) != Some("docker") {
        return;
    }
    let requires_posix_permissions = signature.requires_docker_posix_file_mode_semantics();
    if !requires_posix_permissions && !signature.host_runtime_command_can_miss_docker_target() {
        return;
    }
    let (severity, message, target, safer_default, cleanup_hint) = if requires_posix_permissions {
        (
            ProcessRiskSeverity::High,
            "Windows host command cannot satisfy inferred Docker/Linux POSIX file-mode semantics",
            TARGET_HOST_WINDOWS_VS_DOCKER_POSIX_PERMISSIONS,
            "run the command inside the target Docker/Linux runtime or stop before creating host files",
            "do not treat Windows-created files as verifier-ready POSIX permission artifacts",
        )
    } else {
        (
            ProcessRiskSeverity::Medium,
            "Host runtime command may not affect the inferred Docker verifier runtime",
            "host_runtime_vs_docker_target",
            "run dependency installs and validation inside the target Docker container or image build",
            "do not treat host package installs as verifier setup; document any host changes separately",
        )
    };
    findings.push(ProcessRiskFinding {
        risk_class: ProcessRiskClass::TargetRuntimeMismatch,
        severity,
        message: message.to_owned(),
        detected_manager: Some(signature.command_name.clone()),
        target: Some(target.to_owned()),
        safer_default: Some(safer_default.to_owned()),
        cleanup_hint: Some(cleanup_hint.to_owned()),
        affected_paths: Vec::new(),
        created_paths: Vec::new(),
        cleanup_commands: Vec::new(),
        host_ports: Vec::new(),
        exported_material: false,
    });
}

fn detected_pip_install_manager(signature: &CommandSignature) -> Option<String> {
    let command = signature.command_name.as_str();
    if command.starts_with("pip") && signature.args_contain("install") {
        return Some("pip".to_owned());
    }
    if is_python_command(command)
        && signature.args_window_matches(&["-m", "pip"])
        && signature.args_contain("install")
    {
        return Some("python -m pip".to_owned());
    }
    if command == "uv" && signature.args_window_matches(&["pip", "install"]) {
        return Some("uv pip".to_owned());
    }
    if signature.shell_text_contains_any(&[
        "pip install",
        "python -m pip install",
        "uv pip install",
    ]) {
        return Some("shell pip".to_owned());
    }
    None
}

fn detected_system_package_manager(signature: &CommandSignature) -> Option<String> {
    let command = signature.command_name.as_str();
    let manager = match command {
        "apt" | "apt-get" | "dnf" | "yum" | "brew" | "scoop" | "choco" => {
            signature.args_contain("install").then_some(command)
        }
        "apk" => signature.args_contain("add").then_some(command),
        "pacman" => {
            signature.args.iter().any(|arg| arg == "-s" || arg.contains('s')).then_some(command)
        }
        _ => None,
    };
    manager
        .or_else(|| {
            signature
                .shell_text_contains_any(&[
                    "apt-get install",
                    "apt install",
                    "dnf install",
                    "yum install",
                    "pacman -s",
                    "apk add",
                    "brew install",
                    "scoop install",
                    "choco install",
                ])
                .then_some("shell package manager")
        })
        .map(str::to_owned)
}

fn detected_global_language_package_install(
    signature: &CommandSignature,
) -> Option<(String, String)> {
    let command = signature.command_name.as_str();
    match command {
        "npm"
            if signature.args_contain_any(&["install", "i"])
                && signature.args_contain_any(&["-g", "--global"]) =>
        {
            Some(("npm".to_owned(), "global_node_prefix".to_owned()))
        }
        "pnpm"
            if signature.args_contain_any(&["add", "install"])
                && signature.args_contain_any(&["-g", "--global"]) =>
        {
            Some(("pnpm".to_owned(), "global_node_prefix".to_owned()))
        }
        "yarn" if signature.args_contain("global") => {
            Some(("yarn".to_owned(), "global_node_prefix".to_owned()))
        }
        "cargo" if signature.args_contain("install") => {
            Some(("cargo".to_owned(), "cargo_home_bin".to_owned()))
        }
        "go" if signature.args_contain("install") => {
            Some(("go".to_owned(), "gopath_bin_or_go_bin".to_owned()))
        }
        "gem" if signature.args_contain("install") => {
            Some(("gem".to_owned(), "global_ruby_gems".to_owned()))
        }
        "cpan" | "cpanm" | "cpanminus" => {
            Some((command.to_owned(), "global_perl_library".to_owned()))
        }
        _ if signature.shell_text_contains_any(&[
            "npm install -g",
            "npm i -g",
            "pnpm add -g",
            "pnpm install -g",
            "yarn global",
            "cargo install",
            "go install",
            "gem install",
            "cpanm ",
        ]) =>
        {
            Some((
                "shell language package manager".to_owned(),
                "global_host_language_runtime".to_owned(),
            ))
        }
        _ => None,
    }
}

fn uses_workspace_python_isolation(
    input: &ProcessRunnerToolInput,
    context: ProcessRiskContext<'_>,
) -> bool {
    if input.env.iter().any(|(key, value)| {
        key.eq_ignore_ascii_case("VIRTUAL_ENV")
            && path_mentions_workspace_venv(value, context.workspace_root)
    }) {
        return true;
    }
    let raw_command_and_args = std::iter::once(input.command.as_str())
        .chain(input.args.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" ");
    if path_mentions_workspace_venv(raw_command_and_args.as_str(), context.workspace_root) {
        return true;
    }
    context
        .resolved_cwd
        .is_some_and(|cwd| path_mentions_python_venv(cwd.to_string_lossy().as_ref()))
}

fn path_mentions_workspace_venv(value: &str, workspace_root: Option<&Path>) -> bool {
    let normalized = normalize_text_path(value);
    if !path_mentions_python_venv(normalized.as_str()) {
        return false;
    }
    workspace_root.is_none_or(|root| {
        let root = normalize_text_path(root.to_string_lossy().as_ref());
        root.is_empty() || normalized.starts_with(root.as_str())
    })
}

fn path_mentions_python_venv(value: &str) -> bool {
    let normalized = normalize_text_path(value);
    if normalized.contains("/.venv/") || normalized.ends_with("/.venv") {
        return true;
    }
    normalized.contains("/venv/") || normalized.ends_with("/venv")
}

fn infer_target_runtime(workspace_root: Option<&Path>) -> Option<TargetRuntimeBoundary> {
    let root = workspace_root?;
    infer_task_toml_runtime(root)
        .or_else(|| infer_dockerfile_runtime(root))
        .or_else(|| infer_readme_runtime(root))
}

fn infer_task_toml_runtime(root: &Path) -> Option<TargetRuntimeBoundary> {
    let path = root.join("task.toml");
    let content = fs::read_to_string(path.as_path()).ok()?;
    let lower = content.to_ascii_lowercase();
    if lower.contains("docker") || lower.contains("image") || lower.contains("/app") {
        return Some(TargetRuntimeBoundary {
            kind: "docker".to_owned(),
            source: "task.toml".to_owned(),
            evidence: "task metadata references Docker image or /app runtime".to_owned(),
        });
    }
    None
}

fn infer_dockerfile_runtime(root: &Path) -> Option<TargetRuntimeBoundary> {
    let candidates = [root.join("Dockerfile"), root.join("environment").join("Dockerfile")];
    candidates.iter().find(|path| path.is_file()).map(|path| TargetRuntimeBoundary {
        kind: "docker".to_owned(),
        source: relative_runtime_source(root, path),
        evidence: "Dockerfile present in task workspace".to_owned(),
    })
}

fn infer_readme_runtime(root: &Path) -> Option<TargetRuntimeBoundary> {
    let candidates = [root.join("README.md"), root.join("README")];
    for path in candidates {
        let Ok(content) = fs::read_to_string(path.as_path()) else {
            continue;
        };
        let lower = content.to_ascii_lowercase();
        if lower.contains("/app") || lower.contains("docker") {
            return Some(TargetRuntimeBoundary {
                kind: "docker".to_owned(),
                source: relative_runtime_source(root, path.as_path()),
                evidence: "README references Docker or /app runtime".to_owned(),
            });
        }
    }
    None
}

fn relative_runtime_source(root: &Path, path: &Path) -> String {
    path.strip_prefix(root).unwrap_or(path).to_string_lossy().replace('\\', "/")
}

struct CommandSignature {
    command_name: String,
    args: Vec<String>,
    args_original: Vec<String>,
    all_text: String,
    shell_text: Option<String>,
}

impl CommandSignature {
    fn new(input: &ProcessRunnerToolInput) -> Self {
        let command_name = executable_name(input.command.as_str());
        let args = input.args.iter().map(|arg| arg.to_ascii_lowercase()).collect::<Vec<_>>();
        let all_text = std::iter::once(input.command.as_str())
            .chain(input.args.iter().map(String::as_str))
            .chain(input.env.values().map(String::as_str))
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        let shell_text = shell_payload(&command_name, input.args.as_slice());
        Self { command_name, args, args_original: input.args.clone(), all_text, shell_text }
    }

    fn args_contain(&self, expected: &str) -> bool {
        self.args.iter().any(|arg| arg == expected)
    }

    fn args_contain_any(&self, expected: &[&str]) -> bool {
        expected.iter().any(|candidate| self.args_contain(candidate))
    }

    fn args_window_matches(&self, expected: &[&str]) -> bool {
        self.args
            .windows(expected.len())
            .any(|window| window.iter().map(String::as_str).eq(expected.iter().copied()))
    }

    fn shell_text_contains_any(&self, needles: &[&str]) -> bool {
        self.shell_text
            .as_deref()
            .is_some_and(|text| needles.iter().any(|needle| text.contains(needle)))
    }

    fn contains_private_key_material(&self) -> bool {
        self.all_text.contains("begin openssh private key")
            || self.all_text.contains("begin rsa private key")
    }

    fn references_private_key_path(&self) -> bool {
        self.all_text.contains("id_rsa")
            || self.all_text.contains("id_ed25519")
            || self.all_text.contains("id_ecdsa")
            || self.all_text.contains("id_dsa")
    }

    fn is_copy_to_external_boundary(&self) -> bool {
        matches!(self.command_name.as_str(), "scp" | "rsync")
            || (self.command_name == "docker" && self.args.first().is_some_and(|arg| arg == "cp"))
            || self.shell_text_contains_any(&["scp ", "docker cp ", "rsync "])
    }

    fn mutates_credential_namespace(&self) -> bool {
        matches!(self.command_name.as_str(), "ssh-keygen" | "ssh-add" | "ssh-agent")
            || self.shell_text_contains_any(&["ssh-keygen", "ssh-add", "ssh-agent"])
            || self.all_text.contains(".ssh/config")
            || self.all_text.contains("\\.ssh\\config")
    }

    fn mutates_system_service(&self) -> bool {
        let mutating_verbs = ["enable", "disable", "start", "stop", "restart", "reload"];
        match self.command_name.as_str() {
            "systemctl" | "service" | "launchctl" => {
                self.args.iter().any(|arg| mutating_verbs.iter().any(|verb| arg == verb))
            }
            "sc" => self.args.iter().any(|arg| {
                ["create", "delete", "start", "stop", "config"].iter().any(|verb| arg == verb)
            }),
            _ => self.shell_text_contains_any(&[
                "systemctl enable",
                "systemctl disable",
                "systemctl restart",
                "service ",
                "launchctl load",
                "launchctl unload",
                "sc.exe create",
                "sc create",
            ]),
        }
    }

    fn destructive_filesystem_target(&self) -> Option<String> {
        match self.command_name.as_str() {
            "rm" if self.args.iter().any(|arg| arg.contains('r'))
                && self.args.iter().any(|arg| arg.contains('f')) =>
            {
                first_non_flag_arg(self.args_original.as_slice()).map(|target| target.to_owned())
            }
            "remove-item"
                if self.args_contain_any(&["-recurse", "-recursive"])
                    || self.args.iter().any(|arg| arg == "-r") =>
            {
                first_non_flag_arg(self.args_original.as_slice()).map(|target| target.to_owned())
            }
            _ if self.shell_text_contains_any(&["rm -rf", "rm -fr", "remove-item"]) => {
                Some("shell_recursive_delete".to_owned())
            }
            _ => None,
        }
    }

    fn host_runtime_command_can_miss_docker_target(&self) -> bool {
        if self.command_name == "docker" {
            return false;
        }
        is_python_command(self.command_name.as_str())
            || self.command_name.starts_with("pip")
            || matches!(
                self.command_name.as_str(),
                "uv" | "npm" | "pnpm" | "yarn" | "cargo" | "go" | "gem" | "cpan" | "cpanm"
            )
            || self.shell_text_contains_any(&[
                "pip install",
                "python -m pip",
                "npm install",
                "cargo test",
                "pytest",
            ])
    }

    fn requires_docker_posix_file_mode_semantics(&self) -> bool {
        if self.command_name == "docker" {
            return false;
        }
        self.uses_posix_permission_tool()
            || self.uses_windows_acl_tool_for_posix_substitute()
            || self.generates_tls_private_key_material()
    }

    fn uses_posix_permission_tool(&self) -> bool {
        match self.command_name.as_str() {
            "chmod" | "chown" | "umask" => true,
            "stat" => self.args_contain("-c") || self.args_contain("--format"),
            "ls" => self.args.iter().any(|arg| {
                arg == "-l"
                    || (arg.starts_with('-')
                        && !arg.starts_with("--")
                        && arg.chars().skip(1).any(|ch| ch == 'l'))
            }),
            _ => self.shell_text_contains_any(&[
                "chmod ",
                "chown ",
                "umask ",
                "stat -c",
                "stat --format",
                "ls -l",
                "ls -al",
                "ls -la",
            ]),
        }
    }

    fn uses_windows_acl_tool_for_posix_substitute(&self) -> bool {
        matches!(self.command_name.as_str(), "icacls" | "attrib")
            || self.shell_text_contains_any(&["icacls ", "attrib "])
    }

    fn generates_tls_private_key_material(&self) -> bool {
        let is_openssl = self.command_name == "openssl"
            || self.shell_text.as_deref().is_some_and(|text| text.contains("openssl "));
        is_openssl
            && (self.args_contain("-keyout")
                || self.args_contain("-out")
                || self.all_text.contains(".key")
                || self.all_text.contains("private.key"))
            && (self.args_contain_any(&["req", "genrsa", "genpkey", "rsa", "ecparam"])
                || self.shell_text_contains_any(&[
                    "openssl req",
                    "openssl genrsa",
                    "openssl genpkey",
                    "openssl rsa",
                    "openssl ecparam",
                ]))
    }
}

struct DockerInvocation<'a> {
    args: &'a [String],
}

impl<'a> DockerInvocation<'a> {
    fn new(signature: &'a CommandSignature) -> Option<Self> {
        (signature.command_name == "docker").then_some(Self { args: signature.args.as_slice() })
    }

    fn global_mutation_target(&self) -> Option<String> {
        let first = self.args.first().map(String::as_str)?;
        match first {
            "commit" | "tag" | "push" | "rmi" => Some(format!("docker_{first}")),
            "image" if self.args.get(1).is_some_and(|arg| arg == "rm" || arg == "prune") => {
                Some("docker_image_store".to_owned())
            }
            "volume" if self.args.get(1).is_some_and(|arg| arg == "rm" || arg == "prune") => {
                Some("docker_volume_store".to_owned())
            }
            "system" if self.args.get(1).is_some_and(|arg| arg == "prune") => {
                Some("docker_system_cache".to_owned())
            }
            _ => None,
        }
    }

    fn host_published_ports(&self) -> Vec<u16> {
        if self.args.first().map(String::as_str) != Some("run") {
            return Vec::new();
        }
        let mut ports = BTreeSet::new();
        for (index, arg) in self.args.iter().enumerate() {
            if arg == "-p" || arg == "--publish" {
                if let Some(value) = self.args.get(index + 1) {
                    push_docker_host_port(value, &mut ports);
                }
                continue;
            }
            if let Some(value) = arg.strip_prefix("-p") {
                push_docker_host_port(value, &mut ports);
                continue;
            }
            if let Some(value) = arg.strip_prefix("--publish=") {
                push_docker_host_port(value, &mut ports);
            }
        }
        ports.into_iter().collect()
    }
}

fn push_docker_host_port(raw: &str, ports: &mut BTreeSet<u16>) {
    let pieces = raw.split(':').collect::<Vec<_>>();
    let candidate = match pieces.as_slice() {
        [host] => *host,
        [host, _container] => *host,
        [_ip, host, _container] => *host,
        _ => return,
    };
    if let Ok(port) = candidate.parse::<u16>() {
        if port > 0 {
            ports.insert(port);
        }
    }
}

fn collect_credential_paths(
    input: &ProcessRunnerToolInput,
    signature: &CommandSignature,
) -> Vec<String> {
    let mut paths = BTreeSet::new();
    for value in std::iter::once(input.command.as_str())
        .chain(input.args.iter().map(String::as_str))
        .chain(input.env.values().map(String::as_str))
    {
        if value.to_ascii_lowercase().contains(".ssh")
            || value.to_ascii_lowercase().contains("id_rsa")
            || value.to_ascii_lowercase().contains("id_ed25519")
        {
            paths.insert(sanitize_risk_token(value));
        }
    }
    if signature.all_text.contains("%userprofile%\\.ssh") {
        paths.insert("%USERPROFILE%\\.ssh".to_owned());
    }
    if signature.all_text.contains("$home/.ssh") {
        paths.insert("$HOME/.ssh".to_owned());
    }
    if signature.all_text.contains("~/.ssh") {
        paths.insert("~/.ssh".to_owned());
    }
    paths.into_iter().collect()
}

fn credential_cleanup_commands(paths: &[String]) -> Vec<String> {
    paths.iter().map(|path| format!("review-and-remove {path}")).collect()
}

fn sanitize_risk_token(value: &str) -> String {
    value.trim().chars().take(256).collect()
}

fn executable_name(command: &str) -> String {
    let trimmed = command.trim().trim_matches('"').trim_matches('\'');
    let file_name =
        trimmed.rsplit(['/', '\\']).next().unwrap_or(trimmed).trim_matches('"').trim_matches('\'');
    let normalized = file_name.to_ascii_lowercase();
    strip_known_executable_suffix(normalized.as_str()).to_owned()
}

fn strip_known_executable_suffix(value: &str) -> &str {
    for suffix in [".exe", ".cmd", ".bat"] {
        if let Some(stripped) = value.strip_suffix(suffix) {
            return stripped;
        }
    }
    value
}

fn shell_payload(command_name: &str, args: &[String]) -> Option<String> {
    if !matches!(command_name, "bash" | "sh" | "zsh" | "pwsh" | "powershell" | "cmd") {
        return None;
    }
    args.windows(2)
        .find(|window| matches!(window[0].as_str(), "-c" | "-lc" | "/c"))
        .map(|window| window[1].to_ascii_lowercase())
}

fn first_non_flag_arg(args: &[String]) -> Option<&str> {
    args.iter().find(|arg| !arg.starts_with('-')).map(String::as_str)
}

fn is_python_command(command: &str) -> bool {
    command == "py" || command.starts_with("python")
}

fn normalize_text_path(value: &str) -> String {
    value.replace('\\', "/").to_ascii_lowercase()
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::{
        classify_process_run, ProcessRiskClass, ProcessRiskContext, ProcessRiskSeverity,
        TARGET_HOST_WINDOWS_VS_DOCKER_POSIX_PERMISSIONS,
    };
    use crate::process_runner_input::ProcessRunnerToolInput;

    fn input(command: &str, args: &[&str]) -> ProcessRunnerToolInput {
        ProcessRunnerToolInput {
            command: command.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            cwd: None,
            env: Default::default(),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: Default::default(),
            keep_running_after_run: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        }
    }

    fn has_class(report: &super::ProcessRiskReport, class: ProcessRiskClass) -> bool {
        report.findings.iter().any(|finding| finding.risk_class == class)
    }

    #[test]
    fn pip_install_without_workspace_venv_is_host_mutation() {
        let report = classify_process_run(
            &input("python", &["-m", "pip", "install", "numpy"]),
            ProcessRiskContext::default(),
        );

        assert!(has_class(&report, ProcessRiskClass::HostPackageManagerMutation));
        assert_eq!(report.highest_severity, ProcessRiskSeverity::High);
        assert!(!report.blocks_execution);
        assert!(!report.requires_user_approval);
    }

    #[test]
    fn pip_install_from_workspace_root_without_venv_is_still_host_mutation() {
        let workspace = Path::new("/repo/task");
        let mut command = input("pip", &["install", "numpy"]);
        command.cwd = Some("/repo/task".to_owned());

        let report = classify_process_run(
            &command,
            ProcessRiskContext { workspace_root: Some(workspace), resolved_cwd: Some(workspace) },
        );

        assert!(has_class(&report, ProcessRiskClass::HostPackageManagerMutation));
    }

    #[test]
    fn pip_install_inside_workspace_venv_is_not_flagged_as_global_host_install() {
        let workspace = Path::new("/repo/task");
        let mut command = input("/repo/task/.venv/bin/pip", &["install", "numpy"]);
        command.cwd = Some("/repo/task".to_owned());

        let report = classify_process_run(
            &command,
            ProcessRiskContext { workspace_root: Some(workspace), resolved_cwd: Some(workspace) },
        );

        assert!(!has_class(&report, ProcessRiskClass::HostPackageManagerMutation));
    }

    #[test]
    fn docker_global_mutation_and_port_publish_are_separate_findings() {
        let commit = classify_process_run(
            &input("docker", &["commit", "task", "terminal-bench:latest"]),
            ProcessRiskContext::default(),
        );
        assert!(has_class(&commit, ProcessRiskClass::DockerGlobalMutation));

        let run = classify_process_run(
            &input("docker", &["run", "-p", "0.0.0.0:8443:8443", "image"]),
            ProcessRiskContext::default(),
        );
        assert!(has_class(&run, ProcessRiskClass::DockerHostPortPublish));
        assert_eq!(run.findings[0].host_ports, vec![8443]);
    }

    #[test]
    fn credential_namespace_and_export_are_visible() {
        let report = classify_process_run(
            &input("docker", &["cp", "~/.ssh/id_ed25519", "container:/root/id_ed25519"]),
            ProcessRiskContext::default(),
        );

        assert!(has_class(&report, ProcessRiskClass::CredentialMaterialExport));
        assert!(has_class(&report, ProcessRiskClass::CredentialNamespaceMutation));
    }

    #[test]
    fn docker_target_runtime_warns_for_host_python_install() {
        let workspace = tempfile::tempdir().expect("temp workspace should be created");
        fs::write(workspace.path().join("Dockerfile"), b"FROM python:3.13\nWORKDIR /app\n")
            .expect("Dockerfile should be written");

        let report = classify_process_run(
            &input("pip", &["install", "scipy"]),
            ProcessRiskContext {
                workspace_root: Some(workspace.path()),
                resolved_cwd: Some(workspace.path()),
            },
        );

        assert_eq!(
            report.target_runtime.as_ref().map(|runtime| runtime.kind.as_str()),
            Some("docker")
        );
        assert!(has_class(&report, ProcessRiskClass::TargetRuntimeMismatch));
    }

    #[test]
    fn docker_target_runtime_flags_windows_host_permission_substitutes() {
        let workspace = tempfile::tempdir().expect("temp workspace should be created");
        fs::write(workspace.path().join("Dockerfile"), b"FROM debian:bookworm\nWORKDIR /app\n")
            .expect("Dockerfile should be written");

        let report = classify_process_run(
            &input("icacls", &["server.key", "/inheritance:r"]),
            ProcessRiskContext {
                workspace_root: Some(workspace.path()),
                resolved_cwd: Some(workspace.path()),
            },
        );

        let finding = report
            .findings
            .iter()
            .find(|finding| finding.risk_class == ProcessRiskClass::TargetRuntimeMismatch)
            .expect("Windows ACL substitute should be flagged for Docker/Linux targets");
        assert_eq!(finding.severity, ProcessRiskSeverity::High);
        assert_eq!(
            finding.target.as_deref(),
            Some(TARGET_HOST_WINDOWS_VS_DOCKER_POSIX_PERMISSIONS)
        );
    }

    #[test]
    fn docker_target_runtime_flags_openssl_key_generation_on_host() {
        let workspace = tempfile::tempdir().expect("temp workspace should be created");
        fs::write(workspace.path().join("Dockerfile"), b"FROM debian:bookworm\nWORKDIR /app\n")
            .expect("Dockerfile should be written");

        for command in [
            "openssl",
            "OpenSSL.exe",
            "OpenSSL.EXE",
            r"C:\Tools\OpenSSL.EXE",
            r#""C:\Tools\OpenSSL.EXE""#,
        ] {
            let report = classify_process_run(
                &input(
                    command,
                    &[
                        "req",
                        "-newkey",
                        "rsa:2048",
                        "-nodes",
                        "-keyout",
                        "server.key",
                        "-out",
                        "server.csr",
                    ],
                ),
                ProcessRiskContext {
                    workspace_root: Some(workspace.path()),
                    resolved_cwd: Some(workspace.path()),
                },
            );

            let finding = report
                .findings
                .iter()
                .find(|finding| finding.risk_class == ProcessRiskClass::TargetRuntimeMismatch)
                .unwrap_or_else(|| {
                    panic!(
                        "OpenSSL key generation should be flagged for Docker/Linux targets: {command}"
                    )
                });
            assert_eq!(finding.severity, ProcessRiskSeverity::High, "{command}");
            assert_eq!(
                finding.target.as_deref(),
                Some(TARGET_HOST_WINDOWS_VS_DOCKER_POSIX_PERMISSIONS),
                "{command}"
            );
        }
    }
}
