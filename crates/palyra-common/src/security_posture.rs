//! Shared security-posture contracts used by CLI audits, runtime diagnostics,
//! and future journal-backed read models.
//!
//! This module intentionally contains only serializable data shapes and pure
//! decision functions. Runtime code remains responsible for collecting real
//! inputs, writing journal events, and enforcing policy gates.

use serde::{Deserialize, Serialize};

use crate::redaction::redact_diagnostic_text;

/// Current schema version for attack-surface and sanitizer projections.
pub const SECURITY_POSTURE_SCHEMA_VERSION: u32 = 1;
/// Audit event emitted when an attack-surface audit starts.
pub const ATTACK_SURFACE_AUDIT_STARTED_EVENT_TYPE: &str = "security.attack_surface.audit_started";
/// Audit event emitted for each attack-surface finding.
pub const ATTACK_SURFACE_FINDING_EVENT_TYPE: &str = "security.attack_surface.finding";
/// Audit event emitted when outbound text is sanitized for a target surface.
pub const OUTBOUND_SANITIZED_EVENT_TYPE: &str = "outbound.sanitized";
/// Audit event emitted when rescue mode is entered.
pub const RESCUE_MODE_ENTERED_EVENT_TYPE: &str = "rescue.mode.entered";
/// Audit event emitted when a rescue command is evaluated or executed.
pub const RESCUE_COMMAND_EXECUTED_EVENT_TYPE: &str = "rescue.command.executed";
/// Audit event emitted when rescue mode exits.
pub const RESCUE_MODE_EXITED_EVENT_TYPE: &str = "rescue.mode.exited";

const PATH_REDACTION: &str = "<path_redacted>";
const RUN_ID_REDACTION: &str = "<run_id_redacted>";
const PROVIDER_ERROR_REDACTION: &str = "<provider_error_redacted>";
const STDERR_REDACTION: &str = "<tool_stderr_redacted>";
const STACK_TRACE_REDACTION: &str = "<stack_trace_redacted>";

/// Severity used by attack-surface findings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityPostureSeverity {
    /// Informational finding that improves operator visibility.
    Info,
    /// Risk that should be reviewed before enabling broader exposure.
    Warning,
    /// Blocking security posture that can expose side effects or secrets.
    Critical,
}

impl SecurityPostureSeverity {
    /// Returns the stable wire string used by CLI and JSON reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }
}

/// Human approval posture for a surface, tool, secret, or rescue command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalRequirement {
    /// No policy or human approval is required.
    None,
    /// Policy evaluation is required, but no human approval is required.
    PolicyOnly,
    /// One explicit human approval is required before the action.
    OneHumanApproval,
    /// A two-step approval is required for restrictive repair operations.
    TwoStepHumanApproval,
}

impl ApprovalRequirement {
    /// Returns the number of human approvals represented by this requirement.
    #[must_use]
    pub const fn human_approval_count(self) -> u8 {
        match self {
            Self::None | Self::PolicyOnly => 0,
            Self::OneHumanApproval => 1,
            Self::TwoStepHumanApproval => 2,
        }
    }

    /// Returns whether at least one human approval gates the action.
    #[must_use]
    pub const fn has_human_approval(self) -> bool {
        self.human_approval_count() > 0
    }
}

/// Highest side-effect class reachable from a surface or tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideEffectLevel {
    /// No side effect can occur.
    None,
    /// Only metadata or health state can be read.
    MetadataRead,
    /// Internal diagnostics can be read.
    InternalDiagnostics,
    /// Workspace files can be read.
    WorkspaceRead,
    /// Workspace files can be modified.
    WorkspaceWrite,
    /// Network requests can be made.
    NetworkEgress,
    /// Local process execution can occur.
    ProcessExecution,
    /// Secret lease metadata or secret material can be requested.
    SecretLease,
    /// Admin/runtime state can be mutated.
    AdminMutation,
    /// Remote or arbitrary code execution is possible.
    RemoteCodeExecution,
}

impl SideEffectLevel {
    /// Returns the stable wire string used by reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::MetadataRead => "metadata_read",
            Self::InternalDiagnostics => "internal_diagnostics",
            Self::WorkspaceRead => "workspace_read",
            Self::WorkspaceWrite => "workspace_write",
            Self::NetworkEgress => "network_egress",
            Self::ProcessExecution => "process_execution",
            Self::SecretLease => "secret_lease",
            Self::AdminMutation => "admin_mutation",
            Self::RemoteCodeExecution => "remote_code_execution",
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::None => 0,
            Self::MetadataRead => 1,
            Self::InternalDiagnostics => 2,
            Self::WorkspaceRead => 3,
            Self::WorkspaceWrite => 4,
            Self::NetworkEgress => 5,
            Self::ProcessExecution => 6,
            Self::SecretLease => 7,
            Self::AdminMutation => 8,
            Self::RemoteCodeExecution => 9,
        }
    }

    const fn max(self, other: Self) -> Self {
        if self.rank() >= other.rank() {
            self
        } else {
            other
        }
    }
}

/// Runtime ingress or delivery surface kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressSurfaceKind {
    /// Local operator CLI.
    Cli,
    /// Web console/admin HTTP API.
    ConsoleApi,
    /// Agent Client Protocol bridge.
    Acp,
    /// Discord channel or DM connector.
    DiscordChannel,
    /// Inbound webhook integration.
    Webhook,
    /// Browser extension relay.
    BrowserExtension,
    /// Scheduled routine or unattended task.
    Routine,
    /// Wasm or extension plugin.
    Plugin,
    /// Internal-only diagnostic path.
    InternalDiagnostic,
}

/// Visibility of a channel-like ingress surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelExposure {
    /// Internal process-only surface.
    Internal,
    /// Private operator or direct-message surface.
    Private,
    /// Public or multi-user channel surface.
    Public,
}

/// Filesystem access exposed by workspace or tool surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemAccess {
    /// No filesystem access.
    None,
    /// Read access limited to approved workspace roots.
    WorkspaceRead,
    /// Write access limited to approved workspace roots.
    WorkspaceWrite,
    /// Host read access beyond the workspace.
    HostRead,
    /// Host write access beyond the workspace.
    HostWrite,
}

/// Process execution access exposed by a tool or workspace profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessAccess {
    /// No process execution.
    None,
    /// Process execution inside a bounded sandbox.
    Sandboxed,
    /// Host process execution limited by an allowlist.
    HostAllowlist,
    /// Host process execution with wildcard executable selection.
    HostWildcard,
}

/// Network egress posture exposed by a tool or workspace profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EgressAccess {
    /// No network egress.
    None,
    /// Egress limited to explicit hosts or suffixes.
    Allowlisted,
    /// Egress may target private network addresses.
    PrivateTargets,
    /// Egress is unrestricted.
    Unrestricted,
}

/// Secret material exposure level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretAccess {
    /// No secret access.
    None,
    /// Only vault reference handles are visible.
    VaultReferenceOnly,
    /// Lease metadata can be returned, but not raw secret values.
    LeaseMetadata,
    /// Raw secret material can be returned.
    RawSecret,
}

/// Sandbox tier associated with process or plugin execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxTier {
    /// No sandbox is in effect.
    None,
    /// Tier A Wasm/plugin sandbox.
    TierA,
    /// Tier B bounded host process runner.
    TierB,
    /// Tier C OS-backed process sandbox.
    TierC,
}

/// Stable reason code for attack-surface findings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttackSurfaceReasonCode {
    /// A public channel can reach process execution without human approval.
    PublicChannelProcessWithoutApproval,
    /// A remote operator surface is exposed without admin authentication.
    RemoteAdminWithoutAuth,
    /// Secret access is possible without human approval.
    SecretAccessWithoutHumanApproval,
    /// Host filesystem access is possible without human approval.
    HostFilesystemWithoutApproval,
    /// Unrestricted egress is possible without human approval.
    UnrestrictedEgressWithoutApproval,
    /// Plugin grants are broader than the declared diagnostic purpose.
    PluginGrantTooBroad,
    /// Webhook ingress can trigger mutation without human approval.
    WebhookMutationWithoutApproval,
    /// Diagnostics provider output must remain redacted at surface boundaries.
    DiagnosticsProviderNeedsRedaction,
    /// No risky exposure was found for this projection.
    SafeDefault,
}

impl AttackSurfaceReasonCode {
    /// Returns the stable snake_case reason token used in findings.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublicChannelProcessWithoutApproval => "public_channel_process_without_approval",
            Self::RemoteAdminWithoutAuth => "remote_admin_without_auth",
            Self::SecretAccessWithoutHumanApproval => "secret_access_without_human_approval",
            Self::HostFilesystemWithoutApproval => "host_filesystem_without_approval",
            Self::UnrestrictedEgressWithoutApproval => "unrestricted_egress_without_approval",
            Self::PluginGrantTooBroad => "plugin_grant_too_broad",
            Self::WebhookMutationWithoutApproval => "webhook_mutation_without_approval",
            Self::DiagnosticsProviderNeedsRedaction => "diagnostics_provider_needs_redaction",
            Self::SafeDefault => "safe_default",
        }
    }
}

/// One ingress surface included in an [`AttackSurfaceGraph`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressExposure {
    /// Stable local identifier for the surface, such as `console.admin`.
    pub source_id: String,
    /// Surface category.
    pub source: IngressSurfaceKind,
    /// Principal or principal class allowed to use the surface.
    pub principal: String,
    /// Optional channel, workspace, or deployment scope.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_scope: Option<String>,
    /// Whether the surface is internal, private, or public.
    pub channel_exposure: ChannelExposure,
    /// Whether admin authentication is required before use.
    pub admin_auth_required: bool,
    /// Whether webhook signatures are required for inbound webhook surfaces.
    pub webhook_signature_required: bool,
    /// Approval posture before this surface can trigger side effects.
    pub approval_requirement: ApprovalRequirement,
}

/// One tool or action surface included in an [`AttackSurfaceGraph`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExposure {
    /// Stable tool or action name.
    pub tool_name: String,
    /// Surfaces where the tool can be exposed.
    #[serde(default)]
    pub target_surfaces: Vec<IngressSurfaceKind>,
    /// Highest side effect the tool can perform.
    pub side_effect: SideEffectLevel,
    /// Human/policy approval required before dispatch.
    pub approval_requirement: ApprovalRequirement,
    /// Sandbox tier used for execution.
    pub sandbox_tier: SandboxTier,
    /// Process access granted to the tool.
    pub process_access: ProcessAccess,
    /// Network egress granted to the tool.
    pub egress_access: EgressAccess,
}

/// One secret or credential exposure included in an [`AttackSurfaceGraph`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretExposure {
    /// Stable secret exposure id or vault ref handle.
    pub ref_id: String,
    /// Level of secret access exposed.
    pub access: SecretAccess,
    /// Approval posture for the secret access.
    pub approval_requirement: ApprovalRequirement,
    /// Whether a vault reference exists instead of an inline secret value.
    pub vault_ref_present: bool,
}

/// Workspace-level exposure projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceExposure {
    /// Workspace roots included in the projection, redacted when needed.
    #[serde(default)]
    pub workspace_roots: Vec<String>,
    /// Filesystem access available to the runtime.
    pub filesystem_access: FilesystemAccess,
    /// Process execution access available to the runtime.
    pub process_access: ProcessAccess,
    /// Network egress available to the runtime.
    pub egress_access: EgressAccess,
    /// Whether browser automation is exposed.
    pub browser_access_enabled: bool,
    /// Whether webhook ingress is exposed.
    pub webhook_access_enabled: bool,
}

impl Default for WorkspaceExposure {
    fn default() -> Self {
        Self {
            workspace_roots: Vec::new(),
            filesystem_access: FilesystemAccess::None,
            process_access: ProcessAccess::None,
            egress_access: EgressAccess::None,
            browser_access_enabled: false,
            webhook_access_enabled: false,
        }
    }
}

/// Plugin or extension capability exposure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginExposure {
    /// Stable plugin identifier.
    pub plugin_id: String,
    /// Capability grants requested or issued to the plugin.
    #[serde(default)]
    pub grants: Vec<String>,
    /// Whether the plugin serves diagnostics output.
    pub diagnostics_provider: bool,
    /// Approval posture before plugin host calls can perform side effects.
    pub approval_requirement: ApprovalRequirement,
}

/// Serializable graph of exposed surfaces, actions, secrets, and workspace access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackSurfaceGraph {
    /// Schema version for forward-compatible consumers.
    pub schema_version: u32,
    /// Ingress surfaces that can initiate work.
    #[serde(default)]
    pub ingress: Vec<IngressExposure>,
    /// Tool/action exposures reachable from ingress.
    #[serde(default)]
    pub tools: Vec<ToolExposure>,
    /// Secret exposures visible to tools, plugins, or operators.
    #[serde(default)]
    pub secrets: Vec<SecretExposure>,
    /// Workspace-wide access projection.
    pub workspace: WorkspaceExposure,
    /// Plugin and extension exposure projection.
    #[serde(default)]
    pub plugins: Vec<PluginExposure>,
}

impl Default for AttackSurfaceGraph {
    fn default() -> Self {
        Self {
            schema_version: SECURITY_POSTURE_SCHEMA_VERSION,
            ingress: Vec::new(),
            tools: Vec::new(),
            secrets: Vec::new(),
            workspace: WorkspaceExposure::default(),
            plugins: Vec::new(),
        }
    }
}

/// One attack-surface finding with evidence and remediation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackSurfaceFinding {
    /// Finding severity.
    pub severity: SecurityPostureSeverity,
    /// Stable reason code.
    pub reason_code: AttackSurfaceReasonCode,
    /// Affected path in the graph, such as `tools.palyra.process.run`.
    pub affected_path: String,
    /// Evidence references suitable for journal or replay payloads.
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    /// Operator-facing remediation hint that does not contain secrets.
    pub remediation_hint: String,
}

/// Summary counters and side-effect maxima for an attack-surface audit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackSurfaceSummary {
    /// Number of critical findings.
    pub critical_findings: usize,
    /// Number of warning findings.
    pub warning_findings: usize,
    /// Number of informational findings.
    pub info_findings: usize,
    /// Highest side effect reachable without human approval.
    pub highest_side_effect_without_human_approval: SideEffectLevel,
    /// Highest side effect reachable with no more than one human approval.
    pub highest_side_effect_with_one_approval: SideEffectLevel,
}

/// Complete attack-surface audit read model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackSurfaceAudit {
    /// Schema version for the audit payload.
    pub schema_version: u32,
    /// Event name to record before collecting or evaluating a graph.
    pub audit_started_event_type: String,
    /// Event name to record for each finding.
    pub finding_event_type: String,
    /// Evaluated graph.
    pub graph: AttackSurfaceGraph,
    /// Severity-ranked findings.
    #[serde(default)]
    pub findings: Vec<AttackSurfaceFinding>,
    /// Aggregate counters and side-effect maxima.
    pub summary: AttackSurfaceSummary,
}

/// Evaluates an attack-surface graph into a stable audit payload.
#[must_use]
pub fn audit_attack_surface_graph(graph: &AttackSurfaceGraph) -> AttackSurfaceAudit {
    let mut findings = Vec::new();
    collect_ingress_findings(graph, &mut findings);
    collect_tool_findings(graph, &mut findings);
    collect_secret_findings(graph, &mut findings);
    collect_workspace_findings(graph, &mut findings);
    collect_plugin_findings(graph, &mut findings);

    if findings.is_empty() {
        findings.push(AttackSurfaceFinding {
            severity: SecurityPostureSeverity::Info,
            reason_code: AttackSurfaceReasonCode::SafeDefault,
            affected_path: "$".to_owned(),
            evidence_refs: vec!["attack_surface.graph".to_owned()],
            remediation_hint: "Keep deny-by-default tool, secret, and ingress posture in place."
                .to_owned(),
        });
    }

    findings.sort_by_key(|finding| severity_sort_key(finding.severity));
    let summary = AttackSurfaceSummary {
        critical_findings: findings
            .iter()
            .filter(|finding| finding.severity == SecurityPostureSeverity::Critical)
            .count(),
        warning_findings: findings
            .iter()
            .filter(|finding| finding.severity == SecurityPostureSeverity::Warning)
            .count(),
        info_findings: findings
            .iter()
            .filter(|finding| finding.severity == SecurityPostureSeverity::Info)
            .count(),
        highest_side_effect_without_human_approval: highest_side_effect(graph, 0),
        highest_side_effect_with_one_approval: highest_side_effect(graph, 1),
    };

    AttackSurfaceAudit {
        schema_version: SECURITY_POSTURE_SCHEMA_VERSION,
        audit_started_event_type: ATTACK_SURFACE_AUDIT_STARTED_EVENT_TYPE.to_owned(),
        finding_event_type: ATTACK_SURFACE_FINDING_EVENT_TYPE.to_owned(),
        graph: graph.clone(),
        findings,
        summary,
    }
}

fn severity_sort_key(severity: SecurityPostureSeverity) -> u8 {
    match severity {
        SecurityPostureSeverity::Critical => 0,
        SecurityPostureSeverity::Warning => 1,
        SecurityPostureSeverity::Info => 2,
    }
}

fn collect_ingress_findings(graph: &AttackSurfaceGraph, findings: &mut Vec<AttackSurfaceFinding>) {
    for ingress in &graph.ingress {
        if ingress.channel_exposure == ChannelExposure::Public
            && matches!(ingress.source, IngressSurfaceKind::ConsoleApi | IngressSurfaceKind::Acp)
            && !ingress.admin_auth_required
        {
            findings.push(AttackSurfaceFinding {
                severity: SecurityPostureSeverity::Critical,
                reason_code: AttackSurfaceReasonCode::RemoteAdminWithoutAuth,
                affected_path: format!("ingress.{}", ingress.source_id),
                evidence_refs: vec![format!("ingress:{}", ingress.source_id)],
                remediation_hint:
                    "Require admin authentication before exposing remote operator surfaces."
                        .to_owned(),
            });
        }
    }
}

fn collect_tool_findings(graph: &AttackSurfaceGraph, findings: &mut Vec<AttackSurfaceFinding>) {
    let public_discord_ingress = graph.ingress.iter().any(|ingress| {
        ingress.source == IngressSurfaceKind::DiscordChannel
            && ingress.channel_exposure == ChannelExposure::Public
    });

    for tool in &graph.tools {
        let no_human_approval = !tool.approval_requirement.has_human_approval();
        let public_channel_tool = public_discord_ingress
            && tool.target_surfaces.contains(&IngressSurfaceKind::DiscordChannel);
        if public_channel_tool
            && no_human_approval
            && (tool.side_effect.rank() >= SideEffectLevel::ProcessExecution.rank()
                || tool.process_access == ProcessAccess::HostWildcard)
        {
            findings.push(AttackSurfaceFinding {
                severity: SecurityPostureSeverity::Critical,
                reason_code: AttackSurfaceReasonCode::PublicChannelProcessWithoutApproval,
                affected_path: format!("tools.{}", tool.tool_name),
                evidence_refs: vec![
                    "ingress.discord.public".to_owned(),
                    format!("tool:{}", tool.tool_name),
                ],
                remediation_hint: "Require human approval or remove process-capable tools from public Discord surfaces."
                    .to_owned(),
            });
        }

        if no_human_approval && tool.egress_access == EgressAccess::Unrestricted {
            findings.push(AttackSurfaceFinding {
                severity: SecurityPostureSeverity::Warning,
                reason_code: AttackSurfaceReasonCode::UnrestrictedEgressWithoutApproval,
                affected_path: format!("tools.{}", tool.tool_name),
                evidence_refs: vec![format!("tool:{}", tool.tool_name)],
                remediation_hint:
                    "Constrain egress to allowlisted hosts or require approval before dispatch."
                        .to_owned(),
            });
        }
    }
}

fn collect_secret_findings(graph: &AttackSurfaceGraph, findings: &mut Vec<AttackSurfaceFinding>) {
    for secret in &graph.secrets {
        if !secret.approval_requirement.has_human_approval()
            && matches!(secret.access, SecretAccess::LeaseMetadata | SecretAccess::RawSecret)
        {
            findings.push(AttackSurfaceFinding {
                severity: if secret.access == SecretAccess::RawSecret {
                    SecurityPostureSeverity::Critical
                } else {
                    SecurityPostureSeverity::Warning
                },
                reason_code: AttackSurfaceReasonCode::SecretAccessWithoutHumanApproval,
                affected_path: format!("secrets.{}", secret.ref_id),
                evidence_refs: vec![format!("secret:{}", secret.ref_id)],
                remediation_hint: "Return only vault references or require explicit approval before secret lease requests."
                    .to_owned(),
            });
        }
    }
}

fn collect_workspace_findings(
    graph: &AttackSurfaceGraph,
    findings: &mut Vec<AttackSurfaceFinding>,
) {
    if matches!(
        graph.workspace.filesystem_access,
        FilesystemAccess::HostRead | FilesystemAccess::HostWrite
    ) {
        findings.push(AttackSurfaceFinding {
            severity: if graph.workspace.filesystem_access == FilesystemAccess::HostWrite {
                SecurityPostureSeverity::Critical
            } else {
                SecurityPostureSeverity::Warning
            },
            reason_code: AttackSurfaceReasonCode::HostFilesystemWithoutApproval,
            affected_path: "workspace.filesystem_access".to_owned(),
            evidence_refs: vec!["workspace.filesystem_access".to_owned()],
            remediation_hint:
                "Limit filesystem access to workspace roots or gate host access with approval."
                    .to_owned(),
        });
    }

    if graph.workspace.webhook_access_enabled
        && graph.tools.iter().any(|tool| {
            tool.side_effect.rank() >= SideEffectLevel::WorkspaceWrite.rank()
                && !tool.approval_requirement.has_human_approval()
        })
    {
        findings.push(AttackSurfaceFinding {
            severity: SecurityPostureSeverity::Warning,
            reason_code: AttackSurfaceReasonCode::WebhookMutationWithoutApproval,
            affected_path: "workspace.webhook_access_enabled".to_owned(),
            evidence_refs: vec!["workspace.webhook_access".to_owned()],
            remediation_hint: "Keep webhook-triggered mutation behind policy and human approval."
                .to_owned(),
        });
    }
}

fn collect_plugin_findings(graph: &AttackSurfaceGraph, findings: &mut Vec<AttackSurfaceFinding>) {
    for plugin in &graph.plugins {
        if plugin.diagnostics_provider && !plugin.grants.is_empty() {
            findings.push(AttackSurfaceFinding {
                severity: SecurityPostureSeverity::Info,
                reason_code: AttackSurfaceReasonCode::DiagnosticsProviderNeedsRedaction,
                affected_path: format!("plugins.{}", plugin.plugin_id),
                evidence_refs: vec![format!("plugin:{}", plugin.plugin_id)],
                remediation_hint: "Route diagnostics provider output through surface sanitization before showing it outside internal diagnostics."
                    .to_owned(),
            });
        }

        if !plugin.approval_requirement.has_human_approval()
            && plugin.grants.iter().any(|grant| grant == "*" || grant.contains("raw_secret"))
        {
            findings.push(AttackSurfaceFinding {
                severity: SecurityPostureSeverity::Warning,
                reason_code: AttackSurfaceReasonCode::PluginGrantTooBroad,
                affected_path: format!("plugins.{}", plugin.plugin_id),
                evidence_refs: vec![format!("plugin:{}", plugin.plugin_id)],
                remediation_hint: "Replace wildcard plugin grants with capability-scoped host services and approval gates."
                    .to_owned(),
            });
        }
    }
}

fn highest_side_effect(graph: &AttackSurfaceGraph, max_human_approvals: u8) -> SideEffectLevel {
    let mut highest = SideEffectLevel::None;
    for tool in &graph.tools {
        if tool.approval_requirement.human_approval_count() <= max_human_approvals {
            highest = highest.max(tool.side_effect);
        }
    }
    for secret in &graph.secrets {
        if secret.approval_requirement.human_approval_count() <= max_human_approvals {
            let level = match secret.access {
                SecretAccess::None | SecretAccess::VaultReferenceOnly => SideEffectLevel::None,
                SecretAccess::LeaseMetadata | SecretAccess::RawSecret => {
                    SideEffectLevel::SecretLease
                }
            };
            highest = highest.max(level);
        }
    }
    highest.max(workspace_side_effect(&graph.workspace))
}

fn workspace_side_effect(workspace: &WorkspaceExposure) -> SideEffectLevel {
    let filesystem = match workspace.filesystem_access {
        FilesystemAccess::None => SideEffectLevel::None,
        FilesystemAccess::WorkspaceRead | FilesystemAccess::HostRead => {
            SideEffectLevel::WorkspaceRead
        }
        FilesystemAccess::WorkspaceWrite | FilesystemAccess::HostWrite => {
            SideEffectLevel::WorkspaceWrite
        }
    };
    let process = match workspace.process_access {
        ProcessAccess::None => SideEffectLevel::None,
        ProcessAccess::Sandboxed => SideEffectLevel::WorkspaceWrite,
        ProcessAccess::HostAllowlist | ProcessAccess::HostWildcard => {
            SideEffectLevel::ProcessExecution
        }
    };
    let egress = match workspace.egress_access {
        EgressAccess::None => SideEffectLevel::None,
        EgressAccess::Allowlisted => SideEffectLevel::NetworkEgress,
        EgressAccess::PrivateTargets | EgressAccess::Unrestricted => SideEffectLevel::NetworkEgress,
    };
    filesystem.max(process).max(egress)
}

/// Outbound surface that receives model, tool, connector, or diagnostic text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceKind {
    /// Public or private Discord-facing human channel.
    DiscordHumanChannel,
    /// Console API response.
    ConsoleApi,
    /// Local CLI response.
    Cli,
    /// ACP client response.
    Acp,
    /// Internal-only diagnostic payload.
    InternalDiagnostics,
    /// Webhook delivery body.
    WebhookDelivery,
    /// Routine failure notification.
    RoutineFailureDelivery,
    /// Operator rescue-mode output.
    RescueOutput,
}

impl SurfaceKind {
    /// Returns the stable wire string used in sanitizer audit events.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DiscordHumanChannel => "discord_human_channel",
            Self::ConsoleApi => "console_api",
            Self::Cli => "cli",
            Self::Acp => "acp",
            Self::InternalDiagnostics => "internal_diagnostics",
            Self::WebhookDelivery => "webhook_delivery",
            Self::RoutineFailureDelivery => "routine_failure_delivery",
            Self::RescueOutput => "rescue_output",
        }
    }
}

/// Explicit allowlist of data classes visible on one outbound surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceSanitizationPolicy {
    /// Surface this policy applies to.
    pub surface: SurfaceKind,
    /// Whether raw provider error bodies may be shown.
    pub allow_raw_provider_error: bool,
    /// Whether raw tool stderr may be shown.
    pub allow_tool_stderr: bool,
    /// Whether host file paths may be shown.
    pub allow_file_paths: bool,
    /// Whether redaction placeholders may be shown.
    pub allow_redacted_placeholders: bool,
    /// Whether stable policy reason codes may be shown.
    pub allow_policy_reason_codes: bool,
    /// Whether internal run ids may be shown.
    pub allow_internal_run_ids: bool,
    /// Whether stack traces may be shown.
    pub allow_stack_traces: bool,
    /// Whether model routing decisions may be shown.
    pub allow_model_routing_decision: bool,
    /// Maximum UTF-8 bytes retained after sanitization.
    pub max_text_bytes: usize,
}

/// Returns the default sanitizer policy for an outbound surface.
#[must_use]
pub const fn surface_sanitization_policy(surface: SurfaceKind) -> SurfaceSanitizationPolicy {
    match surface {
        SurfaceKind::InternalDiagnostics => SurfaceSanitizationPolicy {
            surface,
            allow_raw_provider_error: true,
            allow_tool_stderr: true,
            allow_file_paths: true,
            allow_redacted_placeholders: true,
            allow_policy_reason_codes: true,
            allow_internal_run_ids: true,
            allow_stack_traces: true,
            allow_model_routing_decision: true,
            max_text_bytes: 64 * 1024,
        },
        SurfaceKind::ConsoleApi | SurfaceKind::Cli | SurfaceKind::Acp => {
            SurfaceSanitizationPolicy {
                surface,
                allow_raw_provider_error: false,
                allow_tool_stderr: false,
                allow_file_paths: true,
                allow_redacted_placeholders: true,
                allow_policy_reason_codes: true,
                allow_internal_run_ids: true,
                allow_stack_traces: false,
                allow_model_routing_decision: true,
                max_text_bytes: 32 * 1024,
            }
        }
        SurfaceKind::DiscordHumanChannel
        | SurfaceKind::WebhookDelivery
        | SurfaceKind::RoutineFailureDelivery
        | SurfaceKind::RescueOutput => SurfaceSanitizationPolicy {
            surface,
            allow_raw_provider_error: false,
            allow_tool_stderr: false,
            allow_file_paths: false,
            allow_redacted_placeholders: true,
            allow_policy_reason_codes: false,
            allow_internal_run_ids: false,
            allow_stack_traces: false,
            allow_model_routing_decision: false,
            max_text_bytes: 8 * 1024,
        },
    }
}

/// Owned outbound message passed to the sanitizer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundMessage {
    /// Target surface receiving the message.
    pub surface: SurfaceKind,
    /// Raw text before surface sanitization.
    pub text: String,
}

/// Audit payload summarizing an outbound sanitization pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundSanitizationAuditEvent {
    /// Stable event name.
    pub event_type: String,
    /// Target surface.
    pub surface: SurfaceKind,
    /// Coarse redaction level applied.
    pub redaction_level: String,
    /// Stable reason codes describing applied redactions.
    #[serde(default)]
    pub reason_codes: Vec<String>,
    /// Input size in bytes.
    pub original_bytes: usize,
    /// Output size in bytes.
    pub sanitized_bytes: usize,
}

/// Result of surface-aware outbound sanitization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundSanitizationReport {
    /// Sanitized text safe for the target surface.
    pub sanitized_text: String,
    /// Policy that was applied.
    pub policy: SurfaceSanitizationPolicy,
    /// Audit event that can be recorded by the caller.
    pub audit_event: OutboundSanitizationAuditEvent,
}

/// Sanitizes outbound text according to the default policy for its surface.
#[must_use]
pub fn sanitize_outbound_message(message: &OutboundMessage) -> OutboundSanitizationReport {
    let policy = surface_sanitization_policy(message.surface);
    sanitize_outbound_message_with_policy(message, policy)
}

/// Sanitizes outbound text according to an explicit policy.
#[must_use]
pub fn sanitize_outbound_message_with_policy(
    message: &OutboundMessage,
    policy: SurfaceSanitizationPolicy,
) -> OutboundSanitizationReport {
    let mut reason_codes = Vec::new();
    let mut sanitized = redact_diagnostic_text(message.text.as_str());
    push_reason_if_changed(
        message.text.as_str(),
        sanitized.as_str(),
        "secret_or_internal_path_redacted",
        &mut reason_codes,
    );

    if !policy.allow_stack_traces {
        let before = sanitized.clone();
        sanitized = redact_stack_trace_lines(sanitized.as_str());
        push_reason_if_changed(
            before.as_str(),
            sanitized.as_str(),
            "stack_trace_removed",
            &mut reason_codes,
        );
    }
    if !policy.allow_raw_provider_error {
        let before = sanitized.clone();
        sanitized = redact_provider_error_lines(sanitized.as_str());
        push_reason_if_changed(
            before.as_str(),
            sanitized.as_str(),
            "provider_error_body_removed",
            &mut reason_codes,
        );
    }
    if !policy.allow_tool_stderr {
        let before = sanitized.clone();
        sanitized = redact_stderr_lines(sanitized.as_str());
        push_reason_if_changed(
            before.as_str(),
            sanitized.as_str(),
            "tool_stderr_removed",
            &mut reason_codes,
        );
    }
    if !policy.allow_file_paths {
        let before = sanitized.clone();
        sanitized = redact_path_like_tokens(sanitized.as_str());
        push_reason_if_changed(
            before.as_str(),
            sanitized.as_str(),
            "file_paths_removed",
            &mut reason_codes,
        );
    }
    if !policy.allow_internal_run_ids {
        let before = sanitized.clone();
        sanitized = redact_run_id_tokens(sanitized.as_str());
        push_reason_if_changed(
            before.as_str(),
            sanitized.as_str(),
            "internal_run_ids_removed",
            &mut reason_codes,
        );
    }
    if !policy.allow_policy_reason_codes {
        let before = sanitized.clone();
        sanitized = redact_policy_reason_tokens(sanitized.as_str());
        push_reason_if_changed(
            before.as_str(),
            sanitized.as_str(),
            "policy_reason_codes_removed",
            &mut reason_codes,
        );
    }
    if sanitized.len() > policy.max_text_bytes {
        sanitized = truncate_utf8(sanitized.as_str(), policy.max_text_bytes);
        push_unique_reason(&mut reason_codes, "text_truncated");
    }

    let redaction_level = if reason_codes.is_empty() { "none" } else { "surface_restricted" };
    OutboundSanitizationReport {
        audit_event: OutboundSanitizationAuditEvent {
            event_type: OUTBOUND_SANITIZED_EVENT_TYPE.to_owned(),
            surface: policy.surface,
            redaction_level: redaction_level.to_owned(),
            reason_codes,
            original_bytes: message.text.len(),
            sanitized_bytes: sanitized.len(),
        },
        policy,
        sanitized_text: sanitized,
    }
}

fn push_reason_if_changed(before: &str, after: &str, reason: &str, reasons: &mut Vec<String>) {
    if before != after {
        push_unique_reason(reasons, reason);
    }
}

fn push_unique_reason(reasons: &mut Vec<String>, reason: &str) {
    if !reasons.iter().any(|existing| existing == reason) {
        reasons.push(reason.to_owned());
    }
}

fn redact_stack_trace_lines(text: &str) -> String {
    redact_matching_lines(
        text,
        |line| {
            let lowered = line.trim_start().to_ascii_lowercase();
            lowered.starts_with("stack backtrace")
                || lowered.starts_with("backtrace:")
                || lowered.starts_with("at ")
                || lowered.contains("panicked at")
        },
        STACK_TRACE_REDACTION,
    )
}

fn redact_provider_error_lines(text: &str) -> String {
    redact_from_matching_line(
        text,
        |line| {
            let lowered = line.to_ascii_lowercase();
            lowered.contains("raw provider")
                || lowered.contains("provider body")
                || lowered.contains("provider response body")
        },
        PROVIDER_ERROR_REDACTION,
    )
}

fn redact_stderr_lines(text: &str) -> String {
    redact_from_matching_line(
        text,
        |line| {
            let lowered = line.trim_start().to_ascii_lowercase();
            lowered.starts_with("stderr") || lowered.contains("tool stderr")
        },
        STDERR_REDACTION,
    )
}

fn redact_from_matching_line(
    text: &str,
    matches_line: impl Fn(&str) -> bool,
    replacement: &str,
) -> String {
    let mut retained = Vec::new();
    for line in text.lines() {
        if matches_line(line) {
            // Provider bodies and stderr have no trusted terminator, so the
            // complete suffix must be treated as part of the untrusted block.
            retained.push(replacement);
            return retained.join("\n");
        }
        retained.push(line);
    }
    text.to_owned()
}

fn redact_matching_lines(
    text: &str,
    matches_line: impl Fn(&str) -> bool,
    replacement: &str,
) -> String {
    let mut changed = false;
    let lines = text
        .lines()
        .map(|line| {
            if matches_line(line) {
                changed = true;
                replacement
            } else {
                line
            }
        })
        .collect::<Vec<_>>();
    if changed {
        lines.join("\n")
    } else {
        text.to_owned()
    }
}

fn redact_path_like_tokens(text: &str) -> String {
    text.split_whitespace()
        .map(|token| {
            let core = token.trim_matches(|ch: char| {
                matches!(ch, '"' | '\'' | '`' | ',' | ';' | ')' | '(' | '[' | ']')
            });
            if is_path_like_token(core) {
                PATH_REDACTION
            } else {
                token
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_path_like_token(token: &str) -> bool {
    let bytes = token.as_bytes();
    (bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/'))
        || (token.starts_with('/') && token[1..].contains('/'))
}

fn redact_run_id_tokens(text: &str) -> String {
    text.split_whitespace()
        .map(|token| {
            let core = token.trim_matches(|ch: char| {
                matches!(ch, '"' | '\'' | '`' | ',' | ';' | ')' | '(' | '[' | ']')
            });
            if is_internal_run_id_like(core) {
                RUN_ID_REDACTION
            } else {
                token
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_internal_run_id_like(token: &str) -> bool {
    token.strip_prefix("run_").is_some_and(|rest| rest.len() >= 12)
        || (token.len() >= 20
            && token.starts_with("01")
            && token.bytes().all(|byte| byte.is_ascii_alphanumeric()))
}

fn redact_policy_reason_tokens(text: &str) -> String {
    text.split_whitespace()
        .map(|token| {
            if token.starts_with("policy/") || token.starts_with("reason_code=") {
                "<reason_code_redacted>"
            } else {
                token
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_utf8(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}...", &text[..end])
}

/// Rescue-mode command vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RescueCommand {
    /// Read status only.
    Status,
    /// Read health only.
    Health,
    /// Read recent redacted errors.
    RecentErrors,
    /// Pause all tool execution.
    PauseAllToolExecution,
    /// Pause channel ingress.
    PauseChannelIngress,
    /// Disable one connector.
    DisableConnector,
    /// Create a backup.
    CreateBackup,
    /// Export a redacted support bundle.
    SupportBundle,
    /// Run offline doctor diagnostics.
    OfflineDoctor,
    /// Request token rotation without exposing token material.
    TokenRotationRequest,
    /// Restart one worker.
    RestartWorker,
    /// Enter safe mode.
    SafeMode,
    /// Disallowed generic shell access.
    GeneralShell,
    /// Disallowed raw secret read.
    ReadRawSecret,
}

impl RescueCommand {
    /// Returns the stable wire string for the rescue command.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Health => "health",
            Self::RecentErrors => "recent_errors",
            Self::PauseAllToolExecution => "pause_all_tool_execution",
            Self::PauseChannelIngress => "pause_channel_ingress",
            Self::DisableConnector => "disable_connector",
            Self::CreateBackup => "create_backup",
            Self::SupportBundle => "support_bundle",
            Self::OfflineDoctor => "offline_doctor",
            Self::TokenRotationRequest => "token_rotation_request",
            Self::RestartWorker => "restart_worker",
            Self::SafeMode => "safe_mode",
            Self::GeneralShell => "general_shell",
            Self::ReadRawSecret => "read_raw_secret",
        }
    }

    const fn is_never_allowed(self) -> bool {
        matches!(self, Self::GeneralShell | Self::ReadRawSecret)
    }

    const fn is_read_only(self) -> bool {
        matches!(self, Self::Status | Self::Health | Self::RecentErrors | Self::OfflineDoctor)
    }
}

/// Rescue-mode runtime posture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorRescueMode {
    /// Whether rescue mode is enabled at all.
    pub enabled: bool,
    /// Whether remote rescue commands may be accepted.
    pub remote_commands_allowed: bool,
    /// Number of completed approval steps for the current command.
    pub approval_steps_completed: u8,
    /// Restrictive policy namespace used for audit and enforcement.
    pub policy_namespace: String,
}

impl Default for OperatorRescueMode {
    fn default() -> Self {
        Self {
            enabled: false,
            remote_commands_allowed: false,
            approval_steps_completed: 0,
            policy_namespace: "rescue.restricted".to_owned(),
        }
    }
}

/// Rescue command decision status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RescueDecisionStatus {
    /// Command may run.
    Allowed,
    /// Command is recognized but still needs approvals.
    RequiresApproval,
    /// Command is denied.
    Denied,
}

/// Decision for one rescue command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RescueCommandDecision {
    /// Command that was evaluated.
    pub command: RescueCommand,
    /// Decision status.
    pub status: RescueDecisionStatus,
    /// Stable reason code.
    pub reason_code: String,
    /// Required human approval steps before execution.
    pub required_approval_steps: u8,
    /// Audit event type the caller should record.
    pub audit_event_type: String,
    /// Redaction boundary for command output.
    pub redaction_level: String,
}

/// Evaluates whether a rescue command can run under the current rescue posture.
#[must_use]
pub fn evaluate_rescue_command(
    mode: &OperatorRescueMode,
    command: RescueCommand,
    remote_request: bool,
) -> RescueCommandDecision {
    if command.is_never_allowed() {
        return rescue_decision(
            command,
            RescueDecisionStatus::Denied,
            "rescue.command.never_allowed",
            0,
        );
    }
    if !mode.enabled {
        return rescue_decision(command, RescueDecisionStatus::Denied, "rescue.mode.disabled", 0);
    }
    if remote_request && !mode.remote_commands_allowed {
        return rescue_decision(command, RescueDecisionStatus::Denied, "rescue.remote.denied", 0);
    }
    if command.is_read_only() {
        return rescue_decision(
            command,
            RescueDecisionStatus::Allowed,
            "rescue.command.read_only",
            0,
        );
    }
    if mode.approval_steps_completed < 2 {
        return rescue_decision(
            command,
            RescueDecisionStatus::RequiresApproval,
            "rescue.command.requires_two_step_approval",
            2,
        );
    }
    rescue_decision(command, RescueDecisionStatus::Allowed, "rescue.command.two_step_approved", 2)
}

fn rescue_decision(
    command: RescueCommand,
    status: RescueDecisionStatus,
    reason_code: &str,
    required_approval_steps: u8,
) -> RescueCommandDecision {
    RescueCommandDecision {
        command,
        status,
        reason_code: reason_code.to_owned(),
        required_approval_steps,
        audit_event_type: RESCUE_COMMAND_EXECUTED_EVENT_TYPE.to_owned(),
        redaction_level: "rescue_surface_sanitized".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attack_surface_flags_public_discord_process_without_approval() {
        let graph = AttackSurfaceGraph {
            ingress: vec![IngressExposure {
                source_id: "discord.public".to_owned(),
                source: IngressSurfaceKind::DiscordChannel,
                principal: "channel_user".to_owned(),
                channel_scope: Some("guild:public".to_owned()),
                channel_exposure: ChannelExposure::Public,
                admin_auth_required: false,
                webhook_signature_required: false,
                approval_requirement: ApprovalRequirement::None,
            }],
            tools: vec![ToolExposure {
                tool_name: "palyra.process.run".to_owned(),
                target_surfaces: vec![IngressSurfaceKind::DiscordChannel],
                side_effect: SideEffectLevel::ProcessExecution,
                approval_requirement: ApprovalRequirement::None,
                sandbox_tier: SandboxTier::TierB,
                process_access: ProcessAccess::HostWildcard,
                egress_access: EgressAccess::Allowlisted,
            }],
            ..AttackSurfaceGraph::default()
        };

        let audit = audit_attack_surface_graph(&graph);

        assert_eq!(audit.summary.critical_findings, 1);
        assert!(audit.findings.iter().any(|finding| {
            finding.reason_code == AttackSurfaceReasonCode::PublicChannelProcessWithoutApproval
        }));
        assert_eq!(
            audit.summary.highest_side_effect_without_human_approval,
            SideEffectLevel::ProcessExecution
        );
    }

    #[test]
    fn attack_surface_flags_remote_admin_without_auth() {
        let graph = AttackSurfaceGraph {
            ingress: vec![IngressExposure {
                source_id: "console.remote".to_owned(),
                source: IngressSurfaceKind::ConsoleApi,
                principal: "admin".to_owned(),
                channel_scope: Some("0.0.0.0:7142".to_owned()),
                channel_exposure: ChannelExposure::Public,
                admin_auth_required: false,
                webhook_signature_required: false,
                approval_requirement: ApprovalRequirement::None,
            }],
            ..AttackSurfaceGraph::default()
        };

        let audit = audit_attack_surface_graph(&graph);

        assert!(audit.findings.iter().any(|finding| {
            finding.reason_code == AttackSurfaceReasonCode::RemoteAdminWithoutAuth
                && finding.severity == SecurityPostureSeverity::Critical
        }));
    }

    #[test]
    fn attack_surface_serialization_contract_is_stable() {
        let graph = AttackSurfaceGraph::default();
        let encoded = serde_json::to_string(&graph).expect("graph should serialize");
        let decoded: AttackSurfaceGraph =
            serde_json::from_str(encoded.as_str()).expect("graph should deserialize");

        assert_eq!(decoded.schema_version, SECURITY_POSTURE_SCHEMA_VERSION);
        assert!(encoded.contains("\"schema_version\":1"));
    }

    #[test]
    fn discord_outbound_sanitizer_removes_stack_trace_provider_body_paths_and_run_ids() {
        let message = OutboundMessage {
            surface: SurfaceKind::DiscordHumanChannel,
            text: "raw provider body: {\"token\":\"abc\"}\nstderr: failed at C:\\work\\secret.txt\nstack backtrace:\nrun_0123456789abcdef reason_code=policy/denied"
                .to_owned(),
        };

        let report = sanitize_outbound_message(&message);

        assert!(!report.sanitized_text.contains("abc"));
        assert!(!report.sanitized_text.contains("C:\\work\\secret.txt"));
        assert!(!report.sanitized_text.contains("run_0123456789abcdef"));
        assert!(!report.sanitized_text.contains("policy/denied"));
        assert!(report.sanitized_text.contains(PROVIDER_ERROR_REDACTION));
        assert!(report
            .audit_event
            .reason_codes
            .contains(&"provider_error_body_removed".to_owned()));
        assert_eq!(report.audit_event.event_type, OUTBOUND_SANITIZED_EVENT_TYPE);
    }

    #[test]
    fn outbound_sanitizer_removes_complete_multiline_provider_and_stderr_blocks() {
        for (label, body, replacement, reason) in [
            (
                "provider body:",
                "upstream customer payload\n\nsecond secret paragraph",
                PROVIDER_ERROR_REDACTION,
                "provider_error_body_removed",
            ),
            (
                "stderr:",
                "first raw process line\n\nsecond raw process paragraph",
                STDERR_REDACTION,
                "tool_stderr_removed",
            ),
        ] {
            let message = OutboundMessage {
                surface: SurfaceKind::DiscordHumanChannel,
                text: format!("safe prefix\n{label}\n{body}"),
            };

            let report = sanitize_outbound_message(&message);

            assert_eq!(report.sanitized_text, format!("safe prefix {replacement}"));
            assert!(!report.sanitized_text.contains(body));
            assert!(report.audit_event.reason_codes.contains(&reason.to_owned()));
        }
    }

    #[test]
    fn internal_diagnostics_policy_keeps_operational_context_but_redacts_secrets() {
        let message = OutboundMessage {
            surface: SurfaceKind::InternalDiagnostics,
            text:
                "provider body: token=abc\nstderr: C:\\work\\trace.txt\nreason_code=policy/denied"
                    .to_owned(),
        };

        let report = sanitize_outbound_message(&message);

        assert!(report.sanitized_text.contains("provider body"));
        assert!(report.sanitized_text.contains("stderr"));
        assert!(report.sanitized_text.contains("C:\\work\\trace.txt"));
        assert!(report.sanitized_text.contains("reason_code=policy/denied"));
        assert!(!report.sanitized_text.contains("token=abc"));
    }

    #[test]
    fn rescue_mode_default_denies_remote_commands() {
        let decision =
            evaluate_rescue_command(&OperatorRescueMode::default(), RescueCommand::Status, true);

        assert_eq!(decision.status, RescueDecisionStatus::Denied);
        assert_eq!(decision.reason_code, "rescue.mode.disabled");
    }

    #[test]
    fn rescue_write_command_requires_two_step_approval() {
        let mode = OperatorRescueMode {
            enabled: true,
            remote_commands_allowed: false,
            approval_steps_completed: 1,
            policy_namespace: "rescue.restricted".to_owned(),
        };

        let decision = evaluate_rescue_command(&mode, RescueCommand::PauseAllToolExecution, false);

        assert_eq!(decision.status, RescueDecisionStatus::RequiresApproval);
        assert_eq!(decision.required_approval_steps, 2);
        assert_eq!(decision.reason_code, "rescue.command.requires_two_step_approval");
    }

    #[test]
    fn rescue_never_allows_shell_or_raw_secret_access() {
        let mode = OperatorRescueMode {
            enabled: true,
            remote_commands_allowed: true,
            approval_steps_completed: 2,
            policy_namespace: "rescue.restricted".to_owned(),
        };

        for command in [RescueCommand::GeneralShell, RescueCommand::ReadRawSecret] {
            let decision = evaluate_rescue_command(&mode, command, false);
            assert_eq!(decision.status, RescueDecisionStatus::Denied);
            assert_eq!(decision.reason_code, "rescue.command.never_allowed");
        }
    }
}
