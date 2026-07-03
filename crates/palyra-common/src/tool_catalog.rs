//! Static catalog mapping built-in tool names to capabilities and approval sensitivity.
//!
//! This is the deny-by-default source of truth the daemon's policy and approval layers
//! consult before dispatching a tool call: unknown tools always require approval, and
//! capability names here must match the policy engine's vocabulary.

use std::{collections::BTreeSet, error::Error, fmt};

use serde::{Deserialize, Serialize};

/// A capability class a tool may exercise, used for policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCapability {
    ProcessExec,
    Network,
    SecretsRead,
    FilesystemRead,
    FilesystemWrite,
    ArtifactsRead,
}

impl ToolCapability {
    /// Returns the policy-engine name for this capability.
    #[must_use]
    pub const fn policy_name(self) -> &'static str {
        match self {
            Self::ProcessExec => "process_exec",
            Self::Network => "network",
            Self::SecretsRead => "secrets_read",
            Self::FilesystemRead => "filesystem_read",
            Self::FilesystemWrite => "filesystem_write",
            Self::ArtifactsRead => "artifacts_read",
        }
    }
}

/// Capabilities and default approval sensitivity for one catalog tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToolMetadata {
    pub capabilities: &'static [ToolCapability],
    pub default_sensitive: bool,
}

/// Built-in toolset profiles that expand into concrete tool names before
/// runtime policy and catalog visibility gates run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolsetProfileName {
    SafeChat,
    Code,
    Research,
    Automation,
    Ops,
}

impl ToolsetProfileName {
    /// Parses a profile identifier from config or environment input.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "safe_chat" => Some(Self::SafeChat),
            "code" => Some(Self::Code),
            "research" => Some(Self::Research),
            "automation" => Some(Self::Automation),
            "ops" => Some(Self::Ops),
            _ => None,
        }
    }

    /// Stable config label used in snapshots and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SafeChat => "safe_chat",
            Self::Code => "code",
            Self::Research => "research",
            Self::Automation => "automation",
            Self::Ops => "ops",
        }
    }

    /// Ordered tool names granted by this profile before explicit overrides.
    #[must_use]
    pub const fn tools(self) -> &'static [&'static str] {
        match self {
            Self::SafeChat => SAFE_CHAT_PROFILE_TOOLS,
            Self::Code => CODE_PROFILE_TOOLS,
            Self::Research => RESEARCH_PROFILE_TOOLS,
            Self::Automation => AUTOMATION_PROFILE_TOOLS,
            Self::Ops => OPS_PROFILE_TOOLS,
        }
    }
}

/// Expanded tools for one profile, preserved in catalog metadata for explainability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolsetProfileExpansion {
    pub profile: String,
    pub tools: Vec<String>,
}

/// Deterministic expansion report for profiles plus explicit allow/deny overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolsetProfileExpansionReport {
    pub profiles: Vec<String>,
    pub profile_expansions: Vec<ToolsetProfileExpansion>,
    pub explicit_allowed_tools: Vec<String>,
    pub extra_tools: Vec<String>,
    pub disabled_tools: Vec<String>,
    pub effective_allowed_tools: Vec<String>,
}

/// Invalid profile identifier found while expanding a toolset profile list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolsetProfileError {
    invalid_profile: String,
}

impl ToolsetProfileError {
    /// Returns the profile identifier that failed parsing.
    #[must_use]
    pub fn invalid_profile(&self) -> &str {
        self.invalid_profile.as_str()
    }
}

impl fmt::Display for ToolsetProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown toolset profile '{}'", self.invalid_profile)
    }
}

impl Error for ToolsetProfileError {}

const EMPTY_TOOL_CAPABILITIES: &[ToolCapability] = &[];
const PROCESS_RUNNER_CAPABILITIES: &[ToolCapability] = &[ToolCapability::ProcessExec];
const WORKSPACE_FILE_READ_CAPABILITIES: &[ToolCapability] = &[ToolCapability::FilesystemRead];
const WORKSPACE_PATCH_CAPABILITIES: &[ToolCapability] = &[ToolCapability::FilesystemWrite];
const OS_FILE_CAPABILITIES: &[ToolCapability] =
    &[ToolCapability::FilesystemRead, ToolCapability::FilesystemWrite];
const NETWORK_TOOL_CAPABILITIES: &[ToolCapability] = &[ToolCapability::Network];
const BROWSER_ARTIFACT_WRITE_CAPABILITIES: &[ToolCapability] =
    &[ToolCapability::Network, ToolCapability::FilesystemWrite];
const BROWSER_UPLOAD_CAPABILITIES: &[ToolCapability] =
    &[ToolCapability::Network, ToolCapability::FilesystemRead, ToolCapability::SecretsRead];
const HTTP_FETCH_TOOL_CAPABILITIES: &[ToolCapability] =
    &[ToolCapability::Network, ToolCapability::SecretsRead];
const ARTIFACT_READ_CAPABILITIES: &[ToolCapability] = &[ToolCapability::ArtifactsRead];
const WASM_PLUGIN_CAPABILITIES: &[ToolCapability] =
    &[ToolCapability::Network, ToolCapability::SecretsRead, ToolCapability::FilesystemWrite];

const SAFE_CHAT_PROFILE_TOOLS: &[&str] = &[
    "palyra.echo",
    "palyra.sleep",
    "palyra.memory.status",
    "palyra.memory.search",
    "palyra.memory.session_search",
    "palyra.session_search",
    "palyra.memory.reflect",
    "palyra.routines.query",
    "palyra.delegation.query",
    "palyra.artifact.read",
    "palyra.image.observe",
];

const CODE_PROFILE_TOOLS: &[&str] = &[
    "palyra.echo",
    "palyra.sleep",
    "palyra.memory.status",
    "palyra.memory.search",
    "palyra.memory.session_search",
    "palyra.session_search",
    "palyra.fs.read_file",
    "palyra.fs.list_dir",
    "palyra.fs.search",
    "palyra.fs.apply_patch",
    "palyra.process.run",
];

const RESEARCH_PROFILE_TOOLS: &[&str] = &[
    "palyra.echo",
    "palyra.sleep",
    "palyra.memory.status",
    "palyra.memory.search",
    "palyra.memory.recall",
    "palyra.memory.session_search",
    "palyra.session_search",
    "palyra.http.fetch",
    "palyra.browser.session.create",
    "palyra.browser.navigate",
    "palyra.browser.reload",
    "palyra.browser.title",
    "palyra.browser.observe",
    "palyra.browser.screenshot",
    "palyra.browser.pdf",
    "palyra.browser.scroll",
    "palyra.browser.wait_for",
    "palyra.browser.tabs.list",
    "palyra.browser.tabs.open",
    "palyra.browser.tabs.switch",
    "palyra.browser.tabs.close",
];

const AUTOMATION_PROFILE_TOOLS: &[&str] = &[
    "palyra.echo",
    "palyra.sleep",
    "palyra.memory.status",
    "palyra.memory.search",
    "palyra.memory.retain",
    "palyra.retain",
    "palyra.memory.replace",
    "palyra.memory.delete",
    "palyra.routines.query",
    "palyra.routines.control",
    "palyra.delegation.query",
    "palyra.delegation.control",
    "sessions_spawn",
    "palyra.http.fetch",
    "palyra.browser.session.create",
    "palyra.browser.session.close",
    "palyra.browser.navigate",
    "palyra.browser.click",
    "palyra.browser.type",
    "palyra.browser.fill",
    "palyra.browser.press",
    "palyra.browser.select",
    "palyra.browser.scroll",
    "palyra.browser.wait_for",
];

const OPS_PROFILE_TOOLS: &[&str] = &[
    "palyra.echo",
    "palyra.sleep",
    "palyra.memory.status",
    "palyra.memory.search",
    "palyra.fs.read_file",
    "palyra.fs.list_dir",
    "palyra.fs.search",
    "palyra.fs.apply_patch",
    "palyra.fs.os_file",
    "palyra.http.fetch",
    "palyra.process.run",
    "palyra.tool_program.run",
    "palyra.plugin.run",
    "palyra.browser.session.create",
    "palyra.browser.session.close",
    "palyra.browser.navigate",
    "palyra.browser.reload",
    "palyra.browser.observe",
    "palyra.browser.storage",
    "palyra.browser.network_log",
    "palyra.browser.console_log",
    "palyra.browser.downloads.list",
    "palyra.browser.downloads.get",
    "palyra.browser.permissions.get",
    "palyra.browser.permissions.set",
    "palyra.browser.reset_state",
];

/// Policy names of the capabilities that always force approval gating.
pub const SENSITIVE_CAPABILITY_POLICY_NAMES: &[&str] =
    &["process_exec", "network", "secrets_read", "filesystem_write"];

/// Catalog exposure mode used when projecting an authorized tool catalog to a provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCatalogExposureMode {
    Direct,
    Compact,
    Hybrid,
}

impl ToolCatalogExposureMode {
    /// Parses a config or environment label.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "direct" => Some(Self::Direct),
            "compact" => Some(Self::Compact),
            "hybrid" => Some(Self::Hybrid),
            _ => None,
        }
    }

    /// Stable snake_case label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Compact => "compact",
            Self::Hybrid => "hybrid",
        }
    }
}

/// Expands profile names and explicit overrides into an effective tool allowlist.
///
/// `disabled_tools` is applied last and therefore wins over profile, explicit,
/// and extra grants. `palyra.process.run` expands to its lifecycle companions
/// before disables are applied so operators can still remove a companion by name.
///
/// # Errors
/// Returns [`ToolsetProfileError`] when a profile name is not built in.
pub fn expand_toolset_profiles(
    profiles: &[String],
    explicit_allowed_tools: &[String],
    extra_tools: &[String],
    disabled_tools: &[String],
) -> Result<ToolsetProfileExpansionReport, ToolsetProfileError> {
    let mut effective = Vec::new();
    let mut seen = BTreeSet::new();
    let mut profile_names = Vec::new();
    let mut profile_expansions = Vec::new();

    for profile in normalize_configured_tool_names(profiles) {
        let Some(profile_name) = ToolsetProfileName::parse(profile.as_str()) else {
            return Err(ToolsetProfileError { invalid_profile: profile });
        };
        let label = profile_name.as_str().to_owned();
        profile_names.push(label.clone());
        let tools = normalize_profile_tools(profile_name.tools());
        for tool in &tools {
            push_effective_tool(tool.as_str(), &mut effective, &mut seen);
        }
        profile_expansions.push(ToolsetProfileExpansion { profile: label, tools });
    }

    let explicit_allowed_tools = normalize_configured_tool_names(explicit_allowed_tools);
    for tool in &explicit_allowed_tools {
        push_effective_tool(tool.as_str(), &mut effective, &mut seen);
    }

    let extra_tools = normalize_configured_tool_names(extra_tools);
    for tool in &extra_tools {
        push_effective_tool(tool.as_str(), &mut effective, &mut seen);
    }

    let disabled_tools = normalize_configured_tool_names(disabled_tools);
    let disabled = disabled_tools.iter().cloned().collect::<BTreeSet<_>>();
    effective.retain(|tool| !disabled.contains(tool));

    Ok(ToolsetProfileExpansionReport {
        profiles: profile_names,
        profile_expansions,
        explicit_allowed_tools,
        extra_tools,
        disabled_tools,
        effective_allowed_tools: effective,
    })
}

/// Returns normalized unique tool/profile identifiers from config input.
#[must_use]
pub fn normalize_configured_tool_names(values: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let candidate = value.trim().to_ascii_lowercase();
        if candidate.is_empty() || !seen.insert(candidate.clone()) {
            continue;
        }
        normalized.push(candidate);
    }
    normalized
}

fn normalize_profile_tools(tools: &[&str]) -> Vec<String> {
    let mut normalized = Vec::new();
    let mut seen = BTreeSet::new();
    for tool in tools {
        push_effective_tool(tool, &mut normalized, &mut seen);
    }
    normalized
}

fn push_effective_tool(tool: &str, tools: &mut Vec<String>, seen: &mut BTreeSet<String>) {
    let tool = tool.trim().to_ascii_lowercase();
    if tool.is_empty() {
        return;
    }
    if tool == "palyra.process.run" {
        for companion in ["palyra.process.stop", "palyra.process.status", "palyra.process.list"] {
            if seen.insert(companion.to_owned()) {
                tools.push(companion.to_owned());
            }
        }
    }
    if seen.insert(tool.clone()) {
        tools.push(tool);
    }
}

/// Looks up catalog metadata for a tool name; `None` means the tool is not built in.
#[must_use]
pub fn tool_metadata(tool_name: &str) -> Option<ToolMetadata> {
    match tool_name {
        "palyra.echo" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.sleep" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.tools.search" | "palyra.tools.describe" | "palyra.tools.invoke" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.memory.status" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.memory.search" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.memory.recall" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.memory.session_search" | "palyra.session_search" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.memory.retain" | "palyra.retain" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.memory.delete" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.memory.replace" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.memory.reflect" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.routines.query" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.routines.control" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.delegation.query" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.delegation.control" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "sessions_spawn" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.plan.manage" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: false })
        }
        "palyra.artifact.read" => Some(ToolMetadata {
            capabilities: ARTIFACT_READ_CAPABILITIES,
            default_sensitive: false,
        }),
        "palyra.image.observe" => Some(ToolMetadata {
            capabilities: ARTIFACT_READ_CAPABILITIES,
            default_sensitive: false,
        }),
        "palyra.http.fetch" => Some(ToolMetadata {
            capabilities: HTTP_FETCH_TOOL_CAPABILITIES,
            default_sensitive: true,
        }),
        "palyra.process.run"
        | "palyra.process.stop"
        | "palyra.process.status"
        | "palyra.process.list" => Some(ToolMetadata {
            capabilities: PROCESS_RUNNER_CAPABILITIES,
            default_sensitive: true,
        }),
        "palyra.tool_program.run" => {
            Some(ToolMetadata { capabilities: EMPTY_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.fs.read_file" => Some(ToolMetadata {
            capabilities: WORKSPACE_FILE_READ_CAPABILITIES,
            default_sensitive: false,
        }),
        "palyra.fs.list_dir" => Some(ToolMetadata {
            capabilities: WORKSPACE_FILE_READ_CAPABILITIES,
            default_sensitive: false,
        }),
        "palyra.fs.search" => Some(ToolMetadata {
            capabilities: WORKSPACE_FILE_READ_CAPABILITIES,
            default_sensitive: false,
        }),
        "palyra.fs.apply_patch" => Some(ToolMetadata {
            capabilities: WORKSPACE_PATCH_CAPABILITIES,
            default_sensitive: true,
        }),
        "palyra.fs.os_file" => {
            Some(ToolMetadata { capabilities: OS_FILE_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.session.create" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.session.close" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.navigate" | "palyra.browser.reload" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.click" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.type" | "palyra.browser.fill" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.upload" => Some(ToolMetadata {
            capabilities: BROWSER_UPLOAD_CAPABILITIES,
            default_sensitive: true,
        }),
        "palyra.browser.press" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.select" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.viewport" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.highlight" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.scroll" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.wait_for" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.title" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.screenshot" | "palyra.browser.pdf" => Some(ToolMetadata {
            capabilities: BROWSER_ARTIFACT_WRITE_CAPABILITIES,
            default_sensitive: true,
        }),
        "palyra.browser.observe" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.storage" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.network_log" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.console_log" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.reset_state" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.tabs.list" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.tabs.open" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.tabs.switch" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.tabs.close" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.permissions.get" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.permissions.set" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.downloads.list" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.downloads.get" => Some(ToolMetadata {
            capabilities: BROWSER_ARTIFACT_WRITE_CAPABILITIES,
            default_sensitive: true,
        }),
        "palyra.plugin.run" => {
            Some(ToolMetadata { capabilities: WASM_PLUGIN_CAPABILITIES, default_sensitive: true })
        }
        _ => None,
    }
}

/// Returns whether a tool call must pass approval before execution.
///
/// Fails closed: tools missing from the catalog require approval unconditionally.
#[must_use]
pub fn tool_requires_approval(tool_name: &str) -> bool {
    let Some(metadata) = tool_metadata(tool_name) else {
        return true;
    };
    metadata.default_sensitive
        || metadata.capabilities.iter().any(|capability| {
            matches!(
                capability,
                ToolCapability::ProcessExec
                    | ToolCapability::Network
                    | ToolCapability::SecretsRead
                    | ToolCapability::FilesystemWrite
            )
        })
}

/// Returns the sorted, deduplicated policy capability names for a tool (empty if unknown).
#[must_use]
pub fn tool_policy_capability_names(tool_name: &str) -> Vec<String> {
    let Some(metadata) = tool_metadata(tool_name) else {
        return Vec::new();
    };
    let mut capabilities = metadata
        .capabilities
        .iter()
        .map(|capability| capability.policy_name().to_owned())
        .collect::<Vec<_>>();
    capabilities.sort();
    capabilities.dedup();
    capabilities
}

/// Filters an allowlist down to approval-requiring tools, lowercased for stable matching.
#[must_use]
pub fn sensitive_allowlisted_tool_names(allowlisted_tools: &[String]) -> Vec<String> {
    allowlisted_tools
        .iter()
        .filter(|tool_name| tool_requires_approval(tool_name.as_str()))
        .map(|tool_name| tool_name.to_ascii_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_runner_is_approval_required() {
        assert!(tool_requires_approval("palyra.process.run"));
        assert_eq!(tool_policy_capability_names("palyra.process.run"), vec!["process_exec"]);
    }

    #[test]
    fn echo_is_not_approval_required() {
        assert!(!tool_requires_approval("palyra.echo"));
        assert!(tool_policy_capability_names("palyra.echo").is_empty());
    }

    #[test]
    fn memory_retain_alias_matches_canonical_sensitivity() {
        assert!(tool_requires_approval("palyra.memory.retain"));
        assert!(tool_requires_approval("palyra.retain"));
        assert!(tool_policy_capability_names("palyra.retain").is_empty());
    }

    #[test]
    fn sessions_spawn_is_sensitive_without_extra_policy_capabilities() {
        assert!(tool_requires_approval("sessions_spawn"));
        assert!(tool_policy_capability_names("sessions_spawn").is_empty());
    }

    #[test]
    fn memory_status_is_read_only_without_approval() {
        assert!(!tool_requires_approval("palyra.memory.status"));
        assert!(tool_policy_capability_names("palyra.memory.status").is_empty());
    }

    #[test]
    fn code_profile_expands_stably_and_disabled_tools_win() {
        let report = expand_toolset_profiles(
            &[String::from("code")],
            &[String::from("palyra.echo")],
            &[String::from("palyra.http.fetch")],
            &[String::from("palyra.process.status")],
        )
        .expect("code profile should be valid");

        assert_eq!(report.profiles, vec!["code"]);
        assert!(report.effective_allowed_tools.contains(&"palyra.fs.apply_patch".to_owned()));
        assert!(report.effective_allowed_tools.contains(&"palyra.process.run".to_owned()));
        assert!(!report.effective_allowed_tools.contains(&"palyra.process.status".to_owned()));
        assert_eq!(
            report
                .effective_allowed_tools
                .iter()
                .filter(|tool| tool.as_str() == "palyra.echo")
                .count(),
            1
        );
    }

    #[test]
    fn automation_profile_exposes_sessions_spawn() {
        let report = expand_toolset_profiles(&[String::from("automation")], &[], &[], &[])
            .expect("automation profile should be valid");

        assert!(report.effective_allowed_tools.contains(&"sessions_spawn".to_owned()));
    }

    #[test]
    fn tool_catalog_bridge_tools_do_not_require_approval() {
        for tool_name in ["palyra.tools.search", "palyra.tools.describe", "palyra.tools.invoke"] {
            assert!(!tool_requires_approval(tool_name));
            assert!(tool_policy_capability_names(tool_name).is_empty());
        }
    }

    #[test]
    fn workspace_read_tools_are_not_approval_required_by_default() {
        for tool_name in ["palyra.fs.read_file", "palyra.fs.list_dir", "palyra.fs.search"] {
            assert!(!tool_requires_approval(tool_name), "{tool_name} should be read-only");
            assert_eq!(tool_policy_capability_names(tool_name), vec!["filesystem_read"]);
        }
    }

    #[test]
    fn image_observe_is_read_only_without_approval() {
        assert!(!tool_requires_approval("palyra.image.observe"));
        assert_eq!(tool_policy_capability_names("palyra.image.observe"), vec!["artifacts_read"]);
    }

    #[test]
    fn browser_reload_matches_browser_network_sensitivity() {
        assert!(tool_requires_approval("palyra.browser.reload"));
        assert_eq!(tool_policy_capability_names("palyra.browser.reload"), vec!["network"]);
    }

    #[test]
    fn browser_upload_exposes_network_filesystem_and_secret_read_capabilities() {
        assert!(tool_requires_approval("palyra.browser.upload"));
        assert_eq!(
            tool_policy_capability_names("palyra.browser.upload"),
            vec!["filesystem_read", "network", "secrets_read"]
        );
    }

    #[test]
    fn browser_artifact_output_tools_include_filesystem_write_capability() {
        for tool_name in
            ["palyra.browser.screenshot", "palyra.browser.pdf", "palyra.browser.downloads.get"]
        {
            assert!(tool_requires_approval(tool_name));
            assert_eq!(
                tool_policy_capability_names(tool_name),
                vec!["filesystem_write", "network"],
                "{tool_name} must advertise both browser/network and output_path write effects"
            );
        }
    }
}
