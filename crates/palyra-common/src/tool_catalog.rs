//! Static catalog mapping built-in tool names to capabilities and approval sensitivity.
//!
//! This is the deny-by-default source of truth the daemon's policy and approval layers
//! consult before dispatching a tool call: unknown tools always require approval, and
//! capability names here must match the policy engine's vocabulary.

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

const EMPTY_TOOL_CAPABILITIES: &[ToolCapability] = &[];
const PROCESS_RUNNER_CAPABILITIES: &[ToolCapability] = &[ToolCapability::ProcessExec];
const WORKSPACE_FILE_READ_CAPABILITIES: &[ToolCapability] = &[ToolCapability::FilesystemRead];
const WORKSPACE_PATCH_CAPABILITIES: &[ToolCapability] = &[ToolCapability::FilesystemWrite];
const OS_FILE_CAPABILITIES: &[ToolCapability] =
    &[ToolCapability::FilesystemRead, ToolCapability::FilesystemWrite];
const NETWORK_TOOL_CAPABILITIES: &[ToolCapability] = &[ToolCapability::Network];
const HTTP_FETCH_TOOL_CAPABILITIES: &[ToolCapability] =
    &[ToolCapability::Network, ToolCapability::SecretsRead];
const ARTIFACT_READ_CAPABILITIES: &[ToolCapability] = &[ToolCapability::ArtifactsRead];
const WASM_PLUGIN_CAPABILITIES: &[ToolCapability] =
    &[ToolCapability::Network, ToolCapability::SecretsRead, ToolCapability::FilesystemWrite];

/// Policy names of the capabilities that always force approval gating.
pub const SENSITIVE_CAPABILITY_POLICY_NAMES: &[&str] =
    &["process_exec", "network", "secrets_read", "filesystem_write"];

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
        "palyra.artifact.read" => Some(ToolMetadata {
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
        "palyra.browser.type" | "palyra.browser.fill" | "palyra.browser.upload" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
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
        "palyra.browser.screenshot" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
        "palyra.browser.pdf" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
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
        "palyra.browser.downloads.list" | "palyra.browser.downloads.get" => {
            Some(ToolMetadata { capabilities: NETWORK_TOOL_CAPABILITIES, default_sensitive: true })
        }
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
    fn memory_status_is_read_only_without_approval() {
        assert!(!tool_requires_approval("palyra.memory.status"));
        assert!(tool_policy_capability_names("palyra.memory.status").is_empty());
    }

    #[test]
    fn workspace_read_tools_are_not_approval_required_by_default() {
        for tool_name in ["palyra.fs.read_file", "palyra.fs.list_dir", "palyra.fs.search"] {
            assert!(!tool_requires_approval(tool_name), "{tool_name} should be read-only");
            assert_eq!(tool_policy_capability_names(tool_name), vec!["filesystem_read"]);
        }
    }

    #[test]
    fn browser_reload_matches_browser_network_sensitivity() {
        assert!(tool_requires_approval("palyra.browser.reload"));
        assert_eq!(tool_policy_capability_names("palyra.browser.reload"), vec!["network"]);
    }
}
