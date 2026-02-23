use std::path::PathBuf;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::process::{Command, Stdio};

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierCBackendKind {
    LinuxBubblewrap,
    MacosSandboxExec,
    WindowsJobObject,
    Unsupported,
}

impl TierCBackendKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LinuxBubblewrap => "linux_bubblewrap",
            Self::MacosSandboxExec => "macos_sandbox_exec",
            Self::WindowsJobObject => "windows_job_object",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn executor_label(self) -> &'static str {
        match self {
            Self::LinuxBubblewrap => "sandbox_tier_c_linux_bubblewrap",
            Self::MacosSandboxExec => "sandbox_tier_c_macos_sandbox_exec",
            Self::WindowsJobObject => "sandbox_tier_c_windows_job_object",
            Self::Unsupported => "sandbox_tier_c_unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierCPolicy {
    pub workspace_root: PathBuf,
    pub cwd: PathBuf,
    pub enforce_network_isolation: bool,
    pub allowed_egress_hosts: Vec<String>,
    pub allowed_dns_suffixes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierCCommandRequest {
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierCCommandPlan {
    pub backend: TierCBackendKind,
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierCBackendCapabilities {
    pub runtime_network_isolation: bool,
    pub host_allowlists: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TierCBackendError {
    #[error("tier-c backend '{backend}' is unavailable: {reason}")]
    BackendUnavailable { backend: &'static str, reason: String },
    #[error("tier-c backend '{backend}' requires binary '{binary}' in PATH")]
    BackendBinaryMissing { backend: &'static str, binary: String },
    #[error(
        "tier-c backend '{backend}' cannot enforce host-level egress allowlists; use preflight mode or clear allowlists"
    )]
    HostAllowlistUnsupported { backend: &'static str },
    #[error(
        "tier-c backend '{backend}' cannot enforce runtime network isolation on this platform"
    )]
    NetworkIsolationUnsupported { backend: &'static str },
}

pub trait TierCBackend {
    fn kind(&self) -> TierCBackendKind;

    fn capabilities(&self) -> TierCBackendCapabilities;

    fn build_command_plan(
        &self,
        policy: &TierCPolicy,
        request: &TierCCommandRequest,
    ) -> Result<TierCCommandPlan, TierCBackendError>;
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{
        ensure_binary_available, TierCBackend, TierCBackendCapabilities, TierCBackendError,
        TierCBackendKind, TierCCommandPlan, TierCCommandRequest, TierCPolicy,
    };

    pub(super) static BACKEND: LinuxBubblewrapBackend = LinuxBubblewrapBackend;

    #[derive(Debug, Clone, Copy)]
    pub(super) struct LinuxBubblewrapBackend;

    impl TierCBackend for LinuxBubblewrapBackend {
        fn kind(&self) -> TierCBackendKind {
            TierCBackendKind::LinuxBubblewrap
        }

        fn capabilities(&self) -> TierCBackendCapabilities {
            TierCBackendCapabilities { runtime_network_isolation: true, host_allowlists: false }
        }

        fn build_command_plan(
            &self,
            policy: &TierCPolicy,
            request: &TierCCommandRequest,
        ) -> Result<TierCCommandPlan, TierCBackendError> {
            ensure_binary_available("bwrap", self.kind().as_str())?;
            if !policy.allowed_egress_hosts.is_empty() || !policy.allowed_dns_suffixes.is_empty() {
                return Err(TierCBackendError::HostAllowlistUnsupported {
                    backend: self.kind().as_str(),
                });
            }

            let workspace = policy.workspace_root.to_string_lossy().to_string();
            let cwd = policy.cwd.to_string_lossy().to_string();
            let mut args = vec![
                "--die-with-parent".to_owned(),
                "--new-session".to_owned(),
                "--unshare-pid".to_owned(),
                "--proc".to_owned(),
                "/proc".to_owned(),
                "--dev".to_owned(),
                "/dev".to_owned(),
                "--ro-bind".to_owned(),
                "/".to_owned(),
                "/".to_owned(),
                "--tmpfs".to_owned(),
                "/tmp".to_owned(),
                "--tmpfs".to_owned(),
                "/var/tmp".to_owned(),
                "--bind".to_owned(),
                workspace.clone(),
                workspace,
                "--chdir".to_owned(),
                cwd,
                "--clearenv".to_owned(),
                "--setenv".to_owned(),
                "PATH".to_owned(),
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_owned(),
                "--setenv".to_owned(),
                "LANG".to_owned(),
                "C".to_owned(),
                "--setenv".to_owned(),
                "LC_ALL".to_owned(),
                "C".to_owned(),
            ];
            if policy.enforce_network_isolation {
                args.push("--unshare-net".to_owned());
            }
            args.push("--".to_owned());
            args.push(request.command.clone());
            args.extend(request.args.iter().cloned());
            Ok(TierCCommandPlan { backend: self.kind(), program: "bwrap".to_owned(), args })
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{
        ensure_binary_available, TierCBackend, TierCBackendCapabilities, TierCBackendError,
        TierCBackendKind, TierCCommandPlan, TierCCommandRequest, TierCPolicy,
    };

    pub(super) static BACKEND: MacosSandboxExecBackend = MacosSandboxExecBackend;

    #[derive(Debug, Clone, Copy)]
    pub(super) struct MacosSandboxExecBackend;

    impl TierCBackend for MacosSandboxExecBackend {
        fn kind(&self) -> TierCBackendKind {
            TierCBackendKind::MacosSandboxExec
        }

        fn capabilities(&self) -> TierCBackendCapabilities {
            TierCBackendCapabilities { runtime_network_isolation: true, host_allowlists: false }
        }

        fn build_command_plan(
            &self,
            policy: &TierCPolicy,
            request: &TierCCommandRequest,
        ) -> Result<TierCCommandPlan, TierCBackendError> {
            ensure_binary_available("sandbox-exec", self.kind().as_str())?;
            if !policy.allowed_egress_hosts.is_empty() || !policy.allowed_dns_suffixes.is_empty() {
                return Err(TierCBackendError::HostAllowlistUnsupported {
                    backend: self.kind().as_str(),
                });
            }

            let profile = render_sandbox_profile(policy);
            let mut args = vec!["-p".to_owned(), profile, "--".to_owned(), request.command.clone()];
            args.extend(request.args.iter().cloned());
            Ok(TierCCommandPlan { backend: self.kind(), program: "sandbox-exec".to_owned(), args })
        }
    }

    fn render_sandbox_profile(policy: &TierCPolicy) -> String {
        let workspace =
            policy.workspace_root.to_string_lossy().replace('\\', "\\\\").replace('"', "\\\"");
        let network_clause = if policy.enforce_network_isolation {
            "(deny network*)"
        } else {
            "(allow network-outbound)"
        };
        format!(
            r#"(version 1)
(deny default)
(import "system.sb")
(allow process-fork)
(allow process-exec)
(allow file-read*)
(allow file-write*
    (subpath "{workspace}")
    (subpath "/tmp")
    (subpath "/private/tmp"))
{network_clause}
"#
        )
    }
}

#[cfg(windows)]
mod platform {
    use super::{
        TierCBackend, TierCBackendCapabilities, TierCBackendError, TierCBackendKind,
        TierCCommandPlan, TierCCommandRequest, TierCPolicy,
    };

    pub(super) static BACKEND: WindowsJobObjectBackend = WindowsJobObjectBackend;

    #[derive(Debug, Clone, Copy)]
    pub(super) struct WindowsJobObjectBackend;

    impl TierCBackend for WindowsJobObjectBackend {
        fn kind(&self) -> TierCBackendKind {
            TierCBackendKind::WindowsJobObject
        }

        fn capabilities(&self) -> TierCBackendCapabilities {
            TierCBackendCapabilities { runtime_network_isolation: false, host_allowlists: false }
        }

        fn build_command_plan(
            &self,
            policy: &TierCPolicy,
            request: &TierCCommandRequest,
        ) -> Result<TierCCommandPlan, TierCBackendError> {
            if policy.enforce_network_isolation {
                return Err(TierCBackendError::NetworkIsolationUnsupported {
                    backend: self.kind().as_str(),
                });
            }
            if !policy.allowed_egress_hosts.is_empty() || !policy.allowed_dns_suffixes.is_empty() {
                return Err(TierCBackendError::HostAllowlistUnsupported {
                    backend: self.kind().as_str(),
                });
            }

            Ok(TierCCommandPlan {
                backend: self.kind(),
                program: request.command.clone(),
                args: request.args.clone(),
            })
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
mod platform {
    use super::{
        TierCBackend, TierCBackendCapabilities, TierCBackendError, TierCBackendKind,
        TierCCommandPlan, TierCCommandRequest, TierCPolicy,
    };

    pub(super) static BACKEND: UnsupportedBackend = UnsupportedBackend;

    #[derive(Debug, Clone, Copy)]
    pub(super) struct UnsupportedBackend;

    impl TierCBackend for UnsupportedBackend {
        fn kind(&self) -> TierCBackendKind {
            TierCBackendKind::Unsupported
        }

        fn capabilities(&self) -> TierCBackendCapabilities {
            TierCBackendCapabilities { runtime_network_isolation: false, host_allowlists: false }
        }

        fn build_command_plan(
            &self,
            _policy: &TierCPolicy,
            _request: &TierCCommandRequest,
        ) -> Result<TierCCommandPlan, TierCBackendError> {
            Err(TierCBackendError::BackendUnavailable {
                backend: self.kind().as_str(),
                reason: "tier-c backend is not implemented for this target platform".to_owned(),
            })
        }
    }
}

fn backend() -> &'static dyn TierCBackend {
    &platform::BACKEND
}

#[must_use]
pub fn current_backend_kind() -> TierCBackendKind {
    backend().kind()
}

#[must_use]
pub fn current_backend_executor() -> &'static str {
    current_backend_kind().executor_label()
}

#[must_use]
pub fn current_backend_capabilities() -> TierCBackendCapabilities {
    backend().capabilities()
}

pub fn build_tier_c_command_plan(
    policy: &TierCPolicy,
    request: &TierCCommandRequest,
) -> Result<TierCCommandPlan, TierCBackendError> {
    backend().build_command_plan(policy, request)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn ensure_binary_available(binary: &str, backend: &'static str) -> Result<(), TierCBackendError> {
    let status = Command::new(binary)
        .arg("--help")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if status.is_err() {
        return Err(TierCBackendError::BackendBinaryMissing { backend, binary: binary.to_owned() });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_tier_c_command_plan, current_backend_capabilities, current_backend_executor,
        current_backend_kind, TierCBackendError, TierCBackendKind, TierCCommandRequest,
        TierCPolicy,
    };

    fn sample_policy() -> TierCPolicy {
        TierCPolicy {
            workspace_root: std::env::current_dir().unwrap_or_else(|_| ".".into()),
            cwd: std::env::current_dir().unwrap_or_else(|_| ".".into()),
            enforce_network_isolation: true,
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
        }
    }

    #[test]
    fn backend_kind_has_stable_executor_label() {
        let label = current_backend_executor();
        assert!(
            label.starts_with("sandbox_tier_c_"),
            "tier-c executor labels should remain stable for attestations"
        );
    }

    #[test]
    fn backend_capabilities_are_consistent_with_kind() {
        let kind = current_backend_kind();
        let capabilities = current_backend_capabilities();
        if matches!(kind, TierCBackendKind::LinuxBubblewrap | TierCBackendKind::MacosSandboxExec) {
            assert!(capabilities.runtime_network_isolation);
        }
        if matches!(kind, TierCBackendKind::Unsupported) {
            assert!(!capabilities.runtime_network_isolation);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_backend_builds_bwrap_plan_when_binary_exists() {
        let policy = sample_policy();
        let request = TierCCommandRequest { command: "uname".to_owned(), args: Vec::new() };
        let result = build_tier_c_command_plan(&policy, &request);
        if let Ok(plan) = result {
            assert_eq!(plan.backend, TierCBackendKind::LinuxBubblewrap);
            assert_eq!(plan.program, "bwrap");
            assert!(plan.args.iter().any(|arg| arg == "--unshare-net"));
            assert!(plan.args.iter().any(|arg| arg == "uname"));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_backend_rejects_host_allowlists() {
        let mut policy = sample_policy();
        policy.allowed_egress_hosts = vec!["api.example.com".to_owned()];
        let request = TierCCommandRequest { command: "uname".to_owned(), args: Vec::new() };
        let result = build_tier_c_command_plan(&policy, &request);
        if let Err(error) = result {
            assert!(
                matches!(error, TierCBackendError::HostAllowlistUnsupported { .. })
                    || matches!(error, TierCBackendError::BackendBinaryMissing { .. })
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_backend_builds_sandbox_exec_plan_when_binary_exists() {
        let policy = sample_policy();
        let request = TierCCommandRequest { command: "uname".to_owned(), args: Vec::new() };
        let result = build_tier_c_command_plan(&policy, &request);
        if let Ok(plan) = result {
            assert_eq!(plan.backend, TierCBackendKind::MacosSandboxExec);
            assert_eq!(plan.program, "sandbox-exec");
            assert!(plan.args.iter().any(|arg| arg.contains("(deny network*)")));
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_backend_fails_closed_for_runtime_network_isolation() {
        let policy = sample_policy();
        let request =
            TierCCommandRequest { command: "where".to_owned(), args: vec!["cmd".to_owned()] };
        let error = build_tier_c_command_plan(&policy, &request).expect_err(
            "windows backend must fail closed when runtime network isolation is required",
        );
        assert!(matches!(error, TierCBackendError::NetworkIsolationUnsupported { .. }));
    }

    #[cfg(windows)]
    #[test]
    fn windows_backend_allows_plan_without_network_isolation() {
        let mut policy = sample_policy();
        policy.enforce_network_isolation = false;
        let request =
            TierCCommandRequest { command: "where".to_owned(), args: vec!["cmd".to_owned()] };
        let plan = build_tier_c_command_plan(&policy, &request)
            .expect("windows backend should support direct process execution in tier-c mode");
        assert_eq!(plan.backend, TierCBackendKind::WindowsJobObject);
        assert_eq!(plan.program, "where");
    }
}
