//! Tier C process-sandbox backend planners for Linux, macOS, and Windows.
//!
//! Each compile-time-selected backend turns a [`TierCPolicy`] plus a command request into a
//! launchable [`TierCCommandPlan`] (e.g. a `bwrap` or `sandbox-exec` argv) without spawning the
//! sandboxed process itself; `palyra-daemon`'s sandbox runner executes the plan. Backends that
//! cannot enforce the requested isolation fail closed instead of degrading silently.

use std::path::PathBuf;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::process::{Command, Stdio};

use thiserror::Error;

/// Platform-specific Tier C sandbox backend selected at compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierCBackendKind {
    /// Linux backend isolating via Bubblewrap (`bwrap`) namespaces.
    LinuxBubblewrap,
    /// macOS backend isolating via `sandbox-exec` SBPL profiles.
    MacosSandboxExec,
    /// Windows Job Object backend; currently fails closed because filesystem and token
    /// isolation are not yet OS-enforced.
    WindowsJobObject,
    /// Targets without any Tier C backend implementation.
    Unsupported,
}

impl TierCBackendKind {
    /// Returns the stable backend identifier used in error messages and configuration.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LinuxBubblewrap => "linux_bubblewrap",
            Self::MacosSandboxExec => "macos_sandbox_exec",
            Self::WindowsJobObject => "windows_job_object",
            Self::Unsupported => "unsupported",
        }
    }

    /// Returns the stable executor label recorded in tool attestations.
    ///
    /// These labels are part of the attestation contract; renaming one invalidates
    /// previously recorded attestations.
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

/// Isolation policy for a single Tier C sandboxed command execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierCPolicy {
    /// Directory tree the sandboxed process may read and write.
    pub workspace_root: PathBuf,
    /// Working directory for the sandboxed process; expected to live under `workspace_root`.
    pub cwd: PathBuf,
    /// When `true`, the backend must deny all network access at the OS level.
    pub enforce_network_isolation: bool,
    /// Host-level egress allowlist; no current backend can enforce it at the OS level, so
    /// planning fails closed with [`TierCBackendError::HostAllowlistUnsupported`] when non-empty.
    pub allowed_egress_hosts: Vec<String>,
    /// DNS-suffix egress allowlist; same enforcement status as `allowed_egress_hosts`.
    pub allowed_dns_suffixes: Vec<String>,
}

/// Command and arguments to run inside the Tier C sandbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierCCommandRequest {
    /// Program to execute inside the sandbox.
    pub command: String,
    /// Arguments passed to `command` unchanged.
    pub args: Vec<String>,
}

/// Fully resolved sandbox invocation: spawn `program` with `args`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierCCommandPlan {
    /// Backend that produced this plan.
    pub backend: TierCBackendKind,
    /// Sandbox wrapper binary to spawn (e.g. `bwrap`, `sandbox-exec`).
    pub program: String,
    /// Complete argument list for `program`, including the wrapped command.
    pub args: Vec<String>,
}

/// Isolation features a Tier C backend can enforce at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierCBackendCapabilities {
    /// Backend can deny all network access at the OS level.
    pub runtime_network_isolation: bool,
    /// Backend can enforce per-host egress allowlists at the OS level.
    pub host_allowlists: bool,
}

/// Failures while planning a Tier C sandboxed command; every variant means the command
/// must not run, because no backend degrades to weaker isolation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TierCBackendError {
    /// The backend cannot run on this host or is deliberately disabled.
    #[error("tier-c backend '{backend}' is unavailable: {reason}")]
    BackendUnavailable { backend: &'static str, reason: String },
    /// The helper binary the backend wraps could not be spawned from `PATH`.
    #[error("tier-c backend '{backend}' requires binary '{binary}' in PATH")]
    BackendBinaryMissing { backend: &'static str, binary: String },
    /// The policy contains host/DNS egress allowlists, which no current backend can
    /// enforce at the OS level.
    #[error(
        "tier-c backend '{backend}' cannot enforce host-level egress allowlists; use preflight mode or clear allowlists"
    )]
    HostAllowlistUnsupported { backend: &'static str },
    /// Reserved for backends that cannot satisfy [`TierCPolicy::enforce_network_isolation`];
    /// no in-crate backend emits it today (such platforms fail earlier with
    /// [`TierCBackendError::BackendUnavailable`]), but `palyra-daemon` maps it.
    #[error(
        "tier-c backend '{backend}' cannot enforce runtime network isolation on this platform"
    )]
    NetworkIsolationUnsupported { backend: &'static str },
}

/// Planner for one platform's Tier C sandbox mechanism.
///
/// Implementations only plan the sandboxed invocation; apart from probing for required
/// helper binaries they never spawn processes and never mutate host state.
pub trait TierCBackend {
    /// Identifies which platform backend this is.
    fn kind(&self) -> TierCBackendKind;

    /// Reports which isolation features this backend can enforce at runtime.
    fn capabilities(&self) -> TierCBackendCapabilities;

    /// Builds a launchable command plan for `request` under `policy`.
    ///
    /// # Errors
    ///
    /// Returns a [`TierCBackendError`] when the backend is unavailable on this host, a
    /// required helper binary is missing, or `policy` requests isolation the backend
    /// cannot enforce; planning fails closed rather than weakening the sandbox.
    fn build_command_plan(
        &self,
        policy: &TierCPolicy,
        request: &TierCCommandRequest,
    ) -> Result<TierCCommandPlan, TierCBackendError>;
}

#[cfg(target_os = "linux")]
mod platform {
    //! Linux Tier C backend planning `bwrap` invocations with namespace isolation.

    use std::path::{Component, Path, PathBuf};

    use super::{
        ensure_binary_available, TierCBackend, TierCBackendCapabilities, TierCBackendError,
        TierCBackendKind, TierCCommandPlan, TierCCommandRequest, TierCPolicy,
    };

    pub(super) static BACKEND: LinuxBubblewrapBackend = LinuxBubblewrapBackend;

    #[derive(Debug, Clone, Copy)]
    pub(super) struct LinuxBubblewrapBackend;

    impl LinuxBubblewrapBackend {
        /// Renders a plan after the production caller has verified that `bwrap` can be spawned.
        ///
        /// Separating rendering from the host probe lets unit tests pin the argv contract
        /// without depending on the runner's installed packages.
        pub(super) fn build_command_plan_after_binary_check(
            &self,
            policy: &TierCPolicy,
            request: &TierCCommandRequest,
        ) -> Result<TierCCommandPlan, TierCBackendError> {
            // bwrap can only unshare the network namespace entirely; it cannot filter
            // egress per host, so non-empty allowlists must fail closed.
            if !policy.allowed_egress_hosts.is_empty() || !policy.allowed_dns_suffixes.is_empty() {
                return Err(TierCBackendError::HostAllowlistUnsupported {
                    backend: self.kind().as_str(),
                });
            }

            let workspace = policy.workspace_root.to_string_lossy().into_owned();
            let cwd = policy.cwd.to_string_lossy().into_owned();
            let mut args = vec![
                "--die-with-parent".to_owned(),
                // Namespace isolation does not detach the sandbox from its controlling terminal.
                // A new session prevents terminal ioctls from reaching the operator's session.
                "--new-session".to_owned(),
                "--unshare-pid".to_owned(),
                "--proc".to_owned(),
                "/proc".to_owned(),
                "--dev".to_owned(),
                "/dev".to_owned(),
                "--tmpfs".to_owned(),
                "/tmp".to_owned(),
                "--tmpfs".to_owned(),
                "/var/tmp".to_owned(),
                "--dir".to_owned(),
                "/etc".to_owned(),
            ];
            for runtime_dir in ["/usr", "/bin", "/sbin", "/lib", "/lib64"] {
                append_ro_bind_if_exists(&mut args, runtime_dir, runtime_dir);
            }
            for runtime_file in [
                "/etc/hosts",
                "/etc/resolv.conf",
                "/etc/nsswitch.conf",
                "/etc/passwd",
                "/etc/group",
                "/etc/ld.so.cache",
            ] {
                append_ro_bind_if_exists(&mut args, runtime_file, runtime_file);
            }
            append_workspace_path_scaffold(&mut args, policy.workspace_root.as_path());
            args.extend([
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
                "--setenv".to_owned(),
                "NODE_DISABLE_COMPILE_CACHE".to_owned(),
                "1".to_owned(),
            ]);
            if policy.enforce_network_isolation {
                args.push("--unshare-net".to_owned());
            }
            args.push("--".to_owned());
            args.push(request.command.clone());
            args.extend(request.args.iter().cloned());
            Ok(TierCCommandPlan { backend: self.kind(), program: "bwrap".to_owned(), args })
        }
    }

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
            self.build_command_plan_after_binary_check(policy, request)
        }
    }

    /// Binds `source` read-only only when it exists on the host; bwrap aborts on missing
    /// bind sources, and paths like `/lib64` are distribution-specific.
    fn append_ro_bind_if_exists(args: &mut Vec<String>, source: &str, destination: &str) {
        if !Path::new(source).exists() {
            return;
        }
        args.extend(["--ro-bind".to_owned(), source.to_owned(), destination.to_owned()]);
    }

    /// Emits `--dir` entries for each ancestor of the workspace root so the later
    /// `--bind` has a mount point inside the otherwise-empty sandbox filesystem.
    fn append_workspace_path_scaffold(args: &mut Vec<String>, workspace_root: &Path) {
        let mut current = PathBuf::from("/");
        for component in workspace_root.components() {
            if matches!(component, Component::RootDir) {
                continue;
            }
            current.push(component.as_os_str());
            // The workspace root itself is created by `--bind`; only ancestors need `--dir`.
            if current == workspace_root {
                break;
            }
            args.extend(["--dir".to_owned(), current.to_string_lossy().into_owned()]);
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    //! macOS Tier C backend planning `sandbox-exec` invocations with SBPL profiles.

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
            // SBPL network rules are all-or-nothing here; per-host allowlists cannot be
            // enforced, so they must fail closed.
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

    /// Renders the deny-by-default SBPL profile: read access to the OS runtime, read/write
    /// limited to the workspace and temp directories, network per policy.
    fn render_sandbox_profile(policy: &TierCPolicy) -> String {
        // Escape backslashes before quotes so a hostile workspace path cannot terminate
        // the SBPL string literal and inject profile rules.
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
(allow file-read*
    (subpath "{workspace}")
    (subpath "/bin")
    (subpath "/sbin")
    (subpath "/usr")
    (subpath "/System")
    (subpath "/Library")
    (subpath "/private/var/db/dyld")
    (subpath "/tmp")
    (subpath "/private/tmp"))
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
    //! Windows Tier C backend; deliberately fails closed until OS-enforced isolation lands.

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
            _policy: &TierCPolicy,
            _request: &TierCCommandRequest,
        ) -> Result<TierCCommandPlan, TierCBackendError> {
            Err(TierCBackendError::BackendUnavailable {
                backend: self.kind().as_str(),
                reason: "tier-c backend is disabled on windows until filesystem and token isolation are OS-enforced".to_owned(),
            })
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
mod platform {
    //! Fallback Tier C backend for targets without a sandbox implementation; fails closed.

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

/// Returns the Tier C backend kind compiled for this target.
#[must_use]
pub fn current_backend_kind() -> TierCBackendKind {
    backend().kind()
}

/// Returns the stable executor label of the compiled backend, for tool attestations.
#[must_use]
pub fn current_backend_executor() -> &'static str {
    current_backend_kind().executor_label()
}

/// Returns the runtime isolation capabilities of the compiled backend.
#[must_use]
pub fn current_backend_capabilities() -> TierCBackendCapabilities {
    backend().capabilities()
}

/// Builds a launchable sandbox command plan for `request` under `policy` using the
/// backend compiled for this target.
///
/// # Errors
///
/// Returns a [`TierCBackendError`] when the backend is unavailable, its helper binary is
/// missing, or `policy` requests isolation the backend cannot enforce (fail closed); see
/// [`TierCBackend::build_command_plan`].
pub fn build_tier_c_command_plan(
    policy: &TierCPolicy,
    request: &TierCCommandRequest,
) -> Result<TierCCommandPlan, TierCBackendError> {
    backend().build_command_plan(policy, request)
}

/// Probes availability by spawning `binary --help`; only a spawn failure (typically
/// not-found) counts as missing — a non-zero exit still proves the binary is runnable.
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
    #[cfg(target_os = "linux")]
    use super::platform::LinuxBubblewrapBackend;
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
        if matches!(kind, TierCBackendKind::WindowsJobObject | TierCBackendKind::Unsupported) {
            assert!(
                !capabilities.runtime_network_isolation,
                "unsupported tier-c backends must report missing runtime network isolation"
            );
            assert!(
                !capabilities.host_allowlists,
                "unsupported tier-c backends must report missing runtime host allowlist support"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_backend_starts_a_new_terminal_session() {
        let backend = LinuxBubblewrapBackend;
        let policy = sample_policy();
        let request = TierCCommandRequest { command: "uname".to_owned(), args: Vec::new() };
        let plan = backend
            .build_command_plan_after_binary_check(&policy, &request)
            .expect("Linux argv rendering should not depend on bwrap being installed");

        assert!(
            plan.args.iter().any(|arg| arg == "--new-session"),
            "linux tier-c commands must detach from the operator's controlling terminal"
        );
        assert!(
            plan.args.windows(3).any(|window| window[0] == "--setenv"
                && window[1] == "NODE_DISABLE_COMPILE_CACHE"
                && window[2] == "1"),
            "linux tier-c commands must disable the Node compile cache inside bwrap"
        );
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
            assert!(
                !plan
                    .args
                    .windows(3)
                    .any(|window| window[0] == "--ro-bind" && window[1] == "/" && window[2] == "/"),
                "linux tier-c command plan must not expose host root with '--ro-bind / /'"
            );
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
            let profile = plan
                .args
                .iter()
                .find(|argument| argument.contains("(version 1)"))
                .expect("sandbox-exec profile argument should be present");
            assert!(profile.contains("(deny network*)"));
            assert!(
                !profile.contains("(allow file-read*)"),
                "macOS tier-c profile must not grant global read permissions"
            );
            assert!(
                profile.contains("(subpath \"/usr\")"),
                "macOS tier-c profile should allow minimal runtime reads from /usr"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_backend_is_explicitly_unavailable() {
        let policy = sample_policy();
        let request =
            TierCCommandRequest { command: "where".to_owned(), args: vec!["cmd".to_owned()] };
        let error = build_tier_c_command_plan(&policy, &request).expect_err(
            "windows tier-c backend should fail closed until OS-enforced sandboxing is implemented",
        );
        assert!(matches!(error, TierCBackendError::BackendUnavailable { .. }));
    }
}
