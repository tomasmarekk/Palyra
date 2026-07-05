//! Persistent terminal-session state contracts used by process backends.
//!
//! This module intentionally models session ownership, environment posture,
//! safety guards, tail redaction, and cleanup evidence without spawning a PTY.
//! Concrete execution backends can plug into this state machine without
//! duplicating guard behavior.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

const DEFAULT_TAIL_LIMIT_BYTES: usize = 16 * 1024;

/// Backend family that owns a persistent terminal session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalBackendKind {
    Host,
    Docker,
    SshTunnel,
}

impl TerminalBackendKind {
    /// Returns the stable JSON label used in status payloads.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Docker => "docker",
            Self::SshTunnel => "ssh_tunnel",
        }
    }
}

/// Lifecycle state for a persistent terminal session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalSessionState {
    Active,
    Closed,
    Stale,
}

impl TerminalSessionState {
    /// Returns the stable JSON label used in status payloads.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Closed => "closed",
            Self::Stale => "stale",
        }
    }
}

/// Result status for a model-facing terminal command plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalCommandStatus {
    Planned,
    Denied,
}

impl TerminalCommandStatus {
    /// Returns the stable JSON label used in command plans.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Denied => "denied",
        }
    }
}

/// Request used to create a persistent terminal-session record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSessionCreateRequest {
    pub session_id: String,
    pub owner_run_id: String,
    pub backend: TerminalBackendKind,
    pub cwd: String,
    pub env_profile_id: Option<String>,
    pub pty_requested: bool,
    pub pty_active: bool,
    pub now_unix_ms: u64,
}

/// Disk guard bounds applied before planning a terminal command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalDiskGuard {
    pub available_bytes: u64,
    pub minimum_free_bytes: u64,
}

/// Audit-safe disk guard decision for a terminal command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalDiskGuardDecision {
    pub allowed: bool,
    pub available_bytes: u64,
    pub minimum_free_bytes: u64,
    pub reason_code: String,
}

/// Request used to plan one command inside an existing terminal session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalCommandRequest {
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub env_profile_id: Option<String>,
    pub explicit_env: BTreeMap<String, String>,
    pub vault_env_refs: BTreeMap<String, String>,
    pub elevated_intent: bool,
    pub disk_guard: Option<TerminalDiskGuard>,
    pub now_unix_ms: u64,
}

/// Redacted, bounded environment posture exposed to models and logs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalEnvReport {
    pub profile_id: Option<String>,
    pub explicit_env_keys: Vec<String>,
    pub vault_ref_keys: Vec<String>,
    pub vault_ref_count: usize,
    pub daemon_env_inherited: bool,
}

/// Cleanup evidence associated with terminal session lifecycle transitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalCleanupEvidence {
    pub state: TerminalSessionState,
    pub reason_code: String,
    pub owner_run_id: String,
    pub last_activity_unix_ms: u64,
    pub closed_at_unix_ms: Option<u64>,
}

/// Audit-safe command plan returned before an execution backend runs work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalCommandPlan {
    pub status: TerminalCommandStatus,
    pub reason_code: String,
    pub session_id: String,
    pub backend: String,
    pub cwd: String,
    pub env: TerminalEnvReport,
    pub pty_requested: bool,
    pub pty_active: bool,
    pub requires_approval: bool,
    pub elevated_intent_detected: bool,
    pub disk_guard: Option<TerminalDiskGuardDecision>,
    pub cleanup: TerminalCleanupEvidence,
}

/// Snapshot returned by terminal status and list operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalSessionStatus {
    pub session_id: String,
    pub owner_run_id: String,
    pub backend: String,
    pub cwd: String,
    pub env: TerminalEnvReport,
    pub pty_requested: bool,
    pub pty_active: bool,
    pub state: TerminalSessionState,
    pub last_activity_unix_ms: u64,
    pub cleanup: TerminalCleanupEvidence,
}

/// Persistent terminal session record with bounded redacted tail state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSession {
    session_id: String,
    owner_run_id: String,
    backend: TerminalBackendKind,
    cwd: String,
    env_profile_id: Option<String>,
    pty_requested: bool,
    pty_active: bool,
    state: TerminalSessionState,
    last_activity_unix_ms: u64,
    closed_at_unix_ms: Option<u64>,
    tail_limit_bytes: usize,
    tail: String,
}

impl TerminalSession {
    /// Creates a terminal session after validating ownership and cwd inputs.
    ///
    /// Returns [`TerminalSessionError`] when identifiers or cwd are empty.
    pub fn create(request: TerminalSessionCreateRequest) -> Result<Self, TerminalSessionError> {
        let session_id = normalize_non_empty(request.session_id, "session_id")?;
        let owner_run_id = normalize_non_empty(request.owner_run_id, "owner_run_id")?;
        let cwd = normalize_non_empty(request.cwd, "cwd")?;
        Ok(Self {
            session_id,
            owner_run_id,
            backend: request.backend,
            cwd,
            env_profile_id: request.env_profile_id.filter(|value| !value.trim().is_empty()),
            pty_requested: request.pty_requested,
            pty_active: request.pty_active,
            state: TerminalSessionState::Active,
            last_activity_unix_ms: request.now_unix_ms,
            closed_at_unix_ms: None,
            tail_limit_bytes: DEFAULT_TAIL_LIMIT_BYTES,
            tail: String::new(),
        })
    }

    /// Plans one command for the owning backend and updates activity metadata.
    ///
    /// Denied plans are returned as [`TerminalCommandStatus::Denied`] so callers
    /// can emit synthetic denial output without bypassing the normal tool path.
    pub fn plan_command(
        &mut self,
        request: TerminalCommandRequest,
    ) -> Result<TerminalCommandPlan, TerminalSessionError> {
        self.ensure_active()?;
        validate_terminal_env_keys(&request.explicit_env)?;
        validate_terminal_env_keys(&request.vault_env_refs)?;
        if let Some(cwd) = request.cwd.as_ref().filter(|value| !value.trim().is_empty()) {
            self.cwd = cwd.trim().to_owned();
        }
        if let Some(profile_id) =
            request.env_profile_id.as_ref().filter(|value| !value.trim().is_empty())
        {
            self.env_profile_id = Some(profile_id.trim().to_owned());
        }
        self.last_activity_unix_ms = request.now_unix_ms;

        let elevated =
            command_has_elevated_intent(request.command.as_str(), request.args.as_slice());
        let disk_guard = request.disk_guard.map(disk_guard_decision);
        let denied_reason = if elevated && !request.elevated_intent {
            Some("terminal.elevated_intent.approval_required")
        } else if disk_guard.as_ref().is_some_and(|decision| !decision.allowed) {
            Some("terminal.disk_guard.denied")
        } else {
            None
        };
        let status = if denied_reason.is_some() {
            TerminalCommandStatus::Denied
        } else {
            TerminalCommandStatus::Planned
        };

        Ok(TerminalCommandPlan {
            status,
            reason_code: denied_reason.unwrap_or("terminal.command.planned").to_owned(),
            session_id: self.session_id.clone(),
            backend: self.backend.as_str().to_owned(),
            cwd: self.cwd.clone(),
            env: env_report(
                self.env_profile_id.clone(),
                request.explicit_env.keys(),
                request.vault_env_refs.keys(),
            ),
            pty_requested: self.pty_requested,
            pty_active: self.pty_active,
            requires_approval: elevated,
            elevated_intent_detected: elevated,
            disk_guard,
            cleanup: self.cleanup_evidence("terminal.cleanup.not_required"),
        })
    }

    /// Appends stdout/stderr text to the bounded redacted session tail.
    pub fn append_tail(
        &mut self,
        output: &str,
        now_unix_ms: u64,
    ) -> Result<(), TerminalSessionError> {
        self.ensure_active()?;
        self.tail.push_str(redact_terminal_output(output).as_str());
        truncate_tail_to_limit(&mut self.tail, self.tail_limit_bytes);
        self.last_activity_unix_ms = now_unix_ms;
        Ok(())
    }

    /// Returns the redacted bounded tail retained for this session.
    #[must_use]
    pub fn tail(&self) -> &str {
        self.tail.as_str()
    }

    /// Returns an audit-safe status snapshot for this session.
    #[must_use]
    pub fn status(&self) -> TerminalSessionStatus {
        TerminalSessionStatus {
            session_id: self.session_id.clone(),
            owner_run_id: self.owner_run_id.clone(),
            backend: self.backend.as_str().to_owned(),
            cwd: self.cwd.clone(),
            env: env_report(
                self.env_profile_id.clone(),
                std::iter::empty::<&String>(),
                std::iter::empty::<&String>(),
            ),
            pty_requested: self.pty_requested,
            pty_active: self.pty_active,
            state: self.state,
            last_activity_unix_ms: self.last_activity_unix_ms,
            cleanup: self.cleanup_evidence("terminal.cleanup.snapshot"),
        }
    }

    /// Closes the session and returns cleanup evidence for logs/support bundles.
    pub fn close(&mut self, now_unix_ms: u64) -> TerminalCleanupEvidence {
        self.state = TerminalSessionState::Closed;
        self.closed_at_unix_ms = Some(now_unix_ms);
        self.last_activity_unix_ms = now_unix_ms;
        self.cleanup_evidence("terminal.cleanup.closed")
    }

    /// Marks an idle active session stale and returns cleanup evidence.
    pub fn mark_stale_if_idle(
        &mut self,
        now_unix_ms: u64,
        max_idle_ms: u64,
    ) -> Option<TerminalCleanupEvidence> {
        if self.state != TerminalSessionState::Active {
            return None;
        }
        if now_unix_ms.saturating_sub(self.last_activity_unix_ms) < max_idle_ms {
            return None;
        }
        self.state = TerminalSessionState::Stale;
        Some(self.cleanup_evidence("terminal.cleanup.stale_idle"))
    }

    fn ensure_active(&self) -> Result<(), TerminalSessionError> {
        if self.state == TerminalSessionState::Active {
            return Ok(());
        }
        Err(TerminalSessionError::Closed { session_id: self.session_id.clone(), state: self.state })
    }

    fn cleanup_evidence(&self, reason_code: &str) -> TerminalCleanupEvidence {
        TerminalCleanupEvidence {
            state: self.state,
            reason_code: reason_code.to_owned(),
            owner_run_id: self.owner_run_id.clone(),
            last_activity_unix_ms: self.last_activity_unix_ms,
            closed_at_unix_ms: self.closed_at_unix_ms,
        }
    }
}

/// Terminal session validation and lifecycle errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalSessionError {
    InvalidInput { field: &'static str, message: String },
    Closed { session_id: String, state: TerminalSessionState },
}

impl std::fmt::Display for TerminalSessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInput { field, message } => write!(formatter, "{field}: {message}"),
            Self::Closed { session_id, state } => {
                write!(formatter, "terminal session {session_id} is {}", state.as_str())
            }
        }
    }
}

impl std::error::Error for TerminalSessionError {}

fn normalize_non_empty(value: String, field: &'static str) -> Result<String, TerminalSessionError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(TerminalSessionError::InvalidInput {
            field,
            message: "must be non-empty".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

fn validate_terminal_env_keys(env: &BTreeMap<String, String>) -> Result<(), TerminalSessionError> {
    for key in env.keys() {
        let normalized = key.trim();
        if normalized.is_empty() || normalized.contains('=') || normalized.contains('\0') {
            return Err(TerminalSessionError::InvalidInput {
                field: "env",
                message: "environment keys must be non-empty names without '=' or NUL".to_owned(),
            });
        }
        let upper = normalized.to_ascii_uppercase();
        if matches!(
            upper.as_str(),
            "PATH" | "HOME" | "USERPROFILE" | "HTTPS_PROXY" | "HTTP_PROXY" | "NO_PROXY"
        ) || upper.starts_with("PALYRA_ADMIN_")
            || upper.starts_with("PALYRA_CONFIG")
            || upper.starts_with("PALYRA_VAULT_")
        {
            return Err(TerminalSessionError::InvalidInput {
                field: "env",
                message: format!("environment key {normalized} is reserved by the runtime"),
            });
        }
    }
    Ok(())
}

fn env_report<'a>(
    profile_id: Option<String>,
    explicit_env_keys: impl Iterator<Item = &'a String>,
    vault_ref_keys: impl Iterator<Item = &'a String>,
) -> TerminalEnvReport {
    let explicit_env_keys = explicit_env_keys.cloned().collect::<Vec<_>>();
    let vault_ref_keys = vault_ref_keys.cloned().collect::<Vec<_>>();
    TerminalEnvReport {
        profile_id,
        vault_ref_count: vault_ref_keys.len(),
        explicit_env_keys,
        vault_ref_keys,
        daemon_env_inherited: false,
    }
}

fn disk_guard_decision(guard: TerminalDiskGuard) -> TerminalDiskGuardDecision {
    let allowed = guard.available_bytes >= guard.minimum_free_bytes;
    TerminalDiskGuardDecision {
        allowed,
        available_bytes: guard.available_bytes,
        minimum_free_bytes: guard.minimum_free_bytes,
        reason_code: if allowed {
            "terminal.disk_guard.allowed"
        } else {
            "terminal.disk_guard.denied"
        }
        .to_owned(),
    }
}

fn command_has_elevated_intent(command: &str, args: &[String]) -> bool {
    let command_name =
        command.rsplit(['/', '\\']).next().unwrap_or(command).trim().to_ascii_lowercase();
    matches!(command_name.as_str(), "sudo" | "su" | "doas" | "runas" | "pkexec")
        || args.iter().any(|arg| {
            let normalized = arg.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "sudo" | "su" | "doas" | "runas" | "pkexec")
                || normalized.starts_with("sudo ")
                || normalized.starts_with("doas ")
        })
}

fn redact_terminal_output(output: &str) -> String {
    output.split('\n').map(redact_terminal_line).collect::<Vec<_>>().join("\n")
}

fn redact_terminal_line(line: &str) -> String {
    line.split(' ')
        .map(|segment| {
            let lower = segment.to_ascii_lowercase();
            for marker in ["password=", "token=", "secret=", "api_key=", "apikey="] {
                if lower.starts_with(marker) {
                    let key = segment.split_once('=').map(|(key, _)| key).unwrap_or(marker);
                    return format!("{key}=[REDACTED]");
                }
            }
            segment.to_owned()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn truncate_tail_to_limit(tail: &mut String, limit_bytes: usize) {
    if tail.len() <= limit_bytes {
        return;
    }
    let mut keep_from = tail.len().saturating_sub(limit_bytes);
    while !tail.is_char_boundary(keep_from) {
        keep_from += 1;
    }
    tail.replace_range(..keep_from, "");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_session() -> TerminalSession {
        TerminalSession::create(TerminalSessionCreateRequest {
            session_id: "term_01".to_owned(),
            owner_run_id: "run_01".to_owned(),
            backend: TerminalBackendKind::Host,
            cwd: "/workspace".to_owned(),
            env_profile_id: Some("dev".to_owned()),
            pty_requested: true,
            pty_active: false,
            now_unix_ms: 100,
        })
        .expect("session should be created")
    }

    #[test]
    fn command_plan_tracks_env_profile_without_inheriting_daemon_env() {
        let mut session = create_session();
        let request = TerminalCommandRequest {
            command: "npm".to_owned(),
            args: vec!["run".to_owned(), "dev".to_owned()],
            cwd: Some("/workspace/web".to_owned()),
            env_profile_id: Some("web-dev".to_owned()),
            explicit_env: BTreeMap::from([("APP_MODE".to_owned(), "test".to_owned())]),
            vault_env_refs: BTreeMap::from([(
                "NPM_TOKEN".to_owned(),
                "vault://profile/npm-token".to_owned(),
            )]),
            elevated_intent: false,
            disk_guard: Some(TerminalDiskGuard {
                available_bytes: 4_000_000,
                minimum_free_bytes: 1_000_000,
            }),
            now_unix_ms: 200,
        };

        let plan = session.plan_command(request).expect("command should plan");

        assert_eq!(plan.status, TerminalCommandStatus::Planned);
        assert_eq!(plan.cwd, "/workspace/web");
        assert_eq!(plan.env.profile_id.as_deref(), Some("web-dev"));
        assert_eq!(plan.env.explicit_env_keys, vec!["APP_MODE"]);
        assert_eq!(plan.env.vault_ref_keys, vec!["NPM_TOKEN"]);
        assert!(!plan.env.daemon_env_inherited);
        assert_eq!(plan.disk_guard.as_ref().map(|decision| decision.allowed), Some(true));
    }

    #[test]
    fn elevated_command_without_intent_returns_denied_plan() {
        let mut session = create_session();
        let request = TerminalCommandRequest {
            command: "sudo".to_owned(),
            args: vec!["id".to_owned()],
            cwd: None,
            env_profile_id: None,
            explicit_env: BTreeMap::new(),
            vault_env_refs: BTreeMap::new(),
            elevated_intent: false,
            disk_guard: None,
            now_unix_ms: 200,
        };

        let plan = session.plan_command(request).expect("denial should be modeled as a plan");

        assert_eq!(plan.status, TerminalCommandStatus::Denied);
        assert_eq!(plan.reason_code, "terminal.elevated_intent.approval_required");
        assert!(plan.requires_approval);
        assert!(plan.elevated_intent_detected);
    }

    #[test]
    fn disk_guard_denies_low_free_space() {
        let mut session = create_session();
        let request = TerminalCommandRequest {
            command: "npm".to_owned(),
            args: vec!["install".to_owned()],
            cwd: None,
            env_profile_id: None,
            explicit_env: BTreeMap::new(),
            vault_env_refs: BTreeMap::new(),
            elevated_intent: false,
            disk_guard: Some(TerminalDiskGuard { available_bytes: 100, minimum_free_bytes: 1_000 }),
            now_unix_ms: 200,
        };

        let plan = session.plan_command(request).expect("disk denial should be a plan");

        assert_eq!(plan.status, TerminalCommandStatus::Denied);
        assert_eq!(plan.reason_code, "terminal.disk_guard.denied");
        assert_eq!(
            plan.disk_guard.as_ref().map(|decision| decision.reason_code.as_str()),
            Some("terminal.disk_guard.denied")
        );
    }

    #[test]
    fn tail_is_redacted_and_bounded() {
        let mut session = create_session();
        session.tail_limit_bytes = 32;

        session
            .append_tail("before token=abc123\nsecret=value after", 200)
            .expect("tail should append");

        assert!(!session.tail().contains("abc123"));
        assert!(!session.tail().contains("value"));
        assert!(session.tail().len() <= 32);
    }

    #[test]
    fn stale_and_close_return_cleanup_evidence() {
        let mut session = create_session();

        let stale =
            session.mark_stale_if_idle(2_000, 1_000).expect("idle session should become stale");

        assert_eq!(stale.state, TerminalSessionState::Stale);
        assert_eq!(stale.reason_code, "terminal.cleanup.stale_idle");
        let close = session.close(2_100);
        assert_eq!(close.state, TerminalSessionState::Closed);
        assert_eq!(close.closed_at_unix_ms, Some(2_100));
    }
}
