//! Canonical SSH and paired-desktop transport adapter contracts.
//! Adapters reduce authority to a fixed worker command and negotiated capabilities;
//! they never expose arbitrary shell or implicit computer-use access.

use std::{
    env,
    ffi::OsString,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use palyra_common::redaction::redact_auth_error_strict;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    remote_protocol::RemoteWorkerProtocolV1, WorkerRemoteToolRequestEnvelope,
    WorkerRemoteToolResultEnvelope,
};

const MAX_CAPABILITIES: usize = 128;
const MAX_STDIO_MESSAGE_BYTES: usize = 1_024 * 1_024;
const MAX_ADAPTER_TIMEOUT_MS: u64 = 120_000;
const CANONICAL_SSH_WORKER_COMMAND: &str = "palyra-workerd --stdio";
const REMOTE_WORKER_THREAT_MODEL: &str =
    include_str!("../security/remote-worker-threat-model.v1.json");
const REMOTE_WORKER_THREAT_MODEL_ID: &str = "palyra.remote-worker-threat-model";
const REMOTE_WORKER_THREAT_MODEL_APPROVAL: &str = "approved_for_opt_in";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteWorkerThreatModelEvidence {
    schema_version: u32,
    contract_id: String,
    approval: RemoteWorkerThreatModelApproval,
    rollout: RemoteWorkerThreatModelRollout,
    controls: RemoteWorkerThreatModelControls,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteWorkerThreatModelApproval {
    status: String,
    owner: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteWorkerThreatModelRollout {
    default_enabled: bool,
    silent_fallback_allowed: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteWorkerThreatModelControls {
    authenticated_transport_identity: bool,
    signed_exact_task_delivery: bool,
    capability_and_policy_binding: bool,
    content_addressed_workspace: bool,
    scoped_path_enforcement: bool,
    encrypted_ephemeral_secrets: bool,
    restart_safe_duplicate_reconciliation: bool,
    late_result_rejection: bool,
    verified_cleanup: bool,
    arbitrary_shell_denied: bool,
}

/// SSH adapter profile restricted to one canonical worker command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SshWorkerTransportProfile {
    /// Stable adapter profile identity.
    pub profile_id: String,
    /// Operator-managed SSH endpoint.
    pub endpoint: String,
    /// SHA-256 pin of the expected SSH host key.
    pub host_key_sha256: String,
    /// Vault reference to the client identity.
    pub identity_vault_ref: String,
    /// Exact remote command; must equal `palyra-workerd --stdio`.
    pub remote_command: String,
    /// Remote sandbox root selected by the operator.
    pub sandbox_root: String,
    /// Policy-approved worker capabilities.
    pub capabilities: Vec<String>,
    /// Bounded reconnect attempts.
    pub max_reconnect_attempts: u32,
    /// Profile generation invalidated on key rotation or revoke.
    pub generation: u64,
    /// Whether the adapter is revoked.
    pub revoked: bool,
}

impl SshWorkerTransportProfile {
    /// Validates host pinning, identity indirection, fixed command, and capability bounds.
    ///
    /// # Errors
    /// Returns a typed fail-closed error for arbitrary shell, missing trust, or revoke.
    pub fn validate(&self) -> Result<(), WorkerTransportAdapterError> {
        validate_remote_worker_threat_model()?;
        validate_identity(self.profile_id.as_str(), "profile_id")?;
        validate_identity(self.endpoint.as_str(), "endpoint")?;
        validate_sha256(self.host_key_sha256.as_str(), "host_key_sha256")?;
        if !self.identity_vault_ref.starts_with("vault://") {
            return Err(WorkerTransportAdapterError::IdentityNotVaultBacked);
        }
        if self.remote_command != CANONICAL_SSH_WORKER_COMMAND {
            return Err(WorkerTransportAdapterError::ArbitraryShellDenied);
        }
        if self.sandbox_root.trim().is_empty()
            || self.max_reconnect_attempts == 0
            || self.max_reconnect_attempts > 8
            || self.generation == 0
            || self.capabilities.len() > MAX_CAPABILITIES
        {
            return Err(WorkerTransportAdapterError::InvalidProfile);
        }
        if self.revoked {
            return Err(WorkerTransportAdapterError::Revoked);
        }
        Ok(())
    }

    /// Confirms that a canonical task requests only negotiated capabilities.
    ///
    /// # Errors
    /// Returns a typed error for invalid profile or capability mismatch.
    pub fn authorize(
        &self,
        protocol: &RemoteWorkerProtocolV1,
        observed_at_unix_ms: i64,
    ) -> Result<(), WorkerTransportAdapterError> {
        self.validate()?;
        protocol
            .validate(observed_at_unix_ms)
            .map_err(|error| WorkerTransportAdapterError::Protocol(error.to_string()))?;
        let capability = format!("tool:{}", protocol.task.tool_name);
        if !self.capabilities.iter().any(|allowed| allowed == &capability) {
            return Err(WorkerTransportAdapterError::CapabilityMismatch);
        }
        Ok(())
    }

    /// Builds a fixed-command SSH adapter after verifying its dedicated known-hosts pin.
    ///
    /// The identity file must already have been materialized from `identity_vault_ref`
    /// by the trusted host. Neither task input nor model output can add SSH arguments.
    ///
    /// # Errors
    /// Returns a typed error for an invalid profile, host-key mismatch, or missing
    /// operator-owned SSH material.
    pub fn stdio_adapter(
        &self,
        ssh_executable: impl Into<PathBuf>,
        known_hosts_file: &Path,
        identity_file: &Path,
        timeout_ms: u64,
    ) -> Result<CanonicalWorkerStdioAdapter, WorkerTransportAdapterError> {
        self.validate()?;
        if !known_hosts_file.is_absolute()
            || !identity_file.is_absolute()
            || !known_hosts_file.is_file()
            || !identity_file.is_file()
        {
            return Err(WorkerTransportAdapterError::TransportMaterialUnavailable);
        }
        validate_known_hosts_pin(
            self.endpoint.as_str(),
            known_hosts_file,
            self.host_key_sha256.as_str(),
        )?;
        let arguments = vec![
            OsString::from("-T"),
            OsString::from("-o"),
            OsString::from("BatchMode=yes"),
            OsString::from("-o"),
            OsString::from("StrictHostKeyChecking=yes"),
            OsString::from("-o"),
            OsString::from(format!("UserKnownHostsFile={}", known_hosts_file.display())),
            OsString::from("-o"),
            OsString::from("IdentitiesOnly=yes"),
            OsString::from("-i"),
            identity_file.as_os_str().to_owned(),
            OsString::from(self.endpoint.as_str()),
            OsString::from(CANONICAL_SSH_WORKER_COMMAND),
        ];
        CanonicalWorkerStdioAdapter::new(ssh_executable, arguments, timeout_ms)
    }
}

/// Generation-fenced binding for a paired desktop node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DesktopNodeBindingV2 {
    /// Paired device identity.
    pub device_id: String,
    /// SHA-256 of the paired device identity fingerprint.
    pub identity_fingerprint_sha256: String,
    /// Desktop platform label.
    pub platform: String,
    /// Policy-approved capability inventory.
    pub capabilities: Vec<String>,
    /// Whether each attended task requires fresh user presence.
    pub user_presence_required: bool,
    /// Most recent user-presence confirmation.
    pub user_presence_confirmed_at_unix_ms: Option<i64>,
    /// Maximum age of an attended confirmation.
    pub user_presence_ttl_ms: i64,
    /// Explicit computer-use grant, separate from filesystem/process access.
    pub computer_use_authorized: bool,
    /// Binding generation invalidated on revoke or certificate rotation.
    pub generation: u64,
    /// Binding expiry.
    pub expires_at_unix_ms: i64,
    /// Whether the paired node was revoked.
    pub revoked: bool,
}

impl DesktopNodeBindingV2 {
    /// Validates identity, expiry, requested capabilities, and attended UI authority.
    ///
    /// # Errors
    /// Returns a typed error for revoke, mismatch, stale presence, or implicit UI access.
    pub fn authorize(
        &self,
        required_capabilities: &[String],
        observed_at_unix_ms: i64,
    ) -> Result<(), WorkerTransportAdapterError> {
        validate_remote_worker_threat_model()?;
        validate_identity(self.device_id.as_str(), "device_id")?;
        validate_identity(self.platform.as_str(), "platform")?;
        validate_sha256(self.identity_fingerprint_sha256.as_str(), "identity_fingerprint_sha256")?;
        if self.revoked {
            return Err(WorkerTransportAdapterError::Revoked);
        }
        if self.generation == 0
            || self.expires_at_unix_ms <= observed_at_unix_ms
            || self.capabilities.len() > MAX_CAPABILITIES
        {
            return Err(WorkerTransportAdapterError::InvalidProfile);
        }
        if required_capabilities
            .iter()
            .any(|required| !self.capabilities.iter().any(|value| value == required))
        {
            return Err(WorkerTransportAdapterError::CapabilityMismatch);
        }
        let requests_computer_use =
            required_capabilities.iter().any(|value| value == "computer_use");
        if requests_computer_use && !self.computer_use_authorized {
            return Err(WorkerTransportAdapterError::ComputerUseNotAuthorized);
        }
        if requests_computer_use || self.user_presence_required {
            let confirmed_at = self
                .user_presence_confirmed_at_unix_ms
                .ok_or(WorkerTransportAdapterError::UserPresenceRequired)?;
            if self.user_presence_ttl_ms <= 0
                || observed_at_unix_ms.saturating_sub(confirmed_at) > self.user_presence_ttl_ms
            {
                return Err(WorkerTransportAdapterError::UserPresenceExpired);
            }
        }
        Ok(())
    }

    /// Builds the paired-desktop adapter over the same canonical stdio protocol.
    ///
    /// # Errors
    /// Returns a typed error when the binding cannot authorize the request or
    /// the configured worker executable/timeout is invalid.
    pub fn stdio_adapter(
        &self,
        workerd_executable: impl Into<PathBuf>,
        required_capabilities: &[String],
        observed_at_unix_ms: i64,
        timeout_ms: u64,
    ) -> Result<CanonicalWorkerStdioAdapter, WorkerTransportAdapterError> {
        self.authorize(required_capabilities, observed_at_unix_ms)?;
        CanonicalWorkerStdioAdapter::new(
            workerd_executable,
            vec![OsString::from("--stdio")],
            timeout_ms,
        )
    }
}

/// Fixed child-process adapter shared by SSH and paired-desktop transports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalWorkerStdioAdapter {
    executable: PathBuf,
    arguments: Vec<OsString>,
    timeout_ms: u64,
}

impl CanonicalWorkerStdioAdapter {
    /// Builds the fixed local worker subprocess adapter used by authenticated nodes.
    ///
    /// # Errors
    /// Rejects a non-absolute executable, missing regular file, or invalid timeout.
    pub fn local_workerd(
        executable: impl Into<PathBuf>,
        timeout_ms: u64,
    ) -> Result<Self, WorkerTransportAdapterError> {
        Self::new(executable, vec![OsString::from("--stdio")], timeout_ms)
    }

    fn new(
        executable: impl Into<PathBuf>,
        arguments: Vec<OsString>,
        timeout_ms: u64,
    ) -> Result<Self, WorkerTransportAdapterError> {
        validate_remote_worker_threat_model()?;
        let executable = executable.into();
        if executable.as_os_str().is_empty() || !executable.is_absolute() || !executable.is_file() {
            return Err(WorkerTransportAdapterError::TransportExecutableUnavailable);
        }
        if arguments.is_empty() || timeout_ms == 0 || timeout_ms > MAX_ADAPTER_TIMEOUT_MS {
            return Err(WorkerTransportAdapterError::InvalidProfile);
        }
        Ok(Self { executable, arguments, timeout_ms })
    }

    /// Executes one canonical request and validates the terminal response.
    ///
    /// # Errors
    /// Returns a typed unavailable, timeout, size, decode, or protocol error.
    pub fn execute(
        &self,
        request: &WorkerRemoteToolRequestEnvelope,
        observed_at_unix_ms: i64,
    ) -> Result<WorkerRemoteToolResultEnvelope, WorkerTransportAdapterError> {
        request
            .validate(observed_at_unix_ms)
            .map_err(|error| WorkerTransportAdapterError::Protocol(error.to_string()))?;
        let encoded = serde_json::to_vec(request)
            .map_err(|error| WorkerTransportAdapterError::Protocol(error.to_string()))?;
        if encoded.len() > MAX_STDIO_MESSAGE_BYTES {
            return Err(WorkerTransportAdapterError::MessageLimitExceeded);
        }

        let mut child = Command::new(self.executable.as_path())
            .args(self.arguments.as_slice())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear()
            .envs(minimal_transport_environment())
            .spawn()
            .map_err(|_| WorkerTransportAdapterError::TransportUnavailable)?;
        let stdin = child.stdin.take().ok_or(WorkerTransportAdapterError::TransportUnavailable)?;
        let stdout =
            child.stdout.take().ok_or(WorkerTransportAdapterError::TransportUnavailable)?;
        let stderr =
            child.stderr.take().ok_or(WorkerTransportAdapterError::TransportUnavailable)?;
        let stdin_writer = thread::spawn(move || {
            let mut stdin = stdin;
            stdin
                .write_all(encoded.as_slice())
                .map_err(|_| WorkerTransportAdapterError::TransportUnavailable)
        });
        let stdout_reader = thread::spawn(move || read_bounded(stdout, MAX_STDIO_MESSAGE_BYTES));
        let stderr_reader = thread::spawn(move || read_bounded(stderr, MAX_STDIO_MESSAGE_BYTES));
        let deadline = Instant::now() + Duration::from_millis(self.timeout_ms);
        let status = loop {
            match child.try_wait().map_err(|_| WorkerTransportAdapterError::TransportUnavailable)? {
                Some(status) => break status,
                None if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = stdin_writer.join();
                    let _ = stdout_reader.join();
                    let _ = stderr_reader.join();
                    return Err(WorkerTransportAdapterError::ExecutionTimeout);
                }
                None => thread::sleep(Duration::from_millis(10)),
            }
        };
        stdin_writer.join().map_err(|_| WorkerTransportAdapterError::TransportUnavailable)??;
        let stdout = stdout_reader
            .join()
            .map_err(|_| WorkerTransportAdapterError::TransportUnavailable)??;
        let stderr = stderr_reader
            .join()
            .map_err(|_| WorkerTransportAdapterError::TransportUnavailable)??;
        if !status.success() {
            let detail =
                redact_auth_error_strict(String::from_utf8_lossy(stderr.as_slice()).trim());
            return Err(WorkerTransportAdapterError::RemoteWorkerFailed {
                detail: bounded_detail(detail.as_str()),
            });
        }
        let result = serde_json::from_slice::<WorkerRemoteToolResultEnvelope>(stdout.as_slice())
            .map_err(|error| WorkerTransportAdapterError::ResponseInvalid {
                detail: bounded_detail(error.to_string().as_str()),
            })?;
        result
            .validate_against_request(request, observed_at_unix_ms)
            .map_err(|error| WorkerTransportAdapterError::Protocol(error.to_string()))?;
        Ok(result)
    }

    /// Returns the exact operator-owned process plan for diagnostics and tests.
    #[must_use]
    pub fn process_plan(&self) -> (&Path, &[OsString]) {
        (self.executable.as_path(), self.arguments.as_slice())
    }
}

/// Explicit platform availability projection for worker transports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTransportAvailability {
    /// Transport label.
    pub transport: String,
    /// Platform label.
    pub platform: String,
    /// Whether the adapter is qualified on this platform.
    pub available: bool,
    /// Stable availability or repair reason.
    pub reason_code: String,
}

/// Fail-closed SSH and desktop adapter errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkerTransportAdapterError {
    /// Required profile identity is invalid.
    #[error("worker transport field {field} is invalid")]
    InvalidIdentity { field: &'static str },
    /// Digest is not a canonical lowercase SHA-256 value.
    #[error("worker transport field {field} is not a canonical SHA-256 digest")]
    InvalidDigest { field: &'static str },
    /// SSH identity is not vault-backed.
    #[error("SSH worker identity must use a vault reference")]
    IdentityNotVaultBacked,
    /// SSH attempted to expose arbitrary shell authority.
    #[error("SSH worker arbitrary shell authority is denied")]
    ArbitraryShellDenied,
    /// The host key observed by the trusted SSH bootstrap did not match the profile.
    #[error("SSH worker host key does not match the pinned profile")]
    HostKeyMismatch,
    /// Operator-owned known-hosts or identity material was unavailable.
    #[error("SSH worker transport material is unavailable")]
    TransportMaterialUnavailable,
    /// The transport executable is not an absolute regular file.
    #[error("worker transport executable is unavailable")]
    TransportExecutableUnavailable,
    /// The reviewed security contract is missing, changed, or no longer approved.
    #[error("worker transport security contract is not approved")]
    ThreatModelUnapproved,
    /// Profile generation, reconnect, or expiry is invalid.
    #[error("worker transport profile is invalid or expired")]
    InvalidProfile,
    /// Binding was revoked.
    #[error("worker transport binding is revoked")]
    Revoked,
    /// Requested capability was not negotiated.
    #[error("worker transport capability mismatch")]
    CapabilityMismatch,
    /// Computer use lacks an explicit independent grant.
    #[error("desktop node computer use is not authorized")]
    ComputerUseNotAuthorized,
    /// Fresh user presence is absent.
    #[error("desktop node requires fresh user presence")]
    UserPresenceRequired,
    /// User-presence confirmation expired.
    #[error("desktop node user presence expired")]
    UserPresenceExpired,
    /// Canonical task validation failed.
    #[error("worker transport canonical protocol failed: {0}")]
    Protocol(String),
    /// The configured child transport could not be started or observed.
    #[error("worker transport is unavailable")]
    TransportUnavailable,
    /// The child transport did not settle within its bounded lease window.
    #[error("worker transport execution timed out")]
    ExecutionTimeout,
    /// A transport message exceeded its fixed bound.
    #[error("worker transport message exceeds its byte limit")]
    MessageLimitExceeded,
    /// The worker process exited unsuccessfully.
    #[error("worker transport remote process failed: {detail}")]
    RemoteWorkerFailed { detail: String },
    /// The terminal worker response was not a canonical result.
    #[error("worker transport response is invalid: {detail}")]
    ResponseInvalid { detail: String },
}

impl WorkerTransportAdapterError {
    /// Stable operator-facing reason code used across SSH and desktop adapters.
    #[must_use]
    pub const fn reason_code(&self) -> &'static str {
        match self {
            Self::InvalidIdentity { .. } | Self::InvalidDigest { .. } | Self::InvalidProfile => {
                "worker.transport.profile_invalid"
            }
            Self::IdentityNotVaultBacked
            | Self::TransportMaterialUnavailable
            | Self::TransportExecutableUnavailable => "worker.transport.credential_unavailable",
            Self::ThreatModelUnapproved => "worker.transport.security_contract_unapproved",
            Self::ArbitraryShellDenied => "worker.transport.arbitrary_shell_denied",
            Self::HostKeyMismatch => "worker.transport.host_key_mismatch",
            Self::Revoked => "worker.transport.revoked",
            Self::CapabilityMismatch => "worker.transport.capability_mismatch",
            Self::ComputerUseNotAuthorized => "worker.transport.computer_use_denied",
            Self::UserPresenceRequired => "worker.transport.user_presence_required",
            Self::UserPresenceExpired => "worker.transport.user_presence_expired",
            Self::Protocol(_) | Self::ResponseInvalid { .. } => {
                "worker.transport.protocol_mismatch"
            }
            Self::TransportUnavailable | Self::RemoteWorkerFailed { .. } => {
                "worker.transport.unavailable"
            }
            Self::ExecutionTimeout => "worker.transport.partition_timeout",
            Self::MessageLimitExceeded => "worker.transport.message_limit_exceeded",
        }
    }
}

fn validate_identity(value: &str, field: &'static str) -> Result<(), WorkerTransportAdapterError> {
    if value.trim().is_empty() || value.len() > 256 {
        return Err(WorkerTransportAdapterError::InvalidIdentity { field });
    }
    Ok(())
}

fn validate_sha256(value: &str, field: &'static str) -> Result<(), WorkerTransportAdapterError> {
    if value.len() != 64
        || value
            .chars()
            .any(|character| !character.is_ascii_hexdigit() || character.is_ascii_uppercase())
    {
        return Err(WorkerTransportAdapterError::InvalidDigest { field });
    }
    Ok(())
}

fn read_bounded(
    reader: impl Read,
    max_bytes: usize,
) -> Result<Vec<u8>, WorkerTransportAdapterError> {
    let mut bytes = Vec::new();
    reader
        .take(u64::try_from(max_bytes).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| WorkerTransportAdapterError::TransportUnavailable)?;
    if bytes.len() > max_bytes {
        return Err(WorkerTransportAdapterError::MessageLimitExceeded);
    }
    Ok(bytes)
}

fn bounded_detail(value: &str) -> String {
    value.chars().take(256).collect()
}

fn validate_remote_worker_threat_model() -> Result<(), WorkerTransportAdapterError> {
    let evidence =
        serde_json::from_str::<RemoteWorkerThreatModelEvidence>(REMOTE_WORKER_THREAT_MODEL)
            .map_err(|_| WorkerTransportAdapterError::ThreatModelUnapproved)?;
    let controls = evidence.controls;
    let controls_approved = controls.authenticated_transport_identity
        && controls.signed_exact_task_delivery
        && controls.capability_and_policy_binding
        && controls.content_addressed_workspace
        && controls.scoped_path_enforcement
        && controls.encrypted_ephemeral_secrets
        && controls.restart_safe_duplicate_reconciliation
        && controls.late_result_rejection
        && controls.verified_cleanup
        && controls.arbitrary_shell_denied;
    if evidence.schema_version != 1
        || evidence.contract_id != REMOTE_WORKER_THREAT_MODEL_ID
        || evidence.approval.status != REMOTE_WORKER_THREAT_MODEL_APPROVAL
        || evidence.approval.owner != "@tomasmarekk"
        || evidence.rollout.default_enabled
        || evidence.rollout.silent_fallback_allowed
        || !controls_approved
    {
        return Err(WorkerTransportAdapterError::ThreatModelUnapproved);
    }
    Ok(())
}

fn minimal_transport_environment() -> Vec<(OsString, OsString)> {
    ["SYSTEMROOT", "WINDIR", "COMSPEC", "LANG", "LC_ALL", "TZ"]
        .into_iter()
        .filter_map(|key| env::var_os(key).map(|value| (OsString::from(key), value)))
        .collect()
}

fn validate_known_hosts_pin(
    endpoint: &str,
    known_hosts_file: &Path,
    expected_sha256: &str,
) -> Result<(), WorkerTransportAdapterError> {
    let content = std::fs::read_to_string(known_hosts_file)
        .map_err(|_| WorkerTransportAdapterError::TransportMaterialUnavailable)?;
    let endpoint_host = endpoint.rsplit_once('@').map_or(endpoint, |(_, host)| host);
    let expected_host = endpoint_host
        .rsplit_once(':')
        .filter(|(_, port)| port.chars().all(|character| character.is_ascii_digit()))
        .map_or_else(|| endpoint_host.to_owned(), |(host, port)| format!("[{host}]:{port}"));
    for line in content.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let Some(hosts) = fields.next() else {
            continue;
        };
        let Some(_key_type) = fields.next() else {
            continue;
        };
        let Some(encoded_key) = fields.next() else {
            continue;
        };
        if !hosts.split(',').any(|host| host == expected_host) {
            continue;
        }
        let key = STANDARD
            .decode(encoded_key)
            .map_err(|_| WorkerTransportAdapterError::TransportMaterialUnavailable)?;
        if hex::encode(Sha256::digest(key.as_slice())) == expected_sha256 {
            return Ok(());
        }
    }
    Err(WorkerTransportAdapterError::HostKeyMismatch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        remote_protocol::RemoteWorkerProtocolV1, RuntimeGeneration, WorkerArtifactTransport,
        WorkerRemoteIdentity, WorkerRemoteLeaseBinding, WorkerRemoteToolKind,
        WorkerRemoteWorkspaceEntry, WorkerRemoteWorkspaceEntryKind, WorkerRemoteWorkspaceTransfer,
        WorkerWorkspaceScope, WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
    };

    fn ssh_profile() -> SshWorkerTransportProfile {
        SshWorkerTransportProfile {
            profile_id: "ssh-worker-1".to_owned(),
            endpoint: "worker.example:22".to_owned(),
            host_key_sha256: hex::encode(Sha256::digest(b"test host key")),
            identity_vault_ref: "vault://ssh/worker-key".to_owned(),
            remote_command: CANONICAL_SSH_WORKER_COMMAND.to_owned(),
            sandbox_root: "/srv/palyra/workspaces".to_owned(),
            capabilities: vec!["tool:palyra.fs.read_file".to_owned()],
            max_reconnect_attempts: 3,
            generation: 1,
            revoked: false,
        }
    }

    #[test]
    fn ssh_profile_denies_arbitrary_shell_and_invalid_host_key_pin() {
        let mut profile = ssh_profile();
        profile.remote_command = "bash -lc anything".to_owned();
        assert_eq!(profile.validate(), Err(WorkerTransportAdapterError::ArbitraryShellDenied));

        profile = ssh_profile();
        profile.host_key_sha256 = "not-a-pin".to_owned();
        assert_eq!(
            profile.validate(),
            Err(WorkerTransportAdapterError::InvalidDigest { field: "host_key_sha256" })
        );
    }

    #[test]
    fn adapters_require_approved_opt_in_threat_model_evidence() {
        validate_remote_worker_threat_model().expect("reviewed threat model");
        let evidence =
            serde_json::from_str::<RemoteWorkerThreatModelEvidence>(REMOTE_WORKER_THREAT_MODEL)
                .expect("threat model evidence");

        assert_eq!(evidence.approval.status, "approved_for_opt_in");
        assert!(!evidence.rollout.default_enabled);
        assert!(!evidence.rollout.silent_fallback_allowed);
    }

    #[test]
    fn ssh_adapter_pins_host_key_and_builds_only_the_fixed_worker_command() {
        let material = tempfile::tempdir().expect("SSH material");
        let known_hosts = material.path().join("known_hosts");
        let identity = material.path().join("identity");
        let ssh_executable = material.path().join(if cfg!(windows) { "ssh.exe" } else { "ssh" });
        let encoded_key = STANDARD.encode(b"test host key");
        std::fs::write(
            known_hosts.as_path(),
            format!("[worker.example]:22 ssh-ed25519 {encoded_key}\n"),
        )
        .expect("known hosts fixture");
        std::fs::write(identity.as_path(), "test identity").expect("identity fixture");
        std::fs::write(ssh_executable.as_path(), "test executable").expect("SSH fixture");
        let profile = ssh_profile();

        let mut mismatched_profile = profile.clone();
        mismatched_profile.host_key_sha256 = "b".repeat(64);
        let mismatch = mismatched_profile
            .stdio_adapter(
                ssh_executable.as_path(),
                known_hosts.as_path(),
                identity.as_path(),
                10_000,
            )
            .expect_err("mismatched host key must fail closed");
        assert_eq!(mismatch, WorkerTransportAdapterError::HostKeyMismatch);
        assert_eq!(mismatch.reason_code(), "worker.transport.host_key_mismatch");

        let adapter = profile
            .stdio_adapter(
                ssh_executable.as_path(),
                known_hosts.as_path(),
                identity.as_path(),
                10_000,
            )
            .expect("pinned SSH adapter");
        let (_, arguments) = adapter.process_plan();
        let rendered = arguments.iter().map(|value| value.to_string_lossy()).collect::<Vec<_>>();
        assert_eq!(rendered.last().map(|value| value.as_ref()), Some(CANONICAL_SSH_WORKER_COMMAND));
        assert!(rendered.iter().any(|value| value == "StrictHostKeyChecking=yes"));
        assert!(!rendered.iter().any(|value| value.contains("bash") || value.contains("sh -c")));

        assert_eq!(
            CanonicalWorkerStdioAdapter::new(
                "relative-workerd",
                vec![OsString::from("--stdio")],
                10_000
            ),
            Err(WorkerTransportAdapterError::TransportExecutableUnavailable)
        );
    }

    #[test]
    fn desktop_binding_requires_separate_computer_use_and_fresh_presence() {
        let mut binding = DesktopNodeBindingV2 {
            device_id: "desktop-1".to_owned(),
            identity_fingerprint_sha256: "b".repeat(64),
            platform: "windows".to_owned(),
            capabilities: vec!["computer_use".to_owned()],
            user_presence_required: true,
            user_presence_confirmed_at_unix_ms: Some(10_000),
            user_presence_ttl_ms: 5_000,
            computer_use_authorized: false,
            generation: 1,
            expires_at_unix_ms: 100_000,
            revoked: false,
        };
        assert_eq!(
            binding.authorize(&["computer_use".to_owned()], 12_000),
            Err(WorkerTransportAdapterError::ComputerUseNotAuthorized)
        );
        binding.computer_use_authorized = true;
        assert!(binding.authorize(&["computer_use".to_owned()], 12_000).is_ok());
        assert_eq!(
            binding.authorize(&["computer_use".to_owned()], 20_000),
            Err(WorkerTransportAdapterError::UserPresenceExpired)
        );

        binding.revoked = true;
        assert_eq!(
            binding.authorize(&["computer_use".to_owned()], 12_000),
            Err(WorkerTransportAdapterError::Revoked)
        );
    }

    #[test]
    fn fake_transport_partition_rejects_late_output() {
        let executable = std::env::current_exe().expect("current test executable");
        let adapter = CanonicalWorkerStdioAdapter::new(
            executable,
            vec![
                OsString::from("--exact"),
                OsString::from("transport_adapters::tests::fake_transport_child_emits_late_output"),
                OsString::from("--nocapture"),
            ],
            40,
        )
        .expect("fake transport adapter");

        let error = adapter
            .execute(&adapter_request(), 60_000)
            .expect_err("late output must not settle after the partition deadline");

        assert_eq!(error, WorkerTransportAdapterError::ExecutionTimeout);
        assert_eq!(error.reason_code(), "worker.transport.partition_timeout");
    }

    #[test]
    fn fake_transport_stderr_is_strictly_redacted() {
        let executable = std::env::current_exe().expect("current test executable");
        let adapter = CanonicalWorkerStdioAdapter::new(
            executable,
            vec![
                OsString::from("--exact"),
                OsString::from(
                    "transport_adapters::tests::fake_transport_child_writes_secret_stderr",
                ),
                OsString::from("--nocapture"),
            ],
            5_000,
        )
        .expect("fake transport adapter");

        let error = adapter.execute(&adapter_request(), 60_000).expect_err("fake child must fail");
        let WorkerTransportAdapterError::RemoteWorkerFailed { detail } = error else {
            panic!("expected bounded child failure");
        };
        assert!(!detail.contains("PALYRA_TEST_TRANSPORT_SECRET"));
        assert!(detail.contains("<redacted>"));
    }

    #[test]
    fn fake_transport_child_emits_late_output() {
        if std::env::args().any(|argument| argument == "--exact") {
            thread::sleep(Duration::from_millis(250));
            println!("late transport output must be ignored");
        }
    }

    #[test]
    fn fake_transport_child_writes_secret_stderr() {
        if std::env::args().any(|argument| argument == "--exact") {
            eprintln!("authorization=PALYRA_TEST_TRANSPORT_SECRET");
            panic!("intentional fake transport failure");
        }
    }

    fn adapter_request() -> WorkerRemoteToolRequestEnvelope {
        let input_json = r#"{"path":"note.txt"}"#.to_owned();
        let entry = WorkerRemoteWorkspaceEntry {
            path: "note.txt".to_owned(),
            kind: WorkerRemoteWorkspaceEntryKind::File,
            sha256: hex::encode(Sha256::digest(b"hello")),
            bytes: b"hello".to_vec(),
        };
        let mut request = WorkerRemoteToolRequestEnvelope {
            protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
            schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
            request_id: "request-adapter-1".to_owned(),
            proposal_id: "proposal-adapter-1".to_owned(),
            tool_name: "palyra.fs.read_file".to_owned(),
            tool_kind: WorkerRemoteToolKind::FsRead,
            input_json_sha256: hex::encode(Sha256::digest(input_json.as_bytes())),
            input_json,
            lease: WorkerRemoteLeaseBinding {
                lease_id: "lease-adapter-1".to_owned(),
                worker_id: "worker-adapter-1".to_owned(),
                session_id: "session-adapter-1".to_owned(),
                run_id: "run-adapter-1".to_owned(),
                run_generation: RuntimeGeneration::new(1).expect("generation"),
                grant_id: "grant-adapter-1".to_owned(),
                grant_tool_name: "palyra.fs.read_file".to_owned(),
                expires_at_unix_ms: 120_000,
                required_capabilities: vec!["tool:palyra.fs.read_file".to_owned()],
                work_graph_claim: None,
                work_graph_posture: Default::default(),
                workspace_scope: WorkerWorkspaceScope {
                    workspace_root: "/workspace".to_owned(),
                    allowed_paths: vec!["note.txt".to_owned()],
                    read_only: true,
                },
                artifact_transport: WorkerArtifactTransport {
                    input_manifest_sha256: "1".repeat(64),
                    output_manifest_sha256: "2".repeat(64),
                    log_stream_id: "logs-adapter-1".to_owned(),
                    scratch_directory_id: "scratch-adapter-1".to_owned(),
                },
            },
            worker_identity: WorkerRemoteIdentity {
                worker_id: "worker-adapter-1".to_owned(),
                image_digest_sha256: "3".repeat(64),
                build_digest_sha256: "4".repeat(64),
                artifact_digest_sha256: "5".repeat(64),
                capability_authority_sha256: Some("6".repeat(64)),
                sdk_protocol_version: 1,
                wit_abi_version: "palyra-worker-abi/v1".to_owned(),
            },
            workspace_transfer: WorkerRemoteWorkspaceTransfer::scoped("7".repeat(64), vec![entry])
                .expect("scoped transfer"),
            encrypted_secret_artifact: None,
            canonical_protocol: None,
        };
        request.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&request));
        request
    }
}
