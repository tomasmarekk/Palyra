//! Canonical SSH and paired-desktop transport adapter contracts.
//! Adapters reduce authority to a fixed worker command and negotiated capabilities;
//! they never expose arbitrary shell or implicit computer-use access.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::remote_protocol::RemoteWorkerProtocolV1;

const MAX_CAPABILITIES: usize = 128;
const CANONICAL_SSH_WORKER_COMMAND: &str = "palyra-workerd --stdio";

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

#[cfg(test)]
mod tests {
    use super::*;

    fn ssh_profile() -> SshWorkerTransportProfile {
        SshWorkerTransportProfile {
            profile_id: "ssh-worker-1".to_owned(),
            endpoint: "worker.example:22".to_owned(),
            host_key_sha256: "a".repeat(64),
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
    }
}
