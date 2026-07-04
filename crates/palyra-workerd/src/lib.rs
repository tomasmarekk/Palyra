//! Fail-closed worker fleet contracts: attestation, leases, lifecycle, and cleanup.
//!
//! [`WorkerFleetManager`] is the in-memory ledger the daemon drives: workers must
//! present a valid [`WorkerAttestation`] to register, every lease is bounded by
//! [`WorkerFleetPolicy`], and quarantined or orphaned workers stay unassignable
//! until they re-register with a fresh attestation.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Canonical worker lifecycle states, re-exported from `palyra-common` runtime contracts.
pub use palyra_common::runtime_contracts::WorkerLifecycleState;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

const MAX_WORKER_ID_BYTES: usize = 128;
const MAX_GRANT_ID_BYTES: usize = 128;
const MAX_RECENT_LIFECYCLE_EVENTS: usize = 64;
const DEFAULT_WORKER_SDK_PROTOCOL_VERSION: u32 = 1;
const DEFAULT_WORKER_WIT_ABI_VERSION: &str = "palyra-worker-abi/v1";

fn default_worker_sdk_protocol_version() -> u32 {
    DEFAULT_WORKER_SDK_PROTOCOL_VERSION
}

fn default_worker_wit_abi_version() -> String {
    DEFAULT_WORKER_WIT_ABI_VERSION.to_owned()
}

/// Identity, integrity, and compatibility claims a worker presents to join the fleet.
///
/// Digest and version fields are checked against [`WorkerAttestationExpectation`] and
/// [`WorkerFleetPolicy`] at registration and revalidated on heartbeat and assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerAttestation {
    /// Fleet-unique worker identifier; must be non-blank and at most 128 bytes.
    pub worker_id: String,
    pub image_digest_sha256: String,
    pub build_digest_sha256: String,
    pub artifact_digest_sha256: String,
    /// Whether the worker booted bound to an attested egress proxy (required by default).
    pub egress_proxy_attested: bool,
    /// Capabilities the worker self-reports; granting one additionally requires policy trust.
    #[serde(default)]
    pub supported_capabilities: Vec<String>,
    /// Digest of the capability authority the worker was provisioned with, when reported.
    #[serde(default)]
    pub capability_authority_sha256: Option<String>,
    /// Worker SDK protocol version; payloads that omit it default to version 1.
    #[serde(default = "default_worker_sdk_protocol_version")]
    pub sdk_protocol_version: u32,
    /// Worker WIT ABI identifier; payloads that omit it default to `palyra-worker-abi/v1`.
    #[serde(default = "default_worker_wit_abi_version")]
    pub wit_abi_version: String,
    /// Last worker-reported heartbeat in unix ms; `0` (the serde default) means unreported.
    #[serde(default)]
    pub heartbeat_unix_ms: i64,
    /// Start of the attestation validity window in unix ms.
    pub issued_at_unix_ms: i64,
    /// End of the attestation validity window in unix ms; expired once `now >= expires_at`.
    pub expires_at_unix_ms: i64,
}

/// Verifier-side requirements a [`WorkerAttestation`] must satisfy.
///
/// `None` digest fields leave that digest unpinned. The default expectation requires
/// an attested egress proxy, keeping the fail-closed baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerAttestationExpectation {
    pub require_egress_proxy: bool,
    pub image_digest_sha256: Option<String>,
    pub build_digest_sha256: Option<String>,
    pub artifact_digest_sha256: Option<String>,
}

impl Default for WorkerAttestationExpectation {
    fn default() -> Self {
        Self {
            require_egress_proxy: true,
            image_digest_sha256: None,
            build_digest_sha256: None,
            artifact_digest_sha256: None,
        }
    }
}

/// Reasons a [`WorkerAttestation`] fails validation.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkerAttestationError {
    #[error("worker attestation missing worker identifier")]
    MissingWorkerId,
    #[error("worker attestation is expired")]
    Expired,
    #[error("worker attestation is not yet valid")]
    NotYetValid,
    #[error("worker attestation does not include an attested egress proxy binding")]
    MissingEgressProxyBinding,
    #[error("worker attestation {field} digest did not match the expected value")]
    DigestMismatch { field: &'static str },
}

impl WorkerAttestation {
    /// Validates this attestation against `expected` as of `now_unix_ms`.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerAttestationError::MissingWorkerId`] for a blank or oversized
    /// worker id, [`WorkerAttestationError::NotYetValid`] or
    /// [`WorkerAttestationError::Expired`] outside the validity window,
    /// [`WorkerAttestationError::MissingEgressProxyBinding`] when the expectation
    /// requires an attested egress proxy, and
    /// [`WorkerAttestationError::DigestMismatch`] when a pinned digest differs.
    pub fn validate(
        &self,
        expected: &WorkerAttestationExpectation,
        now_unix_ms: i64,
    ) -> Result<(), WorkerAttestationError> {
        if self.worker_id.trim().is_empty() || self.worker_id.len() > MAX_WORKER_ID_BYTES {
            return Err(WorkerAttestationError::MissingWorkerId);
        }
        if self.issued_at_unix_ms > now_unix_ms {
            return Err(WorkerAttestationError::NotYetValid);
        }
        if self.expires_at_unix_ms <= now_unix_ms {
            return Err(WorkerAttestationError::Expired);
        }
        if expected.require_egress_proxy && !self.egress_proxy_attested {
            return Err(WorkerAttestationError::MissingEgressProxyBinding);
        }
        if expected
            .image_digest_sha256
            .as_deref()
            .is_some_and(|expected_digest| expected_digest != self.image_digest_sha256)
        {
            return Err(WorkerAttestationError::DigestMismatch { field: "image" });
        }
        if expected
            .build_digest_sha256
            .as_deref()
            .is_some_and(|expected_digest| expected_digest != self.build_digest_sha256)
        {
            return Err(WorkerAttestationError::DigestMismatch { field: "build" });
        }
        if expected
            .artifact_digest_sha256
            .as_deref()
            .is_some_and(|expected_digest| expected_digest != self.artifact_digest_sha256)
        {
            return Err(WorkerAttestationError::DigestMismatch { field: "artifact" });
        }
        Ok(())
    }
}

/// Filesystem scope a leased worker may operate in during a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerWorkspaceScope {
    pub workspace_root: String,
    #[serde(default)]
    pub allowed_paths: Vec<String>,
    pub read_only: bool,
}

/// Digest-pinned artifact manifests and stream identifiers for a single run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerArtifactTransport {
    pub input_manifest_sha256: String,
    pub output_manifest_sha256: String,
    pub log_stream_id: String,
    pub scratch_directory_id: String,
}

/// Approval grant authorizing a single tool run.
///
/// At assignment the grant must be unexpired and its `run_id` must match the lease
/// request's `run_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRunGrant {
    pub grant_id: String,
    pub run_id: String,
    pub tool_name: String,
    pub expires_at_unix_ms: i64,
}

/// Parameters for leasing a worker to execute one run.
///
/// `ttl_ms` must be positive and at most [`WorkerFleetPolicy::max_ttl_ms`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLeaseRequest {
    pub run_id: String,
    pub ttl_ms: u64,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    pub workspace_scope: WorkerWorkspaceScope,
    pub artifact_transport: WorkerArtifactTransport,
    pub grant: WorkerRunGrant,
}

/// An active lease binding one run to one attested worker.
///
/// `lease_id` is a generated ULID and `expires_at_unix_ms` is the assignment time
/// plus the requested ttl.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLease {
    pub lease_id: String,
    pub worker_id: String,
    pub run_id: String,
    pub expires_at_unix_ms: i64,
    pub required_capabilities: Vec<String>,
    pub workspace_scope: WorkerWorkspaceScope,
    pub artifact_transport: WorkerArtifactTransport,
    pub grant: WorkerRunGrant,
}

/// Post-run cleanup results reported for a worker.
///
/// Cleanup counts as verified only when all three removals succeeded and
/// `failure_reason` is `None`; anything else drives the worker fail-closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCleanupReport {
    pub removed_workspace_scope: bool,
    pub removed_artifacts: bool,
    pub removed_logs: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

impl WorkerCleanupReport {
    /// Returns whether the worker reported complete removal of all run-scoped state.
    #[must_use]
    pub fn is_verified(&self) -> bool {
        self.removed_workspace_scope
            && self.removed_artifacts
            && self.removed_logs
            && self.failure_reason.is_none()
    }
}

/// Wire protocol identifier for remote worker tool execution envelopes.
pub const WORKER_REMOTE_TOOL_PROTOCOL: &str = "palyra-worker-rpc/v1";
/// Schema version for [`WorkerRemoteToolRequestEnvelope`] and
/// [`WorkerRemoteToolResultEnvelope`].
pub const WORKER_REMOTE_TOOL_SCHEMA_VERSION: u32 = 1;

/// Capability strings trusted for the initial networked-worker remote tool subset.
pub const WORKER_REMOTE_TOOL_CAPABILITIES: &[&str] = &[
    "tool:palyra.fs.read_file",
    "tool:palyra.fs.list_dir",
    "tool:palyra.fs.search",
    "tool:palyra.process.run",
    "tool:palyra.fs.apply_patch",
    "tool:palyra.artifact.read",
    "tool:palyra.tool_program.run",
];

/// Tool families a networked worker may execute through the remote RPC envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRemoteToolKind {
    FsRead,
    FsList,
    FsSearch,
    ProcessRun,
    ApplyPatch,
    ArtifactRead,
    ToolProgramRun,
}

impl WorkerRemoteToolKind {
    /// Stable lowercase label used in worker RPC envelopes and attestations.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FsRead => "fs_read",
            Self::FsList => "fs_list",
            Self::FsSearch => "fs_search",
            Self::ProcessRun => "process_run",
            Self::ApplyPatch => "apply_patch",
            Self::ArtifactRead => "artifact_read",
            Self::ToolProgramRun => "tool_program_run",
        }
    }

    /// Resolves the canonical Palyra tool name to a remote worker tool kind.
    #[must_use]
    pub fn from_tool_name(tool_name: &str) -> Option<Self> {
        match tool_name.trim().to_ascii_lowercase().as_str() {
            "palyra.fs.read_file" => Some(Self::FsRead),
            "palyra.fs.list_dir" => Some(Self::FsList),
            "palyra.fs.search" => Some(Self::FsSearch),
            "palyra.process.run" => Some(Self::ProcessRun),
            "palyra.fs.apply_patch" => Some(Self::ApplyPatch),
            "palyra.artifact.read" => Some(Self::ArtifactRead),
            "palyra.tool_program.run" => Some(Self::ToolProgramRun),
            _ => None,
        }
    }

    /// Canonical Palyra tool name represented by this kind.
    #[must_use]
    pub fn tool_name(self) -> &'static str {
        match self {
            Self::FsRead => "palyra.fs.read_file",
            Self::FsList => "palyra.fs.list_dir",
            Self::FsSearch => "palyra.fs.search",
            Self::ProcessRun => "palyra.process.run",
            Self::ApplyPatch => "palyra.fs.apply_patch",
            Self::ArtifactRead => "palyra.artifact.read",
            Self::ToolProgramRun => "palyra.tool_program.run",
        }
    }

    /// Capability string that must be present on the worker lease.
    #[must_use]
    pub fn required_capability(self) -> String {
        format!("tool:{}", self.tool_name())
    }
}

/// Immutable worker identity copied into remote request and result envelopes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRemoteIdentity {
    pub worker_id: String,
    pub image_digest_sha256: String,
    pub build_digest_sha256: String,
    pub artifact_digest_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability_authority_sha256: Option<String>,
    pub sdk_protocol_version: u32,
    pub wit_abi_version: String,
}

impl From<&WorkerAttestation> for WorkerRemoteIdentity {
    fn from(attestation: &WorkerAttestation) -> Self {
        Self {
            worker_id: attestation.worker_id.clone(),
            image_digest_sha256: attestation.image_digest_sha256.clone(),
            build_digest_sha256: attestation.build_digest_sha256.clone(),
            artifact_digest_sha256: attestation.artifact_digest_sha256.clone(),
            capability_authority_sha256: attestation.capability_authority_sha256.clone(),
            sdk_protocol_version: attestation.sdk_protocol_version,
            wit_abi_version: attestation.wit_abi_version.clone(),
        }
    }
}

/// Workspace transfer mode used to prepare a remote worker run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRemoteWorkspaceTransferMode {
    Manifest,
    ScopedBundle,
}

impl WorkerRemoteWorkspaceTransferMode {
    /// Stable lowercase label used in worker RPC envelopes.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
            Self::ScopedBundle => "scoped_bundle",
        }
    }
}

/// Integrity metadata for workspace material made visible to a remote worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRemoteWorkspaceTransfer {
    pub mode: WorkerRemoteWorkspaceTransferMode,
    pub workspace_manifest_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scoped_bundle_sha256: Option<String>,
    pub writeback_mode: String,
}

impl WorkerRemoteWorkspaceTransfer {
    /// Builds a manifest-only transfer descriptor for read-only remote runs.
    #[must_use]
    pub fn manifest(workspace_manifest_sha256: String) -> Self {
        Self {
            mode: WorkerRemoteWorkspaceTransferMode::Manifest,
            workspace_manifest_sha256,
            scoped_bundle_sha256: None,
            writeback_mode: "patch_bundle".to_owned(),
        }
    }
}

/// Lease fields copied into the worker RPC request so the worker can verify its grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRemoteLeaseBinding {
    pub lease_id: String,
    pub worker_id: String,
    pub run_id: String,
    pub grant_id: String,
    pub grant_tool_name: String,
    pub expires_at_unix_ms: i64,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    pub workspace_scope: WorkerWorkspaceScope,
    pub artifact_transport: WorkerArtifactTransport,
}

impl From<&WorkerLease> for WorkerRemoteLeaseBinding {
    fn from(lease: &WorkerLease) -> Self {
        Self {
            lease_id: lease.lease_id.clone(),
            worker_id: lease.worker_id.clone(),
            run_id: lease.run_id.clone(),
            grant_id: lease.grant.grant_id.clone(),
            grant_tool_name: lease.grant.tool_name.clone(),
            expires_at_unix_ms: lease.expires_at_unix_ms,
            required_capabilities: lease.required_capabilities.clone(),
            workspace_scope: lease.workspace_scope.clone(),
            artifact_transport: lease.artifact_transport.clone(),
        }
    }
}

/// Request envelope sent to a leased networked worker for one tool execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRemoteToolRequestEnvelope {
    pub protocol: String,
    pub schema_version: u32,
    pub request_id: String,
    pub proposal_id: String,
    pub tool_name: String,
    pub tool_kind: WorkerRemoteToolKind,
    pub input_json: String,
    pub input_json_sha256: String,
    pub lease: WorkerRemoteLeaseBinding,
    pub worker_identity: WorkerRemoteIdentity,
    pub workspace_transfer: WorkerRemoteWorkspaceTransfer,
}

impl WorkerRemoteToolRequestEnvelope {
    /// Validates protocol, tool-kind, manifest, identity, and lease invariants.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerRemoteToolContractError`] when the envelope is malformed,
    /// expired, unsupported, or not bound to the requested worker lease.
    pub fn validate(&self, now_unix_ms: i64) -> Result<(), WorkerRemoteToolContractError> {
        validate_protocol(self.protocol.as_str(), self.schema_version)?;
        validate_required_string(self.request_id.as_str(), "request_id")?;
        validate_required_string(self.proposal_id.as_str(), "proposal_id")?;
        validate_required_string(self.tool_name.as_str(), "tool_name")?;
        validate_required_string(self.input_json.as_str(), "input_json")?;
        validate_sha256_hex(self.input_json_sha256.as_str(), "input_json_sha256")?;
        validate_sha256_hex(
            self.workspace_transfer.workspace_manifest_sha256.as_str(),
            "workspace_manifest_sha256",
        )?;
        if matches!(self.workspace_transfer.mode, WorkerRemoteWorkspaceTransferMode::ScopedBundle)
            && self.workspace_transfer.scoped_bundle_sha256.is_none()
        {
            return Err(WorkerRemoteToolContractError::MissingScopedBundleDigest);
        }
        if let Some(digest) = self.workspace_transfer.scoped_bundle_sha256.as_deref() {
            validate_sha256_hex(digest, "scoped_bundle_sha256")?;
        }
        let expected_kind = WorkerRemoteToolKind::from_tool_name(self.tool_name.as_str())
            .ok_or_else(|| WorkerRemoteToolContractError::UnsupportedTool {
                tool_name: self.tool_name.clone(),
            })?;
        if expected_kind != self.tool_kind {
            return Err(WorkerRemoteToolContractError::ToolKindMismatch {
                tool_name: self.tool_name.clone(),
                expected: expected_kind.as_str(),
                actual: self.tool_kind.as_str(),
            });
        }
        if self.lease.worker_id != self.worker_identity.worker_id {
            return Err(WorkerRemoteToolContractError::WorkerIdentityMismatch {
                expected: self.lease.worker_id.clone(),
                actual: self.worker_identity.worker_id.clone(),
            });
        }
        if self.lease.grant_tool_name != self.tool_name {
            return Err(WorkerRemoteToolContractError::LeaseToolMismatch {
                expected: self.tool_name.clone(),
                actual: self.lease.grant_tool_name.clone(),
            });
        }
        let required_capability = self.tool_kind.required_capability();
        if !self
            .lease
            .required_capabilities
            .iter()
            .any(|capability| capability == required_capability.as_str())
        {
            return Err(WorkerRemoteToolContractError::MissingRequiredCapability {
                capability: required_capability,
            });
        }
        if self.lease.expires_at_unix_ms <= now_unix_ms {
            return Err(WorkerRemoteToolContractError::LeaseExpired {
                lease_id: self.lease.lease_id.clone(),
                expires_at_unix_ms: self.lease.expires_at_unix_ms,
                observed_at_unix_ms: now_unix_ms,
            });
        }
        Ok(())
    }
}

/// Response envelope returned by a remote worker after executing one tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRemoteToolResultEnvelope {
    pub protocol: String,
    pub schema_version: u32,
    pub request_id: String,
    pub proposal_id: String,
    pub tool_name: String,
    pub tool_kind: WorkerRemoteToolKind,
    pub worker_id: String,
    pub lease_id: String,
    pub success: bool,
    pub output_json: String,
    pub output_json_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub output_manifest_sha256: String,
    pub cleanup_report: WorkerCleanupReport,
    pub worker_identity: WorkerRemoteIdentity,
    pub completed_at_unix_ms: i64,
}

impl WorkerRemoteToolResultEnvelope {
    /// Validates a remote result against its original request envelope.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerRemoteToolContractError`] when the worker changed identity,
    /// the lease expired, cleanup was incomplete, or result fields do not match
    /// the request binding.
    pub fn validate_against_request(
        &self,
        request: &WorkerRemoteToolRequestEnvelope,
        now_unix_ms: i64,
    ) -> Result<(), WorkerRemoteToolContractError> {
        request.validate(now_unix_ms)?;
        validate_protocol(self.protocol.as_str(), self.schema_version)?;
        validate_sha256_hex(self.output_json_sha256.as_str(), "output_json_sha256")?;
        validate_sha256_hex(self.output_manifest_sha256.as_str(), "output_manifest_sha256")?;
        if self.request_id != request.request_id
            || self.proposal_id != request.proposal_id
            || self.tool_name != request.tool_name
            || self.tool_kind != request.tool_kind
            || self.lease_id != request.lease.lease_id
        {
            return Err(WorkerRemoteToolContractError::ResultBindingMismatch);
        }
        if self.worker_id != request.lease.worker_id
            || self.worker_identity != request.worker_identity
        {
            return Err(WorkerRemoteToolContractError::WorkerIdentityMismatch {
                expected: request.worker_identity.worker_id.clone(),
                actual: self.worker_identity.worker_id.clone(),
            });
        }
        if self.completed_at_unix_ms > request.lease.expires_at_unix_ms {
            return Err(WorkerRemoteToolContractError::LeaseExpired {
                lease_id: request.lease.lease_id.clone(),
                expires_at_unix_ms: request.lease.expires_at_unix_ms,
                observed_at_unix_ms: self.completed_at_unix_ms,
            });
        }
        if !self.cleanup_report.is_verified() {
            return Err(WorkerRemoteToolContractError::CleanupGap {
                lease_id: request.lease.lease_id.clone(),
                reason: self
                    .cleanup_report
                    .failure_reason
                    .clone()
                    .unwrap_or_else(|| "incomplete_cleanup_report".to_owned()),
            });
        }
        Ok(())
    }
}

/// Validation failures for networked worker RPC envelopes.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkerRemoteToolContractError {
    #[error("unsupported remote worker tool '{tool_name}'")]
    UnsupportedTool { tool_name: String },
    #[error("worker remote tool envelope uses unsupported protocol or schema")]
    UnsupportedProtocol,
    #[error("worker remote tool envelope missing required field '{field}'")]
    MissingRequiredField { field: &'static str },
    #[error("worker remote tool envelope has invalid SHA-256 digest in '{field}'")]
    InvalidSha256Digest { field: &'static str },
    #[error("worker remote scoped bundle transfer requires scoped_bundle_sha256")]
    MissingScopedBundleDigest,
    #[error("worker remote tool kind mismatch for {tool_name}: expected {expected}, got {actual}")]
    ToolKindMismatch { tool_name: String, expected: &'static str, actual: &'static str },
    #[error("worker remote lease tool mismatch: expected {expected}, got {actual}")]
    LeaseToolMismatch { expected: String, actual: String },
    #[error("worker remote lease missing capability '{capability}'")]
    MissingRequiredCapability { capability: String },
    #[error("worker remote lease '{lease_id}' expired at {expires_at_unix_ms}; observed at {observed_at_unix_ms}")]
    LeaseExpired { lease_id: String, expires_at_unix_ms: i64, observed_at_unix_ms: i64 },
    #[error("worker remote identity mismatch: expected {expected}, got {actual}")]
    WorkerIdentityMismatch { expected: String, actual: String },
    #[error("worker remote result does not match the request binding")]
    ResultBindingMismatch,
    #[error("worker remote cleanup gap for lease '{lease_id}': {reason}")]
    CleanupGap { lease_id: String, reason: String },
}

fn validate_protocol(
    protocol: &str,
    schema_version: u32,
) -> Result<(), WorkerRemoteToolContractError> {
    if protocol == WORKER_REMOTE_TOOL_PROTOCOL
        && schema_version == WORKER_REMOTE_TOOL_SCHEMA_VERSION
    {
        Ok(())
    } else {
        Err(WorkerRemoteToolContractError::UnsupportedProtocol)
    }
}

fn validate_required_string(
    value: &str,
    field: &'static str,
) -> Result<(), WorkerRemoteToolContractError> {
    if value.trim().is_empty() {
        Err(WorkerRemoteToolContractError::MissingRequiredField { field })
    } else {
        Ok(())
    }
}

fn validate_sha256_hex(
    value: &str,
    field: &'static str,
) -> Result<(), WorkerRemoteToolContractError> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(WorkerRemoteToolContractError::InvalidSha256Digest { field })
    }
}

/// Audit record of a single worker lifecycle transition.
///
/// `reason_code` is a stable machine-readable code (for example `worker.registered`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLifecycleEvent {
    pub worker_id: String,
    pub state: WorkerLifecycleState,
    pub run_id: Option<String>,
    pub reason_code: String,
    pub timestamp_unix_ms: i64,
}

/// Outcome of finalizing a run: the lifecycle event plus the cleanup verification verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCleanupOutcome {
    pub event: WorkerLifecycleEvent,
    pub cleanup_report: WorkerCleanupReport,
    pub cleanup_succeeded: bool,
}

/// Point-in-time aggregate counts over the fleet.
///
/// Counts are independent filters and may overlap: a failed worker is counted in both
/// `degraded_workers` and `failed_closed_workers`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WorkerFleetSnapshot {
    pub registered_workers: usize,
    pub attested_workers: usize,
    pub active_leases: usize,
    pub available_workers: usize,
    pub busy_workers: usize,
    pub degraded_workers: usize,
    pub draining_workers: usize,
    pub offline_workers: usize,
    pub orphaned_workers: usize,
    pub failed_closed_workers: usize,
}

/// Operator policy bounding leases and gating attestation, compatibility, and trust.
///
/// The `required_*` fields disable their check when `None`. Capability trust is
/// matched ASCII case-insensitively.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerFleetPolicy {
    pub max_ttl_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub trusted_capabilities: Vec<String>,
    pub required_capability_authority_sha256: Option<String>,
    pub required_sdk_protocol_version: Option<u32>,
    pub required_wit_abi_version: Option<String>,
    pub attestation: WorkerAttestationExpectation,
}

impl Default for WorkerFleetPolicy {
    fn default() -> Self {
        Self {
            max_ttl_ms: 15 * 60 * 1_000,
            heartbeat_timeout_ms: 30_000,
            trusted_capabilities: ["tool:palyra.echo", "tool:palyra.sleep"]
                .into_iter()
                .chain(WORKER_REMOTE_TOOL_CAPABILITIES.iter().copied())
                .map(str::to_owned)
                .collect(),
            required_capability_authority_sha256: None,
            required_sdk_protocol_version: Some(DEFAULT_WORKER_SDK_PROTOCOL_VERSION),
            required_wit_abi_version: Some(DEFAULT_WORKER_WIT_ABI_VERSION.to_owned()),
            attestation: WorkerAttestationExpectation::default(),
        }
    }
}

/// Trust state for an observed worker or paired-node endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustedEndpointTrustState {
    Unknown,
    PendingApproval,
    Trusted,
    Rejected,
    Revoked,
}

/// Transport used by an observed worker or paired-node endpoint.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustedEndpointTransport {
    Quic,
    Grpc,
    Http,
    Local,
    LanDiscoveryPreview,
    TailscalePreview,
}

/// Last health observation for a trusted endpoint identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedEndpointHealth {
    pub healthy: bool,
    pub checked_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

/// Persistable registry record for one worker or paired-node endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedEndpointRecord {
    pub endpoint_id: String,
    pub trust_state: TrustedEndpointTrustState,
    pub last_seen_unix_ms: i64,
    pub capabilities: Vec<String>,
    pub transport: TrustedEndpointTransport,
    pub identity_digest_sha256: String,
    #[serde(default)]
    pub policy_bindings: Vec<String>,
    pub health: TrustedEndpointHealth,
}

/// Policy controlling endpoint trust and preview discovery features.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedEndpointPolicy {
    pub trusted_capabilities: Vec<String>,
    pub allow_trust_on_first_use_without_approval: bool,
    pub lan_discovery_enabled: bool,
    pub tailscale_profile_enabled: bool,
}

impl Default for TrustedEndpointPolicy {
    fn default() -> Self {
        Self {
            trusted_capabilities: WORKER_REMOTE_TOOL_CAPABILITIES
                .iter()
                .copied()
                .map(str::to_owned)
                .collect(),
            allow_trust_on_first_use_without_approval: false,
            lan_discovery_enabled: false,
            tailscale_profile_enabled: false,
        }
    }
}

/// Machine-readable capability negotiation result for backend selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedEndpointCapabilityNegotiation {
    pub endpoint_id: String,
    pub usable: bool,
    pub trust_state: TrustedEndpointTrustState,
    pub healthy_identity: bool,
    pub granted_capabilities: Vec<String>,
    pub denied_capabilities: Vec<String>,
    pub decision_reason: String,
}

/// Errors returned by [`TrustedEndpointRegistry`] validation and lookup.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TrustedEndpointError {
    #[error("trusted endpoint id must not be empty")]
    MissingEndpointId,
    #[error("trusted endpoint identity digest must not be empty")]
    MissingIdentityDigest,
    #[error("trusted endpoint '{0}' is not registered")]
    UnknownEndpoint(String),
    #[error("trusted endpoint '{0}' requires explicit approval before use")]
    TrustRequired(String),
    #[error("trusted endpoint '{0}' identity health is not usable")]
    UnhealthyIdentity(String),
}

/// In-memory trusted endpoint registry; callers persist [`TrustedEndpointRecord`] values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedEndpointRegistry {
    endpoints: BTreeMap<String, TrustedEndpointRecord>,
}

impl TrustedEndpointRegistry {
    /// Inserts or refreshes an observed endpoint without granting trust.
    ///
    /// Unknown endpoints remain [`TrustedEndpointTrustState::PendingApproval`]
    /// unless policy explicitly allows TOFU without approval.
    ///
    /// # Errors
    /// Returns [`TrustedEndpointError`] for blank endpoint ids or identity digests.
    pub fn observe_endpoint(
        &mut self,
        mut record: TrustedEndpointRecord,
        policy: &TrustedEndpointPolicy,
    ) -> Result<TrustedEndpointRecord, TrustedEndpointError> {
        validate_trusted_endpoint_record(&record)?;
        if !policy.allow_trust_on_first_use_without_approval
            && matches!(record.trust_state, TrustedEndpointTrustState::Unknown)
        {
            record.trust_state = TrustedEndpointTrustState::PendingApproval;
        }
        record.capabilities.sort_by_key(|capability| capability.to_ascii_lowercase());
        record.capabilities.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
        self.endpoints.insert(record.endpoint_id.clone(), record.clone());
        Ok(record)
    }

    /// Marks an observed endpoint trusted after an explicit operator/admin decision.
    ///
    /// # Errors
    /// Returns [`TrustedEndpointError::UnknownEndpoint`] when the endpoint was not observed first.
    pub fn approve_endpoint(
        &mut self,
        endpoint_id: &str,
        policy_bindings: Vec<String>,
        now_unix_ms: i64,
    ) -> Result<TrustedEndpointRecord, TrustedEndpointError> {
        let endpoint = self
            .endpoints
            .get_mut(endpoint_id)
            .ok_or_else(|| TrustedEndpointError::UnknownEndpoint(endpoint_id.to_owned()))?;
        endpoint.trust_state = TrustedEndpointTrustState::Trusted;
        endpoint.policy_bindings = policy_bindings;
        endpoint.last_seen_unix_ms = now_unix_ms;
        Ok(endpoint.clone())
    }

    /// Negotiates a requested capability set for backend selection.
    ///
    /// Unknown, untrusted, unhealthy, preview-disabled, or policy-denied endpoints
    /// return `usable=false`; callers must not select them for execution.
    ///
    /// # Errors
    /// Returns [`TrustedEndpointError::UnknownEndpoint`] when `endpoint_id` is not registered.
    pub fn negotiate_capabilities(
        &self,
        endpoint_id: &str,
        requested_capabilities: &[String],
        policy: &TrustedEndpointPolicy,
    ) -> Result<TrustedEndpointCapabilityNegotiation, TrustedEndpointError> {
        let endpoint = self
            .endpoints
            .get(endpoint_id)
            .ok_or_else(|| TrustedEndpointError::UnknownEndpoint(endpoint_id.to_owned()))?;
        let preview_gate = match endpoint.transport {
            TrustedEndpointTransport::LanDiscoveryPreview => policy.lan_discovery_enabled,
            TrustedEndpointTransport::TailscalePreview => policy.tailscale_profile_enabled,
            _ => true,
        };
        if !preview_gate {
            return Ok(endpoint_negotiation(
                endpoint,
                false,
                Vec::new(),
                requested_capabilities.to_vec(),
                "preview_transport_disabled",
            ));
        }
        if !matches!(endpoint.trust_state, TrustedEndpointTrustState::Trusted) {
            return Ok(endpoint_negotiation(
                endpoint,
                false,
                Vec::new(),
                requested_capabilities.to_vec(),
                "trust_required",
            ));
        }
        if !endpoint.health.healthy {
            return Ok(endpoint_negotiation(
                endpoint,
                false,
                Vec::new(),
                requested_capabilities.to_vec(),
                "unhealthy_identity",
            ));
        }

        let endpoint_capabilities = endpoint
            .capabilities
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let trusted_capabilities = policy
            .trusted_capabilities
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let mut granted = Vec::new();
        let mut denied = Vec::new();
        for capability in requested_capabilities {
            let normalized = capability.to_ascii_lowercase();
            if endpoint_capabilities.contains(normalized.as_str())
                && trusted_capabilities.contains(normalized.as_str())
            {
                granted.push(capability.clone());
            } else {
                denied.push(capability.clone());
            }
        }
        let usable = denied.is_empty();
        Ok(endpoint_negotiation(
            endpoint,
            usable,
            granted,
            denied,
            if usable { "granted" } else { "capability_denied" },
        ))
    }
}

fn validate_trusted_endpoint_record(
    record: &TrustedEndpointRecord,
) -> Result<(), TrustedEndpointError> {
    if record.endpoint_id.trim().is_empty() {
        return Err(TrustedEndpointError::MissingEndpointId);
    }
    if record.identity_digest_sha256.trim().is_empty() {
        return Err(TrustedEndpointError::MissingIdentityDigest);
    }
    Ok(())
}

fn endpoint_negotiation(
    endpoint: &TrustedEndpointRecord,
    usable: bool,
    granted_capabilities: Vec<String>,
    denied_capabilities: Vec<String>,
    decision_reason: &str,
) -> TrustedEndpointCapabilityNegotiation {
    TrustedEndpointCapabilityNegotiation {
        endpoint_id: endpoint.endpoint_id.clone(),
        usable,
        trust_state: endpoint.trust_state,
        healthy_identity: endpoint.health.healthy,
        granted_capabilities,
        denied_capabilities,
        decision_reason: decision_reason.to_owned(),
    }
}

/// Errors returned by [`WorkerFleetManager`] lifecycle operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkerLifecycleError {
    #[error(transparent)]
    Attestation(#[from] WorkerAttestationError),
    #[error("worker '{0}' is already registered")]
    AlreadyRegistered(String),
    #[error("worker '{0}' is not registered")]
    UnknownWorker(String),
    #[error("requested worker lease ttl exceeds the configured maximum")]
    TtlExceeded,
    #[error("worker '{0}' already has an active lease")]
    LeaseAlreadyActive(String),
    #[error("worker '{0}' is fail-closed and cannot accept work")]
    WorkerFailClosed(String),
    #[error("worker '{0}' is draining and cannot accept new work")]
    WorkerDraining(String),
    #[error("worker '{0}' heartbeat is stale")]
    WorkerOffline(String),
    #[error("no attested worker is available for the requested capabilities")]
    NoAvailableWorker,
    #[error("worker compatibility check failed: {0}")]
    CompatibilityMismatch(String),
    #[error("worker lease request is invalid: {0}")]
    InvalidLeaseRequest(String),
    #[error("worker cleanup failed and the worker stayed fail-closed")]
    CleanupFailed,
}

#[derive(Debug, Clone)]
struct WorkerRecord {
    attestation: WorkerAttestation,
    state: WorkerLifecycleState,
    lease: Option<WorkerLease>,
    last_heartbeat_unix_ms: i64,
}

/// In-memory fleet ledger enforcing fail-closed worker lifecycle transitions.
///
/// Every mutating operation revalidates attestation and policy compatibility before
/// granting work, and emits a [`WorkerLifecycleEvent`] into a bounded recent-event
/// buffer for audit surfaces. `Default` starts an empty fleet.
#[derive(Debug, Default)]
pub struct WorkerFleetManager {
    workers: BTreeMap<String, WorkerRecord>,
    recent_events: VecDeque<WorkerLifecycleEvent>,
}

impl WorkerFleetManager {
    /// Returns aggregate fleet counts for the current in-memory state.
    #[must_use]
    pub fn snapshot(&self) -> WorkerFleetSnapshot {
        let registered_workers = self.workers.len();
        let attested_workers = self
            .workers
            .values()
            .filter(|worker| {
                worker.attestation.egress_proxy_attested
                    && !matches!(worker.state, WorkerLifecycleState::Failed)
            })
            .count();
        let active_leases = self.workers.values().filter(|worker| worker.lease.is_some()).count();
        let available_workers = self
            .workers
            .values()
            .filter(|worker| {
                worker.lease.is_none()
                    && matches!(
                        worker.state,
                        WorkerLifecycleState::Registered
                            | WorkerLifecycleState::Available
                            | WorkerLifecycleState::Completed
                    )
            })
            .count();
        let busy_workers = self
            .workers
            .values()
            .filter(|worker| {
                worker.lease.is_some()
                    || matches!(
                        worker.state,
                        WorkerLifecycleState::Assigned | WorkerLifecycleState::Busy
                    )
            })
            .count();
        let degraded_workers = self
            .workers
            .values()
            .filter(|worker| {
                matches!(
                    worker.state,
                    WorkerLifecycleState::Degraded
                        | WorkerLifecycleState::Failed
                        | WorkerLifecycleState::Orphaned
                )
            })
            .count();
        let draining_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker.state, WorkerLifecycleState::Draining))
            .count();
        let offline_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker.state, WorkerLifecycleState::Offline))
            .count();
        let orphaned_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker.state, WorkerLifecycleState::Orphaned))
            .count();
        let failed_closed_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker.state, WorkerLifecycleState::Failed))
            .count();
        WorkerFleetSnapshot {
            registered_workers,
            attested_workers,
            active_leases,
            available_workers,
            busy_workers,
            degraded_workers,
            draining_workers,
            offline_workers,
            orphaned_workers,
            failed_closed_workers,
        }
    }

    /// Returns retained lifecycle events, most recent first (capped at 64 entries).
    #[must_use]
    pub fn recent_events(&self) -> Vec<WorkerLifecycleEvent> {
        self.recent_events.iter().cloned().collect()
    }

    /// Returns the stored attestation for a registered worker.
    #[must_use]
    pub fn worker_attestation(&self, worker_id: &str) -> Option<WorkerAttestation> {
        self.workers.get(worker_id).map(|worker| worker.attestation.clone())
    }

    /// Registers a new worker after validating its attestation and compatibility.
    ///
    /// A worker that did not report a heartbeat is seeded with `now_unix_ms`.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::Attestation`] or
    /// [`WorkerLifecycleError::CompatibilityMismatch`] when validation fails, and
    /// [`WorkerLifecycleError::AlreadyRegistered`] when the worker id is already known.
    pub fn register_worker(
        &mut self,
        attestation: WorkerAttestation,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        attestation.validate(&policy.attestation, now_unix_ms)?;
        validate_worker_compatibility(&attestation, policy)?;
        if self.workers.contains_key(attestation.worker_id.as_str()) {
            return Err(WorkerLifecycleError::AlreadyRegistered(attestation.worker_id));
        }
        let worker_id = attestation.worker_id.clone();
        // `0` is the serde default for payloads that omitted the heartbeat field; seed
        // those with `now` so a fresh worker is not instantly considered stale.
        let last_heartbeat_unix_ms = if attestation.heartbeat_unix_ms > 0 {
            attestation.heartbeat_unix_ms
        } else {
            now_unix_ms
        };
        self.workers.insert(
            worker_id.clone(),
            WorkerRecord {
                attestation,
                state: WorkerLifecycleState::Registered,
                lease: None,
                last_heartbeat_unix_ms,
            },
        );
        let event = WorkerLifecycleEvent {
            worker_id,
            state: WorkerLifecycleState::Registered,
            run_id: None,
            reason_code: "worker.registered".to_owned(),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    /// Leases the named worker for `request` after full revalidation.
    ///
    /// Revalidates attestation, compatibility, lifecycle state, heartbeat freshness,
    /// and capability trust before issuing the lease.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::InvalidLeaseRequest`] or
    /// [`WorkerLifecycleError::TtlExceeded`] for a malformed request,
    /// [`WorkerLifecycleError::UnknownWorker`] for an unregistered id, and otherwise
    /// the first failing worker gate: [`WorkerLifecycleError::Attestation`],
    /// [`WorkerLifecycleError::CompatibilityMismatch`],
    /// [`WorkerLifecycleError::LeaseAlreadyActive`],
    /// [`WorkerLifecycleError::WorkerFailClosed`],
    /// [`WorkerLifecycleError::WorkerDraining`],
    /// [`WorkerLifecycleError::WorkerOffline`], or
    /// [`WorkerLifecycleError::NoAvailableWorker`] when capabilities do not match.
    pub fn assign_work(
        &mut self,
        worker_id: &str,
        request: WorkerLeaseRequest,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), WorkerLifecycleError> {
        validate_lease_request(&request, policy, now_unix_ms)?;
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        let (lease, event) = assign_worker_record(worker_id, worker, request, policy, now_unix_ms)?;
        self.push_recent_event(event.clone());
        Ok((lease, event))
    }

    /// Leases the first worker (in ascending worker-id order) able to accept `request`.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::InvalidLeaseRequest`] or
    /// [`WorkerLifecycleError::TtlExceeded`] for a malformed request, and
    /// [`WorkerLifecycleError::NoAvailableWorker`] when no registered worker passes
    /// the attestation, compatibility, state, heartbeat, and capability gates.
    pub fn assign_next_work(
        &mut self,
        request: WorkerLeaseRequest,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), WorkerLifecycleError> {
        validate_lease_request(&request, policy, now_unix_ms)?;
        let Some(worker_id) = self.workers.iter().find_map(|(worker_id, worker)| {
            worker_record_can_accept(worker, &request, policy, now_unix_ms)
                .then(|| worker_id.clone())
        }) else {
            return Err(WorkerLifecycleError::NoAvailableWorker);
        };
        let worker = self
            .workers
            .get_mut(worker_id.as_str())
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.clone()))?;
        let (lease, event) =
            assign_worker_record(worker_id.as_str(), worker, request, policy, now_unix_ms)?;
        self.push_recent_event(event.clone());
        Ok((lease, event))
    }

    /// Finalizes the worker's current run, requiring verified cleanup.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id and
    /// [`WorkerLifecycleError::CleanupFailed`] when the cleanup report is unverified;
    /// the worker is then left fail-closed.
    pub fn complete_work(
        &mut self,
        worker_id: &str,
        cleanup: &WorkerCleanupReport,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let outcome = self.finalize_work(worker_id, cleanup.clone(), now_unix_ms)?;
        if outcome.cleanup_succeeded {
            Ok(outcome.event)
        } else {
            Err(WorkerLifecycleError::CleanupFailed)
        }
    }

    /// Finalizes the worker's current run, recording the cleanup outcome.
    ///
    /// Unlike [`Self::complete_work`] this does not treat unverified cleanup as an
    /// error: the worker is driven fail-closed and the outcome is returned so callers
    /// can journal it. Verified cleanup on an already fail-closed worker keeps the
    /// fail-closed state and only records that cleanup was verified.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id.
    pub fn finalize_work(
        &mut self,
        worker_id: &str,
        cleanup: WorkerCleanupReport,
        now_unix_ms: i64,
    ) -> Result<WorkerCleanupOutcome, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
        let cleanup_succeeded = cleanup.failure_reason.is_none()
            && cleanup.removed_workspace_scope
            && cleanup.removed_artifacts
            && cleanup.removed_logs;
        let event = if cleanup_succeeded {
            // Verified cleanup must not lift a fail-closed (failed/orphaned) worker
            // back into rotation: it stays unassignable until it re-registers with a
            // fresh attestation.
            let requires_fresh_attestation =
                worker_fail_closed_state_requires_fresh_attestation(worker.state)
                    && worker.lease.is_none();
            if !requires_fresh_attestation {
                worker.state = WorkerLifecycleState::Completed;
            }
            worker.lease = None;
            WorkerLifecycleEvent {
                worker_id: worker_id.to_owned(),
                state: worker.state,
                run_id,
                reason_code: if requires_fresh_attestation {
                    "worker.cleanup_verified_requires_reattestation".to_owned()
                } else {
                    "worker.completed".to_owned()
                },
                timestamp_unix_ms: now_unix_ms,
            }
        } else {
            worker.state = WorkerLifecycleState::Failed;
            worker.lease = None;
            WorkerLifecycleEvent {
                worker_id: worker_id.to_owned(),
                state: WorkerLifecycleState::Failed,
                run_id,
                reason_code: "worker.cleanup_failed".to_owned(),
                timestamp_unix_ms: now_unix_ms,
            }
        };
        self.push_recent_event(event.clone());
        Ok(WorkerCleanupOutcome { event, cleanup_report: cleanup, cleanup_succeeded })
    }

    /// Quarantines the worker: revokes any lease and marks it `Failed`.
    ///
    /// Non-conforming reason codes fall back to `worker.quarantined_by_operator`.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id.
    pub fn quarantine_worker(
        &mut self,
        worker_id: &str,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
        worker.state = WorkerLifecycleState::Failed;
        worker.lease = None;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Failed,
            run_id,
            reason_code: normalize_operator_reason_code(
                reason_code,
                "worker.quarantined_by_operator",
            ),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    /// Quarantines every worker, returning the emitted lifecycle events.
    ///
    /// Workers that are already `Failed` and hold no lease are skipped. Non-conforming
    /// reason codes fall back to `worker.drained_by_operator`.
    pub fn quarantine_all_workers(
        &mut self,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Vec<WorkerLifecycleEvent> {
        let reason_code = normalize_operator_reason_code(reason_code, "worker.drained_by_operator");
        let mut events = Vec::new();
        for (worker_id, worker) in &mut self.workers {
            if matches!(worker.state, WorkerLifecycleState::Failed) && worker.lease.is_none() {
                continue;
            }
            let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
            worker.state = WorkerLifecycleState::Failed;
            worker.lease = None;
            events.push(WorkerLifecycleEvent {
                worker_id: worker_id.clone(),
                state: WorkerLifecycleState::Failed,
                run_id,
                reason_code: reason_code.clone(),
                timestamp_unix_ms: now_unix_ms,
            });
        }
        for event in &events {
            self.push_recent_event(event.clone());
        }
        events
    }

    /// Revalidates an idle worker's stored attestation and returns it to `Registered`.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id,
    /// [`WorkerLifecycleError::LeaseAlreadyActive`] while a lease is held,
    /// [`WorkerLifecycleError::WorkerFailClosed`] for failed or orphaned workers
    /// (those require fresh registration), and any attestation or compatibility
    /// rejection.
    pub fn reverify_worker(
        &mut self,
        worker_id: &str,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        if worker.lease.is_some() {
            return Err(WorkerLifecycleError::LeaseAlreadyActive(worker_id.to_owned()));
        }
        if worker_fail_closed_state_requires_fresh_attestation(worker.state) {
            return Err(WorkerLifecycleError::WorkerFailClosed(worker_id.to_owned()));
        }
        worker.attestation.validate(&policy.attestation, now_unix_ms)?;
        validate_worker_compatibility(&worker.attestation, policy)?;
        worker.state = WorkerLifecycleState::Registered;
        worker.last_heartbeat_unix_ms = now_unix_ms;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Registered,
            run_id: None,
            reason_code: "worker.reverified_by_operator".to_owned(),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    /// Records a worker heartbeat after revalidating attestation and compatibility.
    ///
    /// Only an idle `Offline` worker recovers to `Registered`; fail-closed workers
    /// keep their state even though the heartbeat is recorded.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id and any
    /// attestation or compatibility rejection.
    pub fn heartbeat_worker(
        &mut self,
        worker_id: &str,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        worker.attestation.validate(&policy.attestation, now_unix_ms)?;
        validate_worker_compatibility(&worker.attestation, policy)?;
        worker.last_heartbeat_unix_ms = now_unix_ms;
        if matches!(worker.state, WorkerLifecycleState::Offline) && worker.lease.is_none() {
            worker.state = WorkerLifecycleState::Registered;
        }
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: worker.state,
            run_id: worker.lease.as_ref().map(|lease| lease.run_id.clone()),
            reason_code: "worker.heartbeat".to_owned(),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    /// Marks the worker `Draining`: it keeps any current lease but accepts no new work.
    ///
    /// Non-conforming reason codes fall back to `worker.draining`.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id.
    pub fn drain_worker(
        &mut self,
        worker_id: &str,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        worker.state = WorkerLifecycleState::Draining;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Draining,
            run_id: worker.lease.as_ref().map(|lease| lease.run_id.clone()),
            reason_code: normalize_operator_reason_code(reason_code, "worker.draining"),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    /// Revokes any active lease and marks the worker `Orphaned` pending cleanup and
    /// re-registration.
    ///
    /// Non-conforming reason codes fall back to `worker.lease_revoked`.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id.
    pub fn revoke_lease(
        &mut self,
        worker_id: &str,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let worker = self
            .workers
            .get_mut(worker_id)
            .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
        let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
        worker.lease = None;
        worker.state = WorkerLifecycleState::Orphaned;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Orphaned,
            run_id,
            reason_code: normalize_operator_reason_code(reason_code, "worker.lease_revoked"),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    /// Operator entry point for recording a cleanup report; alias of
    /// [`Self::finalize_work`].
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id.
    pub fn force_cleanup_worker(
        &mut self,
        worker_id: &str,
        cleanup: WorkerCleanupReport,
        now_unix_ms: i64,
    ) -> Result<WorkerCleanupOutcome, WorkerLifecycleError> {
        self.finalize_work(worker_id, cleanup, now_unix_ms)
    }

    /// Orphans every worker whose lease ttl has elapsed, returning the emitted events.
    pub fn reap_expired_workers(&mut self, now_unix_ms: i64) -> Vec<WorkerLifecycleEvent> {
        let mut events = Vec::new();
        for (worker_id, worker) in &mut self.workers {
            let expired =
                worker.lease.as_ref().is_some_and(|lease| lease.expires_at_unix_ms <= now_unix_ms);
            if expired {
                let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
                worker.state = WorkerLifecycleState::Orphaned;
                worker.lease = None;
                events.push(WorkerLifecycleEvent {
                    worker_id: worker_id.clone(),
                    state: WorkerLifecycleState::Orphaned,
                    run_id,
                    reason_code: "worker.ttl_expired".to_owned(),
                    timestamp_unix_ms: now_unix_ms,
                });
            }
        }
        for event in &events {
            self.push_recent_event(event.clone());
        }
        events
    }

    /// Transitions workers with stale heartbeats, returning the emitted events.
    ///
    /// A stale worker holding a lease is orphaned (its lease is revoked); a stale idle
    /// worker goes `Offline` and may recover via [`Self::heartbeat_worker`].
    pub fn mark_stale_heartbeat_workers(
        &mut self,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Vec<WorkerLifecycleEvent> {
        let mut events = Vec::new();
        for (worker_id, worker) in &mut self.workers {
            if matches!(
                worker.state,
                WorkerLifecycleState::Failed
                    | WorkerLifecycleState::Offline
                    | WorkerLifecycleState::Orphaned
            ) {
                continue;
            }
            if worker_heartbeat_is_fresh(worker, policy, now_unix_ms) {
                continue;
            }
            let run_id = worker.lease.as_ref().map(|lease| lease.run_id.clone());
            let next_state = if worker.lease.is_some() {
                WorkerLifecycleState::Orphaned
            } else {
                WorkerLifecycleState::Offline
            };
            worker.state = next_state;
            worker.lease = None;
            events.push(WorkerLifecycleEvent {
                worker_id: worker_id.clone(),
                state: next_state,
                run_id,
                reason_code: "worker.heartbeat_stale".to_owned(),
                timestamp_unix_ms: now_unix_ms,
            });
        }
        for event in &events {
            self.push_recent_event(event.clone());
        }
        events
    }

    fn push_recent_event(&mut self, event: WorkerLifecycleEvent) {
        self.recent_events.push_front(event);
        while self.recent_events.len() > MAX_RECENT_LIFECYCLE_EVENTS {
            self.recent_events.pop_back();
        }
    }
}

/// Sanitizes an operator-supplied reason code, substituting `fallback` when it is
/// empty, longer than 128 bytes, or contains characters outside `[A-Za-z0-9._-]`.
///
/// Reason codes flow verbatim into audit events, so unconstrained operator input is
/// replaced rather than escaped.
fn normalize_operator_reason_code(raw: &str, fallback: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        return fallback.to_owned();
    }
    if trimmed.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')) {
        trimmed.to_owned()
    } else {
        fallback.to_owned()
    }
}

/// Rejects malformed lease requests before any worker state is touched.
fn validate_lease_request(
    request: &WorkerLeaseRequest,
    policy: &WorkerFleetPolicy,
    now_unix_ms: i64,
) -> Result<(), WorkerLifecycleError> {
    if request.run_id.trim().is_empty() {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "run_id must not be empty".to_owned(),
        ));
    }
    if request.ttl_ms == 0 {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "ttl_ms must be positive".to_owned(),
        ));
    }
    if request.ttl_ms > policy.max_ttl_ms {
        return Err(WorkerLifecycleError::TtlExceeded);
    }
    if request.grant.grant_id.trim().is_empty() || request.grant.grant_id.len() > MAX_GRANT_ID_BYTES
    {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "grant_id must be present and bounded".to_owned(),
        ));
    }
    if request.grant.run_id != request.run_id {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "grant run_id must match lease run_id".to_owned(),
        ));
    }
    if request.grant.expires_at_unix_ms <= now_unix_ms {
        return Err(WorkerLifecycleError::InvalidLeaseRequest("grant is expired".to_owned()));
    }
    Ok(())
}

/// Checks the policy-required capability authority, SDK protocol, and WIT ABI pins.
fn validate_worker_compatibility(
    attestation: &WorkerAttestation,
    policy: &WorkerFleetPolicy,
) -> Result<(), WorkerLifecycleError> {
    if let Some(expected) = policy.required_capability_authority_sha256.as_deref() {
        let Some(actual) = attestation.capability_authority_sha256.as_deref() else {
            return Err(WorkerLifecycleError::CompatibilityMismatch(
                "capability authority digest is required".to_owned(),
            ));
        };
        if actual != expected {
            return Err(WorkerLifecycleError::CompatibilityMismatch(
                "capability authority digest mismatch".to_owned(),
            ));
        }
    }
    if let Some(expected) = policy.required_sdk_protocol_version {
        if attestation.sdk_protocol_version != expected {
            return Err(WorkerLifecycleError::CompatibilityMismatch(format!(
                "sdk_protocol_version={} expected={expected}",
                attestation.sdk_protocol_version
            )));
        }
    }
    if let Some(expected) = policy.required_wit_abi_version.as_deref() {
        if attestation.wit_abi_version != expected {
            return Err(WorkerLifecycleError::CompatibilityMismatch(format!(
                "wit_abi_version={} expected={expected}",
                attestation.wit_abi_version
            )));
        }
    }
    Ok(())
}

fn worker_heartbeat_is_fresh(
    worker: &WorkerRecord,
    policy: &WorkerFleetPolicy,
    now_unix_ms: i64,
) -> bool {
    now_unix_ms.saturating_sub(worker.last_heartbeat_unix_ms)
        <= i64::try_from(policy.heartbeat_timeout_ms).unwrap_or(i64::MAX)
}

/// States that must never return to rotation without a fresh registration/attestation.
fn worker_fail_closed_state_requires_fresh_attestation(state: WorkerLifecycleState) -> bool {
    matches!(state, WorkerLifecycleState::Failed | WorkerLifecycleState::Orphaned)
}

/// Issues a lease on `worker` after revalidating every fail-closed assignment gate.
fn assign_worker_record(
    worker_id: &str,
    worker: &mut WorkerRecord,
    request: WorkerLeaseRequest,
    policy: &WorkerFleetPolicy,
    now_unix_ms: i64,
) -> Result<(WorkerLease, WorkerLifecycleEvent), WorkerLifecycleError> {
    worker.attestation.validate(&policy.attestation, now_unix_ms)?;
    validate_worker_compatibility(&worker.attestation, policy)?;
    if worker.lease.is_some() {
        return Err(WorkerLifecycleError::LeaseAlreadyActive(worker_id.to_owned()));
    }
    if worker_fail_closed_state_requires_fresh_attestation(worker.state) {
        return Err(WorkerLifecycleError::WorkerFailClosed(worker_id.to_owned()));
    }
    if matches!(worker.state, WorkerLifecycleState::Draining) {
        return Err(WorkerLifecycleError::WorkerDraining(worker_id.to_owned()));
    }
    if !worker_heartbeat_is_fresh(worker, policy, now_unix_ms) {
        worker.state = WorkerLifecycleState::Offline;
        return Err(WorkerLifecycleError::WorkerOffline(worker_id.to_owned()));
    }
    if !worker_supports_capabilities(worker, request.required_capabilities.as_slice(), policy) {
        return Err(WorkerLifecycleError::NoAvailableWorker);
    }
    let lease = WorkerLease {
        lease_id: Ulid::new().to_string(),
        worker_id: worker_id.to_owned(),
        run_id: request.run_id.clone(),
        // NOTE: `ttl_ms as i64` can wrap only when `policy.max_ttl_ms` is
        // configured above i64::MAX milliseconds; the wrapped (negative) ttl yields an
        // already-expired lease that fails closed at the next reap, rather than
        // granting unbounded time, so the cast is kept as-is.
        expires_at_unix_ms: now_unix_ms.saturating_add(request.ttl_ms as i64),
        required_capabilities: request.required_capabilities,
        workspace_scope: request.workspace_scope,
        artifact_transport: request.artifact_transport,
        grant: request.grant,
    };
    let event = WorkerLifecycleEvent {
        worker_id: worker_id.to_owned(),
        state: WorkerLifecycleState::Assigned,
        run_id: Some(lease.run_id.clone()),
        reason_code: "worker.assigned".to_owned(),
        timestamp_unix_ms: now_unix_ms,
    };
    worker.state = WorkerLifecycleState::Assigned;
    worker.lease = Some(lease.clone());
    Ok((lease, event))
}

/// Read-only mirror of the gates enforced by `assign_worker_record`, used to pick an
/// assignment candidate without mutating worker state.
fn worker_record_can_accept(
    worker: &WorkerRecord,
    request: &WorkerLeaseRequest,
    policy: &WorkerFleetPolicy,
    now_unix_ms: i64,
) -> bool {
    worker.lease.is_none()
        && !matches!(worker.state, WorkerLifecycleState::Draining | WorkerLifecycleState::Offline)
        && !worker_fail_closed_state_requires_fresh_attestation(worker.state)
        && worker.attestation.validate(&policy.attestation, now_unix_ms).is_ok()
        && validate_worker_compatibility(&worker.attestation, policy).is_ok()
        && worker_heartbeat_is_fresh(worker, policy, now_unix_ms)
        && worker_supports_capabilities(worker, request.required_capabilities.as_slice(), policy)
}

/// A capability is grantable only when the policy trusts it *and* the worker
/// self-reports it (both compared ASCII case-insensitively); neither side alone may
/// widen access.
fn worker_supports_capabilities(
    worker: &WorkerRecord,
    required_capabilities: &[String],
    policy: &WorkerFleetPolicy,
) -> bool {
    if required_capabilities.is_empty() {
        return true;
    }
    let trusted_capabilities = policy
        .trusted_capabilities
        .iter()
        .map(|capability| capability.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    required_capabilities.iter().all(|required| {
        let normalized_required = required.to_ascii_lowercase();
        trusted_capabilities.contains(normalized_required.as_str())
            && worker
                .attestation
                .supported_capabilities
                .iter()
                .any(|available| available.eq_ignore_ascii_case(required))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        TrustedEndpointHealth, TrustedEndpointPolicy, TrustedEndpointRecord,
        TrustedEndpointRegistry, TrustedEndpointTransport, TrustedEndpointTrustState,
        WorkerArtifactTransport, WorkerAttestation, WorkerCleanupReport, WorkerFleetManager,
        WorkerFleetPolicy, WorkerLeaseRequest, WorkerLifecycleError, WorkerLifecycleState,
        WorkerRemoteIdentity, WorkerRemoteLeaseBinding, WorkerRemoteToolContractError,
        WorkerRemoteToolKind, WorkerRemoteToolRequestEnvelope, WorkerRemoteToolResultEnvelope,
        WorkerRemoteWorkspaceTransfer, WorkerRunGrant, WorkerWorkspaceScope,
        WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
    };

    fn hex_digest(byte: &str) -> String {
        byte.repeat(64)
    }

    fn attestation(worker_id: &str) -> WorkerAttestation {
        WorkerAttestation {
            worker_id: worker_id.to_owned(),
            image_digest_sha256: "img".repeat(16),
            build_digest_sha256: "bld".repeat(16),
            artifact_digest_sha256: "art".repeat(16),
            egress_proxy_attested: true,
            supported_capabilities: vec!["tool:palyra.echo".to_owned()],
            capability_authority_sha256: None,
            sdk_protocol_version: 1,
            wit_abi_version: "palyra-worker-abi/v1".to_owned(),
            heartbeat_unix_ms: 2_000,
            issued_at_unix_ms: 1_000,
            expires_at_unix_ms: 10_000,
        }
    }

    fn trusted_endpoint_record() -> TrustedEndpointRecord {
        TrustedEndpointRecord {
            endpoint_id: "worker-endpoint-a".to_owned(),
            trust_state: TrustedEndpointTrustState::Unknown,
            last_seen_unix_ms: 2_000,
            capabilities: vec!["tool:palyra.fs.read_file".to_owned()],
            transport: TrustedEndpointTransport::Quic,
            identity_digest_sha256: hex_digest("a"),
            policy_bindings: Vec::new(),
            health: TrustedEndpointHealth {
                healthy: true,
                checked_at_unix_ms: 2_000,
                failure_reason: None,
            },
        }
    }

    #[test]
    fn trusted_endpoint_observation_requires_explicit_approval() {
        let mut registry = TrustedEndpointRegistry::default();
        let policy = TrustedEndpointPolicy::default();

        let record = registry
            .observe_endpoint(trusted_endpoint_record(), &policy)
            .expect("endpoint observation should persist");
        let negotiation = registry
            .negotiate_capabilities(
                record.endpoint_id.as_str(),
                &["tool:palyra.fs.read_file".to_owned()],
                &policy,
            )
            .expect("observed endpoint should be negotiable");

        assert_eq!(record.trust_state, TrustedEndpointTrustState::PendingApproval);
        assert!(!negotiation.usable);
        assert_eq!(negotiation.decision_reason, "trust_required");
    }

    #[test]
    fn trusted_endpoint_negotiation_grants_only_policy_capabilities() {
        let mut registry = TrustedEndpointRegistry::default();
        let policy = TrustedEndpointPolicy::default();
        registry
            .observe_endpoint(trusted_endpoint_record(), &policy)
            .expect("endpoint observation should persist");
        registry
            .approve_endpoint("worker-endpoint-a", vec!["operator-approved".to_owned()], 2_100)
            .expect("endpoint approval should succeed");

        let negotiation = registry
            .negotiate_capabilities(
                "worker-endpoint-a",
                &["tool:palyra.fs.read_file".to_owned(), "tool:palyra.untrusted".to_owned()],
                &policy,
            )
            .expect("trusted endpoint should negotiate");

        assert!(!negotiation.usable);
        assert_eq!(negotiation.granted_capabilities, ["tool:palyra.fs.read_file"]);
        assert_eq!(negotiation.denied_capabilities, ["tool:palyra.untrusted"]);
        assert_eq!(negotiation.decision_reason, "capability_denied");
    }

    #[test]
    fn trusted_endpoint_preview_transports_are_disabled_by_default() {
        let mut registry = TrustedEndpointRegistry::default();
        let policy = TrustedEndpointPolicy::default();
        let mut record = trusted_endpoint_record();
        record.transport = TrustedEndpointTransport::LanDiscoveryPreview;
        registry.observe_endpoint(record, &policy).expect("endpoint observation should persist");
        registry
            .approve_endpoint("worker-endpoint-a", vec!["operator-approved".to_owned()], 2_100)
            .expect("endpoint approval should succeed");

        let negotiation = registry
            .negotiate_capabilities(
                "worker-endpoint-a",
                &["tool:palyra.fs.read_file".to_owned()],
                &policy,
            )
            .expect("trusted endpoint should negotiate");

        assert!(!negotiation.usable);
        assert_eq!(negotiation.decision_reason, "preview_transport_disabled");
    }

    fn lease_request(run_id: &str, ttl_ms: u64) -> WorkerLeaseRequest {
        WorkerLeaseRequest {
            run_id: run_id.to_owned(),
            ttl_ms,
            required_capabilities: Vec::new(),
            workspace_scope: WorkerWorkspaceScope {
                workspace_root: "/workspace".to_owned(),
                allowed_paths: vec!["src".to_owned()],
                read_only: false,
            },
            artifact_transport: WorkerArtifactTransport {
                input_manifest_sha256: "in".repeat(32),
                output_manifest_sha256: "out".repeat(32),
                log_stream_id: "log-stream".to_owned(),
                scratch_directory_id: "scratch".to_owned(),
            },
            grant: WorkerRunGrant {
                grant_id: format!("grant-{run_id}"),
                run_id: run_id.to_owned(),
                tool_name: "palyra.echo".to_owned(),
                expires_at_unix_ms: 9_000,
            },
        }
    }

    fn policy_for(capability: &str) -> WorkerFleetPolicy {
        WorkerFleetPolicy { trusted_capabilities: vec![capability.into()], ..Default::default() }
    }

    fn remote_identity(worker_id: &str) -> WorkerRemoteIdentity {
        WorkerRemoteIdentity {
            worker_id: worker_id.to_owned(),
            image_digest_sha256: hex_digest("a"),
            build_digest_sha256: hex_digest("b"),
            artifact_digest_sha256: hex_digest("c"),
            capability_authority_sha256: Some(hex_digest("d")),
            sdk_protocol_version: 1,
            wit_abi_version: "palyra-worker-abi/v1".to_owned(),
        }
    }

    fn remote_request(tool_name: &str) -> WorkerRemoteToolRequestEnvelope {
        let tool_kind = WorkerRemoteToolKind::from_tool_name(tool_name)
            .expect("test tool should be remote-capable");
        WorkerRemoteToolRequestEnvelope {
            protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
            schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
            request_id: "remote-request-01".to_owned(),
            proposal_id: "proposal-01".to_owned(),
            tool_name: tool_name.to_owned(),
            tool_kind,
            input_json: r#"{"path":"src/lib.rs"}"#.to_owned(),
            input_json_sha256: hex_digest("1"),
            lease: WorkerRemoteLeaseBinding {
                lease_id: "lease-01".to_owned(),
                worker_id: "worker-remote-01".to_owned(),
                run_id: "run-01".to_owned(),
                grant_id: "grant-01".to_owned(),
                grant_tool_name: tool_name.to_owned(),
                expires_at_unix_ms: 3_000,
                required_capabilities: vec![tool_kind.required_capability()],
                workspace_scope: WorkerWorkspaceScope {
                    workspace_root: "/workspace".to_owned(),
                    allowed_paths: vec!["src".to_owned()],
                    read_only: true,
                },
                artifact_transport: WorkerArtifactTransport {
                    input_manifest_sha256: hex_digest("2"),
                    output_manifest_sha256: hex_digest("3"),
                    log_stream_id: "logs/run-01/proposal-01".to_owned(),
                    scratch_directory_id: "scratch/run-01/proposal-01".to_owned(),
                },
            },
            worker_identity: remote_identity("worker-remote-01"),
            workspace_transfer: WorkerRemoteWorkspaceTransfer::manifest(hex_digest("4")),
        }
    }

    fn remote_result(
        request: &WorkerRemoteToolRequestEnvelope,
        output_json: &str,
    ) -> WorkerRemoteToolResultEnvelope {
        WorkerRemoteToolResultEnvelope {
            protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
            schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
            request_id: request.request_id.clone(),
            proposal_id: request.proposal_id.clone(),
            tool_name: request.tool_name.clone(),
            tool_kind: request.tool_kind,
            worker_id: request.lease.worker_id.clone(),
            lease_id: request.lease.lease_id.clone(),
            success: true,
            output_json: output_json.to_owned(),
            output_json_sha256: hex_digest("5"),
            error: None,
            output_manifest_sha256: hex_digest("6"),
            cleanup_report: WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: None,
            },
            worker_identity: request.worker_identity.clone(),
            completed_at_unix_ms: 2_000,
        }
    }

    #[test]
    fn remote_tool_kind_maps_backend_parity_tools() {
        let cases = [
            ("palyra.fs.read_file", WorkerRemoteToolKind::FsRead),
            ("palyra.fs.list_dir", WorkerRemoteToolKind::FsList),
            ("palyra.fs.search", WorkerRemoteToolKind::FsSearch),
            ("palyra.process.run", WorkerRemoteToolKind::ProcessRun),
            ("palyra.fs.apply_patch", WorkerRemoteToolKind::ApplyPatch),
            ("palyra.artifact.read", WorkerRemoteToolKind::ArtifactRead),
            ("palyra.tool_program.run", WorkerRemoteToolKind::ToolProgramRun),
        ];

        for (tool_name, expected) in cases {
            assert_eq!(WorkerRemoteToolKind::from_tool_name(tool_name), Some(expected));
            assert_eq!(expected.tool_name(), tool_name);
            assert_eq!(expected.required_capability(), format!("tool:{tool_name}"));
        }
        assert_eq!(WorkerRemoteToolKind::from_tool_name("palyra.http.fetch"), None);
    }

    #[test]
    fn remote_request_validates_lease_and_manifest_contract() {
        let request = remote_request("palyra.fs.read_file");
        request.validate(2_000).expect("well-formed request should validate");

        let mut expired = request.clone();
        expired.lease.expires_at_unix_ms = 1_999;
        let error = expired.validate(2_000).expect_err("expired lease must fail closed");
        assert!(matches!(error, WorkerRemoteToolContractError::LeaseExpired { .. }));

        let mut missing_capability = request.clone();
        missing_capability.lease.required_capabilities.clear();
        let error = missing_capability
            .validate(2_000)
            .expect_err("missing tool capability must fail closed");
        assert!(matches!(error, WorkerRemoteToolContractError::MissingRequiredCapability { .. }));
    }

    #[test]
    fn remote_result_requires_cleanup_and_identity_stability() {
        let request = remote_request("palyra.process.run");
        let result = remote_result(&request, r#"{"schema_version":2,"exit_code":0}"#);
        result.validate_against_request(&request, 2_000).expect("matching result should validate");

        let mut cleanup_gap = result.clone();
        cleanup_gap.cleanup_report.removed_logs = false;
        let error = cleanup_gap
            .validate_against_request(&request, 2_000)
            .expect_err("cleanup gaps must fail closed");
        assert!(matches!(error, WorkerRemoteToolContractError::CleanupGap { .. }));

        let mut identity_mismatch = result;
        identity_mismatch.worker_identity.worker_id = "worker-remote-02".to_owned();
        let error = identity_mismatch
            .validate_against_request(&request, 2_000)
            .expect_err("identity drift must fail closed");
        assert!(matches!(error, WorkerRemoteToolContractError::WorkerIdentityMismatch { .. }));
    }

    #[test]
    fn worker_lifecycle_supports_successful_handshake_assignment_and_cleanup() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();

        let register = manager
            .register_worker(attestation("worker-a"), &policy, 2_000)
            .expect("worker should register");
        assert_eq!(register.reason_code, "worker.registered");

        let (lease, assign) = manager
            .assign_work("worker-a", lease_request("run-1", 500), &policy, 2_500)
            .expect("worker should accept a lease");
        assert_eq!(lease.run_id, "run-1");
        assert_eq!(assign.state, WorkerLifecycleState::Assigned);

        let complete = manager
            .complete_work(
                "worker-a",
                &WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: true,
                    removed_logs: true,
                    failure_reason: None,
                },
                3_000,
            )
            .expect("cleanup should succeed");
        assert_eq!(complete.state, WorkerLifecycleState::Completed);
        assert_eq!(manager.snapshot().active_leases, 0);
    }

    #[test]
    fn worker_registration_rejects_missing_egress_proxy_attestation() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        let mut worker_attestation = attestation("worker-b");
        worker_attestation.egress_proxy_attested = false;

        let error = manager
            .register_worker(worker_attestation, &policy, 2_000)
            .expect_err("egress proxy binding should be required");
        assert!(matches!(
            error,
            WorkerLifecycleError::Attestation(
                super::WorkerAttestationError::MissingEgressProxyBinding
            )
        ));
    }

    #[test]
    fn worker_cleanup_failure_stays_fail_closed() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-c"), &policy, 2_000).unwrap();
        manager.assign_work("worker-c", lease_request("run-2", 500), &policy, 2_500).unwrap();

        let error = manager
            .complete_work(
                "worker-c",
                &WorkerCleanupReport {
                    removed_workspace_scope: false,
                    removed_artifacts: true,
                    removed_logs: true,
                    failure_reason: Some("artifact cleanup failure".to_owned()),
                },
                3_000,
            )
            .expect_err("cleanup failure should not be ignored");
        assert_eq!(error, WorkerLifecycleError::CleanupFailed);
    }

    #[test]
    fn worker_ttl_reap_marks_orphaned_instances() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-d"), &policy, 2_000).unwrap();
        manager.assign_work("worker-d", lease_request("run-3", 250), &policy, 2_500).unwrap();

        let events = manager.reap_expired_workers(2_751);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].state, WorkerLifecycleState::Orphaned);
        assert_eq!(manager.snapshot().orphaned_workers, 1);
    }

    #[test]
    fn worker_auto_assignment_matches_required_capabilities() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-e"), &policy, 2_000).unwrap();

        let mut request = lease_request("run-4", 500);
        request.required_capabilities = vec!["tool:palyra.echo".to_owned()];
        let (lease, event) = manager
            .assign_next_work(request, &policy, 2_500)
            .expect("matching worker should accept the lease");

        assert_eq!(lease.worker_id, "worker-e");
        assert_eq!(lease.required_capabilities, vec!["tool:palyra.echo"]);
        assert_eq!(event.state, WorkerLifecycleState::Assigned);
        assert_eq!(manager.recent_events().len(), 2);
    }

    #[test]
    fn worker_auto_assignment_rejects_missing_capability() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-f"), &policy, 2_000).unwrap();

        let mut request = lease_request("run-5", 500);
        request.required_capabilities = vec!["tool:palyra.sleep".to_owned()];
        let error = manager
            .assign_next_work(request, &policy, 2_500)
            .expect_err("missing worker capability should fail closed");

        assert_eq!(error, WorkerLifecycleError::NoAvailableWorker);
        assert_eq!(manager.snapshot().active_leases, 0);
    }

    #[test]
    fn worker_cleanup_failure_records_failed_event_for_journal_surfaces() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-g"), &policy, 2_000).unwrap();
        manager.assign_work("worker-g", lease_request("run-6", 500), &policy, 2_500).unwrap();

        let outcome = manager
            .finalize_work(
                "worker-g",
                WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: false,
                    removed_logs: true,
                    failure_reason: Some("artifact cleanup failure".to_owned()),
                },
                3_000,
            )
            .expect("cleanup outcome should be returned for journal emission");

        assert!(!outcome.cleanup_succeeded);
        assert_eq!(outcome.event.state, WorkerLifecycleState::Failed);
        assert_eq!(outcome.event.reason_code, "worker.cleanup_failed");
        assert_eq!(manager.snapshot().failed_closed_workers, 1);
        let error = manager
            .assign_work("worker-g", lease_request("run-7", 500), &policy, 3_100)
            .expect_err("failed worker must stay fail closed");
        assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));
    }

    #[test]
    fn operator_quarantine_and_drain_fail_closed() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-h"), &policy, 2_000).unwrap();
        manager.assign_work("worker-h", lease_request("run-8", 500), &policy, 2_500).unwrap();

        let quarantine = manager
            .quarantine_worker("worker-h", "worker.operator.quarantine", 2_750)
            .expect("operator quarantine should be recorded");
        assert_eq!(quarantine.state, WorkerLifecycleState::Failed);
        assert_eq!(quarantine.run_id.as_deref(), Some("run-8"));
        assert_eq!(manager.snapshot().failed_closed_workers, 1);

        manager.register_worker(attestation("worker-i"), &policy, 2_800).unwrap();
        let drain = manager.quarantine_all_workers("worker.operator.drain", 3_000);
        assert_eq!(drain.len(), 1);
        assert_eq!(drain[0].reason_code, "worker.operator.drain");
        assert_eq!(manager.snapshot().failed_closed_workers, 2);
    }

    #[test]
    fn operator_reverify_rejects_fail_closed_workers_and_active_leases() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-j"), &policy, 2_000).unwrap();
        manager.assign_work("worker-j", lease_request("run-9", 500), &policy, 2_100).unwrap();

        let active_lease_error = manager
            .reverify_worker("worker-j", &policy, 2_200)
            .expect_err("active lease must not be reverified in place");
        assert!(matches!(active_lease_error, WorkerLifecycleError::LeaseAlreadyActive(_)));

        manager.quarantine_worker("worker-j", "worker.operator.quarantine", 2_300).unwrap();

        let error = manager
            .reverify_worker("worker-j", &policy, 2_400)
            .expect_err("fail-closed worker must not be reverified without fresh registration");
        assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));

        let error = manager
            .assign_work("worker-j", lease_request("run-9b", 500), &policy, 2_500)
            .expect_err("failed worker must stay unassignable");
        assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));
    }

    #[test]
    fn force_cleanup_promotes_only_verified_cleanup_reports() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-k"), &policy, 2_000).unwrap();
        manager.assign_work("worker-k", lease_request("run-10", 500), &policy, 2_500).unwrap();

        let failed = manager
            .force_cleanup_worker(
                "worker-k",
                WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: false,
                    removed_logs: true,
                    failure_reason: Some("operator could not remove artifact".to_owned()),
                },
                2_700,
            )
            .expect("cleanup report should be recorded");
        assert!(!failed.cleanup_succeeded);
        assert_eq!(failed.event.state, WorkerLifecycleState::Failed);

        let recovered = manager
            .force_cleanup_worker(
                "worker-k",
                WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: true,
                    removed_logs: true,
                    failure_reason: None,
                },
                2_900,
            )
            .expect("verified cleanup should be recorded");
        assert!(recovered.cleanup_succeeded);
        assert_eq!(recovered.event.state, WorkerLifecycleState::Failed);
        assert_eq!(recovered.event.reason_code, "worker.cleanup_verified_requires_reattestation");
        assert_eq!(manager.snapshot().failed_closed_workers, 1);
        let error = manager
            .assign_work("worker-k", lease_request("run-10b", 500), &policy, 3_000)
            .expect_err("cleanup verification alone must not make failed workers assignable");
        assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));
    }

    #[test]
    fn capability_matching_requires_worker_self_report_and_policy_trust() {
        let mut manager = WorkerFleetManager::default();
        let policy = policy_for("tool:palyra.sleep");
        let mut attestation = attestation("worker-l");
        attestation.supported_capabilities = vec!["tool:palyra.sleep".to_owned()];
        manager.register_worker(attestation, &policy, 2_000).unwrap();

        let mut request = lease_request("run-11", 500);
        request.required_capabilities = vec!["tool:palyra.echo".to_owned()];
        let error = manager
            .assign_next_work(request, &policy, 2_500)
            .expect_err("untrusted capability must fail closed even if another tool is trusted");

        assert_eq!(error, WorkerLifecycleError::NoAvailableWorker);
        assert_eq!(manager.snapshot().active_leases, 0);
    }

    #[test]
    fn stale_heartbeat_with_active_lease_orphans_worker_until_remediation() {
        let mut manager = WorkerFleetManager::default();
        let policy =
            WorkerFleetPolicy { heartbeat_timeout_ms: 100, ..policy_for("tool:palyra.echo") };
        manager.register_worker(attestation("worker-m"), &policy, 2_000).unwrap();
        manager.assign_work("worker-m", lease_request("run-12", 500), &policy, 2_050).unwrap();

        let events = manager.mark_stale_heartbeat_workers(&policy, 2_250);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].state, WorkerLifecycleState::Orphaned);
        assert_eq!(events[0].run_id.as_deref(), Some("run-12"));
        assert_eq!(manager.snapshot().orphaned_workers, 1);
        assert_eq!(manager.snapshot().active_leases, 0);

        let heartbeat = manager
            .heartbeat_worker("worker-m", &policy, 2_260)
            .expect("orphaned worker heartbeat should be recorded without automatic reuse");
        assert_eq!(heartbeat.state, WorkerLifecycleState::Orphaned);
        let error = manager
            .assign_work("worker-m", lease_request("run-12b", 500), &policy, 2_270)
            .expect_err("orphaned stale worker must not receive work before remediation");
        assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));

        let cleanup = manager
            .force_cleanup_worker(
                "worker-m",
                WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: true,
                    removed_logs: true,
                    failure_reason: None,
                },
                2_280,
            )
            .expect("orphan cleanup verification should be recorded");
        assert!(cleanup.cleanup_succeeded);
        assert_eq!(cleanup.event.state, WorkerLifecycleState::Orphaned);
        assert_eq!(cleanup.event.reason_code, "worker.cleanup_verified_requires_reattestation");
        let error = manager
            .assign_work("worker-m", lease_request("run-12c", 500), &policy, 2_290)
            .expect_err("orphan cleanup verification alone must not make worker assignable");
        assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));
    }

    #[test]
    fn stale_idle_worker_can_recover_with_fresh_heartbeat() {
        let mut manager = WorkerFleetManager::default();
        let policy =
            WorkerFleetPolicy { heartbeat_timeout_ms: 100, ..policy_for("tool:palyra.echo") };
        manager.register_worker(attestation("worker-idle"), &policy, 2_000).unwrap();

        let events = manager.mark_stale_heartbeat_workers(&policy, 2_250);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].state, WorkerLifecycleState::Offline);
        assert_eq!(manager.snapshot().offline_workers, 1);

        let heartbeat = manager
            .heartbeat_worker("worker-idle", &policy, 2_260)
            .expect("idle offline worker should accept a fresh heartbeat");
        assert_eq!(heartbeat.state, WorkerLifecycleState::Registered);
        manager
            .assign_work("worker-idle", lease_request("run-idle", 500), &policy, 2_270)
            .expect("fresh idle worker should be reusable");
    }

    #[test]
    fn draining_worker_rejects_new_leases_without_quarantine() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-n"), &policy, 2_000).unwrap();

        let drain = manager
            .drain_worker("worker-n", "worker.operator.drain", 2_100)
            .expect("drain should be recorded");
        assert_eq!(drain.state, WorkerLifecycleState::Draining);

        let error = manager
            .assign_work("worker-n", lease_request("run-13", 500), &policy, 2_200)
            .expect_err("draining worker must not accept a new lease");
        assert!(matches!(error, WorkerLifecycleError::WorkerDraining(_)));
        assert_eq!(manager.snapshot().draining_workers, 1);
        assert_eq!(manager.snapshot().failed_closed_workers, 0);
    }

    #[test]
    fn compatibility_matrix_rejects_unversioned_worker_abi() {
        let mut manager = WorkerFleetManager::default();
        let policy = WorkerFleetPolicy {
            required_sdk_protocol_version: Some(2),
            required_wit_abi_version: Some("palyra-worker-abi/v2".to_owned()),
            ..WorkerFleetPolicy::default()
        };

        let error = manager
            .register_worker(attestation("worker-o"), &policy, 2_000)
            .expect_err("worker ABI mismatch must fail closed");

        assert!(matches!(error, WorkerLifecycleError::CompatibilityMismatch(_)));
        assert_eq!(manager.snapshot().registered_workers, 0);
    }
}
