//! Fail-closed worker fleet contracts: attestation, leases, lifecycle, and cleanup.
//!
//! [`WorkerFleetManager`] is the in-memory ledger the daemon drives: workers must
//! present a valid [`WorkerAttestation`] to register, every lease is bounded by
//! [`WorkerFleetPolicy`], and quarantined or orphaned workers stay unassignable
//! until they re-register with a fresh attestation.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use palyra_common::qa_fault_injection::{
    QaFaultAction, QaFaultActivationDirective, QaFaultCheckpoint, QaFaultDirective,
    QaFaultProbeError, QaFaultProbeHandle, QaFaultRecoveryClass,
};
/// Canonical worker lifecycle states, re-exported from `palyra-common` runtime contracts.
pub use palyra_common::runtime_contracts::{RuntimeGeneration, WorkerLifecycleState};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

pub mod computer_use;
pub mod network_runtime;
pub mod remote_protocol;
pub mod transport_adapters;

const MAX_WORKER_ID_BYTES: usize = 128;
const MAX_GRANT_ID_BYTES: usize = 128;
const MAX_RECENT_LIFECYCLE_EVENTS: usize = 64;
const NETWORKED_WORKER_EXPIRY_EVENT_ID_PREFIX: &str = "worker-expiry:";
const NETWORKED_WORKER_LIFECYCLE_EVENT_ID_PREFIX: &str = "worker-lifecycle:";
const DEFAULT_WORKER_SDK_PROTOCOL_VERSION: u32 = 1;
const DEFAULT_WORKER_WIT_ABI_VERSION: &str = "palyra-worker-abi/v1";
const MAX_REMOTE_WORKSPACE_ENTRIES: usize = 128;
const MAX_REMOTE_WORKSPACE_BYTES: usize = 384 * 1_024;
const MAX_REMOTE_PROCESS_EXECUTABLES: usize = 32;
const MAX_REMOTE_PROCESS_EXECUTABLE_BYTES: usize = 256;

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

impl WorkerLease {
    /// Returns the immutable identity a completion must present for this lease.
    #[must_use]
    pub fn identity(&self) -> WorkerLeaseIdentity {
        WorkerLeaseIdentity { lease_id: self.lease_id.clone(), run_id: self.run_id.clone() }
    }
}

/// Stable lease identity required when reporting run completion.
///
/// Both fields are checked against the worker's active lease before cleanup can
/// mutate lifecycle state. This prevents a delayed completion from an older run
/// from clearing a newer lease assigned to the same worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLeaseIdentity {
    pub lease_id: String,
    pub run_id: String,
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
pub const WORKER_REMOTE_TOOL_PROTOCOL: &str = "palyra-worker-rpc/v2";
/// Schema version for [`WorkerRemoteToolRequestEnvelope`] and
/// [`WorkerRemoteToolResultEnvelope`].
pub const WORKER_REMOTE_TOOL_SCHEMA_VERSION: u32 = 2;

/// Canonical capability required for attended computer-use execution.
pub const WORKER_REMOTE_COMPUTER_USE_CAPABILITY: &str = "tool:palyra.computer.use";

/// Capability strings trusted for the initial networked-worker remote tool subset.
pub const WORKER_REMOTE_TOOL_CAPABILITIES: &[&str] = &[
    "tool:palyra.fs.read_file",
    "tool:palyra.fs.list_dir",
    "tool:palyra.fs.search",
    "tool:palyra.process.run",
    "tool:palyra.fs.apply_patch",
    "tool:palyra.artifact.read",
    "tool:palyra.tool_program.run",
    WORKER_REMOTE_COMPUTER_USE_CAPABILITY,
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
    ComputerUse,
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
            Self::ComputerUse => "computer_use",
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
            "palyra.computer.use" => Some(Self::ComputerUse),
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
            Self::ComputerUse => "palyra.computer.use",
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

/// Kind of one bounded entry in a content-addressed scoped workspace bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRemoteWorkspaceEntryKind {
    /// Regular file whose exact bytes are carried by the bundle.
    File,
    /// Regular file represented only by listing-safe metadata.
    MetadataOnlyFile,
    /// Directory required to preserve the scoped tree shape.
    Directory,
}

/// One workspace-relative entry transferred to an isolated remote worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerRemoteWorkspaceEntry {
    /// Slash-separated path relative to the leased workspace root.
    pub path: String,
    /// Entry kind used during materialization.
    pub kind: WorkerRemoteWorkspaceEntryKind,
    /// SHA-256 of file bytes, metadata size, or an empty byte slice for directories.
    pub sha256: String,
    /// Original file size for metadata-only directory listings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_size_bytes: Option<u64>,
    /// Exact bounded file bytes. Metadata-only entries and directories must be empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bytes: Vec<u8>,
}

/// Integrity metadata for workspace material made visible to a remote worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRemoteWorkspaceTransfer {
    pub mode: WorkerRemoteWorkspaceTransferMode,
    pub workspace_manifest_sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scoped_bundle_sha256: Option<String>,
    /// Bounded content-addressed entries for a scoped-bundle transfer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scoped_entries: Vec<WorkerRemoteWorkspaceEntry>,
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
            scoped_entries: Vec::new(),
            writeback_mode: "patch_bundle".to_owned(),
        }
    }

    /// Builds a bounded content-addressed transfer for a portable worker workspace.
    ///
    /// # Errors
    /// Returns a typed contract error when an entry path escapes the scope, a
    /// digest is invalid, metadata-only/directory bytes are present, or aggregate limits are
    /// exceeded.
    pub fn scoped(
        workspace_manifest_sha256: String,
        scoped_entries: Vec<WorkerRemoteWorkspaceEntry>,
    ) -> Result<Self, WorkerRemoteToolContractError> {
        let mut transfer = Self {
            mode: WorkerRemoteWorkspaceTransferMode::ScopedBundle,
            workspace_manifest_sha256,
            scoped_bundle_sha256: None,
            scoped_entries,
            writeback_mode: "patch_bundle".to_owned(),
        };
        transfer.scoped_bundle_sha256 = Some(transfer.canonical_bundle_sha256()?);
        Ok(transfer)
    }

    /// Validates entry integrity and returns the canonical scoped-bundle digest.
    ///
    /// # Errors
    /// Returns a typed contract error for malformed paths, digests, duplicate
    /// entries, directory payloads, or aggregate resource-limit violations.
    pub fn canonical_bundle_sha256(&self) -> Result<String, WorkerRemoteToolContractError> {
        use std::path::{Component, Path};

        if self.scoped_entries.len() > MAX_REMOTE_WORKSPACE_ENTRIES {
            return Err(WorkerRemoteToolContractError::WorkspaceBundleLimitExceeded);
        }
        let mut total_bytes = 0_usize;
        let mut previous_path: Option<&str> = None;
        for entry in &self.scoped_entries {
            let path = Path::new(entry.path.as_str());
            if entry.path.trim().is_empty()
                || entry.path.contains('\\')
                || entry
                    .path
                    .split('/')
                    .any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
                || path.is_absolute()
                || path.components().any(|component| {
                    matches!(
                        component,
                        Component::ParentDir | Component::RootDir | Component::Prefix(_)
                    )
                })
            {
                return Err(WorkerRemoteToolContractError::WorkspaceEntryPathInvalid {
                    path: entry.path.clone(),
                });
            }
            if previous_path.is_some_and(|previous| previous >= entry.path.as_str()) {
                return Err(WorkerRemoteToolContractError::WorkspaceEntriesNotCanonical);
            }
            previous_path = Some(entry.path.as_str());
            validate_sha256_hex(entry.sha256.as_str(), "workspace_entry_sha256")?;
            match entry.kind {
                WorkerRemoteWorkspaceEntryKind::File => {
                    if entry.source_size_bytes.is_some()
                        || entry.sha256 != sha256_hex(entry.bytes.as_slice())
                    {
                        return Err(WorkerRemoteToolContractError::WorkspaceEntryDigestMismatch {
                            path: entry.path.clone(),
                        });
                    }
                }
                WorkerRemoteWorkspaceEntryKind::MetadataOnlyFile => {
                    let Some(source_size_bytes) = entry.source_size_bytes else {
                        return Err(WorkerRemoteToolContractError::WorkspaceEntryDigestMismatch {
                            path: entry.path.clone(),
                        });
                    };
                    if !entry.bytes.is_empty()
                        || entry.sha256 != sha256_hex(source_size_bytes.to_be_bytes().as_slice())
                    {
                        return Err(WorkerRemoteToolContractError::WorkspaceEntryDigestMismatch {
                            path: entry.path.clone(),
                        });
                    }
                }
                WorkerRemoteWorkspaceEntryKind::Directory => {
                    if entry.source_size_bytes.is_some()
                        || !entry.bytes.is_empty()
                        || entry.sha256 != sha256_hex(&[])
                    {
                        return Err(WorkerRemoteToolContractError::WorkspaceEntryDigestMismatch {
                            path: entry.path.clone(),
                        });
                    }
                }
            }
            total_bytes = total_bytes
                .checked_add(entry.bytes.len())
                .ok_or(WorkerRemoteToolContractError::WorkspaceBundleLimitExceeded)?;
            if total_bytes > MAX_REMOTE_WORKSPACE_BYTES {
                return Err(WorkerRemoteToolContractError::WorkspaceBundleLimitExceeded);
            }
        }
        serde_json::to_vec(&self.scoped_entries)
            .map(|bytes| sha256_hex(bytes.as_slice()))
            .map_err(|_| WorkerRemoteToolContractError::WorkspaceEntriesNotCanonical)
    }
}

/// Lease fields copied into the worker RPC request so the worker can verify its grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRemoteLeaseBinding {
    pub lease_id: String,
    pub worker_id: String,
    pub session_id: String,
    pub run_id: String,
    pub run_generation: RuntimeGeneration,
    pub grant_id: String,
    pub grant_tool_name: String,
    /// Host-observed issuance time used by the canonical task clock fence.
    pub issued_at_unix_ms: i64,
    pub expires_at_unix_ms: i64,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// Exact host-policy executable tokens admitted for `palyra.process.run`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub process_executable_allowlist: Vec<String>,
    /// Exact WorkGraph claim authority, if this task was claim-dispatched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_graph_claim: Option<remote_protocol::RemoteWorkGraphClaimBinding>,
    /// Typed posture prevents an absent claim from being interpreted implicitly.
    #[serde(default)]
    pub work_graph_posture: remote_protocol::RemoteWorkGraphTaskPosture,
    pub workspace_scope: WorkerWorkspaceScope,
    pub artifact_transport: WorkerArtifactTransport,
}

impl WorkerRemoteLeaseBinding {
    /// Copies an assigned lease together with the host-issued run generation.
    #[must_use]
    pub fn from_lease(
        lease: &WorkerLease,
        session_id: String,
        run_generation: RuntimeGeneration,
        issued_at_unix_ms: i64,
    ) -> Self {
        Self {
            lease_id: lease.lease_id.clone(),
            worker_id: lease.worker_id.clone(),
            session_id,
            run_id: lease.run_id.clone(),
            run_generation,
            grant_id: lease.grant.grant_id.clone(),
            grant_tool_name: lease.grant.tool_name.clone(),
            issued_at_unix_ms,
            expires_at_unix_ms: lease.expires_at_unix_ms,
            required_capabilities: lease.required_capabilities.clone(),
            process_executable_allowlist: Vec::new(),
            // A normal worker lease has no WorkClaimAuthority. Claim-aware callers
            // must bind the exact host claim instead of deriving it from run IDs.
            work_graph_claim: None,
            work_graph_posture: remote_protocol::RemoteWorkGraphTaskPosture::DirectToolDispatch,
            workspace_scope: lease.workspace_scope.clone(),
            artifact_transport: lease.artifact_transport.clone(),
        }
    }

    /// Replaces the direct-dispatch posture with exact host claim authority.
    #[must_use]
    pub fn with_work_graph_claim(
        mut self,
        claim: remote_protocol::RemoteWorkGraphClaimBinding,
    ) -> Self {
        self.work_graph_claim = Some(claim);
        self.work_graph_posture = remote_protocol::RemoteWorkGraphTaskPosture::Claimed;
        self
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
    /// Optional short-lived ciphertext addressed and authenticated by its descriptor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypted_secret_artifact: Option<remote_protocol::EncryptedWorkerSecretArtifact>,
    /// Additive canonical protocol binding shared by every production adapter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_protocol: Option<remote_protocol::RemoteWorkerProtocolV1>,
}

impl WorkerRemoteToolRequestEnvelope {
    /// Validates protocol, tool-kind, manifest, identity, and lease invariants.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerRemoteToolContractError`] when the envelope is malformed,
    /// expired, unsupported, or not bound to the requested worker lease.
    pub fn validate(&self, now_unix_ms: i64) -> Result<(), WorkerRemoteToolContractError> {
        self.validate_contract()?;
        if self.lease.expires_at_unix_ms <= now_unix_ms {
            return Err(WorkerRemoteToolContractError::LeaseExpired {
                lease_id: self.lease.lease_id.clone(),
                expires_at_unix_ms: self.lease.expires_at_unix_ms,
                observed_at_unix_ms: now_unix_ms,
            });
        }
        if let Some(canonical) = self.canonical_protocol.as_ref() {
            canonical.validate(now_unix_ms).map_err(|error| {
                WorkerRemoteToolContractError::CanonicalProtocol { reason: error.to_string() }
            })?;
            let expected = remote_protocol::RemoteWorkerProtocolV1::from_remote_request(self);
            if canonical != &expected {
                return Err(WorkerRemoteToolContractError::CanonicalProtocol {
                    reason: "canonical task does not match the established RPC envelope".to_owned(),
                });
            }
        }
        Ok(())
    }

    fn validate_contract(&self) -> Result<(), WorkerRemoteToolContractError> {
        validate_protocol(self.protocol.as_str(), self.schema_version)?;
        validate_required_string(self.request_id.as_str(), "request_id")?;
        validate_required_string(self.proposal_id.as_str(), "proposal_id")?;
        validate_required_string(self.tool_name.as_str(), "tool_name")?;
        validate_required_string(self.input_json.as_str(), "input_json")?;
        validate_required_string(self.lease.lease_id.as_str(), "lease_id")?;
        validate_required_string(self.lease.worker_id.as_str(), "worker_id")?;
        validate_required_string(self.lease.session_id.as_str(), "session_id")?;
        validate_required_string(self.lease.run_id.as_str(), "run_id")?;
        validate_sha256_hex(self.input_json_sha256.as_str(), "input_json_sha256")?;
        if sha256_hex(self.input_json.as_bytes()) != self.input_json_sha256 {
            return Err(WorkerRemoteToolContractError::DigestMismatch {
                field: "input_json_sha256",
            });
        }
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
        match self.workspace_transfer.mode {
            WorkerRemoteWorkspaceTransferMode::Manifest => {
                if !self.workspace_transfer.scoped_entries.is_empty() {
                    return Err(WorkerRemoteToolContractError::UnexpectedScopedBundleEntries);
                }
            }
            WorkerRemoteWorkspaceTransferMode::ScopedBundle => {
                let observed = self.workspace_transfer.canonical_bundle_sha256()?;
                if self.workspace_transfer.scoped_bundle_sha256.as_deref()
                    != Some(observed.as_str())
                {
                    return Err(WorkerRemoteToolContractError::DigestMismatch {
                        field: "scoped_bundle_sha256",
                    });
                }
                if self.lease.workspace_scope.allowed_paths.is_empty()
                    || self.lease.workspace_scope.allowed_paths.len() > MAX_REMOTE_WORKSPACE_ENTRIES
                {
                    return Err(WorkerRemoteToolContractError::WorkspaceScopeInvalid);
                }
                for entry in &self.workspace_transfer.scoped_entries {
                    if !workspace_entry_is_allowed(
                        entry.path.as_str(),
                        self.lease.workspace_scope.allowed_paths.as_slice(),
                    )? {
                        return Err(
                            WorkerRemoteToolContractError::WorkspaceEntryOutsideLeaseScope {
                                path: entry.path.clone(),
                            },
                        );
                    }
                }
            }
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
        let workspace_entry_kind_is_invalid =
            self.workspace_transfer.scoped_entries.iter().any(|entry| match expected_kind {
                WorkerRemoteToolKind::FsList => {
                    matches!(entry.kind, WorkerRemoteWorkspaceEntryKind::File)
                }
                _ => matches!(entry.kind, WorkerRemoteWorkspaceEntryKind::MetadataOnlyFile),
            });
        if workspace_entry_kind_is_invalid {
            return Err(WorkerRemoteToolContractError::WorkspaceEntryKindNotAllowed {
                tool_name: self.tool_name.clone(),
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
        validate_process_executable_authority(
            self.tool_kind,
            self.lease.process_executable_allowlist.as_slice(),
        )?;
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
    /// Host-issued run generation copied from the request lease binding.
    pub run_generation: RuntimeGeneration,
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
    /// Host receipt time is the lease authority boundary. The worker-reported completion timestamp
    /// remains audit metadata and cannot make a result observed after expiry settle successfully.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerRemoteToolContractError`] when the worker changed identity,
    /// the lease expired before host observation, cleanup was incomplete, a digest
    /// does not match its payload, or result fields do not match the request binding.
    pub fn validate_against_request(
        &self,
        request: &WorkerRemoteToolRequestEnvelope,
        observed_at_unix_ms: i64,
    ) -> Result<(), WorkerRemoteToolContractError> {
        request.validate(observed_at_unix_ms)?;
        self.validate_result_contract(request)
    }

    /// Validates the result and derives the host-side receipt digest used for durable settlement.
    ///
    /// # Errors
    ///
    /// Returns the same contract errors as [`Self::validate_against_request`], including malformed
    /// digest fields or a result that is not bound to the exact generation-bearing request.
    pub fn validated_receipt_sha256(
        &self,
        request: &WorkerRemoteToolRequestEnvelope,
        observed_at_unix_ms: i64,
    ) -> Result<String, WorkerRemoteToolContractError> {
        self.validate_against_request(request, observed_at_unix_ms)?;
        let request_sha256 =
            decode_sha256(request.input_json_sha256.as_str(), "input_json_sha256")?;
        let output_json_sha256 =
            decode_sha256(self.output_json_sha256.as_str(), "output_json_sha256")?;
        let output_manifest_sha256 =
            decode_sha256(self.output_manifest_sha256.as_str(), "output_manifest_sha256")?;

        let mut hasher = sha2::Sha256::new();
        use sha2::Digest as _;
        hasher.update(b"palyra.networked_worker.validated_result.v1\0");
        update_length_prefixed(&mut hasher, request.request_id.as_bytes());
        hasher.update(request_sha256);
        hasher.update([u8::from(self.success)]);
        hasher.update(output_json_sha256);
        hasher.update(output_manifest_sha256);
        hasher.update(self.run_generation.get().to_be_bytes());
        match self.error.as_deref() {
            Some(error) => {
                hasher.update([1]);
                update_length_prefixed(&mut hasher, error.as_bytes());
            }
            None => hasher.update([0]),
        }
        hasher.update(self.completed_at_unix_ms.to_be_bytes());
        hasher.update(b"cleanup_verified");
        Ok(hex::encode(hasher.finalize()))
    }

    fn validate_result_contract(
        &self,
        request: &WorkerRemoteToolRequestEnvelope,
    ) -> Result<(), WorkerRemoteToolContractError> {
        request.validate_contract()?;
        validate_protocol(self.protocol.as_str(), self.schema_version)?;
        validate_sha256_hex(self.output_json_sha256.as_str(), "output_json_sha256")?;
        validate_sha256_hex(self.output_manifest_sha256.as_str(), "output_manifest_sha256")?;
        if sha256_hex(self.output_json.as_bytes()) != self.output_json_sha256 {
            return Err(WorkerRemoteToolContractError::DigestMismatch {
                field: "output_json_sha256",
            });
        }
        if self.request_id != request.request_id
            || self.proposal_id != request.proposal_id
            || self.tool_name != request.tool_name
            || self.tool_kind != request.tool_kind
            || self.lease_id != request.lease.lease_id
            || self.run_generation != request.lease.run_generation
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
        if self.completed_at_unix_ms < 0 {
            return Err(WorkerRemoteToolContractError::InvalidCompletionTimestamp);
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
    #[error("worker remote tool envelope SHA-256 digest does not match '{field}' payload")]
    DigestMismatch { field: &'static str },
    #[error("worker remote scoped bundle transfer requires scoped_bundle_sha256")]
    MissingScopedBundleDigest,
    #[error("worker remote manifest transfer cannot carry scoped workspace entries")]
    UnexpectedScopedBundleEntries,
    #[error("worker remote scoped workspace entry path is invalid: {path}")]
    WorkspaceEntryPathInvalid { path: String },
    #[error("worker remote scoped workspace entries are not sorted and unique")]
    WorkspaceEntriesNotCanonical,
    #[error("worker remote scoped workspace entry digest mismatch: {path}")]
    WorkspaceEntryDigestMismatch { path: String },
    #[error("worker remote scoped workspace entry kind is not allowed for tool '{tool_name}'")]
    WorkspaceEntryKindNotAllowed { tool_name: String },
    #[error("worker remote scoped workspace bundle exceeds its entry or byte limit")]
    WorkspaceBundleLimitExceeded,
    #[error("worker remote workspace lease allowlist is missing or invalid")]
    WorkspaceScopeInvalid,
    #[error("worker remote workspace entry is outside its lease allowlist: {path}")]
    WorkspaceEntryOutsideLeaseScope { path: String },
    #[error("worker remote tool kind mismatch for {tool_name}: expected {expected}, got {actual}")]
    ToolKindMismatch { tool_name: String, expected: &'static str, actual: &'static str },
    #[error("worker remote lease tool mismatch: expected {expected}, got {actual}")]
    LeaseToolMismatch { expected: String, actual: String },
    #[error("worker remote lease missing capability '{capability}'")]
    MissingRequiredCapability { capability: String },
    #[error("worker remote process executable authority is missing or ambiguous")]
    ProcessExecutableAuthorityInvalid,
    #[error("worker remote lease '{lease_id}' expired at {expires_at_unix_ms}; observed at {observed_at_unix_ms}")]
    LeaseExpired { lease_id: String, expires_at_unix_ms: i64, observed_at_unix_ms: i64 },
    #[error("worker remote identity mismatch: expected {expected}, got {actual}")]
    WorkerIdentityMismatch { expected: String, actual: String },
    #[error("worker remote result does not match the request binding")]
    ResultBindingMismatch,
    #[error("worker remote result has an invalid completion timestamp")]
    InvalidCompletionTimestamp,
    #[error("worker remote cleanup gap for lease '{lease_id}': {reason}")]
    CleanupGap { lease_id: String, reason: String },
    #[error("worker remote canonical protocol validation failed: {reason}")]
    CanonicalProtocol { reason: String },
}

fn validate_process_executable_authority(
    tool_kind: WorkerRemoteToolKind,
    allowlist: &[String],
) -> Result<(), WorkerRemoteToolContractError> {
    if !matches!(tool_kind, WorkerRemoteToolKind::ProcessRun) {
        return if allowlist.is_empty() {
            Ok(())
        } else {
            Err(WorkerRemoteToolContractError::ProcessExecutableAuthorityInvalid)
        };
    }
    if allowlist.is_empty() || allowlist.len() > MAX_REMOTE_PROCESS_EXECUTABLES {
        return Err(WorkerRemoteToolContractError::ProcessExecutableAuthorityInvalid);
    }
    let mut previous: Option<&str> = None;
    for executable in allowlist {
        if !remote_process_executable_is_unambiguous(executable)
            || previous.is_some_and(|value| value >= executable.as_str())
        {
            return Err(WorkerRemoteToolContractError::ProcessExecutableAuthorityInvalid);
        }
        previous = Some(executable.as_str());
    }
    Ok(())
}

fn remote_process_executable_is_unambiguous(executable: &str) -> bool {
    if executable.is_empty()
        || executable.len() > MAX_REMOTE_PROCESS_EXECUTABLE_BYTES
        || executable.trim() != executable
        || executable.contains('\\')
        || executable.bytes().any(|byte| {
            byte.is_ascii_whitespace() || byte == b'\0' || b"*;&|><`$'\"()".contains(&byte)
        })
    {
        return false;
    }
    if let Some(absolute) = executable.strip_prefix('/') {
        return !absolute.is_empty()
            && absolute
                .split('/')
                .all(|segment| !segment.is_empty() && !matches!(segment, "." | ".."));
    }
    !executable.contains('/')
        && executable
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-'))
}

fn workspace_entry_is_allowed(
    entry_path: &str,
    allowed_paths: &[String],
) -> Result<bool, WorkerRemoteToolContractError> {
    let entry = portable_workspace_segments(entry_path)?;
    for raw in allowed_paths {
        let allowed = portable_workspace_segments(raw.as_str())?;
        if allowed.is_empty()
            || entry == allowed
            || (entry.len() > allowed.len() && entry.starts_with(allowed.as_slice()))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn portable_workspace_segments(raw: &str) -> Result<Vec<String>, WorkerRemoteToolContractError> {
    if raw.contains('\\') {
        return Err(WorkerRemoteToolContractError::WorkspaceScopeInvalid);
    }
    let trimmed = raw.trim().trim_matches('/');
    let normalized = trimmed.strip_prefix("workspace/").unwrap_or(trimmed);
    if normalized.is_empty() || matches!(normalized, "." | "workspace") {
        return Ok(Vec::new());
    }
    let segments = normalized.split('/').map(str::to_owned).collect::<Vec<_>>();
    if segments.iter().any(|segment| segment.is_empty() || matches!(segment.as_str(), "." | "..")) {
        return Err(WorkerRemoteToolContractError::WorkspaceScopeInvalid);
    }
    Ok(segments)
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

fn decode_sha256(
    value: &str,
    field: &'static str,
) -> Result<[u8; 32], WorkerRemoteToolContractError> {
    validate_sha256_hex(value, field)?;
    let bytes = hex::decode(value)
        .map_err(|_| WorkerRemoteToolContractError::InvalidSha256Digest { field })?;
    bytes.try_into().map_err(|_| WorkerRemoteToolContractError::InvalidSha256Digest { field })
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest as _;

    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Audit record of a single worker lifecycle transition.
///
/// `reason_code` is a stable machine-readable code (for example `worker.registered`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLifecycleEvent {
    pub worker_id: String,
    pub state: WorkerLifecycleState,
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    pub reason_code: String,
    pub timestamp_unix_ms: i64,
}

/// Returns whether `event` is valid exact evidence for one TTL-expired worker lease.
#[must_use]
pub fn is_exact_networked_worker_expiry_event(event: &WorkerLifecycleEvent) -> bool {
    event.state == WorkerLifecycleState::Orphaned
        && event.reason_code == "worker.ttl_expired"
        && event.timestamp_unix_ms >= 0
        && is_bounded_worker_id(event.worker_id.as_str())
        && event.run_id.as_deref().is_some_and(is_bounded_runtime_identity)
        && event.lease_id.as_deref().is_some_and(is_bounded_runtime_identity)
}

/// Derives the deterministic journal/outbox identity for exact TTL-expiry evidence.
///
/// # Errors
/// Returns [`WorkerLifecycleError::InvalidExpiryEvidence`] when the event does not carry an exact
/// bounded worker, run, and lease identity for a TTL-expiry transition.
pub fn networked_worker_expiry_event_id(
    event: &WorkerLifecycleEvent,
) -> Result<String, WorkerLifecycleError> {
    if !is_exact_networked_worker_expiry_event(event) {
        return Err(WorkerLifecycleError::InvalidExpiryEvidence);
    }
    let run_id = event.run_id.as_deref().ok_or(WorkerLifecycleError::InvalidExpiryEvidence)?;
    let lease_id = event.lease_id.as_deref().ok_or(WorkerLifecycleError::InvalidExpiryEvidence)?;
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest as _;
    hasher.update(b"palyra.networked_worker.expiry_event.v2\0");
    hasher.update(event.worker_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(run_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(lease_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(event.reason_code.as_bytes());
    hasher.update(b"\0");
    hasher.update(event.timestamp_unix_ms.to_be_bytes());
    let digest = hex::encode(hasher.finalize());
    Ok(format!("{NETWORKED_WORKER_EXPIRY_EVENT_ID_PREFIX}{}", &digest[..32]))
}

/// Derives a transition-bound identity for non-expiry worker lifecycle evidence.
///
/// # Errors
/// Returns [`WorkerLifecycleError::InvalidLifecycleEvidence`] when the transition or event
/// contains an invalid bounded identity, reason code, or timestamp.
pub fn networked_worker_lifecycle_event_id(
    transition_id: &str,
    event: &WorkerLifecycleEvent,
) -> Result<String, WorkerLifecycleError> {
    if !is_bounded_runtime_identity(transition_id)
        || !is_bounded_worker_id(event.worker_id.as_str())
        || event.reason_code.is_empty()
        || event.reason_code.len() > 128
        || !event
            .reason_code
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
        || event.timestamp_unix_ms < 0
        || event.run_id.as_deref().is_some_and(|value| !is_bounded_runtime_identity(value))
        || event.lease_id.as_deref().is_some_and(|value| !is_bounded_runtime_identity(value))
    {
        return Err(WorkerLifecycleError::InvalidLifecycleEvidence);
    }

    let mut hasher = sha2::Sha256::new();
    use sha2::Digest as _;
    hasher.update(b"palyra.networked_worker.lifecycle_event.v1\0");
    update_length_prefixed(&mut hasher, transition_id.as_bytes());
    update_length_prefixed(&mut hasher, event.worker_id.as_bytes());
    update_length_prefixed(&mut hasher, event.state.as_str().as_bytes());
    update_optional_length_prefixed(&mut hasher, event.run_id.as_deref());
    update_optional_length_prefixed(&mut hasher, event.lease_id.as_deref());
    update_length_prefixed(&mut hasher, event.reason_code.as_bytes());
    hasher.update(event.timestamp_unix_ms.to_be_bytes());
    let digest = hex::encode(hasher.finalize());
    Ok(format!("{NETWORKED_WORKER_LIFECYCLE_EVENT_ID_PREFIX}{}", &digest[..32]))
}

fn update_length_prefixed(hasher: &mut sha2::Sha256, value: &[u8]) {
    use sha2::Digest as _;
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn update_optional_length_prefixed(hasher: &mut sha2::Sha256, value: Option<&str>) {
    use sha2::Digest as _;
    match value {
        Some(value) => {
            hasher.update([1]);
            update_length_prefixed(hasher, value.as_bytes());
        }
        None => hasher.update([0]),
    }
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
    #[error("worker '{0}' cleanup requires the active lease to be revoked first")]
    CleanupRequiresLeaseRevocation(String),
    #[error("worker '{worker_id}' completion does not match its active lease")]
    StaleLeaseCompletion { worker_id: String, completed_lease_id: String, completed_run_id: String },
    #[error("worker '{worker_id}' expiry plan no longer matches lease '{lease_id}'")]
    ExpiryPlanConflict { worker_id: String, lease_id: String },
    #[error("networked worker expiry evidence is invalid")]
    InvalidExpiryEvidence,
    #[error("networked worker lifecycle evidence is invalid")]
    InvalidLifecycleEvidence,
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
    #[error(transparent)]
    QaFaultProbe(#[from] QaFaultProbeError),
    #[error(
        "QA fault activation '{activation_id}' requested action {action:?} at '{point_id}' for actor '{actor}'"
    )]
    QaFaultActivated {
        activation_id: String,
        point_id: String,
        actor: String,
        action: QaFaultAction,
    },
}

/// Durable worker record used to reconstruct active lease authority after restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerFleetRecord {
    pub attestation: WorkerAttestation,
    pub state: WorkerLifecycleState,
    pub lease: Option<WorkerLease>,
    pub last_heartbeat_unix_ms: i64,
}

#[derive(Debug)]
struct StaleReclaimCandidate {
    worker_id: String,
    effective_now_unix_ms: i64,
    recovery_activation_id: Option<String>,
}

#[derive(Debug)]
struct StaleReclaimScan {
    candidates: Vec<StaleReclaimCandidate>,
    failed_closed_activation_ids: Vec<String>,
    reclaimed_activation_ids: Vec<String>,
}

/// Opaque two-phase lease-expiry plan whose events can be durably fenced before revocation.
#[derive(Debug)]
pub struct WorkerLeaseExpiryPlan {
    events: Vec<WorkerLifecycleEvent>,
    recovery_activation_ids: Vec<Option<String>>,
    failed_closed_activation_ids: Vec<String>,
    reclaimed_activation_ids: Vec<String>,
}

impl WorkerLeaseExpiryPlan {
    /// Returns the exact lease-bound lifecycle evidence that applying this plan will emit.
    #[must_use]
    pub fn events(&self) -> &[WorkerLifecycleEvent] {
        self.events.as_slice()
    }
}

/// In-memory fleet ledger enforcing fail-closed worker lifecycle transitions.
///
/// Every mutating operation revalidates attestation and policy compatibility before
/// granting work, and emits a [`WorkerLifecycleEvent`] into a bounded recent-event
/// buffer for audit surfaces. `Default` starts an empty fleet.
#[derive(Debug, Default, Clone)]
pub struct WorkerFleetManager {
    workers: BTreeMap<String, WorkerFleetRecord>,
    recent_events: VecDeque<WorkerLifecycleEvent>,
    qa_fault_probe: QaFaultProbeHandle,
}

impl WorkerFleetManager {
    /// Replaces the disabled probe with an explicit QA-only fault probe.
    ///
    /// This constructor is unavailable unless the non-default
    /// `qa-fault-injection` crate feature is enabled.
    #[cfg(feature = "qa-fault-injection")]
    #[must_use]
    pub fn with_qa_fault_probe(mut self, probe: QaFaultProbeHandle) -> Self {
        self.qa_fault_probe = probe;
        self
    }

    /// Records the recovery class observed by a QA harness after an injected fault.
    ///
    /// # Errors
    /// Returns a probe error when the activation was not observed or recovery
    /// evidence was already recorded.
    #[cfg(feature = "qa-fault-injection")]
    pub fn record_qa_fault_recovery(
        &self,
        activation_id: &str,
        recovery_class: QaFaultRecoveryClass,
    ) -> Result<(), WorkerLifecycleError> {
        self.qa_fault_probe.record_recovery(activation_id, recovery_class)?;
        Ok(())
    }

    /// Reconstructs a fleet from durable worker records.
    ///
    /// # Errors
    /// Returns a lifecycle error when a record key disagrees with its attestation, a lease is
    /// bound to another worker, or the record contains a state/lease combination that cannot own
    /// runtime authority.
    pub fn from_durable_records(
        records: BTreeMap<String, WorkerFleetRecord>,
    ) -> Result<Self, WorkerLifecycleError> {
        for (worker_id, record) in &records {
            validate_durable_worker_record(worker_id, record)?;
        }
        Ok(Self {
            workers: records,
            recent_events: VecDeque::new(),
            qa_fault_probe: QaFaultProbeHandle::default(),
        })
    }

    /// Returns exact worker records for durable crash recovery.
    #[must_use]
    pub fn durable_records(&self) -> BTreeMap<String, WorkerFleetRecord> {
        self.workers.clone()
    }

    /// Restores the bounded recent-event ring after a caller commits durable evidence.
    pub fn retain_recent_event(&mut self, event: WorkerLifecycleEvent) {
        self.push_recent_event(event);
    }

    /// Restores an exact durable snapshot while preserving process-local diagnostics.
    ///
    /// # Errors
    /// Returns a lifecycle error when the supplied records do not form a valid durable fleet.
    pub fn restore_durable_records(
        &mut self,
        records: BTreeMap<String, WorkerFleetRecord>,
    ) -> Result<(), WorkerLifecycleError> {
        for (worker_id, record) in &records {
            validate_durable_worker_record(worker_id, record)?;
        }
        self.workers = records;
        Ok(())
    }

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
            WorkerFleetRecord {
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
            lease_id: None,
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
        self.reject_activated_qa_fault("worker.claim.before_effect", request.run_id.as_str())?;
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
        self.assign_next_work_matching(request, policy, now_unix_ms, None)
    }

    /// Leases the first eligible worker whose id is present in `candidate_worker_ids`.
    ///
    /// Transport owners use this after a read-only compatibility preflight so an incompatible
    /// endpoint cannot acquire durable fleet authority. Assignment still repeats every normal
    /// attestation, lifecycle, heartbeat, capability, and fault-injection gate.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLifecycleError::InvalidLeaseRequest`] or
    /// [`WorkerLifecycleError::TtlExceeded`] for a malformed request, and
    /// [`WorkerLifecycleError::NoAvailableWorker`] when no allowed worker passes every fleet gate.
    pub fn assign_next_work_from_candidates(
        &mut self,
        candidate_worker_ids: &BTreeSet<String>,
        request: WorkerLeaseRequest,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), WorkerLifecycleError> {
        self.assign_next_work_matching(request, policy, now_unix_ms, Some(candidate_worker_ids))
    }

    fn assign_next_work_matching(
        &mut self,
        request: WorkerLeaseRequest,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
        candidate_worker_ids: Option<&BTreeSet<String>>,
    ) -> Result<(WorkerLease, WorkerLifecycleEvent), WorkerLifecycleError> {
        validate_lease_request(&request, policy, now_unix_ms)?;
        // A released barrier participant must durably consume its release before
        // another participant's winning lease changes worker availability.
        self.reject_activated_qa_fault("worker.claim.before_effect", request.run_id.as_str())?;
        let Some(worker_id) = self.workers.iter().find_map(|(worker_id, worker)| {
            candidate_worker_ids
                .is_none_or(|candidates| candidates.contains(worker_id))
                .then_some(worker)
                .filter(|worker| worker_record_can_accept(worker, &request, policy, now_unix_ms))
                .map(|_| worker_id.clone())
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
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id,
    /// [`WorkerLifecycleError::StaleLeaseCompletion`] when `lease_identity` does
    /// not identify the active lease, and [`WorkerLifecycleError::CleanupFailed`]
    /// when the cleanup report is unverified; the worker is then left fail-closed.
    pub fn complete_work(
        &mut self,
        worker_id: &str,
        lease_identity: &WorkerLeaseIdentity,
        cleanup: &WorkerCleanupReport,
        now_unix_ms: i64,
    ) -> Result<WorkerLifecycleEvent, WorkerLifecycleError> {
        let outcome =
            self.finalize_work(worker_id, lease_identity, cleanup.clone(), now_unix_ms)?;
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
    /// Returns [`WorkerLifecycleError::UnknownWorker`] for an unregistered id or
    /// [`WorkerLifecycleError::StaleLeaseCompletion`] when `lease_identity` does
    /// not identify the active lease. Identity rejection leaves state unchanged.
    pub fn finalize_work(
        &mut self,
        worker_id: &str,
        lease_identity: &WorkerLeaseIdentity,
        cleanup: WorkerCleanupReport,
        now_unix_ms: i64,
    ) -> Result<WorkerCleanupOutcome, WorkerLifecycleError> {
        let outcome = {
            let worker = self
                .workers
                .get_mut(worker_id)
                .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
            validate_completion_identity(worker_id, worker, lease_identity, now_unix_ms)?;
            finalize_worker_cleanup(worker_id, worker, cleanup, now_unix_ms)
        };
        self.push_recent_event(outcome.event.clone());
        Ok(outcome)
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
        let (run_id, lease_id) = worker
            .lease
            .as_ref()
            .map(|lease| (Some(lease.run_id.clone()), Some(lease.lease_id.clone())))
            .unwrap_or((None, None));
        worker.state = WorkerLifecycleState::Failed;
        worker.lease = None;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Failed,
            run_id,
            lease_id,
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
            let (run_id, lease_id) = worker
                .lease
                .as_ref()
                .map(|lease| (Some(lease.run_id.clone()), Some(lease.lease_id.clone())))
                .unwrap_or((None, None));
            worker.state = WorkerLifecycleState::Failed;
            worker.lease = None;
            events.push(WorkerLifecycleEvent {
                worker_id: worker_id.clone(),
                state: WorkerLifecycleState::Failed,
                run_id,
                lease_id,
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
            lease_id: None,
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
        self.reject_activated_qa_fault("worker.heartbeat.before_effect", worker_id)?;
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
            lease_id: worker.lease.as_ref().map(|lease| lease.lease_id.clone()),
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
            lease_id: worker.lease.as_ref().map(|lease| lease.lease_id.clone()),
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
        let (run_id, lease_id) = worker
            .lease
            .as_ref()
            .map(|lease| (Some(lease.run_id.clone()), Some(lease.lease_id.clone())))
            .unwrap_or((None, None));
        worker.lease = None;
        worker.state = WorkerLifecycleState::Orphaned;
        let event = WorkerLifecycleEvent {
            worker_id: worker_id.to_owned(),
            state: WorkerLifecycleState::Orphaned,
            run_id,
            lease_id,
            reason_code: normalize_operator_reason_code(reason_code, "worker.lease_revoked"),
            timestamp_unix_ms: now_unix_ms,
        };
        self.push_recent_event(event.clone());
        Ok(event)
    }

    /// Operator entry point for recording a cleanup report during remediation.
    ///
    /// Unlike runtime completion, this privileged path does not require a lease identity, but it
    /// accepts only a worker whose active lease has already been revoked. This prevents operator
    /// cleanup from clearing dispatch authority that still belongs to an exact lease.
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
        let outcome = {
            let worker = self
                .workers
                .get_mut(worker_id)
                .ok_or_else(|| WorkerLifecycleError::UnknownWorker(worker_id.to_owned()))?;
            if worker.lease.is_some() {
                return Err(WorkerLifecycleError::CleanupRequiresLeaseRevocation(
                    worker_id.to_owned(),
                ));
            }
            finalize_worker_cleanup(worker_id, worker, cleanup, now_unix_ms)
        };
        self.push_recent_event(outcome.event.clone());
        Ok(outcome)
    }

    /// Orphans every worker whose lease ttl has elapsed, returning the emitted events.
    ///
    /// # Errors
    /// Returns a checkpoint or exact-plan validation error before an unrelated lease is mutated.
    pub fn reap_expired_workers(
        &mut self,
        now_unix_ms: i64,
    ) -> Result<Vec<WorkerLifecycleEvent>, WorkerLifecycleError> {
        self.reap_expired_workers_bounded(now_unix_ms, usize::MAX)
    }

    /// Orphans at most `limit` workers whose lease ttl has elapsed.
    ///
    /// # Errors
    /// Returns a checkpoint or exact-plan validation error before an unrelated lease is mutated.
    pub fn reap_expired_workers_bounded(
        &mut self,
        now_unix_ms: i64,
        limit: usize,
    ) -> Result<Vec<WorkerLifecycleEvent>, WorkerLifecycleError> {
        let plan = self.plan_expired_workers_bounded(now_unix_ms, limit)?;
        self.apply_expired_worker_plan(plan)
    }

    /// Plans at most `limit` exact lease expiries without mutating worker state.
    ///
    /// Callers that require crash-safe evidence can persist [`WorkerLeaseExpiryPlan::events`]
    /// before applying the returned plan.
    ///
    /// # Errors
    /// Returns a checkpoint error before any candidate worker is mutated.
    pub fn plan_expired_workers_bounded(
        &self,
        now_unix_ms: i64,
        limit: usize,
    ) -> Result<WorkerLeaseExpiryPlan, WorkerLifecycleError> {
        if limit == 0 {
            return Ok(WorkerLeaseExpiryPlan {
                events: Vec::new(),
                recovery_activation_ids: Vec::new(),
                failed_closed_activation_ids: Vec::new(),
                reclaimed_activation_ids: Vec::new(),
            });
        }
        let mut reclaim_scan = self.stale_reclaim_candidates(
            now_unix_ms,
            |worker, effective_now| {
                worker.lease.as_ref().is_some_and(|lease| lease.expires_at_unix_ms <= effective_now)
            },
            |worker| worker.state == WorkerLifecycleState::Orphaned && worker.lease.is_none(),
        )?;
        reclaim_scan.candidates.truncate(limit);
        let mut events = Vec::with_capacity(reclaim_scan.candidates.len());
        let mut recovery_activation_ids = Vec::with_capacity(reclaim_scan.candidates.len());
        for candidate in reclaim_scan.candidates {
            let worker = self
                .workers
                .get(candidate.worker_id.as_str())
                .ok_or_else(|| WorkerLifecycleError::UnknownWorker(candidate.worker_id.clone()))?;
            let lease = worker.lease.as_ref().ok_or_else(|| {
                WorkerLifecycleError::InvalidLeaseRequest(format!(
                    "worker '{}' lost its expiry lease while planning",
                    candidate.worker_id
                ))
            })?;
            events.push(WorkerLifecycleEvent {
                worker_id: candidate.worker_id,
                state: WorkerLifecycleState::Orphaned,
                run_id: Some(lease.run_id.clone()),
                lease_id: Some(lease.lease_id.clone()),
                reason_code: "worker.ttl_expired".to_owned(),
                timestamp_unix_ms: lease.expires_at_unix_ms,
            });
            recovery_activation_ids.push(candidate.recovery_activation_id);
        }
        Ok(WorkerLeaseExpiryPlan {
            events,
            recovery_activation_ids,
            failed_closed_activation_ids: reclaim_scan.failed_closed_activation_ids,
            reclaimed_activation_ids: reclaim_scan.reclaimed_activation_ids,
        })
    }

    /// Applies a previously planned exact set of lease expiries.
    ///
    /// # Errors
    /// Returns [`WorkerLifecycleError::ExpiryPlanConflict`] if any worker no longer holds the
    /// lease captured by the plan. Validation completes before the first mutation.
    pub fn apply_expired_worker_plan(
        &mut self,
        plan: WorkerLeaseExpiryPlan,
    ) -> Result<Vec<WorkerLifecycleEvent>, WorkerLifecycleError> {
        for event in &plan.events {
            let worker = self
                .workers
                .get(event.worker_id.as_str())
                .ok_or_else(|| WorkerLifecycleError::UnknownWorker(event.worker_id.clone()))?;
            let expected_lease_id = event.lease_id.as_deref().unwrap_or_default();
            let expected_run_id = event.run_id.as_deref().unwrap_or_default();
            let exact_lease = worker.lease.as_ref().is_some_and(|lease| {
                lease.lease_id == expected_lease_id && lease.run_id == expected_run_id
            });
            if !exact_lease {
                return Err(WorkerLifecycleError::ExpiryPlanConflict {
                    worker_id: event.worker_id.clone(),
                    lease_id: expected_lease_id.to_owned(),
                });
            }
        }

        let mut applied_events = Vec::with_capacity(plan.events.len());
        for (event, recovery_activation_id) in
            plan.events.into_iter().zip(plan.recovery_activation_ids)
        {
            let worker = self
                .workers
                .get_mut(event.worker_id.as_str())
                .ok_or_else(|| WorkerLifecycleError::UnknownWorker(event.worker_id.clone()))?;
            worker.state = WorkerLifecycleState::Orphaned;
            worker.lease = None;
            self.push_recent_event(event.clone());
            if let Some(activation_id) = recovery_activation_id {
                // `Reclaimed` is proof of the completed exact lease revocation.
                self.qa_fault_probe
                    .record_recovery(activation_id.as_str(), QaFaultRecoveryClass::Reclaimed)?;
            }
            applied_events.push(event);
        }
        for activation_id in plan.failed_closed_activation_ids {
            self.qa_fault_probe
                .record_recovery(activation_id.as_str(), QaFaultRecoveryClass::FailedClosed)?;
        }
        for activation_id in plan.reclaimed_activation_ids {
            self.qa_fault_probe
                .record_recovery(activation_id.as_str(), QaFaultRecoveryClass::Reclaimed)?;
        }
        Ok(applied_events)
    }

    /// Transitions workers with stale heartbeats, returning the emitted events.
    ///
    /// A stale worker holding a lease is orphaned (its lease is revoked); a stale idle
    /// worker goes `Offline` and may recover via [`Self::heartbeat_worker`].
    ///
    /// # Errors
    ///
    /// Returns a checkpoint error before any candidate worker is mutated. If
    /// recovery evidence cannot be recorded, the affected worker has already
    /// entered its fail-closed state and its lifecycle event is retained.
    pub fn mark_stale_heartbeat_workers(
        &mut self,
        policy: &WorkerFleetPolicy,
        now_unix_ms: i64,
    ) -> Result<Vec<WorkerLifecycleEvent>, WorkerLifecycleError> {
        let stale_scan = self.stale_reclaim_candidates(
            now_unix_ms,
            |worker, effective_now| {
                !matches!(
                    worker.state,
                    WorkerLifecycleState::Failed
                        | WorkerLifecycleState::Offline
                        | WorkerLifecycleState::Orphaned
                ) && !worker_heartbeat_is_fresh(worker, policy, effective_now)
            },
            |worker| {
                matches!(
                    worker.state,
                    WorkerLifecycleState::Offline | WorkerLifecycleState::Orphaned
                ) && worker.lease.is_none()
            },
        )?;
        let mut events = Vec::new();
        for candidate in stale_scan.candidates {
            let event = {
                let worker =
                    self.workers.get_mut(candidate.worker_id.as_str()).ok_or_else(|| {
                        WorkerLifecycleError::UnknownWorker(candidate.worker_id.clone())
                    })?;
                let (run_id, lease_id) = worker
                    .lease
                    .as_ref()
                    .map(|lease| (Some(lease.run_id.clone()), Some(lease.lease_id.clone())))
                    .unwrap_or((None, None));
                let next_state = if worker.lease.is_some() {
                    WorkerLifecycleState::Orphaned
                } else {
                    WorkerLifecycleState::Offline
                };
                worker.state = next_state;
                worker.lease = None;
                WorkerLifecycleEvent {
                    worker_id: candidate.worker_id,
                    state: next_state,
                    run_id,
                    lease_id,
                    reason_code: "worker.heartbeat_stale".to_owned(),
                    timestamp_unix_ms: candidate.effective_now_unix_ms,
                }
            };
            self.push_recent_event(event.clone());
            if let Some(activation_id) = candidate.recovery_activation_id {
                // `Reclaimed` is proof of the exact stale-state transition, so the
                // transition and its lifecycle event must exist before attestation.
                self.qa_fault_probe
                    .record_recovery(activation_id.as_str(), QaFaultRecoveryClass::Reclaimed)?;
            }
            events.push(event);
        }
        for activation_id in stale_scan.failed_closed_activation_ids {
            self.qa_fault_probe
                .record_recovery(activation_id.as_str(), QaFaultRecoveryClass::FailedClosed)?;
        }
        for activation_id in stale_scan.reclaimed_activation_ids {
            self.qa_fault_probe
                .record_recovery(activation_id.as_str(), QaFaultRecoveryClass::Reclaimed)?;
        }
        Ok(events)
    }

    fn reject_activated_qa_fault(
        &self,
        point_id: &str,
        actor: &str,
    ) -> Result<(), WorkerLifecycleError> {
        let directive = self.qa_fault_probe.checkpoint(QaFaultCheckpoint { point_id, actor })?;
        match directive {
            QaFaultDirective::Continue => Ok(()),
            QaFaultDirective::Activate(activation) => Err(qa_fault_activation_error(activation)),
        }
    }

    fn stale_reclaim_candidates(
        &self,
        now_unix_ms: i64,
        is_stale: impl Fn(&WorkerFleetRecord, i64) -> bool,
        is_reclaimed: impl Fn(&WorkerFleetRecord) -> bool,
    ) -> Result<StaleReclaimScan, WorkerLifecycleError> {
        const POINT_ID: &str = "worker.stale_reclaim.before_effect";
        let mut candidates = Vec::new();
        let mut failed_closed_recoveries = Vec::new();
        let mut reclaimed_recoveries = Vec::new();
        let baseline_candidates = self
            .workers
            .iter()
            .filter(|(_, worker)| is_stale(worker, now_unix_ms))
            .map(|(worker_id, _)| worker_id.clone())
            .collect::<Vec<_>>();
        let active_barrier = self.prepare_stale_reclaim_barrier(baseline_candidates.as_slice())?;

        for (worker_id, worker) in &self.workers {
            let directive = self
                .qa_fault_probe
                .checkpoint(QaFaultCheckpoint { point_id: POINT_ID, actor: worker_id.as_str() })?;
            let (effective_now, activation_id) = match directive {
                QaFaultDirective::Continue => (now_unix_ms, None),
                QaFaultDirective::Activate(activation) => match &activation.activation.action {
                    QaFaultAction::AdvanceLogicalTime { milliseconds } => (
                        now_unix_ms
                            .saturating_add(i64::try_from(*milliseconds).unwrap_or(i64::MAX)),
                        Some(activation.activation.id.clone()),
                    ),
                    _ => return Err(qa_fault_activation_error(activation)),
                },
            };
            let stale = is_stale(worker, effective_now);
            if stale {
                candidates.push(StaleReclaimCandidate {
                    worker_id: worker_id.clone(),
                    effective_now_unix_ms: effective_now,
                    recovery_activation_id: activation_id,
                });
            } else if let Some(activation_id) = activation_id {
                failed_closed_recoveries.push(activation_id);
            }
        }
        if let Some((activation_id, actors)) = active_barrier {
            let already_reclaimed = actors
                .iter()
                .any(|actor| self.workers.get(actor.as_str()).is_some_and(&is_reclaimed));
            if candidates.is_empty() && !already_reclaimed {
                failed_closed_recoveries.push(activation_id);
            } else {
                reclaimed_recoveries.push(activation_id);
            }
        }
        Ok(StaleReclaimScan {
            candidates,
            failed_closed_activation_ids: failed_closed_recoveries,
            reclaimed_activation_ids: reclaimed_recoveries,
        })
    }

    fn prepare_stale_reclaim_barrier(
        &self,
        candidate_actors: &[String],
    ) -> Result<Option<(String, Vec<String>)>, WorkerLifecycleError> {
        const POINT_ID: &str = "worker.stale_reclaim.batch_before_effect";
        let active = self
            .qa_fault_probe
            .active_barriers()?
            .into_iter()
            .find(|barrier| barrier.point_id == POINT_ID)
            .map(|barrier| (barrier.activation_id, barrier.actors, barrier.participants));
        if let Some((activation_id, actors, participants)) = active {
            if actors.len() == usize::from(participants) {
                self.consume_stale_reclaim_barrier_releases(POINT_ID, actors.as_slice())?;
                return Ok(Some((activation_id, actors)));
            }
            let joined = actors.iter().cloned().collect::<BTreeSet<_>>();
            let mut latest_activation = None;
            let mut joined_count = joined.len();
            for actor in candidate_actors.iter().filter(|actor| !joined.contains(*actor)) {
                match self
                    .qa_fault_probe
                    .checkpoint(QaFaultCheckpoint { point_id: POINT_ID, actor: actor.as_str() })?
                {
                    QaFaultDirective::Continue => {}
                    QaFaultDirective::Activate(activation) => {
                        if !matches!(activation.activation.action, QaFaultAction::Barrier { .. }) {
                            return Err(qa_fault_activation_error(activation));
                        }
                        joined_count = joined_count.saturating_add(1);
                        latest_activation = Some(activation);
                        if joined_count >= usize::from(participants) {
                            break;
                        }
                    }
                }
            }
            if let Some(activation) = latest_activation {
                return Err(qa_fault_activation_error(activation));
            }
            return Err(QaFaultProbeError::AdapterFailure(
                "qa_fault.barrier_waiting_for_candidates",
            )
            .into());
        }

        if candidate_actors.len() < 2 {
            return Ok(None);
        }
        let mut latest_activation = None;
        let mut joined_actors = BTreeSet::new();
        for actor in candidate_actors {
            match self
                .qa_fault_probe
                .checkpoint(QaFaultCheckpoint { point_id: POINT_ID, actor: actor.as_str() })?
            {
                QaFaultDirective::Continue => {}
                QaFaultDirective::Activate(activation) => {
                    let participants = match &activation.activation.action {
                        QaFaultAction::Barrier { participants } => *participants,
                        _ => return Err(qa_fault_activation_error(activation)),
                    };
                    joined_actors.insert(activation.actor.clone());
                    latest_activation = Some(activation);
                    if joined_actors.len() >= usize::from(participants) {
                        break;
                    }
                }
            }
        }
        if let Some(activation) = latest_activation {
            return Err(qa_fault_activation_error(activation));
        }
        Ok(None)
    }

    fn consume_stale_reclaim_barrier_releases(
        &self,
        point_id: &str,
        actors: &[String],
    ) -> Result<(), WorkerLifecycleError> {
        let mut pending = actors.iter().cloned().collect::<BTreeSet<_>>();
        for _ in 0..actors.len() {
            let mut progressed = false;
            for actor in pending.iter().cloned().collect::<Vec<_>>() {
                match self
                    .qa_fault_probe
                    .checkpoint(QaFaultCheckpoint { point_id, actor: actor.as_str() })
                {
                    Ok(QaFaultDirective::Continue)
                    | Err(QaFaultProbeError::AdapterFailure(
                        "qa_fault.barrier_actor_already_released",
                    )) => {
                        pending.remove(actor.as_str());
                        progressed = true;
                    }
                    Err(QaFaultProbeError::AdapterFailure(
                        "qa_fault.barrier_release_not_ready",
                    )) => {}
                    Ok(QaFaultDirective::Activate(activation)) => {
                        return Err(qa_fault_activation_error(activation));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            if pending.is_empty() {
                return Ok(());
            }
            if !progressed {
                return Err(
                    QaFaultProbeError::AdapterFailure("qa_fault.barrier_release_stalled").into()
                );
            }
        }
        Err(QaFaultProbeError::AdapterFailure("qa_fault.barrier_release_stalled").into())
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

fn is_bounded_worker_id(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed == value
        && trimmed.len() <= MAX_WORKER_ID_BYTES
        && trimmed.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn is_bounded_runtime_identity(value: &str) -> bool {
    palyra_common::runtime_contracts::RuntimeOperationId::parse(value).is_ok()
}

fn validate_durable_worker_record(
    worker_id: &str,
    record: &WorkerFleetRecord,
) -> Result<(), WorkerLifecycleError> {
    if record.attestation.worker_id != worker_id {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "durable worker key does not match attestation identity".to_owned(),
        ));
    }
    if record.last_heartbeat_unix_ms < 0 {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "durable worker heartbeat timestamp is invalid".to_owned(),
        ));
    }
    if let Some(lease) = record.lease.as_ref() {
        if lease.worker_id != worker_id
            || lease.lease_id.trim().is_empty()
            || lease.run_id.trim().is_empty()
            || lease.expires_at_unix_ms < 0
        {
            return Err(WorkerLifecycleError::InvalidLeaseRequest(
                "durable worker lease identity is invalid".to_owned(),
            ));
        }
        if !matches!(
            record.state,
            WorkerLifecycleState::Assigned
                | WorkerLifecycleState::Busy
                | WorkerLifecycleState::Degraded
                | WorkerLifecycleState::Draining
        ) {
            return Err(WorkerLifecycleError::InvalidLeaseRequest(
                "durable worker lease is incompatible with lifecycle state".to_owned(),
            ));
        }
    } else if matches!(record.state, WorkerLifecycleState::Assigned | WorkerLifecycleState::Busy) {
        return Err(WorkerLifecycleError::InvalidLeaseRequest(
            "durable active worker state is missing its lease".to_owned(),
        ));
    }
    Ok(())
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
    worker: &WorkerFleetRecord,
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

fn qa_fault_activation_error(activation: QaFaultActivationDirective) -> WorkerLifecycleError {
    WorkerLifecycleError::QaFaultActivated {
        activation_id: activation.activation.id,
        point_id: activation.activation.point_id,
        actor: activation.actor,
        action: activation.activation.action,
    }
}

/// Rejects missing or superseded leases before completion mutates worker state.
fn validate_completion_identity(
    worker_id: &str,
    worker: &WorkerFleetRecord,
    completed: &WorkerLeaseIdentity,
    now_unix_ms: i64,
) -> Result<(), WorkerLifecycleError> {
    let matches_active_lease = worker.lease.as_ref().is_some_and(|active| {
        active.lease_id == completed.lease_id
            && active.run_id == completed.run_id
            && now_unix_ms < active.expires_at_unix_ms
    });
    if matches_active_lease {
        return Ok(());
    }
    Err(WorkerLifecycleError::StaleLeaseCompletion {
        worker_id: worker_id.to_owned(),
        completed_lease_id: completed.lease_id.clone(),
        completed_run_id: completed.run_id.clone(),
    })
}

/// Applies cleanup state transitions after the caller authorizes the operation.
fn finalize_worker_cleanup(
    worker_id: &str,
    worker: &mut WorkerFleetRecord,
    cleanup: WorkerCleanupReport,
    now_unix_ms: i64,
) -> WorkerCleanupOutcome {
    let (run_id, lease_id) = worker
        .lease
        .as_ref()
        .map(|lease| (Some(lease.run_id.clone()), Some(lease.lease_id.clone())))
        .unwrap_or((None, None));
    let cleanup_succeeded = cleanup.is_verified();
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
            lease_id: lease_id.clone(),
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
            lease_id,
            reason_code: "worker.cleanup_failed".to_owned(),
            timestamp_unix_ms: now_unix_ms,
        }
    };
    WorkerCleanupOutcome { event, cleanup_report: cleanup, cleanup_succeeded }
}

/// Issues a lease on `worker` after revalidating every fail-closed assignment gate.
fn assign_worker_record(
    worker_id: &str,
    worker: &mut WorkerFleetRecord,
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
        lease_id: Some(lease.lease_id.clone()),
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
    worker: &WorkerFleetRecord,
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
    worker: &WorkerFleetRecord,
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
mod tests;
