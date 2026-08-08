//! Canonical transport-neutral task and outcome contracts for remote workers.
//! The protocol binds policy, workspace, idempotency, cancellation, generations,
//! artifacts, resource usage, cleanup, and mutually authenticated transport evidence.

use std::path::{Component, Path};

use hkdf::Hkdf;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};
use ring::hmac;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};
use zeroize::Zeroizing;

use crate::{
    RuntimeGeneration, WorkerCleanupReport, WorkerRemoteToolRequestEnvelope,
    WorkerRemoteToolResultEnvelope,
};

/// Canonical protocol identifier for all remote worker transports.
pub const REMOTE_WORKER_PROTOCOL_V1: &str = "palyra.remote-worker/v1";
/// Current schema version of the canonical protocol.
pub const REMOTE_WORKER_PROTOCOL_SCHEMA_VERSION: u32 = 1;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_ARTIFACTS: usize = 128;
const MAX_PATCH_PATHS: usize = 1_024;
const MAX_CLOCK_SKEW_MS: i64 = 60_000;
const MAX_SECRET_LEASE_PLAINTEXT_BYTES: usize = 64 * 1_024;
const WORKER_SECRET_LEASE_ALGORITHM: &str = "x25519-hkdf-sha256-chacha20poly1305";

/// Hash-addressed artifact transferred into or out of a remote task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentAddressedArtifact {
    /// Stable artifact identifier.
    pub artifact_id: String,
    /// SHA-256 digest of the artifact bytes.
    pub sha256: String,
    /// Bounded artifact size.
    pub size_bytes: u64,
    /// Non-secret media type used for dispatch and diagnostics.
    pub media_type: String,
}

/// Short-lived encrypted secret lease descriptor.
///
/// The encrypted bytes travel through the named content-addressed artifact;
/// workers must never persist the decrypted value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptedWorkerSecretLease {
    /// Host-issued lease identity.
    pub lease_id: String,
    /// Content-addressed input artifact carrying only encrypted bytes.
    pub ciphertext_artifact_id: String,
    /// Digest of the encrypted payload.
    pub ciphertext_sha256: String,
    /// Ephemeral X25519 public key used for this lease only.
    pub ephemeral_public_key_hex: String,
    /// ChaCha20-Poly1305 nonce, unique under the derived task key.
    pub nonce_hex: String,
    /// Digest of the canonical authenticated lease context.
    pub aad_sha256: String,
    /// Closed algorithm suite for recipient-bound authenticated encryption.
    pub encryption_algorithm: String,
    /// Digest of the recipient encryption key.
    pub recipient_key_sha256: String,
    /// Lease expiry enforced by host and worker.
    pub expires_at_unix_ms: i64,
    /// Must remain false; persistence is never delegated to a worker.
    pub persistence_allowed: bool,
}

/// Encrypted secret artifact delivered separately from the canonical descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptedWorkerSecretArtifact {
    /// Short-lived descriptor bound into the signed task envelope.
    pub lease: EncryptedWorkerSecretLease,
    /// ChaCha20-Poly1305 ciphertext with its authentication tag appended.
    pub ciphertext: Vec<u8>,
}

/// Reviewed patch bundle returned by a mutating remote task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemotePatchBundle {
    /// SHA-256 digest of canonical patch bytes.
    pub patch_sha256: String,
    /// Workspace-relative paths touched by the patch.
    pub touched_paths: Vec<String>,
    /// Whether host review is required before authoritative mutation.
    pub review_required: bool,
}

/// Bounded worker resource accounting for one terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteResourceUsage {
    /// Wall time consumed by the task.
    pub duration_ms: u64,
    /// Peak resident memory reported by the isolated worker.
    pub peak_memory_bytes: u64,
    /// CPU time consumed by the task.
    pub cpu_time_ms: u64,
    /// Bytes read from input artifacts.
    pub input_bytes: u64,
    /// Bytes written to output artifacts.
    pub output_bytes: u64,
}

/// Host-owned resource limits bound into a canonical task before dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteResourceLimits {
    /// Maximum wall-clock duration.
    pub wall_time_ms: u64,
    /// Maximum resident memory admitted for the worker process.
    pub memory_bytes: u64,
    /// Maximum CPU time admitted for the task.
    pub cpu_time_ms: u64,
    /// Maximum aggregate input artifact bytes.
    pub input_artifact_bytes: u64,
    /// Maximum aggregate output artifact bytes.
    pub output_artifact_bytes: u64,
}

impl RemoteResourceLimits {
    fn validate(&self) -> Result<(), RemoteWorkerProtocolError> {
        if self.wall_time_ms == 0
            || self.memory_bytes == 0
            || self.cpu_time_ms == 0
            || self.input_artifact_bytes == 0
            || self.output_artifact_bytes == 0
        {
            return Err(RemoteWorkerProtocolError::ResourceBoundsInvalid);
        }
        Ok(())
    }
}

/// Optional WorkGraph claim authority bound to a remote task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteWorkGraphClaimBinding {
    /// Durable graph identity.
    pub graph_id: String,
    /// Exact claimed node identity.
    pub node_id: String,
    /// Host claim identity used for compare-and-set settlement.
    pub claim_id: String,
    /// Claim generation fenced against reclaim.
    pub claim_generation: u64,
}

impl RemoteWorkGraphClaimBinding {
    fn validate(&self) -> Result<(), RemoteWorkerProtocolError> {
        validate_identity(self.graph_id.as_str(), "work_graph_id")?;
        validate_identity(self.node_id.as_str(), "work_graph_node_id")?;
        validate_identity(self.claim_id.as_str(), "work_graph_claim_id")?;
        if self.claim_generation == 0 {
            return Err(RemoteWorkerProtocolError::GenerationMismatch);
        }
        Ok(())
    }
}

/// Typed authority posture for tasks outside or inside a WorkGraph claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RemoteWorkGraphTaskPosture {
    /// The daemon dispatched an ordinary tool call without claim authority.
    #[default]
    DirectToolDispatch,
    /// The task carries an exact host-owned claim binding.
    Claimed,
}

/// Cleanup settlement for all task-scoped worker state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteCleanupAttestation {
    /// Whether the workspace scope was removed.
    pub workspace_removed: bool,
    /// Whether task artifacts were removed from mutable scratch storage.
    pub scratch_artifacts_removed: bool,
    /// Whether task logs were removed from worker-local storage.
    pub logs_removed: bool,
    /// Whether decrypted secret material was absent after cleanup.
    pub secret_material_removed: bool,
    /// Stable cleanup result reason.
    pub reason_code: String,
}

impl RemoteCleanupAttestation {
    /// Returns whether every required cleanup dimension was verified.
    #[must_use]
    pub const fn is_verified(&self) -> bool {
        self.workspace_removed
            && self.scratch_artifacts_removed
            && self.logs_removed
            && self.secret_material_removed
    }
}

/// Canonical host-to-worker task envelope shared by network, SSH, and desktop adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTaskEnvelope {
    /// Stable task identity.
    pub task_id: String,
    /// Host request identity.
    pub request_id: String,
    /// Duplicate-suppression key retained across retries.
    pub idempotency_key: String,
    /// Stable identifier used for out-of-band cancellation.
    pub cancellation_id: String,
    /// Host issue timestamp.
    pub issued_at_unix_ms: i64,
    /// Hard terminal deadline.
    pub deadline_unix_ms: i64,
    /// SHA-256 digest of the exact policy and approval grant.
    pub policy_sha256: String,
    /// SHA-256 digest of the workspace manifest.
    pub workspace_manifest_sha256: String,
    /// SHA-256 digest of the canonical tool input.
    pub input_sha256: String,
    /// Canonical tool name.
    pub tool_name: String,
    /// Canonical JSON input.
    pub input_json: String,
    /// Content-addressed inputs available to the worker.
    pub input_artifacts: Vec<ContentAddressedArtifact>,
    /// Optional encrypted secret lease.
    pub secret_lease: Option<EncryptedWorkerSecretLease>,
    /// Host-issued runtime generation.
    pub run_generation: RuntimeGeneration,
    /// Side-effect fence generation.
    pub fence_generation: u64,
    /// WorkGraph claim identity when the task belongs to a graph.
    pub work_graph_claim: Option<RemoteWorkGraphClaimBinding>,
    /// Explicit explanation for whether WorkGraph claim authority exists.
    pub work_graph_posture: RemoteWorkGraphTaskPosture,
    /// Host-owned resource-governor limits.
    pub resource_limits: RemoteResourceLimits,
    /// Bounded output budget.
    pub max_output_bytes: u64,
}

impl WorkerTaskEnvelope {
    /// Projects the established worker RPC envelope into the canonical task contract.
    #[must_use]
    pub fn from_remote_request(request: &WorkerRemoteToolRequestEnvelope) -> Self {
        let policy_sha256 = canonical_sha256(&(
            request.lease.grant_id.as_str(),
            request.lease.grant_tool_name.as_str(),
            request.lease.required_capabilities.as_slice(),
            request.lease.process_executable_allowlist.as_slice(),
        ));
        let mut input_artifacts = if request.workspace_transfer.scoped_entries.is_empty() {
            vec![ContentAddressedArtifact {
                artifact_id: "workspace-input-manifest".to_owned(),
                sha256: request.lease.artifact_transport.input_manifest_sha256.clone(),
                size_bytes: 0,
                media_type: "application/vnd.palyra.workspace-manifest+json".to_owned(),
            }]
        } else {
            request
                .workspace_transfer
                .scoped_entries
                .iter()
                .map(|entry| ContentAddressedArtifact {
                    artifact_id: entry.path.clone(),
                    sha256: entry.sha256.clone(),
                    size_bytes: u64::try_from(entry.bytes.len()).unwrap_or(u64::MAX),
                    media_type: match entry.kind {
                        crate::WorkerRemoteWorkspaceEntryKind::File => "application/octet-stream",
                        crate::WorkerRemoteWorkspaceEntryKind::Directory => {
                            "application/vnd.palyra.workspace-directory"
                        }
                    }
                    .to_owned(),
                })
                .collect()
        };
        if let Some(secret_artifact) = request.encrypted_secret_artifact.as_ref() {
            input_artifacts.push(secret_artifact.content_addressed_artifact());
        }
        Self {
            task_id: request.proposal_id.clone(),
            request_id: request.request_id.clone(),
            idempotency_key: canonical_sha256(&(
                request.lease.run_id.as_str(),
                request.proposal_id.as_str(),
                request.input_json_sha256.as_str(),
            )),
            cancellation_id: canonical_sha256(&(
                "cancel",
                request.lease.lease_id.as_str(),
                request.request_id.as_str(),
            )),
            issued_at_unix_ms: request.lease.expires_at_unix_ms.saturating_sub(60_000),
            deadline_unix_ms: request.lease.expires_at_unix_ms,
            policy_sha256,
            workspace_manifest_sha256: request.workspace_transfer.workspace_manifest_sha256.clone(),
            input_sha256: request.input_json_sha256.clone(),
            tool_name: request.tool_name.clone(),
            input_json: request.input_json.clone(),
            input_artifacts,
            secret_lease: request
                .encrypted_secret_artifact
                .as_ref()
                .map(|artifact| artifact.lease.clone()),
            run_generation: request.lease.run_generation,
            fence_generation: request.lease.run_generation.get(),
            work_graph_claim: request.lease.work_graph_claim.clone(),
            work_graph_posture: request.lease.work_graph_posture,
            resource_limits: RemoteResourceLimits {
                wall_time_ms: 60_000,
                memory_bytes: 512 * 1_024 * 1_024,
                cpu_time_ms: 60_000,
                input_artifact_bytes: 384 * 1_024,
                output_artifact_bytes: 512 * 1_024,
            },
            max_output_bytes: 512 * 1_024,
        }
    }

    /// Validates all authority, integrity, deadline, and boundedness invariants.
    ///
    /// # Errors
    /// Returns a typed fail-closed error for malformed identities, digests,
    /// clocks, duplicate keys, secret leases, artifact bounds, or patch authority.
    pub fn validate(&self, observed_at_unix_ms: i64) -> Result<(), RemoteWorkerProtocolError> {
        validate_identity(self.task_id.as_str(), "task_id")?;
        validate_identity(self.request_id.as_str(), "request_id")?;
        validate_identity(self.idempotency_key.as_str(), "idempotency_key")?;
        validate_identity(self.cancellation_id.as_str(), "cancellation_id")?;
        validate_identity(self.tool_name.as_str(), "tool_name")?;
        validate_sha256(self.idempotency_key.as_str(), "idempotency_key")?;
        validate_sha256(self.cancellation_id.as_str(), "cancellation_id")?;
        validate_sha256(self.policy_sha256.as_str(), "policy_sha256")?;
        validate_sha256(self.workspace_manifest_sha256.as_str(), "workspace_manifest_sha256")?;
        validate_sha256(self.input_sha256.as_str(), "input_sha256")?;
        if self.input_sha256 != sha256_hex(self.input_json.as_bytes()) {
            return Err(RemoteWorkerProtocolError::DigestMismatch { field: "input_sha256" });
        }
        if self.issued_at_unix_ms > observed_at_unix_ms.saturating_add(MAX_CLOCK_SKEW_MS) {
            return Err(RemoteWorkerProtocolError::ClockSkew);
        }
        if self.deadline_unix_ms <= observed_at_unix_ms
            || self.deadline_unix_ms <= self.issued_at_unix_ms
        {
            return Err(RemoteWorkerProtocolError::DeadlineExpired);
        }
        if self.run_generation.get() == 0
            || self.fence_generation == 0
            || self.fence_generation != self.run_generation.get()
        {
            return Err(RemoteWorkerProtocolError::GenerationMismatch);
        }
        if self.max_output_bytes == 0 || self.input_artifacts.len() > MAX_ARTIFACTS {
            return Err(RemoteWorkerProtocolError::ResourceBoundsInvalid);
        }
        for artifact in &self.input_artifacts {
            artifact.validate()?;
        }
        if let Some(secret) = self.secret_lease.as_ref() {
            secret.validate(observed_at_unix_ms, self.deadline_unix_ms)?;
            let ciphertext = self
                .input_artifacts
                .iter()
                .find(|artifact| artifact.artifact_id == secret.ciphertext_artifact_id)
                .ok_or(RemoteWorkerProtocolError::SecretLeaseInvalid)?;
            if ciphertext.sha256 != secret.ciphertext_sha256 {
                return Err(RemoteWorkerProtocolError::DigestMismatch {
                    field: "ciphertext_sha256",
                });
            }
        }
        match (self.work_graph_posture, self.work_graph_claim.as_ref()) {
            (RemoteWorkGraphTaskPosture::DirectToolDispatch, None) => {}
            (RemoteWorkGraphTaskPosture::Claimed, Some(claim)) => claim.validate()?,
            _ => return Err(RemoteWorkerProtocolError::WorkGraphBindingInvalid),
        }
        self.resource_limits.validate()?;
        let input_artifact_bytes = self
            .input_artifacts
            .iter()
            .try_fold(0_u64, |total, artifact| total.checked_add(artifact.size_bytes))
            .ok_or(RemoteWorkerProtocolError::ResourceBoundsInvalid)?;
        if input_artifact_bytes > self.resource_limits.input_artifact_bytes {
            return Err(RemoteWorkerProtocolError::ResourceBoundsInvalid);
        }
        if self.max_output_bytes > self.resource_limits.output_artifact_bytes {
            return Err(RemoteWorkerProtocolError::ResourceBoundsInvalid);
        }
        Ok(())
    }
}

impl ContentAddressedArtifact {
    fn validate(&self) -> Result<(), RemoteWorkerProtocolError> {
        validate_identity(self.artifact_id.as_str(), "artifact_id")?;
        validate_identity(self.media_type.as_str(), "media_type")?;
        validate_sha256(self.sha256.as_str(), "artifact_sha256")
    }
}

impl EncryptedWorkerSecretLease {
    fn validate(
        &self,
        observed_at_unix_ms: i64,
        task_deadline_unix_ms: i64,
    ) -> Result<(), RemoteWorkerProtocolError> {
        validate_identity(self.lease_id.as_str(), "secret_lease_id")?;
        validate_identity(self.ciphertext_artifact_id.as_str(), "ciphertext_artifact_id")?;
        validate_sha256(self.ciphertext_sha256.as_str(), "ciphertext_sha256")?;
        validate_fixed_hex(self.ephemeral_public_key_hex.as_str(), 32, "ephemeral_public_key_hex")?;
        validate_fixed_hex(self.nonce_hex.as_str(), 12, "secret_nonce_hex")?;
        validate_sha256(self.aad_sha256.as_str(), "secret_aad_sha256")?;
        validate_sha256(self.recipient_key_sha256.as_str(), "recipient_key_sha256")?;
        if self.encryption_algorithm != WORKER_SECRET_LEASE_ALGORITHM {
            return Err(RemoteWorkerProtocolError::SecretLeaseInvalid);
        }
        if self.persistence_allowed {
            return Err(RemoteWorkerProtocolError::SecretPersistenceRequested);
        }
        if self.expires_at_unix_ms <= observed_at_unix_ms
            || self.expires_at_unix_ms > task_deadline_unix_ms
        {
            return Err(RemoteWorkerProtocolError::SecretLeaseInvalid);
        }
        Ok(())
    }
}

impl EncryptedWorkerSecretArtifact {
    /// Returns the content-addressed metadata carried in the task input manifest.
    #[must_use]
    pub fn content_addressed_artifact(&self) -> ContentAddressedArtifact {
        ContentAddressedArtifact {
            artifact_id: self.lease.ciphertext_artifact_id.clone(),
            sha256: self.lease.ciphertext_sha256.clone(),
            size_bytes: u64::try_from(self.ciphertext.len()).unwrap_or(u64::MAX),
            media_type: "application/vnd.palyra.worker-secret-lease+encrypted".to_owned(),
        }
    }
}

/// Encrypts one short-lived secret to the worker's paired X25519 key.
///
/// # Errors
/// Returns a typed error for invalid identifiers, bounds, randomness, key
/// derivation, or authenticated-encryption failure.
pub fn seal_worker_secret_lease(
    lease_id: &str,
    recipient_public_key: [u8; 32],
    plaintext: &[u8],
    expires_at_unix_ms: i64,
) -> Result<EncryptedWorkerSecretArtifact, RemoteWorkerProtocolError> {
    validate_identity(lease_id, "secret_lease_id")?;
    if plaintext.is_empty()
        || plaintext.len() > MAX_SECRET_LEASE_PLAINTEXT_BYTES
        || expires_at_unix_ms <= 0
    {
        return Err(RemoteWorkerProtocolError::SecretLeaseInvalid);
    }
    let mut ephemeral_secret_bytes = Zeroizing::new([0_u8; 32]);
    getrandom::fill(&mut *ephemeral_secret_bytes)
        .map_err(|_| RemoteWorkerProtocolError::SecretLeaseCryptoFailed)?;
    let ephemeral_secret = StaticSecret::from(*ephemeral_secret_bytes);
    let ephemeral_public = X25519PublicKey::from(&ephemeral_secret).to_bytes();
    let recipient_public = X25519PublicKey::from(recipient_public_key);
    let shared_secret = ephemeral_secret.diffie_hellman(&recipient_public);
    let key_bytes = derive_secret_lease_key(
        shared_secret.as_bytes(),
        lease_id,
        &ephemeral_public,
        &recipient_public_key,
    )?;
    let mut nonce_bytes = [0_u8; 12];
    getrandom::fill(&mut nonce_bytes)
        .map_err(|_| RemoteWorkerProtocolError::SecretLeaseCryptoFailed)?;
    let ciphertext_artifact_id = format!("secret-lease-{}", sha256_hex(lease_id.as_bytes()));
    let aad = secret_lease_aad(
        lease_id,
        ciphertext_artifact_id.as_str(),
        expires_at_unix_ms,
        &ephemeral_public,
        &recipient_public_key,
    )?;
    let key = LessSafeKey::new(
        UnboundKey::new(&CHACHA20_POLY1305, key_bytes.as_slice())
            .map_err(|_| RemoteWorkerProtocolError::SecretLeaseCryptoFailed)?,
    );
    let mut ciphertext = plaintext.to_vec();
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce_bytes),
        Aad::from(aad.as_slice()),
        &mut ciphertext,
    )
    .map_err(|_| RemoteWorkerProtocolError::SecretLeaseCryptoFailed)?;
    let lease = EncryptedWorkerSecretLease {
        lease_id: lease_id.to_owned(),
        ciphertext_artifact_id,
        ciphertext_sha256: sha256_hex(ciphertext.as_slice()),
        ephemeral_public_key_hex: hex::encode(ephemeral_public),
        nonce_hex: hex::encode(nonce_bytes),
        aad_sha256: sha256_hex(aad.as_slice()),
        encryption_algorithm: WORKER_SECRET_LEASE_ALGORITHM.to_owned(),
        recipient_key_sha256: sha256_hex(recipient_public_key.as_slice()),
        expires_at_unix_ms,
        persistence_allowed: false,
    };
    Ok(EncryptedWorkerSecretArtifact { lease, ciphertext })
}

/// Decrypts a valid, unexpired lease into zeroizing process-local memory.
///
/// # Errors
/// Returns a typed error for stale/tampered descriptors, wrong recipient keys,
/// digest mismatches, or failed authenticated decryption.
pub fn open_worker_secret_lease(
    artifact: &EncryptedWorkerSecretArtifact,
    recipient_secret: &StaticSecret,
    observed_at_unix_ms: i64,
) -> Result<Zeroizing<Vec<u8>>, RemoteWorkerProtocolError> {
    if artifact.ciphertext.len() <= CHACHA20_POLY1305.tag_len()
        || artifact.ciphertext.len()
            > MAX_SECRET_LEASE_PLAINTEXT_BYTES.saturating_add(CHACHA20_POLY1305.tag_len())
    {
        return Err(RemoteWorkerProtocolError::SecretLeaseInvalid);
    }
    artifact.lease.validate(observed_at_unix_ms, artifact.lease.expires_at_unix_ms)?;
    if artifact.lease.ciphertext_sha256 != sha256_hex(artifact.ciphertext.as_slice()) {
        return Err(RemoteWorkerProtocolError::DigestMismatch { field: "ciphertext_sha256" });
    }
    let recipient_public = X25519PublicKey::from(recipient_secret).to_bytes();
    if artifact.lease.recipient_key_sha256 != sha256_hex(recipient_public.as_slice()) {
        return Err(RemoteWorkerProtocolError::SecretLeaseInvalid);
    }
    let ephemeral_public_bytes =
        decode_fixed_hex::<32>(artifact.lease.ephemeral_public_key_hex.as_str())
            .ok_or(RemoteWorkerProtocolError::SecretLeaseInvalid)?;
    let nonce_bytes = decode_fixed_hex::<12>(artifact.lease.nonce_hex.as_str())
        .ok_or(RemoteWorkerProtocolError::SecretLeaseInvalid)?;
    let ephemeral_public = X25519PublicKey::from(ephemeral_public_bytes);
    let shared_secret = recipient_secret.diffie_hellman(&ephemeral_public);
    let key_bytes = derive_secret_lease_key(
        shared_secret.as_bytes(),
        artifact.lease.lease_id.as_str(),
        &ephemeral_public_bytes,
        &recipient_public,
    )?;
    let aad = secret_lease_aad(
        artifact.lease.lease_id.as_str(),
        artifact.lease.ciphertext_artifact_id.as_str(),
        artifact.lease.expires_at_unix_ms,
        &ephemeral_public_bytes,
        &recipient_public,
    )?;
    if artifact.lease.aad_sha256 != sha256_hex(aad.as_slice()) {
        return Err(RemoteWorkerProtocolError::DigestMismatch { field: "secret_aad_sha256" });
    }
    let key = LessSafeKey::new(
        UnboundKey::new(&CHACHA20_POLY1305, key_bytes.as_slice())
            .map_err(|_| RemoteWorkerProtocolError::SecretLeaseCryptoFailed)?,
    );
    let mut plaintext = Zeroizing::new(artifact.ciphertext.clone());
    let plaintext_len = key
        .open_in_place(
            Nonce::assume_unique_for_key(nonce_bytes),
            Aad::from(aad.as_slice()),
            plaintext.as_mut_slice(),
        )
        .map_err(|_| RemoteWorkerProtocolError::SecretLeaseCryptoFailed)?
        .len();
    plaintext.truncate(plaintext_len);
    Ok(plaintext)
}

/// Worker-to-host terminal outcome bound to one canonical task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteTaskOutcome {
    /// Task identity copied from the request.
    pub task_id: String,
    /// Idempotency key copied from the request.
    pub idempotency_key: String,
    /// Attested worker identity.
    pub worker_id: String,
    /// Monotonic response sequence; terminal response is exactly sequence one.
    pub response_sequence: u64,
    /// Host-issued runtime generation.
    pub run_generation: RuntimeGeneration,
    /// Side-effect fence generation.
    pub fence_generation: u64,
    /// Whether the tool itself succeeded.
    pub success: bool,
    /// SHA-256 digest of the bounded output document.
    pub output_sha256: String,
    /// Content-addressed output artifacts.
    pub output_artifacts: Vec<ContentAddressedArtifact>,
    /// Optional reviewed patch bundle.
    pub patch_bundle: Option<RemotePatchBundle>,
    /// Bounded resource accounting.
    pub usage: RemoteResourceUsage,
    /// Verified cleanup evidence.
    pub cleanup: RemoteCleanupAttestation,
    /// Worker completion timestamp.
    pub completed_at_unix_ms: i64,
    /// Stable terminal reason.
    pub reason_code: String,
}

impl RemoteTaskOutcome {
    /// Projects an established worker RPC result into the canonical outcome contract.
    #[must_use]
    pub fn from_remote_result(
        request: &WorkerRemoteToolRequestEnvelope,
        result: &WorkerRemoteToolResultEnvelope,
    ) -> Self {
        let patch_bundle = serde_json::from_str::<serde_json::Value>(&result.output_json)
            .ok()
            .and_then(|value| value.get("remote_patch_bundle").cloned())
            .and_then(|value| serde_json::from_value(value).ok());
        Self {
            task_id: request.proposal_id.clone(),
            idempotency_key: WorkerTaskEnvelope::from_remote_request(request).idempotency_key,
            worker_id: result.worker_id.clone(),
            response_sequence: 1,
            run_generation: result.run_generation,
            fence_generation: result.run_generation.get(),
            success: result.success,
            output_sha256: result.output_json_sha256.clone(),
            output_artifacts: vec![ContentAddressedArtifact {
                artifact_id: "output-manifest".to_owned(),
                sha256: result.output_manifest_sha256.clone(),
                size_bytes: 0,
                media_type: "application/vnd.palyra.worker-output-manifest+json".to_owned(),
            }],
            patch_bundle,
            usage: RemoteResourceUsage {
                duration_ms: 0,
                peak_memory_bytes: 0,
                cpu_time_ms: 0,
                input_bytes: u64::try_from(request.input_json.len()).unwrap_or(u64::MAX),
                output_bytes: u64::try_from(result.output_json.len()).unwrap_or(u64::MAX),
            },
            cleanup: RemoteCleanupAttestation::from(&result.cleanup_report),
            completed_at_unix_ms: result.completed_at_unix_ms,
            reason_code: if result.success {
                "worker.task.succeeded"
            } else {
                "worker.task.failed"
            }
            .to_owned(),
        }
    }

    /// Validates exact request binding, sequence, artifact, patch, and cleanup evidence.
    ///
    /// # Errors
    /// Returns a typed error when a late, duplicate, stale, malicious, or
    /// incompletely cleaned result attempts to settle.
    pub fn validate_against(
        &self,
        task: &WorkerTaskEnvelope,
        observed_at_unix_ms: i64,
    ) -> Result<(), RemoteWorkerProtocolError> {
        task.validate(observed_at_unix_ms.min(task.deadline_unix_ms.saturating_sub(1)))?;
        validate_identity(self.worker_id.as_str(), "worker_id")?;
        validate_identity(self.reason_code.as_str(), "reason_code")?;
        validate_sha256(self.idempotency_key.as_str(), "idempotency_key")?;
        validate_sha256(self.output_sha256.as_str(), "output_sha256")?;
        if self.task_id != task.task_id || self.idempotency_key != task.idempotency_key {
            return Err(RemoteWorkerProtocolError::TaskBindingMismatch);
        }
        if self.response_sequence != 1 {
            return Err(RemoteWorkerProtocolError::ResponseSequenceInvalid);
        }
        if self.run_generation != task.run_generation
            || self.fence_generation != task.fence_generation
        {
            return Err(RemoteWorkerProtocolError::GenerationMismatch);
        }
        if observed_at_unix_ms > task.deadline_unix_ms.saturating_add(MAX_CLOCK_SKEW_MS)
            || self.completed_at_unix_ms > task.deadline_unix_ms
        {
            return Err(RemoteWorkerProtocolError::LateOutcome);
        }
        if self.output_artifacts.len() > MAX_ARTIFACTS {
            return Err(RemoteWorkerProtocolError::ResourceBoundsInvalid);
        }
        let output_artifact_bytes = self
            .output_artifacts
            .iter()
            .try_fold(0_u64, |total, artifact| total.checked_add(artifact.size_bytes))
            .ok_or(RemoteWorkerProtocolError::ResourceBoundsInvalid)?;
        for artifact in &self.output_artifacts {
            artifact.validate()?;
        }
        if self.usage.duration_ms > task.resource_limits.wall_time_ms
            || self.usage.peak_memory_bytes > task.resource_limits.memory_bytes
            || self.usage.cpu_time_ms > task.resource_limits.cpu_time_ms
            || self.usage.input_bytes > task.resource_limits.input_artifact_bytes
            || self.usage.output_bytes > task.resource_limits.output_artifact_bytes
            || output_artifact_bytes > task.resource_limits.output_artifact_bytes
        {
            return Err(RemoteWorkerProtocolError::ResourceBoundsInvalid);
        }
        if let Some(patch) = self.patch_bundle.as_ref() {
            patch.validate()?;
        }
        if (task.tool_name == "palyra.fs.apply_patch") != self.patch_bundle.is_some() {
            return Err(RemoteWorkerProtocolError::PatchAuthorityInvalid);
        }
        if !self.cleanup.is_verified() {
            return Err(RemoteWorkerProtocolError::CleanupIncomplete);
        }
        Ok(())
    }
}

impl From<&WorkerCleanupReport> for RemoteCleanupAttestation {
    fn from(report: &WorkerCleanupReport) -> Self {
        let verified = report.is_verified();
        Self {
            workspace_removed: report.removed_workspace_scope,
            scratch_artifacts_removed: report.removed_artifacts,
            logs_removed: report.removed_logs,
            secret_material_removed: verified,
            reason_code: report.failure_reason.clone().unwrap_or_else(|| {
                if verified { "worker.cleanup.ok" } else { "worker.cleanup.incomplete" }.to_owned()
            }),
        }
    }
}

impl RemotePatchBundle {
    fn validate(&self) -> Result<(), RemoteWorkerProtocolError> {
        validate_sha256(self.patch_sha256.as_str(), "patch_sha256")?;
        if !self.review_required || self.touched_paths.len() > MAX_PATCH_PATHS {
            return Err(RemoteWorkerProtocolError::PatchAuthorityInvalid);
        }
        for raw in &self.touched_paths {
            let path = Path::new(raw);
            if raw.trim().is_empty()
                || raw.contains('\\')
                || raw.split('/').any(|segment| segment.is_empty() || matches!(segment, "." | ".."))
                || path.is_absolute()
                || path.components().any(|component| {
                    matches!(
                        component,
                        Component::ParentDir | Component::RootDir | Component::Prefix(_)
                    )
                })
            {
                return Err(RemoteWorkerProtocolError::PatchPathEscapesWorkspace {
                    path: raw.clone(),
                });
            }
        }
        Ok(())
    }
}

/// Trust wrapper binding a canonical task to the authenticated transport and worker claims.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteWorkerProtocolV1 {
    /// Canonical protocol identifier.
    pub protocol: String,
    /// Contract schema version.
    pub schema_version: u32,
    /// SHA-256 digest of the mutually authenticated transport identity.
    pub mutual_auth_binding_sha256: String,
    /// SHA-256 digest of worker attestation claims.
    pub worker_attestation_sha256: String,
    /// Canonical task envelope.
    pub task: WorkerTaskEnvelope,
}

impl RemoteWorkerProtocolV1 {
    /// Creates a protocol wrapper from the established mTLS-bound request.
    #[must_use]
    pub fn from_remote_request(request: &WorkerRemoteToolRequestEnvelope) -> Self {
        Self {
            protocol: REMOTE_WORKER_PROTOCOL_V1.to_owned(),
            schema_version: REMOTE_WORKER_PROTOCOL_SCHEMA_VERSION,
            mutual_auth_binding_sha256: canonical_sha256(&(
                request.lease.worker_id.as_str(),
                request.lease.session_id.as_str(),
                request.lease.run_generation.get(),
            )),
            worker_attestation_sha256: canonical_sha256(&request.worker_identity),
            task: WorkerTaskEnvelope::from_remote_request(request),
        }
    }

    /// Validates protocol identity, trust bindings, and the nested task.
    ///
    /// # Errors
    /// Returns a typed error when any transport, attestation, or task invariant fails.
    pub fn validate(&self, observed_at_unix_ms: i64) -> Result<(), RemoteWorkerProtocolError> {
        if self.protocol != REMOTE_WORKER_PROTOCOL_V1
            || self.schema_version != REMOTE_WORKER_PROTOCOL_SCHEMA_VERSION
        {
            return Err(RemoteWorkerProtocolError::ProtocolMismatch);
        }
        validate_sha256(self.mutual_auth_binding_sha256.as_str(), "mutual_auth_binding_sha256")?;
        validate_sha256(self.worker_attestation_sha256.as_str(), "worker_attestation_sha256")?;
        self.task.validate(observed_at_unix_ms)
    }
}

/// Fail-closed canonical remote worker contract errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RemoteWorkerProtocolError {
    /// Protocol identifier or schema is unsupported.
    #[error("remote worker protocol or schema mismatch")]
    ProtocolMismatch,
    /// Required identity is missing or exceeds its bound.
    #[error("remote worker field {field} is missing or oversized")]
    InvalidIdentity { field: &'static str },
    /// SHA-256 field is malformed.
    #[error("remote worker field {field} is not a lowercase SHA-256 digest")]
    InvalidDigest { field: &'static str },
    /// Payload bytes do not match their declared digest.
    #[error("remote worker digest mismatch for {field}")]
    DigestMismatch { field: &'static str },
    /// Host and worker clocks exceed the accepted issuance skew.
    #[error("remote worker task clock skew exceeds the accepted bound")]
    ClockSkew,
    /// Task deadline is absent or expired.
    #[error("remote worker task deadline is expired")]
    DeadlineExpired,
    /// Runtime or side-effect fence generations differ.
    #[error("remote worker generation or fence mismatch")]
    GenerationMismatch,
    /// Artifact or output bounds are invalid.
    #[error("remote worker resource bounds are invalid")]
    ResourceBoundsInvalid,
    /// WorkGraph posture and claim authority do not agree.
    #[error("remote worker WorkGraph binding is invalid")]
    WorkGraphBindingInvalid,
    /// A worker was asked to persist secret material.
    #[error("remote worker secret persistence is forbidden")]
    SecretPersistenceRequested,
    /// Secret lease expiry is invalid.
    #[error("remote worker secret lease is expired or outlives the task")]
    SecretLeaseInvalid,
    /// Secret lease key agreement, derivation, or authenticated encryption failed.
    #[error("remote worker secret lease cryptography failed")]
    SecretLeaseCryptoFailed,
    /// Outcome does not bind to the exact task and idempotency key.
    #[error("remote worker outcome task binding mismatch")]
    TaskBindingMismatch,
    /// A duplicate or out-of-order terminal response was received.
    #[error("remote worker response sequence is invalid")]
    ResponseSequenceInvalid,
    /// Outcome arrived after its authoritative deadline.
    #[error("remote worker outcome arrived after its deadline")]
    LateOutcome,
    /// Cleanup evidence is incomplete.
    #[error("remote worker cleanup evidence is incomplete")]
    CleanupIncomplete,
    /// Patch lacks mandatory review or exceeds path bounds.
    #[error("remote worker patch authority is invalid")]
    PatchAuthorityInvalid,
    /// Patch path escapes the leased workspace.
    #[error("remote worker patch path escapes the workspace: {path}")]
    PatchPathEscapesWorkspace { path: String },
}

fn validate_identity(value: &str, field: &'static str) -> Result<(), RemoteWorkerProtocolError> {
    if value.trim().is_empty() || value.len() > MAX_IDENTITY_BYTES {
        return Err(RemoteWorkerProtocolError::InvalidIdentity { field });
    }
    Ok(())
}

fn validate_sha256(value: &str, field: &'static str) -> Result<(), RemoteWorkerProtocolError> {
    if value.len() != 64
        || value
            .chars()
            .any(|character| !character.is_ascii_hexdigit() || character.is_ascii_uppercase())
    {
        return Err(RemoteWorkerProtocolError::InvalidDigest { field });
    }
    Ok(())
}

fn validate_fixed_hex(
    value: &str,
    byte_len: usize,
    field: &'static str,
) -> Result<(), RemoteWorkerProtocolError> {
    if value.len() != byte_len.saturating_mul(2)
        || value
            .chars()
            .any(|character| !character.is_ascii_hexdigit() || character.is_ascii_uppercase())
    {
        return Err(RemoteWorkerProtocolError::InvalidDigest { field });
    }
    Ok(())
}

fn decode_fixed_hex<const N: usize>(value: &str) -> Option<[u8; N]> {
    let bytes = hex::decode(value).ok()?;
    bytes.try_into().ok()
}

fn derive_secret_lease_key(
    shared_secret: &[u8; 32],
    lease_id: &str,
    ephemeral_public: &[u8; 32],
    recipient_public: &[u8; 32],
) -> Result<Zeroizing<[u8; 32]>, RemoteWorkerProtocolError> {
    let hkdf = Hkdf::<Sha256>::new(Some(lease_id.as_bytes()), shared_secret);
    let mut info = Vec::with_capacity(128);
    info.extend_from_slice(b"palyra.worker-secret-lease.v1\0");
    info.extend_from_slice(ephemeral_public);
    info.extend_from_slice(recipient_public);
    let mut key = Zeroizing::new([0_u8; 32]);
    hkdf.expand(info.as_slice(), key.as_mut())
        .map_err(|_| RemoteWorkerProtocolError::SecretLeaseCryptoFailed)?;
    Ok(key)
}

fn secret_lease_aad(
    lease_id: &str,
    artifact_id: &str,
    expires_at_unix_ms: i64,
    ephemeral_public: &[u8; 32],
    recipient_public: &[u8; 32],
) -> Result<Vec<u8>, RemoteWorkerProtocolError> {
    serde_json::to_vec(&(
        "palyra.worker-secret-lease.aad.v1",
        lease_id,
        artifact_id,
        expires_at_unix_ms,
        hex::encode(ephemeral_public),
        hex::encode(recipient_public),
    ))
    .map_err(|_| RemoteWorkerProtocolError::SecretLeaseCryptoFailed)
}

fn canonical_sha256<T: Serialize>(value: &T) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    sha256_hex(bytes.as_slice())
}

fn sha256_hex(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

/// Computes the one-time delivery MAC for an exact remote worker payload.
///
/// The key is the high-entropy fetch token released only over the authenticated
/// node mTLS channel. The MAC therefore binds payload integrity to the exact
/// transport delivery attempt without introducing a second long-lived key.
#[must_use]
pub fn authenticated_delivery_hmac_sha256(
    fetch_token: &str,
    request_sha256: &str,
    payload: &[u8],
) -> String {
    hex::encode(authenticated_delivery_hmac(fetch_token, request_sha256, payload))
}

/// Verifies an authenticated delivery MAC without data-dependent comparison exits.
#[must_use]
pub fn verify_authenticated_delivery_hmac_sha256(
    fetch_token: &str,
    request_sha256: &str,
    payload: &[u8],
    observed_hmac_sha256: &str,
) -> bool {
    let Ok(observed) = hex::decode(observed_hmac_sha256) else {
        return false;
    };
    let key = hmac::Key::new(hmac::HMAC_SHA256, fetch_token.as_bytes());
    hmac::verify(
        &key,
        authenticated_delivery_message(request_sha256, payload).as_slice(),
        observed.as_slice(),
    )
    .is_ok()
}

fn authenticated_delivery_hmac(
    fetch_token: &str,
    request_sha256: &str,
    payload: &[u8],
) -> hmac::Tag {
    let key = hmac::Key::new(hmac::HMAC_SHA256, fetch_token.as_bytes());
    hmac::sign(&key, authenticated_delivery_message(request_sha256, payload).as_slice())
}

fn authenticated_delivery_message(request_sha256: &str, payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(
        b"palyra.networked-worker.delivery.v1\0"
            .len()
            .saturating_add(request_sha256.len())
            .saturating_add(payload.len()),
    );
    message.extend_from_slice(b"palyra.networked-worker.delivery.v1\0");
    message.extend_from_slice(request_sha256.as_bytes());
    message.extend_from_slice(payload);
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        WorkerArtifactTransport, WorkerRemoteIdentity, WorkerRemoteLeaseBinding,
        WorkerRemoteToolKind, WorkerRemoteWorkspaceTransfer, WorkerWorkspaceScope,
        WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
    };

    fn remote_request() -> WorkerRemoteToolRequestEnvelope {
        let input_json = r#"{"path":"README.md"}"#.to_owned();
        WorkerRemoteToolRequestEnvelope {
            protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
            schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
            request_id: "request-1".to_owned(),
            proposal_id: "proposal-1".to_owned(),
            tool_name: "palyra.fs.read_file".to_owned(),
            tool_kind: WorkerRemoteToolKind::FsRead,
            input_json_sha256: sha256_hex(input_json.as_bytes()),
            input_json,
            lease: WorkerRemoteLeaseBinding {
                lease_id: "lease-1".to_owned(),
                worker_id: "worker-1".to_owned(),
                session_id: "session-1".to_owned(),
                run_id: "run-1".to_owned(),
                run_generation: RuntimeGeneration::new(7).expect("generation"),
                grant_id: "grant-1".to_owned(),
                grant_tool_name: "palyra.fs.read_file".to_owned(),
                expires_at_unix_ms: 120_000,
                required_capabilities: vec!["tool:palyra.fs.read_file".to_owned()],
                process_executable_allowlist: Vec::new(),
                work_graph_claim: None,
                work_graph_posture: Default::default(),
                workspace_scope: WorkerWorkspaceScope {
                    workspace_root: "/workspace".to_owned(),
                    allowed_paths: vec!["README.md".to_owned()],
                    read_only: true,
                },
                artifact_transport: WorkerArtifactTransport {
                    input_manifest_sha256: "1".repeat(64),
                    output_manifest_sha256: "2".repeat(64),
                    log_stream_id: "logs-1".to_owned(),
                    scratch_directory_id: "scratch-1".to_owned(),
                },
            },
            worker_identity: WorkerRemoteIdentity {
                worker_id: "worker-1".to_owned(),
                image_digest_sha256: "3".repeat(64),
                build_digest_sha256: "4".repeat(64),
                artifact_digest_sha256: "5".repeat(64),
                capability_authority_sha256: Some("6".repeat(64)),
                sdk_protocol_version: 1,
                wit_abi_version: "palyra-worker-abi/v1".to_owned(),
            },
            workspace_transfer: WorkerRemoteWorkspaceTransfer::manifest("7".repeat(64)),
            encrypted_secret_artifact: None,
            canonical_protocol: None,
        }
    }

    #[test]
    fn canonical_task_binds_policy_deadline_idempotency_and_fence() {
        let protocol = RemoteWorkerProtocolV1::from_remote_request(&remote_request());
        protocol.validate(60_000).expect("canonical task should validate");
        assert_eq!(protocol.task.deadline_unix_ms, 120_000);
        assert_eq!(protocol.task.run_generation.get(), protocol.task.fence_generation);
        assert_eq!(protocol.task.idempotency_key.len(), 64);
        assert_eq!(protocol.task.cancellation_id.len(), 64);
    }

    #[test]
    fn canonical_task_rejects_clock_skew_and_persistent_secret() {
        let mut protocol = RemoteWorkerProtocolV1::from_remote_request(&remote_request());
        protocol.task.issued_at_unix_ms = 200_001;
        assert_eq!(protocol.validate(60_000), Err(RemoteWorkerProtocolError::ClockSkew));

        protocol.task.issued_at_unix_ms = 60_000;
        protocol.task.secret_lease = Some(EncryptedWorkerSecretLease {
            lease_id: "secret-1".to_owned(),
            ciphertext_artifact_id: "workspace-input-manifest".to_owned(),
            ciphertext_sha256: "1".repeat(64),
            ephemeral_public_key_hex: "2".repeat(64),
            nonce_hex: "3".repeat(24),
            aad_sha256: "4".repeat(64),
            encryption_algorithm: WORKER_SECRET_LEASE_ALGORITHM.to_owned(),
            recipient_key_sha256: "9".repeat(64),
            expires_at_unix_ms: 100_000,
            persistence_allowed: true,
        });
        assert_eq!(
            protocol.validate(60_000),
            Err(RemoteWorkerProtocolError::SecretPersistenceRequested)
        );
    }

    #[test]
    fn secret_lease_encrypts_for_one_recipient_and_rejects_tamper_or_expiry() {
        let recipient = StaticSecret::from([7_u8; 32]);
        let recipient_public = X25519PublicKey::from(&recipient).to_bytes();
        let artifact = seal_worker_secret_lease(
            "secret-lease-1",
            recipient_public,
            b"PALYRA_TEST_EPHEMERAL_SECRET",
            90_000,
        )
        .expect("secret lease should seal");

        let plaintext =
            open_worker_secret_lease(&artifact, &recipient, 60_000).expect("lease should open");
        assert_eq!(plaintext.as_slice(), b"PALYRA_TEST_EPHEMERAL_SECRET");
        assert!(!artifact
            .ciphertext
            .windows(b"PALYRA_TEST_EPHEMERAL_SECRET".len())
            .any(|window| window == b"PALYRA_TEST_EPHEMERAL_SECRET"));
        assert_eq!(artifact.content_addressed_artifact().sha256, artifact.lease.ciphertext_sha256);

        let wrong_recipient = StaticSecret::from([8_u8; 32]);
        assert_eq!(
            open_worker_secret_lease(&artifact, &wrong_recipient, 60_000),
            Err(RemoteWorkerProtocolError::SecretLeaseInvalid)
        );
        assert_eq!(
            open_worker_secret_lease(&artifact, &recipient, 90_000),
            Err(RemoteWorkerProtocolError::SecretLeaseInvalid)
        );

        let mut tampered = artifact;
        tampered.ciphertext[0] ^= 1;
        assert!(matches!(
            open_worker_secret_lease(&tampered, &recipient, 60_000),
            Err(RemoteWorkerProtocolError::DigestMismatch { field: "ciphertext_sha256" })
        ));
    }

    #[test]
    fn delivery_hmac_rejects_payload_and_credential_tampering() {
        let payload = br#"{"request_id":"request-1"}"#;
        let request_sha256 = sha256_hex(payload);
        let hmac =
            authenticated_delivery_hmac_sha256("one-time-fetch-token", &request_sha256, payload);

        assert!(verify_authenticated_delivery_hmac_sha256(
            "one-time-fetch-token",
            &request_sha256,
            payload,
            hmac.as_str()
        ));
        assert!(!verify_authenticated_delivery_hmac_sha256(
            "stale-fetch-token",
            &request_sha256,
            payload,
            hmac.as_str()
        ));
        assert!(!verify_authenticated_delivery_hmac_sha256(
            "one-time-fetch-token",
            &request_sha256,
            br#"{"request_id":"tampered"}"#,
            hmac.as_str()
        ));
    }

    #[test]
    fn canonical_request_rejects_policy_claim_and_resource_mismatch() {
        let mut request = remote_request();
        request.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&request));
        request.canonical_protocol.as_mut().expect("canonical protocol").task.policy_sha256 =
            "f".repeat(64);
        assert!(matches!(
            request.validate(60_000),
            Err(crate::WorkerRemoteToolContractError::CanonicalProtocol { .. })
        ));

        let mut protocol = RemoteWorkerProtocolV1::from_remote_request(&remote_request());
        protocol.task.work_graph_claim = Some(RemoteWorkGraphClaimBinding {
            graph_id: "graph-1".to_owned(),
            node_id: "node-1".to_owned(),
            claim_id: "claim-1".to_owned(),
            claim_generation: 0,
        });
        protocol.task.work_graph_posture = RemoteWorkGraphTaskPosture::Claimed;
        assert_eq!(protocol.validate(60_000), Err(RemoteWorkerProtocolError::GenerationMismatch));

        protocol.task.work_graph_claim = None;
        protocol.task.work_graph_posture = RemoteWorkGraphTaskPosture::DirectToolDispatch;
        protocol.task.max_output_bytes =
            protocol.task.resource_limits.output_artifact_bytes.saturating_add(1);
        assert_eq!(
            protocol.validate(60_000),
            Err(RemoteWorkerProtocolError::ResourceBoundsInvalid)
        );
    }

    #[test]
    fn canonical_projection_preserves_claim_or_explicit_direct_posture() {
        let direct = RemoteWorkerProtocolV1::from_remote_request(&remote_request());
        assert_eq!(direct.task.work_graph_posture, RemoteWorkGraphTaskPosture::DirectToolDispatch);
        assert!(direct.task.work_graph_claim.is_none());

        let claim = RemoteWorkGraphClaimBinding {
            graph_id: "graph-1".to_owned(),
            node_id: "node-1".to_owned(),
            claim_id: "claim-1".to_owned(),
            claim_generation: 3,
        };
        let mut claimed_request = remote_request();
        claimed_request.lease = claimed_request.lease.with_work_graph_claim(claim.clone());
        let claimed = RemoteWorkerProtocolV1::from_remote_request(&claimed_request);

        assert_eq!(claimed.task.work_graph_posture, RemoteWorkGraphTaskPosture::Claimed);
        assert_eq!(claimed.task.work_graph_claim.as_ref(), Some(&claim));
        claimed.validate(60_000).expect("claimed task should preserve exact authority");
    }

    #[test]
    fn remote_outcome_rejects_duplicate_sequence_and_malicious_patch_path() {
        let request = remote_request();
        let task = WorkerTaskEnvelope::from_remote_request(&request);
        let mut outcome = RemoteTaskOutcome {
            task_id: task.task_id.clone(),
            idempotency_key: task.idempotency_key.clone(),
            worker_id: "worker-1".to_owned(),
            response_sequence: 2,
            run_generation: task.run_generation,
            fence_generation: task.fence_generation,
            success: true,
            output_sha256: "a".repeat(64),
            output_artifacts: Vec::new(),
            patch_bundle: None,
            usage: RemoteResourceUsage {
                duration_ms: 1,
                peak_memory_bytes: 1,
                cpu_time_ms: 1,
                input_bytes: 1,
                output_bytes: 1,
            },
            cleanup: RemoteCleanupAttestation {
                workspace_removed: true,
                scratch_artifacts_removed: true,
                logs_removed: true,
                secret_material_removed: true,
                reason_code: "worker.cleanup.ok".to_owned(),
            },
            completed_at_unix_ms: 80_000,
            reason_code: "worker.task.succeeded".to_owned(),
        };
        assert_eq!(
            outcome.validate_against(&task, 80_000),
            Err(RemoteWorkerProtocolError::ResponseSequenceInvalid)
        );

        outcome.response_sequence = 1;
        outcome.patch_bundle = Some(RemotePatchBundle {
            patch_sha256: "b".repeat(64),
            touched_paths: vec!["../escape.txt".to_owned()],
            review_required: true,
        });
        assert_eq!(
            outcome.validate_against(&task, 80_000),
            Err(RemoteWorkerProtocolError::PatchPathEscapesWorkspace {
                path: "../escape.txt".to_owned()
            })
        );

        outcome.patch_bundle = None;
        outcome.usage.peak_memory_bytes = task.resource_limits.memory_bytes.saturating_add(1);
        assert_eq!(
            outcome.validate_against(&task, 80_000),
            Err(RemoteWorkerProtocolError::ResourceBoundsInvalid)
        );

        outcome.usage.peak_memory_bytes = 1;
        outcome.completed_at_unix_ms = task.deadline_unix_ms.saturating_add(1);
        assert_eq!(
            outcome.validate_against(&task, task.deadline_unix_ms.saturating_add(1)),
            Err(RemoteWorkerProtocolError::LateOutcome)
        );
    }
}
